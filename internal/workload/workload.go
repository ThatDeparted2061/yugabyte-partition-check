// Package workload runs a bank-transfer workload against YSQL and records
// every operation: concurrent serializable transfers between accounts, and
// snapshot reads of every balance.
package workload

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Outcome of an operation. "info" means indeterminate: the commit may or may
// not have happened (the connection died while COMMIT was in flight).
const (
	OK   = "ok"
	Fail = "fail"
	Info = "info"
)

// Op is one recorded operation.
type Op struct {
	Kind     string        `json:"kind"` // transfer | read
	Node     string        `json:"node"`
	Start    time.Duration `json:"start"`
	End      time.Duration `json:"end"`
	Outcome  string        `json:"outcome"`
	Error    string        `json:"error,omitempty"`
	From     int           `json:"from,omitempty"`
	To       int           `json:"to,omitempty"`
	Amount   int64         `json:"amount,omitempty"`
	Balances []int64       `json:"balances,omitempty"` // reads: balance of every account, by id
}

// Config sizes the workload.
type Config struct {
	Nodes     []string // node names, parallel to DSNs
	DSNs      []string `json:"-"`
	Accounts  int
	Initial   int64 // starting balance of each account
	Writers   int
	Readers   int
	OpTimeout time.Duration
}

// Total is the money in the bank, which must never change.
func (c Config) Total() int64 { return int64(c.Accounts) * c.Initial }

// Runner executes the workload and keeps its history.
type Runner struct {
	cfg   Config
	start time.Time

	mu  sync.Mutex
	ops []Op
}

func New(cfg Config, start time.Time) *Runner { return &Runner{cfg: cfg, start: start} }

// Setup (re)creates the accounts table with every account at Initial.
func Setup(ctx context.Context, dsn string, accounts int, initial int64) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `DROP TABLE IF EXISTS accounts`); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, `CREATE TABLE accounts (id int PRIMARY KEY, balance bigint NOT NULL) SPLIT INTO 3 TABLETS`); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `INSERT INTO accounts SELECT g, $1 FROM generate_series(0, $2 - 1) g`, initial, accounts)
	return err
}

// Ops returns a copy of the history so far.
func (r *Runner) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Op(nil), r.ops...)
}

func (r *Runner) record(op Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

// Run starts the writers and readers and blocks until ctx is done.
func (r *Runner) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := range r.cfg.Writers + r.cfg.Readers {
		reader := i >= r.cfg.Writers
		wg.Go(func() { r.worker(ctx, i, reader) })
	}
	wg.Wait()
}

// worker keeps one connection, starting on node i mod n and moving to the
// next node whenever its connection breaks, as a client-side load balancer would.
func (r *Runner) worker(ctx context.Context, id int, reader bool) {
	rng := rand.New(rand.NewPCG(uint64(id), uint64(time.Now().UnixNano())))
	node := id % len(r.cfg.DSNs)
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			conn.Close(context.Background())
		}
	}()
	for ctx.Err() == nil {
		if conn == nil {
			cctx, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
			c, err := pgx.Connect(cctx, r.cfg.DSNs[node])
			cancel()
			if err != nil {
				node = (node + 1) % len(r.cfg.DSNs)
				sleep(ctx, 100*time.Millisecond)
				continue
			}
			conn = c
		}
		var op Op
		if reader {
			op = r.read(ctx, conn)
		} else {
			op = r.transfer(ctx, conn, rng)
		}
		if ctx.Err() != nil && op.Outcome != OK {
			return // shutting down: do not record ops cut short by the end of the run
		}
		op.Node = r.cfg.Nodes[node]
		r.record(op)
		if op.Outcome != OK && broken(conn, op) {
			conn.Close(context.Background())
			conn = nil
			node = (node + 1) % len(r.cfg.DSNs)
		}
	}
}

func (r *Runner) since() time.Duration { return time.Since(r.start) }

func (r *Runner) transfer(ctx context.Context, conn *pgx.Conn, rng *rand.Rand) (op Op) {
	from := rng.IntN(r.cfg.Accounts)
	to := (from + 1 + rng.IntN(r.cfg.Accounts-1)) % r.cfg.Accounts
	op = Op{Kind: "transfer", From: from, To: to, Amount: 1 + rng.Int64N(50), Start: r.since()}
	defer func() { op.End = r.since() }() // named result: the defer sees what is returned

	octx, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
	defer cancel()
	tx, err := conn.BeginTx(octx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return failed(op, err)
	}
	// Lock both rows in id order, so two transfers between the same accounts
	// queue behind each other instead of deadlocking.
	var balance int64
	for _, id := range []int{min(from, to), max(from, to)} {
		var b int64
		if err := tx.QueryRow(octx, `SELECT balance FROM accounts WHERE id = $1 FOR UPDATE`, id).Scan(&b); err != nil {
			tx.Rollback(context.Background())
			return failed(op, err)
		}
		if id == from {
			balance = b
		}
	}
	if balance < op.Amount {
		tx.Rollback(context.Background())
		op.Outcome, op.Error = Fail, "insufficient funds"
		return op
	}
	for _, q := range []struct {
		id    int
		delta int64
	}{{from, -op.Amount}, {to, op.Amount}} {
		if _, err := tx.Exec(octx, `UPDATE accounts SET balance = balance + $1 WHERE id = $2`, q.delta, q.id); err != nil {
			tx.Rollback(context.Background())
			return failed(op, err)
		}
	}
	if err := tx.Commit(octx); err != nil {
		// A server-side error on COMMIT means it was rolled back. Anything
		// else (timeout, broken connection) leaves the outcome unknown.
		var pg *pgconn.PgError
		if errors.As(err, &pg) || errors.Is(err, pgx.ErrTxCommitRollback) {
			return failed(op, err)
		}
		op.Outcome, op.Error = Info, classify(err)
		return op
	}
	op.Outcome = OK
	return op
}

func (r *Runner) read(ctx context.Context, conn *pgx.Conn) (op Op) {
	op = Op{Kind: "read", Start: r.since()}
	defer func() { op.End = r.since() }()
	octx, cancel := context.WithTimeout(ctx, r.cfg.OpTimeout)
	defer cancel()
	tx, err := conn.BeginTx(octx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return failed(op, err)
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(octx, `SELECT id, balance FROM accounts ORDER BY id`)
	if err != nil {
		return failed(op, err)
	}
	balances := make([]int64, r.cfg.Accounts)
	seen := 0
	for rows.Next() {
		var id int
		var b int64
		if err := rows.Scan(&id, &b); err != nil {
			rows.Close()
			return failed(op, err)
		}
		if id >= 0 && id < len(balances) {
			balances[id] = b
			seen++
		}
	}
	if err := rows.Err(); err != nil {
		return failed(op, err)
	}
	if seen != r.cfg.Accounts {
		op.Error = fmt.Sprintf("saw %d of %d accounts", seen, r.cfg.Accounts) // the checker flags the missing money
	}
	op.Outcome, op.Balances = OK, balances
	return op
}

// FinalBalances reads every balance once the cluster is healthy again,
// retrying until it succeeds or ctx ends.
func FinalBalances(ctx context.Context, dsns []string, accounts int) ([]int64, error) {
	var last error
	for i := 0; ctx.Err() == nil; i++ {
		r := &Runner{cfg: Config{Accounts: accounts, OpTimeout: 10 * time.Second}, start: time.Now()}
		conn, err := pgx.Connect(ctx, dsns[i%len(dsns)])
		if err == nil {
			op := r.read(ctx, conn)
			conn.Close(ctx)
			if op.Outcome == OK {
				return op.Balances, nil
			}
			err = errors.New(op.Error)
		}
		last = err
		sleep(ctx, time.Second)
	}
	return nil, fmt.Errorf("final read never succeeded: %w", last)
}

func failed(op Op, err error) Op {
	op.Outcome, op.Error = Fail, classify(err)
	return op
}

// classify shortens an error to a stable label for the report.
func classify(err error) string {
	var pg *pgconn.PgError
	switch {
	case errors.As(err, &pg):
		return pg.Code + " " + pg.Severity
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, pgx.ErrTxCommitRollback):
		return "rolled back"
	}
	return "connection error"
}

// broken reports whether the connection should be replaced after a failure.
func broken(conn *pgx.Conn, op Op) bool {
	return conn.IsClosed() || op.Error == "timeout" || op.Error == "connection error" || op.Outcome == Info
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
