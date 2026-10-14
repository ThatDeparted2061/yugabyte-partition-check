// Command ybcheck runs a bank-transfer workload against a 3-node YugabyteDB
// cluster while injecting partitions, node kills, pauses and latency, then
// checks that no money was created or lost and no read saw a partial
// transfer. It writes results/<run>.json and results/<run>.html.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ThatDeparted2061/yugabyte-partition-check/internal/checker"
	"github.com/ThatDeparted2061/yugabyte-partition-check/internal/nemesis"
	"github.com/ThatDeparted2061/yugabyte-partition-check/internal/report"
	"github.com/ThatDeparted2061/yugabyte-partition-check/internal/workload"
)

func main() {
	duration := flag.Duration("duration", 10*time.Minute, "how long to run the workload")
	quiet := flag.Duration("quiet", 10*time.Second, "healthy time between faults")
	faultFor := flag.Duration("fault", 15*time.Second, "how long each fault lasts")
	accounts := flag.Int("accounts", 10, "bank accounts")
	initial := flag.Int64("initial", 1000, "starting balance per account")
	writers := flag.Int("writers", 8, "concurrent transfer clients")
	readers := flag.Int("readers", 2, "concurrent snapshot readers")
	nodes := flag.String("nodes", "yb1,yb2,yb3", "container names")
	ports := flag.String("ports", "5433,5434,5435", "host YSQL port of each node")
	noFaults := flag.Bool("no-faults", false, "run the workload without a nemesis (baseline)")
	out := flag.String("out", "results", "output directory")
	faultImage := flag.String("fault-image", "faultbox:local", "image with iptables and tc")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	names, ps := strings.Split(*nodes, ","), strings.Split(*ports, ",")
	var dsns []string
	for _, p := range ps {
		dsns = append(dsns, fmt.Sprintf("postgres://yugabyte:yugabyte@localhost:%s/yugabyte?connect_timeout=3&sslmode=disable", p))
	}
	cfg := workload.Config{Nodes: names, DSNs: dsns, Accounts: *accounts, Initial: *initial, Writers: *writers, Readers: *readers, OpTimeout: 5 * time.Second}

	if err := workload.Setup(ctx, dsns[0], cfg.Accounts, cfg.Initial); err != nil {
		log.Fatalf("setup: %v", err)
	}
	start := time.Now()
	nem := &nemesis.Nemesis{Nodes: names, FaultImage: *faultImage, Start: start, Log: func(f string, a ...any) {
		log.Printf("[%6s] %s", time.Since(start).Round(time.Second), fmt.Sprintf(f, a...))
	}}
	nem.HealAll(ctx)

	run := workload.New(cfg, start)
	wctx, wcancel := context.WithTimeout(ctx, *duration)
	defer wcancel()
	var wg sync.WaitGroup
	wg.Go(func() { run.Run(wctx) })

	var faults []nemesis.Fault
	if !*noFaults {
		rng := rand.New(rand.NewPCG(uint64(start.UnixNano()), 7))
		for {
			select {
			case <-wctx.Done():
			case <-time.After(*quiet):
			}
			if wctx.Err() != nil || time.Until(start.Add(*duration)) < *faultFor {
				break
			}
			f, err := nem.Inject(ctx, rng, *faultFor)
			if err != nil {
				log.Printf("fault %s failed: %v", f.Kind, err)
				nem.HealAll(ctx)
				continue
			}
			faults = append(faults, f)
		}
	}
	wg.Wait()
	elapsed := time.Since(start)

	log.Printf("workload done; healing the cluster and reading final balances")
	nem.HealAll(context.Background())
	waitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for !nem.Healthy(waitCtx) && waitCtx.Err() == nil {
		time.Sleep(2 * time.Second)
	}
	final, err := workload.FinalBalances(waitCtx, dsns, cfg.Accounts)
	if err != nil {
		log.Fatal(err)
	}

	ops := run.Ops()
	result := checker.Check(ops, cfg.Accounts, cfg.Initial, final)
	perSecond, errs := report.Summarise(ops, elapsed)
	r := report.Run{Started: start, Duration: elapsed, YBVersion: ybVersion(), Config: cfg, Faults: faults, Check: result, Errors: errs, PerSecond: perSecond}

	os.MkdirAll(*out, 0o755)
	name := filepath.Join(*out, start.Format("20060102-150405"))
	b, _ := json.MarshalIndent(r, "", "  ")
	os.WriteFile(name+".json", b, 0o644)
	f, err := os.Create(name + ".html")
	if err != nil {
		log.Fatal(err)
	}
	if err := report.Write(f, r); err != nil {
		log.Fatal(err)
	}
	f.Close()
	os.WriteFile(filepath.Join(*out, "latest.json"), b, 0o644)
	copyFile(name+".html", filepath.Join(*out, "latest.html"))

	counts := r.FaultCounts()
	fmt.Printf("\n%d partitions (%d majority/minority, %d non-transitive), %d node kills, %d pauses, %d latency faults over %s\n",
		counts["partition"]+counts["bridge"], counts["partition"], counts["bridge"], counts["kill"], counts["pause"], counts["latency"], elapsed.Round(time.Second))
	fmt.Printf("transfers: %d committed, %d failed, %d indeterminate; reads checked: %d, torn: %d\n",
		result.Transfers, result.Failed, result.Indeterminate, result.Reads, result.BadReads)
	fmt.Printf("final total %d (want %d); state explained: %v (%s)\n", result.FinalTotal, result.ExpectedTotal, result.Explained, result.ExplainMethod)
	if result.Valid {
		fmt.Println("INVARIANT HELD")
	} else {
		fmt.Println("INVARIANT VIOLATED:", strings.Join(result.Problems, "; "))
		os.Exit(1)
	}
	fmt.Println("report:", name+".html")
}

func ybVersion() string {
	out, err := exec.Command("docker", "inspect", "-f", "{{.Config.Image}}", "yb1").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimPrefix(strings.TrimSpace(string(out)), "yugabytedb/yugabyte:")
}

func copyFile(src, dst string) {
	b, err := os.ReadFile(src)
	if err == nil {
		os.WriteFile(dst, b, 0o644)
	}
}
