// Package checker verifies a bank-transfer history.
//
// Invariants:
//  1. Every snapshot read sees the full total and no negative balance: no read
//     observes half of a transfer.
//  2. The final balances add up to the starting total: money is neither
//     created nor destroyed.
//  3. Every account's final balance is explained by the transfers that
//     committed, plus some subset of the indeterminate ones (whose COMMIT
//     outcome the client never learned): no acknowledged transfer was lost
//     and no failed one took effect.
package checker

import (
	"fmt"

	"github.com/ThatDeparted2061/yugabyte-partition-check/internal/workload"
)

// Result summarises a check.
type Result struct {
	Valid          bool     `json:"valid"`
	Reads          int      `json:"reads"`
	BadReads       int      `json:"bad_reads"` // reads that broke invariant 1
	Transfers      int      `json:"transfers_ok"`
	Failed         int      `json:"transfers_failed"`
	Indeterminate  int      `json:"transfers_indeterminate"`
	ExpectedTotal  int64    `json:"expected_total"`
	FinalTotal     int64    `json:"final_total"`
	Explained      bool     `json:"final_state_explained"` // invariant 3
	ExplainMethod  string   `json:"explain_method"`
	Problems       []string `json:"problems,omitempty"`
	FinalBalances  []int64  `json:"final_balances"`
	FirstBadReadAt string   `json:"first_bad_read_at,omitempty"`
}

// maxExact is the most indeterminate transfers for which every subset is tried.
const maxExact = 20

// Check runs all three invariants.
func Check(ops []workload.Op, accounts int, initial int64, final []int64) Result {
	r := Result{ExpectedTotal: int64(accounts) * initial, FinalBalances: final}
	base := make([]int64, accounts) // initial + committed transfers
	for i := range base {
		base[i] = initial
	}
	var unknown []workload.Op
	for _, op := range ops {
		switch {
		case op.Kind == "read" && op.Outcome == workload.OK:
			r.Reads++
			var sum int64
			neg := false
			for _, b := range op.Balances {
				sum += b
				neg = neg || b < 0
			}
			if sum != r.ExpectedTotal || neg {
				r.BadReads++
				if r.FirstBadReadAt == "" {
					r.FirstBadReadAt = op.Start.String()
				}
				if len(r.Problems) < 10 {
					r.Problems = append(r.Problems, fmt.Sprintf("read at %s on %s saw total %d (want %d)", op.Start.Round(1e6), op.Node, sum, r.ExpectedTotal))
				}
			}
		case op.Kind == "transfer" && op.Outcome == workload.OK:
			r.Transfers++
			base[op.From] -= op.Amount
			base[op.To] += op.Amount
		case op.Kind == "transfer" && op.Outcome == workload.Info:
			r.Indeterminate++
			unknown = append(unknown, op)
		case op.Kind == "transfer":
			r.Failed++
		}
	}
	for _, b := range final {
		r.FinalTotal += b
	}
	if r.FinalTotal != r.ExpectedTotal {
		r.Problems = append(r.Problems, fmt.Sprintf("final total %d, want %d", r.FinalTotal, r.ExpectedTotal))
	}
	r.Explained, r.ExplainMethod = explain(base, unknown, final)
	if !r.Explained {
		r.Problems = append(r.Problems, "final balances cannot be produced by the committed transfers plus any subset of the indeterminate ones")
	}
	r.Valid = r.BadReads == 0 && r.FinalTotal == r.ExpectedTotal && r.Explained
	return r
}

// explain reports whether final = base + the effect of some subset of unknown.
func explain(base []int64, unknown []workload.Op, final []int64) (bool, string) {
	if len(final) != len(base) {
		return false, "account count mismatch"
	}
	need := make([]int64, len(base))
	for i := range base {
		need[i] = final[i] - base[i]
	}
	if len(unknown) <= maxExact {
		for mask := 0; mask < 1<<len(unknown); mask++ {
			got := make([]int64, len(base))
			for i, op := range unknown {
				if mask&(1<<i) != 0 {
					got[op.From] -= op.Amount
					got[op.To] += op.Amount
				}
			}
			if equal(got, need) {
				return true, fmt.Sprintf("exact: checked subsets of %d indeterminate transfers", len(unknown))
			}
		}
		return false, fmt.Sprintf("exact: no subset of %d indeterminate transfers fits", len(unknown))
	}
	// Too many for an exhaustive search: check each account's bounds instead.
	lo, hi := make([]int64, len(base)), make([]int64, len(base))
	for _, op := range unknown {
		lo[op.From] -= op.Amount
		hi[op.To] += op.Amount
	}
	for i := range need {
		if need[i] < lo[i] || need[i] > hi[i] {
			return false, "bounds"
		}
	}
	return true, fmt.Sprintf("bounds: %d indeterminate transfers", len(unknown))
}

func equal(a, b []int64) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
