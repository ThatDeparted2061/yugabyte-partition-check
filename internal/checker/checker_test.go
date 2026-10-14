package checker

import (
	"testing"

	"github.com/ThatDeparted2061/yugabyte-partition-check/internal/workload"
)

func tr(from, to int, amt int64, outcome string) workload.Op {
	return workload.Op{Kind: "transfer", From: from, To: to, Amount: amt, Outcome: outcome}
}

func TestCheck(t *testing.T) {
	ops := []workload.Op{
		tr(0, 1, 10, workload.OK),
		tr(1, 2, 5, workload.Fail),
		tr(2, 0, 7, workload.Info),
		{Kind: "read", Outcome: workload.OK, Balances: []int64{90, 110, 100}},
	}
	// The indeterminate transfer happened.
	if r := Check(ops, 3, 100, []int64{97, 110, 93}); !r.Valid {
		t.Fatalf("valid history rejected: %+v", r)
	}
	// It did not happen: also fine.
	if r := Check(ops, 3, 100, []int64{90, 110, 100}); !r.Valid {
		t.Fatalf("valid history rejected: %+v", r)
	}
	// The failed transfer took effect: total is intact but the state is unexplainable.
	if r := Check(ops, 3, 100, []int64{90, 105, 105}); r.Valid || r.Explained {
		t.Fatalf("a failed transfer that took effect must be caught: %+v", r)
	}
	// A read saw half a transfer.
	bad := append(ops, workload.Op{Kind: "read", Outcome: workload.OK, Balances: []int64{90, 100, 100}})
	if r := Check(bad, 3, 100, []int64{90, 110, 100}); r.Valid || r.BadReads != 1 {
		t.Fatalf("a torn read must be caught: %+v", r)
	}
	// Money destroyed.
	if r := Check(ops, 3, 100, []int64{90, 110, 99}); r.Valid {
		t.Fatalf("lost money must be caught: %+v", r)
	}
}
