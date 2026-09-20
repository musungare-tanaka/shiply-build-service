package main

import (
	"context"
	"testing"
	"time"
)

func TestStageLedgerClaimLifecycle(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ledger := newMemoryStageLedger()
	ledger.now = func() time.Time { return now }

	disposition, record, err := ledger.Claim(context.Background(), "deployment-1", "build", time.Minute)
	if err != nil || disposition != ClaimAcquired || record.Attempt != 1 {
		t.Fatalf("fresh claim = %v %#v %v", disposition, record, err)
	}

	disposition, _, _ = ledger.Claim(context.Background(), "deployment-1", "build", time.Minute)
	if disposition != ClaimBusy {
		t.Fatalf("live lease disposition = %v", disposition)
	}

	now = now.Add(2 * time.Minute)
	disposition, record, _ = ledger.Claim(context.Background(), "deployment-1", "build", time.Minute)
	if disposition != ClaimAcquired || record.Attempt != 2 {
		t.Fatalf("expired claim = %v attempt %d", disposition, record.Attempt)
	}

	if err := ledger.Complete(context.Background(), "deployment-1", "build", map[string]string{"image": "tag"}); err != nil {
		t.Fatal(err)
	}
	disposition, record, _ = ledger.Claim(context.Background(), "deployment-1", "build", time.Minute)
	if disposition != ClaimCompleted || len(record.ResultJSON) == 0 {
		t.Fatalf("completed claim = %v %#v", disposition, record)
	}
}
