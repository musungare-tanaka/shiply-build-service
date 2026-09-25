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

func TestStageLedgerRetryCountSurvivesReleaseAndClaim(t *testing.T) {
	ledger := newMemoryStageLedger()
	ctx := context.Background()

	_, _, err := ledger.Claim(ctx, "deployment-retry", "build", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for expected := 1; expected <= 5; expected++ {
		actual, retryErr := ledger.RecordRetry(ctx, "deployment-retry", "build", "temporary failure")
		if retryErr != nil {
			t.Fatal(retryErr)
		}
		if actual != expected {
			t.Fatalf("expected retry count %d, got %d", expected, actual)
		}
	}
	if err := ledger.Release(ctx, "deployment-retry", "build"); err != nil {
		t.Fatal(err)
	}
	_, record, err := ledger.Claim(ctx, "deployment-retry", "build", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if record.RetryCount != 5 {
		t.Fatalf("expected durable retry count 5, got %d", record.RetryCount)
	}
}
