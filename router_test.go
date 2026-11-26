package router

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestOrderLifecycleSuccess(t *testing.T) {
	r := NewOrderRouter(5 * time.Minute)
	orderID := "ord-1001"

	events := []OrderEvent{
		{EventID: "evt-1", OrderID: orderID, Sequence: 1, Status: StatusCreated, Timestamp: time.Now()},
		{EventID: "evt-2", OrderID: orderID, Sequence: 2, Status: StatusPaymentPending, Timestamp: time.Now()},
		{EventID: "evt-3", OrderID: orderID, Sequence: 3, Status: StatusPaid, Timestamp: time.Now()},
		{EventID: "evt-4", OrderID: orderID, Sequence: 4, Status: StatusInventoryReserved, Timestamp: time.Now()},
		{EventID: "evt-5", OrderID: orderID, Sequence: 5, Status: StatusFulfilled, Timestamp: time.Now()},
	}

	for _, e := range events {
		res := r.ProcessEvent(e)
		if res.Verdict != VerdictAccepted {
			t.Fatalf("expected ACCEPTED for %s, got %s: %s", e.Status, res.Verdict, res.Reason)
		}
	}

	status, ok := r.GetStatus(orderID)
	if !ok || status != StatusFulfilled {
		t.Fatalf("expected order %s to be FULFILLED, got %s", orderID, status)
	}
}

func TestDeduplication(t *testing.T) {
	r := NewOrderRouter(5 * time.Minute)
	orderID := "ord-1002"

	e := OrderEvent{
		EventID:        "evt-dup-1",
		OrderID:        orderID,
		Sequence:       1,
		Status:         StatusCreated,
		IdempotencyKey: "idem-key-1002",
		Timestamp:      time.Now(),
	}

	res1 := r.ProcessEvent(e)
	if res1.Verdict != VerdictAccepted {
		t.Fatalf("expected first event accepted, got %s", res1.Verdict)
	}

	// Replay exact duplicate event
	res2 := r.ProcessEvent(e)
	if res2.Verdict != VerdictDuplicate {
		t.Fatalf("expected DUPLICATE on replay, got %s", res2.Verdict)
	}
}

func TestOutOfOrderSequencing(t *testing.T) {
	r := NewOrderRouter(5 * time.Minute)
	orderID := "ord-1003"

	// Attempt to arrive with PAID before CREATED
	premature := OrderEvent{
		EventID:   "evt-premature",
		OrderID:   orderID,
		Sequence:  2,
		Status:    StatusPaid,
		Timestamp: time.Now(),
	}
	res := r.ProcessEvent(premature)
	if res.Verdict != VerdictOutOfOrder {
		t.Fatalf("expected OUT_OF_ORDER, got %s", res.Verdict)
	}
	if !res.Retryable {
		t.Fatalf("expected out-of-order event to be retryable")
	}
}

func TestIllegalStateTransition(t *testing.T) {
	r := NewOrderRouter(5 * time.Minute)
	orderID := "ord-1004"

	// Create order
	r.ProcessEvent(OrderEvent{EventID: "e1", OrderID: orderID, Sequence: 1, Status: StatusCreated})

	// Illegal skip directly to FULFILLED
	illegal := OrderEvent{
		EventID:   "e2",
		OrderID:   orderID,
		Sequence:  2,
		Status:    StatusFulfilled,
		Timestamp: time.Now(),
	}
	res := r.ProcessEvent(illegal)
	if res.Verdict != VerdictInvalidTransition {
		t.Fatalf("expected INVALID_TRANSITION, got %s", res.Verdict)
	}
}

func TestConcurrentEventProcessing(t *testing.T) {
	r := NewOrderRouter(5 * time.Minute)
	var wg sync.WaitGroup

	numOrders := 50
	for i := 0; i < numOrders; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ord := fmt.Sprintf("order-concurrent-%d", idx)
			r.ProcessEvent(OrderEvent{EventID: fmt.Sprintf("c1-%d", idx), OrderID: ord, Sequence: 1, Status: StatusCreated})
			r.ProcessEvent(OrderEvent{EventID: fmt.Sprintf("c2-%d", idx), OrderID: ord, Sequence: 2, Status: StatusPaymentPending})
			r.ProcessEvent(OrderEvent{EventID: fmt.Sprintf("c3-%d", idx), OrderID: ord, Sequence: 3, Status: StatusPaid})
		}(i)
	}
	wg.Wait()

	// Verify all created
	for i := 0; i < numOrders; i++ {
		ord := fmt.Sprintf("order-concurrent-%d", i)
		st, ok := r.GetStatus(ord)
		if !ok || st != StatusPaid {
			t.Fatalf("expected order %s to be PAID, got %s", ord, st)
		}
	}
}
