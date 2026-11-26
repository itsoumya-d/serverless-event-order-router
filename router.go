package router

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Version indicates the current package release.
const Version = "0.1.0"

// OrderStatus defines the lifecycle states of an e-commerce order.
type OrderStatus string

const (
	StatusCreated           OrderStatus = "CREATED"
	StatusPaymentPending    OrderStatus = "PAYMENT_PENDING"
	StatusPaid              OrderStatus = "PAID"
	StatusInventoryReserved OrderStatus = "INVENTORY_RESERVED"
	StatusFulfilled         OrderStatus = "FULFILLED"
	StatusCancelled         OrderStatus = "CANCELLED"
)

// RoutingVerdict represents the router's disposition for an event.
type RoutingVerdict string

const (
	VerdictAccepted          RoutingVerdict = "ACCEPTED"
	VerdictDuplicate         RoutingVerdict = "DUPLICATE"
	VerdictOutOfOrder        RoutingVerdict = "OUT_OF_ORDER"
	VerdictInvalidTransition RoutingVerdict = "INVALID_TRANSITION"
)

// OrderEvent represents a serverless event envelope.
type OrderEvent struct {
	EventID        string
	OrderID        string
	Sequence       uint64
	Status         OrderStatus
	Timestamp      time.Time
	IdempotencyKey string
	Payload        map[string]any
}

// RoutingResult describes the outcome of routing an event.
type RoutingResult struct {
	Verdict   RoutingVerdict
	OrderID   string
	Reason    string
	Retryable bool
}

type orderRecord struct {
	status  OrderStatus
	lastSeq uint64
}

// OrderRouter coordinates idempotent ingestion and strict order event sequencing.
type OrderRouter struct {
	mu           sync.RWMutex
	dedupWindow  time.Duration
	seenEvents   map[string]time.Time
	orderStates  map[string]orderRecord
	validFlow    map[OrderStatus]map[OrderStatus]bool
}

// NewOrderRouter initializes an OrderRouter with an idempotency retention window.
func NewOrderRouter(dedupWindow time.Duration) *OrderRouter {
	if dedupWindow <= 0 {
		dedupWindow = 10 * time.Minute
	}

	validFlow := map[OrderStatus]map[OrderStatus]bool{
		StatusCreated: {
			StatusPaymentPending: true,
			StatusCancelled:      true,
		},
		StatusPaymentPending: {
			StatusPaid:      true,
			StatusCancelled: true,
		},
		StatusPaid: {
			StatusInventoryReserved: true,
			StatusCancelled:         true,
		},
		StatusInventoryReserved: {
			StatusFulfilled: true,
			StatusCancelled: true,
		},
		StatusFulfilled: {},
		StatusCancelled: {},
	}

	return &OrderRouter{
		dedupWindow: dedupWindow,
		seenEvents:  make(map[string]time.Time),
		orderStates: make(map[string]orderRecord),
		validFlow:   validFlow,
	}
}

// ProcessEvent routes an incoming order event, enforcing deduplication and state sequence.
func (r *OrderRouter) ProcessEvent(e OrderEvent) RoutingResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	r.evictExpired(now)

	// 1. Idempotency Check
	idempotencyHash := r.computeHash(e)
	if _, seen := r.seenEvents[idempotencyHash]; seen {
		return RoutingResult{
			Verdict:   VerdictDuplicate,
			OrderID:   e.OrderID,
			Reason:    fmt.Sprintf("duplicate event suppressed: %s", idempotencyHash[:12]),
			Retryable: false,
		}
	}

	// 2. State & Sequence Validation
	record, exists := r.orderStates[e.OrderID]
	if !exists {
		if e.Status != StatusCreated {
			return RoutingResult{
				Verdict:   VerdictOutOfOrder,
				OrderID:   e.OrderID,
				Reason:    fmt.Sprintf("first event must be CREATED, got %s", e.Status),
				Retryable: true,
			}
		}
		r.orderStates[e.OrderID] = orderRecord{status: e.Status, lastSeq: e.Sequence}
		r.seenEvents[idempotencyHash] = now
		return RoutingResult{
			Verdict:   VerdictAccepted,
			OrderID:   e.OrderID,
			Reason:    "order initialized",
			Retryable: false,
		}
	}

	// Sequence check
	if e.Sequence <= record.lastSeq {
		return RoutingResult{
			Verdict:   VerdictOutOfOrder,
			OrderID:   e.OrderID,
			Reason:    fmt.Sprintf("stale sequence: incoming seq %d <= current seq %d", e.Sequence, record.lastSeq),
			Retryable: false,
		}
	}

	// Transition check
	allowedNext, hasTransitions := r.validFlow[record.status]
	if !hasTransitions || !allowedNext[e.Status] {
		return RoutingResult{
			Verdict:   VerdictInvalidTransition,
			OrderID:   e.OrderID,
			Reason:    fmt.Sprintf("illegal state transition from %s to %s", record.status, e.Status),
			Retryable: false,
		}
	}

	// Update state
	r.orderStates[e.OrderID] = orderRecord{status: e.Status, lastSeq: e.Sequence}
	r.seenEvents[idempotencyHash] = now

	return RoutingResult{
		Verdict:   VerdictAccepted,
		OrderID:   e.OrderID,
		Reason:    fmt.Sprintf("transitioned to %s", e.Status),
		Retryable: false,
	}
}

// GetStatus returns the current order status and presence.
func (r *OrderRouter) GetStatus(orderID string) (OrderStatus, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.orderStates[orderID]
	if !ok {
		return "", false
	}
	return rec.status, true
}

func (r *OrderRouter) computeHash(e OrderEvent) string {
	raw := fmt.Sprintf("%s:%s:%d:%s:%s", e.EventID, e.OrderID, e.Sequence, e.Status, e.IdempotencyKey)
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:])
}

func (r *OrderRouter) evictExpired(now time.Time) {
	for k, ts := range r.seenEvents {
		if now.Sub(ts) > r.dedupWindow {
			delete(r.seenEvents, k)
		}
	}
}
