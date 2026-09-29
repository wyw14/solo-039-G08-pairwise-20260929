package reflux

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/wyw14/solo-039/internal/model"
	"strings"
	"sync"
)

// CapacityError is returned when a new lease request cannot fit the currently
// healthy capacity. It carries the numbers and the health reasons so callers
// never have to guess why capacity was refused.
type CapacityError struct {
	Requested float64
	Used      float64
	Available float64
	Rated     float64
	Factor    float64
	Reasons   []string
}

func (e *CapacityError) Error() string {
	var b strings.Builder
	if e.Factor >= 1 {
		fmt.Fprintf(&b, "reflux capacity unavailable: requested %.3f, %.3f already in use of %.3f rated",
			e.Requested, e.Used, e.Rated)
	} else {
		fmt.Fprintf(&b, "reflux capacity unavailable: requested %.3f, %.3f already in use of %.3f available (%.3f rated derated to %.0f%% by pump health)",
			e.Requested, e.Used, e.Available, e.Rated, e.Factor*100)
	}
	if len(e.Reasons) > 0 {
		fmt.Fprintf(&b, ": %s", strings.Join(e.Reasons, "; "))
	}
	return b.String()
}

// LeaseOvercommit describes how much of one existing lease exceeds the current
// healthy capacity. Existing leases are never trimmed silently: they are
// reported here so the caller can decide (revoke, renegotiate, or escalate).
type LeaseOvercommit struct {
	Lease   model.Lease
	Covered float64
	Excess  float64
}

// HealthReport is the result of reconciling outstanding leases against the
// latest pump health verdict.
type HealthReport struct {
	Rated         float64
	Available     float64
	Used          float64
	Factor        float64
	Reasons       []string
	Overcommitted bool
	TotalExcess   float64
	Leases        []LeaseOvercommit
}

type Coordinator struct {
	mu       sync.Mutex
	capacity float64
	health   *HealthMonitor
	states   map[string]*State
	leases   map[uuid.UUID]model.Lease
	order    []uuid.UUID
}

func NewCoordinator(capacity float64) *Coordinator {
	return NewCoordinatorWithHealth(capacity, NewHealthMonitor(DefaultHealthThresholds()))
}

// NewCoordinatorWithHealth builds a coordinator backed by an externally
// configured health monitor (custom thresholds or a shared fleet monitor).
func NewCoordinatorWithHealth(capacity float64, health *HealthMonitor) *Coordinator {
	return &Coordinator{
		capacity: capacity,
		health:   health,
		states:   make(map[string]*State),
		leases:   make(map[uuid.UUID]model.Lease),
	}
}

func (c *Coordinator) Register(name string, state *State) {
	c.mu.Lock()
	c.states[name] = state
	c.mu.Unlock()
}

// Health exposes the pump health monitor so readings can be reported.
func (c *Coordinator) Health() *HealthMonitor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.health
}

// RatedCapacity is the installed pump capacity ignoring health.
func (c *Coordinator) RatedCapacity() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capacity
}

// AvailableCapacity is the capacity that can currently be promised after
// applying the pump-health derate.
func (c *Coordinator) AvailableCapacity() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.capacity * c.health.FleetFactor()
}

// Acquire grants a NEW lease only when the full amount fits inside the
// currently healthy capacity. It never oversubscribes: unhealthy derating
// shrinks what new requests may take, and the returned CapacityError says by
// how much and why.
func (c *Coordinator) Acquire(name string, amount float64, fence uint64) (model.Lease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	factor := c.health.FleetFactor()
	available := c.capacity * factor
	used := c.usedLocked()
	if amount <= 0 || used+amount > available {
		return model.Lease{}, &CapacityError{
			Requested: amount,
			Used:      used,
			Available: available,
			Rated:     c.capacity,
			Factor:    factor,
			Reasons:   c.health.FleetReasons(),
		}
	}
	lease := model.Lease{ID: uuid.New(), Owner: name, Capacity: amount, Fence: fence, Active: true}
	c.leases[lease.ID] = lease
	c.order = append(c.order, lease.ID)
	if state := c.states[name]; state != nil {
		state.SetLease(lease)
	}
	return lease, nil
}

// Release retires a lease. Releasing an unknown or already inactive lease is a
// no-op so capacity is never double-counted.
func (c *Coordinator) Release(lease model.Lease) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current, ok := c.leases[lease.ID]
	if !ok || !current.Active {
		return
	}
	current.Active = false
	c.leases[lease.ID] = current
}

func (c *Coordinator) usedLocked() float64 {
	var used float64
	for _, lease := range c.leases {
		if lease.Active {
			used += lease.Capacity
		}
	}
	return used
}

func (c *Coordinator) Used() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.usedLocked()
}

// ActiveLeases returns outstanding leases in issuance order (oldest first).
func (c *Coordinator) ActiveLeases() []model.Lease {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.Lease, 0, len(c.order))
	for _, id := range c.order {
		if lease := c.leases[id]; lease.Active {
			out = append(out, lease)
		}
	}
	return out
}

// ReconcileHealth compares leases granted BEFORE the latest health verdict with
// the capacity that is safe NOW. Old leases are honoured rather than silently
// trimmed, but any portion no longer covered is reported per lease (oldest
// first) with the total excess and the derating reasons.
func (c *Coordinator) ReconcileHealth() HealthReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	factor := c.health.FleetFactor()
	available := c.capacity * factor
	report := HealthReport{
		Rated:     c.capacity,
		Available: available,
		Factor:    factor,
		Reasons:   c.health.FleetReasons(),
		Leases:    make([]LeaseOvercommit, 0),
	}
	remaining := available
	for _, id := range c.order {
		lease := c.leases[id]
		if !lease.Active {
			continue
		}
		report.Used += lease.Capacity
		covered := lease.Capacity
		if remaining > 0 {
			if covered > remaining {
				covered = remaining
			}
			remaining -= covered
		} else {
			covered = 0
		}
		excess := lease.Capacity - covered
		if excess > 0 {
			report.Overcommitted = true
			report.TotalExcess += excess
			report.Leases = append(report.Leases, LeaseOvercommit{Lease: lease, Covered: covered, Excess: excess})
		}
	}
	return report
}
