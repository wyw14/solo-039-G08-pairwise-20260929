package reflux

import (
	"errors"
	"fmt"
	"github.com/wyw14/solo-039/internal/model"
	"sort"
	"sync"
)

// DefaultPump is the pump used by leases that do not name one explicitly.
const DefaultPump = "P-101"

// Overcommit describes an existing lease that no longer fits the pump's health-derived capacity.
type Overcommit struct {
	Pump      string
	Lease     model.Lease
	Granted   float64
	Available float64
	Reasons   []string
}

func (o Overcommit) Error() string {
	return fmt.Sprintf("pump %s lease %.3f exceeds health-limited capacity %.3f", o.Pump, o.Granted, o.Available)
}

type pumpLedger struct {
	used     float64
	leases   map[string]model.Lease
	reported map[string]bool
}

type Coordinator struct {
	mu       sync.Mutex
	capacity float64
	pumps    map[string]*pumpLedger
	states   map[string]*State
	health   *HealthMonitor
}

func NewCoordinator(capacity float64) *Coordinator {
	c := &Coordinator{
		capacity: capacity,
		pumps:    make(map[string]*pumpLedger),
		states:   make(map[string]*State),
		health:   NewHealthMonitor(DefaultLimits()),
	}
	c.pumps[DefaultPump] = &pumpLedger{leases: make(map[string]model.Lease), reported: make(map[string]bool)}
	return c
}

// WithHealthMonitor replaces the default health monitor (must be called before use).
func (c *Coordinator) WithHealthMonitor(monitor *HealthMonitor) *Coordinator {
	c.mu.Lock()
	c.health = monitor
	c.mu.Unlock()
	return c
}

// Health exposes the pump health monitor so telemetry points can be ingested.
func (c *Coordinator) Health() *HealthMonitor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.health
}

func (c *Coordinator) Register(name string, state *State) {
	c.mu.Lock()
	c.states[name] = state
	c.mu.Unlock()
}

func (c *Coordinator) ledgerFor(pump string) *pumpLedger {
	p, ok := c.pumps[pump]
	if !ok {
		p = &pumpLedger{leases: make(map[string]model.Lease), reported: make(map[string]bool)}
		c.pumps[pump] = p
	}
	return p
}

// availableCapacityLocked returns the health-limited capacity for a pump and the reasons it is reduced.
// Caller must hold c.mu.
func (c *Coordinator) availableCapacityLocked(pump string) (float64, []string) {
	assessment := c.health.Health(pump)
	return c.capacity * assessment.AvailableFactor, assessment.Reasons
}

// Acquire grants a lease on the default pump. It is the pump-targeting form for
// callers that do not name a pump.
func (c *Coordinator) Acquire(name string, amount float64, fence uint64) (model.Lease, error) {
	return c.AcquireOn(DefaultPump, name, amount, fence)
}

// AcquireOn grants a lease only when the named pump's health-derived capacity can cover it.
// New applications are rejected on deration rather than silently overcommitting; the
// returned error states the capacity currently available and the cause.
func (c *Coordinator) AcquireOn(pump, name string, amount float64, fence uint64) (model.Lease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pump == "" {
		pump = DefaultPump
	}
	if amount <= 0 {
		return model.Lease{}, errors.New("reflux capacity unavailable")
	}
	available, reasons := c.availableCapacityLocked(pump)
	ledger := c.ledgerFor(pump)
	if _, exists := ledger.leases[name]; exists {
		return model.Lease{}, fmt.Errorf("pump %s already has an active lease for %s", pump, name)
	}
	if ledger.used+amount > available {
		if len(reasons) > 0 {
			return model.Lease{}, fmt.Errorf("reflux capacity unavailable: requested %.3f but pump %s offers %.3f (%v)", amount, pump, available, reasons)
		}
		return model.Lease{}, errors.New("reflux capacity unavailable")
	}
	ledger.used += amount
	lease := model.Lease{ID: model.NewCampaign(name).ID, Owner: name, Capacity: amount, Fence: fence, Active: true}
	ledger.leases[name] = lease
	if state := c.states[name]; state != nil {
		state.SetLease(lease)
	}
	return lease, nil
}

func (c *Coordinator) Release(lease model.Lease) {
	c.ReleaseOn(DefaultPump, lease)
}

// ReleaseOn releases a lease previously granted on the named pump.
func (c *Coordinator) ReleaseOn(pump string, lease model.Lease) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !lease.Active {
		return
	}
	if pump == "" {
		pump = DefaultPump
	}
	if ledger, ok := c.pumps[pump]; ok {
		if active, ok := ledger.leases[lease.Owner]; ok && active.ID == lease.ID {
			ledger.used -= lease.Capacity
			delete(ledger.leases, lease.Owner)
			delete(ledger.reported, lease.Owner)
		}
	}
}

// ReconcileLeases compares existing leases against each pump's current health-limited
// capacity. Old leases are never shrunk or evicted silently: every one that now exceeds
// the available capacity is returned as an Overcommit so the caller can fence or release it.
func (c *Coordinator) ReconcileLeases() []Overcommit {
	c.mu.Lock()
	defer c.mu.Unlock()
	var overcommitted []Overcommit
	pumps := make([]string, 0, len(c.pumps))
	for pump := range c.pumps {
		pumps = append(pumps, pump)
	}
	sort.Strings(pumps)
	for _, pump := range pumps {
		ledger := c.pumps[pump]
		available, reasons := c.availableCapacityLocked(pump)
		owners := make([]string, 0, len(ledger.leases))
		for owner := range ledger.leases {
			owners = append(owners, owner)
		}
		sort.Strings(owners)
		for _, owner := range owners {
			lease := ledger.leases[owner]
			if !lease.Active {
				continue
			}
			if lease.Capacity > available {
				if ledger.reported[owner] {
					// Already surfaced to the caller; do not raise duplicate alerts on every point.
					continue
				}
				ledger.reported[owner] = true
				overcommitted = append(overcommitted, Overcommit{
					Pump:      pump,
					Lease:     lease,
					Granted:   lease.Capacity,
					Available: available,
					Reasons:   append([]string(nil), reasons...),
				})
			} else {
				// Fits again (e.g. health recovered): re-arm so a future deration reports it anew.
				ledger.reported[owner] = false
			}
		}
	}
	return overcommitted
}

// PumpHealth reports the health assessment, available capacity and current leases for one pump.
func (c *Coordinator) PumpHealth(pump string) map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	health := c.health.Health(pump)
	available, _ := c.availableCapacityLocked(pump)
	ledger := c.pumps[pump]
	leases := []model.Lease{}
	used := 0.0
	if ledger != nil {
		used = ledger.used
		for _, lease := range ledger.leases {
			leases = append(leases, lease)
		}
	}
	sort.Slice(leases, func(i, j int) bool { return leases[i].Owner < leases[j].Owner })
	return map[string]any{
		"pump":               pump,
		"status":             health.Status,
		"rated_capacity":     c.capacity,
		"available_capacity": available,
		"used":               used,
		"factor":             health.AvailableFactor,
		"reasons":            health.Reasons,
		"healthy_streak":     health.HealthyStreak,
		"recovered":          health.Confirmed,
		"last_vibration":     health.LastVibration.Value,
		"vibration_at":       health.LastVibration.At.UnixNano(),
		"last_temperature":   health.LastTemperature.Celsius,
		"temperature_at":     health.LastTemperature.At.UnixNano(),
		"leases":             leases,
		"overcommitted":      used > available,
	}
}

// PumpHealthAll reports health for every pump that holds a lease or has been observed.
func (c *Coordinator) PumpHealthAll() []map[string]any {
	c.mu.Lock()
	seen := make(map[string]struct{})
	for pump := range c.pumps {
		seen[pump] = struct{}{}
	}
	for _, h := range c.health.Snapshot() {
		seen[h.Pump] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	c.mu.Unlock()
	sort.Strings(names)
	reports := make([]map[string]any, 0, len(names))
	for _, name := range names {
		reports = append(reports, c.PumpHealth(name))
	}
	return reports
}

// LeaseOn returns the active lease for an owner on a pump.
func (c *Coordinator) LeaseOn(pump, owner string) (model.Lease, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pump == "" {
		pump = DefaultPump
	}
	ledger, ok := c.pumps[pump]
	if !ok {
		return model.Lease{}, false
	}
	lease, ok := ledger.leases[owner]
	return lease, ok
}

// Used returns the capacity committed across all pumps.
func (c *Coordinator) Used() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	total := 0.0
	for _, ledger := range c.pumps {
		total += ledger.used
	}
	return total
}
