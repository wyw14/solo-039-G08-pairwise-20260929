package reflux

import (
	"errors"
	"math"
	"testing"
)

func newTestCoordinator(t *testing.T, recoveryRequired int) (*Coordinator, *HealthMonitor) {
	t.Helper()
	thresholds := DefaultHealthThresholds()
	if recoveryRequired > 0 {
		thresholds.RecoveryRequired = recoveryRequired
	}
	monitor := NewHealthMonitor(thresholds)
	return NewCoordinatorWithHealth(1, monitor), monitor
}

func approxEqual(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func reading(at int64, pump string, vibration, temperature float64) PumpReading {
	return PumpReading{At: at, Pump: pump, Vibration: vibration, Temperature: temperature}
}

func TestHealthClassifiesVibrationAndBearing(t *testing.T) {
	monitor := NewHealthMonitor(DefaultHealthThresholds())

	healthy, err := monitor.Observe(reading(1, "P-1", 3, 50))
	if err != nil {
		t.Fatal(err)
	}
	if healthy.Level != "healthy" || healthy.Factor != 1 {
		t.Fatalf("expected healthy at full factor, got %s factor %.2f", healthy.Level, healthy.Factor)
	}

	degraded, _ := monitor.Observe(reading(2, "P-1", 8.0, 50))
	if degraded.Level != "degraded" || degraded.Factor != DefaultDegradedFactor {
		t.Fatalf("expected degraded at %.2f factor, got %s %.2f", DefaultDegradedFactor, degraded.Level, degraded.Factor)
	}
	if !containsReason(degraded.Reasons, "vibration high") {
		t.Fatalf("expected vibration reason, got %v", degraded.Reasons)
	}

	hot, _ := monitor.Observe(reading(3, "P-1", 3, 85))
	if hot.Level != "degraded" {
		t.Fatalf("expected degraded on bearing heat, got %s", hot.Level)
	}
	if !containsReason(hot.Reasons, "bearing temperature high") {
		t.Fatalf("expected bearing reason, got %v", hot.Reasons)
	}

	critical, _ := monitor.Observe(reading(4, "P-1", 12, 100))
	if critical.Level != "critical" || critical.Factor != DefaultCriticalFactor {
		t.Fatalf("expected critical at zero factor, got %s %.2f", critical.Level, critical.Factor)
	}
}

func TestHealthRecoveryRequiresConsecutiveObservations(t *testing.T) {
	thresholds := DefaultHealthThresholds()
	thresholds.RecoveryRequired = 3
	monitor := NewHealthMonitor(thresholds)

	if _, err := monitor.Observe(reading(1, "P-1", 9, 50)); err != nil {
		t.Fatal(err)
	}
	if monitor.FleetFactor() != DefaultDegradedFactor {
		t.Fatalf("expected derated fleet factor %.2f", DefaultDegradedFactor)
	}

	// One good observation must not restore capacity.
	first, _ := monitor.Observe(reading(2, "P-1", 3, 50))
	if first.Level != "degraded" || first.Factor != DefaultDegradedFactor {
		t.Fatalf("single good reading must not restore capacity, got %s %.2f", first.Level, first.Factor)
	}
	if first.HealthyStreak != 1 {
		t.Fatalf("expected healthy streak 1, got %d", first.HealthyStreak)
	}

	// A bad reading during recovery restarts the clock.
	if _, err := monitor.Observe(reading(3, "P-1", 9, 50)); err != nil {
		t.Fatal(err)
	}
	monitor.Observe(reading(4, "P-1", 3, 50))
	monitor.Observe(reading(5, "P-1", 3, 50))
	stillDegraded, _ := monitor.Observe(reading(6, "P-1", 3, 50))
	// streak after the bad reading: readings 4,5,6 are the three clean ones
	if stillDegraded.Level != "healthy" || stillDegraded.Factor != 1 {
		t.Fatalf("expected recovery after 3 consecutive clean readings, got %s %.2f streak %d",
			stillDegraded.Level, stillDegraded.Factor, stillDegraded.HealthyStreak)
	}
}

func TestHealthRejectsInvalidReadings(t *testing.T) {
	monitor := NewHealthMonitor(DefaultHealthThresholds())
	if _, err := monitor.Observe(reading(1, "", 3, 50)); err == nil {
		t.Fatal("missing pump id should be rejected")
	}
	if _, err := monitor.Observe(reading(1, "P-1", -1, 50)); err == nil {
		t.Fatal("negative vibration should be rejected")
	}
}

func TestFleetFactorIsBoundedByWorstPump(t *testing.T) {
	monitor := NewHealthMonitor(DefaultHealthThresholds())
	monitor.Observe(reading(1, "P-1", 3, 50))
	monitor.Observe(reading(1, "P-2", 9, 50))  // degraded
	monitor.Observe(reading(1, "P-3", 12, 50)) // critical

	if factor := monitor.FleetFactor(); factor != 0 {
		t.Fatalf("critical pump must bound fleet to zero, got %.2f", factor)
	}
	reasons := monitor.FleetReasons()
	if len(reasons) == 0 {
		t.Fatal("expected fleet derating reasons")
	}
}

func TestNewLeaseRejectedAgainstDeratedCapacity(t *testing.T) {
	coordinator, monitor := newTestCoordinator(t, 0)
	monitor.Observe(reading(1, "P-1", 9, 50)) // factor 0.6 -> 0.6 available

	if _, err := coordinator.Acquire("T-200", 0.7, 1); err == nil {
		t.Fatal("lease above derated capacity must be rejected")
	} else {
		var capacityErr *CapacityError
		if !errors.As(err, &capacityErr) {
			t.Fatalf("expected *CapacityError, got %T", err)
		}
		if !approxEqual(capacityErr.Available, 0.6) || !approxEqual(capacityErr.Factor, 0.6) {
			t.Fatalf("unexpected capacity figures: %+v", capacityErr)
		}
		if len(capacityErr.Reasons) == 0 {
			t.Fatal("capacity error must explain the derating reason")
		}
	}

	// A smaller request that fits the derated capacity is granted.
	lease, err := coordinator.Acquire("T-201", 0.4, 1)
	if err != nil {
		t.Fatalf("fitting lease should be granted: %v", err)
	}
	if lease.Capacity != 0.4 {
		t.Fatalf("lease capacity changed: %.2f", lease.Capacity)
	}

	// No silent oversubscription.
	if used := coordinator.Used(); used > 0.6+1e-9 {
		t.Fatalf("used capacity %.2f exceeds healthy capacity", used)
	}
	if _, err := coordinator.Acquire("T-202", 0.3, 1); err == nil {
		t.Fatal("request that would cross derated capacity must be rejected")
	}
}

func TestCriticalPumpBlocksAllNewLeases(t *testing.T) {
	coordinator, monitor := newTestCoordinator(t, 0)
	monitor.Observe(reading(1, "P-1", 12, 50))
	if _, err := coordinator.Acquire("T-200", 0.001, 1); err == nil {
		t.Fatal("critical pump (zero factor) must block every new lease")
	}
}

func TestOldLeasesOvercommittedButNotSilentlyTrimmed(t *testing.T) {
	coordinator, monitor := newTestCoordinator(t, 0)
	first, err := coordinator.Acquire("T-101", 0.5, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := coordinator.Acquire("T-102", 0.5, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Pump degrades to 60%: 1.0 is committed but only 0.6 is safe.
	monitor.Observe(reading(1, "P-1", 9, 50))
	report := coordinator.ReconcileHealth()
	if !report.Overcommitted || !approxEqual(report.TotalExcess, 0.4) {
		t.Fatalf("expected 0.4 excess, got over=%v excess=%.2f", report.Overcommitted, report.TotalExcess)
	}
	if len(report.Leases) != 1 {
		t.Fatalf("only the newest lease should be uncovered, got %d", len(report.Leases))
	}
	over := report.Leases[0]
	if over.Lease.ID != second.ID || !approxEqual(over.Covered, 0.1) || !approxEqual(over.Excess, 0.4) {
		t.Fatalf("unexpected overcommit entry: %+v", over)
	}

	// The original leases are untouched: no silent trimming.
	if coordinator.Used() != 1.0 {
		t.Fatalf("existing leases must remain at granted capacity, used=%.2f", coordinator.Used())
	}
	active := coordinator.ActiveLeases()
	if len(active) != 2 || active[0].ID != first.ID || active[1].ID != second.ID {
		t.Fatalf("lease set or order changed: %+v", active)
	}
}

func TestCriticalHealthFlagsAllOldLeases(t *testing.T) {
	coordinator, monitor := newTestCoordinator(t, 0)
	coordinator.Acquire("T-101", 0.4, 1)
	coordinator.Acquire("T-102", 0.4, 1)
	monitor.Observe(reading(1, "P-1", 12, 50))

	report := coordinator.ReconcileHealth()
	if report.Available != 0 || !report.Overcommitted || !approxEqual(report.TotalExcess, 0.8) {
		t.Fatalf("expected total 0.8 excess with zero available, got %+v", report)
	}
	if len(report.Leases) != 2 {
		t.Fatalf("both leases must be reported, got %d", len(report.Leases))
	}
}

func TestRecoveryRestoresCapacityAndClearsOvercommit(t *testing.T) {
	coordinator, monitor := newTestCoordinator(t, 2)
	coordinator.Acquire("T-101", 0.5, 1)
	monitor.Observe(reading(1, "P-1", 12, 50))
	if coordinator.ReconcileHealth().Overcommitted != true {
		t.Fatal("expected overcommit while critical")
	}

	monitor.Observe(reading(2, "P-1", 3, 50))
	if coordinator.ReconcileHealth().Overcommitted != true {
		t.Fatal("overcommit must persist until recovery is confirmed")
	}
	monitor.Observe(reading(3, "P-1", 3, 50))

	report := coordinator.ReconcileHealth()
	if report.Overcommitted || report.Factor != 1 || report.Available != 1 {
		t.Fatalf("expected full recovery, got %+v", report)
	}

	// New capacity is available again for fresh applications.
	if _, err := coordinator.Acquire("T-102", 0.5, 1); err != nil {
		t.Fatalf("new lease should succeed after confirmed recovery: %v", err)
	}
}

func TestAllocatorDefersAndRetriesRequestWithoutDropping(t *testing.T) {
	coordinator, monitor := newTestCoordinator(t, 2)
	allocator := NewAllocator(coordinator)

	if err := allocator.Submit(Request{Owner: "T-200", Capacity: 0.8, Fence: 1, Priority: 1}); err != nil {
		t.Fatal(err)
	}
	monitor.Observe(reading(1, "P-1", 9, 50)) // only 0.6 available

	if _, err := allocator.Next(); !errors.Is(err, ErrDeferred) {
		t.Fatalf("expected ErrDeferred, got %v", err)
	}
	pending := allocator.Pending()
	if len(pending) != 1 || pending[0].Owner != "T-200" {
		t.Fatalf("refused request must stay queued, got %+v", pending)
	}

	// Retrying immediately still fails and stays queued (no oversubscription).
	if _, err := allocator.Next(); !errors.Is(err, ErrDeferred) {
		t.Fatalf("retry while still derated should defer, got %v", err)
	}

	// Confirmed recovery frees capacity; the queued request is then granted.
	monitor.Observe(reading(2, "P-1", 3, 50))
	monitor.Observe(reading(3, "P-1", 3, 50))
	lease, err := allocator.Next()
	if err != nil {
		t.Fatalf("deferred request should succeed after recovery: %v", err)
	}
	if lease.Owner != "T-200" || lease.Capacity != 0.8 {
		t.Fatalf("unexpected lease: %+v", lease)
	}
	if len(allocator.Pending()) != 0 {
		t.Fatal("queue should be drained after grant")
	}
}

func containsReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
