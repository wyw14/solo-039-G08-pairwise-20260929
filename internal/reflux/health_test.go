package reflux

import (
	"testing"
	"time"
)

func healthyPoint(monitor *HealthMonitor, pump string, at time.Time) {
	if err := monitor.Record(pump, at, 2.0, 50, true, true); err != nil {
		panic(err)
	}
}

func TestNewApplicationsRejectedWhenDerated(t *testing.T) {
	coordinator := NewCoordinator(1)
	monitor := coordinator.Health()
	at := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)

	// Warning-level vibration derates the pump to half capacity.
	if err := monitor.RecordVibration(VibrationReading{Pump: DefaultPump, At: at, Value: 5.0}); err != nil {
		t.Fatal(err)
	}
	health := monitor.Health(DefaultPump)
	if health.Status != StatusDerated || health.AvailableFactor != 0.5 {
		t.Fatalf("expected derated 0.5, got %s %f (%v)", health.Status, health.AvailableFactor, health.Reasons)
	}
	if len(health.Reasons) == 0 {
		t.Fatal("deration must explain the cause")
	}

	// A new full-size application must not silently overcommit.
	if _, err := coordinator.Acquire("T-201", 0.8, 1); err == nil {
		t.Fatal("full-capacity lease granted on a derated pump")
	}
	// A request that fits the remaining health-limited capacity is allowed.
	lease, err := coordinator.Acquire("T-202", 0.4, 1)
	if err != nil {
		t.Fatalf("within-deration lease rejected: %v", err)
	}
	if coordinator.Used() > 0.5 {
		t.Fatalf("used %.2f exceeds derated capacity", coordinator.Used())
	}
	coordinator.Release(lease)
}

func TestAlarmStopsNewAssignments(t *testing.T) {
	coordinator := NewCoordinator(1)
	at := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	if err := coordinator.Health().RecordTemperature(TemperatureReading{Pump: DefaultPump, At: at, Celsius: 95}); err != nil {
		t.Fatal(err)
	}
	health := coordinator.Health().Health(DefaultPump)
	if health.Status != StatusUnhealthy || health.AvailableFactor != 0 {
		t.Fatalf("expected unhealthy/0, got %s/%f", health.Status, health.AvailableFactor)
	}
	if _, err := coordinator.Acquire("T-201", 0.1, 1); err == nil {
		t.Fatal("lease granted while bearing temperature in alarm")
	}
}

func TestOldLeaseReportedNotSilentlyEvicted(t *testing.T) {
	coordinator := NewCoordinator(1)
	lease, err := coordinator.Acquire("T-101", 0.9, 1)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	if err := coordinator.Health().RecordVibration(VibrationReading{Pump: DefaultPump, At: at, Value: 6.0}); err != nil {
		t.Fatal(err)
	}

	// The old lease is still booked; nothing is resized behind the owner's back.
	if coordinator.Used() != 0.9 {
		t.Fatalf("old lease altered silently: used %.2f", coordinator.Used())
	}
	overcommitted := coordinator.ReconcileLeases()
	if len(overcommitted) != 1 {
		t.Fatalf("expected 1 overcommit, got %d", len(overcommitted))
	}
	over := overcommitted[0]
	if over.Lease.ID != lease.ID || over.Available != 0.5 {
		t.Fatalf("unexpected overcommit report: %+v", over)
	}
	if len(over.Reasons) == 0 {
		t.Fatal("overcommit must state the deration reason")
	}

	// Meanwhile, no new lease can deepen the overcommit.
	if _, err := coordinator.Acquire("T-202", 0.1, 1); err == nil {
		t.Fatal("new lease deepened the overcommit")
	}

	// The old lease is resolved only by explicit release.
	coordinator.Release(lease)
	if coordinator.Used() != 0 {
		t.Fatalf("used %.2f after explicit release", coordinator.Used())
	}
	if len(coordinator.ReconcileLeases()) != 0 {
		t.Fatal("overcommit should clear after release")
	}
}

func TestRecoveryRequiresConsecutiveHealthyObservations(t *testing.T) {
	monitor := NewHealthMonitor(DefaultLimits())
	pump := DefaultPump
	at := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)

	if err := monitor.Record(pump, at, 6.0, 80, true, true); err != nil {
		t.Fatal(err)
	}
	if monitor.Health(pump).Status != StatusDerated {
		t.Fatal("expected derated after warning readings")
	}

	// Two clean observations are not enough; deration stays in force.
	healthyPoint(monitor, pump, at.Add(time.Minute))
	healthyPoint(monitor, pump, at.Add(2*time.Minute))
	if h := monitor.Health(pump); h.Status != StatusDerated || h.Confirmed {
		t.Fatalf("recovery confirmed too early: %+v", h)
	}

	// A single fresh abnormal reading resets the streak.
	if err := monitor.Record(pump, at.Add(3*time.Minute), 6.0, 50, true, true); err != nil {
		t.Fatal(err)
	}
	if h := monitor.Health(pump); h.HealthyStreak != 0 {
		t.Fatalf("streak not reset: %d", h.HealthyStreak)
	}

	// Only a sustained run of clean points restores full capacity.
	healthyPoint(monitor, pump, at.Add(4*time.Minute))
	healthyPoint(monitor, pump, at.Add(5*time.Minute))
	healthyPoint(monitor, pump, at.Add(6*time.Minute))
	h := monitor.Health(pump)
	if h.Status != StatusHealthy || h.AvailableFactor != 1 || !h.Confirmed {
		t.Fatalf("recovery not confirmed after consecutive observations: %+v", h)
	}

	// After recovery the full capacity is assignable again.
	coordinator := NewCoordinator(1).WithHealthMonitor(monitor)
	if _, err := coordinator.Acquire("T-301", 0.9, 1); err != nil {
		t.Fatalf("full capacity unavailable after confirmed recovery: %v", err)
	}
}

func TestReconcileReportsOvercommitOnceAndRearms(t *testing.T) {
	coordinator := NewCoordinator(1)
	monitor := coordinator.Health()
	if _, err := coordinator.Acquire("T-101", 0.9, 1); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	breach := func(offset time.Duration) {
		if err := monitor.RecordVibration(VibrationReading{Pump: DefaultPump, At: at.Add(offset), Value: 6}); err != nil {
			t.Fatal(err)
		}
	}
	breach(0)
	if len(coordinator.ReconcileLeases()) != 1 {
		t.Fatal("first reconciliation must report the overcommit")
	}
	breach(time.Minute)
	if reports := coordinator.ReconcileLeases(); len(reports) != 0 {
		t.Fatalf("repeated points re-reported the same lease: %d", len(reports))
	}
	// Confirmed recovery makes the lease fit; alerting re-arms for a later degradation.
	healthyPoint(monitor, DefaultPump, at.Add(2*time.Minute))
	healthyPoint(monitor, DefaultPump, at.Add(3*time.Minute))
	healthyPoint(monitor, DefaultPump, at.Add(4*time.Minute))
	if reports := coordinator.ReconcileLeases(); len(reports) != 0 {
		t.Fatalf("healthy pump should not report overcommit: %d", len(reports))
	}
	breach(5 * time.Minute)
	if reports := coordinator.ReconcileLeases(); len(reports) != 1 {
		t.Fatalf("re-deration after recovery should report again, got %d", len(reports))
	}
}

func TestUnknownPumpDefaultsHealthy(t *testing.T) {
	monitor := NewHealthMonitor(DefaultLimits())
	h := monitor.Health("P-999")
	if h.Status != StatusHealthy || h.AvailableFactor != 1 {
		t.Fatalf("unknown pump should be healthy, got %+v", h)
	}
}

func TestCombinedPointCountsAsOneObservation(t *testing.T) {
	monitor := NewHealthMonitor(DefaultLimits())
	pump := DefaultPump
	at := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	if err := monitor.Record(pump, at, 6.0, 80, true, true); err != nil {
		t.Fatal(err)
	}
	// Each time-stamped point carrying both channels advances the streak by exactly one.
	healthyPoint(monitor, pump, at.Add(time.Minute))
	if streak := monitor.Health(pump).HealthyStreak; streak != 1 {
		t.Fatalf("combined healthy point counted %d times, want 1", streak)
	}
	healthyPoint(monitor, pump, at.Add(2*time.Minute))
	healthyPoint(monitor, pump, at.Add(3*time.Minute))
	if h := monitor.Health(pump); h.Status != StatusHealthy {
		t.Fatalf("expected recovery after exactly 3 combined points, got streak %d", h.HealthyStreak)
	}
}

func TestInvalidReadingsRejected(t *testing.T) {
	monitor := NewHealthMonitor(DefaultLimits())
	at := time.Now()
	if err := monitor.RecordVibration(VibrationReading{At: at, Value: 1}); err == nil {
		t.Fatal("vibration without pump id accepted")
	}
	if err := monitor.RecordTemperature(TemperatureReading{At: at, Celsius: 1}); err == nil {
		t.Fatal("temperature without pump id accepted")
	}
	if err := monitor.RecordVibration(VibrationReading{Pump: "P", At: at, Value: -1}); err == nil {
		t.Fatal("negative vibration accepted")
	}
}

func TestPerPumpCapacityIsolation(t *testing.T) {
	coordinator := NewCoordinator(1)
	at := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	// P-A is derated, P-B is untouched.
	if err := coordinator.Health().RecordVibration(VibrationReading{Pump: "P-A", At: at, Value: 6}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.AcquireOn("P-A", "T-1", 0.9, 1); err == nil {
		t.Fatal("derated pump accepted full lease")
	}
	if _, err := coordinator.AcquireOn("P-B", "T-2", 0.9, 1); err != nil {
		t.Fatalf("healthy pump rejected: %v", err)
	}
}
