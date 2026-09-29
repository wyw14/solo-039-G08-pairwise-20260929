package reflux

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// PumpReading is one vibration or bearing-temperature observation for one pump.
type PumpReading struct {
	At          int64   `json:"at"`
	Pump        string  `json:"pump"`
	Vibration   float64 `json:"vibration"`
	Temperature float64 `json:"temperature"`
}

// HealthLevel rates a pump.
type HealthLevel string

const (
	HealthHealthy  HealthLevel = "healthy"
	HealthDegraded HealthLevel = "degraded"
	HealthCritical HealthLevel = "critical"
)

// PumpSnapshot is an immutable view of one pump's current verdict.
type PumpSnapshot struct {
	Pump          string    `json:"pump"`
	Level         string    `json:"level"`
	Vibration     float64   `json:"vibration"`
	Temperature   float64   `json:"temperature"`
	Factor        float64   `json:"capacity_factor"`
	Reasons       []string  `json:"reasons"`
	ObservedAt    int64     `json:"observed_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	HealthyStreak int       `json:"healthy_streak"`
}

// Default thresholds: typical mid-size centrifugal reflux-pump bands
// (vibration in mm/s RMS, bearing temperature in Celsius).
const (
	DefaultVibrationWarn     = 7.1
	DefaultVibrationCritical = 11.2
	DefaultBearingWarn       = 80
	DefaultBearingCritical   = 95
	DefaultRecoveryRequired  = 3
	DefaultDegradedFactor    = 0.6
	DefaultCriticalFactor    = 0
)

// HealthThresholds and capacity policy for the pump fleet.
type HealthThresholds struct {
	VibrationWarn     float64
	VibrationCritical float64
	BearingWarn       float64
	BearingCritical   float64
	// RecoveryRequired is the number of consecutive in-range observations
	// needed before a non-healthy pump is allowed back to full capacity.
	RecoveryRequired int
	// DegradedFactor / CriticalFactor scale rated capacity at each level.
	DegradedFactor float64
	CriticalFactor float64
}

func DefaultHealthThresholds() HealthThresholds {
	return HealthThresholds{
		VibrationWarn:     DefaultVibrationWarn,
		VibrationCritical: DefaultVibrationCritical,
		BearingWarn:       DefaultBearingWarn,
		BearingCritical:   DefaultBearingCritical,
		RecoveryRequired:  DefaultRecoveryRequired,
		DegradedFactor:    DefaultDegradedFactor,
		CriticalFactor:    DefaultCriticalFactor,
	}
}

type pumpState struct {
	level         HealthLevel
	vibration     float64
	temperature   float64
	factor        float64
	reasons       []string // derating cause while abnormal / recovering
	derateReasons []string // reasons from the most recent abnormal reading
	observedAt    int64
	updatedAt     time.Time
	recovering    bool // non-healthy and counting clean observations
	healthyStreak int
	hasReading    bool
}

// HealthMonitor tracks vibration and bearing-temperature readings per pump and
// turns them into a per-pump capacity factor. A pump that trips does not regain
// full capacity on a single good reading: it must post RecoveryRequired
// consecutive in-range observations.
type HealthMonitor struct {
	mu         sync.Mutex
	thresholds HealthThresholds
	pumps      map[string]*pumpState
}

func NewHealthMonitor(thresholds HealthThresholds) *HealthMonitor {
	if thresholds.RecoveryRequired < 1 {
		thresholds.RecoveryRequired = DefaultRecoveryRequired
	}
	return &HealthMonitor{thresholds: thresholds, pumps: make(map[string]*pumpState)}
}

// Observe records one reading and returns the pump's new status.
func (m *HealthMonitor) Observe(reading PumpReading) (PumpSnapshot, error) {
	if reading.Pump == "" {
		return PumpSnapshot{}, errors.New("pump reading requires a pump identifier")
	}
	if reading.At < 0 || reading.Vibration < 0 || reading.Temperature < 0 {
		return PumpSnapshot{}, errors.New("pump reading contains a negative value")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	pump := m.pumps[reading.Pump]
	if pump == nil {
		pump = &pumpState{}
		m.pumps[reading.Pump] = pump
	}

	rawLevel, rawReasons := m.classify(reading)

	switch {
	case rawLevel != HealthHealthy:
		// Any abnormal reading imposes (or changes) the derate and restarts
		// the recovery clock, even one arriving mid-recovery.
		pump.recovering = true
		pump.healthyStreak = 0
		pump.level = rawLevel
		pump.derateReasons = append([]string(nil), rawReasons...)
		pump.reasons = append([]string(nil), rawReasons...)
		if rawLevel == HealthCritical {
			pump.factor = m.thresholds.CriticalFactor
		} else {
			pump.factor = m.thresholds.DegradedFactor
		}
	case pump.recovering:
		// Reading is in range but the pump has not earned recovery yet.
		// Keep the previous derate in force until the streak completes so a
		// single good sample cannot silently reopen full capacity.
		pump.healthyStreak++
		if pump.healthyStreak >= m.thresholds.RecoveryRequired {
			pump.recovering = false
			pump.healthyStreak = 0
			pump.level = HealthHealthy
			pump.factor = 1
			pump.reasons = []string{"recovered after consecutive healthy observations"}
		} else {
			pump.reasons = append(append([]string(nil), pump.derateReasons...),
				"recovery pending: awaiting consecutive healthy observations")
		}
	default:
		// Continuously healthy: full capacity, no derating reason.
		pump.level = HealthHealthy
		pump.factor = 1
		pump.reasons = nil
	}

	pump.hasReading = true
	pump.vibration = reading.Vibration
	pump.temperature = reading.Temperature
	pump.observedAt = reading.At
	pump.updatedAt = time.Now()
	return m.snapshotLocked(reading.Pump), nil
}

func (m *HealthMonitor) classify(r PumpReading) (HealthLevel, []string) {
	t := m.thresholds
	level := HealthHealthy
	var reasons []string
	switch {
	case r.Vibration >= t.VibrationCritical:
		level = HealthCritical
		reasons = append(reasons, "vibration critical")
	case r.Vibration >= t.VibrationWarn:
		level = HealthDegraded
		reasons = append(reasons, "vibration high")
	}
	switch {
	case r.Temperature >= t.BearingCritical:
		level = HealthCritical
		reasons = append(reasons, "bearing temperature critical")
	case r.Temperature >= t.BearingWarn:
		if level == HealthHealthy {
			level = HealthDegraded
		}
		reasons = append(reasons, "bearing temperature high")
	}
	return level, reasons
}

// Pump returns the current verdict for one pump. Pumps with no readings yet are
// treated as healthy at full capacity.
func (m *HealthMonitor) Pump(name string) PumpSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked(name)
}

// Pumps returns all observed pumps ordered by identifier.
func (m *HealthMonitor) Pumps() []PumpSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.pumps))
	for name, pump := range m.pumps {
		if pump.hasReading {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make([]PumpSnapshot, 0, len(names))
	for _, name := range names {
		out = append(out, m.snapshotLocked(name))
	}
	return out
}

// FleetFactor is the capacity currently available as a fraction of rated
// capacity: the minimum factor across observed pumps, since the least healthy
// pump throttles the shared reflux capacity. With no readings yet, capacity is
// assumed fully available.
func (m *HealthMonitor) FleetFactor() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fleetFactorLocked()
}

// FleetReasons explains the current fleet derating using the binding (worst)
// pumps' active reasons.
func (m *HealthMonitor) FleetReasons() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	factor := m.fleetFactorLocked()
	names := make([]string, 0, len(m.pumps))
	for name, pump := range m.pumps {
		if pump.hasReading {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	var reasons []string
	for _, name := range names {
		pump := m.pumps[name]
		if pump.factor <= factor {
			for _, reason := range pump.reasons {
				reasons = append(reasons, name+": "+reason)
			}
		}
	}
	return reasons
}

func (m *HealthMonitor) fleetFactorLocked() float64 {
	factor := 1.0
	observed := false
	for _, pump := range m.pumps {
		if pump.hasReading {
			observed = true
			if pump.factor < factor {
				factor = pump.factor
			}
		}
	}
	if !observed {
		return 1
	}
	return factor
}

func (m *HealthMonitor) snapshotLocked(name string) PumpSnapshot {
	pump := m.pumps[name]
	if pump == nil || !pump.hasReading {
		return PumpSnapshot{Pump: name, Level: string(HealthHealthy), Factor: 1}
	}
	return PumpSnapshot{
		Pump:          name,
		Level:         string(pump.level),
		Vibration:     pump.vibration,
		Temperature:   pump.temperature,
		Factor:        pump.factor,
		Reasons:       append([]string(nil), pump.reasons...),
		ObservedAt:    pump.observedAt,
		UpdatedAt:     pump.updatedAt,
		HealthyStreak: pump.healthyStreak,
	}
}
