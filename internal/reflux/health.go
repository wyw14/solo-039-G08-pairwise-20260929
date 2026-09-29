package reflux

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// HealthStatus describes how a pump's condition maps to usable capacity.
type HealthStatus string

const (
	StatusHealthy   HealthStatus = "healthy"
	StatusDerated   HealthStatus = "derated"
	StatusUnhealthy HealthStatus = "unhealthy"
)

// VibrationReading is a vibration telemetry point tagged with the pump and sample time.
type VibrationReading struct {
	Pump string
	At   time.Time
	// RMS vibration velocity in mm/s.
	Value float64
}

// TemperatureReading is a bearing temperature telemetry point tagged with the pump and sample time.
type TemperatureReading struct {
	Pump string
	At   time.Time
	// Bearing temperature in degrees Celsius.
	Celsius float64
}

// Health captures the current usable fraction of a pump's rated capacity and why.
type Health struct {
	Pump   string
	Status HealthStatus
	// AvailableFactor is the usable fraction of rated capacity in the range [0, 1].
	AvailableFactor float64
	Reasons         []string
	// LastVibration and LastTemperature are the most recent accepted points (zero time if never seen).
	LastVibration   VibrationReading
	LastTemperature TemperatureReading
	// HealthyStreak counts the consecutive in-limits observations since the last breach.
	// Recovery is only confirmed once the streak reaches the configured threshold.
	HealthyStreak int
	// Confirmed reports whether a recovery streak has been completed.
	Confirmed bool
}

// Limits are the pump health thresholds (ISO-style vibration zones, bearing temperature).
type Limits struct {
	VibrationWarn  float64
	VibrationAlarm float64
	BearingWarn    float64
	BearingAlarm   float64
	// DerateFactor is the usable fraction when only warning thresholds are crossed.
	DerateFactor float64
	// HealthyConfirmations is the number of consecutive in-limits observations required
	// before a derated/unhealthy pump is considered recovered.
	HealthyConfirmations int
}

// DefaultLimits matches common centrifugal-pump guidance: vibration velocity in mm/s RMS
// and bearing temperature in Celsius.
func DefaultLimits() Limits {
	return Limits{
		VibrationWarn:        4.5,
		VibrationAlarm:       7.1,
		BearingWarn:          75,
		BearingAlarm:         90,
		DerateFactor:         0.5,
		HealthyConfirmations: 3,
	}
}

// HealthMonitor tracks per-pump vibration and bearing temperature observations and
// derives the capacity each pump can currently offer.
type HealthMonitor struct {
	mu     sync.Mutex
	limits Limits
	pumps  map[string]*pumpHealth
}

type pumpHealth struct {
	status          HealthStatus
	factor          float64
	reasons         []string
	breachReasons   []string
	lastVibration   VibrationReading
	lastTemperature TemperatureReading
	healthyStreak   int
	confirmed       bool
}

func NewHealthMonitor(limits Limits) *HealthMonitor {
	return &HealthMonitor{limits: limits, pumps: make(map[string]*pumpHealth)}
}

// RecordVibration ingests one vibration point and re-evaluates pump health.
func (m *HealthMonitor) RecordVibration(reading VibrationReading) error {
	if reading.Pump == "" {
		return errors.New("vibration reading missing pump id")
	}
	if reading.Value < 0 {
		return errors.New("vibration reading must be non-negative")
	}
	if reading.At.IsZero() {
		reading.At = time.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pumpFor(reading.Pump)
	p.lastVibration = reading
	m.reevaluate(p)
	return nil
}

// RecordTemperature ingests one bearing temperature point and re-evaluates pump health.
func (m *HealthMonitor) RecordTemperature(reading TemperatureReading) error {
	if reading.Pump == "" {
		return errors.New("temperature reading missing pump id")
	}
	if reading.At.IsZero() {
		reading.At = time.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pumpFor(reading.Pump)
	p.lastTemperature = reading
	m.reevaluate(p)
	return nil
}

// Record ingests a vibration and a bearing temperature point together for the same pump
// and re-evaluates health once. Either point may be zero-valued to indicate it is absent.
func (m *HealthMonitor) Record(pump string, at time.Time, vibration, celsius float64, hasVibration, hasTemperature bool) error {
	if pump == "" {
		return errors.New("health reading missing pump id")
	}
	if at.IsZero() {
		at = time.Now()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.pumpFor(pump)
	if hasVibration {
		if vibration < 0 {
			return errors.New("vibration reading must be non-negative")
		}
		p.lastVibration = VibrationReading{Pump: pump, At: at, Value: vibration}
	}
	if hasTemperature {
		p.lastTemperature = TemperatureReading{Pump: pump, At: at, Celsius: celsius}
	}
	m.reevaluate(p)
	return nil
}

func (m *HealthMonitor) pumpFor(pump string) *pumpHealth {
	p, ok := m.pumps[pump]
	if !ok {
		p = &pumpHealth{status: StatusHealthy, factor: 1, confirmed: true}
		m.pumps[pump] = p
	}
	return p
}

// reevaluate derives status from the latest points. Degradation applies immediately;
// recovery to full capacity only after a run of consecutive in-limits observations.
// Caller must hold m.mu.
func (m *HealthMonitor) reevaluate(p *pumpHealth) {
	l := m.limits
	reasons := make([]string, 0, 2)
	breached := false
	alarm := false

	if v := p.lastVibration; !v.At.IsZero() {
		switch {
		case v.Value >= l.VibrationAlarm:
			reasons = append(reasons, "vibration alarm exceeded")
			breached = true
			alarm = true
		case v.Value >= l.VibrationWarn:
			reasons = append(reasons, "vibration above warning threshold")
			breached = true
		}
	}
	if t := p.lastTemperature; !t.At.IsZero() {
		switch {
		case t.Celsius >= l.BearingAlarm:
			reasons = append(reasons, "bearing temperature alarm exceeded")
			breached = true
			alarm = true
		case t.Celsius >= l.BearingWarn:
			reasons = append(reasons, "bearing temperature above warning threshold")
			breached = true
		}
	}

	if breached {
		// Any abnormal reading breaks the recovery streak immediately.
		p.healthyStreak = 0
		p.confirmed = false
		p.breachReasons = append([]string(nil), reasons...)
		p.reasons = reasons
		if alarm {
			p.status = StatusUnhealthy
			p.factor = 0
		} else {
			p.status = StatusDerated
			p.factor = l.DerateFactor
		}
		return
	}

	// In-limits observation: health is restored only after a sustained run of clean points,
	// so a single good sample cannot mask an ongoing fault.
	if p.status != StatusHealthy || !p.confirmed {
		p.healthyStreak++
		if p.healthyStreak >= l.HealthyConfirmations {
			p.status = StatusHealthy
			p.factor = 1
			p.reasons = nil
			p.breachReasons = nil
			p.confirmed = true
		} else {
			// Keep the deration in force; explain both the original cause and the pending confirmation.
			p.reasons = append(append([]string(nil), p.breachReasons...),
				"recovery pending: awaiting consecutive healthy observations")
		}
	}
}

// AvailableFactor returns the usable capacity fraction [0, 1] for a pump.
// Unknown pumps default to fully available until evidence says otherwise.
func (m *HealthMonitor) AvailableFactor(pump string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.pumps[pump]; ok {
		return p.factor
	}
	return 1
}

// Health returns a copy of the current health assessment for a pump.
func (m *HealthMonitor) Health(pump string) Health {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.pumps[pump]; ok {
		return Health{
			Pump:            pump,
			Status:          p.status,
			AvailableFactor: p.factor,
			Reasons:         append([]string(nil), p.reasons...),
			LastVibration:   p.lastVibration,
			LastTemperature: p.lastTemperature,
			HealthyStreak:   p.healthyStreak,
			Confirmed:       p.confirmed,
		}
	}
	return Health{Pump: pump, Status: StatusHealthy, AvailableFactor: 1, Confirmed: true}
}

// Snapshot returns health assessments ordered by pump id.
func (m *HealthMonitor) Snapshot() []Health {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.pumps))
	for name := range m.pumps {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]Health, 0, len(names))
	for _, name := range names {
		p := m.pumps[name]
		out = append(out, Health{
			Pump:            name,
			Status:          p.status,
			AvailableFactor: p.factor,
			Reasons:         append([]string(nil), p.reasons...),
			LastVibration:   p.lastVibration,
			LastTemperature: p.lastTemperature,
			HealthyStreak:   p.healthyStreak,
			Confirmed:       p.confirmed,
		})
	}
	return out
}
