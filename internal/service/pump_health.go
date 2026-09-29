package service

import (
	"fmt"
	"strings"
	"time"

	"github.com/wyw14/solo-039/internal/reflux"
)

// PumpHealthResult is returned after a pump reading is ingested. It combines
// the observed pump's verdict with the fleet-wide reconciliation against
// leases that were granted earlier.
type PumpHealthResult struct {
	Pump          reflux.PumpSnapshot      `json:"pump"`
	Pumps         []reflux.PumpSnapshot    `json:"pumps"`
	Rated         float64                  `json:"rated_capacity"`
	Available     float64                  `json:"available_capacity"`
	Factor        float64                  `json:"capacity_factor"`
	Reasons       []string                 `json:"derating_reasons"`
	Overcommitted bool                     `json:"overcommitted"`
	TotalExcess   float64                  `json:"total_excess"`
	Leases        []reflux.LeaseOvercommit `json:"overcommitted_leases"`
}

// ObservePumpHealth ingests one vibration / bearing-temperature reading,
// recomputes the pump's usable capacity, and reconciles leases issued before
// the reading arrived.
//
// New lease applications are rejected up-front by the coordinator against the
// derated capacity. Leases already held are NEVER trimmed silently: when they
// exceed the now-safe capacity the excess is reported per lease and an incident
// is opened; a confirmed recovery clears the overcommit and resolves it.
func (p *Plant) ObservePumpHealth(reading reflux.PumpReading) (PumpHealthResult, error) {
	if reading.At == 0 {
		reading.At = time.Now().Unix()
	}
	monitor := p.Reflux.Health()
	status, err := monitor.Observe(reading)
	if err != nil {
		return PumpHealthResult{}, err
	}
	report := p.Reflux.ReconcileHealth()

	result := PumpHealthResult{
		Pump:          status,
		Pumps:         monitor.Pumps(),
		Rated:         report.Rated,
		Available:     report.Available,
		Factor:        report.Factor,
		Reasons:       report.Reasons,
		Overcommitted: report.Overcommitted,
		TotalExcess:   report.TotalExcess,
		Leases:        report.Leases,
	}

	revision := p.Campaign.Snapshot().Revision.String()
	if report.Overcommitted {
		message := fmt.Sprintf("pump %s %s: existing leases exceed healthy capacity by %.3f (available %.3f of %.3f)",
			reading.Pump, status.Level, report.TotalExcess, report.Available, report.Rated)
		if len(report.Reasons) > 0 {
			message += ": " + strings.Join(report.Reasons, "; ")
		}
		p.openIncidentOnce("reflux-pump-overcommit", message, revision)
	} else {
		p.resolveIncidents("reflux-pump-overcommit")
	}

	payload := fmt.Sprintf("%s:%s:%.3f:%.3f:%.3f:%.3f:%t:%.3f",
		reading.Pump, status.Level, report.Factor, report.Available, reading.Vibration, reading.Temperature,
		report.Overcommitted, report.TotalExcess)
	if err := p.record("pump.health", payload); err != nil {
		p.fail(err)
		return result, err
	}
	return result, nil
}

// PumpHealth exposes the current fleet health without requiring a new reading.
func (p *Plant) PumpHealth() PumpHealthResult {
	monitor := p.Reflux.Health()
	report := p.Reflux.ReconcileHealth()
	return PumpHealthResult{
		Pumps:         monitor.Pumps(),
		Rated:         report.Rated,
		Available:     report.Available,
		Factor:        report.Factor,
		Reasons:       report.Reasons,
		Overcommitted: report.Overcommitted,
		TotalExcess:   report.TotalExcess,
		Leases:        report.Leases,
	}
}

func (p *Plant) openIncidentOnce(kind, message, revision string) {
	for _, incident := range p.Incidents.List(true) {
		if incident.Kind == kind {
			return
		}
	}
	p.Incidents.Open(kind, message, revision)
}

func (p *Plant) resolveIncidents(kind string) int {
	resolved := 0
	for _, incident := range p.Incidents.List(true) {
		if incident.Kind == kind && p.Incidents.Resolve(incident.ID) {
			resolved++
		}
	}
	return resolved
}
