package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

type transitionRequest struct {
	Grade  string `json:"grade"`
	Source string `json:"source"`
}
type sampleRequest struct {
	Value float64 `json:"value"`
	At    int64   `json:"at"`
}
type routeRequest struct {
	Target string `json:"target"`
}
type leaseRequest struct {
	Owner  string  `json:"owner"`
	Pump   string  `json:"pump"`
	Amount float64 `json:"amount"`
}
type pumpHealthRequest struct {
	Pump        string   `json:"pump"`
	At          int64    `json:"at"`
	Vibration   *float64 `json:"vibration"`
	Temperature *float64 `json:"temperature"`
}
type releaseLeaseRequest struct {
	Owner string `json:"owner"`
	Pump  string `json:"pump"`
}

func decodeJSON(r *http.Request, value any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	return nil
}

func boolQuery(r *http.Request, key string) (bool, error) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, errors.New("invalid boolean query")
	}
	return value, nil
}

func methodNotAllowed(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
}
