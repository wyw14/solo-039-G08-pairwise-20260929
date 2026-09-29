package api

import (
	"net/http"

	"github.com/wyw14/solo-039/internal/reflux"
)

func (s *Server) pumpHealth(w http.ResponseWriter, r *http.Request) {
	var input reflux.PumpReading
	if err := decodeJSON(r, &input); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	result, err := s.plant.ObservePumpHealth(input)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}
