package models

import (
	"encoding/json"
	"time"
)

// ReconciliationException represents an exception logged during reconciliation.
type ReconciliationException struct {
	ID         int64           `json:"id"`
	Type       string          `json:"type"`
	Severity   string          `json:"severity"`
	EntityRef  string          `json:"entity_ref"`
	Expected   json.RawMessage `json:"expected"`
	Observed   json.RawMessage `json:"observed"`
	Status     string          `json:"status"`
	DetectedAt time.Time       `json:"detected_at"`
	ResolvedAt *time.Time      `json:"resolved_at,omitempty"`
}
