package handlers

import (
	"net/http"
	"time"

	"worldtradefuture/indexer/internal/api/responses"
)

// HealthHandler responds immediately for liveness checks without database queries.
func HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		responses.WriteSuccess(w, http.StatusOK, map[string]any{
			"status":    "ok",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		}, nil)
	}
}
