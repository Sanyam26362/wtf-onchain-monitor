package handlers

import (
	"context"
	"math/big"
	"net/http"
	"strings"

	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/models"
)

// EscrowQueryRepository defines the query interface required by EscrowEventsHandler.
type EscrowQueryRepository interface {
	GetEscrowEventsByEscrowID(ctx context.Context, escrowID string) ([]*models.EscrowEvent, error)
}

// EscrowEventsHandler retrieves all lifecycle events for a specific escrow contract ID.
func EscrowEventsHandler(repo EscrowQueryRepository, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idParam := strings.TrimSpace(r.PathValue("id"))
		if idParam == "" {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, "missing escrow id parameter")
			return
		}

		var escrowIDStr string
		if strings.HasPrefix(strings.ToLower(idParam), "0x") {
			bi := new(big.Int)
			bi, ok := bi.SetString(idParam[2:], 16)
			if !ok {
				responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, "invalid hex escrow id")
				return
			}
			escrowIDStr = bi.String()
		} else {
			bi := new(big.Int)
			bi, ok := bi.SetString(idParam, 10)
			if !ok {
				responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, "invalid decimal escrow id: "+idParam)
				return
			}
			escrowIDStr = bi.String()
		}

		events, err := repo.GetEscrowEventsByEscrowID(r.Context(), escrowIDStr)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to query escrow events: "+err.Error())
			return
		}

		if events == nil {
			events = []*models.EscrowEvent{}
		}

		responses.WriteSuccess(w, http.StatusOK, events, map[string]any{
			"total":     len(events),
			"escrow_id": escrowIDStr,
		})
	}
}
