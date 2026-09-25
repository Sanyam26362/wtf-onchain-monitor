package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/models"
	"worldtradefuture/indexer/internal/repository"
)

// SyncStatusHandler returns high-level indexing status and stream lags for monitoring dashboard.
func SyncStatusHandler(syncRepo *repository.SyncRepository, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		checkpoints, err := syncRepo.GetAllCheckpoints(ctx, cfg.ChainID)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to load sync checkpoints: "+err.Error())
			return
		}

		latestBlock, err := syncRepo.GetLatestChainBlock(ctx, cfg.ChainID)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to determine latest block: "+err.Error())
			return
		}

		var safeBlock uint64
		if latestBlock > cfg.ConfirmationDepth {
			safeBlock = latestBlock - cfg.ConfirmationDepth
		} else {
			safeBlock = latestBlock
		}

		streams := make([]*models.StreamStatus, 0, len(checkpoints))
		for _, cp := range checkpoints {
			streams = append(streams, repository.BuildStreamStatus(cp, latestBlock, cfg.TokenAddress))
		}

		statusResp := models.SyncStatusResponse{
			ChainID:     cfg.ChainID,
			LatestBlock: latestBlock,
			SafeBlock:   safeBlock,
			Streams:     streams,
			LastError:   nil,
		}

		responses.WriteSuccess(w, http.StatusOK, statusResp, nil)
	}
}

// BackfillRequest represents the incoming payload for POST /v1/sync/backfill.
type BackfillRequest struct {
	FromBlock uint64 `json:"from_block"`
	ToBlock   uint64 `json:"to_block"`
	StreamID  string `json:"stream_id"`
}

// SyncBackfillHandler validates and initiates historical block-range indexing on operator demand.
func SyncBackfillHandler(cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BackfillRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeBadRequest, "invalid JSON request body: "+err.Error())
			return
		}

		if req.FromBlock == 0 {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidBlockRange, "from_block must be greater than 0")
			return
		}

		if req.ToBlock < req.FromBlock {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidBlockRange, fmt.Sprintf("to_block (%d) must be greater than or equal to from_block (%d)", req.ToBlock, req.FromBlock))
			return
		}

		const maxRange = 50000
		if req.ToBlock-req.FromBlock > maxRange {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidBlockRange, fmt.Sprintf("requested range exceeds maximum allowed limit of %d blocks", maxRange))
			return
		}

		streamID := req.StreamID
		if streamID == "" {
			streamID = "all_streams"
		}

		_ = streamID
		responses.WriteError(
			w,
			http.StatusNotImplemented,
			responses.ErrCodeNotImplemented,
			"asynchronous backfill queue is not yet implemented; RPC backfill must be triggered via cmd/indexer --stream=escrow --from-block=X --to-block=Y",
		)
	}
}
