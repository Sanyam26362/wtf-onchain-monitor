package handlers

import (
	"fmt"
	"net/http"

	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/api/validation"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/models"
	"worldtradefuture/indexer/internal/repository"
)

// TransactionHandler retrieves transaction details, confirmations, and decoded events.
func TransactionHandler(txRepo *repository.TransactionsRepository, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rawHash := r.PathValue("hash")

		txHash, err := validation.ValidateTxHash(rawHash)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidTransactionHash, err.Error())
			return
		}

		tx, events, err := txRepo.GetByHash(ctx, cfg.ChainID, txHash)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to query transaction: "+err.Error())
			return
		}

		if tx == nil {
			responses.WriteError(w, http.StatusNotFound, responses.ErrCodeTransactionNotFound, fmt.Sprintf("transaction %s not found on chain %d", txHash.Hex(), cfg.ChainID))
			return
		}

		// Compute dynamic confirmation depth
		confirmations := tx.Confirmations
		if tx.BlockNumber != nil {
			latestBlock, err := txRepo.GetLatestBlockNumber(ctx, cfg.ChainID)
			if err == nil && latestBlock >= *tx.BlockNumber {
				computed := latestBlock - *tx.BlockNumber + 1
				if computed > confirmations {
					confirmations = computed
				}
			}
		}

		var recipientStr *string
		if tx.Recipient != nil {
			rHex := tx.Recipient.Hex()
			recipientStr = &rHex
		}

		var gasUsedStr *string
		if tx.GasUsed != nil {
			gStr := fmt.Sprintf("%d", *tx.GasUsed)
			gasUsedStr = &gStr
		}

		if events == nil {
			events = make([]*models.TransactionEvent, 0)
		}

		resp := models.TransactionResponse{
			Hash:          tx.TxHash.Hex(),
			ChainID:       tx.ChainID,
			Sender:        tx.Sender.Hex(),
			Recipient:     recipientStr,
			Status:        tx.Status,
			BlockNumber:   tx.BlockNumber,
			Confirmations: confirmations,
			GasUsed:       gasUsedStr,
			Events:        events,
			ExplorerURL:   cfg.ExplorerTxURL(tx.TxHash.Hex()),
			ErrorData:     tx.ErrorData,
			FirstSeenAt:   tx.FirstSeenAt,
		}

		responses.WriteSuccess(w, http.StatusOK, resp, nil)
	}
}
