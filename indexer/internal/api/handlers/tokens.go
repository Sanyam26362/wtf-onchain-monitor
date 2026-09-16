package handlers

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/api/validation"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/repository"
)

// TokenTransfersHandler lists ERC-20 transfers for any token with directional and range filters.
func TokenTransfersHandler(tokensRepo *repository.TokensRepository, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rawToken := r.PathValue("address")

		tokenAddr, err := validation.ValidateAddress(rawToken)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, "invalid token address: "+err.Error())
			return
		}

		pag, err := validation.ParsePagination(r)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidPagination, err.Error())
			return
		}

		blocks, err := validation.ParseBlockRange(r)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidBlockRange, err.Error())
			return
		}

		dates, err := validation.ParseDateRange(r)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidDateRange, err.Error())
			return
		}

		var walletAddr *common.Address
		if val := r.URL.Query().Get("wallet"); val != "" {
			addr, err := validation.ValidateAddress(val)
			if err != nil {
				responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, "invalid wallet address: "+err.Error())
				return
			}
			walletAddr = &addr
		}

		direction := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("direction")))
		if direction == "" {
			direction = "all"
		} else if direction != "in" && direction != "out" && direction != "all" {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidDirection, fmt.Sprintf("invalid direction '%s': must be 'in', 'out', or 'all'", direction))
			return
		}

		sortCols := map[string]string{
			"block_number":    "block_number",
			"block_timestamp": "block_timestamp",
			"amount":          "amount",
		}
		sortParams, err := validation.ParseSort(r, sortCols, "block_number", "DESC")
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeBadRequest, err.Error())
			return
		}

		filter := repository.TokenTransferFilter{
			ChainID:       cfg.ChainID,
			Token:         tokenAddr,
			Wallet:        walletAddr,
			Direction:     direction,
			FromBlock:     blocks.FromBlock,
			ToBlock:       blocks.ToBlock,
			FromDate:      dates.FromDate,
			ToDate:        dates.ToDate,
			Limit:         pag.PageSize,
			Offset:        pag.Offset,
			SortBy:        sortParams.SortBy,
			SortDirection: sortParams.Direction,
		}

		transfers, total, err := tokensRepo.ListTransfers(ctx, filter)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to query token transfers: "+err.Error())
			return
		}

		responses.WriteList(w, transfers, pag.Page, pag.PageSize, total)
	}
}
