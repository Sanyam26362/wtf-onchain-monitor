package handlers

import (
	"net/http"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/api/validation"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/repository"
)

// PayrollFundingsHandler lists historical payroll funding events with pagination and filtering.
func PayrollFundingsHandler(payrollRepo *repository.PayrollRepository, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

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

		var employerAddr *common.Address
		if val := r.URL.Query().Get("employer"); val != "" {
			addr, err := validation.ValidateAddress(val)
			if err != nil {
				responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, "invalid employer address: "+err.Error())
				return
			}
			employerAddr = &addr
		}

		var employeeAddr *common.Address
		if val := r.URL.Query().Get("employee"); val != "" {
			addr, err := validation.ValidateAddress(val)
			if err != nil {
				responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, "invalid employee address: "+err.Error())
				return
			}
			employeeAddr = &addr
		}

		sortCols := map[string]string{
			"block_number":    "block_number",
			"block_timestamp": "block_timestamp",
			"amount_paid":     "amount_paid",
			"amount_credited": "amount_credited",
		}
		sortParams, err := validation.ParseSort(r, sortCols, "block_number", "DESC")
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeBadRequest, err.Error())
			return
		}

		filter := repository.PayrollFundingFilter{
			ChainID:       cfg.ChainID,
			Employer:      employerAddr,
			Employee:      employeeAddr,
			FromBlock:     blocks.FromBlock,
			ToBlock:       blocks.ToBlock,
			FromDate:      dates.FromDate,
			ToDate:        dates.ToDate,
			Limit:         pag.PageSize,
			Offset:        pag.Offset,
			SortBy:        sortParams.SortBy,
			SortDirection: sortParams.Direction,
		}

		fundings, total, err := payrollRepo.ListFundings(ctx, filter)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to query payroll fundings: "+err.Error())
			return
		}

		responses.WriteList(w, fundings, pag.Page, pag.PageSize, total)
	}
}

// PayrollClaimsHandler lists historical salary claim events with pagination and filtering.
func PayrollClaimsHandler(payrollRepo *repository.PayrollRepository, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

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

		var employeeAddr *common.Address
		if val := r.URL.Query().Get("employee"); val != "" {
			addr, err := validation.ValidateAddress(val)
			if err != nil {
				responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, "invalid employee address: "+err.Error())
				return
			}
			employeeAddr = &addr
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

		filter := repository.SalaryClaimFilter{
			ChainID:       cfg.ChainID,
			Employee:      employeeAddr,
			FromBlock:     blocks.FromBlock,
			ToBlock:       blocks.ToBlock,
			FromDate:      dates.FromDate,
			ToDate:        dates.ToDate,
			Limit:         pag.PageSize,
			Offset:        pag.Offset,
			SortBy:        sortParams.SortBy,
			SortDirection: sortParams.Direction,
		}

		claims, total, err := payrollRepo.ListClaims(ctx, filter)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to query salary claims: "+err.Error())
			return
		}

		responses.WriteList(w, claims, pag.Page, pag.PageSize, total)
	}
}
