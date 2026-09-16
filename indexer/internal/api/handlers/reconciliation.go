package handlers

import (
	"fmt"
	"net/http"
	"strings"

	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/api/validation"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/repository"
)

// ReconciliationExceptionsHandler lists reconciliation exceptions with status, severity, and date filters.
func ReconciliationExceptionsHandler(recRepo *repository.ReconciliationRepository, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		pag, err := validation.ParsePagination(r)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidPagination, err.Error())
			return
		}

		dates, err := validation.ParseDateRange(r)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidDateRange, err.Error())
			return
		}

		var severity *string
		if val := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("severity"))); val != "" {
			switch val {
			case "low", "medium", "high", "critical":
				severity = &val
			default:
				responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeBadRequest, fmt.Sprintf("invalid severity '%s': must be low, medium, high, or critical", val))
				return
			}
		}

		var status *string
		if val := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("status"))); val != "" {
			switch val {
			case "open", "resolved":
				status = &val
			default:
				responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeBadRequest, fmt.Sprintf("invalid status '%s': must be open or resolved", val))
				return
			}
		}

		var excType *string
		if val := strings.TrimSpace(r.URL.Query().Get("type")); val != "" {
			excType = &val
		}

		sortCols := map[string]string{
			"detected_at": "detected_at",
			"severity":    "severity",
			"status":      "status",
		}
		sortParams, err := validation.ParseSort(r, sortCols, "detected_at", "DESC")
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeBadRequest, err.Error())
			return
		}

		filter := repository.ReconciliationFilter{
			Severity:      severity,
			Status:        status,
			Type:          excType,
			FromDate:      dates.FromDate,
			ToDate:        dates.ToDate,
			Limit:         pag.PageSize,
			Offset:        pag.Offset,
			SortBy:        sortParams.SortBy,
			SortDirection: sortParams.Direction,
		}

		exceptions, total, err := recRepo.ListExceptions(ctx, filter)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to query reconciliation exceptions: "+err.Error())
			return
		}

		responses.WriteList(w, exceptions, pag.Page, pag.PageSize, total)
	}
}
