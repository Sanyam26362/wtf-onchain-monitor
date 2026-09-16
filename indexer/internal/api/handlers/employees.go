package handlers

import (
	"fmt"
	"net/http"

	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/api/validation"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/repository"
)

// EmployeeHandler retrieves employee profile, funding history, and claim history.
func EmployeeHandler(empRepo *repository.EmployeesRepository, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rawAddress := r.PathValue("address")

		addr, err := validation.ValidateAddress(rawAddress)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, err.Error())
			return
		}

		employee, err := empRepo.GetByAddress(ctx, cfg.ChainID, addr)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to query employee: "+err.Error())
			return
		}

		if employee == nil {
			responses.WriteError(w, http.StatusNotFound, responses.ErrCodeEmployeeNotFound, fmt.Sprintf("employee %s not found on chain %d", addr.Hex(), cfg.ChainID))
			return
		}

		responses.WriteSuccess(w, http.StatusOK, employee, nil)
	}
}
