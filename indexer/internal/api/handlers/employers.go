package handlers

import (
	"fmt"
	"net/http"

	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/api/validation"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/repository"
)

// EmployerHandler retrieves employer profile and list of associated employees.
func EmployerHandler(empRepo *repository.EmployersRepository, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		rawAddress := r.PathValue("address")

		addr, err := validation.ValidateAddress(rawAddress)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, responses.ErrCodeInvalidAddress, err.Error())
			return
		}

		employer, err := empRepo.GetByAddress(ctx, cfg.ChainID, addr)
		if err != nil {
			responses.WriteError(w, http.StatusInternalServerError, responses.ErrCodeInternalError, "failed to query employer: "+err.Error())
			return
		}

		if employer == nil {
			responses.WriteError(w, http.StatusNotFound, responses.ErrCodeEmployerNotFound, fmt.Sprintf("employer %s not found on chain %d", addr.Hex(), cfg.ChainID))
			return
		}

		responses.WriteSuccess(w, http.StatusOK, employer, nil)
	}
}
