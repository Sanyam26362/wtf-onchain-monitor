package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/config"
)

// ReadyHandler verifies database connectivity and core configuration state.
func ReadyHandler(pool *pgxpool.Pool, cfg *config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if pool == nil {
			responses.WriteError(w, http.StatusServiceUnavailable, responses.ErrCodeServiceNotReady, "database connection pool is nil")
			return
		}

		if err := pool.Ping(ctx); err != nil {
			responses.WriteError(w, http.StatusServiceUnavailable, responses.ErrCodeServiceNotReady, "database ping failed: "+err.Error())
			return
		}

		if cfg == nil || cfg.RPCURL == "" {
			responses.WriteError(w, http.StatusServiceUnavailable, responses.ErrCodeServiceNotReady, "RPC URL is unconfigured")
			return
		}

		responses.WriteSuccess(w, http.StatusOK, map[string]any{
			"status":                 "ready",
			"database":               "connected",
			"chain_id":               cfg.ChainID,
			"environment":            cfg.DeploymentEnvironment,
			"payroll_contract":       cfg.PayrollContractAddress,
			"generic_token_contract": cfg.TokenAddress,
		}, nil)
	}
}
