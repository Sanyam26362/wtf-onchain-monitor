package api

import (
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"worldtradefuture/indexer/internal/api/handlers"
	"worldtradefuture/indexer/internal/api/middleware"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/repository"
)

// NewRouter sets up all endpoints and middleware for the REST API.
func NewRouter(pool *pgxpool.Pool, cfg *config.Config, logger *slog.Logger, redisClient ...*redis.Client) http.Handler {
	mux := http.NewServeMux()

	var rdb *redis.Client
	if len(redisClient) > 0 {
		rdb = redisClient[0]
	}

	// Initialize repositories
	txRepo := repository.NewTransactionsRepository(pool)
	empRepo := repository.NewEmployersRepository(pool)
	employeeRepo := repository.NewEmployeesRepository(pool)
	payrollRepo := repository.NewPayrollRepository(pool)
	tokensRepo := repository.NewTokensRepository(pool)
	syncRepo := repository.NewSyncRepository(pool)
	recRepo := repository.NewReconciliationRepository(pool)
	chainEventsRepo := repository.NewChainEventsRepository(pool)

	// Webhook handler (Alchemy Notify)
	webhookHandler := handlers.NewWebhookHandler(cfg, chainEventsRepo, rdb)
	mux.HandleFunc("POST /api/indexer/webhook", webhookHandler.HandleAlchemyWebhook)

	// System & Health endpoints
	mux.HandleFunc("GET /health", handlers.HealthHandler())
	mux.HandleFunc("GET /ready", handlers.ReadyHandler(pool, cfg))

	// Sync status & Operator endpoints
	mux.HandleFunc("GET /v1/sync/status", handlers.SyncStatusHandler(syncRepo, cfg))
	mux.Handle("POST /v1/sync/backfill", middleware.RequireOperatorAuth(cfg.OperatorAPIKey)(handlers.SyncBackfillHandler(cfg)))

	// Core domain endpoints
	mux.HandleFunc("GET /v1/transactions/{hash}", handlers.TransactionHandler(txRepo, cfg))
	mux.HandleFunc("GET /v1/employers/{address}", handlers.EmployerHandler(empRepo, cfg))
	mux.HandleFunc("GET /v1/employees/{address}", handlers.EmployeeHandler(employeeRepo, cfg))
	mux.HandleFunc("GET /v1/payroll/fundings", handlers.PayrollFundingsHandler(payrollRepo, cfg))
	mux.HandleFunc("GET /v1/payroll/claims", handlers.PayrollClaimsHandler(payrollRepo, cfg))
	mux.HandleFunc("GET /v1/tokens/{address}/transfers", handlers.TokenTransfersHandler(tokensRepo, cfg))
	mux.HandleFunc("GET /v1/reconciliation/exceptions", handlers.ReconciliationExceptionsHandler(recRepo, cfg))

	// Global middleware chain: RequestID -> CORS -> Logger -> Recoverer -> ServeMux
	var handler http.Handler = mux
	handler = middleware.Recoverer(logger)(handler)
	handler = middleware.Logger(logger)(handler)
	handler = middleware.CORS(cfg.CORSAllowedOrigins)(handler)
	handler = middleware.RequestID(handler)

	return handler
}
