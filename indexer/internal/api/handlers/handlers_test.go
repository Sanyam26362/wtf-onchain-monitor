package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"worldtradefuture/indexer/internal/api/handlers"
	"worldtradefuture/indexer/internal/api/responses"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/persistence"
	"worldtradefuture/indexer/internal/repository"
)

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	handler := handlers.HealthHandler()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got: %d", rec.Code)
	}

	var env responses.SuccessEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("failed to parse json response: %v", err)
	}

	dataMap, ok := env.Data.(map[string]any)
	if !ok || dataMap["status"] != "ok" {
		t.Fatalf("expected status 'ok', got: %v", dataMap["status"])
	}
	if dataMap["timestamp"] == nil {
		t.Fatal("expected non-nil timestamp in health response")
	}
}

func TestReadyHandler_NotReady(t *testing.T) {
	t.Run("nil db pool", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rec := httptest.NewRecorder()

		handler := handlers.ReadyHandler(nil, &config.Config{RPCURL: "http://localhost:8545"})
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable, got: %d", rec.Code)
		}

		var errEnv responses.ErrorEnvelope
		if err := json.Unmarshal(rec.Body.Bytes(), &errEnv); err != nil {
			t.Fatalf("failed to parse json response: %v", err)
		}
		if errEnv.Error.Code != responses.ErrCodeServiceNotReady {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeServiceNotReady, errEnv.Error.Code)
		}
	})

	t.Run("missing RPC URL", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rec := httptest.NewRecorder()

		// Note: passing nil pool also triggers 503, but let's test missing RPC config
		handler := handlers.ReadyHandler(nil, &config.Config{RPCURL: ""})
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 Service Unavailable, got: %d", rec.Code)
		}
	})
}

func TestSyncBackfillHandler(t *testing.T) {
	cfg := &config.Config{
		ChainID: 11155111,
	}
	handler := handlers.SyncBackfillHandler(cfg)

	t.Run("invalid json body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/sync/backfill", bytes.NewBufferString("not-json"))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
	})

	t.Run("from_block is zero", func(t *testing.T) {
		payload := `{"from_block": 0, "to_block": 100}`
		req := httptest.NewRequest(http.MethodPost, "/v1/sync/backfill", bytes.NewBufferString(payload))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeInvalidBlockRange {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeInvalidBlockRange, errEnv.Error.Code)
		}
	})

	t.Run("to_block less than from_block", func(t *testing.T) {
		payload := `{"from_block": 500, "to_block": 400}`
		req := httptest.NewRequest(http.MethodPost, "/v1/sync/backfill", bytes.NewBufferString(payload))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
	})

	t.Run("range exceeds maximum limit", func(t *testing.T) {
		payload := `{"from_block": 1000, "to_block": 100000}`
		req := httptest.NewRequest(http.MethodPost, "/v1/sync/backfill", bytes.NewBufferString(payload))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
	})

	t.Run("valid backfill request", func(t *testing.T) {
		payload := `{"from_block": 11714400, "to_block": 11714450, "stream_id": "erc20_transfers"}`
		req := httptest.NewRequest(http.MethodPost, "/v1/sync/backfill", bytes.NewBufferString(payload))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("expected 202 Accepted, got: %d", rec.Code)
		}

		var env responses.SuccessEnvelope
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		data := env.Data.(map[string]any)
		if data["status"] != "accepted" {
			t.Fatalf("expected accepted status, got: %v", data["status"])
		}
	})
}

func TestValidationErrorsInHandlers(t *testing.T) {
	cfg := &config.Config{
		ChainID: 11155111,
	}

	t.Run("TransactionHandler invalid hash", func(t *testing.T) {
		h := handlers.TransactionHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/transactions/not-a-hash", nil)
		req.SetPathValue("hash", "not-a-hash")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeInvalidTransactionHash {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeInvalidTransactionHash, errEnv.Error.Code)
		}
	})

	t.Run("EmployerHandler invalid address", func(t *testing.T) {
		h := handlers.EmployerHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/employers/invalid-address", nil)
		req.SetPathValue("address", "invalid-address")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeInvalidAddress {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeInvalidAddress, errEnv.Error.Code)
		}
	})

	t.Run("EmployeeHandler invalid address", func(t *testing.T) {
		h := handlers.EmployeeHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/employees/invalid-address", nil)
		req.SetPathValue("address", "invalid-address")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
	})

	t.Run("TokenTransfersHandler invalid token address", func(t *testing.T) {
		h := handlers.TokenTransfersHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/tokens/invalid-token/transfers", nil)
		req.SetPathValue("address", "invalid-token")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
	})

	t.Run("TokenTransfersHandler invalid direction", func(t *testing.T) {
		h := handlers.TokenTransfersHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/tokens/0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238/transfers?direction=diagonal", nil)
		req.SetPathValue("address", "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeInvalidDirection {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeInvalidDirection, errEnv.Error.Code)
		}
	})

	t.Run("PayrollFundingsHandler invalid block range", func(t *testing.T) {
		h := handlers.PayrollFundingsHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/payroll/fundings?from_block=1000&to_block=500", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeInvalidBlockRange {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeInvalidBlockRange, errEnv.Error.Code)
		}
	})

	t.Run("PayrollFundingsHandler invalid date range", func(t *testing.T) {
		h := handlers.PayrollFundingsHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/payroll/fundings?from_date=2026-09-20T00:00:00Z&to_date=2026-09-10T00:00:00Z", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeInvalidDateRange {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeInvalidDateRange, errEnv.Error.Code)
		}
	})

	t.Run("PayrollFundingsHandler invalid pagination", func(t *testing.T) {
		h := handlers.PayrollFundingsHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/payroll/fundings?page=-1", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeInvalidPagination {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeInvalidPagination, errEnv.Error.Code)
		}
	})

	t.Run("ReconciliationExceptionsHandler invalid severity", func(t *testing.T) {
		h := handlers.ReconciliationExceptionsHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/reconciliation/exceptions?severity=catastrophic", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
	})

	t.Run("ReconciliationExceptionsHandler invalid status", func(t *testing.T) {
		h := handlers.ReconciliationExceptionsHandler(nil, cfg)
		req := httptest.NewRequest(http.MethodGet, "/v1/reconciliation/exceptions?status=unknown", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
	})
}

func TestDatabaseIntegrationHandlers(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:root@localhost:5432/wtf_onchain"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pg, err := persistence.NewPostgres(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping live database integration tests: %v", err)
	}
	defer pg.Close()

	cfg := &config.Config{
		ChainID:               11155111,
		TokenAddress:          "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
		ConfirmationDepth:     64,
		DeploymentEnvironment: "test",
		RPCURL:                "https://rpc.example.com",
	}

	// 1. Ready Handler
	t.Run("ReadyHandler Live DB", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ready", nil)
		rec := httptest.NewRecorder()

		h := handlers.ReadyHandler(pg.Pool(), cfg)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK from ready handler, got: %d", rec.Code)
		}
	})

	// 2. Sync Status Handler
	t.Run("SyncStatusHandler Live DB", func(t *testing.T) {
		syncRepo := repository.NewSyncRepository(pg.Pool())
		h := handlers.SyncStatusHandler(syncRepo, cfg)

		req := httptest.NewRequest(http.MethodGet, "/v1/sync/status", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got: %d", rec.Code)
		}

		var env responses.SuccessEnvelope
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
	})

	// 3. Token Transfers Handler
	t.Run("TokenTransfersHandler Live DB", func(t *testing.T) {
		tokensRepo := repository.NewTokensRepository(pg.Pool())
		h := handlers.TokenTransfersHandler(tokensRepo, cfg)

		req := httptest.NewRequest(http.MethodGet, "/v1/tokens/0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238/transfers?page=1&page_size=10", nil)
		req.SetPathValue("address", "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got: %d (body: %s)", rec.Code, rec.Body.String())
		}

		var listEnv responses.ListEnvelope
		if err := json.Unmarshal(rec.Body.Bytes(), &listEnv); err != nil {
			t.Fatalf("failed to decode list response: %v", err)
		}

		if listEnv.Meta.Page != 1 || listEnv.Meta.PageSize != 10 {
			t.Fatalf("unexpected pagination meta: %+v", listEnv.Meta)
		}
	})

	// 4. Payroll Fundings Handler
	t.Run("PayrollFundingsHandler Live DB", func(t *testing.T) {
		payrollRepo := repository.NewPayrollRepository(pg.Pool())
		h := handlers.PayrollFundingsHandler(payrollRepo, cfg)

		req := httptest.NewRequest(http.MethodGet, "/v1/payroll/fundings?page=1&page_size=5", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got: %d", rec.Code)
		}
	})

	// 5. Payroll Claims Handler
	t.Run("PayrollClaimsHandler Live DB", func(t *testing.T) {
		payrollRepo := repository.NewPayrollRepository(pg.Pool())
		h := handlers.PayrollClaimsHandler(payrollRepo, cfg)

		req := httptest.NewRequest(http.MethodGet, "/v1/payroll/claims?page=1&page_size=5", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got: %d", rec.Code)
		}
	})

	// 6. Reconciliation Exceptions Handler
	t.Run("ReconciliationExceptionsHandler Live DB", func(t *testing.T) {
		recRepo := repository.NewReconciliationRepository(pg.Pool())
		h := handlers.ReconciliationExceptionsHandler(recRepo, cfg)

		req := httptest.NewRequest(http.MethodGet, "/v1/reconciliation/exceptions?page=1&page_size=5", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got: %d", rec.Code)
		}
	})

	// 7. Transaction Handler (Not Found case)
	t.Run("TransactionHandler Not Found Live DB", func(t *testing.T) {
		txRepo := repository.NewTransactionsRepository(pg.Pool())
		h := handlers.TransactionHandler(txRepo, cfg)

		req := httptest.NewRequest(http.MethodGet, "/v1/transactions/0x0000000000000000000000000000000000000000000000000000000000000001", nil)
		req.SetPathValue("hash", "0x0000000000000000000000000000000000000000000000000000000000000001")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found, got: %d", rec.Code)
		}

		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeTransactionNotFound {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeTransactionNotFound, errEnv.Error.Code)
		}
	})

	// 8. Employer Handler (Not Found case)
	t.Run("EmployerHandler Not Found Live DB", func(t *testing.T) {
		empRepo := repository.NewEmployersRepository(pg.Pool())
		h := handlers.EmployerHandler(empRepo, cfg)

		req := httptest.NewRequest(http.MethodGet, "/v1/employers/0x0000000000000000000000000000000000000001", nil)
		req.SetPathValue("address", "0x0000000000000000000000000000000000000001")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found, got: %d", rec.Code)
		}

		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeEmployerNotFound {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeEmployerNotFound, errEnv.Error.Code)
		}
	})

	// 9. Employee Handler (Not Found case)
	t.Run("EmployeeHandler Not Found Live DB", func(t *testing.T) {
		employeeRepo := repository.NewEmployeesRepository(pg.Pool())
		h := handlers.EmployeeHandler(employeeRepo, cfg)

		req := httptest.NewRequest(http.MethodGet, "/v1/employees/0x0000000000000000000000000000000000000001", nil)
		req.SetPathValue("address", "0x0000000000000000000000000000000000000001")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 Not Found, got: %d", rec.Code)
		}

		var errEnv responses.ErrorEnvelope
		_ = json.Unmarshal(rec.Body.Bytes(), &errEnv)
		if errEnv.Error.Code != responses.ErrCodeEmployeeNotFound {
			t.Fatalf("expected error code %s, got: %s", responses.ErrCodeEmployeeNotFound, errEnv.Error.Code)
		}
	})
}
