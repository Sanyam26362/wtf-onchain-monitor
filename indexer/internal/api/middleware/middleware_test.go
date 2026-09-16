package middleware_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"worldtradefuture/indexer/internal/api/middleware"
	"worldtradefuture/indexer/internal/api/responses"
)

func TestRequestID(t *testing.T) {
	t.Run("generates new request id when absent", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		rec := httptest.NewRecorder()

		var ctxReqID string
		handler := middleware.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctxReqID = middleware.GetRequestID(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

		handler.ServeHTTP(rec, req)

		headerID := rec.Header().Get("X-Request-ID")
		if headerID == "" {
			t.Fatal("expected X-Request-ID header to be set")
		}
		if ctxReqID != headerID {
			t.Fatalf("expected context request ID (%s) to match header (%s)", ctxReqID, headerID)
		}
	})

	t.Run("preserves incoming request id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Request-ID", "custom-client-id-1234")
		rec := httptest.NewRecorder()

		handler := middleware.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		handler.ServeHTTP(rec, req)

		if rec.Header().Get("X-Request-ID") != "custom-client-id-1234" {
			t.Fatalf("expected preserved request id, got: %s", rec.Header().Get("X-Request-ID"))
		}
	})
}

func TestCORS(t *testing.T) {
	t.Run("handles options preflight", func(t *testing.T) {
		corsMw := middleware.CORS("*")
		req := httptest.NewRequest(http.MethodOptions, "/test", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		rec := httptest.NewRecorder()

		handler := corsMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204 No Content for OPTIONS, got: %d", rec.Code)
		}
		if rec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
			t.Fatalf("expected CORS origin header set, got: %s", rec.Header().Get("Access-Control-Allow-Origin"))
		}
	})
}

func TestRecoverer(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	recMw := middleware.Recoverer(logger)

	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("database connection collapsed unexpectedly")
	})

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rec := httptest.NewRecorder()

	handler := recMw(panicHandler)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 status code, got: %d", rec.Code)
	}

	var errEnv responses.ErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &errEnv); err != nil {
		t.Fatalf("failed to decode error envelope: %v", err)
	}

	if errEnv.Error.Code != responses.ErrCodeInternalError {
		t.Fatalf("expected error code %s, got %s", responses.ErrCodeInternalError, errEnv.Error.Code)
	}
}

func TestRequireOperatorAuth(t *testing.T) {
	const secretKey = "super-secret-operator-key-999"

	tests := []struct {
		name         string
		authHeader   string
		apiKeyHeader string
		wantStatus   int
	}{
		{
			name:       "no auth headers",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "invalid bearer token",
			authHeader: "Bearer wrong-key",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "valid bearer token",
			authHeader: "Bearer " + secretKey,
			wantStatus: http.StatusOK,
		},
		{
			name:         "valid X-API-Key header",
			apiKeyHeader: secretKey,
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authMw := middleware.RequireOperatorAuth(secretKey)
			req := httptest.NewRequest(http.MethodPost, "/v1/sync/backfill", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			if tt.apiKeyHeader != "" {
				req.Header.Set("X-API-Key", tt.apiKeyHeader)
			}
			rec := httptest.NewRecorder()

			handler := authMw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))

			handler.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, rec.Code)
			}
		})
	}
}
