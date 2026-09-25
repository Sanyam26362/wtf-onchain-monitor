package api_test

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"worldtradefuture/indexer/internal/api"
	"worldtradefuture/indexer/internal/config"
)

func TestRouter_WebhookRouteMounted(t *testing.T) {
	cfg := &config.Config{
		CORSAllowedOrigins:       "*",
		AlchemyWebhookSigningKey: "test_key",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Pass nil pool and nil redis for lightweight route mounting test
	router := api.NewRouter(nil, cfg, logger)

	req := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewBufferString("{}"))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	// Since request does not have x-alchemy-signature, it should reach WebhookHandler and return 401 Unauthorized (not 404 Not Found)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized from mounted webhook route, got: %d", rec.Code)
	}
}

func TestRouter_EscrowEventsRouteMounted(t *testing.T) {
	cfg := &config.Config{
		CORSAllowedOrigins: "*",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	router := api.NewRouter(nil, cfg, logger)

	req := httptest.NewRequest(http.MethodGet, "/v1/escrow/42/events", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	// Since pool is nil, it will attempt to query repo and fail with 500, but NOT 404 Not Found (proving route is mounted)
	if rec.Code == http.StatusNotFound {
		t.Fatalf("expected route /v1/escrow/{id}/events to be mounted, got 404 Not Found")
	}
}

func TestRouter_ChainEventsRouteMounted(t *testing.T) {
	cfg := &config.Config{
		CORSAllowedOrigins: "*",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	router := api.NewRouter(nil, cfg, logger)

	req := httptest.NewRequest(http.MethodGet, "/api/chain/events/42", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	// Since pool is nil, it will fail at repo query with 500, but NOT 404 Not Found (proving route is mounted)
	if rec.Code == http.StatusNotFound {
		t.Fatalf("expected alias route /api/chain/events/{id} to be mounted, got 404 Not Found")
	}
}

