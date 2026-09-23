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
