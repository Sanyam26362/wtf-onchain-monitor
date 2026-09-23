package handlers_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"worldtradefuture/indexer/internal/api/handlers"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/models"
	"worldtradefuture/indexer/internal/persistence"
	"worldtradefuture/indexer/internal/repository"
)

type mockChainEventsRepo struct {
	mu       sync.Mutex
	inserted []*models.ChainEvent
	insertFn func(ctx context.Context, event *models.ChainEvent) error
}

func (m *mockChainEventsRepo) InsertChainEvent(ctx context.Context, event *models.ChainEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.insertFn != nil {
		return m.insertFn(ctx, event)
	}
	m.inserted = append(m.inserted, event)
	return nil
}

func (m *mockChainEventsRepo) GetEventsByEscrowID(ctx context.Context, escrowID string) ([]models.ChainEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var res []models.ChainEvent
	for _, e := range m.inserted {
		if e.EscrowID == escrowID {
			res = append(res, *e)
		}
	}
	return res, nil
}

func (m *mockChainEventsRepo) GetEventByTxAndLogIndex(ctx context.Context, txHash string, logIndex int) (*models.ChainEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.inserted {
		if strings.EqualFold(e.TxHash, txHash) && e.LogIndex == logIndex {
			return e, nil
		}
	}
	return nil, nil
}

func computeSignature(body []byte, key string) string {
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func getTestRedis(t *testing.T) *redis.Client {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rdb, err := persistence.NewRedisClient(ctx, redisURL, "", 0)
	if err != nil {
		t.Skipf("skipping test requiring live Redis: %v", err)
	}
	return rdb
}

func getTestPostgres(t *testing.T) (*persistence.Postgres, *repository.ChainEventsRepository) {
	_ = godotenv.Load("../../.env")
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://wtf_user:wtf_password@localhost:5433/wtf_indexer?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pg, err := persistence.NewPostgres(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping live database test: failed to connect to postgres: %v", err)
	}

	migrationsDir := "../../migrations"
	if _, err := os.Stat(migrationsDir); err == nil {
		_ = pg.RunMigrations(ctx, migrationsDir)
	}

	repo := repository.NewChainEventsRepository(pg.Pool())
	return pg, repo
}

// Test 1: Invalid or missing HMAC signature returns 401 Unauthorized
func TestWebhookHandler_SignatureValidation(t *testing.T) {
	signingKey := "whsec_test_secret_key"
	cfg := &config.Config{
		AlchemyWebhookSigningKey: signingKey,
		WTFEscrowContractAddress: "0x0000000000000000000000000000000000000000",
	}

	repo := &mockChainEventsRepo{}
	handler := handlers.NewWebhookHandler(cfg, repo, nil)

	validPayload := `{"webhookId":"wh_123","event":{"data":{"block":{"logs":[]}}}}`

	t.Run("missing signature header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewBufferString(validPayload))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized, got: %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "Invalid signature") {
			t.Fatalf("expected 'Invalid signature' in body, got: %s", rec.Body.String())
		}
	})

	t.Run("invalid signature content", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewBufferString(validPayload))
		req.Header.Set("x-alchemy-signature", "0xdeadbeefbadsignature1234567890abcdef")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 Unauthorized, got: %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "Invalid signature") {
			t.Fatalf("expected 'Invalid signature' in body, got: %s", rec.Body.String())
		}
	})

	t.Run("malformed json with valid signature", func(t *testing.T) {
		malformed := `{"webhookId": not valid json`
		sig := computeSignature([]byte(malformed), signingKey)

		req := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewBufferString(malformed))
		req.Header.Set("x-alchemy-signature", sig)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 Bad Request, got: %d", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "Malformed JSON payload") {
			t.Fatalf("expected 'Malformed JSON payload' in body, got: %s", rec.Body.String())
		}
	})
}

// Test 2 & Test 3: Valid signature processes event, inserts into DB, broadcasts to Redis, and deduplicates replay
func TestWebhookHandler_ValidIngestionAndDeduplication(t *testing.T) {
	rdb := getTestRedis(t)
	defer rdb.Close()

	ctx := context.Background()
	signingKey := "whsec_test_secret_key"
	cfg := &config.Config{
		ChainID:                  11155111,
		AlchemyWebhookSigningKey: signingKey,
		WTFEscrowContractAddress: "0x0000000000000000000000000000000000000000",
	}

	repo := &mockChainEventsRepo{}
	handler := handlers.NewWebhookHandler(cfg, repo, rdb)

	nonce := time.Now().UnixNano()
	txHash := fmt.Sprintf("0xtx_%016x_%016x", nonce, nonce+1)
	logIndex := 1
	escrowID := fmt.Sprintf("0xescrow_%016x", nonce)
	dedupeKey := fmt.Sprintf("wtf:chain:evt:%s:%d", txHash, logIndex)

	// Clean up Redis key before test
	_ = rdb.Del(ctx, dedupeKey).Err()
	defer func() { _ = rdb.Del(ctx, dedupeKey).Err() }()

	// Subscribe to Redis Pub/Sub channel
	pubsub := rdb.Subscribe(ctx, "wtf:chain:settled")
	defer pubsub.Close()

	// Wait briefly for subscription to activate
	_, err := pubsub.Receive(ctx)
	if err != nil {
		t.Fatalf("failed to receive subscription confirmation: %v", err)
	}

	payloadStruct := models.AlchemyWebhookPayload{
		WebhookID: "wh_phase3_test",
		ID:        "evt_phase3_test",
		CreatedAt: time.Now().UTC(),
		Type:      "ADDRESS_ACTIVITY",
		Event: models.AlchemyEvent{
			Data: models.AlchemyData{
				Block: models.AlchemyBlock{
					Number: "0x112233",
					Hash:   "0xblockhash_phase3",
					Logs: []models.AlchemyLog{
						{
							TransactionHash:  txHash,
							LogIndex:         logIndex,
							TransactionIndex: 0,
							BlockNumber:      "0x112233",
							Address:          "0x111122223333444455556666777788889999aaaa",
							Topics:           []string{"0xtopic0_sig", escrowID},
						},
					},
				},
			},
		},
	}

	bodyBytes, err := json.Marshal(payloadStruct)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}
	signature := computeSignature(bodyBytes, signingKey)

	// --- Step 1: Initial Ingestion (Test 2) ---
	req1 := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewReader(bodyBytes))
	req1.Header.Set("x-alchemy-signature", signature)
	rec1 := httptest.NewRecorder()

	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on initial ingestion, got: %d (body: %s)", rec1.Code, rec1.Body.String())
	}

	// Verify DB insert called
	repo.mu.Lock()
	if len(repo.inserted) != 1 {
		repo.mu.Unlock()
		t.Fatalf("expected exactly 1 inserted event in repo, got %d", len(repo.inserted))
	}
	inserted := repo.inserted[0]
	repo.mu.Unlock()

	if inserted.TxHash != txHash {
		t.Errorf("expected TxHash %s, got %s", txHash, inserted.TxHash)
	}
	if inserted.LogIndex != logIndex {
		t.Errorf("expected LogIndex %d, got %d", logIndex, inserted.LogIndex)
	}
	if inserted.EscrowID != escrowID {
		t.Errorf("expected EscrowID %s, got %s", escrowID, inserted.EscrowID)
	}
	if inserted.EventName != "EscrowSettled" {
		t.Errorf("expected EventName EscrowSettled, got %s", inserted.EventName)
	}
	if inserted.BlockNumber != 0x112233 {
		t.Errorf("expected BlockNumber %d, got %d", 0x112233, inserted.BlockNumber)
	}

	// Verify Redis Pub/Sub message received
	msgCh := pubsub.Channel()
	select {
	case msg := <-msgCh:
		var settled models.ChainSettledMessage
		if err := json.Unmarshal([]byte(msg.Payload), &settled); err != nil {
			t.Fatalf("failed to decode pubsub message: %v", err)
		}
		if settled.EscrowID != escrowID {
			t.Errorf("expected pubsub EscrowID %s, got %s", escrowID, settled.EscrowID)
		}
		if settled.TxHash != txHash {
			t.Errorf("expected pubsub TxHash %s, got %s", txHash, settled.TxHash)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message on wtf:chain:settled")
	}

	// Verify Redis dedupe key exists
	val, err := rdb.Get(ctx, dedupeKey).Result()
	if err != nil || val != "1" {
		t.Fatalf("expected dedupe key %s to be '1', got val: %s, err: %v", dedupeKey, val, err)
	}

	// --- Step 2: Replay Ingestion (Test 3) ---
	req2 := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewReader(bodyBytes))
	req2.Header.Set("x-alchemy-signature", signature)
	rec2 := httptest.NewRecorder()

	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on replayed ingestion, got: %d", rec2.Code)
	}

	// Repository should NOT have any new inserts (still count = 1)
	repo.mu.Lock()
	if len(repo.inserted) != 1 {
		repo.mu.Unlock()
		t.Fatalf("expected repo inserted count to remain 1 after deduplication, got: %d", len(repo.inserted))
	}
	repo.mu.Unlock()

	// Redis Pub/Sub should NOT receive any duplicate message
	select {
	case dupMsg := <-msgCh:
		t.Fatalf("unexpected pubsub message received on duplicate webhook: %+v", dupMsg)
	case <-time.After(200 * time.Millisecond):
		// Expected: no message sent
	}
}

// Test 4: Database idempotency safety net: verify DB insert succeeds cleanly without duplicate row errors even if Redis is bypassed
func TestWebhookHandler_DatabaseIdempotencySafetyNet(t *testing.T) {
	pg, repo := getTestPostgres(t)
	defer pg.Close()

	ctx := context.Background()
	signingKey := "whsec_test_secret_key"
	cfg := &config.Config{
		ChainID:                  11155111,
		AlchemyWebhookSigningKey: signingKey,
		WTFEscrowContractAddress: "0x0000000000000000000000000000000000000000",
	}

	// Nil Redis client simulates Redis outage or bypassed in-memory deduplication tier
	handler := handlers.NewWebhookHandler(cfg, repo, nil)

	nonce := time.Now().UnixNano()
	txHash := fmt.Sprintf("0xtx_db_safety_%016x", nonce)
	logIndex := 0
	escrowID := fmt.Sprintf("0xescrow_db_safety_%016x", nonce)

	payloadStruct := models.AlchemyWebhookPayload{
		WebhookID: "wh_db_safety_test",
		ID:        "evt_db_safety_test",
		CreatedAt: time.Now().UTC(),
		Type:      "ADDRESS_ACTIVITY",
		Event: models.AlchemyEvent{
			Data: models.AlchemyData{
				Block: models.AlchemyBlock{
					Number: "12345678",
					Hash:   "0xblock_hash_db_safety",
					Logs: []models.AlchemyLog{
						{
							TransactionHash:  txHash,
							LogIndex:         logIndex,
							TransactionIndex: 0,
							BlockNumber:      "12345678",
							Address:          "0x111122223333444455556666777788889999aaaa",
							Topics:           []string{"0xtopic0", escrowID},
						},
					},
				},
			},
		},
	}

	bodyBytes, err := json.Marshal(payloadStruct)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}
	signature := computeSignature(bodyBytes, signingKey)

	// First execution: inserts event
	req1 := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewReader(bodyBytes))
	req1.Header.Set("x-alchemy-signature", signature)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on first request, got: %d (%s)", rec1.Code, rec1.Body.String())
	}

	// Verify row in database
	events, err := repo.GetEventsByEscrowID(ctx, escrowID)
	if err != nil {
		t.Fatalf("failed to get events by escrowID: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 row in DB for escrowID %s, got %d", escrowID, len(events))
	}

	// Second execution (Redis bypassed): database unique constraint handles idempotency cleanly
	req2 := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewReader(bodyBytes))
	req2.Header.Set("x-alchemy-signature", signature)
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on repeated request even with bypassed Redis, got: %d (%s)", rec2.Code, rec2.Body.String())
	}

	// Verify database row was NOT duplicated
	eventsAfter, err := repo.GetEventsByEscrowID(ctx, escrowID)
	if err != nil {
		t.Fatalf("failed to query events after second insert: %v", err)
	}
	if len(eventsAfter) != 1 {
		t.Fatalf("expected exactly 1 row in DB after repeated insert, got %d", len(eventsAfter))
	}
}

// Test 5: Contract Address Filtering
func TestWebhookHandler_ContractAddressFiltering(t *testing.T) {
	signingKey := "whsec_test_secret_key"
	targetContract := "0x7777777777777777777777777777777777777777"
	otherContract := "0x9999999999999999999999999999999999999999"

	cfg := &config.Config{
		AlchemyWebhookSigningKey: signingKey,
		WTFEscrowContractAddress: targetContract,
	}

	repo := &mockChainEventsRepo{}
	handler := handlers.NewWebhookHandler(cfg, repo, nil)

	payloadStruct := models.AlchemyWebhookPayload{
		WebhookID: "wh_filter_test",
		Event: models.AlchemyEvent{
			Data: models.AlchemyData{
				Block: models.AlchemyBlock{
					Logs: []models.AlchemyLog{
						{
							TransactionHash: "0xtx_ignored",
							LogIndex:        0,
							Address:         otherContract,
							Topics:          []string{"0xsig", "0xescrow_ignored"},
						},
						{
							TransactionHash: "0xtx_matched",
							LogIndex:        1,
							Address:         targetContract,
							Topics:          []string{"0xsig", "0xescrow_matched"},
						},
					},
				},
			},
		},
	}

	bodyBytes, _ := json.Marshal(payloadStruct)
	signature := computeSignature(bodyBytes, signingKey)

	req := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewReader(bodyBytes))
	req.Header.Set("x-alchemy-signature", signature)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got: %d", rec.Code)
	}

	// Only the matching contract log should be processed
	repo.mu.Lock()
	defer repo.mu.Unlock()
	if len(repo.inserted) != 1 {
		t.Fatalf("expected exactly 1 inserted event, got %d", len(repo.inserted))
	}
	if repo.inserted[0].TxHash != "0xtx_matched" {
		t.Errorf("expected matched tx 0xtx_matched, got: %s", repo.inserted[0].TxHash)
	}
}
