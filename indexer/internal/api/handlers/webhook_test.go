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

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"

	"worldtradefuture/indexer/internal/api/handlers"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/models"
	"worldtradefuture/indexer/internal/persistence"
	"worldtradefuture/indexer/internal/repository"
)

type mockEscrowEventsRepo struct {
	mu       sync.Mutex
	inserted []*models.EscrowEvent
	insertFn func(ctx context.Context, event *models.EscrowEvent) error
}

func (m *mockEscrowEventsRepo) SaveEscrowEvent(ctx context.Context, event *models.EscrowEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.insertFn != nil {
		return m.insertFn(ctx, event)
	}
	m.inserted = append(m.inserted, event)
	return nil
}

func (m *mockEscrowEventsRepo) GetEscrowEventsByEscrowID(ctx context.Context, escrowID string) ([]*models.EscrowEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var res []*models.EscrowEvent
	for _, e := range m.inserted {
		if e.EscrowID != nil && *e.EscrowID == escrowID {
			res = append(res, e)
		}
	}
	return res, nil
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

func getTestPostgres(t *testing.T) (*persistence.Postgres, *repository.EscrowEventsRepository) {
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

	repo := repository.NewEscrowEventsRepository(pg.Pool())
	return pg, repo
}

// Test 1: Invalid or missing HMAC signature returns 401 Unauthorized
func TestWebhookHandler_SignatureValidation(t *testing.T) {
	signingKey := "whsec_test_secret_key"
	cfg := &config.Config{
		AlchemyWebhookSigningKey: signingKey,
		WTFEscrowContractAddress: "0x0000000000000000000000000000000000000000",
	}

	repo := &mockEscrowEventsRepo{}
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

	repo := &mockEscrowEventsRepo{}
	handler := handlers.NewWebhookHandler(cfg, repo, rdb)

	nonce := time.Now().UnixNano()
	txHash := fmt.Sprintf("0x%032x%032x", nonce, nonce+1)
	logIndex := 1
	escrowIDHex := fmt.Sprintf("0x%064x", nonce)
	dedupeKey := fmt.Sprintf("wtf:chain:evt:%s:%d", txHash, logIndex)

	// Clean up Redis key before test
	_ = rdb.Del(ctx, dedupeKey).Err()
	defer func() { _ = rdb.Del(ctx, dedupeKey).Err() }()

	// Subscribe to Redis Pub/Sub channel
	pubsub := rdb.Subscribe(ctx, "wtf:chain:settled")
	defer pubsub.Close()

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
							Topics:           []string{"0xtopic0_sig", escrowIDHex},
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

	// Initial Ingestion
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

	if inserted.TxHash != common.HexToHash(txHash) {
		t.Errorf("expected TxHash %s, got %s", txHash, inserted.TxHash.Hex())
	}
	if inserted.LogIndex != uint(logIndex) {
		t.Errorf("expected LogIndex %d, got %d", logIndex, inserted.LogIndex)
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
		if settled.TxHash != txHash {
			t.Errorf("expected pubsub TxHash %s, got %s", txHash, settled.TxHash)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message on wtf:chain:settled")
	}

	// Replay Ingestion (deduplication)
	req2 := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewReader(bodyBytes))
	req2.Header.Set("x-alchemy-signature", signature)
	rec2 := httptest.NewRecorder()

	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on replayed ingestion, got: %d", rec2.Code)
	}

	repo.mu.Lock()
	if len(repo.inserted) != 1 {
		repo.mu.Unlock()
		t.Fatalf("expected repo inserted count to remain 1 after deduplication, got: %d", len(repo.inserted))
	}
	repo.mu.Unlock()
}

// Test 4: Real ABI Decoding - EscrowCreated, EscrowReleased, DisputeRaised
func TestWebhookHandler_RealABIDecoding(t *testing.T) {
	signingKey := "whsec_test_secret_key"
	cfg := &config.Config{
		ChainID:                  11155111,
		AlchemyWebhookSigningKey: signingKey,
		EscrowContractAddress:    "0x3333333333333333333333333333333333333333",
	}

	repo := &mockEscrowEventsRepo{}
	handler := handlers.NewWebhookHandler(cfg, repo, nil)

	// 1. EscrowCreated event
	// Topic0: 0x9405ad0a6208539879349284d71265479b1623846f70303da1f9890d6e8c10a7
	// Topic1: escrowId = 100
	// Topic2: buyer = 0x1111111111111111111111111111111111111111
	// Topic3: seller = 0x2222222222222222222222222222222222222222
	// Data: amount = 1000 (0x3e8, 32 bytes)
	createdLog := models.AlchemyLog{
		TransactionHash:  "0x1111111111111111111111111111111111111111111111111111111111111111",
		LogIndex:         0,
		TransactionIndex: 0,
		BlockNumber:      "12345",
		Address:          "0x3333333333333333333333333333333333333333",
		Topics: []string{
			"0x9405ad0a6208539879349284d71265479b1623846f70303da1f9890d6e8c10a7",
			"0x0000000000000000000000000000000000000000000000000000000000000064",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x0000000000000000000000002222222222222222222222222222222222222222",
		},
		Data: "0x00000000000000000000000000000000000000000000000000000000000003e8",
	}

	// 2. EscrowReleased event
	// Topic0: 0x10ce17ae7e78eb775b13182ea618b201c2c81afc8fee55c287291f8686f17eac
	// Topic1: escrowId = 100
	// Data: amount = 1000
	releasedLog := models.AlchemyLog{
		TransactionHash:  "0x2222222222222222222222222222222222222222222222222222222222222222",
		LogIndex:         1,
		TransactionIndex: 0,
		BlockNumber:      "12346",
		Address:          "0x3333333333333333333333333333333333333333",
		Topics: []string{
			"0x10ce17ae7e78eb775b13182ea618b201c2c81afc8fee55c287291f8686f17eac",
			"0x0000000000000000000000000000000000000000000000000000000000000064",
		},
		Data: "0x00000000000000000000000000000000000000000000000000000000000003e8",
	}

	payload := models.AlchemyWebhookPayload{
		WebhookID: "wh_abi_test",
		Event: models.AlchemyEvent{
			Data: models.AlchemyData{
				Block: models.AlchemyBlock{
					Number: "12346",
					Logs:   []models.AlchemyLog{createdLog, releasedLog},
				},
			},
		},
	}

	bodyBytes, _ := json.Marshal(payload)
	signature := computeSignature(bodyBytes, signingKey)

	req := httptest.NewRequest(http.MethodPost, "/api/indexer/webhook", bytes.NewReader(bodyBytes))
	req.Header.Set("x-alchemy-signature", signature)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got: %d (%s)", rec.Code, rec.Body.String())
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.inserted) != 2 {
		t.Fatalf("expected 2 events decoded and inserted, got %d", len(repo.inserted))
	}

	// Verify EscrowCreated
	ev0 := repo.inserted[0]
	if ev0.EventType != "EscrowCreated" {
		t.Errorf("expected EventType 'EscrowCreated', got %s", ev0.EventType)
	}
	if ev0.EscrowID == nil || *ev0.EscrowID != "100" {
		t.Errorf("expected EscrowID 100, got %v", ev0.EscrowID)
	}
	if ev0.RawData["amount"] != "1000" {
		t.Errorf("expected amount '1000', got %v", ev0.RawData["amount"])
	}
	if !strings.EqualFold(fmt.Sprintf("%v", ev0.RawData["buyer"]), "0x1111111111111111111111111111111111111111") {
		t.Errorf("unexpected buyer: %v", ev0.RawData["buyer"])
	}
	if !strings.EqualFold(fmt.Sprintf("%v", ev0.RawData["seller"]), "0x2222222222222222222222222222222222222222") {
		t.Errorf("unexpected seller: %v", ev0.RawData["seller"])
	}

	// Verify EscrowReleased
	ev1 := repo.inserted[1]
	if ev1.EventType != "EscrowReleased" {
		t.Errorf("expected EventType 'EscrowReleased', got %s", ev1.EventType)
	}
	if ev1.EscrowID == nil || *ev1.EscrowID != "100" {
		t.Errorf("expected EscrowID 100, got %v", ev1.EscrowID)
	}
	if ev1.RawData["amount"] != "1000" {
		t.Errorf("expected amount '1000', got %v", ev1.RawData["amount"])
	}
}

// Test 5: Escrow Query Endpoint GET /v1/escrow/{id}/events
func TestEscrowEventsHandler_QueryEndpoint(t *testing.T) {
	repo := &mockEscrowEventsRepo{}
	cfg := &config.Config{}

	escrowID := "42"
	amount := "5000"
	contractAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	repo.inserted = append(repo.inserted,
		&models.EscrowEvent{
			ID:              1,
			ChainID:         11155111,
			ContractAddress: contractAddr,
			EventType:       "EscrowCreated",
			TxHash:          common.HexToHash("0xaaa1"),
			BlockNumber:     100,
			LogIndex:        0,
			EscrowID:        &escrowID,
			Amount:          &amount,
			RawData:         map[string]any{"amount": "5000"},
		},
		&models.EscrowEvent{
			ID:              2,
			ChainID:         11155111,
			ContractAddress: contractAddr,
			EventType:       "EscrowReleased",
			TxHash:          common.HexToHash("0xaaa2"),
			BlockNumber:     105,
			LogIndex:        0,
			EscrowID:        &escrowID,
			Amount:          &amount,
			RawData:         map[string]any{"amount": "5000"},
		},
	)

	handler := handlers.EscrowEventsHandler(repo, cfg)

	t.Run("query by decimal id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/escrow/42/events", nil)
		req.SetPathValue("id", "42")
		rec := httptest.NewRecorder()

		handler(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got: %d (%s)", rec.Code, rec.Body.String())
		}

		var resp struct {
			Data []models.EscrowEvent `json:"data"`
			Meta map[string]any       `json:"meta"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode response JSON: %v", err)
		}
		if len(resp.Data) != 2 {
			t.Fatalf("expected 2 events returned, got %d", len(resp.Data))
		}
		if resp.Data[0].EventType != "EscrowCreated" || resp.Data[1].EventType != "EscrowReleased" {
			t.Errorf("unexpected event types returned: %+v", resp.Data)
		}
	})

	t.Run("query by hex id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/escrow/0x2a/events", nil)
		req.SetPathValue("id", "0x2a")
		rec := httptest.NewRecorder()

		handler(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got: %d (%s)", rec.Code, rec.Body.String())
		}

		var resp struct {
			Data []models.EscrowEvent `json:"data"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if len(resp.Data) != 2 {
			t.Fatalf("expected 2 events returned for hex id 0x2a, got %d", len(resp.Data))
		}
	})
}

// Test 6: Live Postgres Integration
func TestWebhookHandler_LivePostgresIntegration(t *testing.T) {
	pg, repo := getTestPostgres(t)
	defer pg.Close()

	ctx := context.Background()
	signingKey := "whsec_test_secret_key"
	cfg := &config.Config{
		ChainID:                  11155111,
		AlchemyWebhookSigningKey: signingKey,
		EscrowContractAddress:    "0x7777777777777777777777777777777777777777",
	}

	handler := handlers.NewWebhookHandler(cfg, repo, nil)

	nonce := time.Now().UnixNano()
	txHash := fmt.Sprintf("0x%032x%032x", nonce, nonce+1)
	logIndex := int(nonce % 500)
	escrowIDInt := nonce % 1000000
	escrowIDHex := fmt.Sprintf("0x%064x", escrowIDInt)

	createdLog := models.AlchemyLog{
		TransactionHash:  txHash,
		LogIndex:         logIndex,
		TransactionIndex: 0,
		BlockNumber:      "12345678",
		Address:          "0x7777777777777777777777777777777777777777",
		Topics: []string{
			"0x9405ad0a6208539879349284d71265479b1623846f70303da1f9890d6e8c10a7",
			escrowIDHex,
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x0000000000000000000000002222222222222222222222222222222222222222",
		},
		Data: "0x00000000000000000000000000000000000000000000000000000000000003e8",
	}

	payloadStruct := models.AlchemyWebhookPayload{
		WebhookID: "wh_live_test",
		CreatedAt: time.Now().UTC(),
		Event: models.AlchemyEvent{
			Data: models.AlchemyData{
				Block: models.AlchemyBlock{
					Number: "12345678",
					Logs:   []models.AlchemyLog{createdLog},
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
		t.Fatalf("expected 200 OK, got: %d (%s)", rec.Code, rec.Body.String())
	}

	events, err := repo.GetEscrowEventsByEscrowID(ctx, fmt.Sprintf("%d", escrowIDInt))
	if err != nil {
		t.Fatalf("failed to query live postgres escrow events: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("expected inserted event in live postgres, got 0")
	}
	if events[0].EventType != "EscrowCreated" {
		t.Errorf("expected EventType 'EscrowCreated', got %s", events[0].EventType)
	}
}

func TestWebhookHandler_ReorgRemovedMapping(t *testing.T) {
	signingKey := "whsec_test_reorg_key"
	cfg := &config.Config{
		ChainID:                  11155111,
		AlchemyWebhookSigningKey: signingKey,
		EscrowContractAddress:    "0x7777777777777777777777777777777777777777",
	}

	repo := &mockEscrowEventsRepo{}
	handler := handlers.NewWebhookHandler(cfg, repo, nil)

	reorgLog := models.AlchemyLog{
		TransactionHash: "0xreorg1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
		LogIndex:        0,
		BlockNumber:     "12345678",
		Address:         "0x7777777777777777777777777777777777777777",
		Removed:         true,
		Topics: []string{
			"0x9405ad0a6208539879349284d71265479b1623846f70303da1f9890d6e8c10a7",
			"0x000000000000000000000000000000000000000000000000000000000000002a",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x0000000000000000000000002222222222222222222222222222222222222222",
		},
		Data: "0x00000000000000000000000000000000000000000000000000000000000003e8",
	}

	payloadStruct := models.AlchemyWebhookPayload{
		WebhookID: "wh_reorg_test",
		CreatedAt: time.Now().UTC(),
		Event: models.AlchemyEvent{
			Data: models.AlchemyData{
				Block: models.AlchemyBlock{
					Number: "12345678",
					Logs:   []models.AlchemyLog{reorgLog},
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

	if len(repo.inserted) != 1 {
		t.Fatalf("expected 1 inserted event, got %d", len(repo.inserted))
	}
	if !repo.inserted[0].Removed {
		t.Errorf("expected event.Removed to be true for reorg log, got false")
	}
}

func TestWebhookHandler_RedisFailure_FailClosed(t *testing.T) {
	signingKey := "whsec_test_failclosed_key"
	cfg := &config.Config{
		ChainID:                  11155111,
		AlchemyWebhookSigningKey: signingKey,
		EscrowContractAddress:    "0x7777777777777777777777777777777777777777",
	}

	// Create a redis client pointing to an unreachable port so SetNX errors out
	badRedis := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:59999",
		DialTimeout: 50 * time.Millisecond,
		ReadTimeout: 50 * time.Millisecond,
	})
	defer badRedis.Close()

	repo := &mockEscrowEventsRepo{}
	handler := handlers.NewWebhookHandler(cfg, repo, badRedis)

	testLog := models.AlchemyLog{
		TransactionHash: "0xfailclosed1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
		LogIndex:        0,
		BlockNumber:     "12345678",
		Address:         "0x7777777777777777777777777777777777777777",
		Topics: []string{
			"0x9405ad0a6208539879349284d71265479b1623846f70303da1f9890d6e8c10a7",
			"0x000000000000000000000000000000000000000000000000000000000000002a",
			"0x0000000000000000000000001111111111111111111111111111111111111111",
			"0x0000000000000000000000002222222222222222222222222222222222222222",
		},
		Data: "0x00000000000000000000000000000000000000000000000000000000000003e8",
	}

	payloadStruct := models.AlchemyWebhookPayload{
		WebhookID: "wh_failclosed_test",
		CreatedAt: time.Now().UTC(),
		Event: models.AlchemyEvent{
			Data: models.AlchemyData{
				Block: models.AlchemyBlock{
					Number: "12345678",
					Logs:   []models.AlchemyLog{testLog},
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
		t.Fatalf("expected 200 OK HTTP response, got: %d", rec.Code)
	}

	// Must have failed closed: 0 events saved in repository
	if len(repo.inserted) != 0 {
		t.Fatalf("expected 0 inserted events due to fail-closed on redis error, got %d", len(repo.inserted))
	}
}

