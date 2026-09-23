package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"worldtradefuture/indexer/internal/api/middleware"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/models"
)

// ChainEventsRepository defines the storage interface required by WebhookHandler.
type ChainEventsRepository interface {
	InsertChainEvent(ctx context.Context, event *models.ChainEvent) error
	GetEventsByEscrowID(ctx context.Context, escrowID string) ([]models.ChainEvent, error)
	GetEventByTxAndLogIndex(ctx context.Context, txHash string, logIndex int) (*models.ChainEvent, error)
}

// WebhookHandler processes incoming Alchemy Notify webhooks with Redis deduplication and Pub/Sub broadcast.
type WebhookHandler struct {
	cfg         *config.Config
	eventsRepo  ChainEventsRepository
	redisClient *redis.Client
}

// NewWebhookHandler creates a new WebhookHandler instance.
func NewWebhookHandler(cfg *config.Config, eventsRepo ChainEventsRepository, redisClient *redis.Client) *WebhookHandler {
	return &WebhookHandler{
		cfg:         cfg,
		eventsRepo:  eventsRepo,
		redisClient: redisClient,
	}
}

// ServeHTTP enables WebhookHandler to satisfy http.Handler directly.
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.HandleAlchemyWebhook(w, r)
}

// HandleAlchemyWebhook ingests and processes Alchemy Notify webhook requests.
func (h *WebhookHandler) HandleAlchemyWebhook(w http.ResponseWriter, r *http.Request) {
	// 1. Read raw body
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "Failed to read request body"}`))
		return
	}
	defer r.Body.Close()

	// 2. Extract header x-alchemy-signature
	sigHeader := r.Header.Get("x-alchemy-signature")
	if sigHeader == "" {
		sigHeader = r.Header.Get("X-Alchemy-Signature")
	}

	signingKey := ""
	if h.cfg != nil {
		signingKey = h.cfg.AlchemyWebhookSigningKey
	}

	// 3. Validate signature using VerifyAlchemySignature
	if !middleware.VerifyAlchemySignature(rawBody, sigHeader, signingKey) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error": "Invalid signature"}`))
		return
	}

	// 4. Parse JSON into models.AlchemyWebhookPayload
	var payload models.AlchemyWebhookPayload
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "Malformed JSON payload"}`))
		return
	}

	// 5. Iterate through payload.Event.Data.Block.Logs
	expectedContract := ""
	if h.cfg != nil {
		expectedContract = strings.TrimSpace(h.cfg.WTFEscrowContractAddress)
	}

	ctx := r.Context()

	for _, log := range payload.Event.Data.Block.Logs {
		logAddr := log.Address
		if logAddr == "" {
			logAddr = log.Account.Address
		}

		// Contract address check: filter if expected address is set and not zero-address
		if expectedContract != "" && !isZeroAddress(expectedContract) {
			if logAddr != "" && !strings.EqualFold(logAddr, expectedContract) {
				continue
			}
		}

		// Redis Deduplication
		if h.redisClient != nil {
			dedupeKey := fmt.Sprintf("wtf:chain:evt:%s:%d", log.TransactionHash, log.LogIndex)
			ok, err := h.redisClient.SetNX(ctx, dedupeKey, "1", 30*24*time.Hour).Result()
			if err != nil {
				slog.Warn("failed to check redis deduplication", "key", dedupeKey, "error", err)
			} else if !ok {
				slog.Info("chain event already processed, skipping duplicate", "key", dedupeKey, "txHash", log.TransactionHash, "logIndex", log.LogIndex)
				continue
			}
		}

		// Parse block number
		blockNumStr := log.BlockNumber
		if blockNumStr == "" {
			blockNumStr = payload.Event.Data.Block.Number
		}
		blockNum := parseBlockNumber(blockNumStr)

		// Parse escrow_id: if len(topics) > 1, escrow_id = topics[1]
		escrowID := ""
		if len(log.Topics) > 1 {
			escrowID = log.Topics[1]
		}

		// ChainID: 11155111 (Sepolia) or from config
		chainID := int64(11155111)
		if h.cfg != nil && h.cfg.ChainID > 0 {
			chainID = h.cfg.ChainID
		}

		contractAddr := logAddr
		if contractAddr == "" && h.cfg != nil {
			contractAddr = h.cfg.WTFEscrowContractAddress
		}

		blockTimestamp := payload.CreatedAt.Unix()
		if blockTimestamp <= 0 {
			blockTimestamp = time.Now().Unix()
		}

		event := models.ChainEvent{
			ChainID:          chainID,
			ContractAddress:  contractAddr,
			EventName:        "EscrowSettled",
			TxHash:           log.TransactionHash,
			BlockNumber:      blockNum,
			BlockHash:        payload.Event.Data.Block.Hash,
			LogIndex:         log.LogIndex,
			TxIndex:          log.TransactionIndex,
			BlockTimestamp:   blockTimestamp,
			EscrowID:         escrowID,
			Buyer:            "0x",
			Seller:           "0x",
			Amount:           "0",
			AlchemyWebhookID: payload.WebhookID,
			RawPayload:       string(rawBody),
			IndexedAt:        time.Now().UTC(),
		}

		// Store in DB: idempotent insert
		if h.eventsRepo != nil {
			if err := h.eventsRepo.InsertChainEvent(ctx, &event); err != nil {
				slog.Error("failed to insert chain event into database", "txHash", log.TransactionHash, "logIndex", log.LogIndex, "error", err)
			}
		}

		// Redis Pub/Sub: publish to wtf:chain:settled
		if h.redisClient != nil {
			msg := models.ChainSettledMessage{
				EscrowID: escrowID,
				TxHash:   log.TransactionHash,
			}
			msgBytes, err := json.Marshal(msg)
			if err != nil {
				slog.Error("failed to marshal chain settled message", "error", err)
			} else {
				if err := h.redisClient.Publish(ctx, "wtf:chain:settled", msgBytes).Err(); err != nil {
					slog.Error("failed to publish chain settled message to redis", "channel", "wtf:chain:settled", "error", err)
				}
			}
		}
	}

	// 6. Return 200 OK with JSON {"ok": true}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok": true}`))
}

func parseBlockNumber(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		num, err := strconv.ParseInt(s[2:], 16, 64)
		if err == nil {
			return num
		}
	}
	num, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return num
	}
	return 0
}

func isZeroAddress(addr string) bool {
	a := strings.TrimPrefix(strings.ToLower(addr), "0x")
	return a == "" || strings.Trim(a, "0") == ""
}
