package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	gethabi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"

	indexerABI "worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/api/middleware"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/models"
)

// EscrowEventsRepository defines the storage interface required by WebhookHandler.
type EscrowEventsRepository interface {
	SaveEscrowEvent(ctx context.Context, event *models.EscrowEvent) error
}

// WebhookHandler processes incoming Alchemy Notify webhooks with real ABI decoding,
// Redis deduplication, and Pub/Sub broadcast.
type WebhookHandler struct {
	cfg         *config.Config
	eventsRepo  EscrowEventsRepository
	redisClient *redis.Client
	abi         gethabi.ABI
}

// NewWebhookHandler creates a new WebhookHandler instance.
func NewWebhookHandler(cfg *config.Config, eventsRepo EscrowEventsRepository, redisClient *redis.Client) *WebhookHandler {
	parsedABI, err := gethabi.JSON(strings.NewReader(indexerABI.WTFEscrowABI))
	if err != nil {
		slog.Error("failed to parse WTFEscrow ABI for webhook handler", "error", err)
	}
	return &WebhookHandler{
		cfg:         cfg,
		eventsRepo:  eventsRepo,
		redisClient: redisClient,
		abi:         parsedABI,
	}
}

// ServeHTTP enables WebhookHandler to satisfy http.Handler directly.
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.HandleAlchemyWebhook(w, r)
}

// HandleAlchemyWebhook ingests, validates, decodes, and persists Alchemy Notify webhook requests.
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
		if h.cfg.EscrowContractAddress != "" {
			expectedContract = strings.TrimSpace(h.cfg.EscrowContractAddress)
		} else {
			expectedContract = strings.TrimSpace(h.cfg.WTFEscrowContractAddress)
		}
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

		// ChainID: 11155111 (Sepolia) or from config
		chainID := int64(11155111)
		if h.cfg != nil && h.cfg.ChainID > 0 {
			chainID = h.cfg.ChainID
		}

		contractAddr := logAddr
		if contractAddr == "" && h.cfg != nil {
			contractAddr = h.cfg.EscrowContractAddress
			if contractAddr == "" {
				contractAddr = h.cfg.WTFEscrowContractAddress
			}
		}

		blockTime := payload.CreatedAt.UTC()
		if blockTime.IsZero() {
			blockTime = time.Now().UTC()
		}

		// Decode Log with canonical WTFEscrow ABI signatures
		eventName, escrowID, amount, rawData := h.decodeAlchemyLog(log)

		escrowEvent := models.EscrowEvent{
			ChainID:         chainID,
			ContractAddress: common.HexToAddress(contractAddr),
			EventType:       eventName,
			TxHash:          common.HexToHash(log.TransactionHash),
			BlockNumber:     uint64(blockNum),
			BlockTimestamp:  blockTime,
			LogIndex:        uint(log.LogIndex),
			Removed:         false,
			EscrowID:        escrowID,
			Amount:          amount,
			RawData:         rawData,
			CreatedAt:       time.Now().UTC(),
		}

		// Store in DB: idempotent insert
		if h.eventsRepo != nil {
			if err := h.eventsRepo.SaveEscrowEvent(ctx, &escrowEvent); err != nil {
				slog.Error("failed to insert escrow event into database", "txHash", log.TransactionHash, "logIndex", log.LogIndex, "error", err)
			}
		}

		// Redis Pub/Sub: broadcast event notification payload to wtf:chain:events and wtf:chain:settled
		if h.redisClient != nil {
			escrowIDStr := ""
			if escrowID != nil {
				escrowIDStr = *escrowID
			}
			amountStr := ""
			if amount != nil {
				amountStr = *amount
			}

			payload := map[string]any{
				"escrowId":  escrowIDStr,
				"txHash":    log.TransactionHash,
				"eventType": eventName,
				"amount":    amountStr,
			}
			if msgBytes, err := json.Marshal(payload); err == nil {
				_ = h.redisClient.Publish(ctx, "wtf:chain:events", msgBytes).Err()
				_ = h.redisClient.Publish(ctx, "wtf:chain:settled", msgBytes).Err()
			}
		}
	}

	// 6. Return 200 OK with JSON {"ok": true}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok": true}`))
}

// decodeAlchemyLog attempts to decode the event using the parsed canonical ABI.
func (h *WebhookHandler) decodeAlchemyLog(log models.AlchemyLog) (string, *string, *string, map[string]any) {
	rawData := make(map[string]any)
	var escrowID *string
	var amount *string

	if len(log.Topics) == 0 {
		return "Unknown", nil, nil, rawData
	}

	// Extract escrowId from Topics[1] as *big.Int and store as string
	if len(log.Topics) > 1 {
		escrowHex := log.Topics[1]
		if bi := parseBigIntHex(escrowHex); bi != nil {
			s := bi.String()
			escrowID = &s
			rawData["escrowId"] = s
		}
	}

	topic0 := common.HexToHash(log.Topics[0])

	switch topic0 {
	case indexerABI.TopicEscrowCreated:
		// buyer (Topics[2]), seller (Topics[3]), amount from log.Data[0:32]
		if len(log.Topics) > 2 {
			buyer := common.HexToAddress(log.Topics[2])
			rawData["buyer"] = buyer.Hex()
		}
		if len(log.Topics) > 3 {
			seller := common.HexToAddress(log.Topics[3])
			rawData["seller"] = seller.Hex()
		}
		if log.Data != "" && log.Data != "0x" {
			dataBytes := common.FromHex(log.Data)
			if len(dataBytes) >= 32 {
				amt := new(big.Int).SetBytes(dataBytes[0:32])
				s := amt.String()
				amount = &s
				rawData["amount"] = s
			}
		}
		return "EscrowCreated", escrowID, amount, rawData

	case indexerABI.TopicEscrowReleased:
		// amount from log.Data[0:32]
		if log.Data != "" && log.Data != "0x" {
			dataBytes := common.FromHex(log.Data)
			if len(dataBytes) >= 32 {
				amt := new(big.Int).SetBytes(dataBytes[0:32])
				s := amt.String()
				amount = &s
				rawData["amount"] = s
			}
		}
		return "EscrowReleased", escrowID, amount, rawData

	case indexerABI.TopicEscrowRefunded:
		// amount from log.Data[0:32]
		if log.Data != "" && log.Data != "0x" {
			dataBytes := common.FromHex(log.Data)
			if len(dataBytes) >= 32 {
				amt := new(big.Int).SetBytes(dataBytes[0:32])
				s := amt.String()
				amount = &s
				rawData["amount"] = s
			}
		}
		return "EscrowRefunded", escrowID, amount, rawData

	case indexerABI.TopicDisputeResolved:
		// winner (Topics[2]), amountReleased from log.Data[0:32]
		if len(log.Topics) > 2 {
			winner := common.HexToAddress(log.Topics[2])
			rawData["winner"] = winner.Hex()
		}
		if log.Data != "" && log.Data != "0x" {
			dataBytes := common.FromHex(log.Data)
			if len(dataBytes) >= 32 {
				amt := new(big.Int).SetBytes(dataBytes[0:32])
				s := amt.String()
				amount = &s
				rawData["amountReleased"] = s
				rawData["amount"] = s
			}
		}
		return "DisputeResolved", escrowID, amount, rawData

	case indexerABI.TopicDeliveryAcknowledged:
		if log.Data != "" && log.Data != "0x" {
			dataBytes := common.FromHex(log.Data)
			if len(dataBytes) >= 32 {
				dt := new(big.Int).SetBytes(dataBytes[0:32])
				s := dt.String()
				rawData["deliveryTime"] = s
			}
		}
		return "DeliveryAcknowledged", escrowID, nil, rawData

	case indexerABI.TopicDisputeRaised:
		if len(log.Topics) > 2 {
			raisedBy := common.HexToAddress(log.Topics[2])
			rawData["raisedBy"] = raisedBy.Hex()
		}
		if log.Data != "" && log.Data != "0x" {
			dataBytes := common.FromHex(log.Data)
			if len(dataBytes) >= 32 {
				fee := new(big.Int).SetBytes(dataBytes[0:32])
				s := fee.String()
				rawData["disputeFee"] = s
			}
		}
		return "DisputeRaised", escrowID, nil, rawData

	default:
		if eventDef, err := h.abi.EventByID(topic0); err == nil {
			return eventDef.Name, escrowID, nil, rawData
		}
		return "Unknown", escrowID, nil, rawData
	}
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

func parseBigIntHex(s string) *big.Int {
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	bi := new(big.Int)
	bi, ok := bi.SetString(s, 16)
	if !ok {
		return nil
	}
	return bi
}

func isZeroAddress(addr string) bool {
	a := strings.TrimPrefix(strings.ToLower(addr), "0x")
	return a == "" || strings.Trim(a, "0") == ""
}
