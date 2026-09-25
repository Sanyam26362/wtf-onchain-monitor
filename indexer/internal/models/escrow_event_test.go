package models_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/models"
)

func TestEscrowEvent_Serialization(t *testing.T) {
	escrowID := "11155111222333444555666777888999"
	amount := "5000000000000000000"
	now := time.Now().UTC().Truncate(time.Second)

	ev := models.EscrowEvent{
		ID:              1,
		ChainID:         11155111,
		ContractAddress: common.HexToAddress("0x807EB6317FbdF219C18B58ac0BF941bC4af268D5"),
		EventType:       "EscrowCreated",
		TxHash:          common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
		BlockNumber:     11732519,
		BlockTimestamp:  now,
		LogIndex:        2,
		Removed:         false,
		EscrowID:        &escrowID,
		Amount:          &amount,
		RawData: map[string]any{
			"buyer":  "0x1111111111111111111111111111111111111111",
			"seller": "0x2222222222222222222222222222222222222222",
			"amount": amount,
		},
		CreatedAt: now,
	}

	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("failed to marshal EscrowEvent: %v", err)
	}

	var decoded models.EscrowEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal EscrowEvent: %v", err)
	}

	if decoded.EscrowID == nil || *decoded.EscrowID != escrowID {
		t.Errorf("expected EscrowID %s, got %v", escrowID, decoded.EscrowID)
	}

	if decoded.Amount == nil || *decoded.Amount != amount {
		t.Errorf("expected Amount %s, got %v", amount, decoded.Amount)
	}

	if decoded.EventType != "EscrowCreated" {
		t.Errorf("expected EventType EscrowCreated, got %s", decoded.EventType)
	}
}
