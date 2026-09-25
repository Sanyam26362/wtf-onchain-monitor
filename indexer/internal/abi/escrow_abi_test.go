package abi_test

import (
	"strings"
	"testing"

	gethabi "github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"worldtradefuture/indexer/internal/abi"
)

func TestWTFEscrowABI_ParsingAndTopics(t *testing.T) {
	parsedABI, err := gethabi.JSON(strings.NewReader(abi.WTFEscrowABI))
	if err != nil {
		t.Fatalf("failed to parse WTFEscrowABI: %v", err)
	}

	expectedTopics := map[string]struct {
		expectedHash common.Hash
		sig          string
	}{
		"EscrowCreated": {
			expectedHash: abi.TopicEscrowCreated,
			sig:          "EscrowCreated(uint256,address,address,uint256)",
		},
		"EscrowReleased": {
			expectedHash: abi.TopicEscrowReleased,
			sig:          "EscrowReleased(uint256,uint256)",
		},
		"EscrowRefunded": {
			expectedHash: abi.TopicEscrowRefunded,
			sig:          "EscrowRefunded(uint256,uint256)",
		},
		"DisputeResolved": {
			expectedHash: abi.TopicDisputeResolved,
			sig:          "DisputeResolved(uint256,address,uint256)",
		},
		"DeliveryAcknowledged": {
			expectedHash: abi.TopicDeliveryAcknowledged,
			sig:          "DeliveryAcknowledged(uint256,uint256)",
		},
		"DisputeRaised": {
			expectedHash: abi.TopicDisputeRaised,
			sig:          "DisputeRaised(uint256,address,uint256)",
		},
	}

	for eventName, exp := range expectedTopics {
		ev, ok := parsedABI.Events[eventName]
		if !ok {
			t.Fatalf("event %s missing in parsed WTFEscrowABI", eventName)
		}

		calculatedHash := crypto.Keccak256Hash([]byte(exp.sig))
		if calculatedHash != exp.expectedHash {
			t.Errorf("event %s signature %s keccak mismatch: got %s, expected %s",
				eventName, exp.sig, calculatedHash.Hex(), exp.expectedHash.Hex())
		}

		if ev.ID != exp.expectedHash {
			t.Errorf("parsed event %s ID mismatch: got %s, expected %s",
				eventName, ev.ID.Hex(), exp.expectedHash.Hex())
		}

		if !abi.IsKnownEscrowTopic(ev.ID) {
			t.Errorf("IsKnownEscrowTopic returned false for canonical event %s (%s)", eventName, ev.ID.Hex())
		}
	}

	// Verify unknown topic returns false
	unknownTopic := common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	if abi.IsKnownEscrowTopic(unknownTopic) {
		t.Errorf("IsKnownEscrowTopic returned true for random unknown topic")
	}
}
