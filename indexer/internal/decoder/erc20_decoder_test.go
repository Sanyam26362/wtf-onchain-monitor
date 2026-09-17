package decoder

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	indexerABI "worldtradefuture/indexer/internal/abi"
)

func TestERC20Decoder_DecodeTransfer(t *testing.T) {
	tokenAddr := common.HexToAddress("0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238")
	fromAddr := common.HexToAddress("0x5555555555555555555555555555555555555555")
	toAddr := common.HexToAddress("0x6666666666666666666666666666666666666666")
	amount := big.NewInt(750000)

	parsedABI, err := indexerABI.LoadERC20ABI("", `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		}
	]`)
	if err != nil {
		t.Fatalf("failed to parse ABI: %v", err)
	}

	filterer, err := indexerABI.NewGenericERC20Filterer(tokenAddr, parsedABI)
	if err != nil {
		t.Fatalf("failed to create filterer: %v", err)
	}

	dec, err := NewERC20Decoder(filterer)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	packedData, err := parsedABI.Events["Transfer"].Inputs.NonIndexed().Pack(amount)
	if err != nil {
		t.Fatalf("failed to pack data: %v", err)
	}

	txHash := common.HexToHash("0x9876543210abcdef9876543210abcdef9876543210abcdef9876543210abcdef")
	now := time.Now().UTC().Truncate(time.Second)

	rawLog := types.Log{
		Address: tokenAddr,
		Topics: []common.Hash{
			dec.TransferTopic(),
			common.BytesToHash(fromAddr.Bytes()),
			common.BytesToHash(toAddr.Bytes()),
		},
		Data:        packedData,
		BlockNumber: 11223344,
		TxHash:      txHash,
		Index:       7,
		Removed:     true, // test reorg flag preservation
	}

	transfer, err := dec.DecodeTransfer(11155111, rawLog, now)
	if err != nil {
		t.Fatalf("failed to decode transfer: %v", err)
	}

	if transfer.ChainID != 11155111 {
		t.Errorf("expected chain ID 11155111, got %d", transfer.ChainID)
	}
	if transfer.Token != tokenAddr {
		t.Errorf("expected token %s, got %s", tokenAddr.Hex(), transfer.Token.Hex())
	}
	if transfer.FromAddress != fromAddr {
		t.Errorf("expected from %s, got %s", fromAddr.Hex(), transfer.FromAddress.Hex())
	}
	if transfer.ToAddress != toAddr {
		t.Errorf("expected to %s, got %s", toAddr.Hex(), transfer.ToAddress.Hex())
	}
	if transfer.Amount.Cmp(amount) != 0 {
		t.Errorf("expected amount %s, got %s", amount.String(), transfer.Amount.String())
	}
	if transfer.TxHash != txHash {
		t.Errorf("expected tx hash %s, got %s", txHash.Hex(), transfer.TxHash.Hex())
	}
	if transfer.BlockNumber != 11223344 {
		t.Errorf("expected block number 11223344, got %d", transfer.BlockNumber)
	}
	if transfer.LogIndex != 7 {
		t.Errorf("expected log index 7, got %d", transfer.LogIndex)
	}
	if !transfer.Removed {
		t.Errorf("expected removed to be true for reorg awareness, got false")
	}
	if !transfer.BlockTimestamp.Equal(now) {
		t.Errorf("expected block timestamp %v, got %v", now, transfer.BlockTimestamp)
	}
}

func TestERC20Decoder_DecodeApproval(t *testing.T) {
	tokenAddr := common.HexToAddress("0x378AFb93CaDd39AFF154704d2D90Af8c401137E7")
	ownerAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	spenderAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	amount := big.NewInt(5000000)

	parsedABI, err := indexerABI.LoadERC20ABI("", `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "owner", "type": "address"},
				{"indexed": true, "name": "spender", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Approval",
			"type": "event"
		}
	]`)
	if err != nil {
		t.Fatalf("failed to parse ABI: %v", err)
	}

	filterer, err := indexerABI.NewGenericERC20Filterer(tokenAddr, parsedABI)
	if err != nil {
		t.Fatalf("failed to create filterer: %v", err)
	}

	dec, err := NewERC20Decoder(filterer)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	if !dec.HasApproval() {
		t.Fatal("expected HasApproval to be true")
	}

	apprTopic, ok := dec.ApprovalTopic()
	if !ok {
		t.Fatal("expected ApprovalTopic to return true")
	}

	packedData, err := parsedABI.Events["Approval"].Inputs.NonIndexed().Pack(amount)
	if err != nil {
		t.Fatalf("failed to pack data: %v", err)
	}

	txHash := common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890")
	now := time.Now().UTC().Truncate(time.Second)

	rawLog := types.Log{
		Address: tokenAddr,
		Topics: []common.Hash{
			apprTopic,
			common.BytesToHash(ownerAddr.Bytes()),
			common.BytesToHash(spenderAddr.Bytes()),
		},
		Data:        packedData,
		BlockNumber: 11717940,
		TxHash:      txHash,
		Index:       3,
		Removed:     false,
	}

	approval, err := dec.DecodeApproval(11155111, rawLog, now)
	if err != nil {
		t.Fatalf("failed to decode approval: %v", err)
	}

	if approval.Token != tokenAddr {
		t.Errorf("expected token %s, got %s", tokenAddr.Hex(), approval.Token.Hex())
	}
	if approval.Owner != ownerAddr {
		t.Errorf("expected owner %s, got %s", ownerAddr.Hex(), approval.Owner.Hex())
	}
	if approval.Spender != spenderAddr {
		t.Errorf("expected spender %s, got %s", spenderAddr.Hex(), approval.Spender.Hex())
	}
	if approval.Value.Cmp(amount) != 0 {
		t.Errorf("expected value %s, got %s", amount.String(), approval.Value.String())
	}
}
