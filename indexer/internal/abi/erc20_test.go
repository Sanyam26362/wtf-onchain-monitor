package abi

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestGenericERC20Filterer_ParseTransfer_StandardABI(t *testing.T) {
	tokenAddr := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	fromAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	toAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")

	// 1,000,000 * 10^18 (1 million tokens with 18 decimals)
	amount, _ := new(big.Int).SetString("1000000000000000000000000", 10)

	parsedABI, err := LoadERC20ABI("", `[
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
		t.Fatalf("failed to load ABI: %v", err)
	}

	filterer, err := NewGenericERC20Filterer(tokenAddr, parsedABI)
	if err != nil {
		t.Fatalf("failed to create filterer: %v", err)
	}

	// Pack data
	transferEvent := parsedABI.Events["Transfer"]
	packedData, err := transferEvent.Inputs.NonIndexed().Pack(amount)
	if err != nil {
		t.Fatalf("failed to pack data: %v", err)
	}

	rawLog := types.Log{
		Address: tokenAddr,
		Topics: []common.Hash{
			transferEvent.ID,
			common.BytesToHash(fromAddr.Bytes()),
			common.BytesToHash(toAddr.Bytes()),
		},
		Data:        packedData,
		BlockNumber: 1234567,
		TxHash:      common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"),
		Index:       4,
		Removed:     false,
	}

	decoded, err := filterer.ParseTransfer(rawLog)
	if err != nil {
		t.Fatalf("failed to parse Transfer: %v", err)
	}

	if decoded.Token != tokenAddr {
		t.Errorf("expected token %s, got %s", tokenAddr.Hex(), decoded.Token.Hex())
	}
	if decoded.From != fromAddr {
		t.Errorf("expected from %s, got %s", fromAddr.Hex(), decoded.From.Hex())
	}
	if decoded.To != toAddr {
		t.Errorf("expected to %s, got %s", toAddr.Hex(), decoded.To.Hex())
	}
	if decoded.Value.Cmp(amount) != 0 {
		t.Errorf("expected amount %s, got %s", amount.String(), decoded.Value.String())
	}
	if decoded.Log.TxHash != rawLog.TxHash {
		t.Errorf("expected tx hash %s, got %s", rawLog.TxHash.Hex(), decoded.Log.TxHash.Hex())
	}
	if decoded.Log.Index != 4 {
		t.Errorf("expected log index 4, got %d", decoded.Log.Index)
	}
}

func TestGenericERC20Filterer_ParseTransfer_MaxUint256(t *testing.T) {
	tokenAddr := common.HexToAddress("0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238")
	fromAddr := common.HexToAddress("0x3333333333333333333333333333333333333333")
	toAddr := common.HexToAddress("0x4444444444444444444444444444444444444444")

	// Max uint256: 2^256 - 1
	maxUint256, _ := new(big.Int).SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)

	parsedABI, err := LoadERC20ABI("", `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "_from", "type": "address"},
				{"indexed": true, "name": "_to", "type": "address"},
				{"indexed": false, "name": "_value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		}
	]`)
	if err != nil {
		t.Fatalf("failed to load ABI: %v", err)
	}

	filterer, err := NewGenericERC20Filterer(tokenAddr, parsedABI)
	if err != nil {
		t.Fatalf("failed to create filterer: %v", err)
	}

	packedData, err := parsedABI.Events["Transfer"].Inputs.NonIndexed().Pack(maxUint256)
	if err != nil {
		t.Fatalf("failed to pack data: %v", err)
	}

	rawLog := types.Log{
		Address: tokenAddr,
		Topics: []common.Hash{
			filterer.TransferTopic(),
			common.BytesToHash(fromAddr.Bytes()),
			common.BytesToHash(toAddr.Bytes()),
		},
		Data:        packedData,
		BlockNumber: 9999999,
		Index:       1,
	}

	decoded, err := filterer.ParseTransfer(rawLog)
	if err != nil {
		t.Fatalf("failed to parse Transfer with max uint256: %v", err)
	}

	if decoded.Value.Cmp(maxUint256) != 0 {
		t.Fatalf("expected value %s, got %s", maxUint256.String(), decoded.Value.String())
	}
}

func TestGenericERC20Filterer_RejectsWrongToken(t *testing.T) {
	configuredToken := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	otherToken := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

	parsedABI, _ := LoadERC20ABI("", `[
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

	filterer, _ := NewGenericERC20Filterer(configuredToken, parsedABI)

	rawLog := types.Log{
		Address: otherToken, // Different address!
		Topics: []common.Hash{
			filterer.TransferTopic(),
			common.BytesToHash(common.Address{1}.Bytes()),
			common.BytesToHash(common.Address{2}.Bytes()),
		},
		Data: common.LeftPadBytes(big.NewInt(100).Bytes(), 32),
	}

	if filterer.IsTransferLog(rawLog) {
		t.Fatal("expected IsTransferLog to be false for wrong token address")
	}

	_, err := filterer.ParseTransfer(rawLog)
	if err == nil {
		t.Fatal("expected error when parsing Transfer from unrelated contract")
	}
}

func TestGenericERC20Filterer_ApprovalSupport(t *testing.T) {
	tokenAddr := common.HexToAddress("0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238")
	owner := common.HexToAddress("0x1111111111111111111111111111111111111111")
	spender := common.HexToAddress("0x2222222222222222222222222222222222222222")
	allowance := big.NewInt(5000)

	parsedABI, _ := LoadERC20ABI("", `[
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

	filterer, _ := NewGenericERC20Filterer(tokenAddr, parsedABI)
	appTopic, hasApproval := filterer.ApprovalTopic()
	if !hasApproval {
		t.Fatal("expected Approval event to be recognized")
	}

	packedData, _ := parsedABI.Events["Approval"].Inputs.NonIndexed().Pack(allowance)
	rawLog := types.Log{
		Address: tokenAddr,
		Topics: []common.Hash{
			appTopic,
			common.BytesToHash(owner.Bytes()),
			common.BytesToHash(spender.Bytes()),
		},
		Data: packedData,
	}

	appEvent, err := filterer.ParseApproval(rawLog)
	if err != nil {
		t.Fatalf("failed to parse Approval: %v", err)
	}
	if appEvent.Owner != owner || appEvent.Spender != spender || appEvent.Value.Cmp(allowance) != 0 {
		t.Fatalf("mismatched approval event: %+v", appEvent)
	}
}
