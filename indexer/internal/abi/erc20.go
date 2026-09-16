package abi

import (
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// ERC20TransferEvent represents a decoded Transfer event from any ERC-20 compatible contract.
type ERC20TransferEvent struct {
	Token common.Address `json:"token"`
	From  common.Address `json:"from"`
	To    common.Address `json:"to"`
	Value *big.Int       `json:"value"`
	Log   types.Log      `json:"log"`
}

// ERC20ApprovalEvent represents a decoded Approval event from any ERC-20 compatible contract.
type ERC20ApprovalEvent struct {
	Token   common.Address `json:"token"`
	Owner   common.Address `json:"owner"`
	Spender common.Address `json:"spender"`
	Value   *big.Int       `json:"value"`
	Log     types.Log      `json:"log"`
}

// GenericERC20Filterer provides token-agnostic ABI validation and event decoding.
type GenericERC20Filterer struct {
	abi          *abi.ABI
	tokenAddress common.Address
	transferID   common.Hash
	hasApproval  bool
	approvalID   common.Hash
}

// LoadERC20ABI loads and parses an ERC-20 ABI from a file path or inline JSON string.
// filePath takes priority if provided; otherwise inlineJSON is used.
func LoadERC20ABI(filePath string, inlineJSON string) (*abi.ABI, error) {
	var rawJSON string
	if filePath != "" {
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read ABI file %s: %w", filePath, err)
		}
		rawJSON = string(content)
	} else if inlineJSON != "" {
		rawJSON = inlineJSON
	} else {
		return nil, fmt.Errorf("neither ABI file path nor inline ABI JSON was provided")
	}

	trimmed := strings.TrimSpace(rawJSON)
	if trimmed == "" {
		return nil, fmt.Errorf("ABI content is empty")
	}

	parsedABI, err := abi.JSON(strings.NewReader(trimmed))
	if err != nil {
		return nil, fmt.Errorf("failed to parse ABI JSON: %w", err)
	}

	return &parsedABI, nil
}

// NewGenericERC20Filterer creates a new GenericERC20Filterer after validating that
// the ABI satisfies the minimum ERC-20 Transfer requirement.
func NewGenericERC20Filterer(tokenAddress common.Address, parsedABI *abi.ABI) (*GenericERC20Filterer, error) {
	if parsedABI == nil {
		return nil, fmt.Errorf("ABI is nil")
	}

	if (tokenAddress == common.Address{}) {
		return nil, fmt.Errorf("token address is the zero address")
	}

	// Validate Transfer event existence
	transferEvent, ok := parsedABI.Events["Transfer"]
	if !ok {
		return nil, fmt.Errorf("ABI does not contain required 'Transfer' event")
	}

	// Validate Transfer inputs: (address indexed from, address indexed to, uint256 value)
	if len(transferEvent.Inputs) < 3 {
		return nil, fmt.Errorf("Transfer event must have at least 3 inputs, got %d", len(transferEvent.Inputs))
	}
	if !transferEvent.Inputs[0].Indexed || transferEvent.Inputs[0].Type.T != abi.AddressTy {
		return nil, fmt.Errorf("Transfer event input 0 must be an indexed address")
	}
	if !transferEvent.Inputs[1].Indexed || transferEvent.Inputs[1].Type.T != abi.AddressTy {
		return nil, fmt.Errorf("Transfer event input 1 must be an indexed address")
	}
	nonIndexed := transferEvent.Inputs.NonIndexed()
	if len(nonIndexed) != 1 || nonIndexed[0].Type.T != abi.UintTy {
		return nil, fmt.Errorf("Transfer event must have exactly 1 non-indexed uint256 parameter")
	}

	filterer := &GenericERC20Filterer{
		abi:          parsedABI,
		tokenAddress: tokenAddress,
		transferID:   transferEvent.ID,
	}

	// Check for optional Approval event to prepare architecture
	if approvalEvent, ok := parsedABI.Events["Approval"]; ok {
		if len(approvalEvent.Inputs) >= 3 &&
			approvalEvent.Inputs[0].Indexed && approvalEvent.Inputs[0].Type.T == abi.AddressTy &&
			approvalEvent.Inputs[1].Indexed && approvalEvent.Inputs[1].Type.T == abi.AddressTy {
			filterer.hasApproval = true
			filterer.approvalID = approvalEvent.ID
		}
	}

	return filterer, nil
}

// TokenAddress returns the configured token contract address.
func (f *GenericERC20Filterer) TokenAddress() common.Address {
	return f.tokenAddress
}

// TransferTopic returns the keccak256 hash of the Transfer event signature.
func (f *GenericERC20Filterer) TransferTopic() common.Hash {
	return f.transferID
}

// ApprovalTopic returns the keccak256 hash of the Approval event signature if supported.
func (f *GenericERC20Filterer) ApprovalTopic() (common.Hash, bool) {
	return f.approvalID, f.hasApproval
}

// ABI returns the underlying parsed ABI.
func (f *GenericERC20Filterer) ABI() *abi.ABI {
	return f.abi
}

// IsTransferLog returns true if the log originates from the configured token and matches the Transfer topic.
func (f *GenericERC20Filterer) IsTransferLog(log types.Log) bool {
	if log.Address != f.tokenAddress {
		return false
	}
	if len(log.Topics) < 3 {
		return false
	}
	return log.Topics[0] == f.transferID
}

// ParseTransfer decodes an ERC-20 Transfer log into an ERC20TransferEvent.
func (f *GenericERC20Filterer) ParseTransfer(log types.Log) (*ERC20TransferEvent, error) {
	if !f.IsTransferLog(log) {
		return nil, fmt.Errorf("log at block %d, tx %s, index %d is not a valid Transfer log for token %s",
			log.BlockNumber, log.TxHash.Hex(), log.Index, f.tokenAddress.Hex())
	}

	from := common.BytesToAddress(log.Topics[1].Bytes())
	to := common.BytesToAddress(log.Topics[2].Bytes())

	transferEvent := f.abi.Events["Transfer"]
	unpacked, err := transferEvent.Inputs.NonIndexed().Unpack(log.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack Transfer value: %w", err)
	}

	if len(unpacked) == 0 {
		return nil, fmt.Errorf("no value unpacked from Transfer log data")
	}

	value, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("unpacked value is not *big.Int: %T", unpacked[0])
	}

	return &ERC20TransferEvent{
		Token: f.tokenAddress,
		From:  from,
		To:    to,
		Value: new(big.Int).Set(value),
		Log:   log,
	}, nil
}

// ParseApproval decodes an ERC-20 Approval log into an ERC20ApprovalEvent if Approval is configured.
func (f *GenericERC20Filterer) ParseApproval(log types.Log) (*ERC20ApprovalEvent, error) {
	if !f.hasApproval {
		return nil, fmt.Errorf("Approval event is not defined in the configured ABI")
	}
	if log.Address != f.tokenAddress || len(log.Topics) < 3 || log.Topics[0] != f.approvalID {
		return nil, fmt.Errorf("log is not a valid Approval log for token %s", f.tokenAddress.Hex())
	}

	owner := common.BytesToAddress(log.Topics[1].Bytes())
	spender := common.BytesToAddress(log.Topics[2].Bytes())

	approvalEvent := f.abi.Events["Approval"]
	unpacked, err := approvalEvent.Inputs.NonIndexed().Unpack(log.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack Approval value: %w", err)
	}

	if len(unpacked) == 0 {
		return nil, fmt.Errorf("no value unpacked from Approval log data")
	}

	value, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("unpacked value is not *big.Int: %T", unpacked[0])
	}

	return &ERC20ApprovalEvent{
		Token:   f.tokenAddress,
		Owner:   owner,
		Spender: spender,
		Value:   new(big.Int).Set(value),
		Log:     log,
	}, nil
}
