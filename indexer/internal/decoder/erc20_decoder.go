package decoder

import (
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	indexerABI "worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/models"
)

// ERC20Decoder decodes raw Ethereum logs into normalized models.TokenTransfer records.
type ERC20Decoder struct {
	filterer *indexerABI.GenericERC20Filterer
}

// NewERC20Decoder creates a new ERC20Decoder with a GenericERC20Filterer.
func NewERC20Decoder(filterer *indexerABI.GenericERC20Filterer) (*ERC20Decoder, error) {
	if filterer == nil {
		return nil, fmt.Errorf("generic ERC20 filterer is nil")
	}
	return &ERC20Decoder{filterer: filterer}, nil
}

// Filterer returns the underlying filterer.
func (d *ERC20Decoder) Filterer() *indexerABI.GenericERC20Filterer {
	return d.filterer
}

// TokenAddress returns the configured token address.
func (d *ERC20Decoder) TokenAddress() common.Address {
	return d.filterer.TokenAddress()
}

// TransferTopic returns the Transfer event topic keccak256 hash.
func (d *ERC20Decoder) TransferTopic() common.Hash {
	return d.filterer.TransferTopic()
}

// DecodeTransfer decodes an Ethereum log into a normalized TokenTransfer model.
func (d *ERC20Decoder) DecodeTransfer(
	chainID int64,
	log types.Log,
	blockTimestamp time.Time,
) (*models.TokenTransfer, error) {
	parsed, err := d.filterer.ParseTransfer(log)
	if err != nil {
		return nil, fmt.Errorf("decode transfer at block %d tx %s log %d: %w",
			log.BlockNumber, log.TxHash.Hex(), log.Index, err)
	}

	return &models.TokenTransfer{
		ChainID:        chainID,
		Token:          parsed.Token,
		FromAddress:    parsed.From,
		ToAddress:      parsed.To,
		Amount:         parsed.Value,
		TxHash:         log.TxHash,
		BlockNumber:    log.BlockNumber,
		BlockTimestamp: blockTimestamp,
		LogIndex:       log.Index,
		Removed:        log.Removed,
	}, nil
}
