package indexer

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/decoder"
)

// Service coordinates blockchain log retrieval and event decoding.
// Persistence and checkpointing will be added in later steps.
type Service struct {
	client  *blockchain.Client
	decoder *decoder.Decoder
}

func New(client *blockchain.Client, contractAddress common.Address) (*Service, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client is nil")
	}

	eventDecoder, err := decoder.New(contractAddress)
	if err != nil {
		return nil, fmt.Errorf("create event decoder: %w", err)
	}

	return &Service{
		client:  client,
		decoder: eventDecoder,
	}, nil
}

// IndexRange fetches and decodes all MonthlyPayroll logs in a bounded block range.
// It does not persist anything yet; this step proves the RPC -> logs -> decoder path.
func (s *Service) IndexRange(ctx context.Context, contractAddress common.Address, fromBlock, toBlock uint64) ([]*decoder.DecodedEvent, error) {
	if fromBlock > toBlock {
		return nil, fmt.Errorf("invalid block range: from %d is greater than to %d", fromBlock, toBlock)
	}

	logs, err := s.client.GetLogs(ctx, contractAddress, fromBlock, toBlock)
	if err != nil {
		return nil, err
	}

	events := make([]*decoder.DecodedEvent, 0, len(logs))
	for _, log := range logs {
		decoded, err := s.decoder.Decode(log)
		if err != nil {
			return nil, fmt.Errorf("decode log at block %d, tx %s, log index %d: %w", log.BlockNumber, log.TxHash.Hex(), log.Index, err)
		}
		events = append(events, decoded)
	}

	return events, nil
}

// NextRange returns the next bounded block range for a configured step size.
func NextRange(fromBlock, latestBlock, stepSize uint64) (uint64, uint64, bool) {
	if fromBlock > latestBlock || stepSize == 0 {
		return 0, 0, false
	}

	toBlock := new(big.Int).SetUint64(fromBlock)
	toBlock.Add(toBlock, new(big.Int).SetUint64(stepSize-1))
	latest := new(big.Int).SetUint64(latestBlock)
	if toBlock.Cmp(latest) > 0 {
		toBlock.Set(latest)
	}

	return fromBlock, toBlock.Uint64(), true
}

// Ensure the imported log type remains part of this package's public design when
// the implementation grows to persistence and receipt processing.
var _ types.Log
