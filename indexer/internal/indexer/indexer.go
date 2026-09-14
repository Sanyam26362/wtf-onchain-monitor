package indexer

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/persistence"
)

type Service struct {
	client      *blockchain.Client
	decoder     *decoder.Decoder
	persistence *persistence.Postgres
}

func New(
	client *blockchain.Client,
	contractAddress common.Address,
	db *persistence.Postgres,
) (*Service, error) {
	if client == nil {
		return nil, fmt.Errorf("blockchain client is nil")
	}

	if db == nil {
		return nil, fmt.Errorf("persistence layer is nil")
	}

	eventDecoder, err := decoder.New(contractAddress)
	if err != nil {
		return nil, fmt.Errorf("create event decoder: %w", err)
	}

	return &Service{
		client:      client,
		decoder:     eventDecoder,
		persistence: db,
	}, nil
}

func (s *Service) IndexRange(
	ctx context.Context,
	chainID int64,
	contractAddress common.Address,
	fromBlock uint64,
	toBlock uint64,
) ([]*decoder.DecodedEvent, error) {

	if fromBlock > toBlock {
		return nil, fmt.Errorf(
			"invalid block range: from %d is greater than %d",
			fromBlock,
			toBlock,
		)
	}

	logs, err := s.client.GetLogs(
		ctx,
		contractAddress,
		fromBlock,
		toBlock,
	)
	if err != nil {
		return nil, err
	}

	events := make(
		[]*decoder.DecodedEvent,
		0,
		len(logs),
	)

	for _, log := range logs {
		decoded, err := s.decoder.Decode(log)
		if err != nil {
			return nil, fmt.Errorf(
				"decode log at block %d, tx %s, log index %d: %w",
				log.BlockNumber,
				log.TxHash.Hex(),
				log.Index,
				err,
			)
		}

		events = append(events, decoded)

		metadata, err := s.client.TransactionMetadata(
			ctx,
			log.TxHash,
		)
		if err != nil {
			return nil, err
		}

		blockTimestamp, err := s.client.BlockTimestamp(
			ctx,
			log.BlockNumber,
		)
		if err != nil {
			return nil, err
		}

		if err := s.persistence.SaveTransaction(
			ctx,
			chainID,
			metadata,
		); err != nil {
			return nil, err
		}

		if err := s.persistence.SaveChainEvent(
			ctx,
			chainID,
			contractAddress,
			blockTimestamp,
			decoded,
		); err != nil {
			return nil, err
		}
	}

	return events, nil
}

func NextRange(
	fromBlock uint64,
	latestBlock uint64,
	stepSize uint64,
) (uint64, uint64, bool) {

	if fromBlock > latestBlock || stepSize == 0 {
		return 0, 0, false
	}

	toBlock := fromBlock + stepSize - 1

	if toBlock < fromBlock || toBlock > latestBlock {
		toBlock = latestBlock
	}

	return fromBlock, toBlock, true
}
