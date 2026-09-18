package blockchain

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// BlockchainClient defines the interface for interacting with the blockchain.
type BlockchainClient interface {
	LatestBlock(ctx context.Context) (uint64, error)
	GetLogs(ctx context.Context, contractAddress common.Address, fromBlock uint64, toBlock uint64) ([]types.Log, error)
	GetTokenLogs(ctx context.Context, tokenAddress common.Address, topic common.Hash, fromBlock uint64, toBlock uint64) ([]types.Log, error)
	TransactionMetadata(ctx context.Context, txHash common.Hash) (*TransactionMetadata, error)
	BlockTimestamp(ctx context.Context, blockNumber uint64) (uint64, error)
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	Close()
}

type Client struct {
	eth *ethclient.Client
}

type TransactionMetadata struct {
	Hash        common.Hash
	BlockNumber uint64
	Sender      common.Address
	Recipient   *common.Address
	GasUsed     uint64
	Status      uint64
}

func NewClient(rpcURL string) (*Client, error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC: %w", err)
	}

	return &Client{
		eth: client,
	}, nil
}

// NewFromEthClient wraps an existing *ethclient.Client.
func NewFromEthClient(eth *ethclient.Client) *Client {
	return &Client{eth: eth}
}

func (c *Client) LatestBlock(ctx context.Context) (uint64, error) {
	var blockNumber uint64
	err := retry(ctx, 3, 300*time.Millisecond, func() error {
		var qErr error
		blockNumber, qErr = c.eth.BlockNumber(ctx)
		return qErr
	})
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block: %w", err)
	}

	return blockNumber, nil
}

func (c *Client) GetLogs(
	ctx context.Context,
	contractAddress common.Address,
	fromBlock uint64,
	toBlock uint64,
) ([]types.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{contractAddress},
	}

	var logs []types.Log
	err := retry(ctx, 3, 300*time.Millisecond, func() error {
		var qErr error
		logs, qErr = c.eth.FilterLogs(ctx, query)
		return qErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch logs for %s: %w", contractAddress.Hex(), err)
	}

	return logs, nil
}

// GetTokenLogs fetches logs for a specific token address and event topic in [fromBlock, toBlock].
// This ensures that logs are constrained to the configured token and Transfer topic only.
func (c *Client) GetTokenLogs(
	ctx context.Context,
	tokenAddress common.Address,
	topic common.Hash,
	fromBlock uint64,
	toBlock uint64,
) ([]types.Log, error) {
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Addresses: []common.Address{tokenAddress},
		Topics:    [][]common.Hash{{topic}},
	}

	var logs []types.Log
	err := retry(ctx, 3, 300*time.Millisecond, func() error {
		var qErr error
		logs, qErr = c.eth.FilterLogs(ctx, query)
		return qErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch token logs for %s in blocks [%d, %d]: %w",
			tokenAddress.Hex(), fromBlock, toBlock, err)
	}

	return logs, nil
}

func (c *Client) TransactionMetadata(
	ctx context.Context,
	txHash common.Hash,
) (*TransactionMetadata, error) {
	var (
		tx      *types.Transaction
		pending bool
		receipt *types.Receipt
		sender  common.Address
	)

	err := retry(ctx, 3, 300*time.Millisecond, func() error {
		var qErr error
		tx, pending, qErr = c.eth.TransactionByHash(ctx, txHash)
		if qErr != nil {
			return qErr
		}
		if pending {
			return fmt.Errorf("transaction %s is still pending", txHash.Hex())
		}
		receipt, qErr = c.eth.TransactionReceipt(ctx, txHash)
		if qErr != nil {
			return qErr
		}
		sender, qErr = c.eth.TransactionSender(ctx, tx, receipt.BlockHash, receipt.TransactionIndex)
		return qErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transaction metadata for %s: %w", txHash.Hex(), err)
	}

	var recipient *common.Address
	if tx.To() != nil {
		to := *tx.To()
		recipient = &to
	}

	return &TransactionMetadata{
		Hash:        txHash,
		BlockNumber: receipt.BlockNumber.Uint64(),
		Sender:      sender,
		Recipient:   recipient,
		GasUsed:     receipt.GasUsed,
		Status:      receipt.Status,
	}, nil
}

func (c *Client) BlockTimestamp(
	ctx context.Context,
	blockNumber uint64,
) (uint64, error) {
	var timestamp uint64
	err := retry(ctx, 3, 300*time.Millisecond, func() error {
		block, qErr := c.eth.BlockByNumber(ctx, new(big.Int).SetUint64(blockNumber))
		if qErr != nil {
			return qErr
		}
		timestamp = block.Time()
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("failed to fetch block %d: %w", blockNumber, err)
	}

	return timestamp, nil
}

func (c *Client) CallContract(
	ctx context.Context,
	msg ethereum.CallMsg,
	blockNumber *big.Int,
) ([]byte, error) {
	var result []byte
	err := retry(ctx, 3, 300*time.Millisecond, func() error {
		var qErr error
		result, qErr = c.eth.CallContract(ctx, msg, blockNumber)
		return qErr
	})
	if err != nil {
		toAddr := "unknown"
		if msg.To != nil {
			toAddr = msg.To.Hex()
		}
		return nil, fmt.Errorf("failed to call contract %s: %w", toAddr, err)
	}

	return result, nil
}

func (c *Client) Close() {
	if c.eth != nil {
		c.eth.Close()
	}
}

// retry executes op up to maxAttempts with exponential backoff and rate limit handling.
func retry(ctx context.Context, maxAttempts int, initialBackoff time.Duration, op func() error) error {
	var err error
	backoff := initialBackoff
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = op()
		if err == nil {
			return nil
		}
		// If 429 Too Many Requests, increase backoff
		if strings.Contains(err.Error(), "429") || strings.Contains(err.Error(), "Too Many Requests") {
			backoff = time.Duration(attempt) * 1200 * time.Millisecond
		}
		if attempt < maxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
	}
	return err
}
