package blockchain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

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

func (c *Client) LatestBlock(ctx context.Context) (uint64, error) {
	blockNumber, err := c.eth.BlockNumber(ctx)
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

	logs, err := c.eth.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch logs: %w", err)
	}

	return logs, nil
}

func (c *Client) TransactionMetadata(
	ctx context.Context,
	txHash common.Hash,
) (*TransactionMetadata, error) {

	tx, pending, err := c.eth.TransactionByHash(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transaction %s: %w", txHash.Hex(), err)
	}

	if pending {
		return nil, fmt.Errorf("transaction %s is still pending", txHash.Hex())
	}

	receipt, err := c.eth.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch receipt %s: %w", txHash.Hex(), err)
	}

	sender, err := c.eth.TransactionSender(
		ctx,
		tx,
		receipt.BlockHash,
		receipt.TransactionIndex,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to determine sender for %s: %w", txHash.Hex(), err)
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

	block, err := c.eth.BlockByNumber(
		ctx,
		new(big.Int).SetUint64(blockNumber),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch block %d: %w", blockNumber, err)
	}

	return block.Time(), nil
}

func (c *Client) Close() {
	c.eth.Close()
}
