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

func (c *Client) Close() {
	c.eth.Close()
}