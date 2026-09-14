package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/indexer"
)

func main() {
	// Load environment variables from .env.
	if err := godotenv.Load(); err != nil {
		log.Fatal("failed to load .env file")
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	if !common.IsHexAddress(cfg.PayrollContractAddress) {
		log.Fatalf("invalid PAYROLL_CONTRACT_ADDRESS: %s", cfg.PayrollContractAddress)
	}
	contractAddress := common.HexToAddress(cfg.PayrollContractAddress)

	fmt.Println("WTF On-Chain Indexer starting...")
	fmt.Printf("Chain ID: %d\n", cfg.ChainID)
	fmt.Printf("Payroll Contract: %s\n", contractAddress.Hex())
	fmt.Printf("Start Block: %d\n", cfg.StartBlock)
	fmt.Printf("Block Batch Size: %d\n", cfg.BlockBatchSize)

	client, err := blockchain.NewClient(cfg.RPCURL)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	latestBlock, err := client.LatestBlock(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Latest Sepolia Block: %d\n", latestBlock)

	fromBlock, toBlock, ok := indexer.NextRange(cfg.StartBlock, latestBlock, cfg.BlockBatchSize)
	if !ok {
		log.Fatal("start block is after latest block or batch size is invalid")
	}

	service, err := indexer.New(client, contractAddress)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Indexing block range: %d -> %d\n", fromBlock, toBlock)

	events, err := service.IndexRange(ctx, contractAddress, fromBlock, toBlock)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d MonthlyPayroll event(s)\n", len(events))
	for _, event := range events {
		fmt.Printf(
			"Event=%s Block=%d TxHash=%s TxIndex=%d LogIndex=%d Removed=%t Data=%+v\n",
			event.Type,
			event.Log.BlockNumber,
			event.Log.TxHash.Hex(),
			event.Log.TxIndex,
			event.Log.Index,
			event.Log.Removed,
			event.Data,
		)
	}
}