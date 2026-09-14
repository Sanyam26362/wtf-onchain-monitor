package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/persistence"
)

func main() {
	fmt.Println("WTF On-Chain Indexer starting...")

	if err := godotenv.Load(); err != nil {
		log.Printf("warning: .env file not loaded: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Chain ID: %d\n", cfg.ChainID)
	fmt.Printf("Payroll Contract: %s\n", cfg.PayrollContractAddress)
	fmt.Printf("Start Block: %d\n", cfg.StartBlock)
	fmt.Printf("Block Batch Size: %d\n", cfg.BlockBatchSize)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	contractAddress := common.HexToAddress(
		cfg.PayrollContractAddress,
	)

	client, err := blockchain.NewClient(cfg.RPCURL)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	db, err := persistence.NewPostgres(
		ctx,
		cfg.DatabaseURL,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	service, err := indexer.New(
		client,
		contractAddress,
		db,
	)
	if err != nil {
		log.Fatal(err)
	}

	latestBlock, err := client.LatestBlock(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"Latest Sepolia Block: %d\n",
		latestBlock,
	)

	fromBlock, toBlock, ok := indexer.NextRange(
		cfg.StartBlock,
		latestBlock,
		cfg.BlockBatchSize,
	)

	if !ok {
		fmt.Println("No blocks to index.")
		return
	}

	fmt.Printf(
		"Indexing block range: %d -> %d\n",
		fromBlock,
		toBlock,
	)

	events, err := service.IndexRange(
		ctx,
		cfg.ChainID,
		contractAddress,
		fromBlock,
		toBlock,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"Found %d MonthlyPayroll event(s)\n",
		len(events),
	)

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
