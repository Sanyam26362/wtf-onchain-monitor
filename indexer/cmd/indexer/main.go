package main

import (
	"context"
	"fmt"
	"log"

	"github.com/joho/godotenv"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/config"
)

func main() {
	// Load environment variables from .env
	if err := godotenv.Load(); err != nil {
		log.Fatal("failed to load .env file")
	}

	// Load indexer configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("WTF On-Chain Indexer starting...")
	fmt.Printf("Chain ID: %d\n", cfg.ChainID)
	fmt.Printf("Payroll Contract: %s\n", cfg.PayrollContractAddress)
	fmt.Printf("Start Block: %d\n", cfg.StartBlock)

	// Connect to the blockchain RPC
	client, err := blockchain.NewClient(cfg.RPCURL)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Create a context for the RPC request
	ctx := context.Background()

	// Get the latest Sepolia block
	latestBlock, err := client.LatestBlock(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Latest Sepolia Block: %d\n", latestBlock)
}