package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"

	indexerABI "worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/persistence"
)

func main() {
	fmt.Println("==================================================")
	fmt.Println("WTF On-Chain Monitoring & Indexing Service")
	fmt.Println("==================================================")

	if err := godotenv.Load(); err != nil {
		log.Printf("info: .env file not loaded from cwd, using environment: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	fmt.Printf("Chain ID:                %d\n", cfg.ChainID)
	fmt.Printf("Confirmation Depth:      %d\n", cfg.ConfirmationDepth)
	fmt.Printf("Block Batch Size:        %d\n", cfg.BlockBatchSize)
	if cfg.PayrollContractAddress != "" {
		fmt.Printf("Payroll Contract:        %s\n", cfg.PayrollContractAddress)
		fmt.Printf("Payroll Start Block:     %d\n", cfg.StartBlock)
	}
	if cfg.TokenAddress != "" {
		fmt.Printf("Token Contract:          %s\n", cfg.TokenAddress)
		fmt.Printf("Token ABI Path:          %s\n", cfg.TokenABIPath)
		fmt.Printf("Token Start Block:       %d\n", cfg.TokenStartBlock)
		fmt.Printf("Token Stream ID:         %s\n", cfg.TokenStreamID)
	}
	fmt.Println("--------------------------------------------------")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	client, err := blockchain.NewClient(cfg.RPCURL)
	if err != nil {
		log.Fatalf("failed to connect to RPC: %v", err)
	}
	defer client.Close()

	db, err := persistence.NewPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()

	// 1. Run database migrations if migrations directory exists
	migrationsDir := "./migrations"
	if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
		migrationsDir = "../../migrations"
	}
	if _, err := os.Stat(migrationsDir); err == nil {
		if err := db.RunMigrations(ctx, migrationsDir); err != nil {
			log.Fatalf("failed to run migrations: %v", err)
		}
	}

	latestBlock, err := client.LatestBlock(ctx)
	if err != nil {
		log.Fatalf("failed to fetch latest block: %v", err)
	}
	fmt.Printf("Latest Sepolia Block:    %d\n", latestBlock)

	// 2. Index MonthlyPayroll if configured
	if cfg.PayrollContractAddress != "" && common.IsHexAddress(cfg.PayrollContractAddress) {
		payrollAddr := common.HexToAddress(cfg.PayrollContractAddress)
		payrollService, err := indexer.New(client, payrollAddr, db)
		if err != nil {
			log.Printf("warning: failed to initialize MonthlyPayroll indexer: %v", err)
		} else {
			fromBlock, toBlock, ok := indexer.NextRange(cfg.StartBlock, latestBlock, cfg.BlockBatchSize)
			if ok {
				fmt.Printf("\n[Stream: monthly_payroll] Indexing block range: %d -> %d\n", fromBlock, toBlock)
				events, err := payrollService.IndexRange(ctx, cfg.ChainID, payrollAddr, fromBlock, toBlock)
				if err != nil {
					log.Fatalf("payroll indexing error: %v", err)
				}
				fmt.Printf("[Stream: monthly_payroll] Indexed %d event(s)\n", len(events))
			}
		}
	}

	// 3. Index Generic ERC-20 Token Transfers if configured
	if cfg.TokenAddress != "" {
		if err := cfg.ValidateTokenConfig(); err != nil {
			log.Fatalf("token configuration invalid: %v", err)
		}

		tokenAddr := common.HexToAddress(cfg.TokenAddress)
		tokenFilterer, err := indexerABI.NewGenericERC20Filterer(tokenAddr, cfg.ParsedTokenABI)
		if err != nil {
			log.Fatalf("failed to create generic ERC-20 filterer: %v", err)
		}

		tokenDecoder, err := decoder.NewERC20Decoder(tokenFilterer)
		if err != nil {
			log.Fatalf("failed to create ERC-20 decoder: %v", err)
		}

		tokenIndexer, err := indexer.NewTokenIndexer(
			client,
			tokenDecoder,
			db,
			cfg.ChainID,
			cfg.TokenStartBlock,
			cfg.BlockBatchSize,
			cfg.ConfirmationDepth,
			cfg.TokenStreamID,
		)
		if err != nil {
			log.Fatalf("failed to create token indexer: %v", err)
		}

		fmt.Printf("\n[Stream: %s] Processing generic ERC-20 token: %s\n", cfg.TokenStreamID, cfg.TokenAddress)
		processed, fromBlock, toBlock, count, err := tokenIndexer.ProcessNextBatch(ctx)
		if err != nil {
			log.Fatalf("token indexing error: %v", err)
		}

		if processed {
			fmt.Printf("[Stream: %s] Successfully indexed %d Transfer(s) in block range %d -> %d\n",
				cfg.TokenStreamID, count, fromBlock, toBlock)
		} else {
			fmt.Printf("[Stream: %s] No new blocks to index (up to block %d)\n", cfg.TokenStreamID, toBlock)
		}
	}

	fmt.Println("==================================================")
	fmt.Println("WTF Indexer run complete.")
	fmt.Println("==================================================")
}
