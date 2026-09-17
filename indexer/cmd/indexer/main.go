package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

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
	// Parse command-line flags
	toBlockFlag := flag.Uint64("to-block", 0, "Target block to backfill to (overrides BACKFILL_TO_BLOCK)")
	fromBlockFlag := flag.Uint64("from-block", 0, "Override start block for historical backfill")
	batchSizeFlag := flag.Uint64("batch-size", 0, "Override block batch size")
	flag.Parse()

	// Initialize structured logger
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	fmt.Println("==================================================")
	fmt.Println("WTF On-Chain Monitoring & Historical Backfill")
	fmt.Println("==================================================")

	if err := godotenv.Load(); err != nil {
		log.Printf("info: .env file not loaded from cwd, using environment: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	// Resolve backfill parameters
	startBlock := cfg.StartBlock
	if *fromBlockFlag > 0 {
		startBlock = *fromBlockFlag
	}

	batchSize := cfg.BlockBatchSize
	if *batchSizeFlag > 0 {
		batchSize = *batchSizeFlag
	}

	fmt.Printf("Chain ID:                %d\n", cfg.ChainID)
	fmt.Printf("Confirmation Depth:      %d\n", cfg.ConfirmationDepth)
	fmt.Printf("Block Batch Size:        %d\n", batchSize)
	if cfg.PayrollContractAddress != "" {
		fmt.Printf("Payroll Contract:        %s\n", cfg.PayrollContractAddress)
		fmt.Printf("Payroll Start Block:     %d\n", startBlock)
		fmt.Printf("Payroll Stream ID:       %s\n", cfg.PayrollStreamID)
	}
	if cfg.TokenAddress != "" {
		fmt.Printf("Token Contract:          %s\n", cfg.TokenAddress)
		fmt.Printf("Token ABI Path:          %s\n", cfg.TokenABIPath)
		fmt.Printf("Token Start Block:       %d\n", cfg.TokenStartBlock)
		fmt.Printf("Token Stream ID:         %s\n", cfg.TokenStreamID)
	}
	fmt.Println("--------------------------------------------------")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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

	// Determine backfill target block
	var targetBlock uint64
	if *toBlockFlag > 0 {
		targetBlock = *toBlockFlag
	} else if cfg.BackfillToBlock != nil {
		targetBlock = *cfg.BackfillToBlock
	} else {
		targetBlock = latestBlock
		if cfg.ConfirmationDepth > 0 && latestBlock >= cfg.ConfirmationDepth {
			targetBlock = latestBlock - cfg.ConfirmationDepth
		}
	}
	fmt.Printf("Backfill Target Block:   %d\n", targetBlock)

	// 2. Historical Backfill: Index MonthlyPayroll if configured
	if cfg.PayrollContractAddress != "" && common.IsHexAddress(cfg.PayrollContractAddress) {
		payrollAddr := common.HexToAddress(cfg.PayrollContractAddress)
		payrollService, err := indexer.New(client, payrollAddr, db)
		if err != nil {
			log.Fatalf("failed to initialize MonthlyPayroll indexer: %v", err)
		}

		opts := indexer.BackfillOptions{
			ChainID:         cfg.ChainID,
			ContractAddress: payrollAddr,
			StartBlock:      startBlock,
			TargetBlock:     targetBlock,
			BatchSize:       batchSize,
			StreamID:        cfg.PayrollStreamID,
		}

		fmt.Printf("\n[Stream: %s] Starting historical backfill\n", opts.StreamID)
		lastIndexed, err := payrollService.RunBackfill(ctx, opts)
		if err != nil {
			log.Fatalf("payroll indexing error: %v", err)
		}
		fmt.Printf("[Stream: %s] Successfully processed up to block %d\n", opts.StreamID, lastIndexed)
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
			batchSize,
			cfg.ConfirmationDepth,
			cfg.TokenStreamID,
		)
		if err != nil {
			log.Fatalf("failed to create token indexer: %v", err)
		}

		tokenTargetBlock := targetBlock
		if targetBlock < cfg.TokenStartBlock {
			// If targetBlock was explicitly set lower than token deployment (e.g. for payroll testing), use safeBlock for token
			var safeBlock uint64 = latestBlock
			if cfg.ConfirmationDepth > 0 && latestBlock >= cfg.ConfirmationDepth {
				safeBlock = latestBlock - cfg.ConfirmationDepth
			}
			tokenTargetBlock = safeBlock
		}

		fmt.Printf("\n[Stream: %s] Processing generic ERC-20 token: %s (target block: %d)\n", cfg.TokenStreamID, cfg.TokenAddress, tokenTargetBlock)
		lastIndexedToken, totalTransfers, err := tokenIndexer.RunBackfill(ctx, tokenTargetBlock)
		if err != nil {
			log.Fatalf("token indexing error: %v", err)
		}

		fmt.Printf("[Stream: %s] Successfully indexed %d Transfer(s) up to block %d\n",
			cfg.TokenStreamID, totalTransfers, lastIndexedToken)
	}

	fmt.Println("==================================================")
	fmt.Println("WTF Indexer run complete.")
	fmt.Println("==================================================")
}
