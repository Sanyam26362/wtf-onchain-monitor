package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"

	indexerABI "worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/persistence"
	"worldtradefuture/indexer/internal/reconciliation"
	"worldtradefuture/indexer/internal/repository"
)

func main() {
	// Parse command-line flags
	toBlockFlag := flag.Uint64("to-block", 0, "Target block to backfill to (overrides BACKFILL_TO_BLOCK)")
	fromBlockFlag := flag.Uint64("from-block", 0, "Override start block for historical backfill")
	batchSizeFlag := flag.Uint64("batch-size", 0, "Override block batch size")
	liveFlag := flag.Bool("live", false, "Run in continuous live monitoring mode")
	pollIntervalFlag := flag.Duration("poll-interval", 0, "Override live polling interval (e.g. 5s)")
	confirmationsFlag := flag.Uint64("confirmations", 0, "Override confirmation depth")
	streamFlag := flag.String("stream", "all", "Stream to process ('payroll', 'token', 'all', or 'reconcile')")
	maxRetriesFlag := flag.Int("max-retries", 0, "Override max RPC retries on transient/rate-limit error")
	initialBackoffFlag := flag.Duration("initial-backoff", 0, "Override initial RPC retry backoff duration (e.g. 1s)")
	reconcileFlag := flag.Bool("reconcile", false, "Run reconciliation check")
	reconTypeFlag := flag.String("recon-type", "payroll", "Reconciliation check type ('payroll', 'salary-claims', or 'all')")
	reconWindowFlag := flag.Uint64("recon-window", 0, "Override reconciliation block window size (e.g. 500)")
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

	if *confirmationsFlag > 0 {
		cfg.ConfirmationDepth = *confirmationsFlag
	}

	if *pollIntervalFlag > 0 {
		cfg.LivePollInterval = *pollIntervalFlag
	}

	if *maxRetriesFlag > 0 {
		cfg.RPCMaxRetries = *maxRetriesFlag
	}

	if *initialBackoffFlag > 0 {
		cfg.RPCInitialBackoff = *initialBackoffFlag
	}

	isLiveMode := *liveFlag || cfg.LiveMonitorEnabled

	fmt.Printf("Chain ID:                %d\n", cfg.ChainID)
	fmt.Printf("Confirmation Depth:      %d\n", cfg.ConfirmationDepth)
	fmt.Printf("Block Batch Size:        %d\n", batchSize)
	fmt.Printf("RPC Max Retries:         %d\n", cfg.RPCMaxRetries)
	fmt.Printf("RPC Initial Backoff:     %s\n", cfg.RPCInitialBackoff)
	if isLiveMode {
		fmt.Printf("Mode:                    LIVE MONITORING\n")
		fmt.Printf("Polling Interval:        %s\n", cfg.LivePollInterval)
	} else {
		fmt.Printf("Mode:                    HISTORICAL BACKFILL\n")
	}
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

	// Initialize MonthlyPayroll indexer service if configured
	var payrollService *indexer.Service
	if cfg.PayrollContractAddress != "" && common.IsHexAddress(cfg.PayrollContractAddress) && (*streamFlag == "all" || *streamFlag == "payroll") {
		payrollAddr := common.HexToAddress(cfg.PayrollContractAddress)
		var err error
		payrollService, err = indexer.New(client, payrollAddr, db)
		if err != nil {
			log.Fatalf("failed to initialize MonthlyPayroll indexer: %v", err)
		}
		payrollService.SetRetryPolicy(indexer.RetryPolicy{
			MaxRetries:     cfg.RPCMaxRetries,
			InitialBackoff: cfg.RPCInitialBackoff,
			MaxBackoff:     cfg.RPCMaxBackoff,
			BackoffFactor:  cfg.RPCBackoffFactor,
			Sleeper:        indexer.DefaultSleeper,
		})
	}

	// Initialize Generic ERC-20 Token indexer if configured
	var tokenIndexer *indexer.TokenIndexer
	if cfg.TokenAddress != "" && (*streamFlag == "all" || *streamFlag == "token") {
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

		tokenIndexer, err = indexer.NewTokenIndexer(
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
		tokenIndexer.SetRetryPolicy(indexer.RetryPolicy{
			MaxRetries:     cfg.RPCMaxRetries,
			InitialBackoff: cfg.RPCInitialBackoff,
			MaxBackoff:     cfg.RPCMaxBackoff,
			BackoffFactor:  cfg.RPCBackoffFactor,
			Sleeper:        indexer.DefaultSleeper,
		})
	}

	// Branch: Continuous Live Monitoring vs Historical Backfill
	if isLiveMode {
		fmt.Println("\n==================================================")
		fmt.Println("Starting continuous live monitoring...")
		fmt.Println("==================================================")

		liveCfg := indexer.LiveMonitorConfig{
			ChainID:                cfg.ChainID,
			ConfirmationDepth:      cfg.ConfirmationDepth,
			BatchSize:              batchSize,
			PollInterval:           cfg.LivePollInterval,
			PayrollContractAddress: common.HexToAddress(cfg.PayrollContractAddress),
			PayrollStreamID:        cfg.PayrollStreamID,
			PayrollStartBlock:      startBlock,
			TokenAddress:           common.HexToAddress(cfg.TokenAddress),
			TokenStreamID:          cfg.TokenStreamID,
			TokenStartBlock:        cfg.TokenStartBlock,
		}

		liveMonitor, err := indexer.NewLiveMonitor(liveCfg, client, payrollService, tokenIndexer, db)
		if err != nil {
			log.Fatalf("failed to initialize live monitor: %v", err)
		}

		if err := liveMonitor.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("live monitor error: %v", err)
		}

		fmt.Println("==================================================")
		fmt.Println("WTF Live Monitor shut down cleanly.")
		fmt.Println("==================================================")
		return
	}

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

	// Branch: Reconciliation Check
	if *reconcileFlag || *streamFlag == "reconcile" || *streamFlag == "reconciliation" {
		normReconType := strings.ToLower(strings.TrimSpace(*reconTypeFlag))
		switch normReconType {
		case "payroll", "payroll-funding", "funding":
			normReconType = "payroll"
		case "salary-claims", "salary-claim", "claims", "claim":
			normReconType = "salary-claims"
		case "all", "both":
			normReconType = "all"
		default:
			log.Fatalf("unsupported reconciliation type %q: must be 'payroll', 'salary-claims', or 'all'", *reconTypeFlag)
		}

		fmt.Println("\n==================================================")
		fmt.Printf("Running WTF Reconciliation Check (Type: %s)...\n", strings.ToUpper(normReconType))
		fmt.Println("==================================================")

		reconRepo := repository.NewReconciliationRepository(db.Pool())
		payrollRepo := repository.NewPayrollRepository(db.Pool())
		eventDecoder, err := decoder.New(common.HexToAddress(cfg.PayrollContractAddress))
		if err != nil {
			log.Fatalf("failed to initialize event decoder for reconciliation: %v", err)
		}

		retryPolicy := indexer.RetryPolicy{
			MaxRetries:     cfg.RPCMaxRetries,
			InitialBackoff: cfg.RPCInitialBackoff,
			MaxBackoff:     cfg.RPCMaxBackoff,
			BackoffFactor:  cfg.RPCBackoffFactor,
			Sleeper:        indexer.DefaultSleeper,
		}

		if normReconType == "payroll" || normReconType == "all" {
			reconCfg := reconciliation.PayrollReconcilerConfig{
				ChainID:           cfg.ChainID,
				ContractAddress:   common.HexToAddress(cfg.PayrollContractAddress),
				PayrollStreamID:   cfg.PayrollStreamID,
				ReconStreamID:     cfg.ReconciliationStreamID,
				ConfirmationDepth: cfg.ConfirmationDepth,
				BlockWindow:       cfg.ReconciliationBlockWindow,
				StartBlock:        startBlock,
			}

			reconciler, err := reconciliation.NewPayrollReconciler(
				reconCfg,
				client,
				eventDecoder,
				payrollRepo,
				reconRepo,
				db,
			)
			if err != nil {
				log.Fatalf("failed to initialize payroll reconciler: %v", err)
			}
			reconciler.SetRetryPolicy(retryPolicy)

			var res *reconciliation.ReconciliationResult
			if *reconWindowFlag > 0 {
				fmt.Printf("Reconciling payroll funding recent window of %d blocks...\n", *reconWindowFlag)
				res, err = reconciler.ReconcileRecentWindow(ctx, *reconWindowFlag)
			} else {
				fmt.Printf("Reconciling payroll funding range %d -> %d...\n", startBlock, targetBlock)
				res, err = reconciler.ReconcileRange(ctx, startBlock, targetBlock)
			}
			if err != nil {
				log.Fatalf("payroll reconciliation execution failed: %v", err)
			}
			printReconciliationSummary("Payroll Funding", res)
		}

		if normReconType == "salary-claims" || normReconType == "all" {
			claimReconCfg := reconciliation.SalaryClaimReconcilerConfig{
				ChainID:           cfg.ChainID,
				ContractAddress:   common.HexToAddress(cfg.PayrollContractAddress),
				PayrollStreamID:   cfg.PayrollStreamID,
				ReconStreamID:     cfg.ReconciliationSalaryClaimStreamID,
				ConfirmationDepth: cfg.ConfirmationDepth,
				BlockWindow:       cfg.ReconciliationBlockWindow,
				StartBlock:        startBlock,
			}

			claimReconciler, err := reconciliation.NewSalaryClaimReconciler(
				claimReconCfg,
				client,
				eventDecoder,
				payrollRepo,
				reconRepo,
				db,
			)
			if err != nil {
				log.Fatalf("failed to initialize salary claim reconciler: %v", err)
			}
			claimReconciler.SetRetryPolicy(retryPolicy)

			var res *reconciliation.ReconciliationResult
			if *reconWindowFlag > 0 {
				fmt.Printf("Reconciling salary claims recent window of %d blocks...\n", *reconWindowFlag)
				res, err = claimReconciler.ReconcileRecentWindow(ctx, *reconWindowFlag)
			} else {
				fmt.Printf("Reconciling salary claims range %d -> %d...\n", startBlock, targetBlock)
				res, err = claimReconciler.ReconcileRange(ctx, startBlock, targetBlock)
			}
			if err != nil {
				log.Fatalf("salary claim reconciliation execution failed: %v", err)
			}
			printReconciliationSummary("Salary Claim", res)
		}

		fmt.Println("==================================================")
		fmt.Println("WTF Reconciliation run complete.")
		fmt.Println("==================================================")
		return
	}

	// 2. Historical Backfill: Index MonthlyPayroll if configured
	if payrollService != nil {
		payrollAddr := common.HexToAddress(cfg.PayrollContractAddress)
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
	if tokenIndexer != nil {
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

func printReconciliationSummary(title string, res *reconciliation.ReconciliationResult) {
	if res == nil {
		return
	}
	fmt.Printf("\n--- %s Reconciliation Summary ---\n", title)
	fmt.Printf("Block Range Checked:     %d -> %d\n", res.FromBlock, res.ToBlock)
	fmt.Printf("Safe Target Block:       %d\n", res.SafeTarget)
	fmt.Printf("Indexer Checkpoint:      %d\n", res.IndexerCheckpoint)
	fmt.Printf("On-Chain Events Found:   %d\n", res.OnChainEventsCount)
	fmt.Printf("Database Records Found:  %d\n", res.DBRecordsCount)
	fmt.Printf("Mismatches Detected:     %d\n", res.MismatchesDetected)
	fmt.Printf("Exceptions Created:      %d\n", res.ExceptionsCreated)
	fmt.Printf("Exceptions Resolved:     %d\n", res.ExceptionsResolved)
	fmt.Printf("Exceptions Skipped:      %d (already open)\n", res.ExceptionsSkipped)
}
