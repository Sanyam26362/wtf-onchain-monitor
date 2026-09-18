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
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/decoder"
	"worldtradefuture/indexer/internal/indexer"
	"worldtradefuture/indexer/internal/persistence"
	"worldtradefuture/indexer/internal/reconciliation"
	"worldtradefuture/indexer/internal/repository"
)

func main() {
	fromBlockFlag := flag.Uint64("from-block", 0, "Start block for reconciliation range")
	toBlockFlag := flag.Uint64("to-block", 0, "Target block for reconciliation range")
	windowFlag := flag.Uint64("window", 0, "Reconcile recent block window (e.g. 500)")
	batchSizeFlag := flag.Uint64("batch-size", 0, "Batch size for sequential sweep")
	loopFlag := flag.Bool("loop", false, "Run continuous reconciliation loop")
	intervalFlag := flag.Duration("interval", 0, "Override loop reconciliation interval")
	flag.Parse()

	// Configure structured logger
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	fmt.Println("==================================================")
	fmt.Println("WTF Reconciliation Worker — Payroll Funding Check")
	fmt.Println("==================================================")

	if err := godotenv.Load(); err != nil {
		log.Printf("info: .env file not loaded from cwd, using environment: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	if cfg.PayrollContractAddress == "" {
		log.Fatalf("PAYROLL_CONTRACT_ADDRESS is required for payroll reconciliation")
	}

	interval := cfg.ReconciliationInterval
	if *intervalFlag > 0 {
		interval = *intervalFlag
	}

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

	reconRepo := repository.NewReconciliationRepository(db.Pool())
	payrollRepo := repository.NewPayrollRepository(db.Pool())

	contractAddr := common.HexToAddress(cfg.PayrollContractAddress)
	eventDecoder, err := decoder.New(contractAddr)
	if err != nil {
		log.Fatalf("failed to initialize event decoder: %v", err)
	}

	reconCfg := reconciliation.PayrollReconcilerConfig{
		ChainID:           cfg.ChainID,
		ContractAddress:   contractAddr,
		PayrollStreamID:   cfg.PayrollStreamID,
		ReconStreamID:     cfg.ReconciliationStreamID,
		ConfirmationDepth: cfg.ConfirmationDepth,
		BlockWindow:       cfg.ReconciliationBlockWindow,
		StartBlock:        cfg.StartBlock,
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
		log.Fatalf("failed to initialize reconciler: %v", err)
	}
	reconciler.SetRetryPolicy(indexer.RetryPolicy{
		MaxRetries:     cfg.RPCMaxRetries,
		InitialBackoff: cfg.RPCInitialBackoff,
		MaxBackoff:     cfg.RPCMaxBackoff,
		BackoffFactor:  cfg.RPCBackoffFactor,
		Sleeper:        indexer.DefaultSleeper,
	})

	fmt.Printf("Chain ID:                %d\n", cfg.ChainID)
	fmt.Printf("Payroll Contract:        %s\n", cfg.PayrollContractAddress)
	fmt.Printf("Stream ID:               %s\n", cfg.ReconciliationStreamID)
	fmt.Printf("Confirmation Depth:      %d\n", cfg.ConfirmationDepth)
	if *loopFlag {
		fmt.Printf("Mode:                    CONTINUOUS RECONCILIATION LOOP (interval %s)\n", interval)
	} else if *windowFlag > 0 {
		fmt.Printf("Mode:                    RECENT WINDOW (%d blocks)\n", *windowFlag)
	} else if *fromBlockFlag > 0 && *toBlockFlag > 0 {
		fmt.Printf("Mode:                    RANGE (%d -> %d)\n", *fromBlockFlag, *toBlockFlag)
	} else {
		fmt.Printf("Mode:                    SEQUENTIAL SWEEP\n")
	}
	fmt.Println("--------------------------------------------------")

	executePass := func() error {
		var res *reconciliation.ReconciliationResult
		var pErr error

		if *windowFlag > 0 {
			res, pErr = reconciler.ReconcileRecentWindow(ctx, *windowFlag)
		} else if *fromBlockFlag > 0 && *toBlockFlag > 0 {
			res, pErr = reconciler.ReconcileRange(ctx, *fromBlockFlag, *toBlockFlag)
		} else {
			batchSize := cfg.BlockBatchSize
			if *batchSizeFlag > 0 {
				batchSize = *batchSizeFlag
			}
			res, pErr = reconciler.ReconcileNextBatch(ctx, batchSize)
		}

		if pErr != nil {
			return pErr
		}

		printSummary(res)
		return nil
	}

	if *loopFlag {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Run immediate first pass
		if err := executePass(); err != nil {
			slog.Error("reconciliation pass error", "error", err)
		}

		for {
			select {
			case <-ctx.Done():
				fmt.Println("\nReconciliation worker shutting down cleanly.")
				return
			case <-ticker.C:
				if err := executePass(); err != nil {
					slog.Error("reconciliation pass error", "error", err)
				}
			}
		}
	} else {
		if err := executePass(); err != nil {
			log.Fatalf("reconciliation execution failed: %v", err)
		}
		fmt.Println("==================================================")
		fmt.Println("WTF Reconciliation run completed.")
		fmt.Println("==================================================")
	}
}

func printSummary(res *reconciliation.ReconciliationResult) {
	if res == nil {
		return
	}
	fmt.Printf("\n--- Reconciliation Summary ---\n")
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
