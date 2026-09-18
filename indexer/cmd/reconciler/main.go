package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
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
	typeFlag := flag.String("type", "payroll", "Reconciliation type: 'payroll' (or 'payroll-funding'), 'salary-claims' (or 'salary-claim'), 'token-transfers' (or 'tokens'), 'balance' (or 'token-balance'), or 'all'")
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

	normType := strings.ToLower(strings.TrimSpace(*typeFlag))
	switch normType {
	case "payroll", "payroll-funding", "funding":
		normType = "payroll"
	case "salary-claims", "salary-claim", "claims", "claim":
		normType = "salary-claims"
	case "token-transfers", "token-transfer", "tokens", "token":
		normType = "token-transfers"
	case "balance", "balances", "token-balance", "token-balances", "contract-balance":
		normType = "balance"
	case "all", "both":
		normType = "all"
	default:
		log.Fatalf("unsupported reconciliation type %q: must be 'payroll', 'salary-claims', 'token-transfers', 'balance', or 'all'", *typeFlag)
	}

	fmt.Println("==================================================")
	fmt.Printf("WTF Reconciliation Worker — Type: %s\n", strings.ToUpper(normType))
	fmt.Println("==================================================")

	if err := godotenv.Load(); err != nil {
		log.Printf("info: .env file not loaded from cwd, using environment: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	if (normType == "payroll" || normType == "salary-claims" || normType == "balance" || normType == "all") && cfg.PayrollContractAddress == "" {
		log.Fatalf("PAYROLL_CONTRACT_ADDRESS is required for payroll/balance reconciliation")
	}
	if (normType == "token-transfers" || normType == "balance" || normType == "all") && cfg.TokenAddress == "" {
		log.Fatalf("TOKEN_ADDRESS is required for token transfer/balance reconciliation")
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
	tokensRepo := repository.NewTokensRepository(db.Pool())

	retryPolicy := indexer.RetryPolicy{
		MaxRetries:     cfg.RPCMaxRetries,
		InitialBackoff: cfg.RPCInitialBackoff,
		MaxBackoff:     cfg.RPCMaxBackoff,
		BackoffFactor:  cfg.RPCBackoffFactor,
		Sleeper:        indexer.DefaultSleeper,
	}

	var payrollReconciler *reconciliation.PayrollReconciler
	var salaryClaimReconciler *reconciliation.SalaryClaimReconciler

	if normType == "payroll" || normType == "salary-claims" || normType == "all" {
		contractAddr := common.HexToAddress(cfg.PayrollContractAddress)
		eventDecoder, decErr := decoder.New(contractAddr)
		if decErr != nil {
			log.Fatalf("failed to initialize payroll event decoder: %v", decErr)
		}

		if normType == "payroll" || normType == "all" {
			reconCfg := reconciliation.PayrollReconcilerConfig{
				ChainID:           cfg.ChainID,
				ContractAddress:   contractAddr,
				PayrollStreamID:   cfg.PayrollStreamID,
				ReconStreamID:     cfg.ReconciliationStreamID,
				ConfirmationDepth: cfg.ConfirmationDepth,
				BlockWindow:       cfg.ReconciliationBlockWindow,
				StartBlock:        cfg.StartBlock,
			}

			payrollReconciler, err = reconciliation.NewPayrollReconciler(
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
			payrollReconciler.SetRetryPolicy(retryPolicy)
		}

		if normType == "salary-claims" || normType == "all" {
			claimReconCfg := reconciliation.SalaryClaimReconcilerConfig{
				ChainID:           cfg.ChainID,
				ContractAddress:   contractAddr,
				PayrollStreamID:   cfg.PayrollStreamID,
				ReconStreamID:     cfg.ReconciliationSalaryClaimStreamID,
				ConfirmationDepth: cfg.ConfirmationDepth,
				BlockWindow:       cfg.ReconciliationBlockWindow,
				StartBlock:        cfg.StartBlock,
			}

			salaryClaimReconciler, err = reconciliation.NewSalaryClaimReconciler(
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
			salaryClaimReconciler.SetRetryPolicy(retryPolicy)
		}
	}

	var tokenReconciler *reconciliation.TokenTransferReconciler
	if normType == "token-transfers" || normType == "all" {
		if err := cfg.ValidateTokenConfig(); err != nil {
			log.Fatalf("invalid token configuration for reconciliation: %v", err)
		}

		tokenAddr := common.HexToAddress(cfg.TokenAddress)
		tokenFilterer, fErr := indexerABI.NewGenericERC20Filterer(tokenAddr, cfg.ParsedTokenABI)
		if fErr != nil {
			log.Fatalf("failed to initialize token filterer: %v", fErr)
		}
		erc20Decoder, dErr := decoder.NewERC20Decoder(tokenFilterer)
		if dErr != nil {
			log.Fatalf("failed to initialize ERC-20 decoder: %v", dErr)
		}

		tokenReconCfg := reconciliation.TokenTransferReconcilerConfig{
			ChainID:           cfg.ChainID,
			TokenAddress:      tokenAddr,
			TokenStreamID:     cfg.TokenStreamID,
			ReconStreamID:     cfg.ReconciliationTokenStreamID,
			ConfirmationDepth: cfg.ConfirmationDepth,
			BlockWindow:       cfg.ReconciliationBlockWindow,
			StartBlock:        cfg.TokenStartBlock,
		}

		tokenReconciler, err = reconciliation.NewTokenTransferReconciler(
			tokenReconCfg,
			client,
			erc20Decoder,
			tokensRepo,
			reconRepo,
			db,
		)
		if err != nil {
			log.Fatalf("failed to initialize token transfer reconciler: %v", err)
		}
		tokenReconciler.SetRetryPolicy(retryPolicy)
	}

	var balanceReconciler *reconciliation.BalanceReconciler
	if normType == "balance" || normType == "all" {
		if err := cfg.ValidateTokenConfig(); err != nil {
			log.Fatalf("invalid token configuration for balance reconciliation: %v", err)
		}

		tokenAddr := common.HexToAddress(cfg.TokenAddress)
		payrollAddr := common.HexToAddress(cfg.PayrollContractAddress)

		payrollABI, pErr := abi.JSON(strings.NewReader(indexerABI.MainABI))
		if pErr != nil {
			log.Fatalf("failed to parse payroll ABI for balance reconciler: %v", pErr)
		}

		balanceReconCfg := reconciliation.BalanceReconcilerConfig{
			ChainID:                cfg.ChainID,
			TokenAddress:           tokenAddr,
			PayrollContractAddress: payrollAddr,
			TokenStreamID:          cfg.TokenStreamID,
			ReconStreamID:          cfg.ReconciliationTokenBalanceStreamID,
			ConfirmationDepth:      cfg.ConfirmationDepth,
			StartBlock:             cfg.TokenStartBlock,
		}

		balanceReconciler, err = reconciliation.NewBalanceReconciler(
			balanceReconCfg,
			client,
			cfg.ParsedTokenABI,
			&payrollABI,
			tokensRepo,
			payrollRepo,
			reconRepo,
			db,
		)
		if err != nil {
			log.Fatalf("failed to initialize balance reconciler: %v", err)
		}
		balanceReconciler.SetRetryPolicy(retryPolicy)
	}

	fmt.Printf("Chain ID:                %d\n", cfg.ChainID)
	if cfg.PayrollContractAddress != "" && (payrollReconciler != nil || salaryClaimReconciler != nil || balanceReconciler != nil) {
		fmt.Printf("Payroll Contract:        %s\n", cfg.PayrollContractAddress)
	}
	if payrollReconciler != nil {
		fmt.Printf("Payroll Recon Stream ID: %s\n", cfg.ReconciliationStreamID)
	}
	if salaryClaimReconciler != nil {
		fmt.Printf("Claim Recon Stream ID:   %s\n", cfg.ReconciliationSalaryClaimStreamID)
	}
	if tokenReconciler != nil {
		fmt.Printf("Token Contract:          %s\n", cfg.TokenAddress)
		fmt.Printf("Token Recon Stream ID:   %s\n", cfg.ReconciliationTokenStreamID)
	}
	if balanceReconciler != nil {
		fmt.Printf("Balance Recon Stream ID: %s\n", cfg.ReconciliationTokenBalanceStreamID)
	}
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
		if payrollReconciler != nil {
			var res *reconciliation.ReconciliationResult
			var pErr error

			if *windowFlag > 0 {
				res, pErr = payrollReconciler.ReconcileRecentWindow(ctx, *windowFlag)
			} else if *fromBlockFlag > 0 && *toBlockFlag > 0 {
				res, pErr = payrollReconciler.ReconcileRange(ctx, *fromBlockFlag, *toBlockFlag)
			} else {
				batchSize := cfg.BlockBatchSize
				if *batchSizeFlag > 0 {
					batchSize = *batchSizeFlag
				}
				res, pErr = payrollReconciler.ReconcileNextBatch(ctx, batchSize)
			}

			if pErr != nil {
				return fmt.Errorf("payroll reconciliation failed: %w", pErr)
			}

			printSummary("Payroll Funding Reconciliation", res)
		}

		if salaryClaimReconciler != nil {
			var res *reconciliation.ReconciliationResult
			var sErr error

			if *windowFlag > 0 {
				res, sErr = salaryClaimReconciler.ReconcileRecentWindow(ctx, *windowFlag)
			} else if *fromBlockFlag > 0 && *toBlockFlag > 0 {
				res, sErr = salaryClaimReconciler.ReconcileRange(ctx, *fromBlockFlag, *toBlockFlag)
			} else {
				batchSize := cfg.BlockBatchSize
				if *batchSizeFlag > 0 {
					batchSize = *batchSizeFlag
				}
				res, sErr = salaryClaimReconciler.ReconcileNextBatch(ctx, batchSize)
			}

			if sErr != nil {
				return fmt.Errorf("salary claim reconciliation failed: %w", sErr)
			}

			printSummary("Salary Claim Reconciliation", res)
		}

		if tokenReconciler != nil {
			var res *reconciliation.ReconciliationResult
			var tErr error

			if *windowFlag > 0 {
				res, tErr = tokenReconciler.ReconcileRecentWindow(ctx, *windowFlag)
			} else if *fromBlockFlag > 0 && *toBlockFlag > 0 {
				res, tErr = tokenReconciler.ReconcileRange(ctx, *fromBlockFlag, *toBlockFlag)
			} else {
				batchSize := cfg.BlockBatchSize
				if *batchSizeFlag > 0 {
					batchSize = *batchSizeFlag
				}
				res, tErr = tokenReconciler.ReconcileNextBatch(ctx, batchSize)
			}

			if tErr != nil {
				return fmt.Errorf("token transfer reconciliation failed: %w", tErr)
			}

			printSummary("WTF Token Transfer Reconciliation", res)
		}

		if balanceReconciler != nil {
			var res *reconciliation.BalanceReconciliationResult
			var bErr error

			if *toBlockFlag > 0 {
				res, bErr = balanceReconciler.ReconcileTargetBlock(ctx, *toBlockFlag)
			} else if *windowFlag > 0 {
				res, bErr = balanceReconciler.ReconcileRecentWindow(ctx, *windowFlag)
			} else {
				batchSize := cfg.BlockBatchSize
				if *batchSizeFlag > 0 {
					batchSize = *batchSizeFlag
				}
				res, bErr = balanceReconciler.ReconcileNextBatch(ctx, batchSize)
			}

			if bErr != nil {
				return fmt.Errorf("contract and token balance reconciliation failed: %w", bErr)
			}

			printBalanceSummary("Contract & Token Balance Reconciliation", res)
		}
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

func printSummary(title string, res *reconciliation.ReconciliationResult) {
	if res == nil {
		return
	}
	fmt.Printf("\n--- %s Summary ---\n", title)
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

func printBalanceSummary(title string, res *reconciliation.BalanceReconciliationResult) {
	if res == nil {
		return
	}
	fmt.Printf("\n--- %s Summary ---\n", title)
	fmt.Printf("Target Block Checked:    %d\n", res.TargetBlock)
	fmt.Printf("Safe Target Block:       %d\n", res.SafeTarget)
	fmt.Printf("Indexer Checkpoint:      %d\n", res.IndexerCheckpoint)
	fmt.Printf("Addresses Audited:       %d\n", res.AddressesCheckedCount)
	fmt.Printf("Balances Matching:       %d\n", res.BalancesMatchedCount)
	fmt.Printf("Contract Balances:       %d\n", res.ContractBalancesCount)
	fmt.Printf("States Audited:          %d\n", res.StatesCheckedCount)
	fmt.Printf("States Matching:         %d\n", res.StatesMatchedCount)
	fmt.Printf("Mismatches Detected:     %d\n", res.MismatchesDetected)
	fmt.Printf("Exceptions Created:      %d\n", res.ExceptionsCreated)
	fmt.Printf("Exceptions Resolved:     %d\n", res.ExceptionsResolved)
	fmt.Printf("Exceptions Skipped:      %d (already open)\n", res.ExceptionsSkipped)
}
