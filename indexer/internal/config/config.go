package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	indexerABI "worldtradefuture/indexer/internal/abi"
)

type Config struct {
	ChainID                int64
	RPCURL                 string
	PayrollContractAddress string
	TokenAddress           string
	TokenABIPath           string
	TokenABIJSON           string
	TokenStartBlock        uint64
	TokenStreamID          string
	StartBlock             uint64
	BackfillToBlock        *uint64
	PayrollStreamID        string
	ConfirmationDepth      uint64
	BlockBatchSize         uint64
	DatabaseURL            string
	PollingInterval        time.Duration
	LiveMonitorEnabled     bool
	LivePollInterval       time.Duration
	DeploymentEnvironment  string

	// API Configuration
	APIHost               string
	APIPort               int
	CORSAllowedOrigins    string
	ExplorerTxURLTemplate string
	OperatorAPIKey        string

	// RPC Retry Configuration
	RPCMaxRetries     int
	RPCInitialBackoff time.Duration
	RPCMaxBackoff     time.Duration
	RPCBackoffFactor  float64

	// ParsedTokenABI is populated and cached after successful validation.
	ParsedTokenABI *abi.ABI
}

func Load() (Config, error) {
	chainID, err := getInt64("CHAIN_ID")
	if err != nil {
		return Config{}, err
	}

	startBlock, err := getUint64("START_BLOCK")
	if err != nil {
		return Config{}, err
	}

	confirmationDepthStr := os.Getenv("CONFIRMATIONS")
	if confirmationDepthStr == "" {
		confirmationDepthStr = os.Getenv("CONFIRMATION_DEPTH")
	}
	if confirmationDepthStr == "" {
		return Config{}, fmt.Errorf("missing required environment variable: CONFIRMATION_DEPTH or CONFIRMATIONS")
	}
	confirmationDepth, err := strconv.ParseUint(confirmationDepthStr, 10, 64)
	if err != nil {
		return Config{}, fmt.Errorf("invalid CONFIRMATIONS / CONFIRMATION_DEPTH: %w", err)
	}

	pollingIntervalStr := os.Getenv("LIVE_POLL_INTERVAL")
	if pollingIntervalStr == "" {
		pollingIntervalStr = os.Getenv("POLLING_INTERVAL")
	}
	if pollingIntervalStr == "" {
		return Config{}, fmt.Errorf("missing required environment variable: POLLING_INTERVAL or LIVE_POLL_INTERVAL")
	}
	pollingInterval, err := time.ParseDuration(pollingIntervalStr)
	if err != nil {
		return Config{}, fmt.Errorf("invalid POLLING_INTERVAL: %w", err)
	}

	livePollInterval := pollingInterval
	if val := os.Getenv("LIVE_POLL_INTERVAL"); val != "" {
		parsed, parseErr := time.ParseDuration(val)
		if parseErr == nil && parsed > 0 {
			livePollInterval = parsed
		}
	}

	liveMonitorEnabled := false
	if val := os.Getenv("LIVE_MONITOR_ENABLED"); val != "" {
		liveMonitorEnabled = strings.EqualFold(val, "true") || val == "1"
	}

	blockBatchSize := uint64(50)
	batchVal := os.Getenv("INDEXER_BATCH_SIZE")
	if batchVal == "" {
		batchVal = os.Getenv("BLOCK_BATCH_SIZE")
	}
	if batchVal != "" {
		blockBatchSize, err = strconv.ParseUint(batchVal, 10, 64)
		if err != nil || blockBatchSize == 0 {
			return Config{}, fmt.Errorf("invalid BLOCK_BATCH_SIZE: must be a positive integer")
		}
	}

	rpcURL, err := getRequired("RPC_URL")
	if err != nil {
		return Config{}, err
	}

	dbURL, err := getRequired("DATABASE_URL")
	if err != nil {
		return Config{}, err
	}

	env, err := getRequired("DEPLOYMENT_ENVIRONMENT")
	if err != nil {
		return Config{}, err
	}

	payrollContract := os.Getenv("PAYROLL_CONTRACT_ADDRESS")

	var backfillToBlock *uint64
	if val := os.Getenv("BACKFILL_TO_BLOCK"); val != "" {
		parsed, parseErr := strconv.ParseUint(val, 10, 64)
		if parseErr != nil {
			return Config{}, fmt.Errorf("invalid BACKFILL_TO_BLOCK: %w", parseErr)
		}
		backfillToBlock = &parsed
	}

	payrollStreamID := os.Getenv("PAYROLL_STREAM_ID")
	if payrollStreamID == "" {
		payrollStreamID = "monthly_payroll"
	}

	tokenAddress := os.Getenv("TOKEN_ADDRESS")
	tokenABIPath := os.Getenv("TOKEN_ABI_PATH")
	tokenABIJSON := os.Getenv("TOKEN_ABI_JSON")

	tokenStartBlock := startBlock
	if val := os.Getenv("TOKEN_START_BLOCK"); val != "" {
		parsed, parseErr := strconv.ParseUint(val, 10, 64)
		if parseErr != nil {
			return Config{}, fmt.Errorf("invalid TOKEN_START_BLOCK: %w", parseErr)
		}
		tokenStartBlock = parsed
	}

	tokenStreamID := os.Getenv("TOKEN_STREAM_ID")
	if (tokenStreamID == "" || tokenStreamID == "erc20_transfers") && tokenAddress != "" {
		tokenStreamID = fmt.Sprintf("erc20_transfers_%s", strings.ToLower(tokenAddress))
	} else if tokenStreamID == "" {
		tokenStreamID = "erc20_transfers"
	}

	apiHost := os.Getenv("API_HOST")
	if apiHost == "" {
		apiHost = "0.0.0.0"
	}

	apiPort := 8080
	if val := os.Getenv("API_PORT"); val != "" {
		if p, err := strconv.Atoi(val); err == nil && p > 0 {
			apiPort = p
		}
	}

	corsOrigins := os.Getenv("CORS_ALLOWED_ORIGINS")
	if corsOrigins == "" {
		corsOrigins = "*"
	}

	explorerTemplate := os.Getenv("EXPLORER_TX_URL_TEMPLATE")
	operatorKey := os.Getenv("OPERATOR_API_KEY")

	// RPC retry parameters
	rpcMaxRetries := 5
	if val := os.Getenv("RPC_MAX_RETRIES"); val != "" {
		if r, err := strconv.Atoi(val); err == nil && r > 0 {
			rpcMaxRetries = r
		}
	} else if val := os.Getenv("MAX_RETRIES"); val != "" {
		if r, err := strconv.Atoi(val); err == nil && r > 0 {
			rpcMaxRetries = r
		}
	}

	rpcInitialBackoff := 1 * time.Second
	if val := os.Getenv("RPC_INITIAL_BACKOFF"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			rpcInitialBackoff = d
		}
	} else if val := os.Getenv("INITIAL_BACKOFF"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			rpcInitialBackoff = d
		}
	}

	rpcMaxBackoff := 30 * time.Second
	if val := os.Getenv("RPC_MAX_BACKOFF"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			rpcMaxBackoff = d
		}
	} else if val := os.Getenv("MAX_BACKOFF"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			rpcMaxBackoff = d
		}
	}

	rpcBackoffFactor := 2.0
	if val := os.Getenv("RPC_BACKOFF_FACTOR"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f > 1.0 {
			rpcBackoffFactor = f
		}
	}

	cfg := Config{
		ChainID:                chainID,
		RPCURL:                 rpcURL,
		PayrollContractAddress: payrollContract,
		TokenAddress:           tokenAddress,
		TokenABIPath:           tokenABIPath,
		TokenABIJSON:           tokenABIJSON,
		TokenStartBlock:        tokenStartBlock,
		TokenStreamID:          tokenStreamID,
		StartBlock:             startBlock,
		BackfillToBlock:        backfillToBlock,
		PayrollStreamID:        payrollStreamID,
		ConfirmationDepth:      confirmationDepth,
		BlockBatchSize:         blockBatchSize,
		DatabaseURL:            dbURL,
		PollingInterval:        pollingInterval,
		LiveMonitorEnabled:     liveMonitorEnabled,
		LivePollInterval:       livePollInterval,
		DeploymentEnvironment:  env,
		APIHost:                apiHost,
		APIPort:                apiPort,
		CORSAllowedOrigins:     corsOrigins,
		ExplorerTxURLTemplate:  explorerTemplate,
		OperatorAPIKey:         operatorKey,
		RPCMaxRetries:          rpcMaxRetries,
		RPCInitialBackoff:      rpcInitialBackoff,
		RPCMaxBackoff:          rpcMaxBackoff,
		RPCBackoffFactor:       rpcBackoffFactor,
	}

	return cfg, nil
}

// ExplorerTxURL generates an explorer link for a transaction hash using the configured network/template.
func (c *Config) ExplorerTxURL(txHash string) string {
	if c.ExplorerTxURLTemplate != "" {
		return fmt.Sprintf(c.ExplorerTxURLTemplate, txHash)
	}
	if c.ChainID == 11155111 {
		return fmt.Sprintf("https://sepolia.etherscan.io/tx/%s", txHash)
	}
	return fmt.Sprintf("https://etherscan.io/tx/%s", txHash)
}

// ValidateTokenConfig verifies all requirements for running the generic ERC-20 indexer.
func (c *Config) ValidateTokenConfig() error {
	if c.TokenAddress == "" {
		return fmt.Errorf("TOKEN_ADDRESS is required")
	}

	if !common.IsHexAddress(c.TokenAddress) {
		return fmt.Errorf("TOKEN_ADDRESS is not a valid Ethereum address: %s", c.TokenAddress)
	}

	tokenAddr := common.HexToAddress(c.TokenAddress)
	if (tokenAddr == common.Address{}) {
		return fmt.Errorf("TOKEN_ADDRESS cannot be the zero address")
	}

	if c.PayrollContractAddress != "" && strings.EqualFold(c.TokenAddress, c.PayrollContractAddress) {
		return fmt.Errorf("TOKEN_ADDRESS must not be the same as PAYROLL_CONTRACT_ADDRESS (%s)", c.PayrollContractAddress)
	}

	if c.TokenABIPath == "" && c.TokenABIJSON == "" {
		return fmt.Errorf("TOKEN_ABI_PATH or TOKEN_ABI_JSON is required")
	}

	parsedABI, err := indexerABI.LoadERC20ABI(c.TokenABIPath, c.TokenABIJSON)
	if err != nil {
		return fmt.Errorf("failed to load/parse token ABI: %w", err)
	}

	// Validate Transfer event requirement
	transferEvent, ok := parsedABI.Events["Transfer"]
	if !ok {
		return fmt.Errorf("configured token ABI does not contain required 'Transfer' event")
	}

	if len(transferEvent.Inputs) < 3 {
		return fmt.Errorf("Transfer event must have at least 3 parameters, got %d", len(transferEvent.Inputs))
	}

	if !transferEvent.Inputs[0].Indexed || transferEvent.Inputs[0].Type.T != abi.AddressTy {
		return fmt.Errorf("Transfer event parameter 0 must be an indexed address")
	}

	if !transferEvent.Inputs[1].Indexed || transferEvent.Inputs[1].Type.T != abi.AddressTy {
		return fmt.Errorf("Transfer event parameter 1 must be an indexed address")
	}

	nonIndexed := transferEvent.Inputs.NonIndexed()
	if len(nonIndexed) != 1 || nonIndexed[0].Type.T != abi.UintTy {
		return fmt.Errorf("Transfer event must have exactly 1 non-indexed uint256 parameter")
	}

	if c.RPCURL == "" {
		return fmt.Errorf("RPC_URL is required")
	}

	if c.ChainID <= 0 {
		return fmt.Errorf("CHAIN_ID must be positive, got %d", c.ChainID)
	}

	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}

	c.ParsedTokenABI = parsedABI
	return nil
}

func getRequired(key string) (string, error) {
	value := os.Getenv(key)
	if value == "" {
		return "", fmt.Errorf("missing required environment variable: %s", key)
	}
	return value, nil
}

func getInt64(key string) (int64, error) {
	value, err := getRequired(key)
	if err != nil {
		return 0, err
	}

	result, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}

	return result, nil
}

func getUint64(key string) (uint64, error) {
	value, err := getRequired(key)
	if err != nil {
		return 0, err
	}

	result, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}

	return result, nil
}
