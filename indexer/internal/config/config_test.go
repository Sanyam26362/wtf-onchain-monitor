package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validERC20ABI = `[
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true, "name": "from", "type": "address"},
			{"indexed": true, "name": "to", "type": "address"},
			{"indexed": false, "name": "value", "type": "uint256"}
		],
		"name": "Transfer",
		"type": "event"
	}
]`

const abiWithoutTransfer = `[
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true, "name": "owner", "type": "address"},
			{"indexed": true, "name": "spender", "type": "address"},
			{"indexed": false, "name": "value", "type": "uint256"}
		],
		"name": "Approval",
		"type": "event"
	}
]`

func TestValidateTokenConfig_MissingTokenAddress(t *testing.T) {
	cfg := Config{
		TokenAddress: "",
		TokenABIJSON: validERC20ABI,
		RPCURL:       "https://sepolia.example.com",
		ChainID:      11155111,
		DatabaseURL:  "postgres://localhost:5432/test",
	}

	err := cfg.ValidateTokenConfig()
	if err == nil {
		t.Fatal("expected error for missing TOKEN_ADDRESS, got nil")
	}
	if !strings.Contains(err.Error(), "TOKEN_ADDRESS is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestValidateTokenConfig_InvalidTokenAddress(t *testing.T) {
	testCases := []string{
		"not-an-address",
		"0x123",
		"0xGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG",
		"0x0000000000000000000000000000000000000000", // zero address
	}

	for _, addr := range testCases {
		cfg := Config{
			TokenAddress: addr,
			TokenABIJSON: validERC20ABI,
			RPCURL:       "https://sepolia.example.com",
			ChainID:      11155111,
			DatabaseURL:  "postgres://localhost:5432/test",
		}

		err := cfg.ValidateTokenConfig()
		if err == nil {
			t.Fatalf("expected error for invalid address %s, got nil", addr)
		}
	}
}

func TestValidateTokenConfig_SameAsPayrollAddress(t *testing.T) {
	sameAddr := "0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC"
	cfg := Config{
		PayrollContractAddress: sameAddr,
		TokenAddress:           sameAddr,
		TokenABIJSON:           validERC20ABI,
		RPCURL:                 "https://sepolia.example.com",
		ChainID:                11155111,
		DatabaseURL:            "postgres://localhost:5432/test",
	}

	err := cfg.ValidateTokenConfig()
	if err == nil {
		t.Fatal("expected error when token address matches payroll contract, got nil")
	}
	if !strings.Contains(err.Error(), "must not be the same as PAYROLL_CONTRACT_ADDRESS") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestValidateTokenConfig_MissingABI(t *testing.T) {
	cfg := Config{
		TokenAddress: "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
		TokenABIPath: "",
		TokenABIJSON: "",
		RPCURL:       "https://sepolia.example.com",
		ChainID:      11155111,
		DatabaseURL:  "postgres://localhost:5432/test",
	}

	err := cfg.ValidateTokenConfig()
	if err == nil {
		t.Fatal("expected error for missing ABI, got nil")
	}
	if !strings.Contains(err.Error(), "TOKEN_ABI_PATH or TOKEN_ABI_JSON is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestValidateTokenConfig_InvalidABI(t *testing.T) {
	cfg := Config{
		TokenAddress: "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
		TokenABIJSON: "not-valid-json",
		RPCURL:       "https://sepolia.example.com",
		ChainID:      11155111,
		DatabaseURL:  "postgres://localhost:5432/test",
	}

	err := cfg.ValidateTokenConfig()
	if err == nil {
		t.Fatal("expected error for invalid ABI JSON, got nil")
	}
}

func TestValidateTokenConfig_ABIWithoutTransfer(t *testing.T) {
	cfg := Config{
		TokenAddress: "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
		TokenABIJSON: abiWithoutTransfer,
		RPCURL:       "https://sepolia.example.com",
		ChainID:      11155111,
		DatabaseURL:  "postgres://localhost:5432/test",
	}

	err := cfg.ValidateTokenConfig()
	if err == nil {
		t.Fatal("expected error for ABI without Transfer event, got nil")
	}
	if !strings.Contains(err.Error(), "does not contain required 'Transfer' event") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestValidateTokenConfig_ValidFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	abiFile := filepath.Join(tmpDir, "erc20.json")
	if err := os.WriteFile(abiFile, []byte(validERC20ABI), 0644); err != nil {
		t.Fatalf("failed to write temp ABI file: %v", err)
	}

	cfg := Config{
		TokenAddress: "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
		TokenABIPath: abiFile,
		RPCURL:       "https://sepolia.example.com",
		ChainID:      11155111,
		DatabaseURL:  "postgres://localhost:5432/test",
	}

	if err := cfg.ValidateTokenConfig(); err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}

	if cfg.ParsedTokenABI == nil {
		t.Fatal("expected ParsedTokenABI to be set after validation")
	}
}

func TestLoad_BackfillToBlock(t *testing.T) {
	t.Setenv("CHAIN_ID", "11155111")
	t.Setenv("START_BLOCK", "11080692")
	t.Setenv("BACKFILL_TO_BLOCK", "11080850")
	t.Setenv("PAYROLL_STREAM_ID", "custom_payroll")
	t.Setenv("CONFIRMATION_DEPTH", "5")
	t.Setenv("POLLING_INTERVAL", "12s")
	t.Setenv("RPC_URL", "https://sepolia.example.com")
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/test")
	t.Setenv("DEPLOYMENT_ENVIRONMENT", "test")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.BackfillToBlock == nil || *cfg.BackfillToBlock != 11080850 {
		t.Fatalf("expected BackfillToBlock to be 11080850, got %v", cfg.BackfillToBlock)
	}

	if cfg.PayrollStreamID != "custom_payroll" {
		t.Fatalf("expected PayrollStreamID to be 'custom_payroll', got '%s'", cfg.PayrollStreamID)
	}
}

func TestLoad_LiveMonitoringConfig(t *testing.T) {
	t.Setenv("CHAIN_ID", "11155111")
	t.Setenv("START_BLOCK", "11080692")
	t.Setenv("CONFIRMATIONS", "10")
	t.Setenv("INDEXER_BATCH_SIZE", "25")
	t.Setenv("LIVE_MONITOR_ENABLED", "true")
	t.Setenv("LIVE_POLL_INTERVAL", "3s")
	t.Setenv("RPC_URL", "https://sepolia.example.com")
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/test")
	t.Setenv("DEPLOYMENT_ENVIRONMENT", "test")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.ConfirmationDepth != 10 {
		t.Errorf("expected ConfirmationDepth 10, got %d", cfg.ConfirmationDepth)
	}
	if cfg.BlockBatchSize != 25 {
		t.Errorf("expected BlockBatchSize 25, got %d", cfg.BlockBatchSize)
	}
	if !cfg.LiveMonitorEnabled {
		t.Errorf("expected LiveMonitorEnabled to be true")
	}
	if cfg.LivePollInterval.Seconds() != 3 {
		t.Errorf("expected LivePollInterval 3s, got %v", cfg.LivePollInterval)
	}
}

func TestLoad_RPCRetryConfig(t *testing.T) {
	t.Setenv("CHAIN_ID", "11155111")
	t.Setenv("START_BLOCK", "11080692")
	t.Setenv("CONFIRMATIONS", "5")
	t.Setenv("POLLING_INTERVAL", "5s")
	t.Setenv("RPC_URL", "https://sepolia.example.com")
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/test")
	t.Setenv("DEPLOYMENT_ENVIRONMENT", "test")
	t.Setenv("RPC_MAX_RETRIES", "7")
	t.Setenv("RPC_INITIAL_BACKOFF", "2s")
	t.Setenv("RPC_MAX_BACKOFF", "45s")
	t.Setenv("RPC_BACKOFF_FACTOR", "2.5")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if cfg.RPCMaxRetries != 7 {
		t.Errorf("expected RPCMaxRetries 7, got %d", cfg.RPCMaxRetries)
	}
	if cfg.RPCInitialBackoff.Seconds() != 2 {
		t.Errorf("expected RPCInitialBackoff 2s, got %v", cfg.RPCInitialBackoff)
	}
	if cfg.RPCMaxBackoff.Seconds() != 45 {
		t.Errorf("expected RPCMaxBackoff 45s, got %v", cfg.RPCMaxBackoff)
	}
	if cfg.RPCBackoffFactor != 2.5 {
		t.Errorf("expected RPCBackoffFactor 2.5, got %f", cfg.RPCBackoffFactor)
	}
}

func TestLoad_ReconciliationConfig(t *testing.T) {
	t.Setenv("CHAIN_ID", "11155111")
	t.Setenv("START_BLOCK", "11080692")
	t.Setenv("CONFIRMATIONS", "5")
	t.Setenv("POLLING_INTERVAL", "5s")
	t.Setenv("RPC_URL", "https://sepolia.example.com")
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/test")
	t.Setenv("DEPLOYMENT_ENVIRONMENT", "test")
	t.Setenv("RECONCILIATION_ENABLED", "true")
	t.Setenv("RECONCILIATION_BLOCK_WINDOW", "1000")
	t.Setenv("RECONCILIATION_INTERVAL", "15s")
	t.Setenv("RECONCILIATION_STREAM_ID", "recon_test_stream")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading config: %v", err)
	}

	if !cfg.ReconciliationEnabled {
		t.Errorf("expected ReconciliationEnabled true, got false")
	}
	if cfg.ReconciliationBlockWindow != 1000 {
		t.Errorf("expected ReconciliationBlockWindow 1000, got %d", cfg.ReconciliationBlockWindow)
	}
	if cfg.ReconciliationInterval.Seconds() != 15 {
		t.Errorf("expected ReconciliationInterval 15s, got %v", cfg.ReconciliationInterval)
	}
	if cfg.ReconciliationStreamID != "recon_test_stream" {
		t.Errorf("expected ReconciliationStreamID 'recon_test_stream', got %q", cfg.ReconciliationStreamID)
	}
}

func TestLoad_RedisAndEscrowConfig(t *testing.T) {
	t.Setenv("CHAIN_ID", "11155111")
	t.Setenv("START_BLOCK", "11080692")
	t.Setenv("CONFIRMATIONS", "5")
	t.Setenv("POLLING_INTERVAL", "5s")
	t.Setenv("RPC_URL", "https://sepolia.example.com")
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/test")
	t.Setenv("DEPLOYMENT_ENVIRONMENT", "test")

	// Test defaults
	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("unexpected error loading config with LoadConfig(): %v", err)
	}

	if cfg.RedisURL != "localhost:6379" {
		t.Errorf("expected default RedisURL 'localhost:6379', got %q", cfg.RedisURL)
	}
	if cfg.AlchemyWebhookSigningKey != "whsec_test_dummy_key" {
		t.Errorf("expected default AlchemyWebhookSigningKey 'whsec_test_dummy_key', got %q", cfg.AlchemyWebhookSigningKey)
	}
	if cfg.WTFEscrowContractAddress != "0x0000000000000000000000000000000000000000" {
		t.Errorf("expected default WTFEscrowContractAddress '0x0000000000000000000000000000000000000000', got %q", cfg.WTFEscrowContractAddress)
	}

	// Test custom values
	t.Setenv("REDIS_URL", "redis-custom:6380")
	t.Setenv("ALCHEMY_WEBHOOK_SIGNING_KEY", "whsec_custom_secret_123")
	t.Setenv("WTF_ESCROW_CONTRACT_ADDRESS", "0x1111111111111111111111111111111111111111")

	cfgCustom, err := Load()
	if err != nil {
		t.Fatalf("unexpected error loading custom config: %v", err)
	}

	if cfgCustom.RedisURL != "redis-custom:6380" {
		t.Errorf("expected custom RedisURL 'redis-custom:6380', got %q", cfgCustom.RedisURL)
	}
	if cfgCustom.AlchemyWebhookSigningKey != "whsec_custom_secret_123" {
		t.Errorf("expected custom AlchemyWebhookSigningKey 'whsec_custom_secret_123', got %q", cfgCustom.AlchemyWebhookSigningKey)
	}
	if cfgCustom.WTFEscrowContractAddress != "0x1111111111111111111111111111111111111111" {
		t.Errorf("expected custom WTFEscrowContractAddress '0x1111111111111111111111111111111111111111', got %q", cfgCustom.WTFEscrowContractAddress)
	}
}

