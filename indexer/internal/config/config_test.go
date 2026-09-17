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
