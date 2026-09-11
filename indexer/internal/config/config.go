package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ChainID                int64
	RPCURL                 string
	PayrollContractAddress string
	TokenAddress           string
	StartBlock             uint64
	ConfirmationDepth      uint64
	DatabaseURL            string
	PollingInterval        time.Duration
	DeploymentEnvironment   string
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

	confirmationDepth, err := getUint64("CONFIRMATION_DEPTH")
	if err != nil {
		return Config{}, err
	}

	pollingInterval, err := time.ParseDuration(getRequired("POLLING_INTERVAL"))
	if err != nil {
		return Config{}, fmt.Errorf("invalid POLLING_INTERVAL: %w", err)
	}

	cfg := Config{
		ChainID:                chainID,
		RPCURL:                 getRequired("RPC_URL"),
		PayrollContractAddress: getRequired("PAYROLL_CONTRACT_ADDRESS"),
		TokenAddress:           os.Getenv("TOKEN_ADDRESS"),
		StartBlock:             startBlock,
		ConfirmationDepth:      confirmationDepth,
		DatabaseURL:            getRequired("DATABASE_URL"),
		PollingInterval:        pollingInterval,
		DeploymentEnvironment:   getRequired("DEPLOYMENT_ENVIRONMENT"),
	}

	return cfg, nil
}

func getRequired(key string) string {
	value := os.Getenv(key)

	if value == "" {
		panic(fmt.Sprintf("missing required environment variable: %s", key))
	}

	return value
}

func getInt64(key string) (int64, error) {
	value := getRequired(key)

	result, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}

	return result, nil
}

func getUint64(key string) (uint64, error) {
	value := getRequired(key)

	result, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}

	return result, nil
}