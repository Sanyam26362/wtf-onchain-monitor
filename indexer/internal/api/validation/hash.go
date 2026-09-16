package validation

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// ValidateTxHash checks if the given string is a valid 32-byte Ethereum transaction hash.
func ValidateTxHash(hashStr string) (common.Hash, error) {
	trimmed := strings.TrimSpace(hashStr)
	if trimmed == "" {
		return common.Hash{}, fmt.Errorf("transaction hash is empty")
	}

	if !strings.HasPrefix(trimmed, "0x") && !strings.HasPrefix(trimmed, "0X") {
		return common.Hash{}, fmt.Errorf("transaction hash must start with 0x prefix")
	}

	if len(trimmed) != 66 {
		return common.Hash{}, fmt.Errorf("transaction hash must be 66 characters long, got %d", len(trimmed))
	}

	rawHex := trimmed[2:]
	if _, err := hex.DecodeString(rawHex); err != nil {
		return common.Hash{}, fmt.Errorf("transaction hash contains invalid hex characters: %w", err)
	}

	return common.HexToHash(trimmed), nil
}
