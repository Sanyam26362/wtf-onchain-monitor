package validation

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// ValidateAddress checks if the given string is a valid Ethereum hex address.
func ValidateAddress(addrStr string) (common.Address, error) {
	trimmed := strings.TrimSpace(addrStr)
	if trimmed == "" {
		return common.Address{}, fmt.Errorf("address is empty")
	}

	if !strings.HasPrefix(trimmed, "0x") && !strings.HasPrefix(trimmed, "0X") {
		return common.Address{}, fmt.Errorf("address must start with 0x")
	}

	if len(trimmed) != 42 {
		return common.Address{}, fmt.Errorf("address must be 42 characters long, got %d", len(trimmed))
	}

	if !common.IsHexAddress(trimmed) {
		return common.Address{}, fmt.Errorf("invalid hexadecimal address format")
	}

	addr := common.HexToAddress(trimmed)
	return addr, nil
}
