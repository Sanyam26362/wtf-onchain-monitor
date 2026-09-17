package persistence

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/abi"
	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/decoder"
)

func TestPersistence_Payroll_EmployeeAddedAndRemoved(t *testing.T) {
	pg := getTestPostgres(t)
	defer pg.Close()

	ctx := context.Background()
	chainID := int64(11155111)

	employerAddr := common.HexToAddress("0xa1c2753108F75f597E09ECa49af978696F72B554")
	employeeAddr := common.HexToAddress("0x28a009551412f87918958fac4732281301eAeFce")
	txHash := common.HexToHash("0x1f98ade35e0eed6aa2a323ac9ca8fbc0082c55b0571df7cb0af8f7b3ba512b72")
	blockNum := uint64(11080817)
	blockTimestamp := uint64(1700000000)

	// Ensure transaction exists first
	txMeta := &blockchain.TransactionMetadata{
		Hash:        txHash,
		BlockNumber: blockNum,
		Sender:      employerAddr,
		GasUsed:     50000,
		Status:      1,
	}
	if err := pg.SaveTransaction(ctx, chainID, txMeta); err != nil {
		t.Fatalf("failed to save transaction: %v", err)
	}

	// 1. Test EmployeeAdded
	employeeAddedEvent := &decoder.DecodedEvent{
		Type: decoder.EventEmployeeAdded,
		Data: &abi.ABIEmployeeAddedEvent{
			Employee:   employeeAddr,
			Employer:   employerAddr,
			Salary:     big.NewInt(500),
			Allocation: big.NewInt(1000),
		},
		Log: types.Log{
			Address:     common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC"),
			TxHash:      txHash,
			BlockNumber: blockNum,
			Index:       112,
		},
	}

	// Save chain event
	if err := pg.SaveChainEvent(ctx, chainID, employeeAddedEvent.Log.Address, blockTimestamp, employeeAddedEvent); err != nil {
		t.Fatalf("failed to save chain event: %v", err)
	}

	// Project employee added
	if err := pg.ProjectPayrollEvent(ctx, chainID, blockTimestamp, employeeAddedEvent); err != nil {
		t.Fatalf("failed to project EmployeeAdded: %v", err)
	}

	// Replay (Idempotency test)
	if err := pg.ProjectPayrollEvent(ctx, chainID, blockTimestamp, employeeAddedEvent); err != nil {
		t.Fatalf("idempotency check failed: %v", err)
	}

	// Verify employee in DB
	var (
		wallet          string
		employer        string
		salaryPerSecond string
		active          bool
		allocation      string
	)
	query := `
		SELECT wallet, employer, salary_per_second, active, allocation
		FROM employees
		WHERE chain_id = $1 AND wallet = $2
	`
	err := pg.pool.QueryRow(ctx, query, chainID, employeeAddr.Hex()).Scan(
		&wallet,
		&employer,
		&salaryPerSecond,
		&active,
		&allocation,
	)
	if err != nil {
		t.Fatalf("failed to query employee: %v", err)
	}

	if wallet != employeeAddr.Hex() {
		t.Fatalf("expected wallet %s, got %s", employeeAddr.Hex(), wallet)
	}
	if employer != employerAddr.Hex() {
		t.Fatalf("expected employer %s, got %s", employerAddr.Hex(), employer)
	}
	if salaryPerSecond != "500" {
		t.Fatalf("expected salary 500, got %s", salaryPerSecond)
	}
	if !active {
		t.Fatal("expected employee to be active")
	}
	if allocation != "1000" {
		t.Fatalf("expected allocation 1000, got %s", allocation)
	}

	// 2. Test EmployeeRemoved projection
	employeeRemovedEvent := &decoder.DecodedEvent{
		Type: decoder.EventEmployeeRemoved,
		Data: &abi.ABIEmployeeRemovedEvent{
			Employee: employeeAddr,
			Employer: employerAddr,
		},
		Log: types.Log{
			Address:     common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC"),
			TxHash:      txHash,
			BlockNumber: blockNum + 10,
			Index:       1,
		},
	}

	if err := pg.ProjectPayrollEvent(ctx, chainID, blockTimestamp+3600, employeeRemovedEvent); err != nil {
		t.Fatalf("failed to project EmployeeRemoved: %v", err)
	}

	// Verify employee is now deactivated
	err = pg.pool.QueryRow(ctx, query, chainID, employeeAddr.Hex()).Scan(
		&wallet,
		&employer,
		&salaryPerSecond,
		&active,
		&allocation,
	)
	if err != nil {
		t.Fatalf("failed to re-query employee: %v", err)
	}
	if active {
		t.Fatal("expected employee to be inactive after EmployeeRemoved")
	}

	// Idempotency replay of remove
	if err := pg.ProjectPayrollEvent(ctx, chainID, blockTimestamp+3600, employeeRemovedEvent); err != nil {
		t.Fatalf("idempotency check for EmployeeRemoved failed: %v", err)
	}
}
