package indexer

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/joho/godotenv"

	"worldtradefuture/indexer/internal/api/handlers"
	"worldtradefuture/indexer/internal/config"
	"worldtradefuture/indexer/internal/persistence"
	"worldtradefuture/indexer/internal/repository"
)

func TestVerifyDatabaseAndAPI(t *testing.T) {
	_ = godotenv.Load("../../.env")
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping: DATABASE_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pg, err := persistence.NewPostgres(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping: could not connect to postgres: %v", err)
	}
	defer pg.Close()

	// 1. SELECT event_name, COUNT(*) FROM chain_events GROUP BY event_name ORDER BY event_name;
	t.Log("--- 1. Event Counts by Name ---")
	eventQuery := `
		SELECT event_name, COUNT(*)
		FROM chain_events
		GROUP BY event_name
		ORDER BY event_name;
	`
	rows, err := pg.Pool().Query(ctx, eventQuery)
	if err != nil {
		t.Fatalf("failed to query chain_events: %v", err)
	}
	defer rows.Close()

	foundAny := false
	for rows.Next() {
		var name string
		var count int64
		if err := rows.Scan(&name, &count); err != nil {
			t.Fatalf("scan error: %v", err)
		}
		t.Logf("  Event: %-25s Count: %d", name, count)
		foundAny = true
	}
	if !foundAny {
		t.Fatal("expected at least one chain event in database")
	}

	// 2. SELECT * FROM chain_events WHERE event_name = 'EmployeeAdded' ORDER BY block_number;
	t.Log("--- 2. EmployeeAdded Events in chain_events ---")
	eeQuery := `
		SELECT event_id, chain_id, contract_address, event_name, tx_hash, block_number, log_index, raw_data
		FROM chain_events
		WHERE event_name = 'EmployeeAdded'
		ORDER BY block_number;
	`
	eeRows, err := pg.Pool().Query(ctx, eeQuery)
	if err != nil {
		t.Fatalf("failed to query EmployeeAdded events: %v", err)
	}
	defer eeRows.Close()

	var employeeAddCount int
	var capturedEmployee string
	for eeRows.Next() {
		var (
			eventID     int64
			chainID     int64
			contract    string
			eventName   string
			txHash      string
			blockNumber int64
			logIndex    int
			rawData     []byte
		)
		if err := eeRows.Scan(&eventID, &chainID, &contract, &eventName, &txHash, &blockNumber, &logIndex, &rawData); err != nil {
			t.Fatalf("scan error: %v", err)
		}
		employeeAddCount++
		t.Logf("  [ID: %d] block=%d tx=%s logIndex=%d raw_data=%s", eventID, blockNumber, txHash, logIndex, string(rawData))
	}
	if employeeAddCount == 0 {
		t.Fatal("expected EmployeeAdded event in chain_events table")
	}

	// 3. SELECT * FROM employees;
	t.Log("--- 3. Employees in employees table ---")
	empQuery := `
		SELECT wallet, employer, salary_per_second, active, allocation, added_at, latest_tx_hash
		FROM employees;
	`
	empRows, err := pg.Pool().Query(ctx, empQuery)
	if err != nil {
		t.Fatalf("failed to query employees: %v", err)
	}
	defer empRows.Close()

	var empCount int
	for empRows.Next() {
		var (
			wallet          string
			employer        string
			salaryPerSecond string
			active          bool
			allocation      *string
			addedAt         *time.Time
			latestTxHash    *string
		)
		if err := empRows.Scan(&wallet, &employer, &salaryPerSecond, &active, &allocation, &addedAt, &latestTxHash); err != nil {
			t.Fatalf("scan employee error: %v", err)
		}
		empCount++
		capturedEmployee = wallet
		allocStr := "nil"
		if allocation != nil {
			allocStr = *allocation
		}
		t.Logf("  Employee: %s | Employer: %s | Salary/sec: %s | Active: %v | Allocation: %s",
			wallet, employer, salaryPerSecond, active, allocStr)
	}
	if empCount == 0 {
		t.Fatal("expected employees table to be populated, but it is empty")
	}

	// 4. API Verification: GET /v1/employees/{employee_address}
	t.Log("--- 4. API Verification GET /v1/employees/{address} ---")
	empRepo := repository.NewEmployeesRepository(pg.Pool())
	cfg := &config.Config{
		ChainID: 11155111,
	}
	handler := handlers.EmployeeHandler(empRepo, cfg)

	req := httptest.NewRequest(http.MethodGet, "/v1/employees/"+capturedEmployee, nil)
	req.SetPathValue("address", capturedEmployee)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	t.Logf("  HTTP Status: %d", rec.Code)
	t.Logf("  HTTP Response Body: %s", rec.Body.String())

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 OK for employee %s, got: %d", capturedEmployee, rec.Code)
	}

	// Verify with canonical address
	expectedAddr := common.HexToAddress("0x28a009551412f87918958fac4732281301eAeFce").Hex()
	if common.HexToAddress(capturedEmployee).Hex() != expectedAddr {
		t.Fatalf("expected employee address %s, got %s", expectedAddr, capturedEmployee)
	}

	// 5. API Verification: GET /v1/employers/{employer_address}
	t.Log("--- 5. API Verification GET /v1/employers/{address} ---")
	employerAddr := "0xa1c2753108F75f597E09ECa49af978696F72B554"
	empHandler := handlers.EmployerHandler(repository.NewEmployersRepository(pg.Pool()), cfg)
	empReq := httptest.NewRequest(http.MethodGet, "/v1/employers/"+employerAddr, nil)
	empReq.SetPathValue("address", employerAddr)
	empRec := httptest.NewRecorder()
	empHandler.ServeHTTP(empRec, empReq)
	t.Logf("  Employer HTTP Status: %d", empRec.Code)
	t.Logf("  Employer HTTP Body: %s", empRec.Body.String())
	if empRec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for employer %s, got: %d", employerAddr, empRec.Code)
	}

	// 6. API Verification: GET /v1/sync/status
	t.Log("--- 6. API Verification GET /v1/sync/status ---")
	syncHandler := handlers.SyncStatusHandler(repository.NewSyncRepository(pg.Pool()), cfg)
	syncReq := httptest.NewRequest(http.MethodGet, "/v1/sync/status", nil)
	syncRec := httptest.NewRecorder()
	syncHandler.ServeHTTP(syncRec, syncReq)
	t.Logf("  Sync Status HTTP Status: %d", syncRec.Code)
	t.Logf("  Sync Status HTTP Body: %s", syncRec.Body.String())
	if syncRec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 for sync status, got: %d", syncRec.Code)
	}
}

func TestVerifyWTFTokenDatabaseAndAPI(t *testing.T) {
	_ = godotenv.Load("../../.env")
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("skipping: DATABASE_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pg, err := persistence.NewPostgres(ctx, dbURL)
	if err != nil {
		t.Skipf("skipping: could not connect to postgres: %v", err)
	}
	defer pg.Close()

	chainID := int64(11155111)
	tokenAddr := "0x378AFb93CaDd39AFF154704d2D90Af8c401137E7"
	streamID := fmt.Sprintf("erc20_transfers_%s", tokenAddr)

	t.Logf("--- WTF Token Verification (%s) ---", tokenAddr)

	// 1. Current token checkpoint
	var lastIndexedBlock int64
	var lastBlockHash string
	var updatedAt time.Time
	cpQuery := `
		SELECT last_indexed_block, COALESCE(last_block_hash, ''), updated_at
		FROM sync_checkpoints
		WHERE chain_id = $1 AND stream_id = $2
	`
	err = pg.Pool().QueryRow(ctx, cpQuery, chainID, streamID).Scan(&lastIndexedBlock, &lastBlockHash, &updatedAt)
	if err != nil {
		t.Fatalf("failed to query checkpoint for stream %s: %v", streamID, err)
	}
	t.Logf("  Stream ID:          %s", streamID)
	t.Logf("  Current Checkpoint: %d (updated at %s)", lastIndexedBlock, updatedAt.Format(time.RFC3339))

	// 2. Earliest & latest indexed block in token_transfers
	var earliestBlock *int64
	var latestBlock *int64
	var rowCount int64
	statsQuery := `
		SELECT MIN(block_number), MAX(block_number), COUNT(*)
		FROM token_transfers
		WHERE chain_id = $1 AND LOWER(token) = LOWER($2)
	`
	err = pg.Pool().QueryRow(ctx, statsQuery, chainID, tokenAddr).Scan(&earliestBlock, &latestBlock, &rowCount)
	if err != nil {
		t.Fatalf("failed to query token_transfers stats: %v", err)
	}

	earliestStr := "none"
	if earliestBlock != nil {
		earliestStr = fmt.Sprintf("%d", *earliestBlock)
	}
	latestStr := "none"
	if latestBlock != nil {
		latestStr = fmt.Sprintf("%d", *latestBlock)
	}

	t.Logf("  Total Rows in token_transfers: %d", rowCount)
	t.Logf("  Earliest Block with Transfer:  %s", earliestStr)
	t.Logf("  Latest Block with Transfer:    %s", latestStr)

	// 3. Transfer events in chain_events
	var chainEventCount int64
	ceQuery := `
		SELECT COUNT(*)
		FROM chain_events
		WHERE chain_id = $1 AND LOWER(contract_address) = LOWER($2) AND event_name = 'Transfer'
	`
	err = pg.Pool().QueryRow(ctx, ceQuery, chainID, tokenAddr).Scan(&chainEventCount)
	if err != nil {
		t.Fatalf("failed to query chain_events count: %v", err)
	}
	t.Logf("  Total Transfer events in chain_events: %d", chainEventCount)

	// 4. Duplicate count check in token_transfers
	var duplicateCount int64
	dupQuery := `
		SELECT COUNT(*)
		FROM (
			SELECT chain_id, token, tx_hash, log_index, COUNT(*)
			FROM token_transfers
			WHERE chain_id = $1 AND LOWER(token) = LOWER($2)
			GROUP BY chain_id, token, tx_hash, log_index
			HAVING COUNT(*) > 1
		) duplicates
	`
	err = pg.Pool().QueryRow(ctx, dupQuery, chainID, tokenAddr).Scan(&duplicateCount)
	if err != nil {
		t.Fatalf("failed to query duplicate count: %v", err)
	}
	t.Logf("  Duplicate Count in token_transfers:    %d", duplicateCount)
	if duplicateCount > 0 {
		t.Fatalf("found %d duplicate records in token_transfers!", duplicateCount)
	}

	// 5. REST API verification: GET /v1/tokens/{address}/transfers
	t.Logf("--- API Verification: GET /v1/tokens/%s/transfers ---", tokenAddr)
	tokensRepo := repository.NewTokensRepository(pg.Pool())
	cfg := &config.Config{
		ChainID:      11155111,
		TokenAddress: tokenAddr,
	}
	tokenHandler := handlers.TokenTransfersHandler(tokensRepo, cfg)

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/tokens/%s/transfers?page=1&page_size=10", tokenAddr), nil)
	req.SetPathValue("address", tokenAddr)
	rec := httptest.NewRecorder()

	tokenHandler.ServeHTTP(rec, req)

	t.Logf("  API Status: %d", rec.Code)
	t.Logf("  API Response Body: %s", rec.Body.String())

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 OK from token transfers API, got %d", rec.Code)
	}
}
