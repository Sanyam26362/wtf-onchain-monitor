package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/blockchain"
	"worldtradefuture/indexer/internal/decoder"
)

// TestRetry_429ErrorDetection tests Task 10.1: 429 error detection and classification.
func TestRetry_429ErrorDetection(t *testing.T) {
	cases429 := []struct {
		err  error
		want bool
	}{
		{errors.New("429 Too Many Requests"), true},
		{errors.New("HTTP 429: Too Many Requests"), true},
		{errors.New("rate limit exceeded"), true},
		{errors.New("ratelimit reached"), true},
		{errors.New("project rate limit reached"), true},
		{errors.New("daily request count exceeded"), true},
		{errors.New(`{"code":-32005,"message":"project rate limit reached"}`), true},
		{errors.New("exceeded its compute units per second"), true},
		{errors.New("connection reset by peer"), false},
		{errors.New("invalid ABI syntax"), false},
		{nil, false},
	}

	for _, tc := range cases429 {
		got := Is429Error(tc.err)
		if got != tc.want {
			t.Errorf("Is429Error(%v) = %v; want %v", tc.err, got, tc.want)
		}
		if tc.want && !IsRetryableRPCError(tc.err) {
			t.Errorf("expected 429 error %v to be classified as retryable", tc.err)
		}
	}

	// Verify transient non-429 errors are also retryable
	transientErrors := []error{
		errors.New("connection reset by peer"),
		errors.New("connection refused"),
		errors.New("broken pipe"),
		errors.New("unexpected EOF"),
		errors.New("i/o timeout"),
		errors.New("502 Bad Gateway"),
		errors.New("503 Service Unavailable"),
		errors.New("504 Gateway Timeout"),
		errors.New("500 Internal Server Error"),
		errors.New("simulated RPC failure on GetLogs"),
	}

	for _, err := range transientErrors {
		if !IsRetryableRPCError(err) {
			t.Errorf("expected transient error %v to be retryable", err)
		}
	}

	// Verify permanent errors are NOT retryable
	permanentErrors := []error{
		context.Canceled,
		errors.New("abi: cannot marshal in to Go type"),
		errors.New("execution reverted"),
		errors.New("invalid method name"),
	}

	for _, err := range permanentErrors {
		if IsRetryableRPCError(err) {
			t.Errorf("expected permanent error %v NOT to be retryable", err)
		}
	}
}

// TestRetry_RetryAfter429 tests Task 10.2: successful retry after encountering 429 rate limit.
func TestRetry_RetryAfter429(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	var recordedDurations []time.Duration
	mockSleeper := func(ctx context.Context, d time.Duration) error {
		recordedDurations = append(recordedDurations, d)
		return nil
	}

	policy := RetryPolicy{
		MaxRetries:     5,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		BackoffFactor:  2.0,
		Sleeper:        mockSleeper,
	}

	retryer := NewRetryer(policy)

	err := retryer.RetryRange(ctx, "monthly_payroll", 100, 149, func() error {
		callCount++
		if callCount < 3 {
			return errors.New("429 Too Many Requests")
		}
		return nil // Succeeded on attempt 3
	})

	if err != nil {
		t.Fatalf("expected nil error after retry, got: %v", err)
	}

	if callCount != 3 {
		t.Fatalf("expected 3 calls, got: %d", callCount)
	}

	if len(recordedDurations) != 2 {
		t.Fatalf("expected 2 backoff sleeps, got: %d", len(recordedDurations))
	}
}

// TestRetry_ExponentialBackoff tests Task 10.3: exponential backoff doubling and capping.
func TestRetry_ExponentialBackoff(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	var recordedDurations []time.Duration
	mockSleeper := func(ctx context.Context, d time.Duration) error {
		recordedDurations = append(recordedDurations, d)
		return nil
	}

	policy := RetryPolicy{
		MaxRetries:     6,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     5 * time.Second, // Cap at 5s
		BackoffFactor:  2.0,
		Sleeper:        mockSleeper,
	}

	retryer := NewRetryer(policy)

	_ = retryer.RetryRange(ctx, "monthly_payroll", 100, 149, func() error {
		callCount++
		return errors.New("429 Too Many Requests")
	})

	// 6 attempts means 5 sleeps
	expectedDurations := []time.Duration{
		1 * time.Second, // after attempt 1 (1s)
		2 * time.Second, // after attempt 2 (2s)
		4 * time.Second, // after attempt 3 (4s)
		5 * time.Second, // after attempt 4 (capped at 5s instead of 8s)
		5 * time.Second, // after attempt 5 (capped at 5s instead of 16s)
	}

	if len(recordedDurations) != len(expectedDurations) {
		t.Fatalf("expected %d sleeps, got %d", len(expectedDurations), len(recordedDurations))
	}

	for i, d := range recordedDurations {
		if d != expectedDurations[i] {
			t.Errorf("sleep %d: expected %v, got %v", i, expectedDurations[i], d)
		}
	}
}

// TestRetry_MaximumRetryLimit tests Task 10.4: maximum retry limit exhaustion.
func TestRetry_MaximumRetryLimit(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	mockSleeper := func(ctx context.Context, d time.Duration) error {
		return nil
	}

	policy := RetryPolicy{
		MaxRetries:     4,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		BackoffFactor:  2.0,
		Sleeper:        mockSleeper,
	}

	retryer := NewRetryer(policy)

	err := retryer.RetryRange(ctx, "monthly_payroll", 100, 149, func() error {
		callCount++
		return errors.New("429 Too Many Requests")
	})

	if err == nil {
		t.Fatal("expected error after exhausting max retries, got nil")
	}

	if callCount != 4 {
		t.Fatalf("expected exactly 4 attempts before failure, got: %d", callCount)
	}
}

// TestPayroll_CheckpointDoesNotAdvanceOnFailedRPC tests Task 10.5:
// When RPC rate limits persist, checkpoint MUST NOT advance.
func TestPayroll_CheckpointDoesNotAdvanceOnFailedRPC(t *testing.T) {
	ctx := context.Background()
	mockClient := &mockPayrollClient{
		latestBlock: 500,
		failGetLogs: true, // Permanent 429
	}
	mockDB := newMockPayrollPersistence()

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, err := decoder.New(payrollAddr)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	svc, err := NewWithDecoder(mockClient, dec, mockDB)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Set fast retry policy for test speed
	svc.SetRetryPolicy(RetryPolicy{
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		BackoffFactor:  2.0,
		Sleeper: func(ctx context.Context, d time.Duration) error {
			return nil
		},
	})

	// Initial checkpoint at 11082100
	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, 11155111, streamID, 11082100, "")

	opts := BackfillOptions{
		ChainID:         11155111,
		ContractAddress: payrollAddr,
		StartBlock:      11082100,
		TargetBlock:     11082200,
		BatchSize:       50,
		StreamID:        streamID,
	}

	_, err = svc.RunBackfill(ctx, opts)
	if err == nil {
		t.Fatal("expected backfill error on persistent 429 RPC failure, got nil")
	}

	// Verify checkpoint remains unchanged at 11082100
	cp, found, _ := mockDB.GetCheckpoint(ctx, opts.ChainID, streamID)
	if !found || cp != 11082100 {
		t.Fatalf("checkpoint must remain at 11082100, got: %d", cp)
	}
}

// TestPayroll_CheckpointAdvancesAfterSuccessfulRetry tests Task 10.6:
// Checkpoint advances only after a range succeeds following a transient 429.
func TestPayroll_CheckpointAdvancesAfterSuccessfulRetry(t *testing.T) {
	ctx := context.Background()
	mockClient := &flakyPayrollClient{
		latestBlock: 500,
		failAttempts: map[string]int{
			"101-150": 2, // Fail first 2 attempts for range [101, 150], succeed on 3rd
		},
		attempts: make(map[string]int),
	}
	mockDB := newMockPayrollPersistence()

	payrollAddr := common.HexToAddress("0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC")
	dec, err := decoder.New(payrollAddr)
	if err != nil {
		t.Fatalf("failed to create decoder: %v", err)
	}

	svc, err := NewWithDecoder(mockClient, dec, mockDB)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	svc.SetRetryPolicy(RetryPolicy{
		MaxRetries:     5,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		BackoffFactor:  2.0,
		Sleeper: func(ctx context.Context, d time.Duration) error {
			return nil
		},
	})

	streamID := "monthly_payroll"
	_ = mockDB.SaveCheckpoint(ctx, 11155111, streamID, 100, "")

	opts := BackfillOptions{
		ChainID:         11155111,
		ContractAddress: payrollAddr,
		StartBlock:      100,
		TargetBlock:     200, // 101..150 then 151..200
		BatchSize:       50,
		StreamID:        streamID,
	}

	lastIndexed, err := svc.RunBackfill(ctx, opts)
	if err != nil {
		t.Fatalf("expected backfill to succeed after retry, got: %v", err)
	}

	if lastIndexed != 200 {
		t.Fatalf("expected last indexed block 200, got: %d", lastIndexed)
	}

	cp, found, _ := mockDB.GetCheckpoint(ctx, opts.ChainID, streamID)
	if !found || cp != 200 {
		t.Fatalf("expected checkpoint to reach 200, got: %d", cp)
	}
}

// flakyPayrollClient simulates temporary 429 rate limit failures on specific ranges.
type flakyPayrollClient struct {
	latestBlock  uint64
	failAttempts map[string]int
	attempts     map[string]int
}

func (m *flakyPayrollClient) LatestBlock(ctx context.Context) (uint64, error) {
	return m.latestBlock, nil
}

func (m *flakyPayrollClient) GetLogs(ctx context.Context, contractAddress common.Address, fromBlock uint64, toBlock uint64) ([]types.Log, error) {
	key := fmt.Sprintf("%d-%d", fromBlock, toBlock)
	m.attempts[key]++
	if limit, ok := m.failAttempts[key]; ok && m.attempts[key] <= limit {
		return nil, errors.New("429 Too Many Requests (simulated rate limit)")
	}
	return nil, nil
}

func (m *flakyPayrollClient) GetTokenLogs(ctx context.Context, tokenAddress common.Address, topic common.Hash, fromBlock uint64, toBlock uint64) ([]types.Log, error) {
	return nil, nil
}

func (m *flakyPayrollClient) TransactionMetadata(ctx context.Context, txHash common.Hash) (*blockchain.TransactionMetadata, error) {
	return &blockchain.TransactionMetadata{
		Hash:        txHash,
		BlockNumber: 100,
		Sender:      common.HexToAddress("0x1111111111111111111111111111111111111111"),
		GasUsed:     21000,
		Status:      1,
	}, nil
}

func (m *flakyPayrollClient) BlockTimestamp(ctx context.Context, blockNumber uint64) (uint64, error) {
	return 1700000000, nil
}

func (m *flakyPayrollClient) BlockHeader(ctx context.Context, blockNumber uint64) (*blockchain.BlockHeader, error) {
	return &blockchain.BlockHeader{
		Number:     blockNumber,
		Hash:       common.HexToHash(fmt.Sprintf("0x%064x", blockNumber)),
		ParentHash: common.HexToHash(fmt.Sprintf("0x%064x", blockNumber-1)),
		Timestamp:  1700000000 + blockNumber,
	}, nil
}

func (m *flakyPayrollClient) Close() {}

func (m *flakyPayrollClient) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return nil, nil
}

// TestRetry_SanitizeSecretsInErrors tests that API keys in URLs are never logged.
func TestRetry_SanitizeSecretsInErrors(t *testing.T) {
	rawErr := errors.New("Post \"https://sepolia.infura.io/v3/9476a0582b424d10b9465fcec1cce50d\": 429 Too Many Requests")
	sanitized := SanitizeError(rawErr)

	if sanitized == rawErr.Error() {
		t.Fatal("expected API key to be sanitized from error string")
	}

	if containsSubstring(sanitized, "9476a0582b424d10b9465fcec1cce50d") {
		t.Fatal("sanitized error still contains the secret Infura project ID!")
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) > 0 && indexOf(s, substr) >= 0)
}

func indexOf(s, substr string) int {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
