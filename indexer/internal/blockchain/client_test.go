package blockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

// testHeaderTimestamp is the block timestamp encoded in the canned JSON-RPC fixture below.
const testHeaderTimestamp = uint64(0x65000000)

func fullHeaderMap(number uint64, hash, parentHash common.Hash) map[string]interface{} {
	return map[string]interface{}{
		"number":           hexutil.EncodeUint64(number),
		"hash":             hash.Hex(),
		"parentHash":       parentHash.Hex(),
		"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		"miner":            "0x0000000000000000000000000000000000000000",
		"stateRoot":        common.Hash{}.Hex(),
		"transactionsRoot": common.Hash{}.Hex(),
		"receiptsRoot":     common.Hash{}.Hex(),
		"logsBloom":        hexutil.Encode(make([]byte, 256)),
		"difficulty":       "0x0",
		"gasLimit":         "0x1c9c380",
		"gasUsed":          "0x5208",
		"timestamp":        hexutil.EncodeUint64(testHeaderTimestamp),
		"extraData":        "0x",
		"mixHash":          common.Hash{}.Hex(),
		"nonce":            "0x0000000000000000",
	}
}

func TestClient_BlockHeader_Success(t *testing.T) {
	expectedBlock := uint64(12345)
	expectedHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	expectedParentHash := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     interface{}   `json:"id"`
			Method string        `json:"method"`
			Params []interface{} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Method == "eth_getBlockByNumber" {
			res := map[string]interface{}{
				"jsonrpc": "2.0",
				"id":      req.ID,
				"result":  fullHeaderMap(expectedBlock, expectedHash, expectedParentHash),
			}
			_ = json.NewEncoder(w).Encode(res)
			return
		}

		http.Error(w, fmt.Sprintf("unhandled method: %s", req.Method), http.StatusNotFound)
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	header, err := client.BlockHeader(ctx, expectedBlock)
	if err != nil {
		t.Fatalf("BlockHeader failed: %v", err)
	}

	if header.Number != expectedBlock {
		t.Errorf("expected block number %d, got %d", expectedBlock, header.Number)
	}
	if header.ParentHash != expectedParentHash {
		t.Errorf("expected parent hash %s, got %s", expectedParentHash.Hex(), header.ParentHash.Hex())
	}
	if header.Timestamp != testHeaderTimestamp {
		t.Errorf("expected timestamp %d, got %d", testHeaderTimestamp, header.Timestamp)
	}
	// header.Hash() is derived from the RLP encoding of the header fields, so it
	// intentionally does not equal the "hash" field of the JSON-RPC response. Assert
	// instead that it is populated and distinct from ParentHash, which is what catches
	// a Hash/ParentHash transposition in the implementation.
	if header.Hash == (common.Hash{}) {
		t.Errorf("expected non-empty block hash, got %s", header.Hash.Hex())
	}
	if header.Hash == header.ParentHash {
		t.Errorf("block hash must not equal parent hash (fields transposed?): %s", header.Hash.Hex())
	}
	if header.Hash == expectedParentHash {
		t.Errorf("block hash was populated from ParentHash: %s", header.Hash.Hex())
	}
}

func TestClient_BlockHeader_ErrorPropagation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID interface{} `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		res := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"error": map[string]interface{}{
				"code":    -32000,
				"message": "header not found for given block number",
			},
		}
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.BlockHeader(ctx, 99999999)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !strings.Contains(err.Error(), "failed to fetch block header for 99999999") {
		t.Errorf("unexpected error format: %v", err)
	}
}

func TestClient_BlockHeader_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = client.BlockHeader(ctx, 100)
	if err == nil {
		t.Fatal("expected context canceled error, got nil")
	}
}

func TestClient_BlockHeader_RetryOnTransientFailure(t *testing.T) {
	var attempts int32
	expectedBlock := uint64(500)
	expectedHash := common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555")
	expectedParentHash := common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&attempts, 1)
		var req struct {
			ID interface{} `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		if cur < 2 {
			// Simulate transient 500 error on first attempt
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		res := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  fullHeaderMap(expectedBlock, expectedHash, expectedParentHash),
		}
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	header, err := client.BlockHeader(context.Background(), expectedBlock)
	if err != nil {
		t.Fatalf("expected success on retry, got err: %v", err)
	}

	if header.Number != expectedBlock {
		t.Fatalf("unexpected header block number: %d", header.Number)
	}
	if header.ParentHash != expectedParentHash {
		t.Fatalf("unexpected header parent hash: %s", header.ParentHash.Hex())
	}
	if header.Timestamp != testHeaderTimestamp {
		t.Fatalf("unexpected header timestamp: %d", header.Timestamp)
	}
	if header.Hash == (common.Hash{}) {
		t.Fatalf("unexpected empty block hash: %s", header.Hash.Hex())
	}
	if header.Hash == header.ParentHash {
		t.Fatalf("block hash must not equal parent hash (fields transposed?): %s", header.Hash.Hex())
	}

	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts, got %d", atomic.LoadInt32(&attempts))
	}
}
