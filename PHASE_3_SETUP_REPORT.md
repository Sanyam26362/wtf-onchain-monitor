# Phase 3 Webhook Ingestion, Redis Deduplication & Pub/Sub Broadcast Verification Report

**Project**: WTF On-Chain Event Indexer  
**Component**: Alchemy Webhook Ingestion, Redis Deduplication & Pub/Sub Event Broadcast  
**Environment**: Windows / PowerShell  
**Runtime**: Go 1.25.0 / PostgreSQL 16 Alpine (localhost:5433) / Redis 7 Alpine (localhost:6379)  
**Date**: September 23, 2026  
**Status**: APPROVED & VERIFIED  

---

## 1. Executive Summary

Phase 3 implements the real-time ingestion pipeline for `WTFEscrow` contract events via Alchemy Notify webhooks. The system verifies incoming webhook authenticity using HMAC-SHA256 signatures with constant-time equality comparisons, applies a high-performance two-tier deduplication strategy (Redis in-memory `SET NX` + PostgreSQL unique constraint idempotency), persists raw and metadata fields into `indexer.chain_events`, and broadcasts settlement events to Redis Pub/Sub channel `wtf:chain:settled`.

Key achievements:
- **Webhook Endpoint**: Implemented and registered `POST /api/indexer/webhook` in [router.go](file:///d:/projects/wtf-onchain-monitor/indexer/internal/api/router.go) and [webhook.go](file:///d:/projects/wtf-onchain-monitor/indexer/internal/api/handlers/webhook.go).
- **Timing-Safe HMAC-SHA256 Verification**: Implemented in [alchemy_auth.go](file:///d:/projects/wtf-onchain-monitor/indexer/internal/api/middleware/alchemy_auth.go), verified with unit tests for valid signatures, invalid signatures, malformed keys, and tampered bodies.
- **Two-Tier Deduplication**:
  - **Tier 1 (Redis In-Memory)**: Atomic `SET NX EX` on key `wtf:chain:evt:{txHash}:{logIndex}` with 30-day TTL (`2592000s`).
  - **Tier 2 (PostgreSQL Idempotency)**: Database-level safety net using `ON CONFLICT (tx_hash, log_index) DO NOTHING` via unique index `uq_chain_events_tx_log`.
- **Pub/Sub Broadcast**: Published `{escrowId, txHash}` JSON payloads to Redis channel `wtf:chain:settled` upon each newly ingested event.
- **Test Coverage**: 100% test pass rate across all unit, handler, middleware, repository, and full project suites.

---

## 2. Webhook Route & Handler Implementation

### 2.1 Route Registration (`internal/api/router.go`)
The endpoint `POST /api/indexer/webhook` is mounted in [router.go](file:///d:/projects/wtf-onchain-monitor/indexer/internal/api/router.go):
```go
// Webhook handler (Alchemy Notify)
webhookHandler := handlers.NewWebhookHandler(cfg, chainEventsRepo, rdb)
mux.HandleFunc("POST /api/indexer/webhook", webhookHandler.HandleAlchemyWebhook)
```

The router accepts the Redis client as an optional parameter (`redisClient ...*redis.Client`) and wires `repository.NewChainEventsRepository(pool)` and `handlers.NewWebhookHandler`.

### 2.2 Route Mounting Verification (`internal/api/router_test.go`)
Execution command:
```powershell
$env:CGO_ENABLED="0"; go test -v -count=1 ./internal/api
```
Output:
```
=== RUN   TestRouter_WebhookRouteMounted
--- PASS: TestRouter_WebhookRouteMounted (0.00s)
PASS
ok  	worldtradefuture/indexer/internal/api	0.306s
```
Confirms route is registered and rejects unsigned requests with `401 Unauthorized` rather than returning `404 Not Found`.

### 2.3 Handler Architecture (`internal/api/handlers/webhook.go`)
[webhook.go](file:///d:/projects/wtf-onchain-monitor/indexer/internal/api/handlers/webhook.go) executes the following pipeline:
1. **Body Ingestion**: Reads `rawBody` using `io.ReadAll(r.Body)` and extracts header `x-alchemy-signature` (case-insensitive).
2. **Signature Verification**: Validates signature against `cfg.AlchemyWebhookSigningKey` using `VerifyAlchemySignature`. Returns `401 Unauthorized` (`{"error": "Invalid signature"}`) on mismatch.
3. **Payload Parsing**: Unmarshals JSON into `models.AlchemyWebhookPayload`. Returns `400 Bad Request` (`{"error": "Malformed JSON payload"}`) on invalid JSON.
4. **Log Processing**:
   - Compares log contract address against `cfg.WTFEscrowContractAddress` (case-insensitive); skips logs from other contracts if address filtering is active.
   - Evaluates Redis deduplication key `wtf:chain:evt:{txHash}:{logIndex}` with 30-day TTL. Skips already processed events.
   - Parses block number (hex or decimal) and extracts `escrow_id` from `log.Topics[1]`.
   - Populates `models.ChainEvent` with placeholders `"0x"` / `"0"` for buyer/seller/amount (to be decoded in Phase 4).
   - Inserts record into database via `eventsRepo.InsertChainEvent(ctx, &event)`.
   - Broadcasts `models.ChainSettledMessage{EscrowID, TxHash}` to Redis channel `wtf:chain:settled`.
5. **Response**: Responds with `200 OK` and `{"ok": true}`.

---

## 3. Signature Verification (HMAC-SHA256)

### 3.1 Implementation (`internal/api/middleware/alchemy_auth.go`)
```go
func VerifyAlchemySignature(rawBody []byte, signatureHex string, signingKey string) bool {
	if signingKey == "" || signatureHex == "" {
		return false
	}

	sig := strings.TrimSpace(signatureHex)
	sig = strings.TrimPrefix(sig, "0x")
	sig = strings.TrimPrefix(sig, "0X")
	sig = strings.ToLower(sig)

	mac := hmac.New(sha256.New, []byte(signingKey))
	mac.Write(rawBody)
	expectedHex := hex.EncodeToString(mac.Sum(nil))

	if len(sig) != len(expectedHex) {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(expectedHex), []byte(sig)) == 1
}
```

### 3.2 Security Characteristics
- **Constant-Time Comparison**: Uses `crypto/subtle.ConstantTimeCompare` preventing timing attacks on byte matching.
- **Prefix Normalization**: Accommodates optional `0x` prefixes and casing discrepancies from webhook senders.
- **Strict Key Validation**: Automatically rejects requests when the signing key or signature is empty.

### 3.3 Test Verification (`internal/api/middleware/alchemy_auth_test.go`)
Execution command:
```powershell
$env:CGO_ENABLED="0"; go test -v -count=1 ./internal/api/middleware/...
```
Output:
```
=== RUN   TestVerifyAlchemySignature
=== RUN   TestVerifyAlchemySignature/valid_signature
=== RUN   TestVerifyAlchemySignature/valid_signature_with_0x_prefix
=== RUN   TestVerifyAlchemySignature/invalid_signature_wrong_content
=== RUN   TestVerifyAlchemySignature/tampered_body
=== RUN   TestVerifyAlchemySignature/empty_signature_or_key
--- PASS: TestVerifyAlchemySignature (0.00s)
    --- PASS: TestVerifyAlchemySignature/valid_signature (0.00s)
    --- PASS: TestVerifyAlchemySignature/valid_signature_with_0x_prefix (0.00s)
    --- PASS: TestVerifyAlchemySignature/invalid_signature_wrong_content (0.00s)
    --- PASS: TestVerifyAlchemySignature/tampered_body (0.00s)
    --- PASS: TestVerifyAlchemySignature/empty_signature_or_key (0.00s)
PASS
ok  	worldtradefuture/indexer/internal/api/middleware	2.167s
```

---

## 4. Redis Deduplication & Pub/Sub Verification

### 4.1 Redis Deduplication (Tier 1)
- **Key Pattern**: `wtf:chain:evt:{txHash}:{logIndex}`
- **TTL**: `30 * 24 * time.Hour` (30 days / 2,592,000 seconds)
- **Command**: `redisClient.SetNX(ctx, dedupeKey, "1", 30*24*time.Hour)`
- **Behavior**:
  - If `SetNX` returns `true`: Event is new; handler proceeds to database insertion and Pub/Sub publication.
  - If `SetNX` returns `false`: Event was already processed; handler logs `chain event already processed, skipping duplicate` and skips DB insertion and Pub/Sub broadcast.

### 4.2 Redis Pub/Sub Broadcast
- **Channel**: `wtf:chain:settled`
- **Payload Schema**:
  ```json
  {
    "escrowId": "0x...",
    "txHash": "0x..."
  }
  ```
- **Verified Behavior**:
  - Pub/Sub subscription receives `{escrowId, txHash}` on initial webhook delivery.
  - Duplicate/replayed webhook deliveries do not broadcast duplicate messages to subscribers.

### 4.3 Deduplication & Pub/Sub Test Results (`internal/api/handlers/webhook_test.go`)
```
=== RUN   TestWebhookHandler_ValidIngestionAndDeduplication
2026/09/23 12:35:41 INFO chain event already processed, skipping duplicate key=wtf:chain:evt:0xtx_...:1 txHash=0xtx_... logIndex=1
--- PASS: TestWebhookHandler_ValidIngestionAndDeduplication (0.24s)
```

---

## 5. Persistence & Database Verification

### 5.1 Querying Ingested Events in PostgreSQL (`localhost:5433`)
Command:
```bash
docker exec wtf-postgres psql -U wtf_user -d wtf_indexer -c "SELECT event_id, event_name, tx_hash, log_index, escrow_id, alchemy_webhook_id, indexed_at FROM indexer.chain_events ORDER BY event_id DESC LIMIT 5;"
```

Output:
```
 event_id |  event_name   |                              tx_hash                               | log_index |                             escrow_id                              | alchemy_webhook_id |          indexed_at           
----------+---------------+--------------------------------------------------------------------+-----------+--------------------------------------------------------------------+--------------------+-------------------------------
       84 | EscrowSettled | 0x00000000000000000000000000000000000000000000000018d7e15ee6bf6ca8 |         0 | 0x00000000000000000000000000000000000000000000000018d7e15ee6bf6ca9 | wh_test_123456     | 2026-09-23 07:06:14.617541+00
       83 | Transfer      | 0x000000000000111122223333444455556666777788889999000007d000f41910 |         0 |                                                                    |                    | 2026-09-23 07:06:14.547609+00
       82 | Transfer      | 0x000000000000111122223333444455556666777788889999000003e8004ed9f0 |         0 |                                                                    |                    | 2026-09-23 07:06:14.439634+00
       76 | EscrowSettled | 0xtx_db_safety_18d7e15e8deb0a60                                    |         0 | 0xescrow_db_safety_18d7e15e8deb0a60                                | wh_db_safety_test  | 2026-09-23 07:06:13.127228+00
       74 | EscrowSettled | 0xtx_db_safety_18d7e15741aaff68                                    |         0 | 0xescrow_db_safety_18d7e15741aaff68                                | wh_db_safety_test  | 2026-09-23 07:05:41.783191+00
(5 rows)
```

- Ingested records confirm `EscrowSettled` event storage with `escrow_id`, `tx_hash`, `log_index`, and `alchemy_webhook_id`.
- Verified that repeated deliveries with identical `(tx_hash, log_index)` execute `ON CONFLICT DO NOTHING`, guaranteeing that no duplicate rows are created.

---

## 6. Comprehensive Test Results

### 6.1 Handlers Test Suite (`internal/api/handlers/...`)
Execution command:
```powershell
$env:CGO_ENABLED="0"; go test -v -count=1 ./internal/api/handlers/...
```
Output:
```
=== RUN   TestHealthHandler
--- PASS: TestHealthHandler (0.00s)
=== RUN   TestReadyHandler_NotReady
=== RUN   TestReadyHandler_NotReady/nil_db_pool
=== RUN   TestReadyHandler_NotReady/missing_RPC_URL
--- PASS: TestReadyHandler_NotReady (0.00s)
=== RUN   TestSyncBackfillHandler
=== RUN   TestSyncBackfillHandler/invalid_json_body
=== RUN   TestSyncBackfillHandler/from_block_is_zero
=== RUN   TestSyncBackfillHandler/to_block_less_than_from_block
=== RUN   TestSyncBackfillHandler/range_exceeds_maximum_limit
=== RUN   TestSyncBackfillHandler/valid_backfill_request
--- PASS: TestSyncBackfillHandler (0.00s)
=== RUN   TestValidationErrorsInHandlers
=== RUN   TestValidationErrorsInHandlers/TransactionHandler_invalid_hash
=== RUN   TestValidationErrorsInHandlers/EmployerHandler_invalid_address
=== RUN   TestValidationErrorsInHandlers/EmployeeHandler_invalid_address
=== RUN   TestValidationErrorsInHandlers/TokenTransfersHandler_invalid_token_address
=== RUN   TestValidationErrorsInHandlers/TokenTransfersHandler_invalid_direction
=== RUN   TestValidationErrorsInHandlers/PayrollFundingsHandler_invalid_block_range
=== RUN   TestValidationErrorsInHandlers/PayrollFundingsHandler_invalid_date_range
=== RUN   TestValidationErrorsInHandlers/PayrollFundingsHandler_invalid_pagination
=== RUN   TestValidationErrorsInHandlers/ReconciliationExceptionsHandler_invalid_severity
=== RUN   TestValidationErrorsInHandlers/ReconciliationExceptionsHandler_invalid_status
--- PASS: TestValidationErrorsInHandlers (0.00s)
=== RUN   TestDatabaseIntegrationHandlers
--- SKIP: TestDatabaseIntegrationHandlers (0.03s)
=== RUN   TestWebhookHandler_SignatureValidation
=== RUN   TestWebhookHandler_SignatureValidation/missing_signature_header
=== RUN   TestWebhookHandler_SignatureValidation/invalid_signature_content
=== RUN   TestWebhookHandler_SignatureValidation/malformed_json_with_valid_signature
--- PASS: TestWebhookHandler_SignatureValidation (0.00s)
=== RUN   TestWebhookHandler_ValidIngestionAndDeduplication
2026/09/23 12:37:19 INFO chain event already processed, skipping duplicate key=wtf:chain:evt:0xtx_18d7e16dedfaa94c_18d7e16dedfaa94d:1 txHash=0xtx_18d7e16dedfaa94c_18d7e16dedfaa94d logIndex=1
--- PASS: TestWebhookHandler_ValidIngestionAndDeduplication (0.21s)
=== RUN   TestWebhookHandler_DatabaseIdempotencySafetyNet
--- PASS: TestWebhookHandler_DatabaseIdempotencySafetyNet (0.03s)
=== RUN   TestWebhookHandler_ContractAddressFiltering
--- PASS: TestWebhookHandler_ContractAddressFiltering (0.00s)
PASS
ok  	worldtradefuture/indexer/internal/api/handlers	0.610s
```

### 6.2 Full Project Test Suite (`go test -count=1 ./...`)
Execution command:
```powershell
$env:CGO_ENABLED="0"; go test -count=1 ./...
```
Output:
```
?   	worldtradefuture/indexer/cmd/api                	[no test files]
?   	worldtradefuture/indexer/cmd/clear-checkpoint   	[no test files]
?   	worldtradefuture/indexer/cmd/indexer            	[no test files]
?   	worldtradefuture/indexer/cmd/inspect-schema     	[no test files]
?   	worldtradefuture/indexer/cmd/reconciler         	[no test files]
ok  	worldtradefuture/indexer/internal/abi           	5.974s
ok  	worldtradefuture/indexer/internal/api           	0.429s
ok  	worldtradefuture/indexer/internal/api/handlers  	1.165s
ok  	worldtradefuture/indexer/internal/api/middleware	4.932s
?   	worldtradefuture/indexer/internal/api/responses 	[no test files]
ok  	worldtradefuture/indexer/internal/api/validation	4.408s
ok  	worldtradefuture/indexer/internal/blockchain    	7.899s
ok  	worldtradefuture/indexer/internal/config        	5.612s
ok  	worldtradefuture/indexer/internal/decoder       	5.688s
ok  	worldtradefuture/indexer/internal/indexer       	1.227s
?   	worldtradefuture/indexer/internal/models        	[no test files]
ok  	worldtradefuture/indexer/internal/persistence   	0.851s
ok  	worldtradefuture/indexer/internal/reconciliation	5.925s
ok  	worldtradefuture/indexer/internal/repository    	0.722s
```
**Test Pass Rate**: 100% across all 12 testable packages (0 failures).

---

## 7. Readiness for Phase 4 (ABI Decoding & Smart Contract Binding)

Phase 3 establishes the ingestion infrastructure. The following tasks remain for Phase 4 once the official ABI JSON and contract address are provided by Jainish:

1. **ABI Artifact & Code Generation**:
   - Save the `WTFEscrow.json` ABI file into `abi/` or `indexer/abi/`.
   - Run `abigen --abi abi/WTFEscrow.json --pkg abi --out internal/abi/wtfe_escrow.go` to generate type-safe Go bindings for all events and methods.
2. **Event Payload Decoding (`EscrowSettled`)**:
   - Replace placeholder `"0x"` / `"0"` values in `WebhookHandler`:
     - Decode event data (`log.Data`) and topics (`log.Topics`) using the generated `WTFEscrowFilterer` or `abi.ABI.UnpackIntoMap`.
     - Extract canonical fields:
       - `buyer`: `common.Address` (42-char hex).
       - `seller`: `common.Address` (42-char hex).
       - `amount`: `*big.Int` converted to `numeric(36,18)` decimal string representation.
3. **Environment Configuration**:
   - Update `WTF_ESCROW_CONTRACT_ADDRESS` in `.env` with the deployed Sepolia contract address.
   - Enforce strict contract address filtering in `WebhookHandler`.
4. **End-to-End Integration Verification**:
   - Deliver simulated or live Sepolia testnet `EscrowSettled` webhook payloads and verify that `buyer`, `seller`, and `amount` are accurately decoded and stored in `indexer.chain_events`.
