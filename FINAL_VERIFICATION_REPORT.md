# Final Verification Report: WTF On-Chain Indexer

**Date:** 2026-09-28
**Engineer:** Principal QA & Blockchain Infrastructure
**Overall Status:** PASSED (with config fixes applied)

---

## 1. Environment & Configuration Check

| Variable | Value / Status | Verification |
|---|---|---|
| `RPC_URL` / `ETH_RPC_URL` | `https://eth-sepolia.g.alchemy.com/v2/alch_g-...` | PASS - Valid Alchemy Sepolia HTTPS endpoint |
| `ALCHEMY_WEBHOOK_SIGNING_KEY` | `whsec_...[REDACTED]` | PASS - Real whsec_ prefixed key (override at line 70 takes effect) |
| `DATABASE_URL` | `postgres://wtf_user:wtf_password@localhost:5433/wtf_indexer` | PASS - PostgreSQL container wtf-postgres healthy on port 5433 |
| `REDIS_URL` | `localhost:6379` | PASS - Redis container wtf-redis healthy; PONG received |
| `ESCROW_CONTRACT_ADDRESS` | `0x807EB6317FbdF219C18B58ac0BF941bC4af268D5` | PASS - Correct |
| `START_BLOCK` | `11732519` | PASS - Valid recent Sepolia block |
| `ESCROW_START_BLOCK` | `11732519` (added by fix) | FIXED - Was missing; defaulted to 0, causing Alchemy 10-block range error |

### Pre-existing .env Defects Fixed During This Audit

1. ETH_RPC_URL and RPC_URL at lines 68-69 had unclosed double-quotes causing 404 Apache responses
2. ESCROW_START_BLOCK was absent; indexer scanned from block 0, hitting Alchemy free-tier limit
3. Both issues are now corrected in indexer/.env

---

## 2. Ingestion Pipeline Results

### 2a. Alchemy Webhook (POST /api/indexer/webhook)

Status: HTTP 200 OK - PASSED

#### Root Cause of Prior 400 Bad Request

The 400 did NOT originate from HMAC signature failure (that returns 401 Unauthorized).
The failure was a JSON unmarshal error: test tooling sent logIndex as hex string ("0x0")
instead of JSON integer (0). Real Alchemy webhooks always deliver integers.

#### HMAC Verification (Confirmed Correct)

- Strips 0x/0X prefix, lowercases hex signature
- Computes HMAC-SHA256(signingKey, rawBody)
- Uses crypto/subtle.ConstantTimeCompare (timing-safe)
- Returns 401 on bad/missing signature - confirmed

#### Live Test Result

POST /api/indexer/webhook
x-alchemy-signature: 9227eff...[REDACTED]
-> HTTP 200 OK {"ok": true}

---

### 2b. RPC Live Poller (bin/indexer.exe --stream=escrow)

Status: CONNECTED & POLLING - PASSED

Latest Sepolia Block:    11799735
Backfill Target Block:   11732530
escrow backfill starting  from_block=11732519 target_block=11732530 batch_size=10
escrow range indexed      from=11732519 to=11732528 events=0 checkpoint=11732528
escrow range indexed      from=11732529 to=11732530 events=0 checkpoint=11732530
escrow backfill complete  last_block=11732530 total_events=0

Zero RPC errors. 0 events is expected (no contract interactions in those 11 blocks).
Redis connected successfully on startup.

Root cause of prior 404 error: unclosed quote in RPC_URL resolved to rpc.sepolia.org - now fixed.

---

## 3. Storage & Downstream Systems

### 3a. PostgreSQL (indexer.escrow_events) - VERIFIED

Schema (post migration 000006):
- escrow_id: NUMERIC(78,0)  - 256-bit EVM safe, no int overflow
- amount:    NUMERIC(78,0)  - 256-bit EVM safe
- Constraints: uq_escrow_event (chain_id, contract_address, tx_hash, log_index)
               uq_escrow_events_tx_log (tx_hash, log_index)

Live Data:
- Total events stored: 19
- Distinct transactions: 16
- Events with escrow_id: 19 (100%)
- Events with amount: 13
- Column types confirmed via pg_typeof(): numeric / numeric

Idempotency Test: Duplicate webhook sent twice - DB count remained = 1. PASSED.
Redis dedup key blocked second write at application layer.

---

### 3b. Redis Pub/Sub (wtf:chain:settled & wtf:chain:events) - CONFIRMED

Live subscription captured during webhook delivery:

Channel: wtf:chain:events
{"amount":"","escrowId":"3","eventType":"Unknown","txHash":"0xfeedfeed...0001"}

Channel: wtf:chain:settled
{"amount":"","escrowId":"3","eventType":"Unknown","txHash":"0xfeedfeed...0001"}

Dedup key: wtf:chain:evt:{txHash}:{logIndex} TTL=30d, EXISTS=1. PASSED.

---

### 3c. API Endpoints - ALL PASSING

| Endpoint | HTTP Status | Result |
|---|---|---|
| GET /health | 200 OK | {"data":{"status":"ok"}} |
| GET /ready | 200 OK | {"data":{"status":"ready","database":"connected","chain_id":11155111}} |
| GET /api/chain/events/1 | 200 OK | JSON array with event for escrow_id=1 |
| GET /v1/escrow/1/events | 200 OK | Same data (backward-compatible route) |
| POST /api/indexer/webhook (no sig) | 401 Unauthorized | {"error":"Invalid signature"} |
| POST /api/indexer/webhook (valid) | 200 OK | {"ok":true} |

---

## 4. Unresolved Issues & Action Items

### Critical (Must Fix Before Mainnet)

1. OPERATOR_API_KEY is a placeholder ("your_operator_secret_api_key_here")
   - Generate a strong random secret before deploying

2. Alchemy Free Tier - 10-block range limit
   - Indexer fails on batch sizes > 10 with free tier
   - Upgrade to PAYG or keep BLOCK_BATCH_SIZE=10

3. ESCROW_START_BLOCK was 0 - FIXED in this audit

### Medium Priority

4. Webhook 400 when logIndex is hex string - affects test harness only
5. amount empty for Unknown events - add topic hashes for new event types
6. Silent Redis publish failures - add WARN logging
7. No distributed tracing - add OpenTelemetry for production

### Mainnet Readiness Checklist

[ ] Replace OPERATOR_API_KEY placeholder
[ ] Upgrade Alchemy plan or set BLOCK_BATCH_SIZE=10
[ ] Set LIVE_MONITOR_ENABLED=true for real-time block following
[ ] Set ESCROW_START_BLOCK to contract deployment block on mainnet
[ ] Set APP_ENV=production and DEPLOYMENT_ENVIRONMENT=production
[ ] Run full migration suite against production PostgreSQL
[ ] Add health check integration with container orchestrator
[ ] Configure Redis persistence (AOF/RDB) to survive restarts

---

## Appendix: Infrastructure Summary

Container         Port Mapping    Status
-----------       --------------- --------
wtf-postgres      5433 -> 5432    Healthy
wtf-redis         6379 -> 6379    Healthy
API Server        :8080           Running (bin/api.exe)
ngrok tunnel      -> :8080        Running

Schema Migrations Applied:
  000001 initial_schema
  000002 token_transfers_and_checkpoints
  000003 api_indexes
  000004 add_foreign_keys
  000005 escrow_events
  000006 align_escrow_schema  <- escrow_id/amount -> NUMERIC(78,0)
