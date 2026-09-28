# WTF On-Chain Monitor

A production-grade blockchain monitoring and indexing service for the **WorldTradeFuture (WTF)** platform on Ethereum **Sepolia**.

The service provides a resilient **Dual-Path Ingestion Architecture** (combining authenticated, real-time **Alchemy Webhook push ingestion** with an autonomous **JSON-RPC batch poller and backfiller**), persists normalized records idempotently into **PostgreSQL** with high-precision numeric support (`NUMERIC(78,0)`), maintains Redis fast-path deduplication, broadcasts live state transitions via **Redis Pub/Sub**, and exposes a high-performance **REST API** layer without calling Ethereum RPC for historical GET requests.

---

## Table of Contents

1. [Development Status](#development-status)
2. [Dual-Path Architecture Overview](#dual-path-architecture-overview)
3. [Repository Structure](#repository-structure)
4. [Environment Configuration](#environment-configuration)
5. [Docker Compose Infrastructure](#docker-compose-infrastructure)
6. [WTFEscrow Dual-Path Ingestion Engine](#wtfescrow-dual-path-ingestion-engine)
7. [WTF ERC-20 Token Indexing](#wtf-erc-20-token-indexing)
8. [MonthlyPayroll Indexing](#monthlypayroll-indexing)
9. [Quick Start & Running Services](#quick-start--running-services)
10. [Continuous Live Monitoring](#continuous-live-monitoring)
11. [Reconciliation Worker (Safety Checks)](#reconciliation-worker--payroll-funding-salary-claim--token-transfer-safety-checks)
12. [REST API Reference](#rest-api-reference)
13. [API Testing with Postman & cURL](#api-testing-with-postman--curl)
14. [Automated Testing & QA Verification](#automated-testing--qa-verification)
15. [Security & Fault-Tolerance Principles](#security--fault-tolerance-principles)
16. [Troubleshooting](#troubleshooting)
17. [Roadmap](#roadmap)

---

## Development Status

- [x] **Dual-Path Ingestion Engine**:
  - [x] Reactive push webhook pipeline (`/api/indexer/webhook`) with HMAC-SHA256 verification
  - [x] Redis `SETNX` fast-path deduplication (30-day TTL, fail-closed safety)
  - [x] Redis Pub/Sub broadcast (`wtf:chain:settled`, `wtf:chain:events`)
  - [x] Durable pull RPC poller with chunked backfill and retry backoff
- [x] **Smart Contract Indexers**:
  - [x] **WTFEscrow**: Decoding 8 lifecycle events with `NUMERIC(78,0)` precision
  - [x] **MonthlyPayroll**: Ingestion of employers, employees, fundings, and salary claims
  - [x] **Generic ERC-20**: Token-agnostic `Transfer` & `Approval` indexer
- [x] **PostgreSQL Persistence**:
  - [x] Dedicated `escrow_events` table (Migrations 000001 - 000006)
  - [x] Idempotent upserts (`ON CONFLICT DO UPDATE / NOTHING`)
  - [x] Multi-column unique constraints `(chain_id, contract_address, tx_hash, log_index)`
  - [x] Reorg handling (`removed BOOLEAN` synchronization)
- [x] **Checkpointing & Fault Tolerance**:
  - [x] Stream checkpoints in `sync_checkpoints` (`wtf_escrow`, `monthly_payroll`, `erc20_transfers`)
  - [x] Crash-resilient startup and safe block clamping
- [x] **Production REST API**:
  - [x] Built with Go standard library (`net/http.ServeMux` Go 1.22+)
  - [x] Escrow lifecycle event query (`/v1/escrow/{id}/events` & `/api/chain/events/{id}`)
  - [x] Health (`/health`), readiness (`/ready`), and sync status (`/v1/sync/status`)
  - [x] Request ID tracing, CORS, structured `slog` logging, and panic recovery
- [x] **Reconciliation Worker**:
  - [x] Safety check comparison between on-chain events/balances and database projections
  - [x] Automated discrepancy logging in `reconciliation_exceptions`
- [x] **Verification & Delivery**:
  - [x] End-to-end integration and smoke verification completed ([`FINAL_VERIFICATION_REPORT.md`](FINAL_VERIFICATION_REPORT.md))

| Feature / Milestone | Status | Description |
|---|---|---|
| **Alchemy Webhook Ingestion** | Completed | Sub-second push notifications with HMAC-SHA256 auth, Redis dedup, and DB persistence |
| **WTFEscrow Contract Indexer**| Completed | Full decoding for all 8 Sepolia `WTFEscrow` lifecycle events |
| **Redis Dedup & Pub/Sub** | Completed | Redis `SETNX` fast-path dedup (fail-closed) and publishing to `wtf:chain:settled` |
| **Sepolia RPC Client** | Completed | Verified connection with retry, exponential backoff, and 429 throttling handling |
| **MonthlyPayroll Indexer** | Completed | Ingestion of `EmployerAdded/Removed`, `EmployeeAdded/Removed`, `PayrollFunded`, `SalaryClaimed` |
| **Generic ERC-20 Indexer** | Completed | Completely token-agnostic transfer indexer configurable via environment variables |
| **PostgreSQL Persistence** | Completed | Migrations 000001–000006, `escrow_events`, composite indexes, and NUMERIC precision |
| **Stream Checkpointing** | Completed | Durable block checkpoints in `sync_checkpoints` for crash-resilient restarts |
| **Production REST API** | Completed | Zero RPC calls on historical GET queries; standardized JSON envelopes |
| **Continuous Live Monitoring**| Completed | Background daemon synchronizing safe finalized blocks for all configured streams |
| **Automated Reconciliation** | Completed | Auditing payroll fundings, claims, token transfers, and contract/token balances |
| **E2E QA Verification** | Completed | Live Sepolia smoke test report available at [`FINAL_VERIFICATION_REPORT.md`](FINAL_VERIFICATION_REPORT.md) |

---

## Dual-Path Architecture Overview

```text
                           ┌─────────────────────────────────────────────────────────┐
                           │                 Ethereum Sepolia Chain                  │
                           └──────────────┬───────────────────────────┬──────────────┘
                                          │                           │
                   Alchemy Notify Push    │                           │ eth_getLogs / RPC Poll
            (X-Alchemy-Signature HMAC)    │                           │ (Chunked & Checkpointed)
                                          ▼                           ▼
                             ┌────────────────────────┐  ┌────────────────────────┐
                             │  POST /api/indexer/    │  │       WTF Indexer      │
                             │        webhook         │  │   (cmd/indexer daemon) │
                             └───────────┬────────────┘  └───────────┬────────────┘
                                         │                           │
                                         ▼                           │
                             ┌────────────────────────┐              │
                             │    Redis SETNX Dedup   │              │
                             │ (key: wtf:chain:evt:*) │              │
                             └───────────┬────────────┘              │
                                         │                           │
                                         ▼                           ▼
                             ┌────────────────────────────────────────────────────┐
                             │               PostgreSQL Database                  │
                             │   - escrow_events (NUMERIC(78,0), JSONB raw_data)  │
                             │   - sync_checkpoints (atomic stream checkpoints)   │
                             │   - chain_events, token_transfers, payroll_*       │
                             └───────────┬────────────────────────────────────────┘
                                         │                           │
                       Redis Pub/Sub     │                           │ Parameterized SQL Queries
                  (wtf:chain:settled)    │                           │ (Zero RPC calls on GET)
                                         ▼                           ▼
                             ┌────────────────────────┐  ┌────────────────────────┐
                             │ Downstream Consumers   │  │     REST API Layer     │
                             │ (BullMQ Reconciler,    │  │ (GET /v1/escrow/events,│
                             │  Trading UI)           │  │  GET /v1/transactions) │
                             └────────────────────────┘  └───────────┬────────────┘
                                                                     │ HTTP / JSON
                                                                     ▼
                                                         [ Web Dashboard / Postman ]
```

### Core Architectural Principles

1. **Dual-Path Lambda / Kappa Resilience**:
   - **Path A (Push / High Velocity)**: Webhooks push newly confirmed transactions within ~200ms–1s of on-chain mining, minimizing latency for real-time user interfaces.
   - **Path B (Pull / Deep Reliability)**: The RPC poller runs chunked historical backfills and live monitoring loops bounded by checkpoints, recovering from network partitions, missed webhooks, or system downtime.
2. **Two-Tier Deduplication & ACID Idempotency**:
   - **Layer 1 (Redis Fast-Path)**: `SETNX wtf:chain:evt:{txHash}:{logIndex} 1 EX 2592000` drops duplicate webhook deliveries before database roundtrips. Fails closed (`continue`) if Redis is temporarily unreachable.
   - **Layer 2 (PostgreSQL ACID)**: Unique constraint `CONSTRAINT uq_escrow_event UNIQUE (chain_id, contract_address, tx_hash, log_index)` with `ON CONFLICT DO UPDATE` guarantees zero duplicate rows.
3. **Reorg Safety & Removed Log Propagation**:
   - Webhook payloads and RPC poller decode the `removed` boolean. Reorged blocks update existing database records (`removed = true`), preventing phantom state from corrupting downstream settlement.
4. **Strict Separation of Concerns**:
   - The **Ingestion Layer** writes to PostgreSQL and emits Redis events.
   - The **API Layer** reads exclusively from PostgreSQL for historical queries to eliminate RPC rate limiting.
   - The **Reconciliation Layer** independently audits PostgreSQL projections against on-chain reality.

---

## Repository Structure

```text
wtf-onchain-monitor/
├── docker-compose.yml           # Local PostgreSQL (port 5433) and Redis (port 6379)
├── FINAL_VERIFICATION_REPORT.md # Live Sepolia E2E verification audit & smoke test report
├── docs/                        # Architecture specs and OpenAPI documentation
│   └── openapi.yaml             # Complete OpenAPI 3.0 REST specification
├── abi/                         # Contract ABI definitions (ERC-20, MonthlyPayroll)
└── indexer/                     # Go application codebase
    ├── cmd/
    │   ├── api/main.go          # REST API server entry point
    │   ├── indexer/main.go      # Historical backfill and live monitor daemon
    │   ├── reconciler/main.go   # Independent audit reconciliation worker
    │   └── inspect-schema/      # DB schema debugging utility
    ├── internal/
    │   ├── abi/                 # WTFEscrow, MonthlyPayroll, and ERC-20 ABI bindings
    │   ├── api/                 # REST API layer
    │   │   ├── handlers/        # Webhook, Escrow, Transaction, Payroll, Token handlers
    │   │   ├── middleware/      # HMAC verification, RequestID, CORS, Logging, Auth
    │   │   ├── responses/       # Standardized success/list/error JSON envelopes
    │   │   └── validation/      # Address, hash, block range, and pagination validators
    │   ├── blockchain/          # RPC client with retry and backoff logic
    │   ├── config/              # Centralized environment configuration
    │   ├── decoder/             # Log decoding engine
    │   ├── indexer/             # EscrowIndexer, LiveMonitor, Payroll, Token services
    │   ├── models/              # EscrowEvent, AlchemyLog, ChainSettledMessage models
    │   ├── persistence/         # PostgreSQL connection pool and write-path repositories
    │   ├── reconciliation/      # Independent discrepancy detection engine
    │   └── repository/          # Read-only query repositories for REST API
    ├── migrations/              # Database schema migrations (000001 - 000006)
    ├── .env                     # Local environment configuration (git-ignored)
    └── .env.example             # Example configuration template
```

---

## Environment Configuration

Configuration is loaded from environment variables or local `indexer/.env`. See [`indexer/.env.example`](indexer/.env.example) for a template:

| Variable | Required | Default | Description |
|---|---|---|---|
| `CHAIN_ID` | Yes | `11155111` | Target network chain ID (`11155111` for Sepolia) |
| `RPC_URL` / `ETH_RPC_URL` | Yes | - | Ethereum Sepolia HTTPS JSON-RPC endpoint (Alchemy / Infura) |
| `SEPOLIA_RPC_URL` | No | - | Fallback Sepolia JSON-RPC endpoint |
| `DATABASE_URL` | Yes | - | PostgreSQL connection URL (`postgres://user:pass@host:port/dbname`) |
| `REDIS_URL` | Yes | `localhost:6379` | Redis host:port connection string |
| `DEPLOYMENT_ENVIRONMENT`| Yes | `development` | Environment name (`development`, `staging`, `production`) |
| `ALCHEMY_WEBHOOK_SIGNING_KEY`| Yes (Non-dev)| - | HMAC-SHA256 signing secret from Alchemy Notify (`whsec_...`) |
| `ESCROW_CONTRACT_ADDRESS`| Yes | `0x807EB6317FbdF219C18B58ac0BF941bC4af268D5` | Deployed WTFEscrow contract address on Sepolia |
| `WTF_ESCROW_CONTRACT_ADDRESS`| No | - | Backward-compatible alias for `ESCROW_CONTRACT_ADDRESS` |
| `ESCROW_START_BLOCK` | Yes | `11732500` | Deployment block height for WTFEscrow |
| `ESCROW_STREAM_ID` | No | `wtf_escrow` | Stream identifier recorded in `sync_checkpoints` |
| `PAYROLL_CONTRACT_ADDRESS`| No | `0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC` | Deployed MonthlyPayroll contract address |
| `START_BLOCK` | No | `11080692` | Initial block number for MonthlyPayroll indexing |
| `TOKEN_ADDRESS` | No | `0x378AFb93CaDd39AFF154704d2D90Af8c401137E7` | Deployed WTF ERC-20 token address |
| `TOKEN_ABI_PATH` | No | `./abi/erc20.json` | Path to ERC-20 ABI JSON definition |
| `TOKEN_START_BLOCK` | No | `11717931` | Initial block number for token indexing |
| `CONFIRMATION_DEPTH` | Yes | `5` | Required block confirmations before considering events safe |
| `BLOCK_BATCH_SIZE` | No | `50` | Maximum block span fetched per RPC batch |
| `POLLING_INTERVAL` | Yes | `12s` | Polling interval duration for historical backfiller |
| `LIVE_POLL_INTERVAL` | No | `5s` | Sleep duration between polling cycles in live mode |
| `API_HOST` | No | `0.0.0.0` | Bind address for REST API |
| `API_PORT` | No | `8080` | Port for REST API |
| `OPERATOR_API_KEY` | For Operator| - | Secret key protecting operator endpoints |
| `RPC_MAX_RETRIES` | No | `5` | Maximum retries on transient RPC failures and 429 rate limits |
| `RPC_INITIAL_BACKOFF` | No | `1s` | Initial exponential backoff duration before first retry |
| `RPC_MAX_BACKOFF` | No | `30s` | Maximum ceiling for exponential backoff delay |

---

## Docker Compose Infrastructure

The repository includes a ready-to-run [`docker-compose.yml`](docker-compose.yml) providing local instances of PostgreSQL and Redis:

```bash
# Start PostgreSQL (port 5433) and Redis (port 6379) in the background
docker compose up -d

# Verify container health
docker compose ps

# View container logs
docker compose logs -f
```

- **PostgreSQL**: Bound to local port `5433` (mapped from container `5432`) to avoid conflicts with existing system PostgreSQL instances. Credentials: `wtf_user` / `wtf_password` on database `wtf_indexer`.
- **Redis**: Bound to port `6379`. Accessible without password for local development.

---

## WTFEscrow Dual-Path Ingestion Engine

### 1. Smart Contract Event Specification

The deployed `WTFEscrow` contract (`0x807EB6317FbdF219C18B58ac0BF941bC4af268D5`) emits 8 distinct lifecycle events, all dynamically decoded by the ingestion engine:

```solidity
event BudgetDeposited(uint256 indexed escrowId, address indexed depositor, uint256 amount);
event BudgetWithdrawn(uint256 indexed escrowId, address indexed recipient, uint256 amount);
event DisputeRaised(uint256 indexed escrowId, address indexed plaintiff);
event DisputeResolved(uint256 indexed escrowId, address indexed resolver);
event EscrowClaimed(uint256 indexed escrowId, address indexed claimant, uint256 amount);
event EscrowCreated(uint256 indexed escrowId, address indexed client, address indexed provider, uint256 amount);
event EscrowRefunded(uint256 indexed escrowId, address indexed recipient, uint256 amount);
event EscrowReleased(uint256 indexed escrowId, address indexed recipient, uint256 amount);
```

### 2. Path A: Alchemy Webhook Ingestion (`POST /api/indexer/webhook`)

- **HMAC-SHA256 Signature Verification**: In `internal/api/middleware/alchemy_auth.go`, incoming requests are validated against the `X-Alchemy-Signature` header computed over the raw request body using `ALCHEMY_WEBHOOK_SIGNING_KEY`.
- **Redis Fast-Path Deduplication**:
  - Key: `wtf:chain:evt:{txHash}:{logIndex}` (TTL: 30 days).
  - Uses `SETNX` to guarantee that duplicate webhook deliveries are acknowledged with HTTP 200 without executing duplicate database writes.
  - **Fail-Closed Safety (`BUG-001`)**: If Redis is temporarily unreachable, the handler logs a warning and drops the delivery (`continue`), allowing the RPC poller to catch the event cleanly without producing duplicate downstream Pub/Sub broadcasts.
- **Reorganization Handling (`BUG-003`)**:
  - Maps `log.Removed` directly from the Alchemy payload to the database record.
- **Downstream Pub/Sub Publishing**:
  - Successful ingestions publish a structured JSON payload to Redis channel `wtf:chain:settled` and `wtf:chain:events`:
    ```json
    {
      "eventType": "EscrowCreated",
      "escrowId": "1",
      "txHash": "0x4b7c...",
      "blockNumber": 11732550,
      "contractAddress": "0x807EB6317FbdF219C18B58ac0BF941bC4af268D5",
      "timestamp": "2026-09-28T09:50:00Z"
    }
    ```

### 3. Path B: RPC Batch Poller (`cmd/indexer`)

- Polls Ethereum Sepolia using chunked `eth_getLogs` filtered by `ESCROW_CONTRACT_ADDRESS` and all 8 event topic hashes.
- Tracks sync checkpoints in `sync_checkpoints` under stream `wtf_escrow`.
- Handles contract creation boundaries: safely clamps the starting block to `ESCROW_START_BLOCK` (`11732500+`) to avoid free-tier RPC provider block-span limits.

### 4. Database Schema: `escrow_events` Table

Created in migration `000005_escrow_events.up.sql` and aligned in `000006_align_escrow_schema.up.sql`:

```sql
CREATE TABLE IF NOT EXISTS escrow_events (
    id               BIGSERIAL PRIMARY KEY,
    chain_id         BIGINT        NOT NULL,
    contract_address VARCHAR(42)   NOT NULL,
    event_type       VARCHAR(64)   NOT NULL,
    tx_hash          VARCHAR(66)   NOT NULL,
    block_number     BIGINT        NOT NULL,
    block_timestamp  TIMESTAMP WITH TIME ZONE NOT NULL,
    log_index        INTEGER       NOT NULL,
    removed          BOOLEAN       NOT NULL DEFAULT FALSE,
    escrow_id        NUMERIC(78, 0),
    amount           NUMERIC(78, 0),
    raw_data         JSONB         NOT NULL DEFAULT '{}',
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_escrow_event UNIQUE (chain_id, contract_address, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_escrow_events_escrow_id ON escrow_events (escrow_id) WHERE escrow_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_escrow_events_event_type ON escrow_events (event_type);
CREATE INDEX IF NOT EXISTS idx_escrow_events_chain_block ON escrow_events (chain_id, block_number);
```

---

## WTF ERC-20 Token Indexing

The ERC-20 indexer is designed to be **token-agnostic** while providing isolated checkpointing and robust start-block safety for the **WorldTradeFuture (WTF) Token** on Sepolia (`0x378AFb93CaDd39AFF154704d2D90Af8c401137E7`).

- **ABI Validation**: Dynamically parses standard ERC-20 `Transfer` and `Approval` event signatures from [`abi/erc20.json`](abi/erc20.json).
- **Stream Isolation**: Checkpoints are stored in `sync_checkpoints` under stream ID `erc20_transfers_0x378AFb93CaDd39AFF154704d2D90Af8c401137E7`.
- **Start-Block Clamping**: Protects against querying blocks prior to contract deployment.

---

## MonthlyPayroll Indexing

Tracks the Sepolia `MonthlyPayroll` smart contract (`0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC`):

- **Events Ingested**: `EmployerAdded`, `EmployerRemoved`, `EmployeeAdded`, `EmployeeRemoved`, `PayrollFunded`, `SalaryClaimed`.
- **Domain Projections**: Automatically maintains relational profiles in `employers`, `employees`, `payroll_fundings`, and `salary_claims`.

---

## Quick Start & Running Services

### Prerequisites

- **Go**: Version 1.22 or higher
- **Docker & Docker Compose**: For local PostgreSQL and Redis
- **Sepolia RPC URL**: Valid Alchemy or Infura JSON-RPC URL

### 1. Boot Local Infrastructure

```bash
docker compose up -d
```

### 2. Configure Environment

Copy `indexer/.env.example` to `indexer/.env` and verify required parameters:

```env
CHAIN_ID=11155111
RPC_URL=https://eth-sepolia.g.alchemy.com/v2/YOUR_ALCHEMY_KEY
DATABASE_URL=postgres://wtf_user:wtf_password@localhost:5433/wtf_indexer?sslmode=disable
REDIS_URL=localhost:6379
ALCHEMY_WEBHOOK_SIGNING_KEY=whsec_your_signing_key
ESCROW_CONTRACT_ADDRESS=0x807EB6317FbdF219C18B58ac0BF941bC4af268D5
ESCROW_START_BLOCK=11732500
```

### 3. Build & Run Services

#### Start the REST API Server:
```bash
cd indexer
go run ./cmd/api
```
*Or build and run the binary:*
```bash
go build -o bin/api.exe ./cmd/api
./bin/api.exe
```

#### Run WTFEscrow Historical Backfill:
```bash
cd indexer
go run ./cmd/indexer --stream escrow --from-block 11732500 --to-block 11732600 --batch-size 50
```

#### Expose API via ngrok for Alchemy Webhook Ingestion:
```bash
npx ngrok http 8080
```
Configure your Alchemy Notify Webhook target URL to:
`https://<your-ngrok-subdomain>.ngrok-free.app/api/indexer/webhook`

---

## Continuous Live Monitoring

The live monitor continuously polls Ethereum Sepolia for newly confirmed safe blocks across all configured contract streams (`escrow`, `payroll`, `token`):

```bash
cd indexer

# Start continuous live monitoring for all streams
go run ./cmd/indexer --live --poll-interval 5s --confirmations 5

# Start live monitoring specifically for WTFEscrow
go run ./cmd/indexer --live --stream escrow --poll-interval 5s
```

- **Confirmation Depth**: Queries only safe finalized blocks (`latestBlock - CONFIRMATIONS`).
- **Cooperative Streaming**: Iterates bounded batches across streams to prevent starvation.
- **Graceful Shutdown**: Completes active database transactions on `SIGINT` / `SIGTERM` with zero state corruption.

---

## Reconciliation Worker — Payroll Funding, Salary Claim & Token Transfer Safety Checks

The **Reconciliation Worker** (`cmd/reconciler`) independently verifies database records against on-chain truth:

```bash
cd indexer

# Reconcile contract & token balances at a specific block
go run ./cmd/reconciler -type balance -to-block 11724713

# Reconcile token transfers within recent window
go run ./cmd/reconciler -type token-transfers -window 500

# Reconcile salary claims
go run ./cmd/reconciler -type salary-claims -from-block 11080692 -to-block 11084000

# Run all reconciliation checks in a continuous daemon loop
go run ./cmd/reconciler -type all -loop -interval 30s
```

Mismatches are idempotently recorded in `reconciliation_exceptions` and can be queried via `GET /v1/reconciliation/exceptions`.

---

## REST API Reference

### Health & Readiness Probes

#### `GET /health`
Liveness probe returning immediate HTTP 200 without executing DB queries or RPC calls.

#### `GET /ready`
Readiness probe verifying PostgreSQL connection, Redis ping, and runtime configuration:
```json
{
  "data": {
    "chain_id": 11155111,
    "database": "connected",
    "environment": "development",
    "escrow_contract": "0x807EB6317FbdF219C18B58ac0BF941bC4af268D5",
    "payroll_contract": "0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC",
    "generic_token_contract": "0x378AFb93CaDd39AFF154704d2D90Af8c401137E7",
    "status": "ready"
  }
}
```

---

### Webhook & Escrow Endpoints

#### `POST /api/indexer/webhook`
- **Purpose**: Authenticated ingestion of Alchemy Notify mined transaction webhooks.
- **Headers**:
  - `Content-Type: application/json`
  - `X-Alchemy-Signature: <hmac-sha256-signature>`
- **Response**: HTTP 200 `{"status":"ok"}`.

#### `GET /v1/escrow/{id}/events` & `GET /api/chain/events/{id}`
- **Purpose**: Retrieves all indexed lifecycle events for a specific Escrow ID (`escrow_id`).
- **Path Parameter**: `{id}` — Numerical Escrow ID (e.g. `1`).
- **Response**:
```json
{
  "data": [
    {
      "id": 1,
      "chain_id": 11155111,
      "contract_address": "0x807eb6317fbdf219c18b58ac0bf941bc4af268d5",
      "event_type": "EscrowCreated",
      "tx_hash": "0x4b7c858567e7d95368a5c13b355bb05d4bda21975e54d896173d1e1f76d4949a",
      "block_number": 11732550,
      "block_timestamp": "2026-09-28T09:50:00Z",
      "log_index": 0,
      "removed": false,
      "escrow_id": "1",
      "amount": "1000000000000000000",
      "raw_data": {
        "client": "0x9876...",
        "provider": "0x1234...",
        "amount": "1000000000000000000"
      },
      "created_at": "2026-09-28T09:50:02Z"
    }
  ]
}
```

---

### Sync Status & Core Domain Endpoints

| Method | Path | Description | Authentication |
|---|---|---|---|
| `GET` | `/v1/sync/status` | Reports active stream checkpoints, safe block, and lag | Public |
| `POST`| `/v1/sync/backfill`| Protected backfill trigger (Returns 501 until BullMQ queue wired) | Operator API Key |
| `GET` | `/v1/transactions/{hash}` | Transaction details, confirmations, and decoded logs | Public |
| `GET` | `/v1/employers/{address}` | Employer profile, funds, and employee directory | Public |
| `GET` | `/v1/employees/{address}` | Employee salary rate, leave, and claim history | Public |
| `GET` | `/v1/payroll/fundings` | Paginated and filterable payroll funding deposits | Public |
| `GET` | `/v1/payroll/claims` | Paginated and filterable employee salary withdrawals | Public |
| `GET` | `/v1/tokens/{address}/transfers` | Paginated ERC-20 transfers with wallet direction filters | Public |
| `GET` | `/v1/reconciliation/exceptions`| Paginated audit discrepancy exceptions | Public |

---

## API Testing with Postman & cURL

### cURL Examples

#### 1. Check Service Readiness
```bash
curl -s http://localhost:8080/ready | jq .
```

#### 2. Query Escrow Events by ID
```bash
curl -s http://localhost:8080/v1/escrow/1/events | jq .
```

#### 3. Inspect Sync Stream Checkpoints
```bash
curl -s http://localhost:8080/v1/sync/status | jq .
```

#### 4. Query ERC-20 Transfers for a Wallet
```bash
curl -s "http://localhost:8080/v1/tokens/0x378AFb93CaDd39AFF154704d2D90Af8c401137E7/transfers?wallet=0x5d1beadf6e5f9a1ea564162c797344b1337482de&direction=in" | jq .
```

---

## Automated Testing & QA Verification

### Running the Test Suite

```bash
cd indexer
# Note: Set CGO_ENABLED=0 on environments without a 64-bit GCC toolchain
go test -v -count=1 ./internal/config ./internal/api/handlers ./internal/indexer ./internal/repository
```

### End-to-End System Verification

The entire dual-path pipeline has undergone a complete pre-flight configuration audit, live webhook ingestion test with HMAC signing, direct RPC poller execution, database constraint verification, and Redis Pub/Sub audit.

The full verification report is available at [`FINAL_VERIFICATION_REPORT.md`](FINAL_VERIFICATION_REPORT.md).

**Summary of Verified Results:**
- **Alchemy Webhook (`POST /api/indexer/webhook`)**: Verified 200 OK with HMAC verification, Redis dedup, and DB insert.
- **RPC Live Poller (`bin/indexer.exe`)**: Verified live connection, block range chunking, and checkpoint advances.
- **PostgreSQL (`indexer.escrow_events`)**: Verified `NUMERIC(78,0)` precision for `escrow_id` and `amount`, and unique constraint enforcement.
- **Redis Pub/Sub (`wtf:chain:settled`)**: Verified delivery of real-time event messages.
- **REST Endpoints (`/ready`, `/v1/escrow/1/events`, `/api/chain/events/1`)**: Verified HTTP 200 OK.

---

## Security & Fault-Tolerance Principles

1. **Zero Private Keys**: The service requires zero private keys. All blockchain interactions are strictly read-only.
2. **HMAC Webhook Authentication**: Webhook payloads are verified using constant-time HMAC-SHA256 signature matching. Missing or dummy keys cause immediate startup failure in production environments.
3. **Fail-Closed Deduplication**: Redis failures prevent duplicate event processing by failing closed.
4. **Parameterized SQL Queries**: All database queries use `$1, $2, ...` parameterization via `pgxpool.Pool` to eliminate SQL injection risks.
5. **Secret Sanitization**: Passwords, API keys, and signing secrets are excluded from logs and version control.

---

## Troubleshooting

### 1. Webhook Returns HTTP 401 Unauthorized
- **Cause**: The `X-Alchemy-Signature` header does not match the computed HMAC signature of the body.
- **Solution**: Verify that `ALCHEMY_WEBHOOK_SIGNING_KEY` in `indexer/.env` matches the Signing Key shown in the Alchemy Notify dashboard.

### 2. Alchemy RPC Free-Tier Range Error
- **Cause**: Polling block range exceeds 10 blocks when starting from block 0.
- **Solution**: Ensure `ESCROW_START_BLOCK` is set to `11732500` or higher in `indexer/.env`.

### 3. PostgreSQL Port 5433 Connection Refused
- **Cause**: The Docker container `wtf-postgres` is not running.
- **Solution**: Run `docker compose up -d` and verify ports with `docker compose ps`.

### 4. Redis Connection Errors
- **Cause**: Redis container is not running on port 6379.
- **Solution**: Run `docker compose up -d` and test connection with `redis-cli ping`.

---

## Roadmap

### Completed
- [x] Dual-Path Lambda/Kappa Ingestion Architecture (Webhook Push + RPC Poll)
- [x] WTFEscrow 8-event decoding engine with `NUMERIC(78,0)` precision
- [x] Redis fast-path deduplication with fail-closed safety
- [x] Real-time Redis Pub/Sub broadcast (`wtf:chain:settled`, `wtf:chain:events`)
- [x] MonthlyPayroll contract indexing and domain projections
- [x] Generic ERC-20 transfer indexing
- [x] Stream checkpoints in `sync_checkpoints`
- [x] Production REST API with Go standard library (`net/http.ServeMux`)
- [x] Automated Reconciliation Engine for payroll, claims, token transfers, and balances
- [x] Full End-to-End QA and smoke test verification ([`FINAL_VERIFICATION_REPORT.md`](FINAL_VERIFICATION_REPORT.md))

### Future
- [ ] BullMQ background worker queue integration for asynchronous backfilling
- [ ] Next.js monitoring dashboard UI
- [ ] WebSocket / Server-Sent Events (SSE) for browser live updates
- [ ] Multi-chain indexing support (Arbitrum, Optimism, Base)
