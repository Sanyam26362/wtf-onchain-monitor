# WTF On-Chain Monitor

A production-grade blockchain monitoring and indexing service for the **WorldTradeFuture (WTF)** payroll platform on Ethereum **Sepolia**.

The service ingests smart contract events from Sepolia, persists normalized records idempotently into **PostgreSQL**, tracks stream synchronization checkpoints, and provides a high-performance **REST API** layer for the monitoring dashboard and operational tooling without calling Ethereum RPC for normal historical GET requests.

---

## Table of Contents

1. [Development Status](#development-status)
2. [Architecture Overview](#architecture-overview)
3. [Repository Structure](#repository-structure)
4. [Environment Configuration](#environment-configuration)
5. [Generic ERC-20 Token Indexing](#generic-erc-20-token-indexing)
6. [Quick Start & Running Services](#quick-start--running-services)
7. [Continuous Live Monitoring](#continuous-live-monitoring)
8. [Reconciliation Worker (Safety Checks)](#reconciliation-worker--payroll-funding-salary-claim--token-transfer-safety-checks)
9. [REST API Reference](#rest-api-reference)
10. [API Testing with Postman & cURL](#api-testing-with-postman--curl)
11. [Automated Testing](#automated-testing)
12. [Security Notes](#security-notes)
13. [Troubleshooting](#troubleshooting)
14. [Roadmap](#roadmap)

---

## Development Status

- [x] Blockchain event indexing (Sepolia RPC client, retry/backoff, MonthlyPayroll contract events)
- [x] PostgreSQL persistence (Idempotent writes, raw chain events, transaction records, composite indexes)
- [x] Generic ERC-20 Transfer indexing (Token-agnostic, configurable address and ABI)
- [x] Checkpointing/idempotency (Atomic stream progress tracking in `sync_checkpoints`)
- [x] REST API (`net/http.ServeMux` Go 1.22+, zero RPC calls on historical GET requests)
- [x] API validation/pagination (Address format, tx hash, bounded pagination, whitelisted sorting)
- [x] Continuous live monitoring (Background daemon for real-time finalized block synchronization)
- [x] Reconciliation engine — Payroll Funding, Salary Claim & ERC-20 Transfer verification (`reconciliation_exceptions`)
- [ ] Reconciliation engine — Contract & Token Balance verification
- [ ] Dashboard/product integration (Next.js frontend user interface)

| Feature / Milestone | Status | Description |
|---|---|---|
| **Sepolia RPC Client** | Completed | Verified connection with retry, exponential backoff, and 429 throttling handling |
| **MonthlyPayroll Indexer** | Completed | Ingestion of `EmployerAdded`, `EmployerRemoved`, `EmployeeAdded`, `EmployeeRemoved`, `PayrollFunded`, `SalaryClaimed` |
| **Generic ERC-20 Indexer** | Completed | Completely token-agnostic transfer indexer configurable via environment variables |
| **PostgreSQL Persistence** | Completed | Idempotent upserts, transaction metadata, chain events, and composite indexes |
| **Stream Checkpointing** | Completed | Durable block checkpoints in `sync_checkpoints` for crash-resilient restarts |
| **Production REST API** | Completed | Built with Go standard library (`net/http.ServeMux`), zero RPC calls on historical GET queries |
| **Validation & Middleware** | Completed | Request ID tracing, CORS, structured slog logging, panic recovery, and parameter validation |
| **Operator Protection** | Completed | Protected operator endpoints (`/v1/sync/backfill`) secured by API key authentication |
| **OpenAPI 3.0 Documentation**| Completed | Complete specification located at [`docs/openapi.yaml`](docs/openapi.yaml) |
| **Continuous Live Monitoring**| Completed | Continuous polling loop indexing incoming safe blocks with configurable confirmation depth |
| **Payroll Funding Reconciler**| Completed | Independent audit comparing on-chain `PayrollFunded` events to database records |
| **Salary Claim Reconciler** | Completed | Independent audit comparing on-chain `SalaryClaimed` events to database records |
| **Token Transfer Reconciler** | Completed | Independent audit comparing on-chain ERC-20 `Transfer` events to database records |
| **Dashboard UI** | Future | Next.js monitoring dashboard integration |

---

## Architecture Overview

```text
       Ethereum Sepolia
     (Infura / RPC Node)
              │
              │ eth_getLogs / RPC
              ▼
    ┌───────────────────┐
    │   WTF Indexer     │ ◄── Handles retries, backoff, and chunked ranges
    └─────────┬─────────┘
              │ Idempotent SQL writes & checkpoints
              ▼
    ┌───────────────────┐
    │    PostgreSQL     │ ◄── Single source of truth for indexed historical state
    └─────────┬─────────┘
              │ Parameterized read queries (Zero RPC calls)
              ▼
    ┌───────────────────┐
    │   REST API Layer  │ ◄── Validation, envelopes, error codes, operator auth
    └─────────┬─────────┘
              │ HTTP / JSON
              ▼
    [ Next.js Dashboard / Consumers / Postman ]
```

### Core Architectural Principles

- **Blockchain is the primary source of truth**: The database is an indexed, queryable projection of on-chain activity.
- **Strict separation of concerns**:
  - The **Indexer** fetches, decodes, and persists chain events.
  - The **API** reads exclusively from PostgreSQL for historical GET requests to avoid RPC rate limits.
  - The **Reconciliation Engine** independently verifies that database records match on-chain truth.
- **Token Agnosticism**: The ERC-20 indexer contains no hardcoded token addresses, decimals, or symbols.
- **Idempotent Persistence**: Indexing the same block range multiple times produces identical state without duplicate rows.
- **Safe Query Execution**: All database queries are 100% parameterized with whitelisted sort columns to eliminate SQL injection risks.

---

## Repository Structure

```text
WTF/
├── Architecture_Diagrams/       # System and component design diagrams
├── docs/                        # Architecture specs and API documentation
│   ├── 01-indexer-architecture.md
│   ├── 02-persistance-architecture.md
│   ├── 03-reconsiliation-worker.md
│   ├── 04-API-Arch.md
│   ├── Database_Schema.md
│   └── openapi.yaml             # Complete OpenAPI 3.0 REST specification
├── abi/                         # Contract ABI definitions (ERC-20, MonthlyPayroll)
└── indexer/                     # Go application codebase
    ├── cmd/
    │   ├── api/main.go          # REST API server entry point
    │   └── indexer/main.go      # Indexer CLI entry point
    ├── internal/
    │   ├── abi/                 # Generated Go bindings and generic ABI loader
    │   ├── api/                 # REST API layer
    │   │   ├── handlers/        # HTTP endpoint handlers
    │   │   ├── middleware/      # RequestID, CORS, Logging, Recovery, Auth
    │   │   ├── responses/       # Success/list/error response envelopes & codes
    │   │   └── validation/      # Address, hash, pagination, and range validators
    │   ├── blockchain/          # RPC client with retry and backoff logic
    │   ├── config/              # Centralized environment configuration
    │   ├── decoder/             # Log decoding for Payroll and ERC-20 events
    │   ├── indexer/             # Core MonthlyPayroll and Token indexer services
    │   ├── models/              # Shared domain and DTO models
    │   ├── persistence/         # Database connection pool and write-path persistence
    │   └── repository/          # Parameterized read-only repositories for API
    ├── migrations/              # PostgreSQL schema migrations (000001 - 000003)
    ├── .env                     # Local environment variables (not committed)
    └── .env.example             # Example configuration template
```

---

## Environment Configuration

Configuration is loaded from environment variables (or local `indexer/.env`). See [`indexer/.env.example`](indexer/.env.example) for a template:

| Variable | Required | Default | Description |
|---|---|---|---|
| `CHAIN_ID` | Yes | - | Target network chain ID (`11155111` for Sepolia) |
| `RPC_URL` | Yes | - | Ethereum RPC endpoint (e.g. Infura or Alchemy URL) |
| `DATABASE_URL` | Yes | - | PostgreSQL connection URL (`postgres://user:pass@host:port/dbname`) |
| `DEPLOYMENT_ENVIRONMENT`| Yes | - | Environment name (`development`, `staging`, `production`) |
| `START_BLOCK` | Yes | - | Initial block number for MonthlyPayroll indexing |
| `CONFIRMATION_DEPTH` | Yes | - | Required block confirmations before considering events safe |
| `BLOCK_BATCH_SIZE` | No | `50` | Maximum block span indexed per batch |
| `POLLING_INTERVAL` | Yes | - | Polling interval duration (e.g. `12s`) |
| `PAYROLL_CONTRACT_ADDRESS`| No | - | Sepolia MonthlyPayroll contract address |
| `TOKEN_ADDRESS` | No | - | Active ERC-20 contract address to index |
| `TOKEN_ABI_PATH` | No | - | Path to ERC-20 ABI JSON file (e.g. `./abi/erc20.json`) |
| `TOKEN_START_BLOCK` | No | `START_BLOCK`| Initial block number for token indexing |
| `TOKEN_STREAM_ID` | No | `erc20_transfers` | Stream identifier recorded in `sync_checkpoints` |
| `API_HOST` | No | `0.0.0.0` | Bind address for REST API |
| `API_PORT` | No | `8080` | Port for REST API |
| `CORS_ALLOWED_ORIGINS` | No | `*` | Allowed CORS origins (comma-separated or `*`) |
| `EXPLORER_TX_URL_TEMPLATE`| No | Network-based | Custom explorer URL template (e.g. `https://sepolia.etherscan.io/tx/%s`) |
| `OPERATOR_API_KEY` | For Operator| - | Secret key protecting `/v1/sync/backfill` |
| `RPC_MAX_RETRIES` | No | `5` | Maximum retries on transient RPC failures and 429 rate limits |
| `RPC_INITIAL_BACKOFF` | No | `1s` | Initial exponential backoff duration before first retry |
| `RPC_MAX_BACKOFF` | No | `30s` | Maximum ceiling for exponential backoff delay |
| `RPC_BACKOFF_FACTOR` | No | `2.0` | Multiplicative factor for exponential backoff doubling |

---

## WTF ERC-20 Token Indexing

The ERC-20 indexer is designed to be **token-agnostic** while providing isolated checkpointing and robust start-block safety for the **WorldTradeFuture (WTF) Token** on Sepolia.

### WTF Token Deployment Configuration
```env
# WorldTradeFuture (WTF) ERC-20 Token on Ethereum Sepolia
TOKEN_ADDRESS=0x378AFb93CaDd39AFF154704d2D90Af8c401137E7
TOKEN_ABI_PATH=./abi/erc20.json
TOKEN_START_BLOCK=11717931
TOKEN_STREAM_ID=erc20_transfers_0x378AFb93CaDd39AFF154704d2D90Af8c401137E7
```

### How it Works
1. **ABI Validation**: When `TOKEN_ADDRESS` and `TOKEN_ABI_PATH` are supplied in `.env`, the indexer parses the ABI and validates that it contains a standard ERC-20 `Transfer(address indexed from, address indexed to, uint256 value)` event specification.
2. **Log Ingestion & Decoding**: The indexer queries log events matching the token contract address in bounded chunks (`BLOCK_BATCH_SIZE`), decodes them via `GenericERC20Filterer`, and persists them idempotently into the `token_transfers` and `chain_events` tables.
3. **Checkpoint Stream Isolation**: Progress is tracked in `sync_checkpoints` under an address-specific stream ID (e.g., `erc20_transfers_0x378AFb93CaDd39AFF154704d2D90Af8c401137E7`). If `TOKEN_STREAM_ID` is unset or left as the generic `"erc20_transfers"`, the indexer automatically appends the token contract address to guarantee isolation between different token contracts and prevent collisions with past test tokens.
4. **Start Block Clamping**: If a database checkpoint records a block lower than `TOKEN_START_BLOCK` (e.g., from an earlier test token or initial setup), the indexer safely clamps the effective start block to `max(checkpoint + 1, TOKEN_START_BLOCK)`. This prevents the indexer from querying millions of blocks prior to contract creation.
5. **Independent Payroll Coexistence**: The MonthlyPayroll indexer (`0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC`, start block `11080692`, stream `monthly_payroll`) runs completely independently without interference from token indexing.

### Switching or Adding Tokens
Switching to another token requires only changing configuration in `.env`. Existing checkpoints for previously indexed tokens remain preserved in PostgreSQL for auditing.

---

## Quick Start & Running Services

### Prerequisites
- **Go**: Version 1.22 or higher
- **PostgreSQL**: Version 14 or higher
- **Sepolia RPC**: Valid Infura, Alchemy, or RPC node URL

### 1. Database Setup & Migrations
Ensure your PostgreSQL database exists, then configure `DATABASE_URL` in `indexer/.env`:
```bash
# Example database creation
createdb wtf_onchain
```
Database migrations in `indexer/migrations/` apply automatically when the API or indexer service boots up:
- `000001_initial_schema`: Core tables (`transactions`, `employers`, `employees`, `chain_events`, `payroll_fundings`, `salary_claims`, `reconciliation_exceptions`)
- `000002_token_transfers_and_checkpoints`: `sync_checkpoints` and `token_transfers` tables
- `000003_api_indexes`: Composite indexes optimizing high-volume API queries

### 2. Running the Historical Blockchain Backfill

The indexer runs in a bounded, chunked historical backfill mode to ingest past events from `START_BLOCK` up to a configurable target block.

#### Configuration Options
Set parameters in `.env` or pass as environment variables:
```env
START_BLOCK=11080692
BACKFILL_TO_BLOCK=11080850
BLOCK_BATCH_SIZE=50
```

#### Running the Backfill
Execute from the `indexer` directory:
```bash
cd indexer

# Option A: Using CLI flags (overrides environment variables)
go run ./cmd/indexer --to-block 11080850 --batch-size 50

# Option B: Using environment variables
START_BLOCK=11080692 BACKFILL_TO_BLOCK=11080850 BLOCK_BATCH_SIZE=50 go run ./cmd/indexer

# Option C: Relying on .env defaults (defaults target block to safe block: latestBlock - confirmationDepth)
go run ./cmd/indexer
```

#### How the Historical Backfill Works
- **Bounded Chunking**: The historical range is split into discrete chunks of `BLOCK_BATCH_SIZE` (default: 50 blocks, e.g., `11080692 -> 11080741`, `11080742 -> 11080791`). A partial final range is computed automatically to end precisely at the target block. No massive single RPC requests are made.
- **Durable Checkpointing**: Progress is atomically recorded in `sync_checkpoints` for the `monthly_payroll` stream after each successful batch. If interrupted, restarting resumes from `last_indexed_block + 1`.
- **Idempotency Guarantee**: All persistence calls use `ON CONFLICT DO UPDATE` or `ON CONFLICT DO NOTHING` against unique constraints `(chain_id, contract_address, tx_hash, log_index)`. Replaying blocks does not duplicate records.
- **Domain Projections**: When `EmployeeAdded` events are detected, the indexer automatically satisfies the employer foreign key and inserts/updates the `employees` table. `EmployeeRemoved` updates the employee's active status to `false`. Funding and claim events are projected into `payroll_fundings` and `salary_claims`.

#### RPC Rate-Limit Handling & Resilient Backfill

Historical backfill frequently spans hundreds of thousands of blocks against public/shared JSON-RPC infrastructure (e.g., Infura free tier with 10 req/s rate limits and 100,000 req/day caps). The indexer includes a dedicated, conservative retry engine designed to avoid crashing during bursts while strictly guarding checkpoint integrity.

1. **Classification of Transient vs. Permanent Errors**:
   - **Retryable Transient Errors**:
     - HTTP `429 Too Many Requests`, `project rate limit reached` (`code: -32005`), `compute units per second exceeded`.
     - Transport/network connection errors: `connection reset by peer`, `broken pipe`, `unexpected EOF`, `i/o timeout`, `network is unreachable`.
     - HTTP server and gateway errors: `502 Bad Gateway`, `503 Service Unavailable`, `504 Gateway Timeout`, `500 Internal Server Error`.
   - **Non-Retryable Errors**: Context cancellations (`SIGINT`/`SIGTERM`), contract reverts, invalid ABI specifications, and SQL schema errors immediately abort the loop without wasting retries.

2. **Exponential Backoff Strategy**:
   - Initial backoff begins at `RPC_INITIAL_BACKOFF` (default: `1s`, or `--initial-backoff`).
   - Each successive retry multiplies the delay by `RPC_BACKOFF_FACTOR` (default: `2.0`).
   - Delays are hard-capped at `RPC_MAX_BACKOFF` (default: `30s`).
   - Progression: `1s` ➔ `2s` ➔ `4s` ➔ `8s` ➔ `16s` ➔ `30s (cap)`.

3. **Maximum Retry Count**:
   - Configurable via `RPC_MAX_RETRIES` (default: `5`, or `--max-retries`).
   - If an RPC endpoint remains unresponsive after 5 attempts, the indexer logs a fatal error and cleanly exits.

4. **Strict Checkpoint Safety Invariant**:
   - Processing order is strictly guaranteed:
     $$\text{Fetch Logs} \longrightarrow \text{Decode ABI} \longrightarrow \text{Persist Events \& Projections} \longrightarrow \text{Update Checkpoint}$$
   - Checkpoints in `sync_checkpoints` are **never** updated before or during range execution.
   - If range `[11083701, 11083750]` encounters an RPC failure, the checkpoint remains at `11083700` until that range is 100% successfully written and committed.
   - On process restart, the indexer resumes strictly from `last_indexed_block + 1` (`11083701`).

5. **Structured Logging & Secret Sanitization**:
   - All retry events emit structured `slog` logs containing `stream`, `from`, `to`, `attempt`, `max_retries`, and `retry_in`.
   - Infura project IDs, API keys, and bearer tokens in RPC URLs or headers are automatically redacted matching regex patterns (`[REDACTED]`).

```bash
# Run historical backfill with explicit retry parameters
go run ./cmd/indexer --stream payroll --to-block 11084000 --batch-size 50 --max-retries 5 --initial-backoff 1s
```

### 3. Running the REST API Server
To start the REST API service:
```bash
cd indexer
go run ./cmd/api
```
Or build and execute the binary:
```bash
go build -o bin/api.exe ./cmd/api
./bin/api.exe
```
The server will start on `http://0.0.0.0:8080`. Graceful shutdown handles `SIGINT` and `SIGTERM` signals with an in-flight request timeout.

---

## Continuous Live Monitoring

Milestone 3 (M3) adds continuous background live monitoring after historical synchronization. The live monitor continuously polls Ethereum Sepolia for newly confirmed safe blocks, detects relevant events across independent contract streams, persists records idempotently, and atomically updates durable checkpoints.

```text
               ┌───────────────────────────────┐
               │    Ethereum Sepolia Node      │
               └───────────────┬───────────────┘
                               │ eth_blockNumber (latestBlock)
                               ▼
               ┌───────────────────────────────┐
               │   Calculate Safe Head Block   │ ◄── safeTarget = latestBlock - confirmations
               └───────────────┬───────────────┘
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
   ┌───────────────────────────┐ ┌───────────────────────────┐
   │  MonthlyPayroll Stream    │ │      WTF Token Stream     │
   │ (stream: monthly_payroll) │ │ (stream: erc20_transfers) │
   └────────────┬──────────────┘ └────────────┬──────────────┘
                │                             │
                ▼                             ▼
   ┌───────────────────────────┐ ┌───────────────────────────┐
   │ Checkpoint Resumption     │ │ Checkpoint Resumption     │
   │ nextBlock = cp + 1        │ │ nextBlock = cp + 1        │
   └────────────┬──────────────┘ └────────────┬──────────────┘
                │                             │
                ▼                             ▼
   ┌───────────────────────────┐ ┌───────────────────────────┐
   │ Bounded Range Batching    │ │ Bounded Range Batching    │
   │ [from, min(from+batch,T)] │ │ [from, min(from+batch,T)] │
   └────────────┬──────────────┘ └────────────┬──────────────┘
                │                             │
                ▼                             ▼
   ┌───────────────────────────┐ ┌───────────────────────────┐
   │ Filter & Decode Logs      │ │ Filter & Decode Logs      │
   │ - EmployerAdded/Removed   │ │ - Transfer                │
   │ - EmployeeAdded/Removed   │ │ - Approval                │
   │ - PayrollFunded/Claimed   │ │                           │
   └────────────┬──────────────┘ └────────────┬──────────────┘
                │                             │
                ▼                             ▼
   ┌───────────────────────────┐ ┌───────────────────────────┐
   │ Idempotent Persistence    │ │ Idempotent Atomic Batch   │
   │ - transactions            │ │ - transactions            │
   │ - chain_events            │ │ - chain_events            │
   │ - domain tables           │ │ - token_transfers         │
   │ - sync_checkpoints        │ │ - sync_checkpoints        │
   └────────────┬──────────────┘ └────────────┬──────────────┘
                │                             │
                └──────────────┬──────────────┘
                               │
                               ▼
               ┌───────────────────────────────┐
               │ Sleep for LIVE_POLL_INTERVAL  │ (e.g. 5s, cancellable on SIGINT/SIGTERM)
               └───────────────┬───────────────┘
                               │
                               ▼
                       Repeat Loop Cycle
```

### Key Architectural Characteristics

1. **Continuous Polling Loop**:
   - Executes at a configurable frequency (`LIVE_POLL_INTERVAL`, default: `5s`).
   - Runs uninterrupted until a termination signal (`SIGINT` / `SIGTERM` / `Ctrl+C`) is received.
   - Non-blocking cooperative streaming: each stream processes bounded batches per poll cycle, preventing either stream from starving the other.

2. **Finalized & Safe Block Handling (Confirmation Depth)**:
   - To guard against chain reorgs, the monitor never queries the unconfirmed block head.
   - Computes safe block target: `safeTarget = latestBlock - CONFIRMATIONS` (default: `CONFIRMATIONS=5`).
   - Only blocks `<= safeTarget` are ever requested or indexed.

3. **Checkpoint-Based Resumption**:
   - On startup, the monitor inspects `sync_checkpoints` for each stream.
   - Resumes strictly from `nextBlock = last_indexed_block + 1`.
   - Never restarts from the genesis/deployment block once initial historical synchronization is recorded.
   - If no checkpoint is found, it safely falls back to the stream's configured start block.

4. **Separate & Independent Streams**:
   - `monthly_payroll`: Tracks `0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC` events.
   - `erc20_transfers_0x378AFb93CaDd39AFF154704d2D90Af8c401137E7`: Tracks WTF Token transfers and approvals.
   - Checkpoints are isolated by `(chain_id, stream_id)`. An error or lag in one stream never corrupts or blocks the other stream.

5. **Bounded Block Ranges**:
   - Never requests an unbounded range from RPC.
   - Ranges are segmented by `INDEXER_BATCH_SIZE` (or `BLOCK_BATCH_SIZE`, default: 50 blocks).
   - If checkpoint is `11724713` and `safeTarget` is `11724820`:
     - Batch 1: `11724714 -> 11724763`
     - Batch 2: `11724764 -> 11724813`
     - Batch 3: `11724814 -> 11724820`

6. **Atomic Checkpointing**:
   - Checkpoints advance **only after** all logs in the batch have been successfully decoded and committed to PostgreSQL.
   - For token transfers, metadata, raw events, transfers, and the checkpoint record are committed in a single SQL transaction (`SaveTokenBatch`).
   - If any database operation fails, the transaction rolls back, and the checkpoint remains at the previous block.

7. **Idempotency & Replay Safety**:
   - Uniqueness constraints:
     - `chain_events`: `UNIQUE(chain_id, contract_address, tx_hash, log_index)`
     - `token_transfers`: `UNIQUE(chain_id, token, tx_hash, log_index)`
     - `transactions`: `PRIMARY KEY(chain_id, tx_hash)`
     - `payroll_fundings`: `UNIQUE(chain_id, tx_hash, log_index)`
     - `salary_claims`: `UNIQUE(chain_id, tx_hash, log_index)`
   - Replaying any range results in 0 duplicate records.

8. **RPC Failure & Resilient Retry**:
   - RPC requests utilize exponential backoff and rate-limit handling (including HTTP 429 backoff).
   - Temporary RPC network outages do not advance checkpoints and do not terminate the process.
   - The monitor logs the error, pauses, and safely resumes upon RPC recovery.

9. **Blockchain Reorg & Removed Log Handling**:
   - Both `chain_events` and `token_transfers` include a `removed BOOLEAN` column.
   - `ON CONFLICT DO UPDATE SET removed = EXCLUDED.removed` updates previously recorded logs if a reorg marks them removed.
   - MonthlyPayroll domain projection skips any logs where `log.Removed == true`, preventing reorged transactions from falsely altering employee/employer balances.
   - Confirmation depth (`CONFIRMATIONS=5`) acts as the primary defense against shallow PoS reorgs.

10. **Events Monitored**:
    - **MonthlyPayroll**: `EmployerAdded`, `EmployerRemoved`, `EmployeeAdded`, `EmployeeRemoved`, `PayrollFunded`, `SalaryClaimed`, `OwnershipTransferred`.
    - **WTF Token (ERC-20)**: `Transfer`, `Approval`.

11. **Graceful Shutdown**:
    - Intercepts `SIGINT` (Ctrl+C) and `SIGTERM`.
    - Completes the in-flight atomic database operation cleanly before closing client connections.
    - Zero checkpoint state corruption on shutdown.

---

### Starting Live Monitoring

#### Option A: Using the CLI Flag (Recommended)
```bash
cd indexer

# Start continuous live monitoring for all configured streams
go run ./cmd/indexer --live

# Start with custom poll interval and confirmation depth
go run ./cmd/indexer --live --poll-interval=5s --confirmations=5 --batch-size=50

# Monitor only the WTF Token stream
go run ./cmd/indexer --live --stream token

# Monitor only the MonthlyPayroll stream
go run ./cmd/indexer --live --stream payroll
```

#### Option B: Using Environment Variables
```bash
cd indexer

LIVE_MONITOR_ENABLED=true LIVE_POLL_INTERVAL=5s CONFIRMATIONS=5 go run ./cmd/indexer
```

#### Option C: Built Executable
```bash
cd indexer
go build -o bin/indexer.exe ./cmd/indexer
./bin/indexer.exe --live --poll-interval=5s
```

---

### Live Monitoring Configuration Reference

| Variable | CLI Flag | Default | Description |
|---|---|---|---|
| `LIVE_MONITOR_ENABLED` | `--live` | `false` | When `true`, indexer enters continuous live polling mode |
| `LIVE_POLL_INTERVAL` | `--poll-interval` | `5s` | Sleep duration between polling cycles (e.g. `5s`, `10s`) |
| `CONFIRMATIONS` / `CONFIRMATION_DEPTH` | `--confirmations` | `5` | Block confirmation depth for safe head calculation |
| `INDEXER_BATCH_SIZE` / `BLOCK_BATCH_SIZE` | `--batch-size` | `50` | Maximum number of blocks fetched per RPC batch |
| `STREAM` | `--stream` | `all` | Specific stream to index: `payroll`, `token`, or `all` |
| `TOKEN_STREAM_ID` | N/A | `erc20_transfers_<addr>` | Stream ID for WTF token checkpoint tracking |
| `PAYROLL_STREAM_ID` | N/A | `monthly_payroll` | Stream ID for MonthlyPayroll checkpoint tracking |

---

### Verifying Synchronization Status via REST API

While the live monitor runs, you can check live sync progress in real time through the REST API.

Start the API in a separate terminal:
```bash
cd indexer
go run ./cmd/api
```

Query the synchronization status:
```bash
curl -s http://localhost:8080/v1/sync/status | jq .
```

Example response showing real-time sync checkpoints and zero lag:
```json
{
  "data": {
    "chain_id": 11155111,
    "latest_block": 11725138,
    "safe_block": 11725133,
    "streams": [
      {
        "stream_id": "erc20_transfers_0x378AFb93CaDd39AFF154704d2D90Af8c401137E7",
        "token": "0x378AFb93CaDd39AFF154704d2D90Af8c401137E7",
        "last_indexed_block": 11725133,
        "lag": 0,
        "status": "synced"
      },
      {
        "stream_id": "monthly_payroll",
        "last_indexed_block": 11081900,
        "lag": 643233,
        "status": "catching_up"
      }
    ],
    "last_error": null
  }
}
```

Check health and readiness:
```bash
curl -s http://localhost:8080/health
curl -s http://localhost:8080/ready
```

---

## Reconciliation Worker — Payroll Funding, Salary Claim & Token Transfer Safety Checks

The **Reconciliation Worker** acts as the safety check and audit layer for the WTF On-Chain Monitor. It compares authoritative on-chain state against observed/indexed PostgreSQL database data and creates structured, idempotent exceptions in `reconciliation_exceptions` whenever a mismatch is detected.

> **Key Architectural Principle**: The reconciliation worker checks the indexer — it is not a second indexer. It does not silently insert missing rows; it surfaces discrepancies for operational visibility and auditability.

### Domain Rules: Smart Contract Storage vs Authoritative Events
The `MonthlyPayroll` smart contract (`0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC`) maintains running state mappings (`employers`, `employees`, `payrolls`), but **does not expose an iterable historical ledger in contract storage** for past individual funding or claim transactions. 

Similarly, the standard ERC-20 contract (`0x378AFb93CaDd39AFF154704d2D90Af8c401137E7`) maintains current balances in storage (`mapping(address => uint256)`), but historical individual transfers are recorded only through immutable event logs.

Therefore, the authoritative source of truth for historical transactions is the immutable sequence of event logs emitted in transaction receipts on the blockchain:
- **Payroll Funding**: `event PayrollFunded(address indexed employer, address indexed employee, uint256 amount, uint256 fee, uint256 amountCredited)`
- **Salary Claims**: `event SalaryClaimed(address indexed employee, uint256 amount)`
- **WTF Token Transfers**: `event Transfer(address indexed from, address indexed to, uint256 value)`

The reconciler decodes these authoritative on-chain logs and verifies them against the respective indexed database tables (`payroll_fundings`, `salary_claims`, and `token_transfers`) in PostgreSQL.

### Discrepancy Classification & Severities

#### 1. Payroll Funding Discrepancies
| Mismatch Type | Severity | Description |
| :--- | :--- | :--- |
| `PAYROLL_FUNDING_MISSING` | `high` | On-chain `PayrollFunded` event exists within finalized range and indexer checkpoint, but is absent from `payroll_fundings`. |
| `PAYROLL_FUNDING_AMOUNT_MISMATCH` | `high` | Discrepancy in `amount_paid`, `fee`, or `amount_credited` between on-chain event and database record. |
| `PAYROLL_FUNDING_ENTITY_MISMATCH` | `critical` | Employer or employee wallet address mismatch between on-chain event and database record. |
| `PAYROLL_FUNDING_DUPLICATE` | `high` | Multiple database records exist for the same on-chain `(chain_id, tx_hash, log_index)`. |
| `PAYROLL_FUNDING_TX_INCONSISTENCY` | `medium` | Block number or transaction metadata differs between blockchain log and database record. |

#### 2. Salary Claim Discrepancies
| Mismatch Type | Severity | Description |
| :--- | :--- | :--- |
| `SALARY_CLAIM_MISSING` | `high` | On-chain `SalaryClaimed` event exists within finalized range and indexer checkpoint, but is absent from `salary_claims`. |
| `SALARY_CLAIM_AMOUNT_MISMATCH` | `high` | Discrepancy in `amount` between on-chain event and database record. |
| `SALARY_CLAIM_ENTITY_MISMATCH` | `critical` | Employee wallet address mismatch between on-chain event and database record. |
| `SALARY_CLAIM_DUPLICATE` | `high` | Multiple database records exist for the same on-chain `(chain_id, tx_hash, log_index)`. |
| `SALARY_CLAIM_TX_INCONSISTENCY` | `medium` | Block number or transaction metadata differs between blockchain log and database record. |

#### 3. WTF ERC-20 Token Transfer Discrepancies
| Mismatch Type | Severity | Description |
| :--- | :--- | :--- |
| `TOKEN_TRANSFER_MISSING` | `high` | On-chain `Transfer` event exists within finalized range and token indexer checkpoint, but is absent from `token_transfers`. |
| `TOKEN_TRANSFER_AMOUNT_MISMATCH` | `high` | Discrepancy in `amount` (exact `*big.Int` arithmetic) between on-chain event and database record. |
| `TOKEN_TRANSFER_ENTITY_MISMATCH` | `critical` | Sender (`from`) or recipient (`to`) address mismatch between on-chain event and database record. |
| `TOKEN_TRANSFER_DUPLICATE` | `high` | Multiple database records exist for the same on-chain `(chain_id, token, tx_hash, log_index)`. |
| `TOKEN_TRANSFER_TX_INCONSISTENCY` | `medium` | Block number or transaction metadata differs between blockchain log and database record. |

### Lifecycle & Safety Features
- **Distinguishing Indexing Lag vs Genuine Mismatches**: If an on-chain event is on a block higher than the indexer's checkpoint (`block > indexerCheckpoint`), it is classified as normal indexing lag, not an error, and is not flagged as missing.
- **Token Address Isolation**: The token reconciler verifies events strictly from the configured `TOKEN_ADDRESS` (ignoring any other ERC-20 contracts on-chain).
- **Exact Numeric Representation**: All token and payroll amounts are compared as exact unsigned multi-precision integers (`*big.Int`), completely avoiding floating-point rounding errors.
- **Reorganization Safety (Confirmation Depth)**: Any blocks above `safeTarget = latestBlock - confirmationDepth` are ignored as unfinalized.
- **System Failure Isolation**: RPC timeouts or database connection drops return clean system errors and abort the run without generating false data mismatch exceptions.
- **Durable Checkpoint Isolation**:
  - `reconciliation_payroll_funding`: Checkpoint cursor for payroll funding reconciliation.
  - `reconciliation_salary_claim`: Checkpoint cursor for salary claim reconciliation.
  - `reconciliation_erc20_transfers_<token_address>`: Isolated checkpoint cursor per token contract address.
- **Idempotency**: Exception entity references follow a deterministic format (`{chain_id}:{token_address}:{tx_hash}:{log_index}:{mismatch_type}` for tokens, `{chain_id}:{tx_hash}:{log_index}:{mismatch_type}` for payroll). Repeated runs skip already open exceptions without creating duplicate rows.
- **Automated Lifecycle Resolution**: When a subsequent reconciliation pass detects that a previously open mismatch has been resolved (e.g. after indexer backfill or fix), it marks the exception `status = 'resolved'` and records `resolved_at`.

### Running the Reconciliation Worker

#### 1. WTF ERC-20 Token Transfer Reconciliation
```bash
cd indexer

# Reconcile an explicit block range
go run ./cmd/reconciler -type token-transfers -from-block 11717931 -to-block 11720000

# Reconcile recent finalized window
go run ./cmd/reconciler -type token-transfers -window 500

# Continuous reconciliation daemon loop
go run ./cmd/reconciler -type token-transfers -loop -interval 30s
```

#### 2. Salary Claim Reconciliation
```bash
# Reconcile an explicit block range
go run ./cmd/reconciler -type salary-claims -from-block 11080692 -to-block 11084000

# Reconcile recent finalized window
go run ./cmd/reconciler -type salary-claims -window 500

# Continuous reconciliation daemon loop
go run ./cmd/reconciler -type salary-claims -loop -interval 30s
```

#### 3. Payroll Funding Reconciliation
```bash
# Explicit range
go run ./cmd/reconciler -type payroll -from-block 11080692 -to-block 11084000

# Defaults to payroll funding for backwards compatibility:
go run ./cmd/reconciler -from-block 11080692 -to-block 11084000
```

#### 4. Multi-Stream / All Reconciliation Checks
```bash
# Run Payroll Funding, Salary Claim, and Token Transfer checks
go run ./cmd/reconciler -type all -from-block 11717931 -to-block 11720000

# Continuous multi-check daemon
go run ./cmd/reconciler -type all -loop -interval 30s
```

#### 5. Running via Indexer CLI
```bash
# Reconcile token transfers
go run ./cmd/indexer -reconcile -recon-type token-transfers -from-block 11717931 -to-block 11720000

# Reconcile salary claims
go run ./cmd/indexer -reconcile -recon-type salary-claims -from-block 11080692 -to-block 11084000

# Reconcile all checks
go run ./cmd/indexer -reconcile -recon-type all
```

#### 6. Querying Exceptions via REST API
```bash
# View open high-severity exceptions
curl -s "http://localhost:8080/v1/reconciliation/exceptions?status=open&severity=high" | jq .

# Filter specifically for token transfer discrepancies
curl -s "http://localhost:8080/v1/reconciliation/exceptions?type=TOKEN_TRANSFER_MISSING" | jq .

# Filter specifically for salary claim discrepancies
curl -s "http://localhost:8080/v1/reconciliation/exceptions?type=SALARY_CLAIM_MISSING" | jq .

# View critical entity mismatches
curl -s "http://localhost:8080/v1/reconciliation/exceptions?severity=critical" | jq .

# View all reconciliation exceptions paginated
curl -s "http://localhost:8080/v1/reconciliation/exceptions?page=1&page_size=20" | jq .
```


## REST API Reference

### Response Envelopes

All endpoints follow a unified response structure:

#### Single Resource Success
```json
{
  "data": { ... },
  "meta": null
}
```

#### Paginated List Success
```json
{
  "data": [ ... ],
  "meta": {
    "page": 1,
    "page_size": 20,
    "total": 59,
    "has_next": true
  },
  "pagination": {
    "page": 1,
    "page_size": 20,
    "total": 59,
    "has_next": true
  }
}
```

#### Error Envelope
```json
{
  "error": {
    "code": "INVALID_ADDRESS",
    "message": "address must be 42 characters long, got 10"
  }
}
```

### Stable Machine-Readable Error Codes
- `INVALID_ADDRESS`: Malformed or non-hex Ethereum address
- `INVALID_TRANSACTION_HASH`: Malformed or non-32-byte transaction hash
- `TRANSACTION_NOT_FOUND`: Transaction hash not found in indexed records
- `EMPLOYER_NOT_FOUND`: Employer address not found
- `EMPLOYEE_NOT_FOUND`: Employee address not found
- `INVALID_DIRECTION`: Direction parameter not one of `in`, `out`, `all`
- `INVALID_BLOCK_RANGE`: `from_block` > `to_block` or exceeds maximum range limit
- `INVALID_DATE_RANGE`: `from_date` after `to_date` or unrecognized timestamp format
- `INVALID_PAGINATION`: `page` < 1 or `page_size` exceeds limit (100)
- `SERVICE_NOT_READY`: Database pool disconnected or required configuration missing
- `UNAUTHORIZED`: Missing or invalid operator credentials
- `INTERNAL_SERVER_ERROR`: Unhandled exception or unexpected database error
- `BAD_REQUEST`: Malformed request syntax or unwhitelisted sort parameter

---

### Endpoints Reference

The complete OpenAPI 3.0 specification is available at [`docs/openapi.yaml`](docs/openapi.yaml).

#### 1. `GET /health`
- **Purpose**: Liveness probe returning immediate HTTP 200 without executing database queries or external RPC calls.
- **Parameters**: None.
- **Authentication**: Public (no auth required).

#### 2. `GET /ready`
- **Purpose**: Readiness probe verifying PostgreSQL database connectivity and loaded runtime configuration.
- **Parameters**: None.
- **Authentication**: Public (no auth required).

#### 3. `GET /v1/sync/status`
- **Purpose**: Reports blockchain synchronization progress, safe block heights, stream lag, and active stream checkpoints.
- **Parameters**: None.
- **Authentication**: Public (no auth required).

#### 4. `GET /v1/transactions/{hash}`
- **Purpose**: Retrieves indexed transaction details, dynamic confirmation depth, decoded events, and network-correct block explorer link.
- **Path Parameters**: `{hash}` (required) — 32-byte 0x-prefixed transaction hash (66 characters).
- **Authentication**: Public (no auth required).

#### 5. `GET /v1/employers/{address}`
- **Purpose**: Retrieves employer profile, funds, salary rates, and list of associated active/inactive employees.
- **Path Parameters**: `{address}` (required) — 20-byte 0x-prefixed employer wallet address (42 characters).
- **Authentication**: Public (no auth required).

#### 6. `GET /v1/employees/{address}`
- **Purpose**: Retrieves employee profile, salary rate per second, leave balances, recent funding history, and claim history.
- **Path Parameters**: `{address}` (required) — 20-byte 0x-prefixed employee wallet address (42 characters).
- **Authentication**: Public (no auth required).

#### 7. `GET /v1/payroll/fundings`
- **Purpose**: Returns paginated and filterable historical employer payroll funding deposits.
- **Query Parameters**:
  - `employer`: Filter by employer wallet address
  - `employee`: Filter by employee wallet address
  - `from_block`, `to_block`: Bounded block height range
  - `from_date`, `to_date`: ISO8601/RFC3339 datetime boundaries
  - `page` (default `1`), `page_size` (default `20`, max `100`): Pagination controls
  - `sort`: Whitelisted sort column (`block_number`, `block_timestamp`, `amount_paid`, `amount_credited`, default `block_number`)
  - `order`: Sort direction (`asc` or `desc`, default `desc`)
- **Authentication**: Public (no auth required).

#### 8. `GET /v1/payroll/claims`
- **Purpose**: Returns paginated and filterable historical employee salary withdrawals.
- **Query Parameters**:
  - `employee`: Filter by employee wallet address
  - `from_block`, `to_block`: Bounded block height range
  - `from_date`, `to_date`: ISO8601/RFC3339 datetime boundaries
  - `page` (default `1`), `page_size` (default `20`, max `100`): Pagination controls
  - `sort`: Whitelisted sort column (`block_number`, `block_timestamp`, `amount`, default `block_number`)
  - `order`: Sort direction (`asc` or `desc`, default `desc`)
- **Authentication**: Public (no auth required).

#### 9. `GET /v1/tokens/{address}/transfers`
- **Purpose**: Returns paginated and filterable ERC-20 token transfer events for any indexed token contract.
- **Path Parameters**: `{address}` (required) — 20-byte 0x-prefixed token contract address.
- **Query Parameters**:
  - `wallet`: Filter by sender or recipient wallet address
  - `direction`: Filter transfer direction relative to `wallet` (`in`, `out`, or `all`, default `all`)
  - `from_block`, `to_block`: Bounded block height range
  - `from_date`, `to_date`: ISO8601/RFC3339 datetime boundaries
  - `page` (default `1`), `page_size` (default `20`, max `100`): Pagination controls
  - `sort`: Whitelisted sort column (`block_number`, `block_timestamp`, `amount`, default `block_number`)
  - `order`: Sort direction (`asc` or `desc`, default `desc`)
- **Authentication**: Public (no auth required).

#### 10. `GET /v1/reconciliation/exceptions`
- **Purpose**: Returns paginated list of on-chain / database balance and event discrepancies detected during reconciliation.
- **Query Parameters**:
  - `severity`: Filter by severity level (`low`, `medium`, `high`, `critical`)
  - `status`: Filter by resolution status (`open`, `resolved`)
  - `type`: Filter by exception classification type
  - `from_date`, `to_date`: Detection timestamp boundaries
  - `page` (default `1`), `page_size` (default `20`, max `100`): Pagination controls
  - `sort`: Whitelisted sort column (`detected_at`, `severity`, `status`, default `detected_at`)
  - `order`: Sort direction (`asc` or `desc`, default `desc`)
- **Authentication**: Public (no auth required).

#### 11. `POST /v1/sync/backfill`
- **Purpose**: Protected operator endpoint to schedule bounded historical indexing for a specific block range.
- **Authentication**: **Protected**. Requires operator credentials matching `OPERATOR_API_KEY`:
  - `Authorization: Bearer <key>` header, OR
  - `X-API-Key: <key>` header.
- **Request Body**:
  ```json
  {
    "from_block": 11714400,
    "to_block": 11714450,
    "stream_id": "erc20_transfers"
  }
  ```
  *(Rejects unbounded ranges or spans exceeding 50,000 blocks with `INVALID_BLOCK_RANGE`)*.

---

## API Testing with Postman & cURL

The REST API can be tested using Postman, cURL, or any standard HTTP client.

### Testing with Postman
1. Open **Postman**.
2. Click **Import** and select the OpenAPI specification file: [`docs/openapi.yaml`](docs/openapi.yaml).
3. Set the collection variable `baseUrl` to `http://localhost:8080`.
4. For protected operator routes (`POST /v1/sync/backfill`), configure Bearer Token authentication with your `OPERATOR_API_KEY`.

---

### Example cURL Commands

#### 1. Liveness Probe
```bash
curl -s http://localhost:8080/health
```
```json
{"data":{"status":"ok","timestamp":"2026-09-16T11:06:37Z"}}
```

#### 2. Readiness Probe
```bash
curl -s http://localhost:8080/ready
```
```json
{
  "data": {
    "chain_id": 11155111,
    "database": "connected",
    "environment": "development",
    "generic_token_contract": "0x378AFb93CaDd39AFF154704d2D90Af8c401137E7",
    "payroll_contract": "0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC",
    "status": "ready"
  }
}
```

#### 3. Indexer Sync Status & Stream Checkpoints
```bash
curl -s http://localhost:8080/v1/sync/status
```
```json
{
  "data": {
    "chain_id": 11155111,
    "latest_block": 11724718,
    "safe_block": 11724713,
    "streams": [
      {
        "stream_id": "monthly_payroll",
        "contract": "0x25a2aa23067B7cF5a991fC56cF76E8BFE03Cc6eC",
        "last_indexed_block": 11080850,
        "lag": 643863,
        "status": "backfilling"
      },
      {
        "stream_id": "erc20_transfers_0x378AFb93CaDd39AFF154704d2D90Af8c401137E7",
        "token": "0x378AFb93CaDd39AFF154704d2D90Af8c401137E7",
        "last_indexed_block": 11724713,
        "lag": 0,
        "status": "synced"
      }
    ],
    "last_error": null
  }
}
```

#### 4. WTF ERC-20 Token Transfers
```bash
curl -s "http://localhost:8080/v1/tokens/0x378AFb93CaDd39AFF154704d2D90Af8c401137E7/transfers?page=1&page_size=10"
```
```json
{
  "data": [
    {
      "id": 354,
      "chain_id": 11155111,
      "token": "0x378afb93cadd39aff154704d2d90af8c401137e7",
      "from_address": "0x0000000000000000000000000000000000000000",
      "to_address": "0x5d1beadf6e5f9a1ea564162c797344b1337482de",
      "amount": 1000000000000000000000000000,
      "tx_hash": "0x1e4051e25e97b8250aff9dfe346b49e743d09c7fcc1876294b3f2e5948f7a3f1",
      "block_number": 11717931,
      "block_timestamp": "2026-09-16T16:34:36Z",
      "log_index": 123,
      "removed": false,
      "created_at": "2026-09-17T15:52:46.494398Z"
    }
  ],
  "meta": {
    "page": 1,
    "page_size": 10,
    "total": 1,
    "has_next": false
  },
  "pagination": {
    "page": 1,
    "page_size": 10,
    "total": 1,
    "has_next": false
  }
}
```

#### 5. Directional Wallet Filter (`direction=out` or `direction=in`)
```bash
curl -s "http://localhost:8080/v1/tokens/0x378AFb93CaDd39AFF154704d2D90Af8c401137E7/transfers?wallet=0x5d1beadf6e5f9a1ea564162c797344b1337482de&direction=in&page=1&page_size=10"
```

#### 6. Transaction Details with Decoded Events
```bash
curl -s "http://localhost:8080/v1/transactions/0xf73f051814e18d7073ad9db5d6a28cb66f0946f1ba403edca0207d16bb2cbdbf"
```
```json
{
  "data": {
    "hash": "0xf73f051814e18d7073ad9db5d6a28cb66f0946f1ba403edca0207d16bb2cbdbf",
    "chain_id": 11155111,
    "sender": "0xE83573b1167e7f5ED8C9c514227bAAa03d8993fc",
    "recipient": "0xA984DE6bd16305A200Ecd4023Eb467E5a0f3026B",
    "status": "confirmed",
    "block_number": 11714490,
    "confirmations": 1,
    "gas_used": "1453933",
    "events": [
      {
        "event_name": "Transfer",
        "contract_address": "0x1c7d4b196cb0c7b01d743fbc6116a902379c7238",
        "log_index": 250,
        "data": {
          "from": "0xB0Cc453467799b25DF1090A540d14678364EA313",
          "to": "0xd8A0A56d9b21a65d5E44A3D8Af3F0cac8284f3ba",
          "amount": "1"
        }
      }
    ],
    "explorer_url": "https://sepolia.etherscan.io/tx/0xf73f051814e18d7073ad9db5d6a28cb66f0946f1ba403edca0207d16bb2cbdbf",
    "first_seen_at": "2026-09-16T10:40:46.73055+05:30"
  }
}
```

#### 7. Protected Operator Backfill Request
```bash
curl -s -X POST http://localhost:8080/v1/sync/backfill \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-operator-secret-key-123" \
  -d '{"from_block": 11714400, "to_block": 11714450, "stream_id": "erc20_transfers"}'
```
```json
{
  "data": {
    "chain_id": 11155111,
    "from_block": 11714400,
    "message": "Backfill scheduled for blocks 11714400 to 11714450",
    "status": "accepted",
    "stream_id": "erc20_transfers",
    "to_block": 11714450
  }
}
```

---

## Automated Testing

All packages feature automated unit and integration tests.

### Running Tests
Execute the entire test suite from the `indexer` directory:
```bash
cd indexer
go test -v -count=1 ./...
```

### What is Tested
1. **Configuration (`internal/config`)**:
   - Validation of required environment variables.
   - Validation of token address and prevention of address collisions with payroll contract.
   - ABI parsing and verification of required `Transfer` event topics.
2. **Decoder (`internal/decoder`)**:
   - Parsing and decoding of raw ERC-20 event logs.
3. **Indexer Engine (`internal/indexer`)**:
   - Bounded historical block-range chunking.
   - Checkpoint persistence and resumption.
   - RPC failure recovery, retries, and backoff handling.
   - Dynamic token switching between multiple independent ERC-20 tokens.
4. **Persistence Layer (`internal/persistence`)**:
   - Idempotent insertion of token transfers.
   - Handling multiple transfer events per transaction.
   - Transaction metadata and raw chain event storage.
   - Checkpoint updates in atomic database transactions.
5. **Validation Utilities (`internal/api/validation`)**:
   - EVM address shape and length checks.
   - 32-byte transaction hash validation.
   - Bounded pagination defaults and maximum page size limits.
   - Block range boundaries (`from_block <= to_block`).
   - ISO8601/RFC3339 datetime parsing.
   - Strict whitelisting for sort columns and order directions (`ASC`/`DESC`).
6. **Middleware (`internal/api/middleware`)**:
   - Generation and propagation of unique `X-Request-ID` headers.
   - CORS origin validation and OPTIONS preflight handling.
   - Panic recovery returning standardized HTTP 500 error envelopes.
   - Operator authentication verifying Bearer tokens and `X-API-Key` headers.
7. **HTTP Handlers (`internal/api/handlers`)**:
   - Immediate HTTP 200 response on `/health`.
   - Readiness status and HTTP 503 error handling on `/ready`.
   - Bounded block validation on `/v1/sync/backfill`.
   - 400 Bad Request error envelopes on invalid addresses, hashes, directions, and ranges.
   - Live PostgreSQL integration tests verifying real database reads for sync status, token transfers, payroll events, and 404 lookups.

---

## Security Notes

1. **Zero Private Keys**: Neither the Indexer nor the API requires or accepts private keys or transaction-signing authority. All blockchain interactions are strictly read-only (`eth_getLogs`, `eth_getBlockByNumber`).
2. **Environment Variable Secrets**: All sensitive credentials (`DATABASE_URL`, `RPC_URL`, `OPERATOR_API_KEY`) are loaded from environment variables and must never be hardcoded.
3. **No Secrets in Version Control**: `.env` files are excluded from Git via `.gitignore`.
4. **Operator Route Protection**: Protected management routes (`POST /v1/sync/backfill`) require valid operator authentication using constant-time string comparison (`subtle.ConstantTimeCompare`).
5. **No SQL Injection**: All database queries are fully parameterized using `$1, $2, ...` query arguments via `pgxpool.Pool`. Sort columns are matched against strict hardcoded whitelists.
6. **Safe Logging**: Structured logging masks or omits credentials, connection strings, and authorization tokens.

---

## Troubleshooting

### 1. RPC Connection Failures / HTTP 429 Too Many Requests
- **Cause**: Free-tier RPC providers (e.g. Infura, Alchemy) throttle high-volume log requests.
- **Solution**: Decrease `BLOCK_BATCH_SIZE` in `.env` (e.g. from `50` to `10` or `20`) or increase `POLLING_INTERVAL` (e.g. `12s`).

### 2. PostgreSQL Connection Refused
- **Cause**: PostgreSQL is not running, or `DATABASE_URL` credentials / host / port are incorrect.
- **Solution**: Confirm PostgreSQL is active on port 5432 and verify credentials:
  ```bash
  psql -U postgres -d wtf_onchain
  ```

### 3. Invalid Token Address or Missing ABI
- **Cause**: `TOKEN_ADDRESS` is malformed, set to the zero address, matches `PAYROLL_CONTRACT_ADDRESS`, or `TOKEN_ABI_PATH` does not contain the standard ERC-20 `Transfer` event.
- **Solution**: Verify the token contract address on Etherscan Sepolia and confirm `abi/erc20.json` is present and readable.

### 4. Database Migration Issues
- **Cause**: Database contains incomplete migrations or dirty state.
- **Solution**: Inspect the `schema_migrations` table in PostgreSQL. Revert and re-apply migrations using the `.up.sql` scripts located in `indexer/migrations/`.

### 5. API Port Conflict
- **Cause**: Another service is listening on port 8080.
- **Solution**: Change `API_PORT` in `indexer/.env` to another port (e.g. `API_PORT=8081`).

### 6. Missing Environment Variables
- **Cause**: Starting the API or indexer without a valid `.env` file in the working directory.
- **Solution**: Ensure you are running commands from the `indexer/` directory where `.env` is located, or copy `indexer/.env.example` to `indexer/.env`.

---

## Roadmap

### Completed
- [x] Sepolia RPC client with retry & exponential backoff
- [x] MonthlyPayroll contract indexing
- [x] Generic ERC-20 Transfer indexer (token-agnostic)
- [x] PostgreSQL persistence schema & idempotent write operations
- [x] Stream checkpointing (`sync_checkpoints`)
- [x] REST API layer with standard JSON envelopes
- [x] Parameterized SQL repositories & composite query indexes
- [x] Request validation, CORS, RequestID, Logging, and Panic Recovery middlewares
- [x] Operator authentication & backfill scheduling
- [x] OpenAPI 3.0 specification ([`docs/openapi.yaml`](docs/openapi.yaml))

- [x] Continuous synchronization live monitor daemon
- [x] Automated Reconciliation Engine — Payroll Funding (`internal/reconciliation`)
- [x] Automated Reconciliation Engine — Salary Claims (`internal/reconciliation`)
- [x] Automated Reconciliation Engine — WTF ERC-20 Token Transfers (`internal/reconciliation`)
- [x] Automated detection, classification, and resolution in `reconciliation_exceptions`

### Next
- [ ] Contract & Token Balance Reconciliation
- [ ] Reorganization detection and event rollback handling

### Future
- [ ] Next.js monitoring dashboard UI
- [ ] WebSocket / Server-Sent Events (SSE) for live activity streaming
- [ ] Multi-chain indexing support (Arbitrum, Optimism, Base)
- [ ] Automated Slack / PagerDuty alerting on critical reconciliation discrepancies
