# WTF On-Chain Monitor

A production-grade blockchain monitoring and indexing service for the **WorldTradeFuture (WTF)** platform on Ethereum **Sepolia**.

The service ingests smart contract events from Sepolia, persists normalized records idempotently into **PostgreSQL**, tracks stream synchronization checkpoints, and provides a high-performance **REST API** layer for the monitoring dashboard and operational tooling.

---

## Table of Contents

1. [Development Status](#development-status)
2. [Architecture Overview](#architecture-overview)
3. [Repository Structure](#repository-structure)
4. [Environment Configuration](#environment-configuration)
5. [Generic ERC-20 Token Indexing](#generic-erc-20-token-indexing)
6. [Quick Start & Running Services](#quick-start--running-services)
7. [REST API Reference](#rest-api-reference)
8. [API Testing with Postman & cURL](#api-testing-with-postman--curl)
9. [Automated Testing](#automated-testing)
10. [Security Notes](#security-notes)
11. [Troubleshooting](#troubleshooting)
12. [Roadmap](#roadmap)

---

## Development Status

- [x] Blockchain event indexing (Sepolia RPC client, retry/backoff, MonthlyPayroll contract events)
- [x] PostgreSQL persistence (Idempotent writes, raw chain events, transaction records, composite indexes)
- [x] Generic ERC-20 Transfer indexing (Token-agnostic, configurable address and ABI)
- [x] Checkpointing/idempotency (Atomic stream progress tracking in `sync_checkpoints`)
- [x] REST API (`net/http.ServeMux` Go 1.22+, zero RPC calls on historical GET requests)
- [x] API validation/pagination (Address format, tx hash, bounded pagination, whitelisted sorting)
- [x] OpenAPI documentation (OpenAPI 3.0 specification in [`docs/openapi.yaml`](docs/openapi.yaml))
- [ ] Continuous live monitoring (Background daemon for real-time finalized block synchronization)
- [ ] Reconciliation engine (Automated balance auditing and database drift detection)
- [ ] Dashboard/product integration (Next.js frontend user interface)

| Feature / Milestone | Status | Description |
|---|---|---|
| **Sepolia RPC Client** | Done | Verified connection with retry, exponential backoff, and 429 throttling handling |
| **MonthlyPayroll Indexer** | Done | Ingestion of `EmployerAdded`, `EmployerRemoved`, `EmployeeAdded`, `EmployeeRemoved`, `PayrollFunded`, `SalaryClaimed` |
| **Generic ERC-20 Indexer** | Done | Completely token-agnostic transfer indexer configurable via environment variables |
| **PostgreSQL Persistence** | Done | Idempotent upserts, transaction metadata, chain events, and composite indexes |
| **Stream Checkpointing** | Done | Durable block checkpoints in `sync_checkpoints` for crash-resilient restarts |
| **Production REST API** | Done | Built with Go standard library (`net/http.ServeMux`), zero RPC calls on historical GET queries |
| **Validation & Middleware** | Done | Request ID tracing, CORS, structured slog logging, panic recovery, and parameter validation |
| **Operator Protection** | Done | Protected operator endpoints (`/v1/sync/backfill`) secured by API key authentication |
| **OpenAPI 3.0 Documentation**| Done | Complete specification located at [`docs/openapi.yaml`](docs/openapi.yaml) |
| **Continuous Live Monitoring**| In Progress | Background daemon polling and indexing incoming finalized blocks in real time |
| **Reconciliation Engine** | Next | Asynchronous worker auditing database state against on-chain contract state |
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

---

## Generic ERC-20 Token Indexing

The ERC-20 indexer is designed to be **completely token-agnostic**. It contains no hardcoded addresses, decimals, names, or token-specific business logic.

### How it Works
1. When `TOKEN_ADDRESS` and `TOKEN_ABI_PATH` are supplied in `.env`, the indexer parses the ABI and validates that it contains a standard ERC-20 `Transfer(address indexed from, address indexed to, uint256 value)` event specification.
2. The indexer queries log events matching the contract address, decodes them via `GenericERC20Filterer`, and persists them into the `token_transfers` table.
3. Progress is recorded in `sync_checkpoints` using the stream ID specified by `TOKEN_STREAM_ID`.

### Switching Tokens
Switching from a test token to another ERC-20 token (or the final WTF ERC-20 token upon deployment) requires **only configuration changes** in `.env`:

```env
# Example: Switching to a newly deployed WTF Token
TOKEN_ADDRESS=0xYourNewTokenContractAddress
TOKEN_ABI_PATH=./abi/wtf-token.json
TOKEN_START_BLOCK=11750000
TOKEN_STREAM_ID=erc20_transfers_wtf
```

> **Note:** No Go code changes or recompilation are required when switching tokens, provided the contract adheres to the ERC-20 standard.

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

### 2. Running the Indexer
To run an indexing cycle from the command line:
```bash
cd indexer
go run ./cmd/indexer
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

The OpenAPI 3.0 specification is available at [`docs/openapi.yaml`](docs/openapi.yaml).

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

The REST API can be tested with Postman, cURL, or any standard HTTP client.

### Testing with Postman
1. Open **Postman**.
2. Click **Import** and select the OpenAPI specification file: [`docs/openapi.yaml`](docs/openapi.yaml).
3. Set the collection variable `baseUrl` to `http://localhost:8080`.
4. For protected routes (`POST /v1/sync/backfill`), configure Bearer Token authentication with your `OPERATOR_API_KEY`.

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
    "generic_token_contract": "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
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
    "latest_block": 11714490,
    "safe_block": 11714485,
    "streams": [
      {
        "stream_id": "erc20_transfers",
        "token": "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
        "last_indexed_block": 11714490,
        "lag": 0,
        "status": "synced"
      }
    ],
    "last_error": null
  }
}
```

#### 4. Generic ERC-20 Token Transfers
```bash
curl -s "http://localhost:8080/v1/tokens/0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238/transfers?page=1&page_size=2"
```
```json
{
  "data": [
    {
      "id": 87,
      "chain_id": 11155111,
      "token": "0x1c7d4b196cb0c7b01d743fbc6116a902379c7238",
      "from_address": "0xb0cc453467799b25df1090a540d14678364ea313",
      "to_address": "0xd8a0a56d9b21a65d5e44a3d8af3f0cac8284f3ba",
      "amount": 1,
      "tx_hash": "0xf73f051814e18d7073ad9db5d6a28cb66f0946f1ba403edca0207d16bb2cbdbf",
      "block_number": 11714490,
      "block_timestamp": "2026-09-16T04:33:48Z",
      "log_index": 250,
      "removed": false,
      "created_at": "2026-09-16T05:10:46.726229Z"
    }
  ],
  "meta": {
    "page": 1,
    "page_size": 2,
    "total": 59,
    "has_next": true
  },
  "pagination": {
    "page": 1,
    "page_size": 2,
    "total": 59,
    "has_next": true
  }
}
```

#### 5. Directional Wallet Filter (`direction=out`)
```bash
curl -s "http://localhost:8080/v1/tokens/0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238/transfers?wallet=0xb0cc453467799b25df1090a540d14678364ea313&direction=out&page=1&page_size=1"
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
- [x] OpenAPI 3.0 specification (`docs/openapi.yaml`)

### Next
- [ ] Continuous synchronization worker daemon (automatic polling of newly confirmed blocks)
- [ ] Automated Reconciliation Engine (comparing database ledger with on-chain contract state)
- [ ] Reorganization detection and event rollback handling
- [ ] Automated detection and persistence to `reconciliation_exceptions`

### Future
- [ ] Next.js monitoring dashboard UI
- [ ] WebSocket / Server-Sent Events (SSE) for live activity streaming
- [ ] Multi-chain indexing support (Arbitrum, Optimism, Base)
- [ ] Automated Slack / PagerDuty alerting on critical reconciliation discrepancies
