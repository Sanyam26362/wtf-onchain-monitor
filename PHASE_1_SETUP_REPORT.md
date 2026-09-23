# Phase 1 Infrastructure & Core Configuration Verification Report

**Project**: WTF On-Chain Event Indexer  
**Environment**: Windows / PowerShell  
**Runtime**: Go 1.25.0 / PostgreSQL 16 Alpine / Redis 7 Alpine  
**Date**: September 23, 2026  
**Status**: APPROVED & VERIFIED  

---

## 1. Executive Summary

Phase 1 establishes the foundational infrastructure, runtime configuration, and caching tier for the WTF On-Chain Event Indexer. 

Key milestones achieved:
- **Port Conflict Resolution**: Host port `5432` was already reserved by an external database service (`sih_postgis_db`). PostgreSQL for the WTF Indexer was successfully isolated to host port `5433` (`5433:5432`) without disrupting existing host workloads.
- **Containerized Infrastructure**: Docker Compose orchestration was introduced for `wtf-postgres` and `wtf-redis` with persistent named storage. Both services are operational, accepting connections, and passing health checks.
- **Configuration Management**: `.env` and `internal/config/config.go` were aligned with local infrastructure parameters and extended to support Redis connectivity, Alchemy webhook signing keys, and escrow contract configuration.
- **Dependency & Module Health**: The official Redis client (`github.com/redis/go-redis/v9`) was added, module files tidied, and client lifecycle utilities implemented.
- **Database Schema Validation**: Database connectivity and schema creation were confirmed by applying database migrations (000001 through 000004) to the dedicated `indexer` PostgreSQL schema. 10 core tables and 6 relational foreign key constraints were validated.
- **Test Integrity**: Full unit and integration test suite (`go test ./...`) passes with zero failures.

---

## 2. Infrastructure Status (Container Names, Ports, Health)

Docker Compose configuration was deployed using `postgres:16-alpine` and `redis:7-alpine`.

### Container Status Table

| Container Name | Image | Status | Port Mapping (Host -> Container) | Health / Connectivity Check |
|---|---|---|---|---|
| `wtf-postgres` | `postgres:16-alpine` | Up (Running) | `0.0.0.0:5433->5432/tcp`, `[::]:5433->5432/tcp` | `pg_isready -U wtf_user -d wtf_indexer` (accepting connections) |
| `wtf-redis` | `redis:7-alpine` | Up (Running) | `0.0.0.0:6379->6379/tcp`, `[::]:6379->6379/tcp` | `redis-cli ping` -> `PONG` |

### Existing Workloads Isolation
External containers running on the host system remained untouched and active:
- `sih_postgis_db`: Binding `0.0.0.0:5432->5432/tcp` (unaltered)
- `sih_fastapi_backend`: Binding `0.0.0.0:8000->8000/tcp` (unaltered)

### Storage & Persistence
- Named Volume: `wtf_pgdata` mounted to `/var/lib/postgresql/data` ensures persistence across container restarts and updates.

---

## 3. Configuration & Code Diff Summary

### 3.1 Environment Configuration (`indexer/.env`)
The local runtime configuration was set with explicit parameters matching the Docker Compose deployment:

```dotenv
# Application & Server Configuration
PORT=8080
API_PORT=8080
API_HOST=0.0.0.0
APP_ENV=development
DEPLOYMENT_ENVIRONMENT=development
LOG_LEVEL=debug

# Sepolia / RPC Configuration
CHAIN_ID=11155111
RPC_URL=https://rpc.sepolia.org
SEPOLIA_RPC_URL=https://rpc.sepolia.org

# Database Configuration (PostgreSQL on mapped host port 5433)
DATABASE_URL=postgres://wtf_user:wtf_password@localhost:5433/wtf_indexer?sslmode=disable
DB_SCHEMA=indexer

# Redis Configuration
REDIS_URL=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0

# Alchemy & Webhook Configuration
ALCHEMY_WEBHOOK_SIGNING_KEY=whsec_test_dummy_key

# Contracts
WTF_ESCROW_CONTRACT_ADDRESS=0x0000000000000000000000000000000000000000
PAYROLL_CONTRACT_ADDRESS=0x430e558d403E3668C3A2E3435ff2Fd6a01Eb7F3A
PAYROLL_STREAM_ID=monthly_payroll
...
```

### 3.2 Git Ignore Rules (`.gitignore`)
Updated root `.gitignore` to prevent sensitive credentials and environment files from leaking into version control while maintaining example configuration templates:
- Ignored: `.env`, `*.env`, `indexer/.env`
- Preserved: `!.env.example`, `!indexer/.env.example`

### 3.3 Go Configuration Updates (`indexer/internal/config/config.go`)
1. Extended `Config` struct:
   - `RedisURL string`: Connection address for Redis instance.
   - `AlchemyWebhookSigningKey string`: HMAC verification secret for incoming webhooks.
   - `WTFEscrowContractAddress string`: Smart contract address target for escrow event indexing.
2. Updated `Load()` routine:
   - Added `REDIS_URL` reading with fallback default `"localhost:6379"`.
   - Added `ALCHEMY_WEBHOOK_SIGNING_KEY` reading with fallback default `"whsec_test_dummy_key"`.
   - Added `WTF_ESCROW_CONTRACT_ADDRESS` reading with fallback default `"0x0000000000000000000000000000000000000000"`.
   - Added resilient fallbacks for `RPC_URL` / `SEPOLIA_RPC_URL`, `DEPLOYMENT_ENVIRONMENT` / `APP_ENV`, and `API_PORT` / `PORT`.
3. Added `LoadConfig()` alias function to satisfy standard loading signatures.
4. Added test suite in `indexer/internal/config/config_test.go` (`TestLoad_RedisAndEscrowConfig`) validating defaults and environment overrides.

### 3.4 Redis Client Constructor (`indexer/internal/persistence/redis.go`)
Implemented `NewRedisClient(ctx context.Context, addr string, password string, db int) (*redis.Client, error)` providing pooled, ping-validated Redis client creation for application services.

### 3.5 Schema Inspection CLI (`indexer/cmd/inspect-schema/main.go`)
Updated the schema inspection command to automatically run pending SQL migrations before inspection, enabling one-step validation of database connectivity and relational integrity.

### 3.6 Git Diff Statistics
```
 .gitignore                                       |  4 +-
 docker-compose.yml                               | 20 +++++++++
 indexer/cmd/inspect-schema/main.go               | 11 +++++
 indexer/go.mod                                   |  2 +
 indexer/go.sum                                   | 14 ++++++-
 indexer/internal/config/config.go                | 50 +++++++++++++++++++---
 indexer/internal/config/config_test.go           | 47 ++++++++++++++++++++
 indexer/internal/indexer/db_verification_test.go |  8 ++--
 indexer/internal/persistence/redis.go            | 28 ++++++++++++
 indexer/internal/persistence/redis_test.go       | 33 ++++++++++++++
 10 files changed, 204 insertions(+), 13 deletions(-)
```

---

## 4. Test & Verification Outputs

### 4.1 Dependency Verification (`go.mod` & `go.sum`)
- `indexer/go.mod`: Direct requirement `github.com/redis/go-redis/v9 v9.22.0` present in `require (...)`.
- `indexer/go.sum`: Checksums recorded for `github.com/redis/go-redis/v9 v9.22.0` and its sub-modules.
- Compilation check: `go build ./...` compiled cleanly with zero errors.

### 4.2 Database Connectivity & Schema Inspection Output
Execution of `go run cmd/inspect-schema/main.go`:

```
Applied migration: 000001_initial_schema.up.sql
Applied migration: 000002_token_transfers_and_checkpoints.up.sql
Applied migration: 000003_api_indexes.up.sql
Applied migration: 000004_add_foreign_keys.up.sql
=== tables in schema "indexer" ===
   chain_events
   employees
   employers
   payroll_fundings
   reconciliation_exceptions
   salary_claims
   schema_migrations
   sync_checkpoints
   token_transfers
   transactions

=== foreign keys in schema "indexer" ===
   fk_chain_event_transaction          chain_events -> transactions
   fk_employee_employer                employees -> employers
   fk_payroll_funding_employee         payroll_fundings -> employees
   fk_payroll_funding_employer         payroll_fundings -> employers
   fk_payroll_funding_transaction      payroll_fundings -> transactions
   fk_salary_claim_employee            salary_claims -> employees
   (6 foreign keys)
```

### 4.3 Redis Connectivity Output
- CLI Ping:
  ```powershell
  docker exec wtf-redis redis-cli ping
  # Output: PONG
  ```
- Application Unit Test (`TestNewRedisClient_Ping`):
  ```
  === RUN   TestNewRedisClient_Ping
  --- PASS: TestNewRedisClient_Ping (0.03s)
  PASS
  ok  	worldtradefuture/indexer/internal/persistence	0.559s
  ```

### 4.4 Repository Test Suite (`go test ./...`)
Full package test execution (clean run without cache):
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
ok  	worldtradefuture/indexer/internal/abi           	5.849s
?   	worldtradefuture/indexer/internal/api           	[no test files]
ok  	worldtradefuture/indexer/internal/api/handlers  	0.500s
ok  	worldtradefuture/indexer/internal/api/middleware	4.998s
?   	worldtradefuture/indexer/internal/api/responses 	[no test files]
ok  	worldtradefuture/indexer/internal/api/validation	4.453s
ok  	worldtradefuture/indexer/internal/blockchain    	8.240s
ok  	worldtradefuture/indexer/internal/config        	5.754s
ok  	worldtradefuture/indexer/internal/decoder       	5.791s
ok  	worldtradefuture/indexer/internal/indexer       	1.062s
?   	worldtradefuture/indexer/internal/models        	[no test files]
ok  	worldtradefuture/indexer/internal/persistence   	0.867s
ok  	worldtradefuture/indexer/internal/reconciliation	6.159s
?   	worldtradefuture/indexer/internal/repository    	[no test files]
```
**Test Pass Rate**: 100% (10 packages tested, 0 failures).

---

## 5. Readiness for Phase 2 (Migration & DB Schema for `chain_events`)

The system is fully primed for Phase 2 implementation. The underlying schema and persistence components are ready as follows:

1. **`chain_events` Table Architecture**:
   - The table already exists within the dedicated `indexer` PostgreSQL schema.
   - Structured columns:
     - `event_id`: Sequential `BIGSERIAL PRIMARY KEY`
     - `chain_id`: `BIGINT NOT NULL`
     - `contract_address`: `VARCHAR(42) NOT NULL`
     - `event_name`: `VARCHAR(100) NOT NULL`
     - `tx_hash`: `VARCHAR(66) NOT NULL` with foreign key `fk_chain_event_transaction` referencing `transactions(chain_id, tx_hash)`
     - `block_number`: `BIGINT NOT NULL`
     - `block_hash`: `VARCHAR(66) NOT NULL`
     - `log_index`: `INT NOT NULL`
     - `tx_index`: `INT NOT NULL`
     - `block_timestamp`: `BIGINT NOT NULL`
     - `raw_data`: `BYTEA`
     - `topics`: `TEXT[]`
     - `indexed_at`: `TIMESTAMP WITH TIME ZONE DEFAULT NOW()`
   - Unique Index: `idx_chain_events_unique` enforcing idempotent log ingestion on `(chain_id, tx_hash, log_index)`.

2. **Integration Touchpoints for Phase 2**:
   - **Escrow Contract Indexing**: `WTF_ESCROW_CONTRACT_ADDRESS` is loaded into `Config.WTFEscrowContractAddress` and ready to be bound to event decoder pipelines.
   - **Webhook Verification**: `Config.AlchemyWebhookSigningKey` is configured for HMAC SHA256 header validation on incoming Alchemy webhook requests.
   - **Redis Event Queue / Caching**: Redis 7 is online on `localhost:6379` with `persistence.NewRedisClient` ready to act as a deduplication cache, rate limiter, or event queue broker.
   - **Schema Migration Extensibility**: Additional migration scripts can be added under `indexer/migrations/` (e.g. `000005_escrow_events.up.sql`) and will automatically apply during service startup.

Phase 1 setup is complete, hardened, and verified for Phase 2 execution.
