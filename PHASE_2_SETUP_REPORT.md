# Phase 2 Database Schema & Repository Layer Verification Report

**Project**: WTF On-Chain Event Indexer  
**Component**: WTFEscrow Chain Events Schema & Repository Layer  
**Environment**: Windows / PowerShell  
**Runtime**: Go 1.25.0 / PostgreSQL 16 Alpine (localhost:5433) / Redis 7 Alpine (localhost:6379)  
**Date**: September 23, 2026  
**Status**: APPROVED & VERIFIED  

---

## 1. Executive Summary

Phase 2 implements the database schema evolution and repository abstraction layer required for standalone ingestion and querying of `WTFEscrow` chain events (specifically `EscrowSettled`).

Key accomplishments:
- **Migration 000005 Created & Applied**: Successfully authored and applied [000005_escrow_chain_events.up.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000005_escrow_chain_events.up.sql) against the `indexer` schema in PostgreSQL (`localhost:5433`).
- **Relational Decoupling**: Dropped the foreign key constraint `fk_chain_event_transaction` on `indexer.chain_events`, enabling independent event ingestion directly from incoming Alchemy webhooks without requiring preceding transaction records.
- **Escrow Settlement Columns**: Extended `indexer.chain_events` with fields for `escrow_id`, `buyer`, `seller`, `amount`, `alchemy_webhook_id`, `raw_payload`, `block_hash`, `tx_index`, and `indexed_at`.
- **Performance & Idempotency Indexes**: Created lookup index `idx_chain_events_escrow_id` and unique composite index `uq_chain_events_tx_log` on `(tx_hash, log_index)`.
- **Repository Implementation & Validation**: Implemented `ChainEventsRepository` in [chain_events_repo.go](file:///d:/projects/wtf-onchain-monitor/indexer/internal/repository/chain_events_repo.go) providing idempotent inserts (`ON CONFLICT (tx_hash, log_index) DO NOTHING`) and queries by `escrow_id` or `(tx_hash, log_index)`. Integration tests verified 100% pass rate.

---

## 2. Migration Status

### 2.1 Migration Files Present in `indexer/migrations/`
- [000001_initial_schema.up.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000001_initial_schema.up.sql) / [000001_initial_schema.down.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000001_initial_schema.down.sql)
- [000002_token_transfers_and_checkpoints.up.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000002_token_transfers_and_checkpoints.up.sql) / [000002_token_transfers_and_checkpoints.down.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000002_token_transfers_and_checkpoints.down.sql)
- [000003_api_indexes.up.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000003_api_indexes.up.sql) / [000003_api_indexes.down.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000003_api_indexes.down.sql)
- [000004_add_foreign_keys.up.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000004_add_foreign_keys.up.sql) / [000004_add_foreign_keys.down.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000004_add_foreign_keys.down.sql)
- [000005_escrow_chain_events.up.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000005_escrow_chain_events.up.sql) / [000005_escrow_chain_events.down.sql](file:///d:/projects/wtf-onchain-monitor/indexer/migrations/000005_escrow_chain_events.down.sql)

### 2.2 Applied Migrations Query (`indexer.schema_migrations`)
Command:
```bash
docker exec wtf-postgres psql -U wtf_user -d wtf_indexer -c "SELECT version, applied_at FROM indexer.schema_migrations ORDER BY version;"
```
Output:
```
                    version                    |          applied_at           
-----------------------------------------------+-------------------------------
 000001_initial_schema.up.sql                  | 2026-09-23 06:36:40.26183+00
 000002_token_transfers_and_checkpoints.up.sql | 2026-09-23 06:36:40.307443+00
 000003_api_indexes.up.sql                     | 2026-09-23 06:36:40.329439+00
 000004_add_foreign_keys.up.sql                | 2026-09-23 06:36:40.348056+00
 000005_escrow_chain_events.up.sql             | 2026-09-23 06:50:06.54718+00
(5 rows)
```
Migration `000005_escrow_chain_events.up.sql` is confirmed applied and active in PostgreSQL (`localhost:5433`).

---

## 3. Schema Validation (`indexer.chain_events`)

### 3.1 Column Specifications
Command:
```bash
docker exec wtf-postgres psql -U wtf_user -d wtf_indexer -c "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema = 'indexer' AND table_name = 'chain_events' ORDER BY ordinal_position;"
```

Output:
```
    column_name     |        data_type         | is_nullable |                     column_default                     
--------------------+--------------------------+-------------+--------------------------------------------------------
 event_id           | bigint                   | NO          | nextval('indexer.chain_events_event_id_seq'::regclass)
 chain_id           | bigint                   | NO          | 
 contract_address   | character varying        | NO          | 
 event_name         | character varying        | NO          | 
 tx_hash            | character varying        | NO          | 
 block_number       | bigint                   | NO          | 
 block_timestamp    | timestamp with time zone | NO          | 
 log_index          | integer                  | NO          | 
 removed            | boolean                  | NO          | false
 raw_data           | jsonb                    | YES         | 
 created_at         | timestamp with time zone | NO          | now()
 escrow_id          | character varying        | YES         | 
 buyer              | character varying        | YES         | 
 seller             | character varying        | YES         | 
 amount             | numeric                  | YES         | 
 alchemy_webhook_id | text                     | YES         | 
 raw_payload        | text                     | YES         | 
 block_hash         | character varying        | YES         | 
 tx_index           | integer                  | YES         | 0
 indexed_at         | timestamp with time zone | YES         | now()
(20 rows)
```

Direct column verification:
- `escrow_id`: `character varying(66)`, Nullable: `YES` - WTFEscrow identifier (bytes32 hex format).
- `buyer`: `character varying(42)`, Nullable: `YES` - Buyer Ethereum wallet address.
- `seller`: `character varying(42)`, Nullable: `YES` - Seller / beneficiary Ethereum wallet address.
- `amount`: `numeric(36,18)`, Nullable: `YES` - Settled amount in token / wei denomination.
- `alchemy_webhook_id`: `text`, Nullable: `YES` - Unique Alchemy webhook delivery ID.
- `raw_payload`: `text`, Nullable: `YES` - Full JSON webhook payload for auditing and replaying.
- `block_hash`: `character varying(66)`, Nullable: `YES` - Block header hash.
- `tx_index`: `integer`, Nullable: `YES`, Default: `0` - Transaction index within block.
- `indexed_at`: `timestamp with time zone`, Nullable: `YES`, Default: `now()` - Ingestion timestamp.
- `raw_data`: Nullable changed from `NO` to `YES` to support direct webhook ingestion without full ABI decoding.

### 3.2 Foreign Key Constraint Removal Confirmation
Command:
```bash
docker exec wtf-postgres psql -U wtf_user -d wtf_indexer -c "SELECT conname, contype, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'indexer.chain_events'::regclass;"
```
Output:
```
      conname      | contype |                  pg_get_constraintdef                   
-------------------+---------+---------------------------------------------------------
 chain_events_pkey | p       | PRIMARY KEY (event_id)
 uq_chain_event    | u       | UNIQUE (chain_id, contract_address, tx_hash, log_index)
(2 rows)
```
- No constraints of type `'f'` (foreign key) exist on `indexer.chain_events`.
- Constraint `fk_chain_event_transaction` was removed, allowing standalone event persistence without requiring prior transaction ingestion.

---

## 4. Index Verification

Command:
```bash
docker exec wtf-postgres psql -U wtf_user -d wtf_indexer -c "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'indexer' AND tablename = 'chain_events' ORDER BY indexname;"
```

Output:
```
         indexname          |                                                        indexdef                                                         
----------------------------+-------------------------------------------------------------------------------------------------------------------------
 chain_events_pkey          | CREATE UNIQUE INDEX chain_events_pkey ON indexer.chain_events USING btree (event_id)
 idx_chain_events_chain_tx  | CREATE INDEX idx_chain_events_chain_tx ON indexer.chain_events USING btree (chain_id, tx_hash)
 idx_chain_events_escrow_id | CREATE INDEX idx_chain_events_escrow_id ON indexer.chain_events USING btree (escrow_id)
 uq_chain_event             | CREATE UNIQUE INDEX uq_chain_event ON indexer.chain_events USING btree (chain_id, contract_address, tx_hash, log_index)
 uq_chain_events_tx_log     | CREATE UNIQUE INDEX uq_chain_events_tx_log ON indexer.chain_events USING btree (tx_hash, log_index)
(5 rows)
```

1. **`idx_chain_events_escrow_id`**: Confirmed active. Created via `CREATE INDEX idx_chain_events_escrow_id ON indexer.chain_events USING btree (escrow_id)`. Optimizes filtering by `escrow_id` for API queries and reconciliation jobs.
2. **`uq_chain_events_tx_log`**: Confirmed active. Created via `CREATE UNIQUE INDEX uq_chain_events_tx_log ON indexer.chain_events USING btree (tx_hash, log_index)`. Enforces strict uniqueness on `(tx_hash, log_index)` to guarantee database-level deduplication across concurrent webhook calls.

---

## 5. Repository & Test Results

### 5.1 Repository Implementation Overview ([chain_events_repo.go](file:///d:/projects/wtf-onchain-monitor/indexer/internal/repository/chain_events_repo.go))
- **`InsertChainEvent(ctx, event)`**:
  - Implements idempotent insert with `ON CONFLICT (tx_hash, log_index) DO NOTHING RETURNING event_id`.
  - Captures `event.EventID` upon insertion.
  - Automatically handles conflicts by returning `nil` error without duplicating records.
- **`GetEventsByEscrowID(ctx, escrowID)`**:
  - Retrieves all events for an escrow ordered by `block_number ASC, log_index ASC`.
  - Scans `amount` via `COALESCE(amount::text, '')` to avoid precision loss.
- **`GetEventByTxAndLogIndex(ctx, txHash, logIndex)`**:
  - Fast point lookup using `uq_chain_events_tx_log` index.
  - Returns `nil, nil` when no matching record exists.

### 5.2 Repository Test Execution (`internal/repository`)
Execution command:
```powershell
$env:CGO_ENABLED="0"; go test -v -count=1 ./internal/repository/...
```
Output:
```
=== RUN   TestChainEventsRepository_EscrowSettled_StandaloneInsertAndIdempotency
--- PASS: TestChainEventsRepository_EscrowSettled_StandaloneInsertAndIdempotency (0.04s)
PASS
ok  	worldtradefuture/indexer/internal/repository	0.427s
```

Test validations performed in [chain_events_repo_test.go](file:///d:/projects/wtf-onchain-monitor/indexer/internal/repository/chain_events_repo_test.go):
1. **Standalone Insertion**: Successfully inserted an `EscrowSettled` event without a preceding record in `indexer.transactions`.
2. **Idempotency**: Repeated insertion of identical `(tx_hash, log_index)` completed without error (`err == nil`) and preserved existing state without duplicating rows.
3. **Escrow Querying**: `GetEventsByEscrowID` returned the exact event matching buyer, seller, amount (`500.000000000000000000`), and `alchemy_webhook_id`.
4. **Tuple Querying**: `GetEventByTxAndLogIndex` returned the exact event for existing records and `nil` for non-existent queries.

### 5.3 Persistence Test Execution (`internal/persistence`)
Execution command:
```powershell
$env:CGO_ENABLED="0"; go test -v -count=1 ./internal/persistence/...
```
Output:
```
=== RUN   TestPersistence_Payroll_EmployeeAddedAndRemoved
--- PASS: TestPersistence_Payroll_EmployeeAddedAndRemoved (0.04s)
=== RUN   TestNewRedisClient_Ping
--- PASS: TestNewRedisClient_Ping (0.00s)
=== RUN   TestPersistence_TokenTransfer_PersistenceAndIdempotency
--- PASS: TestPersistence_TokenTransfer_PersistenceAndIdempotency (0.03s)
=== RUN   TestPersistence_TokenTransfer_MultipleEventsPerTransaction
--- PASS: TestPersistence_TokenTransfer_MultipleEventsPerTransaction (0.02s)
=== RUN   TestPersistence_SyncCheckpoint
--- PASS: TestPersistence_SyncCheckpoint (0.02s)
=== RUN   TestPersistence_SaveTokenBatch_StoresBlockHash
--- PASS: TestPersistence_SaveTokenBatch_StoresBlockHash (0.02s)
PASS
ok  	worldtradefuture/indexer/internal/persistence	0.567s
```

### 5.4 Full Suite Test Execution (`go test -count=1 ./...`)
```
?   	worldtradefuture/indexer/cmd/api                	[no test files]
?   	worldtradefuture/indexer/cmd/clear-checkpoint   	[no test files]
?   	worldtradefuture/indexer/cmd/indexer            	[no test files]
?   	worldtradefuture/indexer/cmd/inspect-schema     	[no test files]
?   	worldtradefuture/indexer/cmd/reconciler         	[no test files]
ok  	worldtradefuture/indexer/internal/abi           	6.147s
?   	worldtradefuture/indexer/internal/api           	[no test files]
ok  	worldtradefuture/indexer/internal/api/handlers  	0.821s
ok  	worldtradefuture/indexer/internal/api/middleware	5.010s
?   	worldtradefuture/indexer/internal/api/responses 	[no test files]
ok  	worldtradefuture/indexer/internal/api/validation	4.334s
ok  	worldtradefuture/indexer/internal/blockchain    	8.356s
ok  	worldtradefuture/indexer/internal/config        	6.092s
ok  	worldtradefuture/indexer/internal/decoder       	6.162s
ok  	worldtradefuture/indexer/internal/indexer       	1.712s
?   	worldtradefuture/indexer/internal/models        	[no test files]
ok  	worldtradefuture/indexer/internal/persistence   	1.349s
ok  	worldtradefuture/indexer/internal/reconciliation	6.394s
ok  	worldtradefuture/indexer/internal/repository    	1.138s
```
**Test Pass Rate**: 100% across all 11 packages (0 failures).

---

## 6. Readiness for Phase 3 (Webhook Handler & Redis Deduplication)

The repository and persistence layer are fully prepared for Phase 3 implementation:

1. **HMAC Signature Validation**:
   - `Config.AlchemyWebhookSigningKey` is loaded from `ALCHEMY_WEBHOOK_SIGNING_KEY` (default `"whsec_test_dummy_key"`), ready to validate `X-Alchemy-Signature` headers with SHA256 HMAC.
2. **Two-Tier Deduplication Pattern**:
   - **Tier 1 (Fast In-Memory / Redis)**: Redis instance on `localhost:6379` ([redis.go](file:///d:/projects/wtf-onchain-monitor/indexer/internal/persistence/redis.go)) is ready to execute atomic `SET NX EX` checks on `dedup:webhook:{webhook_id}` or `dedup:event:{tx_hash}:{log_index}` with an expiration TTL (e.g. 24h).
   - **Tier 2 (Durable Database Idempotency)**: `ChainEventsRepository.InsertChainEvent` utilizes `ON CONFLICT (tx_hash, log_index) DO NOTHING` via unique index `uq_chain_events_tx_log`, providing a database-level guarantee that replay attacks or webhook retries will never duplicate rows.
3. **Escrow Contract Scoping**:
   - `Config.WTFEscrowContractAddress` is configured and ready to filter incoming webhook events against the canonical contract address.
4. **Auditability & Replay**:
   - `alchemy_webhook_id` and `raw_payload` columns store complete incoming webhook payloads, enabling audit trails and reprocessing of malformed or reorged blocks if needed.

Phase 2 implementation is complete, verified, and ready for Phase 3 development.
