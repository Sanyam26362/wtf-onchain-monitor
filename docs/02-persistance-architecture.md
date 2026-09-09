# WTF — Persistence Architecture

## 1. What is Persistence?

Persistence is the **storage layer** of the project.

It receives data from the Indexer, stores it safely in PostgreSQL, and provides data for the API and Reconciliation Worker.

![Basic Architecture](../Architecture_Diagrams/Indexer_Arch.drawio.png)

## 2. Main Responsibilities

- Store blockchain events
- Prevent duplicate records
- Save checkpoints
- Store transaction status
- Store payroll and token data
- Handle reorgs safely
- Keep blockchain provenance
- Support efficient reads
- Manage database migrations

## 3. Main Tables

```text
sync_checkpoints
chain_events
transactions
employers
employees
payroll_fundings
salary_claims
token_transfers
reconciliation_exceptions
```

### `chain_events`

This is the **raw event ledger**.

It keeps information such as:

```text
chain_id
contract_address
tx_hash
block_number
log_index
block_hash
removed
raw_data
```

Other tables are built from these events.

## 4. No Duplicates

The database should enforce event uniqueness using:

```text
chain_id
+ contract_address
+ tx_hash
+ log_index
```

So if the same event is processed again:

```text
Already exists → Skip
```

This makes the system safe during retries and restarts.

## 5. Checkpoints

A checkpoint tells the Indexer where it can safely continue.

```text
Process data
    ↓
Save data
    ↓
Update checkpoint
```

Both should happen in the **same database transaction**.

If something fails, neither should be committed.

## 6. Reorg Handling

Never hard-delete blockchain events.

```text
Reorg
  ↓
Mark old events as removed
  ↓
Process canonical events
  ↓
Rebuild projections
```

This keeps the history/audit trail.

## 7. Read Path

API and Reconciliation mainly read from PostgreSQL.

Common filters:

```text
wallet
employer
employee
token
date range
block range
```

Use indexes and pagination so large datasets remain fast.

## 8. Data Provenance

Every event should be traceable back to the blockchain:

```text
chain_id
contract_address
tx_hash
log_index
```

This answers:

> Which blockchain event created this database record?

## 9. Important Guarantees

```text
Duplicate-Safe
Crash-Safe
Reorg-Safe
Rebuildable
Traceable to Blockchain
```

## 10. Module Structure

```text
persistence/
├── migrations/
├── store/
│   ├── events.*
│   ├── checkpoints.*
│   ├── transactions.*
│   ├── payroll.*
│   ├── tokens.*
│   └── reconciliation.*
└── queries/
```

## 11. Simple Definition

> **Persistence is the layer that safely stores blockchain data in PostgreSQL, prevents duplicates, handles reorgs, saves checkpoints, and makes the data available for reading.**
