# WTF On-Chain Monitor

A blockchain monitoring and indexing system for the WTF payroll smart contract.

The project reads on-chain data, stores it in PostgreSQL, exposes it through an API, and uses reconciliation to verify that the database matches the blockchain.

## Architecture

```text
Blockchain
    ↓
Indexer
    ↓
PostgreSQL
    ↓
API
    ↓
Client

        ↓
Reconciliation
        ↓
Blockchain ↔ Database
```

## Main Components

### 1. Indexer

Reads blockchain blocks and relevant contract events.

```text
RPC
 ↓
Block Fetcher
 ↓
Event Filter
 ↓
Event Decoder
 ↓
Normalizer
 ↓
Idempotency Check
 ↓
PostgreSQL
```

Responsibilities:

* Fetch blockchain data
* Process relevant contract events
* Decode and normalize events
* Prevent duplicate data
* Maintain indexing checkpoint
* Handle retries and failures

### 2. Persistence

Stores indexed blockchain data in PostgreSQL.

Responsibilities:

* Store events and contract state
* Maintain indexer checkpoints
* Provide reliable database access
* Support idempotent writes

### 3. API

Provides access to indexed data through application endpoints.

```text
Client
  ↓
API
  ↓
PostgreSQL
```

### 4. Reconciliation

Acts as a safety check between the database and blockchain.

```text
Indexed / Expected State
          ↓
    Expected Balance
          ↓
      Compare
          ↑
    Observed Balance
          ↑
     Contract Read
```

If the expected and observed states do not match, the system can detect a possible indexing or data consistency issue.

## Architecture Diagrams

The `Architecture_Diagrams` directory contains the system design and detailed component diagrams.

Recommended reading order:

1. Basic Architecture
2. Indexer Architecture
3. Persistence Architecture
4. API Architecture
5. Reconciliation Architecture
6. Expected vs Observed Reconciliation

## Technology Stack

* **Blockchain** — Smart Contract / EVM
* **Indexer** — Backend service
* **Database** — PostgreSQL
* **API** — REST API
* **Architecture** — Draw.io

## Project Structure

```text
wtf-onchain-monitor/
│
├── Architecture_Diagrams/
│   ├── Basic_Arch
│   ├── High_Level_API
│   ├── API_Arch
│   ├── Indexer_Arch
│   ├── Persistence_Arch
│   ├── Reconciliation_Arch
│   └── Expected_vs_Observed_Reconciliation
│
├── README.md
└── ...
```

## Design Principles

* **Blockchain is the source of truth**
* **Database is a queryable representation of blockchain state**
* **Indexer must be restartable**
* **Writes must be idempotent**
* **Checkpoint advances only after successful persistence**
* **Reconciliation detects database drift**
* **Architecture should remain simple and implementation-focused**

## Current Status

The project is currently in the **architecture and design phase**.

The main system components and data flows are being defined before implementation.

### Roadmap

* [x] Define high-level architecture
* [x] Design indexer architecture
* [x] Design persistence architecture
* [x] Design API architecture
* [x] Design reconciliation architecture
* [ ] Finalize database schema
* [ ] Implement indexer
* [ ] Implement persistence layer
* [ ] Implement API
* [ ] Implement reconciliation worker
* [ ] Add tests
* [ ] Add monitoring and logging
* [ ] End-to-end testing

## Goal

Build a reliable on-chain monitoring system that can:

1. Read blockchain events.
2. Store them in PostgreSQL.
3. Provide queryable data through an API.
4. Recover safely from failures.
5. Detect inconsistencies between the database and blockchain.
