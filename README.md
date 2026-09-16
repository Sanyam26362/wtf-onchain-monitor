# WTF On-Chain Monitor

A blockchain monitoring and indexing service for the **World Trade Future (WTF)** payroll system.

The service reads payroll and token activity from the blockchain, decodes and normalizes the events, stores them in PostgreSQL, exposes the data through REST APIs, and uses reconciliation to detect inconsistencies between the blockchain and database.

---

---

## Repository Structure

### Data Flow

```text
Sepolia Blockchain
        ↓
   RPC / Log Fetcher
        ↓
   Event Decoder
        ↓
 Normalizer / Indexer
        ↓
    PostgreSQL
        ↓
 ┌──────┴────────┐
 ↓               ↓
API       Reconciliation
 ↓               ↓
Next.js       Exceptions
Dashboard
