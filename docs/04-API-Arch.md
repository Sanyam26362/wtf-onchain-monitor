# WTF — API Architecture

> **Scope:** API module only.
>
> This document defines the architecture and responsibilities of the
> API layer for the WTF on-chain monitoring service.
>
> The Indexer, Persistence/Data Layer, Reconciliation Worker,
> Frontend/Next.js dashboard, Configuration, and Observability modules
> are treated as external dependencies or consumers. Their detailed
> architecture is intentionally outside this document.

---

# 1. Purpose

The API is the read/operate surface of the WTF monitoring service. It
exposes indexed and reconciled blockchain data to the Next.js dashboard
and other backend consumers — it does not fetch from the chain, decode
events, or run reconciliation logic itself.

```text
Chain data (via Indexer)
Reconciliation results (via Reconciliation Worker)
        |
        v
   Persistence Layer
        |
        v
       API
        |
        v
Next.js Dashboard / other consumers
```

The API answers, roughly:

> "What does the system currently know, and is it healthy?"

It never answers:

> "What is happening on the chain right now?" (that's the Indexer's job)
> "Is this data trustworthy?" (that's the Reconciliation Worker's job)

The API's job is to surface what those two modules have already
produced, safely and consistently.

---

# 2. Position in the Overall System

![API Architecture](../Architecture_Diagrams/API_Arch.drawio.png)

The API is a **pure consumer** of the Persistence Layer's read
interface (Section 8 of the Persistence doc). It must not:

- Write directly to `chain_events`, `sync_checkpoints`, or any
  indexer-owned table
- Call the RPC adapter or contracts directly (with one narrow
  exception — see Section 9, backfill triggering)
- Compute reconciliation logic inline (it only reads
  `reconciliation_exceptions`, never decides what belongs there)

---

# 3. Core Responsibilities

```text
1. Expose HTTP endpoints for indexed data
2. Enforce consistent request validation
3. Enforce consistent response envelopes and error codes
4. Apply pagination, filtering, and sorting
5. Protect operator-only endpoints (e.g. triggering backfill)
6. Report system health and sync status
7. Generate chain-correct explorer links
8. Never expose secrets (RPC URLs, DB credentials) in responses or logs
```

---

# 4. High-Level Architecture

![High Level API Architecture](../Architecture_Diagrams/High_Level_API.drawio.png)

---

# 5. Request Lifecycle

```text
Incoming request
       |
       v
Middleware: validate shape/params
       |
       v
Middleware: auth check (if operator route)
       |
       v
Handler: parse filters/pagination
       |
       v
Query layer: build bounded, parameterized query
       |
       v
Persistence read (read-only)
       |
       v
Handler: map rows -> response DTO
       |
       v
Envelope + pagination metadata
       |
       v
Response
```

On error at any stage:

```text
Error
  |
  v
Normalize to a stable error code + message
  |
  v
Never leak: SQL, stack traces, connection strings, RPC URLs
  |
  v
Log full detail internally, return safe detail externally
```

---

# 6. Response Envelope

All endpoints share one envelope shape so consumers can write one
parsing path.

```text
Success:
{
  "data": <resource or array>,
  "pagination": { "limit", "cursor" | "offset", "next", "total"? } | null,
  "meta": { "chain_id", "network" } | null
}

Error:
{
  "error": {
    "code": "STABLE_MACHINE_READABLE_CODE",
    "message": "human-readable summary",
    "details": { ... } | null
  }
}
```

### Rules

```text
- HTTP status code AND error.code both convey the failure category
  (status for transport-level handling, code for programmatic branching)
- error.code values are stable across releases; message text is not
  a contract and may change
- pagination is always present on list endpoints, always null on
  single-resource endpoints
```

---

# 7. Pagination, Filtering, Sorting

```text
List endpoint request
        |
        v
Parse: limit (bounded, e.g. max 200)
       cursor or offset
       sort field + direction (whitelisted only)
       filters (wallet, token, employer, employee,
                date range, block range, status)
        |
        v
Validate each filter against its expected type/format
(address shape, block number, ISO date)
        |
        v
Reject with 400 + stable error code on invalid input
        |
        v
Build parameterized query via Persistence's query helpers
(Section 11 of the Persistence doc)
```

### Guidance

```text
- Prefer cursor-based pagination for chain_events/transfers-style
  high-volume tables (block_number + log_index as a stable cursor)
- Offset pagination is acceptable for small, bounded tables
  (employers, employees)
- Never allow an unbounded query — always cap limit server-side
  regardless of what the client requests
```

---

# 8. Endpoint Groups

This mirrors Section 8 of the original specification; grouped here by
responsibility rather than listed flat.

## 8.1 System health

```text
GET /health     — liveness only, no dependency checks
GET /ready      — checks DB reachability, RPC reachability
                  (via indexer's last-successful-check, not a live
                  RPC call from the API itself), and required
                  configuration presence
GET /v1/sync/status
                — current block, safe/finalized block, lag,
                  last error, per-stream status
                  (reads sync_checkpoints + transactions)
```

`/ready` and `/v1/sync/status` are the two places where the
configuration ambiguity problem (Section 6 of the spec — conflicting
payroll addresses) becomes visible operationally: both must report
the **currently active** chain ID, contract addresses, token address,
and start block, so anyone can verify what the system is actually
watching without reading code.

## 8.2 Operator actions

```text
POST /v1/sync/backfill
     — bounded, operator-only trigger
     — the API does not run the backfill itself; it enqueues/signals
       the Indexer's backfill coordinator and returns immediately
     — requires auth (Section 10, spec's security requirements)
     — validated block range required; reject unbounded ranges
```

## 8.3 Transactions

```text
GET /v1/transactions/{hash}
     — status, confirmations, decoded events, explorer URL
     — explorer URL built from configured network, never hardcoded
       (Section 9 of the spec)
```

## 8.4 Payroll

```text
GET /v1/employers/{address}
     — profile, payroll balance projection, employees, runway inputs
GET /v1/employees/{address}
     — profile, employer, funding history, claim history
GET /v1/payroll/fundings
     — filterable by employer, employee, date, status
GET /v1/payroll/claims
     — filterable by employee, date
```

## 8.5 Token activity

```text
GET /v1/tokens/{address}/transfers
     — filterable by wallet, direction, block range
     — {address} is the configured generic ERC-20 address
       (Section 2/13 of the Indexer doc) — not hardcoded to a
       specific WTF token until one is deployed
```

## 8.6 Reconciliation

```text
GET /v1/reconciliation/exceptions
     — open and historical mismatches, filterable by type, severity,
       status, date range
     — pure read of reconciliation_exceptions; API never writes to
       this table
```

---

# 9. Boundary With the Indexer (Backfill Trigger)

The one place the API touches the Indexer rather than only Persistence:

```text
POST /v1/sync/backfill
        |
        v
Validate range + auth
        |
        v
Signal Indexer (queue message, internal call, or a durable
"requested_backfills" row the Indexer polls)
        |
        v
Return 202 Accepted immediately
        |
        v
Client polls /v1/sync/status for progress
```

The API does **not** block waiting for backfill completion, and does
not perform the fetch/decode/write cycle itself — that stays entirely
in the Indexer module.

---

# 10. Authentication & Authorization

```text
Public read endpoints:
  GET /health, /v1/transactions/*, /v1/employers/*, /v1/employees/*,
  /v1/payroll/*, /v1/tokens/*/transfers, /v1/reconciliation/exceptions

Operator-only endpoints:
  POST /v1/sync/backfill

Auth mechanism:
  Reuse existing platform authentication (Next.js app's session/auth)
  where the API sits behind or alongside it, OR a separate service
  credential (API key / signed service token) if the API is deployed
  standalone.
```

```text
Request to operator route
        |
        v
Auth middleware
        |
    Valid?
   /      \
  No       Yes
  |         |
  v         v
401/403   Proceed to handler
```

No private keys or signing authority are ever accepted by this module
(Section 10 of the spec) — the API triggers backfills, it never
constructs or signs transactions.

---

# 11. Validation Rules

```text
Addresses:    must match EVM address shape; normalize case
              (checksum or lowercase — pick one, apply consistently)
              before querying, per the Persistence doc's address
              normalization concern
Block ranges: start <= end, end - start within a configured max
              (mirrors the Indexer's bounded-chunk requirement)
Dates:        ISO 8601, reject ambiguous formats
Pagination:   limit within [1, max]; cursor must be well-formed
Chain ID:     must match the configured chain; reject/ignore mismatches
              rather than silently querying the wrong chain's data
```

All validation failures return `400` with a stable `error.code` — never
a raw database or parsing error.

---

# 12. Explorer Link Generation

```text
Transaction hash
       |
       v
Configured network (from Section 6/9's active configuration)
       |
       v
Build URL: https://{network}.etherscan.io/tx/{hash}
       |
       v
Never hardcode a specific network's domain in a handler
```

This lives as a small shared utility, not duplicated per-endpoint, so
a future network change (e.g. moving off Sepolia) doesn't require
hunting through every handler.

---

# 13. Failure Handling

Mirrors the Reconciliation doc's distinction between infrastructure
failure and data-level findings — the API has its own version of this
split.

```text
Infrastructure failure
  (DB unreachable, timeout)
        |
        v
  502/503 + observable error
        |
        v
  Does not mean "no data exists" —
  must not be confused with an empty result set

Valid empty result
  (query succeeded, zero rows match)
        |
        v
  200 + empty data array + pagination showing zero total
```

Conflating these two (returning `200` with an empty array when the DB
call actually failed) is the specific failure mode this section exists
to prevent — a dashboard reading "no transactions found" when the real
story is "the API couldn't reach the database" is a silent, dangerous
gap identical in spirit to the address-guessing problem in Section 6
of the spec.

---

# 14. Observability Hooks (Consumed, Not Owned)

The API does not own logging/metrics infrastructure (that's the
Observability module), but every request must emit:

```text
- Structured log line: method, path, status, duration, error.code (if any)
- Redacted logging: never log full connection strings, RPC URLs,
  or auth tokens/credentials
- A request ID propagated through to any downstream Persistence call,
  for tracing a slow query back to its originating request
```

---

# 15. Recommended Project Structure

```text
api/
│
├── server/
│   └── router.*, middleware.*
│
├── handlers/
│   ├── health.*
│   ├── sync.*
│   ├── transactions.*
│   ├── employers.*
│   ├── employees.*
│   ├── payroll.*
│   ├── tokens.*
│   └── reconciliation.*
│
├── dto/
│   └── response shapes per resource (mapped from Persistence rows)
│
├── validation/
│   └── address.*, pagination.*, daterange.*, blockrange.*
│
├── auth/
│   └── middleware.*
│
├── envelope/
│   └── success.*, error.*
│
└── explorer/
    └── link-builder.*
```

---

# 16. Module Responsibility Summary

| Component      | Responsibility                                                |
|-----------------|----------------------------------------------------------------|
| `server`        | Routing, middleware chain, request lifecycle                  |
| `handlers`      | Per-resource endpoint logic, calling Persistence's read layer  |
| `dto`           | Map internal rows to stable external response shapes          |
| `validation`    | Input validation for addresses, ranges, pagination, dates      |
| `auth`          | Operator-route protection                                      |
| `envelope`      | Consistent success/error response shape                        |
| `explorer`      | Network-aware explorer URL generation                          |

---

# 17. API Scope

## Included

```text
HTTP routing and middleware
Request validation
Pagination / filtering / sorting
Response envelope and stable error codes
Health / readiness / sync-status reporting
Backfill trigger (signal only, not execution)
Authentication for operator routes
Explorer link generation
```

## Intentionally Deferred

```text
Fetching/decoding chain data (Indexer's job)
Deciding what is a mismatch (Reconciliation's job)
Database schema and write-path idempotency (Persistence's job)
Dashboard UI rendering (Frontend's job)
Central secrets/config management (Configuration's job)
Metrics/log aggregation infrastructure (Observability's job)
```

---

# 18. One-Line Definition

> **The WTF API is a read-oriented, validation-first HTTP layer that
> exposes indexed payroll, token, transaction, and reconciliation data
> from the Persistence Layer to the dashboard and other consumers —
> enforcing consistent pagination, stable error codes, and a strict
> boundary that never writes chain data or performs reconciliation
> logic itself.**
