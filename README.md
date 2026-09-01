# TransactionFlow

A reference .NET 10 solution that demonstrates a **reliable, at-least-once transaction processing pipeline** built around the **transactional outbox pattern**. A producer generates synthetic transactions, ships them to Kafka, a worker consumes and persists them to PostgreSQL inside the same database transaction as an outbox row, and a background dispatcher relays outbox rows to downstream topics — guaranteeing that a committed transaction is never lost, even if the worker crashes after the DB commit but before publishing to Kafka.

---

## Why this exists

In a naive pipeline you write to the database, then publish to Kafka. If the process dies between those two steps, the transaction is committed but downstream systems never learn about it. This solution shows one of the standard fixes: write the outgoing event to an `outbox_messages` table **in the same DB transaction** as the business write, then let a separate process drain the outbox to Kafka. The database is the single source of truth; Kafka is just a notification channel.

---

## Architecture

```
┌──────────────────┐      Kafka        ┌──────────────────────┐       Postgres       ┌─────────────────────┐
│  Producer        │  transactions ──▶ │  Worker              │ ── same TX ──▶     │  processed_         │
│  (load gen)      │                   │  (TransactionConsumer│   processed_        │  transactions       │
│                  │                   │   + ProcessingSvc)   │   transactions      │  merchant_          │
│                  │                   │                      │   outbox_messages   │  aggregates         │
└──────────────────┘                   │                      │                     │  outbox_messages    │
                                       │  OutboxDispatcher  ──┼──── Kafka ────▶     │                     │
                                       │  (BackgroundService) │   downstream topic  │                     │
                                       └──────────────────────┘                     └─────────────────────┘
```

- **Producer** generates synthetic `TransactionMessage`s at a configurable rate and publishes them to Kafka.
- **Worker** consumes from `transactions`, validates, persists a `processed_transactions` row, upserts a `merchant_aggregates` row, and writes a corresponding `outbox_messages` row — all inside one EF Core transaction.
- **OutboxDispatcher** polls unpublished `outbox_messages`, ships them to Kafka, and marks them published. On failure it increments `Attempts` and stores the error message for inspection.

---

## Solution layout

```
TransactionFlow.slnx
├── docker-compose.yml            # Postgres 17 + Apache Kafka 4.0 (KRaft, single node)
├── init.sql                      # Schema applied to Postgres on first boot
├── docker/postgres/init.sql/     # Mirror used by the compose volume
│
├── TransactionFlow.Domain/              # Pure domain model (Transaction, TransactionStatus, MerchantAggregate)
├── TransactionFlow.Application/         # Use-case orchestration (validation, retry, processing pipeline)
├── TransactionFlow.Contracts/          # Cross-process message DTOs (TransactionMessage, RetryMessage,
│                                       #   DeadLetterMessage, TransactionProcessedEvent)
├── TransactionFlow.Infrastructure/     # EF Core DbContext, repositories, Kafka wiring, outbox store,
│                                       #   OpenTelemetry-aware diagnostics
│
├── TransactionFlow.Producer/            # Console host: synthetic load generator → Kafka
├── TransactionFlow.Worker/              # Console host: Kafka consumer + outbox dispatcher, OpenTelemetry
│
├── TransactionFlow.Domain.Tests/        # xUnit unit tests for Domain
└── TransactionFlow.Integration.Tests/   # xUnit integration tests (DB + Kafka)
```

### Project responsibilities at a glance

| Project          | Responsibility                                                                                                                            |
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `Domain`         | `Transaction` aggregate invariants, `MerchantAggregate`, `TransactionStatus`, domain exceptions. No dependencies.                         |
| `Application`    | `TransactionValidator`, `TransactionProcessor`, `TransactionProcessingService` (retry policy, error classification, transient detection). |
| `Contracts`      | Wire-format DTOs shared by Producer and Worker (`TransactionMessage`, retry envelope, DLQ envelope, `TransactionProcessedEvent`).         |
| `Infrastructure` | `TransactionFlowDbContext` (Npgsql/EF Core), repositories, `OutboxStore`, migrations, `DatabaseCommitLatencyProbe`, DI composition root.  |
| `Producer`       | Hosted console app that synthesizes transactions and pushes to Kafka.                                                                     |
| `Worker`         | Hosted console app with two background services: `TransactionConsumer` and `OutboxDispatcher`. Adds OpenTelemetry metrics/tracing.        |

---

## Prerequisites

- **.NET 10 SDK**
- **Docker** (or Podman with a compose-compatible CLI) for Postgres + Kafka
- Ports `5432` (Postgres) and `9092` (Kafka) free on `localhost`

---

## Quick start

```bash
# 1. Start dependencies (Postgres + Kafka)
docker compose up -d

# 2. Build everything
dotnet build TransactionFlow.slnx

# 3. Generate load (Producer) — defaults: 10,000 msgs @ 1,000/s, 10 merchants
dotnet run --project TransactionFlow.Producer

# 4. In another shell, run the Worker (consumer + outbox dispatcher)
dotnet run --project TransactionFlow.Worker
```

The Worker will:

1. Validate each consumed `TransactionMessage` (delegating to `TransactionValidator`).
2. Open an EF Core transaction and persist the processed transaction, update the merchant aggregate, and insert an outbox row.
3. Retry transient failures (timeout-class exceptions) up to `MaxAttempts` (currently `3`) before surfacing the error.
4. Have the `OutboxDispatcher` background service drain unpublished outbox rows to Kafka on a 500 ms cadence, marking each as published.

---

## Database schema

`init.sql` (and `docker/postgres/init.sql`) creates two tables on first Postgres boot:

- **`processed_transactions`** — one row per accepted transaction, keyed by `transaction_id`. Unique by construction makes processing idempotent.
- **`merchant_aggregates`** — running totals per `(merchant_id, currency)` of successful transaction count and amount. Updated atomically with the processed-transaction insert.
- **`outbox_messages`** — added by EF Core migration `20260901162344_AddOutboxMessage`. Columns include `id`, `occurred_at`, `payload`, `published_at` (nullable), `attempts`, `error`.

Constraints of note:

```sql
CONSTRAINT ck_successful_count_non_negative   CHECK (successful_transaction_count >= 0)
CONSTRAINT ck_successful_amount_non_negative  CHECK (successful_transaction_amount >= 0)
```

---

## Configuration

Both hosted apps read `appsettings.json` with overrides via `appsettings.Development.json` and environment variables (standard `Microsoft.Extensions.Configuration`).

### Producer (`TransactionFlow.Producer/appsettings.json`)

```json
{
  "Kafka": { "BootstrapServers": "localhost:9092", "Topic": "transactions" },
  "Load": { "Count": 10000, "Rate": 1000, "Concurrency": 32 },
  "TransactionGeneration": {
    "MerchantCount": 10,
    "SuccessRate": 0.9,
    "DuplicateRate": 0.0
  }
}
```

| Setting                               | Meaning                                                            |
| ------------------------------------- | ------------------------------------------------------------------ |
| `Load.Count`                          | Total messages to send before the host exits.                      |
| `Load.Rate`                           | Target messages per second.                                        |
| `Load.Concurrency`                    | Producer flush degree.                                             |
| `TransactionGeneration.SuccessRate`   | Fraction of generated transactions with `Status = Success`.        |
| `TransactionGeneration.DuplicateRate` | Fraction of messages emitted twice (used to exercise idempotency). |

### Worker (`TransactionFlow.Worker/appsettings.json`)

```json
{
  "ConnectionStrings": {
    "TransactionFlow": "Host=localhost;Port=5432;Database=transactionflow;Username=transactionflow;Password=transactionflow"
  },
  "Kafka": {
    "BootstrapServers": "localhost:9092",
    "Topic": "transactions",
    "GroupId": "transactionflow-worker",
    "DeadLetterTopic": "transactions.dlq"
  },
  "FailureInjection": { "CrashAfterDatabaseCommit": false }
}
```

`FailureInjection.CrashAfterDatabaseCommit` is a chaos knob wired through `FailureInjectionOptions`: when `true`, the worker deliberately aborts after committing the database transaction but before the Kafka offset is committed, so you can verify that re-running the consumer re-applies the side effects idempotently.

---

## Observability

`TransactionFlow.Worker` registers OpenTelemetry with the resource name `TransactionFlowTelemetry.ServiceName`:

- **Metrics** (`AddMeter(TransactionFlowTelemetry.ServiceName)`) — exported to console; swap `AddConsoleExporter()` for OTLP in production.
- **Tracing** (`AddSource(...)`) — same.

`TransactionFlow.Infrastructure/Diagnostics/DatabaseCommitLatencyProbe` is a one-shot diagnostic you can run from a small host to measure commit latency under load (see the commented block at the bottom of `Worker/Program.cs`).

---

## Testing

```bash
# Everything
dotnet test

# Just the fast in-memory unit tests
dotnet test TransactionFlow.Domain.Tests/TransactionFlow.Domain.Tests.csproj

# Integration tests (require the docker compose stack running)
dotnet test TransactionFlow.Integration.Tests/TransactionFlow.Integration.Tests.csproj
```

`TransactionFlow.Domain.Tests` exercises invariants on `Transaction` and `MerchantAggregate`. `TransactionFlow.Integration.Tests` spins up the real Postgres + Kafka via `docker-compose.yml` and verifies end-to-end behavior, including idempotency and the outbox path.

---

## Development tips

- **Run a single project:** `dotnet run --project TransactionFlow.Worker` (or `.Producer`).
- **Reset state:** `docker compose down -v` will drop both `postgres_data` and `kafka_data` volumes so you start clean.
- **Change the schema:** edit `init.sql` _and_ add an EF Core migration under `TransactionFlow.Infrastructure/Migrations/`; keep them in sync.
- **Tune retry policy:** `TransactionProcessingService` has a `MaxAttempts` constant and an `IsTransient` predicate — extend `IsTransient` to plug in your real classifier (network blips, EF deadlock exceptions, etc.).
- **Extend the outbox:** the dispatcher batches up to 100 messages every 500 ms (`OutboxDispatcher.ExecuteAsync`). Adjust the `Take(100)` and `Task.Delay` for your throughput target.

---

## Roadmap ideas

- Outbox dispatcher with a `SKIP LOCKED` claim loop for multi-replica deployments.
- Replace console OpenTelemetry exporters with OTLP → your collector.
- Backoff-aware error classifier (dead-letter on terminal errors, retry-with-jitter on transient).
- Schema for replayable event versioning.

---

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). See [`LICENSE`](LICENSE) for the full text.

```
Copyright 2026 The TransactionFlow Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
