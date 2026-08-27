---
name: milestone-11-producer-transactions
description: Milestone 11 scope (producer idempotence + transactions), its phase/agent numbering, and the scope decisions taken during planning on 2026-07-30
metadata:
  type: project
---

Milestone 11 translates producer **idempotence and transactions** (Java
`TransactionManager` + its send-path integration) into Rust. Planned
2026-07-30 as 8 phases using **agent numbers 41–48**.

**Why:** `ProducerConfig::enable_idempotence` defaults to `true` but was never
read by `KafkaProducer`/`Sender` — the producer advertised idempotence and
delivered at-least-once. Milestone 11 makes the default honest.

**How to apply:** Plan drafts live at `design/history/Milestone-11/PLAN.md` and
`Milestone-11/Phase-1/PLAN.md`. As of writing they are **DRAFT, uncommitted,
pending user approval** — check git status and the `Status:` header before
treating any of it as agreed.

Scope decisions taken during planning (each recorded with reasoning in the
milestone plan; overturnable at approval):

- **Rust only.** C FFI / Python / gRPC multilanguage harness deferred. The
  sharp reason: a transaction spans multiple FFI calls
  (`begin` → N× `send` → `commit`), so the Milestone-9 "one-operation-in-flight"
  access guard has no story for an *open* transaction. That is a design
  question, not a translation question.
- **`WriteTxnMarkers` request/response is out of scope** — verified
  Admin/coordinator-side only (`Admin.java`, `AbortTransactionHandler.java`);
  zero uses under `producer/**`. Its two Java test files go out of scope with it.
- **`EndTransactionMarker` is out of scope** — broker-side control-record write
  path (`MemoryRecordsBuilder`/`MemoryRecords`), never constructed by the
  producer. Stays in `remaining_classes.txt`.
- **`MockProducer`'s transactional surface is IN scope** (Phase 7) — 44 named
  skipped tests listed at `src/producer/mock_producer.rs:525–577`. This was
  additional scope beyond the original briefing.
- **KIP-890 transaction V2 and KIP-939 2PC are in scope** — both are present in
  the Kafka 4.2 `TransactionManager` and TV2 changes behavior materially (under
  TV2 the producer sends no `AddPartitionsToTxn`/`AddOffsetsToTxn` at all), so
  a V1-only translation would pass against old brokers and silently misbehave
  against modern ones.

Agent-number convention: `N` = global phase number, and root `COMMENTS.<N>.md`
must not collide with archived `COMMENTS.DONE.*.md`. Numbers used through
Milestone 10: 0, 1, 2, 23, 31–35, 37, 39, 40.

See [[stale-design-current-docs]] for the trap when reading project state.
