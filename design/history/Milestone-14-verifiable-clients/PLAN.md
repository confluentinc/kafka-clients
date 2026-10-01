# Milestone 14 — VerifiableProducer & VerifiableConsumer (system-test tools)

**Status:** COMPLETE (2026-09-15), uncommitted (user commits). Both phases done via Actor/Critic 65.
- Phase 1 (VerifiableProducer + ThroughputThrottler): findings #1/#2 fixed, #3/#4/#5 accepted as documented deviations.
- Phase 2 (VerifiableConsumer + StringDeserializer): findings P2-1 (SIGTERM, both bins) / P2-3 fixed, P2-2/P2-4 accepted as documented deviations.
All gates green: workspace build, `cargo test -p verifiable-clients` (57), `cargo test --lib`, format-check, clippy `-D warnings`.
**Scope:** Rust only. No C FFI / Python / gRPC.
**Java source:** Apache Kafka 4.3.1 (`kafka/` submodule at `26b251a`).
**Agent numbers:** 65 (Actor 65 / Critic 65). Highest previously used is 64.

---

## 1. What this milestone delivers

Rust translations of Apache Kafka's two client-facing system-test tools:

- `org.apache.kafka.tools.VerifiableProducer` (575 lines) — produces increasing
  integers to a topic and prints one JSON event per send-attempt / ack / error
  to stdout, so an external harness can verify which messages were acked.
- `org.apache.kafka.tools.VerifiableConsumer` (740 lines) — consumes a topic and
  prints JSON events for rebalances, record batches, and offset commits.

Plus their two out-of-crate dependencies:

- `org.apache.kafka.server.util.ThroughputThrottler` (138 lines) — the
  producer's rate limiter (Phase 1).
- `org.apache.kafka.common.serialization.StringDeserializer` (client class, in
  scope) — the consumer's value/key decoder (Phase 2).

The tools mirror Java's **stdout JSON contract** exactly (event `name`s, field
names, field order), because that contract is what a downstream ducktape-style
harness parses. This is the tool equivalent of DoD §3's "wire-level" fidelity:
the JSON *is* the wire.

## 2. Scope decisions (approved inputs)

1. **Tools-module scope exception.** `VerifiableProducer`/`Consumer` live in
   Kafka's `tools` module, which `CLAUDE.md` ("client only") scopes out. This
   milestone is a **deliberate, one-off exception**, recorded here, to provide
   runnable system-test clients over the already-translated producer/consumer.
   `VerifiableShareConsumer` (KIP-932 share consumer) stays **out of scope**
   (`consumer-threading.md` §20).
2. **Layout: new workspace crate.** A dedicated `tools/verifiable-clients/`
   crate (workspace member, precedent: `consumer-perf/`), mirroring Java's
   separate `tools` module ("preserve original architecture"). Keeps the client
   crate's dependency tree clean.
3. **CLI parsing: hand-rolled.** Manual `std::env::args()` parsing (repo
   convention: `consumer-perf`, `src/bin/message_generator.rs`). **No new
   dependency** (no clap/argparse). Arg names, defaults, `required`,
   mutually-exclusive groups, and `choices` are reproduced by hand to match
   `argParser()` behaviour.
4. **Sequence: producer first.** Phase 1 = VerifiableProducer + ThroughputThrottler;
   Phase 2 = VerifiableConsumer + StringDeserializer. Each is its own
   Actor→Critic cycle.
5. **JSON: `serde` / `serde_json`** (already a crate dependency; no approval
   needed). Field order and names pinned with `#[serde(rename = "...")]` and
   struct field ordering to match Java's `@JsonPropertyOrder` / `@JsonProperty`.
6. **ThroughputThrottler placement.** Inside the new crate
   (`src/throughput_throttler.rs`), documented as a translation of
   `server.util.ThroughputThrottler` pulled in as a tool dependency (there is no
   `server-common` crate to host it, and it has no other consumer).
7. **StringDeserializer placement.** In the **main client crate** at
   `src/common/serialization/string_deserializer.rs` (it is a genuine
   `common.serialization` client class — in scope — and belongs beside the
   existing `string_serializer.rs`), re-exported from
   `crate::common::serialization`.

## 3. Feasibility — no client-API blockers

Verified against the current Rust client surface:

| Java call | Rust API (exists) |
|---|---|
| `producer.send(record, callback)` | `KafkaProducer::send` + `Callback` |
| `new ProducerRecord(topic,null,createTime,k,v)` | `ProducerRecord` timestamp ctor |
| `consumer.subscribe(topics, listener)` | `subscribe_with_listener` |
| `consumer.poll(Duration)` | `poll` (async) |
| `consumer.commitSync(offsets)` | `commit_sync_offsets` |
| `consumer.commitAsync(offsets, cb)` | `commit_async_offsets_with_callback` |
| `ConsumerRebalanceListener` | trait (`#[async_trait]`) exists |
| `OffsetCommitCallback` | trait (`#[async_trait]`) exists |
| `consumer.wakeup()` / `close()` | `wakeup` / `close` |
| `StringSerializer` | `common::serialization::string_serializer` |

The **only** net-new library code is `ThroughputThrottler` and
`StringDeserializer`.

## 4. Async translation (CLAUDE.md §9)

The Rust consumer/producer public API is async where Java blocks. Both tools are
`#[tokio::main] async fn main`, awaiting `poll`/`commit`/`close`/`send`
(precedent: `src/bin/consumer_test.rs`). Specifically:

- Java's `consumer.wakeup()` from a JVM shutdown hook → a `tokio::signal::ctrl_c`
  task calling `consumer.wakeup()` (the `WakeupError` unwinds the poll loop
  exactly as Java's `WakeupException` does).
- Java's `Runtime.addShutdownHook` producer path (`stopProducing=true` + close +
  `ToolData` summary) → a ctrl-c branch that flips an `AtomicBool`, drains, and
  prints the `tool_data` event.
- Java `ThroughputThrottler.throttle()` uses `Object.wait()`; Rust uses
  `tokio::time::sleep` for the timed path and a `Notify` for the
  `targetThroughput == 0` block-until-wakeup path (CLAUDE.md §9.6: no thread
  primitives; "thread"→"task" in any log text, §9.7).
- Error handling: Java `throws`/exceptions → `Result`/`Error`; the word
  "exception" appears only in comments about Java (CLAUDE.md §2, §10). The JSON
  `exception` field name is Java's wire contract and is kept as a serialized
  field name, with a comment noting it is the Java event field, not Rust naming.

## 5. Module layout

```
tools/verifiable-clients/
  Cargo.toml                       # workspace member; deps: confluent-kafka-rust,
                                   #   tokio, serde, serde_json
  src/
    lib.rs                         # re-exports for the bins + unit tests
    throughput_throttler.rs        # Phase 1  (from server.util.ThroughputThrottler)
    verifiable_producer.rs         # Phase 1  (lib: VerifiableProducer + JSON events)
    verifiable_consumer.rs         # Phase 2  (lib: VerifiableConsumer + JSON events)
    bin/
      verifiable_producer.rs       # Phase 1  (thin main → lib)
      verifiable_consumer.rs       # Phase 2  (thin main → lib)

src/common/serialization/
  string_deserializer.rs           # Phase 2  (main crate, in-scope client class)
```

Run: `cargo run -p verifiable-clients --bin verifiable_producer -- --topic t --bootstrap-server localhost:9092`.

## 6. Phase 1 — VerifiableProducer + ThroughputThrottler (Actor/Critic 65)

**Translate**
- `ThroughputThrottler` — fields, `shouldThrottle`, `throttle`, `wakeup`. Timed
  sleep via `tokio::time`; `target==0` block via `Notify`. Keep the javadoc as
  rustdoc.
- `VerifiableProducer` struct + ctor, `getKey`/`getValue`, `send`, `run`, `close`.
- JSON event types: `StartupComplete`, `ShutdownComplete`, `SuccessfulSend`
  (`producer_send_success`), `FailedSend` (`producer_send_error`), `ToolData`
  (`tool_data`) — each with exact `name`, field names, and `@JsonPropertyOrder`
  (`timestamp`, `name`, …). `target_throughput`/`avg_throughput` renamed.
- `PrintInfoCallback` → a `Callback` impl that increments `numAcked` and prints
  `producer_send_success`/`producer_send_error`. **Callback obligation
  preserved** (CLAUDE.md §9.5): exactly one JSON line per completed send.
- `argParser()` / `createFromArgs` / `main` — every arg (`--topic`,
  `--bootstrap-server`, `--max-messages`, `--throughput`, `--acks` with
  choices {0,1,-1}, `--producer.config` (deprecated) / `--command-config`
  mutually-exclusive with it, `--message-create-time`, `--value-prefix`,
  `--repeating-keys`), same defaults, same required/mutually-exclusive rules,
  `--producer.config`+`--command-config` conflict error text preserved. Sets
  `acks`, `retries=0`, key/value serializer = StringSerializer.
- Config-file loading (`loadProps`) → parse a Java-properties file into the
  producer config map.

**Tests** (Java has **no** JUnit test for these classes — they are ducktape-only;
DoD §3 "not relevant / not present" applies, stated explicitly). Add Rust unit
tests for the logic that would otherwise be untested:
- `ThroughputThrottler::should_throttle` truth table (negative target = never;
  elapsed/rate boundary) and `sleep_time_ns` computation.
- `get_key` repeating-key wraparound (0..n then reset); `get_value` prefix vs
  no-prefix formatting.
- JSON serialization of each event asserts the **exact string** (field order +
  names + `name` value) against a known vector — this is the stdout contract
  (DoD §3 "byte-level, not just round-trip").
- Arg parsing: required-arg missing errors, `--acks` choice validation, the
  mutually-exclusive config-file conflict message.

**Verify:** `cargo build`, `cargo test -p verifiable-clients`, `cargo xtask
format-check`, `cargo xtask lint`. DoD §10 (hot-path alloc): N/A — a system-test
tool, not the client send path; state so in the self-review. Commit incrementally.

## 7. Phase 2 — VerifiableConsumer + StringDeserializer (Actor/Critic 65)

**Translate**
- `StringDeserializer` into the main crate (`Deserializer<String>` impl, UTF-8,
  beside `string_serializer.rs`), re-exported; replace the inline copy in
  `src/bin/consumer_test.rs` with it.
- `VerifiableConsumer` implementing `ConsumerRebalanceListener` +
  `OffsetCommitCallback`: `onRecordsReceived`, `onComplete`,
  `onPartitionsAssigned/Revoked`, `commitSync`, `run`, `close`,
  `hasMessageLimit`/`isFinished`.
- JSON events: `StartupComplete`, `ShutdownRequested`, `ShutdownComplete`,
  `PartitionsRevoked`/`Assigned` (with the `TopicPartition` custom serializer =
  `{topic, partition}`), `RecordsConsumed` (`records_consumed`, `count` +
  `partitions` summaries), `RecordData` (`record_data`, verbose only, exact
  `@JsonPropertyOrder`), `OffsetsCommitted` (`offsets_committed`, with
  `@JsonInclude(NON_NULL)` on `error` → `#[serde(skip_serializing_if =
  "Option::is_none")]`), plus `PartitionData`/`CommitData`/`RecordSetSummary`.
- `argParser`/`createFromArgs`/`main` — all args
  (`--bootstrap-server`, `--topic`, `--group-protocol`,
  `--group-remote-assignor`, `--group-id`, `--group-instance-id`,
  `--max-messages`, `--session-timeout`, `--verbose`, `--enable-autocommit`,
  `--reset-policy`, `--assignment-strategy`, `--consumer.config`,
  `--command-config`), same defaults.

**Group-protocol / classic-assignor handling (scope note).** The client is
KIP-848-only (`consumer-threading.md` §20). The tool keeps the classic-only
args so ducktape command lines still parse, but:
- `--assignment-strategy` default is the Java class name string
  `org.apache.kafka.clients.consumer.RangeAssignor` reproduced as a **literal
  constant** (no `RangeAssignor`/`RoundRobinAssignor` type is translated — they
  are used in Java only for `.class.getName()`).
- Selecting `group.protocol=classic` fails at `new_consumer` (the factory's
  existing `unsupported_version` error) — faithful to the client's stated
  support. Documented in the tool's `--help` text and a rustdoc note.

**Tests** (again, no Java JUnit tests — ducktape-only). Add Rust unit tests:
- `StringDeserializer` round-trip + a dedicated test file if the existing
  serialization tests warrant it (match the `string_serializer.rs` test style).
- Exact-string JSON vectors for every consumer event, incl. the `TopicPartition`
  `{topic,partition}` shape, `NON_NULL` error omission, and `RecordData` field
  order.
- `onRecordsReceived` offset math: `maxOffset+1` committed offset, the
  `maxMessages` truncation (`subList`) boundary, `isFinished` early-break.

**Verify:** same gates as Phase 1. `commit_sync` recursion-on-wakeup semantics
(Java 219-222) preserved as an await-loop. DoD §11 (consumer trait surface): the
tool only *consumes* the trait, adds none — state N/A with that reasoning.

## 8. Definition of Done (per phase)

All of `definition-of-done.md` applies, with these tool-specific notes recorded
in each self-review: DoD §3 — no Java JUnit tests exist for these classes
(ducktape system tests only), so "translate the tests" is satisfied by the added
Rust unit tests above; the **JSON stdout contract** is the byte-level fidelity
target. DoD §10 — N/A (not the send hot path). DoD §11 — N/A (consumes the
consumer trait, defines none). `cargo build`, `cargo test`, `cargo xtask
format-check`, `cargo xtask lint` green before a phase is done. Apache-2.0
header (Confluent Inc.) on every new file.

## 9. Manager loop

Per `agent-roles.md`: (1) plan approved → (2) spawn Actor 65 for Phase 1 → (3)
spawn Critic 65 → (4) summarize → (5) if comments, Actor 65 fixes, goto (3);
else Phase 1 done → repeat for Phase 2.
