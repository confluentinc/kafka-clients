# M9/P2 — .NET consumer `Metrics()` + `ClientId()` (Python parity)

**Status:** APPROVED 2026-08-19 (all Manager recommendations confirmed). Agent number **N=37**.
**Branch:** `prashah_dev_dotnet_binding_consumer`.
**Mode:** **A** (feature already at the C ABI; .NET-only work; no Rust authoring).

## Goal

Implement the .NET **consumer** `Metrics()` + `ClientId()` to reach **Python parity**. A
three-way API-parity audit (Java `Consumer<K,V>` → Python `bindings/python/consumer.py` →
.NET consumer on this branch) found these are the **only two** consumer APIs the Python
sibling implements that the .NET consumer is missing. Everything else is at parity or
missing from both siblings.

## Mode A verification (recorded at approval time)

The C ABI already backs both, committed in `7a5425a2` ("Wire Consumer::metrics() through
every binding backend"):

- `src/ffi/consumer.rs:3995` `kafka_consumer_Consumer_metrics` → `*mut kafka_consumer_MetricMap_t`, null on concurrent-access rejection.
- `src/ffi/consumer.rs:2346-2528` the full `MetricMap` accessor family: `_count`,
  `_get_name`/`_group`/`_description`/`_tag_count`/`_tag_key`/`_tag_value`/`_get_value_kind`/`_get_value_double`/`_string`/`_long`/`_int`, `_destroy`.
- Value-kind constants: `0`=double, `1`=string, `2`=long, `3`=int.
- `src/ffi/consumer.rs:4073` `kafka_consumer_Consumer_client_id` → owned `*mut c_char`
  (free via `kafka_consumer_string_destroy`, `src/ffi/consumer.rs:2768`), null on
  concurrent-access rejection.
- Mock is backed: `src/consumer/mock_consumer.rs:413` `client_id()`, `:427` `metrics()`;
  both required by the `Consumer` trait (`src/consumer/mod.rs:171,196`).
- Python consumes them via the same symbols (`bindings/python/consumer.py:292,304`),
  proving the cbindgen export is complete.

**Caveat:** the generated header on disk (`target/include/confluent_kafka.h`, gitignored
build artifact) is **stale** — it predates the metrics commit and lacks the metrics
symbols. The Actor's **step 0 is a Rust *build* only** — `cargo build --features ffi` —
which regenerates the header and exposes the symbols. **The Actor authors NO Rust.** Do
NOT commit the regenerated header.

## Java-shape facts (recorded)

- `metrics()` **is** on Java's public `Consumer` interface (`Consumer.java:187`,
  `Map<MetricName, ? extends Metric> metrics()`) → straight Java-shape target.
- `clientId()` is **package-private** on `KafkaConsumer` (`KafkaConsumer.java:1875`), NOT
  on the `Consumer` interface → adding `ClientId` is a **deliberate Python-parity addition
  beyond the Java shape** (record as a deviation).
- Java `MetricName.equals`/`hashCode` (`MetricName.java`) use **(group, name, tags)**,
  **excluding description**.
- Java `Metric` interface = `MetricName metricName()` + `Object metricValue()`.
- Out of scope: `clientInstanceId(Duration)` (KIP-714 `Uuid`, `Consumer.java:182`; no ABI,
  Python-absent) and `Register`/`UnregisterMetricForSubscription` (KIP-714; no ABI).

## Approved design decisions

- **D1 = Java-faithful shape.** `IReadOnlyDictionary<MetricName, IMetric> Metrics()` with
  public `MetricName` + `IMetric` types. (`MetricName` identity is a proper dict key; the
  core returns a Rust `HashMap`, so keys are unique — build via indexer, not `Add`.)
- **D2 = `IMetric` members named `Name` / `Value`.** `MetricName Name { get; }` +
  `object Value { get; }` (cheap cached reads on an already-marshalled object → properties;
  `Name` avoids the property/type-name collision). No public `Kind` — the boxed CLR type
  (double/string/long/int) conveys the kind.
- **D3 = `Metrics()` is a method** — matches the shipped `Assignment()`/`Subscription()`/
  `Paused()`/`GroupMetadata()` FDG precedent (P/Invoke + marshalling + fresh snapshot + can
  throw) and the Java/Python method shape.
- **D4 = `ClientId()` is a method** — `string ClientId()`, same FDG precedent (fresh
  P/Invoke that allocates+copies a string and can throw; a property would require a
  managed cache, against the "shape not logic" charter). Record the beyond-Java deviation.
- **D5 = `ClientId()` throws `InvalidOperationException` on concurrent-use null** —
  non-nullable `string` return; deliberately **stricter than Python's unguarded
  `client_id()`** (client id is always known, so null means only concurrent access). Record
  as a deviation. `Metrics()` null → `InvalidOperationException` too (matches `Assignment()`
  et al., ffi §B5).
- **D6 = both on `IConsumerCommon`** (non-blocking, sync, shared by both families) — all six
  consumer types inherit them.
- **D7 = mock support** confirmed backed; both mock classes inherit.

## Deliverables (diff scope = `bindings/dotnet/**` only)

New public members on `src/Confluent.Kafka/IConsumerCommon.cs`:
```csharp
IReadOnlyDictionary<MetricName, IMetric> Metrics();   // Java Map<MetricName, ? extends Metric> metrics()
string ClientId();                                    // Python client_id() — beyond-Java (deviation)
```

New public types (`src/Confluent.Kafka/`):
- `MetricName.cs` — `public sealed class MetricName` with `Name`, `Group`, `Description`
  (string) + `Tags` (`IReadOnlyDictionary<string,string>`). **Value equality over
  (Name, Group, Tags), excluding Description, tag-order-independent, hand-implemented**
  (no `IReadOnlyDictionary.Equals`), with a matching `GetHashCode` (Java caches its hash).
- `IMetric.cs` — `public interface IMetric { MetricName Name { get; } object Value { get; } }`
  plus a small **internal** impl carrying the snapshot value.

Implementations:
- `Internal/NativeConsumer.cs` — `Metrics()` (P/Invoke `Consumer_metrics` → iterate the
  `MetricMap` → build the dictionary → `MetricMap_destroy` exactly once; null →
  `InvalidOperationException`) and `ClientId()` (P/Invoke `Consumer_client_id` →
  copy-out the UTF-8 string → `string_destroy`; null → `InvalidOperationException`).
- The four concrete consumer classes surface both members forwarding to `NativeConsumer`,
  mirroring the existing `GroupMetadata()`/`Assignment()`/`Subscription()`/`Paused()`
  surfacing.
- `Internal/Interop/NativeMethods.cs` — new `[DllImport]` decls for `Consumer_metrics`,
  the full `MetricMap_*` family, `MetricMap_destroy`, `Consumer_client_id` (reuse existing
  `string_destroy`); managed mirror of the value-kind constants (0-3).
- Doc-sync: dotnet `CLAUDE.md` §3 sketch + §4 "stays sync" set — mark `Metrics`/`ClientId`
  shipped; record the `ClientId` beyond-Java + throw-on-concurrent deviations.
- `STATUS.md` update.

## Tests / DoD (`bindings/dotnet/definition-of-done.md`)

- New `PublicConsumerMetricsTests` + `PublicConsumerClientIdTests` (or extend existing
  public-consumer test files), mirroring Python's coverage.
- Cover **both** the real consumer (via the mock, no broker) and the mock, across **sync
  and async** families.
- **Assert error-message content** for the concurrent-use `InvalidOperationException`
  ("not safe for multi-threaded access").
- `MetricName` equality/hashcode tests: tag-order-independence and Description-excluded.
- TFM-matrix smoke (net462-via-ns2.0, net8.0, net10.0); non-ASCII round-trip for `ClientId`
  and a `MetricName` string field.
- **Per-record allocation audit: N/A** (not a hot path) — state explicitly in self-review.

## Correctness watch-items (Critic)

- `MetricName` structural value-equality over (Name+Group+Tags), Description excluded,
  tag-order-independent, hand-implemented.
- `MetricMap` handle freed exactly once via `MetricMap_destroy` after copy-out; borrowed
  tag key/value + `client_id` char* copied out **before** their `_destroy`/`string_destroy`.
- Concurrent-use → `InvalidOperationException` with the exact message.
- **Mode A hard line:** diff touches no `src/**`/`src/ffi/**`/`cbindgen.toml`/Rust; the
  regenerated header is NOT committed.

## Verification

`cargo build --features ffi` (header regen) → `make test-dotnet` (build + format + unit on
net8.0 AND net10.0) green.

## Execution conventions

- Agent number **N=37** (next-unused; N=36 = M13/P2 was last). Personas `dotnet-actor` /
  `dotnet-critic`. Loop: Actor → Critic (`COMMENTS.37.md`) → summarize → fix-cycle → clean.
- Per-path `git add` only; NEVER stage `.claude/agents/dotnet-*.md`, `COMMENTS.*`,
  `agent-memory/**`, `target/**`, built binaries, `obj/`/`bin/`; commits `--no-gpg-sign` +
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. Commit locally; **do NOT push**
  (user manages pushes).
