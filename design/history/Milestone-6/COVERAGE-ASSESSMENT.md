# Multilanguage Coverage Assessment for Rust-Only Integration Tests

## Context

Milestone 6 (`design/history/MILESTONE-6/DESIGN-multilanguage-tests.md`, shipped on
branch `dev/milestone-6`) wired the producer integration tests in
`tests/integration/producer_test.rs` to run against three backends: native Rust,
Python (via gRPC → `bindings/python/producer.py`), and C (via gRPC → C FFI).
Eight scenarios × three backends = 24 tests, all green.

The other five files in `tests/integration/` (`api_versions_test.rs`,
`connection_test.rs`, `metadata_test.rs`, `producer_perf_test.rs`,
`ssl_sasl_test.rs`) still run rust-only. The question motivating this document
is: **do those tests use only public Java client API**, such that they could be
mechanically lifted to multilanguage like `producer_test.rs` was?

This is a decision document, not an implementation plan. The output is a
per-file liftability verdict plus the surface area each non-liftable file would
need before it could be lifted. Implementation of any selected subset is a
separate task.

## Method

For each rust-only file, classify every imported type by whether it has a
public-Java-client equivalent (`org.apache.kafka.clients.{producer,consumer,admin}.*`,
`org.apache.kafka.common.*`) or is a Rust crate internal that no Java
application would call directly.

The boundary is:

- **Public**: `KafkaProducer`, `KafkaConsumer`, `AdminClient` and their public
  methods; `ProducerRecord`, `ConsumerRecord`, `RecordMetadata`, `KafkaError`
  variants; config types (`ProducerConfig` ↔ `Properties`); `KafkaFuture`.
- **Internal**: `Selector`, `PlaintextChannelBuilder` /
  `SslChannelBuilder` / `SaslChannelBuilder`, `crate::common::requests::*`
  (`ApiVersionsRequestBuilder`, `MetadataRequestBuilder`, `RequestHeader`),
  manual `ConcreteResponse::parse_response`, `ByteBufferAccessor`,
  `MetadataResponse.data().*` field access. None of these are reachable from
  the Python wrapper or the C FFI today, and none have a counterpart in the
  Java public client (Java apps don't drive the wire protocol manually).

## Findings

| File | Tests | Liftable | Blocking surface |
|---|---:|---|---|
| `producer_perf_test.rs` | 2 | **No (semantic)** | API surface is liftable but gRPC tunneling latency dominates the burst-pattern timings the test measures, making the python/c numbers meaningless. Stays rust-only. |
| `api_versions_test.rs` | 4 | No | `Selector`, `ApiVersionsRequestBuilder`, manual `parse_response` |
| `connection_test.rs` | 3 | No | `Selector`, raw TCP poll, manual handshake driving |
| `metadata_test.rs` | 4 | No | `Selector` + `MetadataRequestBuilder`; would need an `AdminClient` translation |
| `ssl_sasl_test.rs` | 5 | No | `Ssl`/`SaslChannelBuilder` directly; would need security-config driven `KafkaProducer` path + new bindings |

### Per-file detail

**`producer_perf_test.rs:32-35` — API liftable but semantically
incompatible.** Imports `KafkaProducer`, `ProducerConfig`, `ProducerRecord`,
`RecordMetadata`, `ByteArraySerializer`, `KafkaFuture`. Both tests
(`perf_plaintext_burst_pattern:173`, `perf_sasl_ssl_burst_pattern:189`) are
`from_config` → `send` → `future.get_timeout` loops. Public-API only, so the
mechanical refactor would compile.

**Decision: stays rust-only.** What this file measures is per-call latency
under burst load. Tunneling each `send` over a unary gRPC RPC adds two extra
hops (test → gRPC server → Rust client → broker, then back), and the gRPC
serialization + context-switch cost is significant relative to a native
in-process call. The python/c numbers would not just be "slower" — they
would measure the harness, not the producer. Functional correctness under
burst is already covered by the existing
`test_produce_multiple_records_ordering` and `test_flush_sends_pending_records`
multilanguage tests; the perf scenarios add nothing for the bindings. Don't
lift.

**`api_versions_test.rs:23-31` — Not liftable.** Imports `Selector`,
`PlaintextChannelBuilder`, `NetworkSend`, `ApiKeys`, `ByteBufferAccessor`,
`ApiVersionsRequestBuilder`, `RequestBuilder`, `RequestHeader`. Each test
manually drives a TCP connection through `selector.connect()` /
`selector.poll()`, sends an `ApiVersions` request via `RequestBuilder`, parses
the response with `ConcreteResponse::parse_response`, and asserts on the
returned `ApiVersionsResponse` struct's fields. Java apps do none of this —
the Java client negotiates API versions internally. There is no public Java
API for `ApiVersionsRequest`. Not lifting.

**`connection_test.rs:27-35` — Not liftable.** Same shape as above:
`Selector` + `PlaintextChannelBuilder` + manual request/response handling.
Tests are about transport-layer behavior (TCP connect, request/response
correlation), which the Java client hides behind `KafkaProducer.send`.

**`metadata_test.rs:24-30` — Not liftable.** Same internals, plus inspection
of `MetadataResponse.data().brokers[i].{node_id, host, port}` and
`.controller_id`. Public-API equivalent would be Java `AdminClient`'s
`describeCluster()` and `describeTopics()`. **The Rust client doesn't have an
`AdminClient` translation yet, and neither binding exposes one.** This is the
file with the clearest path forward: add `AdminClient` + Python/C bindings,
then refactor.

**`ssl_sasl_test.rs:26-36` — Not liftable.** Imports `SslChannelBuilder`,
`SaslChannelBuilder`, `SslFactory`, `SecurityProtocol`, `SslConfig`,
`SaslConfig`. Tests directly orchestrate a TLS handshake or a SASL
authentication round trip via `Selector`, then assert on success/failure of
specific authentication paths. A *different* SSL/SASL test set could be
written that goes through `KafkaProducer` with `security.protocol=SSL` /
`SASL_SSL` and `sasl.mechanism=PLAIN` config and asserts that `send`
succeeds — that would be liftable — but it's a new set of tests, not a lift
of the existing ones. The existing tests' wrong-credentials and
unsupported-mechanism cases require seeing the *handshake* error directly,
which the producer hides behind `Send` / `Close` errors.

## Binding gaps that would have to close

To lift each non-liftable file:

- **`metadata_test.rs`**: needs `AdminClient` (Rust trait + impl) translated
  from `org.apache.kafka.clients.admin.AdminClient`; needs `kafka_admin_*`
  C FFI; needs `bindings/python/admin.py` wrapper. Then a parallel
  `AdminBackendFactory` + `multilanguage_admin_test!` macro mirroring the
  producer harness. New proto service.
- **`api_versions_test.rs` / `connection_test.rs`**: would require exposing
  `Selector` and the request/response builders through both bindings — but
  these are deliberately `pub(crate)` in `crate::common::network` and
  `crate::common::requests`. Doing so would violate the project's
  `internals` privacy rule (CLAUDE.md). **Not recommended** — these tests are
  Rust client internals tests and should stay Rust-only by design.
- **`ssl_sasl_test.rs`**: a producer-driven SSL/SASL subset is liftable
  today (already covered by `producer_test.rs`'s
  `bootstrap.servers` config path if `security.protocol` is added to
  `make_config`); the wrong-credentials and unsupported-mechanism cases need
  handshake-error inspection that the producer API doesn't expose.

## Recommendation

1. **Defer**: don't lift `api_versions_test.rs`, `connection_test.rs`,
   `ssl_sasl_test.rs`. These are Rust client internals tests by design;
   trying to lift them would either require violating the `internals`
   privacy rule or reimplementing them as different tests on a different
   surface — at which point they're new tests, not a lift.
2. **Don't lift `producer_perf_test.rs`.** Even though it would compile,
   gRPC overhead dominates the burst-pattern timings — the python/c
   variants would be measuring the harness, not the producer. Burst
   *functional* behavior is already covered by the multilanguage
   `test_produce_multiple_records_ordering` and
   `test_flush_sends_pending_records` tests.
3. **Track**: `metadata_test.rs` is the natural multilanguage candidate
   *after* an `AdminClient` translation and bindings exist. Note this as a
   follow-up dependency on whatever milestone introduces `AdminClient`.

## Java tests still on the table (no new Rust classes required)

Beyond lifting existing Rust tests, the Java Kafka 4.2 source tree under
`kafka/` has producer integration tests that haven't been translated yet
and that use **only** the already-translated surface (Producer trait,
ProducerRecord, RecordMetadata, ProducerConfig, KafkaError, KafkaFuture).
Adding any of these to `tests/integration/producer_test.rs` would
automatically run across all three backends via the existing
`multilanguage_test!` macro — no infrastructure work.

Of the 10 candidates, **8 fit the multi-language harness** and 2 are
rust-native-only because they depend on abstractions that don't cross
the gRPC bytes-on-the-wire boundary.

#### Multi-language (8 tests → 8 × 3 = 24 new test invocations)

| Source | Test | Behavior covered | Complexity |
|---|---|---|---|
| `ProducerFailureHandlingTest.java:91` | `testTooLargeRecordWithAckZero` | acks=0 + oversize → metadata.offset == -1, no error | Trivial |
| `ProducerFailureHandlingTest.java:110` | `testTooLargeRecordWithAckOne` | acks=1 + oversize → RecordTooLarge | Trivial |
| `ProducerFailureHandlingTest.java:145` | `testNonExistentTopic` | Send to non-existent topic (auto-create off) → Timeout | Trivial |
| `ProducerFailureHandlingTest.java:159` | `testWrongBrokerList` | Bad bootstrap → metadata-fetch timeout | Trivial |
| `ProducerFailureHandlingTest.java:177` | `testInvalidPartition` | Explicit partition ≥ partition count → Timeout | Trivial |
| `ProducerFailureHandlingTest.java:195` | `testSendAfterClosed` | send() after close() → IllegalState | Trivial |
| `PlaintextProducerSendTest.scala` | `testBatchSizeZero` | batch.size=0 still works | Trivial |
| `PlaintextProducerSendTest.scala` | `testNonBlockingProducer` | send_with_callback fires exactly once | Moderate |

Each fits the existing
`async fn name_inner<F: ProducerBackendFactory>(ctx: &mut TestContext, factory: &F)`
pattern; no new factory methods, no proto changes. Adding all 8 takes
the multi-language producer suite from 8 × 3 = 24 tests to 16 × 3 = 48.

#### Rust-native only (2 tests — plain `#[tokio::test]`, not the macro)

| Source | Test | Why it can't go multi-language |
|---|---|---|
| `BaseProducerSendTest.scala` | `testCloseWithZeroTimeoutFromCallerThread` | Needs pending-future semantics. `MultilanguageProducer.send` awaits the gRPC response and returns a pre-resolved `KafkaFuture::completed`, so there's nothing in-flight for `close_timeout(0)` to interrupt. |
| `PlaintextProducerSendTest.scala` | `testWrongSerializer` | Tests the `Serializer<K>` trait-object error path. The gRPC boundary is `Vec<u8>` keys/values by design; the server uses `ByteArraySerializer` always and never sees the user's serializer. The abstraction simply doesn't cross the wire. |

Both are still worth porting against the native Rust producer — they
add coverage for trait surfaces (`KafkaFuture` cancellation behavior;
`Box<dyn Serializer>` error handling) that nothing else exercises.
They just live as rust-only tests outside the macro.

### Java tests that look in-scope but are blocked

For completeness, these are *not* candidates and the blocker is
substantial:

- **`ProducerCompressionTest.java` (any test that asserts on payload
  contents)** — uses `KafkaConsumer` to read back records and assert
  bytes match. The current Rust `test_produce_with_compression` sidesteps
  this by asserting only that `send` returned a non-negative offset; a
  full payload round-trip needs Consumer translation.
- **`ProducerFailureHandlingTest.java` `testCannotSendToInternalTopic`,
  `testNotEnoughReplicasAfterBrokerShutdown`** — need `AdminClient`
  (topic creation, partition reassignment) and broker-lifecycle control
  (selectively shutting down a broker).
- **`ProducerIdExpirationTest.java`, all transactional tests** —
  blocked on transactional producer methods, which were explicitly
  out of scope for the Producer trait phase
  (`src/producer/producer_trait.rs:34-36`).
- **`KafkaProducerTest.java`, `RecordSendTest.java`** — heavy
  reliance on internal mocking (`MockClient`, `ProduceRequestResult`,
  internal state inspection). These are Rust *unit*-level concerns and
  largely already exist in `tests/producer/mock_producer_test.rs` and
  inline `#[cfg(test)]` modules.

### Recommendation update

These 10 Java tests are the highest-leverage follow-up work: they
extend behavioral coverage of the producer (acks-level paths, lifecycle
errors, callback semantics, serializer error mapping) without requiring
any new Rust classes or harness infrastructure. If a follow-up milestone
touches the producer harness, this is the work to do.

## Critical files referenced

- `tests/integration/producer_perf_test.rs:32-35` — public-API surface
- `tests/integration/api_versions_test.rs:23-31`,
  `tests/integration/connection_test.rs:27-35`,
  `tests/integration/metadata_test.rs:24-30`,
  `tests/integration/ssl_sasl_test.rs:26-36` — internal-API surface
- `bindings/python/producer.py:131-183` — current Python public API surface
- `src/ffi/producer.rs` (function list lines 371-1546) — current C FFI
  public API surface
- `tests/common/backend_factory.rs`, `tests/common/multilanguage_test_macro.rs`
  — the harness that any new lift would use unchanged

## Verification

Not applicable — this is a decision document. If recommendation 2 is later
acted on, verification is identical to milestone 6's:
`make test-multilanguage` runs the producer suite (now 8+2 scenarios × 3
backends).
