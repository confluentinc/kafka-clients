# JavaScript producer: code complete under the JavaScript Binding Conventions (transactions excluded)

Status: **approved 2026-11-12** (E1–E3 below); implementation on `dev/javascript-producer-completion`,
stacked on PR #312. §6 is the hand-off: what is done and what is still missing. Rewritten 2026-11-12 against the root `CLAUDE.md` "JavaScript Binding
Conventions" (PR #327, `kraina-js-claude-rules`), which PR #312 is now based on, and against PR #312's head
`6d47eed89`. Supersedes the 2026-11-09/11 versions of this file. `PLAN-interface-alignment.md` (already carried
out in `c956a779a`) aligned what the producer had; this plan adds what it lacks.

## 1. Goal and scope

The producer is **code complete** when every public member of Java's `Producer`, `KafkaProducer`,
`ProducerRecord`, `RecordMetadata`, `Callback` and `MockProducer` (Apache Kafka 4.4.0 in `kafka/`), and every
public type they reach, is either translated under the conventions or left out for a reason the conventions
give, and that reason is listed in the module docs ("When an entry point is missing, its overload is left out
and listed in the docs", Implementation over the FFI).

**Excluded by request:** transactions — `initTransactions`, `beginTransaction`, `sendOffsetsToTransaction`,
`commitTransaction`, `abortTransaction`, `PreparedTxnState`, `ConsumerGroupMetadata`, `TopicPartitionMap` for
`sendOffsetsToTransaction`, MockProducer's transaction members, and the 3 transaction tests in `JS_GRPC_SKIPS`.

**Rules that govern every phase** (the conventions; not repeated per phase):
- Java decides the content, JavaScript the form; any change is *(deviation)* with Why and Goes when, and any
  addition Java lacks is *(js-only)* and goes on the list of invented names (General rules, Scope, Layout).
- TypeScript in `src/<java package>/`, hand-written helpers in `src/internal/`, the addon in `native/`; one file
  per Java class, one import path per package; generated wherever the generators can (Layout and modules).
- A method returns a Promise if and only if its FFI entry point has a `_cb` twin; the Promise-rule check must
  pass (Futures and waiting).
- Overloads: one method, options object with Java's parameter names, bare value for one-parameter forms, form
  errors in Python's format; `undefined` is not a value; `null` only where Java accepts it (Signatures).
- Data classes have readonly fields; classes that change, or whose reads can fail, keep methods (Class family).
- Closed client: `IllegalStateError` with Java's message, checked after the argument form and before the rest;
  Python's deviations for `flush`, `partitionsFor` and `metrics` carry over (Implementation over the FFI).
- Errors: one generated class per FFI id, `name` = class, no `code`, Java's messages word for word (Errors).
- Tests: Java's tests of each translated class, translated to `node:test` with Java's messages; plus the binding
  tests of call shapes, FFI twins, type tests and no unhandled rejection (Tests).
- No Rust or C changes. The binding adds no FFI entry points.

## 2. Where the producer stands (PR #312 at `6d47eed89`)

| Area | Status |
|---|---|
| `KafkaProducer`: constructor (bare config map or `{ configs }`), `send(record)` / `send({ record, callback })`, `flush()`, `close()` / `close(timeout)` / `close({ timeout })`, `[Symbol.asyncDispose]` | done, aligned with the conventions |
| `ProducerRecord`, `RecordMetadata` (readonly fields), `Callback` (`({ metadata, exception })`), `Headers` / `Header` classes, `KafkaFuture` with all of Java's members, `Duration`, `CloseArgs` | done |
| 175 generated error classes; `ProducerConfig`, `SslConfigs`, `SaslConfigs`, `SecurityConfig`; `SslClientAuth`, `SecurityProtocol` | done |
| Copy of key/value bytes (Q21), `encodeLikeJava` (Q24), the serde config keys refused (Q53) | done |
| gRPC arm (`grpc-server/`, producer RPCs except PartitionsFor, Metrics and transactions), verifiable producer, soak client, chaos workload, npm packaging, NOTICE, MIGRATION.md, API report | done |
| Left out today (`src/producer/index.ts`): constructors with serializers, `partitionsFor`, `metrics`, `clientInstanceId`, `register/unregisterMetricForSubscription`, `NETWORK_THREAD_PREFIX`, `PRODUCER_METRIC_GROUP_NAME`, `RecordMetadata`'s public constructor; `MockProducer` | **this plan** |
| `JS_GRPC_SKIPS`: 2 metrics tests, 4 partitionsFor tests (plus 3 transaction tests, excluded) | **this plan** |

## 3. Phases

Each phase is one Actor/Critic loop (`agent-roles.md`). An item marked **[blocked: Qn]** sits under the
conventions' "Not decided yet": the Actor stops there and asks, and builds the rest of the phase.

### P1. Constants and value types
- `KafkaProducer.NETWORK_THREAD_PREFIX` and `PRODUCER_METRIC_GROUP_NAME`, static members on the class (Names).
- `common/topic_partition.ts`: `TopicPartition`, a data class (`tp.topic`, `tp.partition`), frozen when created
  (Q25), with `toString()` and `toJSON()` (*js-only*, frozen once released). `equals`/`hashCode`: **[blocked: Q29]**.
- `RecordMetadata`'s public constructor, one options object with Java's parameter names
  (`{ topicPartition, baseOffset, batchIndex, timestamp, serializedKeySize, serializedValueSize }`), replacing
  today's private constructor; the binding keeps building it through an `@internal` factory. Its `long`
  parameters: **[blocked: Q23]** (today's fields are `number`; the row decides whether they stay).
- `common/node.ts`: `Node`, a data class (on the conventions' list): `node.id`, `node.host`, `node.port`,
  `node.rack`, `node.isFenced`, and Java's other no-argument methods as fields (`idString`, `hasRack`,
  `isEmpty`); `Node.noNode()` static; Java's three constructors as one options-object constructor.
- `common/partition_info.ts`: `PartitionInfo` keeps **methods** (`info.topic()`, `info.leader()`, …), as in
  Java, Rust and Python (E1); it is not on the conventions' data-class list. `replicas()`,
  `inSyncReplicas()`, `offlineReplicas()` return Java's `Node[]`, which has no row in the Types table:
  **[blocked: Q25]** (lean: frozen arrays).
- Primitive `int`/`boolean` fields use `number`/`boolean`, the Types table's proposal that the existing
  producer already follows; the range check (`RangeError`) waits on Q23.
- `Uuid` is not needed by any non-transactional producer member that the FFI supports (`clientInstanceId` is
  left out, §4), so it waits for the consumer or admin.
- Tests: Java's `TopicPartitionTest`, `NodeTest`, `PartitionInfoTest` (non-deprecated cases), `RecordMetadataTest`.

### P2. `partitionsFor`
- `KafkaProducer.partitionsFor(topic)`: the FFI has `kafka_producer_Producer_partitions_for_cb`, so it returns
  a Promise. Native: walk the `kafka_List_t` of `PartitionInfo` into JS `PartitionInfo` / `Node` values.
- Return type `List<PartitionInfo>`: **[blocked: Q25, other collections]** (lean: a frozen array, as the
  constant `List`s are).
- Closed client: Python's `partitions_for` deviation carries over (Implementation over the FFI).
- `grpc-server/`: the `PartitionsFor` RPC; drop its 4 tests from `JS_GRPC_SKIPS`.
- Tests: Java's `KafkaProducerTest` partitionsFor cases (closed producer, metadata timeout message word for
  word), plus the broker tests for a fresh and an expanded topic.

### P3. `metrics`
- `KafkaProducer.metrics()`: the FFI has only `kafka_producer_Producer_metrics_err`, no `_cb`, so it is
  **synchronous** (Futures and waiting). Native: iterate `MetricsView` into a snapshot *(deviation, as in
  Python: Java returns a live view)*.
- `common/metric_name.ts` (`name()`, `group()`, `description()`, `tags()`) and `common/metric.ts`
  (`metricName()`, `metricValue(): unknown`); `MetricConfig` stays a placeholder `unknown` (Scope).
- Return type `Map<MetricName, Metric>` and `tags(): Map<String, String>`: **[blocked: Q25]**. `long` metric
  values (`MetricValue_as_long`): **[blocked: Q23]**.
- `grpc-server/`: the `Metrics` RPC; drop its 2 tests from `JS_GRPC_SKIPS`.
- Tests: `MetricNameTest`; the producer metrics cases; the 2 multilanguage tests.

### P4. Serializers and typed producers
Decided by the conventions (Serialization, Class family, Signatures):
- A serde is an object `{ serialize({ topic, headers, data }), close?() }` *(deviation: one method for Java's
  two)*; `serialize` may return a Promise, which the binding awaits *(js-only)*; it is called for `null` too.
- `KafkaProducer` constructor forms: the bare config map, `{ configs }`, and
  `{ configs, keySerializer, valueSerializer }`; the type arguments are inferred from the serdes, and an omitted
  serde binds `KafkaProducer<Uint8Array, Uint8Array>`. `ProducerRecord<K, V>` is covariant through `out`.
- Serializers run in JS inside `send()`, NULL serdes go to the core, and a serializer's output is copied like
  any app bytes (Q21). `close()` of each serde runs at client close; a failure goes to `process.emitWarning`.
- Built-in factories (*js-only*) match Java's serializers byte for byte, tested with Java's vectors; passing a
  factory without `()` throws Python's hint "did you forget `()`?".
- Docs: un-awaited sends with an async serializer may reach the core out of call order; `await send()` keeps it.

Blocked items:
- The list of factories: **[blocked: "The list of serde factories"]** (lean: one per Java serializer —
  string, bytes, short, integer, float, double, boolean, uuid, void — with `long` and `list` waiting on Q23/Q25).
- What a string key or value gets without a serializer: **[blocked: Q24/Q27]** (proposal: `TypeError` pointing
  to `stringSerializer()`; today strings are accepted and encoded with `encodeLikeJava`).
- Emanuele's "the methods must be all async": **[blocked: Q50]**.
- Tests: Java's `SerializationTest` (round trips and vectors), the hint, `null` handling, async serde ordering,
  serde `close()` failures as warnings, type tests for the inferred generics.

### P5. `MockProducer`
Decided (Implementation over the FFI, Scope): a TypeScript translation of Java's `MockProducer`, not the FFI's
mock *(temporary, as in Python)*; no preview warning; `history()` returns the user's own records; an injected
error is thrown again as the same instance; a member Java's mock doesn't support throws
`UnsupportedOperationError` with Java's message.
- Members: `send` ×2, `flush`, `partitionsFor`, `metrics`, `close` ×2, `[Symbol.asyncDispose]`, `history()`,
  `clear()`, `completeNext()`, `errorNext(e)`, `closed()`, `flushed()`, `setMockMetrics()`, and Java's public
  fields `sendException`, `flushException`, `partitionsForException`, `closeException` as public fields.
- Constructors: Java's three forms; `Cluster` and `Partitioner` are placeholders `unknown` (Scope).
  Which forms Q30 leaves out: **[blocked: "The list of forms that Q30 leaves out"]**.
- Shares P2/P3/P4's blocks for `partitionsFor`, `metrics` and serdes.
- Left out, listed in the docs, as in Python: `disableTelemetry`, `injectTimeoutException`,
  `setClientInstanceId`, `addedMetrics`.
- `grpc-server/`: an empty config creates a `MockProducer`, as Python's server does.
- Tests: Java's `MockProducerTest`, non-transactional cases.

### P6. Error payloads
Decided (Errors): an error constructor takes `{ message, cause, ...Java's other parameters }`, and payload
getters are methods over private fields, as in `new TopicAuthorizationError({ message, unauthorizedTopics })`.
- `native/error.c` reads the payload accessors the FFI already has (`kafka_common_Error_topic_authorization`,
  `RecordTooLargeError_record_too_large_partitions`, …); the TypeScript emitter of `generate-error-codes` writes
  the getters and constructor keys; `check-generated` covers them.
- Payloads typed `Map<TopicPartition, Long>`: **[blocked: Q23, Q25]**.
- The error class for a second constructor argument: **[blocked: Q39]** (proposal `TypeError`).
- Remove the CHANGELOG's "no payload getters" limitation.

### P7. Docs, CI and packaging the conventions require
- Docs (Documentation): TSDoc on every new member within 150% of the Javadoc, `@remarks` for deviations, the
  module docs list every overload left out (§4); MIGRATION.md gains serdes, `MockProducer`, `partitionsFor`
  and `metrics`; examples for serdes, the mock and a SASL_SSL config.
- `make verify` gains the JavaScript build, lint, type check and unit tests (Tests; this replaces the earlier
  standalone-only decision).
- `grpc-server/` serves the four services over the public API (Tests): the producer service complete; the
  consumer, admin and chaos services follow their clients.
- The performance harness moves to `javascript/bench/` (Layout); it is local-only today and joins the PR then.
- The API report (`api/`) and the Promise-rule check updated for every new member; `engines.node` `>=22.14.0`.

## 4. Left out, listed in the docs

| Item | Reason under the conventions |
|---|---|
| Transactions | Excluded by request |
| `clientInstanceId`, `registerMetricForSubscription`, `unregisterMetricFromSubscription` | No FFI entry point (the core has no KIP-714): "its overload is left out and listed in the docs" |
| A user `Partitioner`, `RoundRobinPartitioner`, `ProducerInterceptor` | Not translated (Scope, as in Python); `partitioner.type` selects a built-in partitioner |
| The `KafkaProducer(Properties, …)` forms as distinct overloads | One `configs` form (Class family) |
| `ProducerConfig.configDef()` / `configNames()` | The `ConfigDef` machinery isn't translated (Scope) |

## 5. Decisions this plan needs

From the conventions' "Not decided yet" (owner as listed there):

| Item | Blocks | Lean |
|---|---|---|
| `long` typing (Q23) | P1 `RecordMetadata` constructor, P3 long metric values, P4 `longSerializer`, P6 payloads | — (team) |
| Other collections (Q25) | P2 `partitionsFor` return, P3 `metrics()` and `tags()`, P4 `listSerializer`, P6 payloads | frozen arrays for `List`, `Map` for `Map` |
| `equals` / `hashCode` / `Comparable` table (Q29) | P1 `TopicPartition`, `Node`, `PartitionInfo` | — |
| The list of serde factories | P4 | one per Java serializer |
| String key/value without a serializer (Q24/Q27) | P4 | `TypeError` pointing to `stringSerializer()` |
| "The methods must be all async" for serdes (Q50) | P4 | waiting on Emanuele |
| The forms Q30 leaves out | P5 constructors | — |
| Error class for a second constructor argument (Q39) | P6 | `TypeError` |

For this plan:

| # | Decision | Outcome (2026-11-12) |
|---|---|---|
| E1 | `PartitionInfo`: fields or methods | **Methods**, as Java, Rust (`PartitionInfo::topic()`) and Python (`info.topic()`) all have; not added to the data-class list |
| E2 | Phase order | **Agreed**: P1 → P2 → P3 → P4 → P5 → P6 → P7; work proceeds on whatever is unblocked |
| E3 | Blocked items | **Agreed**: build each phase's decided parts, leave the blocked member out with a "not decided yet" note in the module docs (no TODO/FIXME), and add it when its row is decided. Most rows are expected to be decided before this PR merges |

## 6. Hand-off: done and missing

Agents picking this up: read §3 for the spec of each item, the root `CLAUDE.md` "JavaScript Binding
Conventions" for the rules, and update this table in the same commit as the work. An item marked
**blocked** waits on the named "Not decided yet" row: do not invent a design for it; check the root
`CLAUDE.md` first, because the row may have been decided since.

| Phase | Item | State |
|---|---|---|
| P1 | `NETWORK_THREAD_PREFIX`, `PRODUCER_METRIC_GROUP_NAME` | done |
| P1 | `TopicPartition` (frozen data class, `toString`, `toJSON`) | done |
| P1 | `TopicPartition` / `Node` / `PartitionInfo` `equals` / `hashCode` | **blocked: Q29** |
| P1 | `Node` (data class) | done (Java's protected `idString` constructor left out: subclasses only) |
| P1 | `PartitionInfo` (methods) except the `Node[]` getters | done: `topic()`, `partition()`, `leader()`, `toString()`; built through `internalCreate` |
| P1 | `PartitionInfo.replicas()` / `inSyncReplicas()` / `offlineReplicas()`, and its public constructors (they take `Node[]`) | **blocked: Q25** |
| P1 | `RecordMetadata` public constructor | **blocked: Q23** (`long` parameters) |
| P2 | `partitionsFor` (Promise via `partitions_for_cb`), gRPC `PartitionsFor`, 4 `JS_GRPC_SKIPS` | **blocked: Q25** (`List<PartitionInfo>`); native walk of `kafka_List_t` can be written first |
| P3 | `metrics()` (sync, `metrics_err`), `MetricName`, `Metric`, gRPC `Metrics`, 2 `JS_GRPC_SKIPS` | **blocked: Q25** (`Map`), Q23 (long values) |
| P4 | Serde shape, `keySerializer`/`valueSerializer` constructor keys, inferred generics, serializing in `send()`, async serdes awaited, serde `close()` at client close, factory-without-`()` hint | missing |
| P4 | Built-in factories (`stringSerializer()`, …) | **blocked: "The list of serde factories"** |
| P4 | A string key/value without a serializer | **blocked: Q24/Q27** (today: accepted, `encodeLikeJava`) |
| P4 | Whether serde methods must all be async | **blocked: Q50** |
| P5 | `MockProducer` (TS translation): send/flush/close/history/clear/completeNext/errorNext/closed/flushed, public exception fields | missing |
| P5 | `MockProducer` constructor forms, `partitionsFor`, `metrics`, `setMockMetrics` | **blocked: Q30 list, Q25, Q23** |
| P6 | Error constructors `{ message, cause, … }` | done (in PR #312) |
| P6 | Payload getters and payload constructors (`unauthorizedTopics()`, `recordTooLargePartitions()`, …) | **blocked: Q25** for collection payloads, Q23 for `Long`; a scalar payload may go ahead |
| P6 | Second constructor argument error class | **blocked: Q39** |
| P7 | TSDoc, MIGRATION.md sections, examples, `make verify` JS targets, API report, `bench/` move | missing (`bench/` stays local until the perf work is pushed) |

