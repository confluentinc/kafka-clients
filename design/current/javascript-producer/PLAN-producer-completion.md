# JavaScript producer: aligned with the binding rules, then code complete (transactions excluded)

Status: **Phase A done on PR #312 (`44ee5ab04`), Phase B done on PR #333** (2026-11-15), but for what §3's
hand-off lists.

Checked against PR #327's head **`05abb9564`** (2026-11-13 15:04 IST). Four read-only reviews of PR #312 + PR #333
fed it: the runtime and the addon, the public surface, packaging/docs/tests/CI, and the rule linter. Before each
phase, fetch `origin/kraina-js-claude-rules` and diff its `CLAUDE.md` against the branch's copy: #327 is still
open and still changing.

## 1. Scope, branches and terms

- **Phase A** (PR #312): align the producer #312 already has with the rules.
- **Phase B** (PR #333, stacked on #312): complete the producer.
- **Phase C**: the generators the rules require (Q4, Q13, Q74). §4 has the timing decision.
- **Not built:** transactions (by request); the rules' "Future improvements" (worker cleanup hook, borrowed-Buffer
  debug check, `[util.inspect.custom]`, refusing a call after a callback's first `await`, Q49); any KafkaJS-style API
  (decided after GA).
- **Branches:** both PRs are rebased locally onto `93daa9d9d`. They need rebasing onto `05abb9564` (only `CLAUDE.md`
  differs) and the force-push approved. PR #312 also has `ccab4b613` (the "callers await" rule) on its old base.
- **Terms, as the rules use them:**
  - **deviation:** differs from a decided rule, so it must change.
  - **stop and ask:** a "Not decided yet" row.
  - **no row:** a type or FFI shape missing from its table, which stops that item until the row exists.
  - **left out:** no FFI entry point, so the member is left out and listed in the docs. That doesn't stop the work.
  - **blocker:** needs a C FFI or Rust core change (§7).

## 2. Phase A: align PR #312 (code that #312 already has)

| # | Change | Rule | Size |
|---|---|---|---|
| A1 | **Zero copy.** Drop `js_bytes_copy`. Borrow the app's key/value (a serializer's output included) with a `napi_ref`, released in the hand-over completion, not at delivery. Put the mandated docs text on `send()` and in the README, the `SharedArrayBuffer` sentence included. Replace the "changed or detached as soon as `send()` returns" tests. | Q21 | M |
| A2 | **`long` is `bigint`.** `RecordMetadata.offset`/`.timestamp` and the addon's int64 values become `bigint`. A `long` argument takes a whole `number` up to 2^53 (`RangeError` above that). `int` range checks on partition, id and port. `toJSON()` writes a `bigint` as a decimal string. | Q23 | M |
| A3 | **Closed state and lifetime.** Remove the closed, closing and destroyed flags in TypeScript and in C. A call after `close()` goes to the FFI. `close()` keeps the native handle; the finalizer calls `_destroy` only for a closed client. No force-close at teardown: remove `producer_force_close`, its `header_conformance` exception and its docs. The tsfn stays referenced from creation and is unreferenced after the close. The serializer runs before the core's closed error. | Q21, Q61 | L |
| A4 | **No repeated preconditions.** Negative timeouts go to the FFI as they are; choose `close_cb` by the form called, not by -1. Freeze records instead of re-checking them at `send()`. | Q35, Q22, Q9 | S |
| A5 | **Configuration.** Remove the ConfigDef table, `checkType`, the null-validator errors and the `kafkaJS` error. Write every value as a string (`bigint` and arrays too; an object becomes `[object Object]`). The `ProducerConfigs` type lists the known keys plus `(string & {})`. The `config_types` emitter writes that key list. | Q52, Q54, Q55 | M |
| A6 | **String key or value without a serializer** throws `TypeError` pointing to `stringSerializer()`. The wording is stop-and-ask (§5). Examples and the README stop sending strings without one. | Q24, Q27 | S |
| A7 | **Send futures over the native handle.** Keep the hand-over's `kafka_common_KafkaFuture_t`; `get()` calls `get_cb` and `get({timeout})` calls `get_with_timeout_cb`, each created on first use. A JS timer runs only for JS-built futures such as `allOf()`'s. | Q44, Q22 | M |
| A8 | **`Headers` and `Header` wrap the FFI.** Use `RecordHeaders` and `RecordHeader` handles (all the entry points exist), and compute `equals`/`hashCode` from the accessors. The two parked rows stay as today (§5). | Q31 | L |
| A9 | **Interfaces and classes.** Add a `Producer` TypeScript interface plus its runtime object, implemented by `KafkaProducer`. `KafkaFuture` becomes an `abstract class`. Add `INSTANCE` on `DisconnectError` and `CoordinatorNotAvailableError`. Translate the key-name classes (`SslConfigs`, `SaslConfigs`, `SecurityConfig`) in full, minus the `ConfigDef` methods. | Q10, Q16, Q55 | M |
| A10 | **Errors.** A second constructor argument throws the form-check message, as in `KafkaError() takes one of (), (message), (cause), ({ message, cause }); got 2 arguments`. | Q39 | S |
| A11 | **Native target.** `NAPI_VERSION=9`. The missing-platform error names a lockfile made on another OS. | Q73 | S |
| A12 | **Docs.** README, MIGRATION and CHANGELOG get: the workers sentence; the decided default partitioner; `kafkaJS` and 1.x keys reaching the core as unknown keys; serde config keys accepted; an idle unclosed producer keeping Node running; the 1.x MIT → Apache-2.0 licence change; the `SuppressedError` note; `bigint`; `TopicPartitionSet`; leaving a Buffer alone until `send()` settles. Also `@remarks` marks for deviations ("a missing serde means bytes"), private-constructor doc comments, and module docs that list left-out members as `@packageDocumentation`. | Q64, Q65, Q16 | M |
| A13 | **Tooling.** `module: nodenext` plus a nodenext typing project. `4.4.0-beta.1` and npm's `beta` tag, in `package.json`, the platform packages, `bump-version` and the release scripts, while the docs keep saying "preview". TypeDoc and an `eslint-plugin-jsdoc` docs lint. Prettier. publint. A tarball header check. The invented-names list gains `[Symbol.asyncDispose]` and `[Symbol.iterator]`. | Q13, Q64, Q74, Q75, Q76, Q6 | M |
| A14 | **Tests on Jest**, `test/lint` included (§5, 5), and `tsc --strict` over the tests (through `checkJs`, the tests staying JavaScript: ts-jest cannot run under jest-light-runner). **Done.** | Q66, Q67 | L |
| A15 | **`javascript/CLAUDE.md` rewritten** to match the root rules. Boundary rules 1, 4, 5, 8 and 10 and the liveness section are reversed. Rows marked "not decided" are now decided. `preview` becomes `beta`, and `node:test` becomes Jest. The readings of §5 are recorded, each with the rule it follows. | all | M |
| A16 | **Enum `toString()` and null binding.** `SslClientAuth.toString()` returns Java's lowercase text (§5, 6). Extra signatures for the null combinations, so a `null` key or value binds `never` (§5, 7). | Q12, Q26 | S |

### Phase A status (PR #312 at `44ee5ab04`)

- **Done:** A1, A2, A3, A4, A5, A7, A9, A10, A11, A12, A13 (but the parts below), A15.
- **A16:** `SslClientAuth.toString()` done; the null binding (§5, 7) moved to B4.
- **A6** moved to B4.
- **A8: done** (on PR #333), with the owner's option 1 of 2026-11-15 (`DECISIONS.md`, entry 21): `Headers` and
  `Header` over the FFI's `RecordHeaders` and `RecordHeader` (`native/headers.c`), a copy handed out while the
  headers can still change and a view once they are read-only. Not built: a consumed record's `Headers` (over
  `kafka_consumer_ConsumerRecord_headers`), which comes with the consumer. The parked row on plain `[key, value]`
  pairs stays parked: `ProducerRecord` takes `Header` objects only, as before.
- **A14: done** (on PR #333): the tests on Jest, and `tsc --strict` over them (`make typecheck-tests`).

Notes from Phase A, for Phase B:

- `stringSerializer()` is decided by the Types rule ("`stringSerializer()` calls it for keys and values"), so
  B4 builds it, and A6's message can name it. Only the rest of the factory list stays parked (§5).
- The timed `get()`'s message is a recorded deviation that needs a core change: the core's text comes from
  `rust/src/common/internals/kafka_future_impl.rs`.
- No runtime `Producer` object is needed (A9): Java's `Producer` has no statics, so only the TypeScript
  interface exists.
- A latent edge to recheck when transactions are built: a zero-timeout `close()` from inside a delivery
  callback may skip a user callback for a record that was appended and then refused.

## 3. Phase B: complete the producer (PR #333)

PR #333 was rebased onto PR #312 at `44ee5ab04`. Its `MockProducer` translation ("javascript: MockProducer,
its non-transactional members") and all its fixups were **dropped**: B5 rewrites the mock over the FFI's
mock, so the gRPC server's empty-config mock goes with it until B5. The serializer commit was re-applied onto
Phase A's send path (zero copy, no closed state, frozen records).

| # | Change | Rule | Size |
|---|---|---|---|
| B1 | **Done.** **Value classes.** `equals`/`hashCode` on `TopicPartition`, `Node` and `PartitionInfo`. `PartitionInfo.replicas()`, `inSyncReplicas()` and `offlineReplicas()` return arrays. `RecordMetadata`'s public constructor, with `bigint` and `toJSON()`. `Object.freeze` in every data class. `PartitionInfo`'s public constructors take iterables of `Node` (§5, 1). | Q29, Q25, Q23, Q9 | M |
| B2 | **Done.** **`partitionsFor`**: a Promise via `kafka_producer_Producer_partitions_for_cb`, returning `PartitionInfo[]`. The gRPC `PartitionsFor` RPC, and its 4 `JS_GRPC_SKIPS` removed. | Q25, Q41 | M |
| B3 | **Done.** **`metrics()`**: synchronous via `kafka_producer_Producer_metrics`. `MetricName` and `Metric`, and `MetricNameMap` keyed by group, name and sorted tags. The gRPC `Metrics` RPC, and its 2 skips removed. The values are a snapshot, with `Metric` and `KafkaMetric` in plain TypeScript (§5, 3). | Q25, Q30 | M |
| B4 | **Done.** **Serializers.** The config-key route: a synchronous `({ configs, isKey }) => serde`. A class name, a serde object or a Promise-returning function throws `ConfigError`. The keys the function reads are kept out of the core's config, and a constructor argument wins. `close()`'s default moves to a `SerializerDefaults` helper class in `src/internal/`. The built-in factories are stop-and-ask (§5). | Q53, Q28 | M |
| B5 | **Done.** **`MockProducer` over the FFI's mock.** Replace the TypeScript translation. Forms: `()` and `{ autoComplete, partitioner: Partitioner \| null, keySerializer, valueSerializer }`. Keys and values pass as `void *` and are held until `clear()` or destroy. `history`, `clear`, `completeNext`, `errorNext`, `closed` and `flushed` call the FFI. `send`, `flush`, `close`, `partitionsFor` and `metrics` go through `__as_Producer`. The serializers run on every send. Java's public error fields become setters: `setSendException`, `setFlushException`, `setCloseException`, `setPartitionsForException` (§5, 2). `setMockMetrics` is left out and listed (§7). | Q62, Q30 | L |
| B6 | **Done.** **Error payloads.** `TopicPartitionMap` and `TopicPartitionSet` (`common`, js-only, on the invented-names list). Payload constructors and getters, generated by the TS emitter from Java's constructor chain evaluated as JavaScript: a `Set` for `Set<String>`, `TopicPartitionMap<bigint>` for `Map<TopicPartition, Long>`, `TopicPartitionSet` for `Set<TopicPartition>`, a `bigint` for `long`; `GroupAuthorizationError.forGroupId` and `QuotaViolationError.toString()`. Errors from the core carry their payloads through the FFI's views (`native/error_payload.c`, generated). The types the payloads reach are translated (Scope, Q30): `TimestampType` (`common/record`), `OffsetAndMetadata` (`consumer`) and the nested `RecordDeserializationError.DeserializationExceptionOrigin`, so `RecordDeserializationError` and `LogTruncationError` have their whole payloads; `TopicPartitionMap` / `TopicPartitionSet` hold `null` as Java's collections do. `QuotaViolationError.metric()` from the core is a `KafkaMetric` built from the view's metric name and the error's value, as .NET builds it (Python keeps the name and group only; `DECISIONS.md`, entry 26). Left out for want of an FFI view: `CorrelationIdMismatchError`'s ids from the core, `-1` as in Python and .NET. `check-java-completeness` asks the error classes for it. | Q39, Q23, Q25 | M |
| B7 | **Done.** **Docs and checks** for everything above: the API report, MIGRATION (`stats_cb` → `metrics()`, `getMetadata` → `partitionsFor`, `MockProducer`, `stringSerializer()`, the payload getters, `TopicPartitionMap` / `TopicPartitionSet`), README, CHANGELOG, and the `@packageDocumentation` lists (the generated packages list what their classes leave out). | Q64, Q70 | S |

### Hand-off: what is left after Phase B

- **A8: done** (on PR #333): `Headers` and `Header` over the FFI's `RecordHeaders` / `RecordHeader`, with the
  owner's option 1 (`DECISIONS.md`, entry 21). Left for the consumer: a consumed record's `Headers`, wrapping
  `kafka_consumer_ConsumerRecord_headers`. The row on plain `[key, value]` pairs stays parked (§5).
- **A13 and A14: done** (on PR #333). The docs lint (`eslint-plugin-jsdoc`'s `require-param` and `require-returns` on
  `src/`, in `make lint-rules`), TypeDoc (`make docs`, checked by `test/static/docs.test.js`), publint (in
  `make pack-check`, every package now `"type": "commonjs"`), Prettier (`make format-check`, in the root
  `lint-javascript`, generated files left out) and
  the tests on Jest (`jest.config.cjs`, through jest-light-runner in a fresh Node.js process per file; ts-jest is not
  used, the tests being JavaScript). `tsc --strict` over the tests (Q67, Q74) is done too: `make typecheck-tests`
  (`test/tsconfig.json`, `checkJs`, in the root `test-javascript-unit`) checks every test file, under `test/` and
  `verifiable_clients/test/`, with no finding,
  each deliberate misuse marked `@ts-expect-error` (`DECISIONS.md`, entries 22 to 25).
- The decisions taken while aligning and completing the producer are logged in `DECISIONS.md`, in this folder.
- **The parked rows (§5)**: the serde factories beyond `stringSerializer()`, Q50 ("the methods must be all async"),
  the `Headers` row on plain `[key, value]` pairs (the other, how a `Header` from a changeable `Headers` stays valid,
  is answered for this binding by the owner's option 1, while the root row stays parked), when and how the never-closed warning is logged, and the other `tsc` settings (Q74).
- **Transactions**: out of scope by request (the producer's and `MockProducer`'s transaction calls).
- **Phase C** (§4): the generators of the class modules and the addon's C wrappers, its timing still to decide.

## 4. Phase C: generators (decide the timing)

The rules say "Code is generated wherever it can be". "Each Java class is one **generated** snake_case file",
`check-generated` covers "the form tables", and Q4 adds generated C wrappers with their TypeScript types. Today
only the errors (with `src/internal/error_registry.ts` and `native/error_payload.c`, which reads an error's payload),
the config constants and the producer's configuration keys (`src/producer/producer_configs.ts` and
`src/internal/producer_config_keys.ts`) are generated. The class modules, form tables, C wrappers and
`native_types.ts` are hand-written.

- **C1 (L):** a design note, then an xtask generator for the addon's C wrappers and
  `src/internal/native_types.ts`. Branded handle types. A test that the addon's exports equal the declared
  functions.
- **C2 (L):** an xtask generator for the class modules and their form tables, under `check-generated`.

**Decision:** C before B, which avoids writing B's classes by hand twice. Or C after A and B, which gets the producer
working on the rules sooner, with the hand-written modules recorded as a deviation until C lands.

## 5. Questions to settle

**"Not decided yet" rows that touch the producer.** Stop and ask; today's code stays as it is:
- The list of serde factories beyond `stringSerializer()`, parked (B4 built `stringSerializer()`, which A6's message names).
- Q50, Emanuele's "the methods must be all async" for serdes.
- How a `Header` from a still-changeable `Headers` stays valid; plain `[key, value]` header pairs. Both parked.
- When and how the never-closed warning is logged, parked. After A3 an open client keeps Node alive, so today's
  `beforeExit` warning can never fire.
- The other `tsc` settings (Q74), parked.

**Settled by the rule for a similar case** (no question needed; `javascript/CLAUDE.md` records each reading):
1. **A Java array taken** (`PartitionInfo(…, Node[] …)`, `KafkaFuture.allOf(KafkaFuture...)`, `Headers(Header[])`)
   takes any iterable, as a `List` taken does. The types table pairs a Java array with a `List` when returned ("A
   `List` or a Java array returned → An array"), and Python maps both alike (`T[]` → `tuple`).
2. **A Java public mutable field** (`MockProducer.sendException`, `flushException`, `closeException`,
   `partitionsForException`) becomes a setter only: `setSendException(exception)` and so on. Python's rule names
   this exact case ("A public mutable field → a setter `set_<field>(*, <field>)` and nothing else:
   `MockProducer.sendException` → `set_send_exception`"). The JS rules keep public state in private fields with
   no accessor properties (Stability, Q17), and the FFI has setters only (`MockProducer_set_send_error`, …).
3. **`metrics()` returns a snapshot.** `Metric` and `KafkaMetric` are plain TypeScript, with values read when
   `metrics()` runs. Only the clients and the mocks call the addon (Implementation over the FFI), and Python's
   `metrics()` is a snapshot ("a list of `dict`s with `name`, `group`, `description`, `tags`, `value`"). Java's
   live view is a recorded deviation, as in Python.
4. **The NUL-byte refusal stays.** Python's marshaling layer refuses the same string ("embedded null character",
   as CPython's `PyArg_Parse("s")` does) without listing it as a check. In JS it is one of the listed "run-time type
   checks that TypeScript can't enforce": a `string` type can't exclude NUL. The message stays Node's own.
5. **`test/lint` moves to Jest** with every other test. "The tests run on Jest" carves out no exception.
6. **An enum's `toString()` follows Java's override,** as in `SslClientAuth`'s lowercase name, and `toJSON()`
   stays the name. Rust §3 prints the constant's name "unless Java overrides it", Python reaches `toString()`
   through `__str__`, and Rule 1 puts content with Java.
7. **A `null` key or value binds `never`** through extra signatures for the null combinations. Python's Binding
   rule adds "one stub per combination" beside its one stub per Java overload, so the per-form rule (Q15) and
   binding (Q26) both hold. This replaces the `send(ProducerRecord<K | null, V | null>)` root-conflict mark.

## 6. Linter changes (each lands with the item that triggers it)

| Linter rule | Change | With | Size |
|---|---|---|---|
| `one_copy` | Becomes a zero-copy check: no copy or allocation of a key or value on the send path, and a `napi_ref` released at the hand-over. | A1 | M |
| `cb_twin_only` | Drop the force-close allowance and flip `header_conformance`. Allow `get_cb` and `get_with_timeout_cb`. | A3, A7 | S |
| `type_mapping` | `bigint` for `long`; iterables taken; returned array / `Set` / `TopicPartitionSet` / `Map` / `MetricNameMap` / `TopicPartitionMap`; `Duration` returns `number`; `Optional` becomes `T \| null`. Drop `mirrored`. | A2, B1–B6 | M |
| `overload_shape` | A `bigint` kind. A `LEFT_OUT` list for Q30's forms. | A2, B5 | S |
| `java_ts` | Placeholders become `Partitioner`, `MetricValueProvider`, `MetricConfig` and `Measurable`. `CONSTANTS_ONLY` keeps only the client config classes. | A9, B3 | S |
| `java_completeness` | Drop the `equals`/`hashCode` exemption. Require error payloads. | B1, B6 | S/M |
| `data_objects` | Require `Object.freeze(this)` in data classes. Replace the mutable-field exception with a check that a Java public mutable field is a setter (§5, 2); `exception_word` drops its field exemption. | B1, B5 | S |
| `no_binding_queue` | Remove the allowances as their code goes: `Headers`, `INT_LABELS`, the mock's fields. Reword the timer allowance. | A5, A8, B5 | S |
| `single_drainer` | Model `_destroy` from the finalizer, for a closed client only. | A3 | S/M |
| `java_name` | Drop the dead `SECOND_TYPES` entry. | — | S |
| Baselines | Stale "not decided" reasons become plain to-dos, or exemptions for left-out forms. | A, B | S |
| **New checks** | No closed flag; no repeated precondition text; tsfn ref/unref only on the close path; Q28 helper classes present; mocks hold a native handle; `encodeLikeJava` callers; the CommonJS pin. Skip what the rules mark "(not checked)". | after A | S each |

## 7. Blockers (a C FFI or Rust core change)

- **None: nothing in the producer is blocked on the core** (`DECISIONS.md`, entry 26).
- `QuotaViolationError.metric()` from the core is built from the view's metric name and the error's value, as
  .NET builds it (Python keeps the name and group only).
- The items left out match the other bindings:
  - `setMockMetrics(MetricName, Metric)` is left out and listed in the docs, as in .NET, whose mock also wraps the
    FFI's: `MockProducer_set_mock_metrics` takes a native `KafkaMetric_t`, and there is no
    `kafka_common_metrics_KafkaMetric_new`.
  - `CorrelationIdMismatchError`'s ids are `-1` from the core in all three bindings (no FFI view of the class).
- The worker cleanup hook, a Future improvement and not built, would need `__set_callbacks_notify(NULL)` and a
  wait for an in-flight hook call, for all three clients.

## 8. Process

- One Actor/Critic loop per phase, or per group of A or B items.
- Each linter change ships with its item.
- Commit with `--no-verify`. Run the full gate once at the end of each PR, at that PR's head.
- Check #327's head before each phase.
- Update the tables here in the same commit as the work.

## 9. Future improvements

- Property-based tests with `fast-check` (the test plan's TOOL-02): not integrated now, by the owner's decision of
  2026-11-18.
- The performance tests: built later, and kept local only.
