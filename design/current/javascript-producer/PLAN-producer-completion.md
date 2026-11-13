# JavaScript producer: aligned with the binding rules, then code complete (transactions excluded)

Status: **draft, awaiting approval** (2026-11-13).

Checked against PR #327's head **`93daa9d9d`** ("JS binding rules: decisions of 2026-11-13"). Before
planning or reviewing, fetch `origin/kraina-js-claude-rules` and diff its `CLAUDE.md` against the branch's
copy: #327 is still open and still changing. This plan replaces the 2026-11-12 version, which was checked
against #312's stale copy of the rules (`1bfe80fd7`, 2026-11-10).

## 1. Scope and branches

- **Phase A (PR #312, `dev/javascript-producer-bindings`):** align the producer that #312 already has with
  the rules at `93daa9d9d`. Those rules rewrite #312's own code, so the work belongs there.
- **Phase B (PR #333, `dev/javascript-producer-completion`, stacked on #312):** complete the producer.
- **Out of scope:** transactions, by request. The rules' "Future improvements" are not built either: the
  worker cleanup hook, the borrowed-Buffer debug check, `[util.inspect.custom]`, refusing a call inside a
  callback after its first `await` (Q46), and a closed client collected with an answer queued (Q49).
- **Branches:** both are rebased onto `93daa9d9d` locally (`rebased/js-producer-bindings-on-327`,
  `rebased/js-producer-completion-on-327`). Only `CLAUDE.md` changed. The force-push is pending approval.

## 2. Phase A: align PR #312 with the rules

Each item names the rule (by its Q number) and what changes in today's code.

| # | Rule | Change to today's code |
|---|---|---|
| A1 | Zero copy on produce (Q21) | Drop `js_bytes_copy`. Borrow the app's key and value, a serializer's output included, and a Buffer over a `SharedArrayBuffer` too. Hold a reference until the send settles (the hand-over) and drop it on the JS thread. Header values stay copied. Put the rule's docs text verbatim on `send()` and in the README, the `SharedArrayBuffer` sentence included. Replace the "changed or detached as soon as `send()` returns" tests with tests of the new contract. Rewrite `javascript/CLAUDE.md` rule 8. |
| A2 | `long` is `bigint` (Q23) | `RecordMetadata.offset` and `.timestamp` become `bigint`. A `long` argument takes a `bigint`, or a whole `number` up to 2^53 (`RangeError` above that). An `int` outside its range throws `RangeError`. |
| A3 | No closed flag; lifetime (Q21, Q61) | Remove `#closed`. A call after `close()` goes to the FFI and gets the core's error. `close()` doesn't free the native handle. The finalizer calls `_destroy` only for a client that's already closed, and never closes one. A client keeps Node running from creation until `close()`: the tsfn stays referenced and is unreferenced after the close. `send()` on a closed producer runs the serializer before the core's error *(deviation in the rule)*. |
| A4 | No binding-side preconditions (Q35, Q22) | Remove the null, closed and negative-timeout checks that the FFI or the core make. Keep the form check, the run-time type checks TypeScript can't enforce, and unknown option keys. |
| A5 | Configuration (Q52, Q54, Q55) | No `ConfigDef` table and no value checks: drop the generated key-type table and convert each value to its string. librdkafka keys and a `kafkaJS` block reach the core as unknown keys, with no warning of the binding's own. The config classes are translated for their constants only. |
| A6 | `Headers` and `Header` wrap the FFI (Q31) | Rewrite both over the FFI's `RecordHeaders` and `RecordHeader` handles: `add`, `remove`, `lastHeader`, `headers` and `toArray` call the addon. **Open:** how a `Header` taken from a `Headers` that can still change stays valid, and whether records take headers as plain `[key, value]` pairs. Build the decided part, leave these two as they are, and list them. |
| A7 | String key or value without a serializer (Q24, Q27) | Throw `TypeError` with a message pointing to `stringSerializer()`, instead of encoding the string. |
| A8 | Client interface, abstract classes, collections (Q10, Q16, Q25, Q29) | `Producer` becomes a TypeScript `interface` plus a runtime object for its statics. A Java abstract class becomes an `abstract class`, whose constructor throws `TypeError` when `new.target` is that class. A Java `Collection` parameter takes any iterable, and a bare string where Java takes `Collection<String>` throws `TypeError`. `equals` and `hashCode` stay methods. |
| A9 | Generated addon wrappers and types (Q4) | A generator writes the addon's C wrappers from the C FFI header, and their TypeScript types in `src/internal/native_types.ts`, with a branded type per handle. The loader gives the addon that type, and a test checks that its exports and the declared functions are the same set. **No such generator exists yet** (`rust/xtask/src/javascript_gen.rs` writes only the errors and config files). This needs a short design note first, saying which of today's hand-written C becomes generated wrappers and what stays hand-written (the callback queue, the send contexts). |
| A10 | Tooling (Q13, Q64, Q66, Q73, Q75, Q76) | Move the tests to Jest (with ts-jest). CommonJS only: `module: nodenext`, no `"type": "module"`, and `exports` entries with `types` and `default` conditions. TypeDoc for the reference. Version `4.4.0-beta.1` under npm's `beta` tag. Every file gets Python's header. `make build-javascript`, `CONFLUENT_KAFKA_LIB_DIR`, and the addon including only `rust/target/include/confluent_kafka_bindings.h`. |
| A11 | Docs (Q1, Q6, Q21, Q53) | One API, shaped like Java's. "The preview doesn't support clients in worker threads…", with no check in the binding. The default partitioner is the core's CRC-32 `consistent_random`: the migration guide says that keys can land on other partitions than with 1.x or Java. The anti-patterns inside callbacks are listed. |

Order within Phase A: A1–A5 and A7 first (behaviour), then A8 and A6, then A10 (the Jest move), then A9
after its design note.

## 3. Phase B: complete the producer in PR #333

| # | Item | State at `93daa9d9d` |
|---|---|---|
| B1 | Value types: the two constants, `TopicPartition`, `Node`, `PartitionInfo` with methods | **Done.** Add `equals`/`hashCode` (Q29, now decided); `PartitionInfo`'s `Node[]` getters as arrays and its public constructors (Q25, now decided); `RecordMetadata`'s public constructor with `bigint` (Q23, now decided). |
| B2 | `partitionsFor` | **Unblocked** (Q25: a `List` returned is an array). A Promise via `partitions_for_cb`, the gRPC `PartitionsFor` RPC, and its 4 `JS_GRPC_SKIPS` removed. |
| B3 | `metrics()`, `MetricName`, `Metric` | `MetricName` and `Metric` can be built: `tags()` is a `Map`, `metricValue()` is `unknown`, and a `long` value is a `bigint`. **`metrics()` itself is still blocked:** it returns `Map<MetricName, Metric>`, and the types table has rows only for maps keyed by strings or by `TopicPartition`. This needs a row from Kaushik. |
| B4 | Serializers | The shape, the constructor keys and inference are **done**. To add: the config-key route (Q53), where a key takes a synchronous `({ configs, isKey }) => serde`. A class name, a serde object or a function returning a Promise throws `ConfigError`. The config object records the keys the function reads, and those keys are left out of the core's config. A constructor argument wins over the key. **Still open:** the list of serde factories (the rules name `stringSerializer()` in A7's error message) and Q50 (Emanuele). |
| B5 | `MockProducer` | **Redo.** It wraps the FFI's `MockProducer` (Q62), replacing #333's TypeScript translation. Keys and values pass as `void *` and are held until `clear()` or destroy. The serializers run on every send. `history()` returns the app's records, and unsupported methods give the core's `UnsupportedOperationError`. **Open:** the Q30 list of the constructor forms it leaves out. |
| B6 | Error payloads | **Unblocked** (Q25, Q23): payload getters and payload constructors, with a `Set` for a `Set<String>` and a `TopicPartitionMap<bigint>` for a `Map<TopicPartition, Long>`. A second constructor argument throws `IllegalArgumentError` in the form check's format (Q39, now decided). |
| B7 | Docs and CI | The TypeDoc reference, `make verify` with the JS build, lint, type check and tests, the API report, and the migration guide. The `bench/` harness stays local. |

## 4. Still open in the rules (stop and ask)

| Item | Blocks |
|---|---|
| The type of `metrics()`'s `Map<MetricName, Metric>` | B3's `metrics()`, and the gRPC `Metrics` RPC with its 2 skips |
| The list of serde factories | B4's built-in factories. A7's message names `stringSerializer()`, so that one may need settling first |
| "The methods must be all async" for serdes (Q50) | Whether async serdes stay |
| The Q30 list of forms left out | B5's constructor forms |
| How `toJSON()` writes a `bigint` (Q23) | `toJSON()` on a class with a `long` field |
| A `Header` from a `Headers` that can still change; headers as plain pairs (Q31, Q60) | Part of A6 |
| When the warning for a never-closed client is logged (Q21) | Its timing; today's warning stays |
| The other `tsc` settings (Q74) | Nothing in the producer |

## 5. Hand-off

Agents update the tables above in the same commit as the work: an item is done, missing, or blocked on a
§4 row. Check #327's head before starting a phase. If a §4 row has been decided since, build it; if a
decided rule changed, flag it before building on it.
