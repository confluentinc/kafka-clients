# Critic 7 — Phase 7a (ProducerConfig) review (resolved)

Reviewed commits `1d02f6d`, `6989a61`, `15d4ac8`, `96ee9c6`, `380bd30`,
`8a8d413` on branch `fresh-impl`.

---

## Suggestion 1: `transaction.timeout.ms` / `transaction.two.phase.commit.enable` mutual-exclusion is implemented but untested

- **File**: `src/producer/producer_config.rs:1346-1354`
- **Severity**: Suggestion
- **Java Reference**: `ProducerConfig.java:642-650`

The mutual-exclusion check that throws when both `2pc=true` and a
user-supplied `transaction.timeout.ms` are present is implemented:

```rust
let enable_2pc = self.inner.get_boolean(TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG)?;
let user_configured_txn_timeout = self.inner.originals().contains_key(TRANSACTION_TIMEOUT_CONFIG);
if enable_2pc && user_configured_txn_timeout {
    return Err(KafkaError::Config(format!(
        "Cannot set {TRANSACTION_TIMEOUT_CONFIG} when {TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG} is set to \
         true. Transactions will not expire with two-phase commit enabled."
    )));
}
```

The Java test `testTwoPhaseCommitIncompatibleWithTransactionTimeout`
that was supposed to exercise this is skipped (`producer_config.rs:1692-1703`).
The skip rationale is:

> SKIPPED for Milestone-1. The Java test sets `enable.idempotence=true`
> AND `transactional.id="test-txn-id"` both of which are rejected at
> construction in Milestone-1 ... Re-translating this test verbatim
> would only exercise the Milestone-1 rejection paths, not the
> 2pc/transaction-timeout mutual exclusion logic Java intends to
> exercise.

The verbatim port is correctly identified as impossible. **However,
the underlying logic IS reachable in Milestone-1.** The check at line
1349 is gated by `enable_2pc && user_configured_txn_timeout` only —
neither `enable.idempotence=true` nor `transactional.id` is required.
A Rust-adapted test such as:

```rust
#[test]
fn test_two_phase_commit_incompatible_with_transaction_timeout() {
    let mut props = minimal_props();
    props.insert(TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG.to_owned(), "true".to_owned());
    props.insert(TRANSACTION_TIMEOUT_CONFIG.to_owned(), "60000".to_owned());
    let err = ProducerConfig::new(props).unwrap_err();
    let msg = err.message();
    assert!(msg.contains(TRANSACTION_TIMEOUT_CONFIG));
    assert!(msg.contains(TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG));
    // And the success cases:
    let mut props = minimal_props();
    props.insert(TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG.to_owned(), "true".to_owned());
    ProducerConfig::new(props).expect("2pc=true without timeout is valid");
    let mut props = minimal_props();
    props.insert(TRANSACTION_TIMEOUT_CONFIG.to_owned(), "60000".to_owned());
    ProducerConfig::new(props).expect("timeout without 2pc is valid");
}
```

…would exercise the Milestone-1-reachable subset of the Java test (the
2pc + timeout mutual-exclusion logic and the two valid-by-symmetry
cases) without hitting either Milestone-1 rejection. As-is, the 8-line
production logic block at `producer_config.rs:1346-1354` has no test
coverage. The skip note is too broad — only the verbatim port is
infeasible, not the underlying invariant.

**Proposed fix**: add the test above to `mod tests`, and update the
`TODO Phase 9` comment at `producer_config.rs:1703` to "TODO Phase 9:
also add the variants that combine 2pc with `enable.idempotence=true`
and `transactional.id="..."` once those are permitted again." Cover
what you can in Milestone-1, defer only what's actually blocked.

**Disposition**: Fixed in commit `d40ad81` (fixup! 380bd30).
Added `test_two_phase_commit_rejects_explicit_transaction_timeout`
that asserts byte-exact error message equality on the rejection path,
plus the two success-by-symmetry cases (2pc=true alone, timeout
alone). The skip comment was rewritten to scope the deferral to "the
variants that combine 2pc with `enable.idempotence=true` /
`transactional.id`" rather than the verbatim test as a whole. Test
count is now 1139 (was 1138).

---

## Suggestion 2: Public DOC constants diverge significantly from Java verbatim text

- **Files**: `src/producer/producer_config.rs:242,247,269-273,283-288,297-300,305-308`
- **Severity**: Suggestion
- **Java Reference**: `ProducerConfig.java:297,301,333-335,339-347,351-353,357-360`

Six `pub const` doc strings in `producer_config.rs` are exposed as
part of the public API surface (per Java's `public static final
String *_DOC` pattern):

| Constant | Rust text length | Java text length |
|---|---|---|
| `KEY_SERIALIZER_CLASS_DOC` | 80 chars | 132 chars |
| `VALUE_SERIALIZER_CLASS_DOC` | 82 chars | 134 chars |
| `INTERCEPTOR_CLASSES_DOC` | 290 chars | 313 chars |
| `ENABLE_IDEMPOTENCE_DOC` | 369 chars | 707 chars |
| `TRANSACTION_TIMEOUT_DOC` | 122 chars | 339 chars |
| `TRANSACTIONAL_ID_DOC` | 207 chars | 540 chars |

For example, `KEY_SERIALIZER_CLASS_DOC` in Rust is:
```
"Serializer class for key that implements the `Serializer` interface."
```
Java is:
```
"Serializer class for key that implements the org.apache.kafka.common.serialization.Serializer interface."
```

CLAUDE.md Rule #4 requires: "Keep similar comments as the Java source,
translate javadoc to rustdoc. Never change the contract of public
API." These public DOC constants are part of the contract — IDE
tooltips, HTML doc generators (Java's `main` method that produces
`producerconfigs_*` HTML), and any user code that reads
`ProducerConfig::KEY_SERIALIZER_CLASS_DOC` directly all see the
verbatim string.

The non-public file-private DOC constants (BATCH_SIZE_DOC, ACKS_DOC,
etc.) being shortened is acceptable since they aren't part of the
public surface.

**Proposed fix**: restore Java's verbatim text for the 6 `pub const ...
_DOC: &str` constants, using `concat!` for the multi-line ones. The
`ENABLE_IDEMPOTENCE_DOC` Milestone-1 deviation note can be appended at
the end of the existing Java text rather than replacing it.

**Disposition**: Fixed in commit `6633ea8` (fixup! 15d4ac8). All six
public DOC constants now carry Java's verbatim Kafka 4.2 text
(`<code>` tags preserved). `ENABLE_IDEMPOTENCE_DOC` ends with a `<p>`
break and the Milestone-1 deviation note pointing at
`design/history/Milestone-1/PLAN.md`, so the Java contract is intact
for the parts that apply and the Milestone-1 carve-out is documented
inline.

---

## Suggestion 3: `SSL_ENABLED_PROTOCOLS_DOC` text is the older Java version

- **File**: `src/common/config/ssl_configs.rs:55-60`
- **Severity**: Suggestion
- **Java Reference**: `SslConfigs.java:50-55`

The Rust doc says:
```
"The list of protocols enabled for SSL connections. The default is 'TLSv1.2,TLSv1.3' when running with Java 11 or
newer, 'TLSv1.2' otherwise. ..."
```

Java 4.2 says (the Apache Kafka 4.2 source bundled at `kafka/`):
```
"The list of protocols enabled for SSL connections. The default is 'TLSv1.2,TLSv1.3'.
This means that clients and servers will prefer TLSv1.3 if both support it
and fallback to TLSv1.2 otherwise (assuming both support at least TLSv1.2).
This default should be fine for most use cases. If this configuration is set to an empty list,
Kafka will use the protocols enabled by default in the underlying SSLEngine, which may include
additional protocols depending on the JVM version. Also see the config documentation for
ssl.protocol to understand how it can impact the TLS version negotiation behavior."
```

The Rust text has stale "Java 11 or newer" wording from an older
Kafka version. Per CLAUDE.md Rule #4 these public-facing doc strings
should match the Apache Kafka 4.2 source.

**Proposed fix**: copy Java's current `SSL_ENABLED_PROTOCOLS_DOC` text
verbatim into `concat!(...)`.

**Disposition**: Fixed in commit `397c0d2` (fixup! 6989a61).
`SSL_ENABLED_PROTOCOLS_DOC` now carries the Kafka 4.2 verbatim text
(no "Java 11" reference) with `<code>` markup.

---

## Suggestion 4: `SSL_PROTOCOL_DOC` text differs from Java's current version

- **File**: `src/common/config/ssl_configs.rs:34-41`
- **Severity**: Suggestion
- **Java Reference**: `SslConfigs.java:33-38`

Rust adds an enumeration of legacy protocols ("`TLS', `TLSv1.1`,
`SSL`, `SSLv2`, `SSLv3` may be supported in older JVMs, but their
usage is discouraged due to known security vulnerabilities") that is
not in the current Java 4.2 `SSL_PROTOCOL_DOC`. Java only describes
TLSv1.2/1.3 fallback semantics. Same fix as Suggestion 3.

**Disposition**: Fixed in commit `397c0d2` (fixup! 6989a61). The
legacy-protocols paragraph and the "may be supported in older JVMs"
sentence were removed; the doc now matches the Kafka 4.2 verbatim
text describing only TLSv1.2/1.3 fallback semantics.

---

## Nit 2: dead `_sasl_anchor` / `_ssl_anchor` use bindings

- **File**: `src/producer/producer_config.rs:1363-1366`
- **Severity**: Nit

```rust
#[allow(unused_imports)]
use sasl_configs as _sasl_anchor;
#[allow(unused_imports)]
use ssl_configs as _ssl_anchor;
```

These appear to be defensive against unused-import warnings, but
`sasl_configs` and `ssl_configs` are already brought into scope at line
52 and used by the test module's full-path references. The two anchor
bindings are unreachable noise and the `#[allow(unused_imports)]`
suppresses what should be an actual lint signal that they're dead.

**Proposed fix**: remove the four lines.

**Disposition**: Fixed in commit `1376f1b` (fixup! 15d4ac8). The
four-line block was removed; in addition, the bare `sasl_configs` /
`ssl_configs` items in the line-52 `use` were also dropped because
all callers use full crate paths
(`crate::common::config::ssl_configs::*`) and the bare module names
were never referenced — the anchor block had been the only thing
keeping them in scope. `cargo build` and `cargo xtask lint` confirm
no `unused_imports` warnings.

---

## Nit 3: `zero_or_more_send_buffer` validator name is misleading

- **File**: `src/producer/producer_config.rs:374-377`
- **Severity**: Nit

```rust
let zero_or_more_send_buffer: Arc<dyn Validator> =
    Arc::new(Range::at_least(common_client_configs::SEND_BUFFER_LOWER_BOUND));
let zero_or_more_recv_buffer: Arc<dyn Validator> =
    Arc::new(Range::at_least(common_client_configs::RECEIVE_BUFFER_LOWER_BOUND));
```

`SEND_BUFFER_LOWER_BOUND` and `RECEIVE_BUFFER_LOWER_BOUND` are both
`-1` (`-1` means "use OS default" per Kafka semantics). The variable
names `zero_or_more_*` falsely imply a `>= 0` constraint when the
constraint is actually `>= -1`. This is harmless because the values
flow into `Range::at_least`, but the names are inconsistent with the
neighbouring `zero_or_more_i32` and `zero_or_more_i64` (which actually
take `0`).

**Proposed fix**: rename to `at_least_send_buffer_lower_bound` /
`at_least_recv_buffer_lower_bound` (or similar) to drop the
"zero" claim.

**Disposition**: Fixed in commit `1376f1b` (fixup! 15d4ac8). Renamed
to `at_least_send_buffer_lower_bound` /
`at_least_recv_buffer_lower_bound`, dropping the false `zero_or_more_*`
claim. Added a 4-line comment that mirrors Java's inline `atLeast(...)`
call sites and notes the actual `>= -1` semantics.

---

# Critic 7 — Phase 7b Round 1 review (resolved)

Reviewed commits `c63329c` (Producer trait skeleton + UnsupportedOperation),
`26073a3` (ProducerRecordTest / RecordMetadataTest gap audit), `f222f20`
(memory).

## Method-by-method audit of `Producer.java` against `src/producer/producer.rs`

| Java method | Rust method | Status |
|---|---|---|
| `void initTransactions()` | `init_transactions(&self) -> Result<(), KafkaError>` | OK (Suggestion 2 follow-up: async-ified in `0fc8655`) |
| `void beginTransaction()` | `begin_transaction(&self) -> Result<(), KafkaError>` | OK (Suggestion 2 follow-up) |
| `void sendOffsetsToTransaction(Map, ConsumerGroupMetadata)` | (deferred) | OK — module rustdoc cites Phase 9 owner |
| `void commitTransaction()` | `commit_transaction(&self) -> Result<(), KafkaError>` | OK (Suggestion 2 follow-up) |
| `void abortTransaction()` | `abort_transaction(&self) -> Result<(), KafkaError>` | OK (Suggestion 2 follow-up) |
| `void registerMetricForSubscription(KafkaMetric)` | (deferred) | OK — module rustdoc cites metrics-translation owner |
| `void unregisterMetricFromSubscription(KafkaMetric)` | (deferred) | OK — same |
| `Future<RecordMetadata> send(ProducerRecord)` | `async fn send(...)` | OK; `Future`-collapse rationale documented |
| `Future<RecordMetadata> send(ProducerRecord, Callback)` | `async fn send_with_callback(...)` | OK |
| `void flush()` | `async fn flush(...)` | OK |
| `List<PartitionInfo> partitionsFor(String)` | `async fn partitions_for(&self, topic: &str)` | OK; `&str` matches CLAUDE.md rule 12 |
| `Map<MetricName, ? extends Metric> metrics()` | `metrics(&self) -> ProducerMetrics` (alias for `HashMap<String, ()>`) | OK; placeholder type alias documented |
| `Uuid clientInstanceId(Duration)` | `async fn client_instance_id(&self, timeout: Duration)` | OK; message byte-exact |
| `void close()` | `async fn close(&self)` | OK |
| `void close(Duration)` | `async fn close_with_timeout(&self, timeout: Duration)` | OK |

The Java interface (`kafka/clients/.../producer/Producer.java`) has
**14 methods**. The trait covers 12 + 2 documented deferrals.

After Round 1 fixup (`0fc8655`):
- `init_transactions_with_keep_prepared` and `prepare_transaction` are
  removed (not on Java's interface — see Suggestion 1 disposition).
- The four transactional methods are now `async fn` (see Suggestion 2
  disposition).

## Trait-shape compliance — verified

- `async fn` in trait (no `#[async_trait]` macro): confirmed at
  `producer.rs`. No `Pin<Box<dyn Future>>`.
- `Send + Sync` trait bound: `producer.rs:113`.
- Module-inception `#[allow(...)]` documented: `producer/mod.rs:28`.
- `pub use producer::{Producer, ProducerMetrics};` in `producer/mod.rs:45`.
- License header: `producer.rs:1-13`.
- Deferred methods enumerated in module-level rustdoc with owning
  phase: `producer.rs:62-77`.

## `UnsupportedOperation` variant — verified

- Defined at `errors.rs:199`.
- `Display` via `java_class_name(): "UnsupportedOperationException"` +
  message → `errors.rs:350, 437-446`.
- `is_retriable() == false`, `is_fatal() == false`,
  `txn_requires_abort() == false` — verified via test
  `unsupported_operation_is_neither_retriable_nor_fatal`
  (`errors.rs:569-584`).
- `code()` returns `ERR_CODE_CONFIG` (`errors.rs:305`).
- `client_instance_id` message byte-exact:
  `"Client telemetry is not implemented in Milestone-1."`.

## Test gap-fill (commit `26073a3`) — verified

- Java `ProducerRecordTest#testEqualsAndHashCode` ↔ Rust
  `equals_and_hash_code` (`producer_record.rs:351-382`). 1:1.
- Java `ProducerRecordTest#testInvalidRecords` ↔ Rust
  `invalid_records` (`producer_record.rs:395-419`). The null-topic
  case is structurally elided (Rust `impl Into<Arc<str>>` cannot be
  null at the type level — rationale documented in test rustdoc).
  The negative-timestamp / negative-partition cases are tested with
  byte-exact error message assertions matching `ProducerRecord.java:74,77`.
- Java `RecordMetadataTest#testConstructionWithMissingBatchIndex` ↔
  Rust `test_construction_with_missing_batch_index`
  (`record_metadata.rs:132-147`). 1:1.
- Java `RecordMetadataTest#testConstructionWithBatchIndexOffset` ↔
  Rust `test_construction_with_batch_index_offset`
  (`record_metadata.rs:151-166`). 1:1.

No `@Test` cases missed.

## Memory commit `f222f20` — verified

- Touches only `.claude/agent-memory/actor-executor/MEMORY.md` and
  `phase7b_producer_trait.md`.
- No edits to `CLAUDE.md` or `.claude/rules/`.

## Round 1 verdict: accepted with 2 Suggestion items, both fixed in `0fc8655`

---

## Suggestion 1 — `init_transactions_with_keep_prepared` and `prepare_transaction` are not in the Java interface

- **File**: `src/producer/producer.rs:134, 168`
- **Severity**: Suggestion
- **Java Reference**: `kafka/clients/src/main/java/org/apache/kafka/clients/producer/Producer.java`
- **Description**: The trait declares two methods that do not exist
  on the Java `Producer<K, V>` interface in this 4.2 source:
  - `init_transactions_with_keep_prepared(&self, keep_prepared_txn: bool)`
    — Java has only `void initTransactions()` (no boolean overload).
    `KafkaProducer.java` likewise has a single `public void initTransactions()`
    at line 648 with no overload.
  - `prepare_transaction(&self) -> Result<bool, KafkaError>`
    — `prepareTransaction()` exists only on
    `internals/TransactionManager.java:342`, which is package-private
    internal API. It is not exposed on `Producer` or `KafkaProducer`.
- **Expected**: Per DoD #7 ("Are there structs or traits that aren't
  present in Java codebase? Avoid adding new structs or traits that
  aren't present in Java codebase"), neither method should be on the
  trait. The brief that drove Phase 7b listed both as expected — but
  the Java source disagrees. Either (a) drop both methods from the
  trait, or (b) keep them with a rustdoc note citing the
  Confluent-internal / KIP-derived rationale they originate from.
- **Actual**: Both methods are present on the trait, returning
  `KafkaError::UnsupportedOperation`. The commit message says they
  "exist to satisfy the Java interface shape" but the Java interface
  shape does not include them.
- **Action**: Recorded for follow-up — the manager may prefer to
  keep the surface and revisit when transactional support lands. Not
  blocking Phase 7c since `KafkaProducer` will simply implement them
  to delegate to `UnsupportedOperation` regardless.

**Disposition**: Fixed in commit `0fc8655` (fixup! c63329c). Both
methods deleted from the trait and the `StubProducer` test impl.
Verified against Java sources: `Producer.java:45` exposes only the
single `void initTransactions()` and `KafkaProducer.java:648` has no
overload; `prepareTransaction` exists only on the package-private
`internals/TransactionManager.java:342`. A Phase 7c carry-over note in
`Phase-7/NOTES.md` directs Phase 7c to re-verify against
`KafkaProducer.java` whether either method has been back-ported as an
inherent method, and to translate them as inherent `impl KafkaProducer`
methods (not trait methods) if so.

---

## Suggestion 2 — `init_transactions` returns `Result<(), KafkaError>` but Java is sync `void` throwing checked exceptions

- **File**: `src/producer/producer.rs:124`
- **Severity**: Suggestion
- **Java Reference**: `Producer.java:45` (`void initTransactions()`)
- **Description**: The transactional `*Transaction` methods on the
  Java interface are synchronous (`void`, not `Future`) — they block
  on broker round-trips. CLAUDE.md rule 9.1 says "if a method is
  blocking in Java it should async in Rust". The Rust translation
  declared them as **sync** `fn` returning `Result`, not `async fn`.
- **Expected**: For Java-blocking → Rust-async parity, these should
  be `async fn -> Result<(), KafkaError>`. The current shape will be
  awkward when the real `KafkaProducer` impl arrives — the body needs
  to await the broker round-trip but the trait method is sync.
- **Actual**: `init_transactions`, `init_transactions_with_keep_prepared`,
  `begin_transaction`, `commit_transaction`, `abort_transaction`,
  `prepare_transaction` are all sync `fn`. The other Java-blocking
  methods on the trait (`send`, `flush`, `partitions_for`, `close`,
  `client_instance_id`) are correctly `async fn`.
- **Note**: For Milestone-1 these methods always return
  `UnsupportedOperation` so the sync/async mismatch is hidden. But it
  becomes a source-breaking change when transactions land in Phase 9
  (any downstream caller written against the sync signature will need
  to add `.await`). Worth fixing now while no one consumes the trait.

**Disposition**: Fixed in commit `0fc8655` (fixup! c63329c). The four
remaining transactional methods (`init_transactions`,
`begin_transaction`, `commit_transaction`, `abort_transaction`) now
return `impl Future<Output = Result<(), KafkaError>> + Send`, matching
the existing `async fn` shape used by `send`/`flush`/`partitions_for`/
`close`/`client_instance_id`. The bodies still resolve immediately to
`KafkaError::UnsupportedOperation` in Milestone-1, but the signature is
future-proof for Phase 9. The `StubProducer` test impl and the
`async_methods_dispatch_through_trait` test were updated in lockstep;
no production callers exist yet (verified via grep).

---

# Critic 7 — Phase 7c Round 1 review (resolved)

Reviewed commits `f540846`, `b696f5d`, `848f9a1`, `0b8b1d9` on branch
`fresh-impl`.

---

## Suggestion 1: `NETWORK_THREAD_PREFIX` exported but never used to identify the spawned task

- **File**: `src/producer/kafka_producer.rs:99,497-499`
- **Severity**: Suggestion
- **Java Reference**: `KafkaProducer.java:455-457`

```rust
pub const NETWORK_THREAD_PREFIX: &str = "kafka-producer-network-thread";
…
let sender_task: JoinHandle<()> = tokio::spawn(async move {
    sender.run_loop().await;
});
```

Java sets `ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId` and
hands it to `Sender.SenderThread`. Tokio doesn't expose native task
names, but the equivalent observability hook is `tracing::info_span!`.
The constant is defined and exported but never used to instrument the
spawned future, so log lines emitted from inside `run_loop` carry only
whatever the `LogContext` prefixes (which already includes the
`clientId`). Net result: equivalent observability, but the constant is
dead weight in this commit.

**Recommendation**: either drop `NETWORK_THREAD_PREFIX` from the public
export until Phase 7e wires it into a `tracing::info_span!`-instrumented
spawn, or wrap the spawned future:

```rust
let span = tracing::info_span!("kafka-producer-network-thread", client_id = %client_id);
let sender_task = tokio::spawn(async move { sender.run_loop().instrument(span).await });
```

The latter matches Java's intent without adding runtime cost.

**Disposition**: Fixed in commit `973218a` (fixup! `b696f5d`). Chose
option (a) — demoted the constant to a documented module-level comment
explaining the `tracing` rationale and the conditions under which the
constant should be reintroduced (i.e., when/if the codebase adopts
`tracing` for span instrumentation). Option (b) was rejected because
this crate uses `log`, not `tracing`, and adding `tracing` as a
dependency for one constant is overkill. The Sender's `LogContext`
already prefixes every log line with `[Producer clientId=...]`, so the
per-message context Java provides via the thread name is preserved
without the prefix constant.

---

## Suggestion 2: `configure_delivery_timeout` silent bump emits no log warning

- **File**: `src/producer/kafka_producer.rs:603-629`
- **Severity**: Suggestion
- **Java Reference**: `KafkaProducer.java:582-587`

When the user **didn't** explicitly set `delivery.timeout.ms` and the
default is too low, Java logs:

```java
log.warn("{} should be equal to or larger than {} + {}. Setting it to {}.",
    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG,
    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
```

so operators can see the auto-bump in their logs. The Rust translation
silently returns `linger_plus_request` without a log line. This is a
behavioral divergence affecting observability, not correctness — the
returned value is correct.

**Recommendation**: emit `tracing::warn!("…Setting it to {}", linger_plus_request)`
in the silent-bump branch before returning. Tracing is already in scope
(`use tracing::*` exists elsewhere in the file ecosystem).

**Disposition**: Fixed in commit `973218a` (fixup! `b696f5d`). Translated
as `log::warn!` (the crate uses `log`, not `tracing`) with the same format
string Java uses verbatim:
`"{} should be equal to or larger than {} + {}. Setting it to {}."`.
Operators now see the auto-bump in their logs as in Java.

---

## Suggestion 3: `config.logUnused()` not translated (Java line 458)

- **File**: `src/producer/kafka_producer.rs:497-499` (constructor tail)
- **Severity**: Suggestion
- **Java Reference**: `KafkaProducer.java:458`

Java's constructor calls `config.logUnused()` immediately after spawning
the IO thread — this prints a `WARN` for every config key the user
provided but the producer didn't consume (typically due to typos or
stale configs). The Rust constructor doesn't call any equivalent.
`AbstractConfig::log_unused()` exists in this repo (verified) and is
called from other config-consumer paths.

**Recommendation**: insert `config.log_unused();` (or the equivalent
method name) after the `tokio::spawn`, mirroring Java's order. Helps
users catch typo'd configs early. Low-risk addition.

**Disposition**: Fixed in commit `973218a` (fixup! `b696f5d`). Added
`config.inner().log_unused()` immediately after the `tokio::spawn`,
mirroring Java line 458. `AbstractConfig::log_unused()` was already
implemented (Phase 1 backfill not needed) and tracks accessed keys via
`AbstractConfig::touch` on every typed `get_*` accessor.

---

## Suggestion 4: Sender's `pub(crate) running_arc` ungate doesn't audit the prior test-only `Sender::is_running()` accessor

- **File**: `src/producer/internals/sender.rs:402-405,407-413`
- **Severity**: Suggestion
- **Context**: The Phase 7c diff at `sender.rs` ungates `running_arc` /
  `force_close_arc` from `#[cfg(test)]` to `pub(crate)` because
  `KafkaProducer::Drop` (production code) needs them. Good. But the
  *same file* has a sibling accessor `pub(crate) fn is_running(&self) -> bool`
  at line 402-405 that reads the same atomic via `&Sender`. If
  production code later wants to ask "is the sender running?" it has two
  paths: read the atomic via `running_arc().load(Acquire)`, or call
  `is_running()`. Both work, but they're equivalent and one of them
  predates the Phase 7c ungating without a doc note tying the two
  together.

**Recommendation**: at the next opportunity, add a one-line cross-doc
to `is_running` mentioning `running_arc()` is the analogous accessor
for the moved-into-spawn case. Phase 7e (which adds async `close`)
will be a natural place to clean this up. Not blocking — just keeps
the surface coherent for the next maintainer.

**Disposition**: Fixed in commit `fabe23b` (fixup! `b696f5d`). Added
cross-doc on both `Sender::is_running` and `Sender::running_arc` so
future maintainers see the two accessors are coherent — both read the
same `running` atomic with `Ordering::Acquire`. The split exists
because `is_running` requires `&Sender` (in-process tests that own the
struct directly) while `running_arc` is the accessor used by callers
that move the sender into a `tokio::spawn` task and need to flip the
flag from outside the spawn.

---

## Nit 1: `drop_aborts_sender_task` test verifies the flag flip but not the abort

- **File**: `src/producer/kafka_producer.rs:903-930`
- **Severity**: Nit
- **Description**: The test name asserts that `Drop` aborts the spawned
  task; the test body only asserts that `running` is `false` after drop.
  But the Drop body explicitly calls `self.sender_running.store(false, …)`
  on every drop path, so the assertion passes regardless of whether
  `JoinHandle::abort()` was called. To actually verify the abort, the
  test would need to capture a counter / completion channel from the
  spawned task before drop and assert it observed shutdown — or capture
  the JoinHandle externally and `tokio::time::timeout(…, handle).await`
  verify it completed.

**Recommendation**: either rename the test to
`drop_flips_running_flag` (truthful), or extend it to verify the task
actually finished. Since `StubKafkaClient::poll` does a 50ms sleep,
the second option is cheap.

**Disposition**: Fixed in commit `62bad53` (fixup! `848f9a1`).
Strengthened the test to verify both halves of the abort contract:
(1) `Drop` flips `sender_running` to `false`, and (2) the spawned
`JoinHandle` actually finishes within a bounded timeout. The test
steals `producer.sender_task` before drop, then `tokio::time::timeout(1s, handle).await`
proves the task ran to completion or was cancelled. Accepts either a
cancelled JoinError (the abort path) or `Ok(())` (the cooperative
running-flag-flip path) — both prove the task actually exited.
