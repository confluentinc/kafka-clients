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

---

# Critic 7 — Phase 7c Round 2 acceptance (archived)

Review window: fixup commits `973218a`, `fabe23b`, `62bad53`, archive
`07d6d6b`, memory `e2f3e1f` on branch `fresh-impl`.

Java references (cross-checked):
- `KafkaProducer.java:454-458` — `config.logUnused()` ordering after
  Sender start.
- `KafkaProducer.java:582-587` — silent `delivery.timeout.ms` bump emits
  `log.warn`.
- `KafkaProducer.java:455` — `NETWORK_THREAD_PREFIX` is the IO-thread
  name; no Tokio analogue without `tracing`.

Build state: `cargo test --lib kafka_producer::` 6/6 pass in 0.01s. Full
suite still 1149/1149 (unchanged from Round 1).

## Per-fixup verifications (summary)

- Suggestion #1 — `NETWORK_THREAD_PREFIX` removed from public API,
  replaced by 10-line module comment explaining the Tokio/`log`
  divergence (`kafka_producer.rs:98-108`). No orphan references.
- Suggestion #2 — silent delivery-timeout bump emits a `log::warn!` at
  `kafka_producer.rs:643-650` byte-equivalent to Java `KafkaProducer.java:584-587`.
- Suggestion #3 — `config.logUnused()` translated as
  `config.inner().log_unused()` after `tokio::spawn(sender.run_loop())`
  at `kafka_producer.rs:508-515`. Order matches Java.
- Suggestion #4 — `Sender::is_running` and `Sender::running_arc` cross-
  reference each other in rustdoc (`sender.rs:402-413, 419-425`).
- Nit #1 — strengthened `drop_aborts_sender_task` to verify both halves
  of the abort contract (running-flag flip + JoinHandle exit within 1s).

All five Round-1 disposition fixups verified against the Round-1
descriptions; archive integrity confirmed; no new defects scanned.

## Round 2 verdict: accepted — Phase 7c ready to close.

---

# Critic 7 — Phase 7d Round 1 review (resolved)

Critic 7. Commits reviewed: `92f79af`, `4d3a950`, `53d8cf4`, `51116c3`,
`34341b2`, `41294f2`. Build state on review: `cargo test --lib` 1159
passing per Actor's report; reviewer did not re-run.

## Verdict: accepted with **0 Blocking, 2 Suggestion, 1 Nit**.

The send path translation is faithful to Java's `doSend`, the
interceptor double-fire was a real bug correctly diagnosed and fixed,
and the new tests genuinely pin the on-error contract. Two minor
divergences below are worth recording — neither breaks the send
contract and both are aligned with project deferral conventions.

## Per-area summary

- **`do_send` step parity**: present. (a) `throw_if_producer_closed`,
  (b) `wait_on_metadata`, (c) remaining-wait recompute, (d) key+value
  serialize with mapped error wrapping, (f) `partition`, (g) headers
  snapshot, (h) `estimate_size_in_bytes_upper_bound`, (i)
  `ensure_valid_record_size`, (j) `accumulator.append`, (l) wakeup-on-
  full, (m) catch-block fan-out — all wired in the same order as Java
  L987–L1080. Step (e) — explicit-partition validation against
  `cluster.partitionsForTopic(topic).size()` — is correctly subsumed by
  `wait_on_metadata` (which Java also relies on; there is no separate
  validator at the Java callsite either; the loop's exit condition
  `partition < partitionsCount` enforces it). Step (k)
  `transactionManager.maybeAddPartition` is correctly gated by an
  `unreachable!()` since Phase 6 plug-in contract pins
  `transaction_manager = None`.
- **Interceptor double-fire fix**: correct and well-targeted. Java's
  catch block at L1058–L1064 fires the user callback *directly*, not
  via `AppendCallbacks.onCompletion` — bypassing the
  `interceptors.onAcknowledgement` re-entry. The Rust catch arm extracts
  `append_cb.user_callback.as_ref()` and fires it without going through
  `AppendCallbacksImpl::on_completion`, then runs
  `interceptors.on_send_error` separately. The new
  `send_returns_record_too_large_and_fires_interceptor_on_send_error`
  test counts `on_acknowledgement(error)` invocations and asserts == 1;
  the doubled-up path would have produced 2.
- **`partition` parity**: matches Java L1476–L1495 exactly.
  Explicit-partition first, user-Partitioner second, key-hash third,
  `UNKNOWN_PARTITION` fallback. Negative-partition rejection as
  `KafkaError::IllegalArgument`. `partitioner_ignore_keys` honoured.
- **`AppendCallbacks` parity**: matches Java L1568–L1626.
  `topic_partition()` priority chain
  (set_partition > record_partition > UNKNOWN) is correct,
  `OnceLock` mirrors Java's `volatile` cache semantics, and
  `on_completion` synthesises a placeholder metadata when the
  accumulator passes `None` (Java L1597–L1599).
- **`wait_on_metadata` parity**: faithful; uses
  `ProducerMetadata::await_update` with a wall-clock-bound deadline
  (so MockTime-based tests still time out). Topic is `add`-ed,
  invalid-topic short-circuits, and the timeout error message is
  Java-verbatim.
- **Zero-copy + no-spawn**: clean. Serialised key/value `Vec<u8>` are
  passed via `as_deref()` → `Option<&[u8]>` into
  `accumulator.append` with no second copy. No `Box::pin` or
  `tokio::spawn` per send.

## Issues

### Suggestion 1 — Catch-block user-callback fires for every error type, not just `ApiException`

- **File**: `src/producer/kafka_producer.rs:840-867` (`do_send`)
- **Severity**: Suggestion (behavior divergence)
- **Java reference**: `KafkaProducer.java:1056-1081`
- **Description**: Java's `doSend` has four distinct catch arms:
  - `ApiException` (L1056) — fires user callback **and** onSendError,
    returns `FutureFailure` (caller sees the error via
    `Future.get()`).
  - `InterruptedException` (L1069), `KafkaException` (L1073),
    `Exception` (L1077) — fires **only** onSendError, then
    *re-throws*. The user does *not* observe a callback fire on these
    paths; they get the synchronous throw on `send()`.

  The Rust translation collapsed all four paths into one match arm
  that fired the user callback for every error. A user who registered
  both a callback AND awaited the `Result` from `send()` would observe
  an error event twice on non-API errors.

**Disposition**: Fixed in commit `67ea5df` (fixup! `53d8cf4`).
Added `KafkaError::is_api_exception()` classifier that returns `true`
for variants whose Java counterpart is a subclass of `ApiException`,
`false` for direct `KafkaException` subclasses (`Serialization`,
`Config`, `Interrupt`, bare `Generic`) and stdlib `RuntimeException`
variants (`IllegalArgument`, `IllegalState`, `UnsupportedOperation`).
The `do_send` catch arm now fires the user callback only when
`err.is_api_exception()`; the interceptor `on_send_error` always fires
(matching all four Java arms). The rustdoc on `do_send` documents the
fan-out table verbatim against Java line numbers. Test pinning: see
`bfba26c` below — `send_does_not_fire_user_callback_for_non_api_exception`
would have failed against the pre-fix behaviour because it reverts to
the `IllegalState` arm where the pre-fix code fired the callback.

### Suggestion 2 — `partitioner.class` config silently ignored

- **File**: `src/producer/kafka_producer.rs:368-374`
  (`new_for_test`, partitioner instantiation)
- **Severity**: Suggestion (regression risk into Phase 7e)
- **Java reference**: `KafkaProducer.java:369-375`
  (`partitionerPlugin = config.getConfiguredInstance(...)`)
- **Description**: The Rust `new_for_test` always set
  `partitioner = None`, even when the user provides
  `partitioner.class = com.example.MyPartitioner` in the config.
  Once Phase 7e wires `new()` to the production NetworkClient, this
  becomes a real silent-drop bug.

**Disposition**: Fixed in commit `0e2dd8e` (fixup! `b696f5d`).
`new_for_test` now emits a `log::warn!` whenever the user supplied a
non-default `partitioner.class` so the deferral is operator-visible.
Phase 7e carryover note in `NOTES.md` strengthened to make explicit
that Phase 7e MUST either reject `partitioner.class` outright or
route it through the builder API once `new(props)` becomes
production. The warn becomes a hard rejection (or routing) at that
point.

### Nit 1 — `set_read_only(record.headers())` not translated

- **File**: `src/producer/kafka_producer.rs:929-942` (`do_send_inner`)
- **Severity**: Nit
- **Java reference**: `KafkaProducer.java:1026, 1084-1088`
- **Description**: Java calls `setReadOnly(record.headers())` (L1026)
  to flip the user's `RecordHeaders` to read-only after `partition()`,
  preventing a misbehaving interceptor (or the user) from mutating
  them between `partition()` and `accumulator.append()`.

**Disposition**: Fixed in commit `67ea5df` (fixup! `53d8cf4`).
Replaced the existing inline comment with an explicit rustdoc-style
block explaining that the Rust ownership model provides the same
guarantee for free: `do_send`'s receiver is `record: ProducerRecord<K, V>`
(by value — moved out of `Producer::send_with_callback`'s intercepted
record), so the user no longer holds any reference to the original
`Headers`. Past this point the only reader of `record.headers()` is
`do_send_inner` itself, and the headers `Vec` passed to
`accumulator.append` is a shallow `cloned()` collection. Interceptors
run before the record reaches `do_send` (in
`Producer::send_with_callback`), so the read-only flag has no Rust
counterpart to defend against. No `set_read_only` API was added.

### Test-pinning of Suggestion 1 (commit `bfba26c`, fixup! `34341b2`)

- New test `send_does_not_fire_user_callback_for_non_api_exception`
  (`kafka_producer.rs`) closes the producer (sets `sender_running =
  false`) before `send_with_callback`, triggering
  `KafkaError::IllegalState` from `throw_if_producer_closed`. Asserts:
  - The error is `IllegalState` and `!err.is_api_exception()`.
  - The user callback fires **0 times** (Java `catch (Exception)` arm
    rethrows without invoking the callback).
  - The interceptor's `on_acknowledgement(error)` fires **exactly 1
    time** (Java `catch (Exception)` arm still fires `onSendError`).
- Existing test `send_returns_record_too_large_and_fires_interceptor_on_send_error`
  strengthened to also register a user callback and assert it fires
  exactly once for `RecordTooLarge` (an `ApiException` subclass —
  Java `catch (ApiException)` arm DOES invoke the user callback).
  Also asserts the error message contains `max.request.size` per
  DoD #3.

## Definition-of-done check

- All listed Java methods translated (`doSend`, `partition`,
  `AppendCallbacks`, `waitOnMetadata`, `throwIfProducerClosed`,
  `ensureValidRecordSize`).
- 11 send-path tests (10 from Round 1 + 1 added in Round 1 fixup) with
  citations to the corresponding Java `@Test`. Error-message content
  asserted where Java specifies it (DoD #3 honoured).
- `tokio::spawn` only at construction, not per send. No
  `Box<dyn Future>` per send. No clones on serialised bytes.
- No `TODO`/`FIXME` introduced.

## Round 1 verdict (resolved)

**Accepted.** All Suggestions and the Nit fixed in Round-1 fixup
commits. Phase 7d ready to close.

---

# Round 1 — Phase 7e (resolved blocks)

Review window: commits `3b39142`, `b7d5425`, `3ce4b7f`, `e3ca2d3`,
`38b0439`, `36f55bc`, `9b24291` on branch `fresh-impl`.

Java references (Apache Kafka 4.2):
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java`
- `kafka/clients/src/test/java/org/apache/kafka/clients/producer/KafkaProducerTest.java`

Round 1 verdict: **0 Blocking, 2 Suggestion, 1 Nit.** Suggestion #1
and Nit #1 are resolved here in Round 1 fixups. Suggestion #2
(`testFlushCompleteSendOfInflightBatches` 50-record concurrency
test) remains in `COMMENTS.7.md` and is deferred to Phase 7f, where
`MockClientImpl` will be hoisted from `pub(super)` to `pub(crate)`
and the multi-record flush test can land alongside the broader
`KafkaProducerTest` translation.

## Issue: graceful close timeout-elapsed branch leaves the JoinHandle
- **File**: `src/producer/kafka_producer.rs:1207-1236`
- **Severity**: Suggestion
- **Java Reference**: `KafkaProducer.java:1432-1446`
- **Description**: In `close_inner` graceful path, when `tokio::time::timeout(timeout, handle).await` elapses (the `Err(_elapsed)` arm), the `handle` is consumed by `tokio::time::timeout` and cannot be re-awaited. The actor sets `force_close=true` and relies on the `Drop` impl's `handle.abort()` to terminate the spawned task on producer drop. Java instead performs `sender.forceClose()` followed by an unbounded `ioThread.join()` (line 1441) inside the same `close()` call, so by the time `close()` returns, the IO thread is guaranteed terminated. The Rust translation returns `Ok(())` while the spawned task may still be running (it observes `force_close` only on the next yield). Tests do not exercise this branch (no test passes a graceful-close timeout shorter than the Sender drain duration). Functionally bounded — the next `Drop` does abort the task — but a `close().await` returning `Ok(())` while the IO task is still live diverges from Java's "ioThread terminated" post-condition.
- **Expected**: Either (a) restructure to use `select!` between a `tokio::time::sleep(timeout)` arm and the `JoinHandle` arm so the elapsed path can still abort + await the handle, or (b) document the divergence in `close_inner` rustdoc and `NOTES.md` as an explicit Phase 8 carry-over.
- **Actual**: The handle is consumed; force-close is set; the function returns `Ok(())` without ensuring the spawned task has terminated.

**Disposition**: Fixed in commit `5e3c2b5` (fixup! e3ca2d3).
Restructured with `tokio::select!` per option (a). Arm 1 awaits
`&mut handle` (cancellation-safe — losing arm drops the borrow,
not the task). Arm 2 sleeps the timeout; on elapse it flips
`force_close`, wakes the loop, calls `handle.abort()`, and `await`s
the cancelled handle so close-return implies task-terminated. The
`tokio::time::sleep` arm itself is trivially cancellation-safe.
Added `close_with_short_timeout_force_closes_and_waits_for_termination`
(test count 1175 → 1176) which:

1. Wires an undrained batch (StubKafkaClient never sends), so the
   graceful drain spins forever.
2. Adds a `polls` counter on `StubKafkaClient` (incremented on
   every `poll` entry) so tests can directly observe whether the
   spawned task is still driving.
3. Calls `close_with_timeout(50ms)`. The elapsed arm fires.
4. Asserts close returns within 2s, `force_close=true`,
   `accumulator.is_closed()`, and the polls counter does not
   advance during a 200ms post-close window.

The test would fail on pre-fix code: removing `handle.abort()` +
`handle.await` from the elapsed arm would let the spawned task
keep polling on its 50ms cadence; the 200ms post-close snapshot
would not equal the at-return snapshot. Genuine regression pin.

The Phase 8 carry-over note in `NOTES.md` is updated to record
this fix (the Java-divergence is now resolved within Milestone-1).

## Issue: nit — `partitioner_class_fqcn_round_robin_resolves` body is effectively empty
- **File**: `src/producer/kafka_producer.rs:2636-2671`
- **Severity**: Nit
- **Java Reference**: n/a
- **Description**: The test constructs a producer with `partitioner.class=FQCN` and only checks `producer.partitioner` is `Some`. The intent (per the rustdoc) was to also exercise the partitioner via `cluster`, but the body ends with `let _ = partitioner; let _ = cluster;` after fetching them. This makes the test functionally identical to `partitioner_class_simple_name_round_robin_resolves` (same construction-only assertion). The end-to-end behavior is covered by `partitioner_class_round_robin_distributes_across_partitions`, so coverage is fine, but the FQCN test rustdoc oversells: it claims "downcast via Arc::as_ref()" but never calls into the partitioner.
- **Expected**: Either remove the unused `cluster` fetch / `let _` lines and simplify the rustdoc, or add a single `partitioner.partition(...)` call so the FQCN-vs-simple-name test does something different.
- **Actual**: Two near-identical tests after the rustdoc is stripped.

**Disposition**: Fixed in commit `c448b6e` (fixup! 3b39142).
Took option (b): rewrote the test to populate metadata for a
3-partition topic and dispatch through `Partitioner::partition` on
the FQCN-resolved instance, asserting the returned partition is
in [0, 3). The FQCN test now exercises the trait surface and is no
longer a near-duplicate of the simple-name test. Rustdoc rewritten
to accurately describe what the test does and how it differs from
its siblings. End-to-end distribution behaviour remains pinned by
`partitioner_class_round_robin_distributes_across_partitions`.

---

# Round 1 — Phase 7f review (resolved)

Review window: commits `11c83cb` (MockClientImpl visibility hoist),
`c5109d0` (KafkaProducerTest non-tx/non-metrics/non-telemetry
translations), `d980c9a` (agent-memory) on branch `fresh-impl`.

Java reference: `kafka/clients/src/test/java/org/apache/kafka/clients/producer/KafkaProducerTest.java`
(2952 LOC, 82 annotated test methods — `@Test` + `@ParameterizedTest`).

## Scope

- 24 Java tests translated + 1 cross-reference sentinel
  (`test_interceptor_partition_set_on_too_large_record_already_translated`).
- 56 Java tests skipped, with rationale.
- 1 production fix: `KafkaProducer::close_inner` calls
  `self.metadata.close()` in both arms (graceful + force-close).
- Test count 1176 → 1201 (+25). Lint, format-check clean.

## Verdict: **accepted with 4 Suggestions; no Blocking issues.**

Phase 7 overall is **ready to close** once the suggestions are
either fixed or explicitly archived as "deferred / acknowledged".

## Issue: Skip-block missing one Java @Test (`closeShouldBeIdempotent`)

- **File**: `src/producer/kafka_producer.rs`
- **Severity**: Suggestion
- **Java Reference**: `KafkaProducerTest.java:1156` (`closeShouldBeIdempotent`)
- **Description**: I enumerated the 82 annotated Java test methods,
  cross-referenced against the 25 translated + 56 skipped lines.
  All but one accounted for. `closeShouldBeIdempotent` (Java line 1156)
  is *already* covered by Phase 7e's `close_is_idempotent`
  (`kafka_producer.rs:3074`), but the Phase 7f skip block does not
  cite it, so the actor's claim "every Java @Test is accounted for in
  either a translation above or one of these skip lines" is one short.
- **Expected**: Add one line to the skip block (or use the same
  cross-reference sentinel pattern as
  `test_interceptor_partition_set_on_too_large_record_already_translated`)
  so the audit trail closes cleanly:
  `closeShouldBeIdempotent (line 1156) — already covered by close_is_idempotent (Phase 7e)`.
- **Actual**: The skip block jumps from `closeWithNegativeTimestampShouldThrow`
  (line 1164) to nothing for line 1156. Reviewers walking the skip
  block top-to-bottom will not find this entry.

**Disposition**: Fixed in commit `3253e76` (fixup! c5109d0).
Added a new "SKIP — already covered by Phase 7e" sub-heading to the
skip block in the `kafka_producer.rs` test module:

```
// SKIP — already covered by Phase 7e:
//  * closeShouldBeIdempotent (line 1156) — COVERED by Phase 7e
//    `close_is_idempotent` — not duplicated here.
```

The audit trail now closes cleanly: the 82 annotated Java tests are
fully accounted for via 25 translations + 1 cross-reference sentinel
+ 56 explicit skip entries.

## Issue: `metadata.close()` Phase 8 carry-over not in NOTES.md

- **File**: `design/history/Milestone-1/Phase-7/NOTES.md`
- **Severity**: Suggestion
- **Java Reference**: `Sender.java:298` → `NetworkClient.java:1325-1326`
  (Java's `client.close()` → `DefaultMetadataUpdater.close()` →
  `metadata.close()` chain)
- **Description**: The actor's commit message and source-comment
  rustdoc for the new `self.metadata.close()` calls
  (`kafka_producer.rs:1213` and `:1286`) explicitly mark this as a
  Phase 8 carry-over: "Phase 8 will move this call back into the
  equivalent `client.close()` path once `DefaultMetadataUpdater` is
  translated." But NOTES.md has no new Phase 7f section that
  consolidates this carry-over alongside the existing Phase 7e ones.
  Phase 8 reviewers reading only NOTES.md will not see the
  obligation; they have to grep the source comments.
- **Expected**: Add a "Phase 7f — landed" section to NOTES.md with at
  minimum one sub-bullet:
  `metadata.close() inlined in close_inner; move back into client.close() chain once DefaultMetadataUpdater lands (Phase 8)`.
- **Actual**: NOTES.md ends at "Phase 7e Round 1 carry-overs" with no
  Phase 7f section. The other claimed Phase 8 carry-overs
  (closeQuietly chain, partitioner reflective, metric/interceptor
  reflective) are visible in the existing Phase 7d/7e blocks; only
  the new metadata.close() one is not consolidated.

**Disposition**: Fixed in commit `3253e76` (fixup! c5109d0).
Added a "Phase 7f — landed (3 commits)" section to
`design/history/Milestone-1/Phase-7/NOTES.md` summarising both
landed commits (`11c83cb` MockClientImpl hoist, `c5109d0` test
translation + `metadata.close()` production fix) and listing two
explicit Phase 7f → Phase 8 carry-overs:

1. The `metadata.close()` ordering: move the call back into the
   `client.close() → DefaultMetadataUpdater.close() → metadata.close()`
   chain when `DefaultMetadataUpdater` is translated, and remove the
   explicit `metadata.close()` calls from `KafkaProducer::close_inner`.
2. The 50-record flush-test fidelity rewrite (Suggestion #4) is also
   consolidated in the same block.

## Issue: `metadata.close()` in graceful-close arm runs BEFORE run-loop drain

- **File**: `src/producer/kafka_producer.rs:1213`
- **Severity**: Suggestion
- **Java Reference**: `KafkaProducer.java:1417-1429` (graceful close
  sequence: `sender.initiateClose()` → `ioThread.join(timeout)` → on
  termination, the run-loop's `client.close()` calls
  `metadata.close()`)
- **Description**: In Java's graceful close, `metadata.close()` runs
  *after* the run-loop drains naturally — not before. A `send`
  blocked in `wait_on_metadata.await_update` may receive a successful
  metadata response if the Sender ticks once more before exiting. In
  the Rust translation, `metadata.close()` is called *before* the
  `tokio::select!` over the JoinHandle, so any in-flight
  `await_update` is aborted immediately even if the run loop would
  have produced a metadata response within the deadline. For the
  *force-close* arm (Duration::ZERO) this is correct behavior; for
  the *graceful* arm it's a mild divergence.
- **Expected**: One of:
  1. Move `self.metadata.close()` in the graceful arm to *after* the
     `select!` block (i.e. after the run-loop terminates cleanly OR
     was aborted on deadline-elapse), so blocked sends still have a
     chance to receive metadata during the graceful window. Document
     why the call still appears in the deadline-elapsed branch (parity
     with force-close).
  2. Acknowledge this as a deliberate Milestone-1 simplification in
     NOTES.md and the rustdoc, with a Phase 8 carry-over to wire the
     call into the eventual `DefaultMetadataUpdater::close()` chain so
     the ordering matches Java automatically.
- **Actual**: The current graceful path calls `metadata.close()` at
  line 1213 — before `wakeup()`, before the JoinHandle await. A
  `send` mid-`await_update` aborts on the metadata-close, even if the
  graceful timeout has not elapsed and the run loop could have
  completed the metadata fetch.
- **Note**: The pinning test
  (`test_close_when_waiting_for_metadata_update`) only exercises the
  Duration::ZERO force-close arm, so the graceful-arm divergence is
  unobserved by tests. A targeted test on the graceful arm would
  surface this; without one, treat as Suggestion not Blocking.

**Disposition**: Fixed in commit `3253e76` (fixup! c5109d0). Took
option (2): documented as a deliberate Milestone-1 simplification.
Expanded the existing rustdoc block at the `metadata.close()` call
site in `close_inner` (graceful arm) with an "ORDERING DIVERGENCE vs
Java" sub-paragraph that explains the divergence, why it is
functionally equivalent in Milestone-1 (both paths set the metadata
closed flag and `wait_on_metadata` unblocks either way), and points
at `Phase-7/NOTES.md` "Phase 7f carry-overs" for the lift point.
Restoring Java's exact ordering is part of the Phase 8
`DefaultMetadataUpdater` translation (Suggestion #2's NOTES.md
carry-over).

## Issue: 50-record flush test bypasses `producer.send()`

- **File**: `src/producer/kafka_producer.rs:4624`
  (`test_flush_complete_send_of_inflight_batches_50_records`)
- **Severity**: Suggestion
- **Java Reference**: `KafkaProducerTest.java:1191-1198` —
  `producer.send(new ProducerRecord<>("topic", "value" + i))`
- **Description**: The Java test sends 50 records via
  `producer.send()`, exercising the full hot path (interceptors,
  partitioner, serializers, `do_send`, `wait_on_metadata`, accumulator
  append). The Rust translation calls `producer.accumulator.append()`
  directly with a manually-fetched cluster snapshot. The flush
  semantics being tested ("`flush()` blocks until in-flight `send`s
  complete") are still pinned — the futures are produced by
  `accumulator.append`, the spawned Sender drains them, and the
  staged MockClient responses ack them — but the *integration* of
  `flush` with the public `send` surface is not what this test
  verifies.
- **Expected**: Either swap to `producer.send(record).await` for each
  of the 50 sends so the test mirrors the Java contract end-to-end,
  OR add a rustdoc note explaining that the bypass is deliberate (and
  why — e.g. partition-determinism for the staged response set) so
  reviewers don't think the public surface is being tested.
- **Actual**: The test rustdoc/comments justify the bypass as "to
  keep the test deterministic w.r.t. partition assignment"
  (lines 4732-4734) but doesn't explicitly tag it as a fidelity
  divergence vs the Java test.

**Disposition**: Fixed in commit `3253e76` (fixup! c5109d0). Added
a DEVIATION block to the test rustdoc explaining that Rust uses
`accumulator.append()` directly to avoid coordinating per-record
MockClient broker-response ticks (each `producer.send().await`
would otherwise serialize against a Sender tick), while still
proving the Phase 7e `flush()` semantic
(`begin_flush() → await_flush_completion()`). Phase 8 may rewrite
the test to drive the public `send()` surface end-to-end once the
`MockClient` harness has a multi-record helper. This Phase 8
follow-up is also recorded in the NOTES.md "Phase 7f carry-overs"
block (Suggestion #2's fix).

## Verified-good areas (no findings)

- **MockClientImpl visibility hoist (`11c83cb`)**: pure
  `pub(super)` → `pub(crate)` flip on the `tests` submodule and on
  every `MockClientImpl` method. No production logic changed; no
  symbols leaked to non-test builds (`#[cfg(test)]` gating preserved).
- **`build_produce_response_for_test` cross-module helper**: thin
  `pub(crate)` wrapper around the existing local
  `build_produce_response`. No struct fields exposed; no new
  invariants pinned. Will not be a maintenance burden in Phase 8.
- **Production fix correctness (graceful path aside)**: the
  force-close arm's `metadata.close()` is genuinely needed and
  test-pinned by `test_close_when_waiting_for_metadata_update`
  (line 4788). I empirically confirmed the test passes; without the
  fix, the spawned `send` would hang on `wait_on_metadata` until
  `max.block.ms = 60_000` elapsed and the surrounding 5-second test
  bound would fail. `Metadata::close()` is idempotent (sets a flag
  and calls `notify_waiters()`, both idempotent).
- **Drop-tracking pattern for serializer/interceptor/partitioner
  close-counts**: `Arc<AtomicUsize>` → `Drop` impl is the correct
  Rust analogue of Java's static-counter idiom and is consistent
  across `testSerializerClose`, `testInterceptorConstructClose`,
  `testPartitionerClose`.
- **`testNullTopicName` / `testPartitionsForWithNullTopic` skip
  rationale**: Java raises NPE/IllegalArgument on null. Rust's
  `&str` / `impl Into<Arc<str>>` cannot represent null at all — the
  skip is the correct adaptation. The Rust translation does not
  silently substitute "empty string" for "null" (which would have
  tested a different code path); it skips with a justified rationale.
- **`testHeadersSuccess` / `testHeadersFailure` deviation**: rustdoc
  clearly documents that Rust's by-value `send(record)` makes Java's
  post-send `record.headers().is_read_only()` check a compile-time
  invariant. The round-trip portion of each test (record with
  headers traverses send + accumulator + mock-broker ack) is still
  exercised.
- **Skip-rationale block structure**: every entry cites a Java line
  number; entries are grouped by skip reason (transactional /
  metrics-telemetry / null-rejection / Duration-non-negative /
  reflective-load / other). Phase 8/9 reviewers walking the block
  can audit the exhaustiveness check at a glance.
- **Memory commit (`d980c9a`)**: touches only
  `.claude/agent-memory/actor-executor/`. No CLAUDE.md or
  `.claude/rules/` edits.

## DoD sign-off

- All Phase 7f translations green.
- Production fix (`metadata.close()` in close path) genuinely needed,
  test-pinned, idempotent.
- One Java @Test (`closeShouldBeIdempotent`) missing from the
  skip-block audit (already covered by `close_is_idempotent` from
  Phase 7e — Suggestion 1).
- Phase 7f Phase-8 carry-over (`metadata.close()` lift point) not in
  NOTES.md — Suggestion 2.
- Graceful-close `metadata.close()` ordering vs Java — mild
  divergence in untested arm — Suggestion 3.
- 50-record flush test bypasses `producer.send` — fidelity
  Suggestion 4.

## Round 1 verdict: accepted with 4 Suggestions — Phase 7 overall ready to close.

All four Suggestions resolved in fixup commit `3253e76`
(fixup! c5109d0).
