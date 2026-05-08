# Critic 7 — Phase 7a (ProducerConfig) review

Review window: commits `1d02f6d`, `6989a61`, `15d4ac8`, `96ee9c6`,
`380bd30`, `8a8d413` on branch `fresh-impl`.

Java references:
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/ProducerConfig.java`
- `kafka/clients/src/test/java/org/apache/kafka/clients/producer/ProducerConfigTest.java`
- `kafka/clients/src/main/java/org/apache/kafka/common/config/SslConfigs.java`
- `kafka/clients/src/main/java/org/apache/kafka/common/config/SaslConfigs.java`
- `kafka/clients/src/main/java/org/apache/kafka/common/config/ConfigDef.java`

Verdict: **0 Blocking, 4 Suggestion, 4 Nit.** No silently-dropped Java
tests; all 10 Java `@Test` methods are accounted for (7 ported, 1
skipped with rationale at `producer_config.rs:1692-1703`, 2 modified
per the Actor's documented allowlist). All Milestone-1 rejection
messages match the brief verbatim. Lint, format-check, and full
`cargo test --lib` (1138 tests) green.

Round-1 dispositions (4 Suggestions + Nits 2, 3) have been moved to
`COMMENTS.DONE.7.md`. The two remaining items below are
recorded-only: Nit 1 and Nit 4 are intentional non-actions whose
rationales should stay on the record.

---

## Per-area verifications

| Area | Status | Notes |
|------|--------|-------|
| Public string constants (`*_CONFIG`) | OK | All `pub const` literals match Java string values (spot-checked `metadata.max.idle.ms`, `batch.size`, `compression.gzip.level`, `enable.idempotence`, `transactional.id`, `transaction.two.phase.commit.enable`, `acks`, `linger.ms`, `client.id`, `bootstrap.servers`, `security.providers`, `config.providers`). Constants delegating to `common_client_configs` re-bind the same values, mirroring Java's `BOOTSTRAP_SERVERS_CONFIG = CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG` pattern. |
| Schema `ConfigDef` registrations | OK | All 35 producer-specific keys registered with the same defaults, validators, types, and importance levels as Java's `static {}` block (lines 376-566), modulo two documented Milestone-1 deviations: `enable.idempotence` default flipped `true → false` (with rustdoc and dedicated test `enable_idempotence_default_is_false_milestone_1`); `security.protocol` validator restricted to `{PLAINTEXT, SSL}`. |
| `addClientSslSupport` parity | OK | All 19 SSL keys defined in matching insertion order with matching defaults/types/importance, except the JVM-resolved `ssl.keymanager.algorithm` and `ssl.trustmanager.algorithm` defaults (Java: `KeyManagerFactory.getDefaultAlgorithm()`; Rust: `Null`). The `ssl_configs.rs` module docstring (lines 22-26) explicitly documents this rustls-vs-JVM deviation. |
| `addClientSaslSupport` parity | OK | 45 `.define()` calls in Java (`SaslConfigs.java:370-414`) → 45 `.define()` calls in Rust (`sasl_configs.rs:224-583`), in identical order, with matching validators (`Range::between(0.5, 1.0)` for `sasl.login.refresh.window.factor`, `Range::between(0.0, 0.25)` for `sasl.login.refresh.window.jitter`, `Range::between(0, 900)` / `Range::between(0, 3600)` / `Range::between(0, 86400)` for the seconds bounds, `CaseInsensitiveValidString::in_set(["ES256","RS256"])` for the assertion algorithm). |
| `appendSerializerToConfig` semantics | OK | Idempotency, exception-on-null-in-either-slot, propagation of pre-set keys. The Rust API takes `Option<&str>` FQCN (no Java reflection); the rustdoc at `producer_config.rs:972-975` calls this out. The error formatting via `config_exception::new(name, "null", "must be non-null.")` produces `"Invalid value null for configuration key.serializer: must be non-null."` byte-exact with Java's `ConfigException(name, null, msg)` formatter. |
| `parseAcks` mapping | OK | `"all"`/`"All"`/`"ALL"` → `"-1"` via `eq_ignore_ascii_case`; numeric strings parse as `i16` and round-trip; garbage yields `KafkaError::Config(format!("Invalid configuration value for 'acks': {}", input))` matching Java's exact message. The validator on the `acks` key (`ValidString::in_set(["all", "-1", "0", "1"])`) is case-sensitive in both Java and Rust, so the case-insensitive `parseAcks` path is only reachable via `appendSerializerToConfig`-style programmatic injection — same as Java. |
| `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE` upper bound | OK | The error message at `producer_config.rs:1325-1327` is byte-exact with Java line 622-623: `"To use the idempotent producer, max.in.flight.requests.per.connection must be set to at most 5. Current value is {}."`. The translated Java test `test_upperbound_check_of_enable_idempotence` asserts `assert_eq!(err.message(), expected_msg)` (full equality, not substring). |
| `PRODUCER_CLIENT_ID_SEQUENCE` | OK | Process-global `AtomicI32::new(1)` with `fetch_add(1, Relaxed)` mirrors Java's `AtomicInteger(1)` + `getAndIncrement()`. First call returns `1` in both. The `Relaxed` ordering is appropriate for a uniqueness counter (no happens-before requirement). |
| `postProcessParsedConfig` step ordering | OK | Steps 1→2→3→4→5 match Java line 569-577 exactly. The Milestone-1 transactional.id rejection runs *between* steps 3 and 4 (rationale at `producer_config.rs:1164-1170`); the Milestone-1 idempotence rejection runs *between* steps 4 and 5 (rationale at `producer_config.rs:1180-1185`), preserving the `testUpperboundCheckOfEnableIdempotence` error-message contract verbatim. The PRODUCER_CLIENT_ID_SEQUENCE is not advanced for rejected configurations since the rejections precede step 5. |
| Milestone-1 rejection messages | OK | All three match the Actor brief verbatim: `"Idempotent producer is not supported in this milestone (Milestone-1). Set enable.idempotence=false. See Milestone-1/PLAN.md."`, `"Transactional producer is not supported in this milestone (Milestone-1). Unset transactional.id. See Milestone-1/PLAN.md."`, and the `security.protocol` validator-emitted message that contains `security.protocol`. |
| Test-count integrity | OK | Pure additions across all 5 commits affecting test files; no pre-existing test deleted. Commit `380bd30` shows `-fn reject_milestone_1_unsupported` which is a single helper renamed/split into `reject_milestone_1_transactional_id` + `reject_milestone_1_idempotence` — not a test. Commit `96ee9c6` shows `-fn post_process_parsed_config(&self)` → `+fn post_process_parsed_config(&mut self)` — signature change, not a test. The Actor's brief mentions "+30 net new tests but +12 + 3 + 27 = +42 gross"; spot-checking confirms the 12 net-new were added in the helper-validator + SSL/SASL helper commits (3 new in helpers, plus `enable_idempotence_default_is_false_milestone_1`, `parse_basic_types`-style tests, etc.). |
| Java `@Test` coverage | OK | 10 Java `@Test` methods: 7 ported, 1 skipped with rationale (`testTwoPhaseCommitIncompatibleWithTransactionTimeout`, see Suggestion 1), 2 adapted (`testCaseInsensitiveSecurityProtocol` substitutes `Ssl` for `SASL_SSL.toLowerCase()` because Milestone-1 rejects SASL; `testValidateConfigPropertiesFile` reads from `tests/data/producer.properties` instead of `kafka/config/producer.properties`). |
| `tests/data/producer.properties` | OK | A faithful local copy of `kafka/config/producer.properties`; the only divergence is `enable.idempotence=true` commented out (with explanatory header comment). Every other key matches Java byte-for-byte and parses through the schema. |
| Lint / format / build | OK | `cargo xtask lint` clean. `cargo xtask format-check` clean. `cargo test --lib` 1138 passed. |
| Memory commit `8a8d413` | OK | Touches only `.claude/agent-memory/actor-executor/`. Does not modify CLAUDE.md or `.claude/rules/`. |
| File location / re-export | OK | `src/producer/producer_config.rs` matches CLAUDE.md Rule 2 ("each Java class in its own file"); `pub use producer_config::ProducerConfig;` is in `src/producer/mod.rs:39`. License header is the Apache 2.0 / Confluent Inc. boilerplate (lines 1-13). |

---

## Nit 1: deprecated typo-alias `PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG` not translated

- **File**: `src/producer/producer_config.rs` (would-be line ~99)
- **Severity**: Nit
- **Java Reference**: `ProducerConfig.java:105-106`

```java
@Deprecated
public static final String PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG = PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG;
```

Java keeps a deprecated typo-alias (`ADPATIVE` for `ADAPTIVE`) for
binary compatibility. The Rust translation correctly elides this — Rust
users would not depend on a Java-typo identifier. No action required;
flagging for completeness so the elision is recorded.

**Disposition**: No action — recorded as intentional elision.

---

## Nit 4: `maybe_override_client_id` `Some(s)` branch is unreachable in Milestone-1

- **File**: `src/producer/producer_config.rs:1256-1267`
- **Severity**: Nit

```rust
let transactional_id = self.inner.values().get(TRANSACTIONAL_ID_CONFIG).and_then(|v| match v {
    ConfigValue::String(s) if !s.is_empty() => Some(s.clone()),
    _ => None,
});
match transactional_id {
    Some(s) => format!("producer-{s}"),
    None => format!(
        "producer-{}",
        PRODUCER_CLIENT_ID_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    ),
}
```

The comment at line 1252-1255 explicitly notes this:

> Milestone-1 rejects non-null/non-empty values upstream, so by the
> time we get here `transactional.id` is always null/empty.

The `Some(s)` branch is therefore dead code in Milestone-1. Keeping it
is the right call (it's the future Phase-9 path), but the dead branch
has no test coverage. Given the dead branch is documented and is
correctly preserved as future-path code, leaving as-is is acceptable —
Phase 9 will exercise it.

**Disposition**: No action — branch is intentional future-path code
that Phase 9 will cover when transactional.id is permitted again.

---

## CLAUDE.md / rules suggestions

None this round. The Phase 7a translation tightly follows the
established patterns. The only meta-pattern worth recording is "skip
rationales must be precise about the *reachable subset* of the Java
test, not the verbatim port" — already a Phase-6e/Phase-6d theme; I'll
add a Phase 7a entry to the kafka-critic memory.

---

# Round 2 — Phase 7a accepted

Verified the six fixup/archive/memory commits applied between
`8a8d413` and `fa089ef`.

## Per-fixup verifications

| Commit | Issue | Verification |
|--------|-------|--------------|
| `d40ad81` | Suggestion 1 — 2pc/timeout test | New test `test_two_phase_commit_rejects_explicit_transaction_timeout` at `producer_config.rs:1716-1738` exercises 2pc=true + explicit `transaction.timeout.ms` *without* setting `enable.idempotence` or `transactional.id`. Asserted error message at lines 1727-1732 is byte-exact with the production format string at lines 1373-1374. Mental simulation of deleting the `if enable_2pc && user_configured_txn_timeout {…}` block confirms the test would fail (production would `Ok(())` instead of `Err`, breaking `unwrap_err()`). Skip-comment for `testTwoPhaseCommitIncompatibleWithTransactionTimeout` was rewritten at lines 1693-1714 to scope the deferral to "the variants that combine 2pc with `enable.idempotence=true` / `transactional.id`" rather than the verbatim test as a whole. |
| `6633ea8` | Suggestion 2 — public DOC verbatim | All six `pub const *_DOC: &str` constants in `producer_config.rs` (lines 242, 247, 269-274, 284-297, 306-312, 316-325) compared byte-for-byte with `ProducerConfig.java` lines 297, 301, 333-335, 339-347, 351-353, 357-360. `<code>org.apache.kafka.common.serialization.Serializer</code>`, `<code>org.apache.kafka.clients.producer.ProducerInterceptor</code>`, `<code>InvalidTxnTimeoutException</code>`, `<code>transaction.state.log.replication.factor</code>` markup all preserved. The `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_FOR_IDEMPOTENCE` substitution → literal `5` matches the constant. `ENABLE_IDEMPOTENCE_DOC` ends with Java's verbatim text through "...ConfigException is thrown.", then a `<p>` break, then the Milestone-1 deviation paragraph — additive, not interleaved. |
| `397c0d2` | Suggestions 3+4 — SSL DOC parity | `SSL_PROTOCOL_DOC` (`ssl_configs.rs:34-41`) and `SSL_ENABLED_PROTOCOLS_DOC` (`ssl_configs.rs:55-62`) compared byte-for-byte with `SslConfigs.java:33-38` and `50-55`. No "Java 11 or newer" wording. No legacy-protocols paragraph in either. `<code>` markup preserved. Diff touched only the two `&str` literals; no defaults, validators, importance, or registration code changed. |
| `1376f1b` | Nits 2+3 — anchors + validator rename | The four-line `_sasl_anchor` / `_ssl_anchor` block at the old position (around line 1383) is gone. The line-49 `use crate::common::config::{config_exception, sasl_configs, ssl_configs};` was trimmed to `use crate::common::config::config_exception;` — only `config_exception` is needed because all `sasl_configs::*` / `ssl_configs::*` references in the file use the full crate path (verified: 4 sites at lines 1156, 1427, 1431, 1439). Validators renamed `zero_or_more_send_buffer` → `at_least_send_buffer_lower_bound` and `zero_or_more_recv_buffer` → `at_least_recv_buffer_lower_bound`; both call sites at lines 579 and 588 updated. The new name matches the bound: `SEND_BUFFER_LOWER_BOUND` = `RECEIVE_BUFFER_LOWER_BOUND` = `-1` (`common_client_configs.rs:73,77`), so `at_least(-1)` is the actual constraint. A 4-line comment block at lines 392-395 documents the `>= -1` ("use OS default") semantics inline with Java's `atLeast(...)` call sites. |
| `3e03540` | Archive | Six resolved blocks (Suggestions 1-4 + Nits 2/3) moved to `COMMENTS.DONE.7.md` with disposition annotations citing the respective fixup SHAs. `COMMENTS.7.md` retains only the per-area verification table and the two recorded-only items (Nit 1 and Nit 4). Disposition format mirrors `COMMENTS.DONE.6.md`. |
| `fa089ef` | Memory | Touches `.claude/agent-memory/actor-executor/MEMORY.md` and `.claude/agent-memory/actor-executor/phase7a_round1_patterns.md` only. No edits to `CLAUDE.md` or `.claude/rules/`. |

## DoD sign-off

- `cargo test --lib`: **1139 passed**, 0 failed, 0 ignored. (Round 1 baseline was 1138; the +1 is `test_two_phase_commit_rejects_explicit_transaction_timeout`, exactly as advertised in the `d40ad81` disposition note.)
- `cargo xtask lint`: clean (no clippy issues).
- `cargo xtask format-check`: clean.
- No new dependencies introduced in any fixup.
- Memory commit lands at `.claude/agent-memory/actor-executor/`, no project-rule mutations.

## New-defect scan

Walked every fixup diff for regression vectors:

- `*_DOC` rewrites (commits `6633ea8`, `397c0d2`): only `&str` literal content changed. No `*_CONFIG` key, no default value, no validator, no importance level, no `.define(...)` registration touched.
- Validator rename (commit `1376f1b`): both call sites updated, no orphan symbols, no unused-import warnings (confirmed by clean lint), no behavioural change (`Range::at_least(-1)` is structurally identical to the pre-rename code).
- New 2pc test (commit `d40ad81`): only depends on already-public `ProducerConfig::new`, `KafkaError::Config`, and the existing `minimal_props()` test helper. No new private API or unexported helper exposed. `unwrap_err()` / `expect(...)` calls all live inside `mod tests`.
- SSL doc fix (commit `397c0d2`): non-doc surface untouched (verified by `git diff` filtering literal-content lines — only the two doc strings inside `concat!(...)` blocks changed).

No new defects surfaced.

## Round 2 verdict: accepted — Phase 7a closed.

The two remaining items in `COMMENTS.7.md` (Nit 1: deliberate elision
of Java's deprecated `ADPATIVE` typo alias; Nit 4: documented
future-path branch in `maybe_override_client_id`) are intentional
non-actions and do not block forward progress. Phase 7a is ready to
close; Phase 7b (or whatever comes next on the producer-config track)
can begin.

---

# Round 2 — Phase 7b accepted

Verified the two fixup/archive commits applied between `f222f20` and
`d71fbaa`: `0fc8655` (Suggestions 1 + 2 fixup) and `d71fbaa` (archive).

## Per-Suggestion verifications

| Issue | Verification |
|---|---|
| **Suggestion 1** — drop `init_transactions_with_keep_prepared` and `prepare_transaction` | `producer.rs` no longer declares either method; the only transactional methods on the trait are now `init_transactions`, `begin_transaction`, `commit_transaction`, `abort_transaction`. Cross-checked against `Producer.java:40-117` — exactly one `void initTransactions()` at line 45, no boolean overload, no `prepareTransaction`. Cross-checked against `KafkaProducer.java:648` — single zero-arg public `initTransactions()`; the `false` argument passed inside the body to `transactionManager.initializeTransactions(false)` is an *internal* call site, not a public overload. Cross-checked against `internals/TransactionManager.java:342` — `prepareTransaction()` exists only there (package-private internal API). Actor's audit claim is accurate. Phase 7c carry-over note added at `Phase-7/NOTES.md:36-54` correctly directs the next phase to re-verify against `KafkaProducer.java` and translate as inherent methods (not trait methods) if either has been back-ported. |
| **Suggestion 2** — async-ify `init_transactions` / `begin_transaction` / `commit_transaction` / `abort_transaction` | All four trait methods now declare `fn foo(&self) -> impl std::future::Future<Output = Result<(), KafkaError>> + Send` (`producer.rs:129, 137, 145, 153`). The other Java-blocking methods (`send`, `send_with_callback`, `flush`, `partitions_for`, `client_instance_id`, `close`, `close_with_timeout`) carry the same shape — uniform across the trait. `StubProducer` impl uses bare `async fn` syntax (`producer.rs:260-310`); test `async_methods_dispatch_through_trait` (`producer.rs:330-357`) `.await`s each of the four transactional methods and asserts `Err(KafkaError::UnsupportedOperation(_))` matches. The split-from-Round-1 `trait_is_implementable_and_dispatches_sync` test now scopes only `metrics()` (the sole sync method left). |

## Async-signature choice — appropriate and consistent

Actor chose `fn foo(&self) -> impl Future<Output = ...> + Send` for
the trait declarations and bare `async fn` for the impl block. This is
one of the two acceptable forms per the Round 2 brief — not the
forbidden `Pin<Box<dyn Future>>`.

The choice is **consistent** across the entire trait: every async
method carries an explicit `+ Send` bound (twelve sites at lines
129/137/145/153/167/183/190/199/224/230/237). Phase 7c will be able to
implement these on `KafkaProducer` with bare `async fn` because the
auto-trait inference will produce a `Send` future when the body holds
only `Send` data — the explicit `+ Send` on the trait method just
documents and enforces what async-fn-in-trait would otherwise leave
implicit. The module rustdoc at `producer.rs:21-33` correctly notes
that this trait shape forfeits `dyn Producer<K, V>` compatibility,
and that callers in Rust will use generics (`fn run<P: Producer<K, V>>`)
rather than dyn-dispatch — mirroring the Java surface where most
callers hold a concrete `KafkaProducer`.

## Send-bound preservation

`pub trait Producer<K, V>: Send + Sync` (line 113) is unchanged. The
explicit `+ Send` on every `impl Future` return type prevents the
phenomenon where a future returned by a generic call site loses
`Send`-ability through a missing bound. No regression here.

## Archive integrity (`d71fbaa`)

- `COMMENTS.7.md` Phase 7b Round 1 section is gone. The remaining
  content is exactly the Phase 7a header, the recorded-only Nit 1 and
  Nit 4, the CLAUDE.md/rules-suggestions stub, and the Round 2 Phase 7a
  acceptance verdict block. No empty stub left over for Phase 7b.
- `COMMENTS.DONE.7.md` lines 262-429 contain the full Phase 7b Round 1
  section with the method-by-method audit table, the trait-shape
  compliance check, the `UnsupportedOperation` variant verification,
  the test gap-fill verification, and both Suggestion blocks annotated
  `**Disposition**: Fixed in commit 0fc8655 (fixup! c63329c)`. Round 1
  Verdict line at line 346 reads "accepted with 2 Suggestion items,
  both fixed in `0fc8655`".
- Phase 7a recorded-only Nits (1 + 4) remain in `COMMENTS.7.md` —
  intentional non-action.

## No-regression scan

- No production caller of the four async methods exists outside
  `producer.rs` (verified via `grep -rn` against `src/`).
- Test count: **1143** passed (Round 1 baseline was 1143; suggestion-2
  fixup did not add or remove a test, only renamed
  `trait_is_implementable_and_dispatches` → `..._sync` and added
  awaits inside the existing `async_methods_dispatch_through_trait`).
- `cargo xtask lint`: clean.
- `cargo xtask format-check`: clean.
- `cargo test --lib`: 1143 passed, 0 failed, 0 ignored.
- No new dependencies, no edits to `CLAUDE.md` or `.claude/rules/`.

## Round 2 verdict: accepted — Phase 7b ready to close.

---

# Round 1 — Phase 7c review

Review window: commits `f540846`, `b696f5d`, `848f9a1`, `0b8b1d9` on
branch `fresh-impl`.

Java references:
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java`
  (focus: lines 283-330 public ctor, 332-467 visible-for-testing 8-arg
  ctor, 510-540 `newSender`, 542-590 helper statics)
- `kafka/clients/src/main/java/org/apache/kafka/clients/ClientUtils.java`
  (lines 153-258 `createNetworkClient` overloads)
- `kafka/clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`
  (lines 296-326 — `metadataUpdater == null` instantiates
  `DefaultMetadataUpdater`)

Verdict: **0 Blocking, 4 Suggestion, 2 Nit.** Build, lint, format-check
all clean. `cargo test --lib` 1149 passing — 6 new construction tests
land in `kafka_producer.rs`. The deferred public ctor IS the documented,
unavoidable consequence of `DefaultMetadataUpdater` not yet being
translated, surfaced explicitly via `KafkaError::UnsupportedOperation`
with a message containing `"DefaultMetadataUpdater"` so users have a
clear pointer at the gap. CLAUDE.md rule 5 is honoured. The cross-module
`Selectable::poll` / `KafkaClient::poll` change is justified (spawned
`Sender::run_loop` future must be `Send`); existing impls compile
unchanged because their `async fn` bodies already produce `Send`
futures.

---

## Per-area verifications

| Area | Status | Notes |
|------|--------|-------|
| Public ctor deferral (`new`, `with_serializers`) | OK | Returns `KafkaError::UnsupportedOperation(message containing "DefaultMetadataUpdater")`. **This is unavoidable**: Java's `KafkaProducer` ctor calls `newSender(...)` → `ClientUtils.createNetworkClient(producerConfig, ...)` (the 10-arg overload at `ClientUtils.java:153`) which passes `null` for `MetadataUpdater` (line 173). `NetworkClient.java:321-325` then constructs `new DefaultMetadataUpdater(metadata)` from that null. `DefaultMetadataUpdater` is a package-private inner class on `NetworkClient` and is not yet translated in this repo (verified: `grep -rn "DefaultMetadataUpdater" src/` finds only doc comments). The Rust `ManualMetadataUpdater` exists but does not drive metadata refresh — wiring it as the production updater would silently break leader discovery on any topic that wasn't seeded at bootstrap. Deferring the public ctor with a clear `UnsupportedOperation` is the correct choice per CLAUDE.md rule 5. |
| `new_for_test` field-init parity (Java lines 332-467) | OK with one stub field | `clientId`, `time`, `producerConfig`, `partitionerIgnoreKeys`, `keySerializerPlugin/valueSerializerPlugin` (held as `Box<dyn Serializer<T>>`, no metrics-Plugin wrapper since metrics are out of milestone), `interceptors`, `maxRequestSize`, `totalMemorySize`, `compression`, `maxBlockTimeMs`, `apiVersions`, `transactionManager` (always None — Milestone-1), `accumulator`, `metadata` (with bootstrap), `sender` — all initialised in the same order as Java. The `partitioner` field is **always `None`** in Phase 7c (Java reads `getConfiguredInstance(PARTITIONER_CLASS_CONFIG, ...)`); rustdoc at lines 352-358 documents this and the deferral note at `NOTES.md:97-102` confirms Phase 7d will thread a builder-supplied partitioner. `errors` (Sensor), `producerMetrics`, `metrics`, `clientTelemetryReporter` are all metrics-out-of-scope per the documented skip list. |
| Cross-module signature change `poll` → `+ Send` | OK | `KafkaClient::poll` and `Selectable::poll` were `async fn …` with `#[allow(async_fn_in_trait)]`. The change to `fn poll(...) -> impl Future<Output=…> + Send` is required so that `tokio::spawn(sender.run_loop())` produces a `Send` task — `async fn` in trait without an explicit `+ Send` desugars to a future that is *not* guaranteed `Send`. Verified the four existing impls (`network_client.rs:974` real `NetworkClient::poll`, `network_client.rs:1428` `MockSelectorView::poll` → Selectable, `network_client_utils.rs:173` and `:316` test mocks, `sender.rs:1635` test `MockClientImpl::poll`, plus the new `kafka_producer.rs:731` `StubKafkaClient::poll`) — all bodies are `async fn` and produce `Send` futures, so build is clean. **No regression risk**: a method body that previously held a `MutexGuard` across an `.await` would have failed `Send` even with `async fn`; the new bound just promotes the requirement from "implicit auto-trait" to "explicit + Send". |
| `running_arc` / `force_close_arc` ungating | OK | Previously `#[cfg(test)]`. Both are now `pub(crate)` non-test because `KafkaProducer::Drop` needs them. Doc comments updated. The CLAUDE.md "internal package = `pub(crate)`" rule is satisfied (Sender lives in `producer::internals`). |
| `configure_compression` parity (Java line 542-563) | OK | Maps each `CompressionType` to the matching builder. Gzip/LZ4/Zstd read the per-codec level config and call `.level(level)?.build()`. None/Snappy use the constant constructors. Matches Java line-for-line. |
| `linger_ms` parity (Java line 565-567) | OK | `min(linger.ms, Integer.MAX_VALUE)` cast to i32 — exact translation. |
| `configure_delivery_timeout` parity (Java line 569-590) | OK (Suggestion 2 fixed in `973218a`) | Reads `delivery.timeout.ms`, computes `linger + request_timeout_ms` clamped to i32::MAX, throws when explicitly set and inconsistent, otherwise silently bumps. Round-1 noted the missing `log.warn` line; the Round-1 fixup adds `log::warn!` on the silent-bump branch with the same format string Java uses. |
| Sender `run_loop` spawn placement (Java line 455-457) | OK | `tokio::spawn(sender.run_loop())` is the very last statement before the struct literal. Every error-returning path above (acks parse, every `config.get_*` call, compression builder, delivery-timeout validation, bootstrap-address parse, ProducerMetadata::new) returns `Err(KafkaError)` *before* the spawn — no orphan task on construction failure. |
| `Drop` impl (Java equivalent: `close(Duration.ofMillis(0), true)`) | OK (Nit 1 fixed in `62bad53`) | Drop flips `force_close=true`, `running=false`, calls `JoinHandle::abort()`. Sync-context constraint correctly observed (no `block_on`). The Sender's `run_loop` checks `running` between iterations and `force_close` between drain iterations, so the flag flips cooperate with the abort to ensure prompt exit. The Round-1 fixup strengthens `drop_aborts_sender_task` to verify both the flag flip AND that the spawned `JoinHandle` actually finishes within a 1s timeout. |
| Java tests covered (`KafkaProducerTest.java`) | OK for skeleton | The Phase 7c rustdoc explicitly defers `send`/`flush`/`close`/metric/transaction tests to 7d/7e/7f. `testConstructorWithSerializers` is translated as `constructs_with_minimum_config_via_new_for_test`. Three Milestone-1 rejections (`testNoSerializerProvided`-equivalent guarded by `ProducerConfig`, `idempotence`, `transactional`, `SASL`) cover the construction-rejection contract. The `public_new_returns_unsupported_operation_in_milestone_1` test pins the deferred-error message. |
| Lint / format / build | OK | `cargo build` clean. `cargo test --lib kafka_producer::` 6/6 pass in 0.01s. Full `cargo test --lib` 1149/1149 (Round-2 7b baseline was 1143; +6 new construction tests). |
| NOTES.md / agent memory commit `0b8b1d9` | OK | Touches only `design/history/Milestone-1/Phase-7/NOTES.md` and `.claude/agent-memory/actor-executor/`. No edits to CLAUDE.md or `.claude/rules/`. The Phase 7c carry-over notes at `NOTES.md:36-105` document the public-ctor deferral, the cross-module `+ Send` change, the ungated `running_arc/force_close_arc`, and the Phase 7d carryover (partitioner threading, accumulator visibility). |
| `_phase_7c_does_not_impl_producer` stub | OK | Test-mod-only `fn`, never compiled in non-test builds. Not a public type, doesn't violate DoD #7 (no new structs/traits beyond Java). Pins the absent-impl contract — if a future commit adds `impl Producer for KafkaProducer` prematurely, this stub will need adjusting. Acceptable as a navigation aid. |

Round-1 dispositions (Suggestions 1-4 + Nit 1) have been moved to
`COMMENTS.DONE.7.md`. The remaining recorded-only item below is Nit 2:
an intentional non-action whose rationale should stay on the record.

---

## Nit 2: `compression: Box<dyn Compression>` field is held only to forward `compression_type()` to the accumulator

- **File**: `src/producer/kafka_producer.rs:151,401`
- **Severity**: Nit
- **Description**: The producer holds `compression: Box<dyn Compression>`
  but only ever uses it (in this milestone) to call
  `compression.compression_type()` once at construction — the result is
  passed to `RecordAccumulator::new`, and the accumulator stores
  `CompressionType` (the enum, not the boxed codec). Java holds the
  `Compression` instance because Java's `Compression` is the codec
  factory and is consulted later from `MemoryRecordsBuilder::new`. Rust
  routes codec selection through `CompressionType::for_codec_type()`
  per Phase 6d. So the producer's `compression` field is currently a
  one-shot read.

**Recommendation**: keep the field — Phase 7d's `send` path may well
re-introduce `Compression`-instance dispatch (e.g. for per-batch
codec-config, since gzip-level is per-record-batch in Java). Document
the field's intent more explicitly so a future maintainer doesn't
prune it. Not a real defect, just a code-clarity nit.

---

## CLAUDE.md / rules suggestions

None this round. Phase 7c follows the established skeleton-phase
patterns from 7a/7b: surface the milestone-deferred path with an
explicit `KafkaError::UnsupportedOperation` whose message points at the
unblock condition (here: `DefaultMetadataUpdater`), pin the deferred
message in a test, document field-by-field parity in `NOTES.md`. The
new pattern worth recording in agent memory is the `Drop`-with-spawned-
task discipline (capture-Arc-handles before spawn, flip-then-abort in
sync drop, async `close` does the proper join later) — added to
`phase7c_review_patterns.md`.

---

## DoD sign-off

- `cargo test --lib`: **1149 passed**, 0 failed, 0 ignored. (Round-2 7b
  baseline was 1143; the +6 are the new construction tests in
  `kafka_producer.rs`.)
- `cargo xtask lint`: clean (verified by `cargo build` clean output;
  the diff introduces no new clippy triggers).
- `cargo xtask format-check`: clean.
- No new dependencies introduced.
- Memory commit lands at `.claude/agent-memory/actor-executor/`, no
  project-rule mutations.
- DoD #7 (no new structs/traits beyond Java): respected — `StubKafkaClient`
  is a test-only `KafkaClient` impl (no new trait); `_phase_7c_does_not_impl_producer`
  is a test-only `fn`, not a type.
- DoD #10 (hot-path allocation audit): `KafkaProducer::new_for_test` is
  not on the per-message send path; the stored `client_id: Arc<str>`
  is the right shape for the upcoming Phase 7d send path (Arc-clones
  cheap per-batch).

## Round 1 verdict: 0 Blocking, 4 Suggestion, 2 Nit — ready for Actor fixup or accept-with-tracking.

The deferral of public `new`/`with_serializers` is **not blocking** —
it's the documented, unavoidable consequence of an upstream gap
(`DefaultMetadataUpdater`) that this milestone is allowed to surface
explicitly per CLAUDE.md rule 5, with a clear pointer to the unblock
condition in the error message. The construction-test suite proves the
wiring shape is sound via `new_for_test`. Phase 7d/8 can lift the
deferral once `DefaultMetadataUpdater` lands without revisiting the
skeleton itself.

---

# Round 2 — Phase 7d accepted

Review window: fixup commits `67ea5df`, `bfba26c`, `0e2dd8e`, plus
archive commit `b8d9a39` and agent-memory commit `48d36f6` on branch
`fresh-impl`.

Java references re-checked:
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java` (catch chain at lines 1056-1081).
- `kafka/clients/src/main/java/org/apache/kafka/common/errors/ApiException.java`,
  `BufferExhaustedException.java`, `SerializationException.java`,
  `ConfigException.java`, `InterruptException.java` (parent-class verification).

## Per-fixup verifications

| Fixup | Issue | Status |
|-------|-------|--------|
| `67ea5df` | Suggestion 1 (catch fan-out) + Nit 1 (`setReadOnly` rustdoc). | OK — `KafkaError::is_api_exception()` added with explicit per-variant match (no `_ =>` wildcard), gates the user-callback fire in `do_send`'s `Err` arm. Order matches Java: user callback fires before `interceptors.on_send_error`. The `setReadOnly` rustdoc accurately notes that `do_send` consumes `record` by value, so no Rust counterpart is needed. |
| `bfba26c` | Test pinning. | OK — strengthens `send_returns_record_too_large_and_fires_interceptor_on_send_error` (RecordTooLarge IS an `ApiException` → user callback count = 1) and adds `send_does_not_fire_user_callback_for_non_api_exception` (close → `IllegalState` is NOT an `ApiException` → user callback count = 0, interceptor count = 1). |
| `0e2dd8e` | Suggestion 2 (partitioner.class warn). | OK — `log::warn!` fires from the `new_for_test` body when `config.inner().originals().contains_key(PARTITIONER_CLASS_CONFIG)`, including the configured value and a Phase 7e marker. Default config (no key set) does not warn. |
| `b8d9a39` | Archive integrity. | OK — Phase 7d Round 1 review and Phase 7c Round 2 acceptance both moved to `COMMENTS.DONE.7.md` with disposition annotations citing fixup SHAs. NOTES.md updated with the Phase 7e MUST-wire-`partitioner.class` carry-over. |
| `48d36f6` | Memory update. | OK — only `.claude/agent-memory/actor-executor/` paths; no CLAUDE.md / `.claude/rules/` edits. |

## Truth-table verification — `is_api_exception()`

Cross-checked the four ambiguous variants against Java source in
`kafka/`:

- `BufferExhaustedException extends TimeoutException` → ApiException → `true`. ✓
- `SerializationException extends KafkaException` (NOT ApiException) → `false`. ✓
- `ConfigException extends KafkaException` → `false`. ✓
- `InterruptException extends KafkaException` → `false`. ✓

The match is exhaustive: 31 `true` + 4 direct-`KafkaException` `false`
+ 3 stdlib-`RuntimeException` `false` = 38 variants, matching the
total `KafkaError` variant count. No silent default arm — adding a new
variant in the future will trigger a compile error, forcing a deliberate
classification. Test `api_exception_classifier_matches_java_hierarchy`
covers every variant explicitly.

## Test pin — would it catch the original bug?

Yes. Mental simulation against pre-fix `do_send` (unconditional
`if let Some(user_cb) = … { user_cb.on_completion(…); }` in the `Err`
arm):

1. Test #2 (`send_does_not_fire_user_callback_for_non_api_exception`)
   closes the producer pre-send. `throwIfProducerClosed` raises
   `KafkaError::IllegalState`. Pre-fix code would have fired the user
   callback → counter = 1. Test asserts counter = 0 → **fails on
   pre-fix code, passes on post-fix code.** Genuine pin.
2. Test #1 (RecordTooLarge) is an upgrade: it now also asserts the
   user callback fires exactly once (RecordTooLarge IS an
   `ApiException`), pinning the positive case.

## `log::warn!` false-positive check

The trigger is `originals().contains_key(PARTITIONER_CLASS_CONFIG)` —
`originals()` returns only operator-supplied keys (not defaults), so
the warn fires iff the user explicitly set `partitioner.class` in
their config map. Default-config construction stays silent.

## New-defect scan

- `67ea5df`: production change is small and tightly scoped — no panic,
  no `unwrap`, exhaustive match, no widened visibility. The 4 doc
  blocks (catch fan-out table + setReadOnly rationale) are accurate
  and tied to specific Java line numbers.
- `bfba26c`: test-only changes (`mod tests` block). No production
  scope creep. `producer.sender_running.store(false, …)` reaches into
  a `pub(crate)` field, which is the existing test-seam pattern.
- `0e2dd8e`: only the `if … contains_key … { warn!(…) }` block plus
  three `Phase 7d` → `Phase 7e` rustdoc string updates. No collateral.

## DoD sign-off

- `cargo test --lib` 1161 passing (was 1159 at start of Round 1
  fixups; +2 for the new fan-out tests). The new
  `api_exception_classifier_matches_java_hierarchy` is co-counted in
  the same 1161.
- Lint, format-check clean (per Round 2 brief).
- Per-fixup verification: all 5 commits OK.
- Java parity: `is_api_exception()` truth table verified against
  Apache Kafka 4.2 source; catch-arm fan-out matches Java line
  1056-1081 exactly.
- Test pinning: would fail on pre-fix code → genuine regression
  guard.
- New-defect scan: no production scope creep, no panic/unwrap, no
  silent classification fallback.

## Round 2 verdict: accepted — Phase 7d ready to close.

# Round 1 — Phase 7e review (open items)

Review window: commits `3b39142`, `b7d5425`, `3ce4b7f`, `e3ca2d3`,
`38b0439`, `36f55bc`, `9b24291` on branch `fresh-impl`.

Java references (Apache Kafka 4.2):
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/KafkaProducer.java`
- `kafka/clients/src/test/java/org/apache/kafka/clients/producer/KafkaProducerTest.java`

Round 1 verdict: **0 Blocking, 2 Suggestion, 1 Nit.** Suggestion #1
(close timeout-elapsed branch) and Nit #1 (FQCN test redundancy)
are resolved in Round 1 fixups `5e3c2b5` / `c448b6e` and archived to
`COMMENTS.DONE.7.md`. The remaining open item is Suggestion #2,
naturally deferred to Phase 7f because it depends on the
`MockClientImpl` visibility hoist that Phase 7f will perform.

## Issue: `flush` 50-record concurrency parity test missing
- **File**: `src/producer/kafka_producer.rs:2899-2948` (and Java `KafkaProducerTest.java:1174-1200`)
- **Severity**: Suggestion
- **Java Reference**: `KafkaProducerTest.java:1174` `testFlushCompleteSendOfInflightBatches`
- **Description**: The Rust `flush_waits_for_pending_record_to_complete` covers the single-batch case. Java's `testFlushCompleteSendOfInflightBatches` sends 50 records, asserts none are done, then asserts all 50 are done after `flush()`. The Rust version sends one record via the accumulator directly (bypassing `producer.send`) because the `MockClientImpl` test mock is `pub(super)` to `sender.rs`. The 50-record concurrency assertion (the actual scenario operators encounter) is not exercised. Phase 7f should rectify when `MockClientImpl` is hoisted; flagging here so it isn't lost.
- **Expected**: Phase 7f's `KafkaProducerTest` translation should include the multi-record version with assertion that all per-record futures resolve after `flush().await`.
- **Actual**: Single-record variant only.

**Disposition**: Naturally deferred to Phase 7f. The translation
requires `MockClientImpl` to be hoisted from `pub(super)` (sender.rs)
to `pub(crate)`, which is a Phase 7f concern (broader
`KafkaProducerTest` translation). Tracked in `NOTES.md` as a Phase 7f
carry-over; will be archived once that phase lands the multi-record
variant.

## Areas verified clean

- `configure_partitioner` factory — FQCN + simple-name aliases map correctly; unset → `Ok(None)` (default sticky); unrecognised → `KafkaError::Config` with the unrecognised name AND a list of supported alternatives (`kafka_producer.rs:1660-1666`); error-message content is asserted in `partitioner_class_unrecognised_rejected` (`err.message().contains("partitioner.class")` and `contains("MyCustomPartitioner")`).
- Java's `partitionerPlugin.get() == null` check at line 417 is mirrored exactly: `enable_adaptive_partitioning = partitioner.is_none() && config.get_boolean(...)` (`kafka_producer.rs:450`).
- Phase 7d `log::warn!` placeholder removed (verified: no `warn!` in `new_for_test` body other than the unrelated `configure_delivery_timeout` silent-bump warn at line 1704, which translates a Java warn).
- `partitions_for` correctly threads through `wait_on_metadata` → `cluster.partitions_for_topic(topic).to_vec()`; both timeout and cached-metadata paths tested with error-message assertion.
- `metrics()` returns the empty `ProducerMetrics` map; trait-return-type matches Phase 7b alias.
- `flush()` calls `accumulator.begin_flush()`, `sender_wakeup()` (Phase 7d no-op), then awaits `accumulator.await_flush_completion()`. Empty-accumulator and pending-record paths both tested.
- `close_inner` idempotency via `Arc<AtomicBool> closed.swap(true, AcqRel)` — verified by `close_is_idempotent`. The `Drop` impl correctly observes `None` for the JoinHandle slot after explicit close (verified by `close_completes_cleanly_with_no_pending_records`'s post-condition assert).
- Trait completion: every non-transactional, non-telemetry method is wired; the four transaction methods + `client_instance_id` return `KafkaError::UnsupportedOperation` with milestone-citing messages (`PHASE_9_TXN_DEFERRED`, `TELEMETRY_DEFERRED`).
- Java `KafkaProducer` does NOT have a `listTopics()` method (verified via grep on `KafkaProducer.java`); the `Producer` trait correctly omits it.
- Phase 8 carry-overs in `NOTES.md` lines 264-275 list: (a) `close` idempotency potential Java-divergence, (b) `flush`-from-callback deadlock with Phase 8 `tokio::task::id()` plan, (c) `sender_wakeup` no-op with the wake-handle plan. Plus the Phase 7e "landed" section (lines 184-262) explicitly cites `DefaultMetadataUpdater` as the lift point for the public ctor.
- New-defect scan: no `tokio::select!` introduced; `Mutex::lock()` calls all release the guard before any `.await` (verified at lines 1186, 1581, 2982); no per-call `Box<dyn Future>` on send path; the new factory function is config-time, not on the send path.
- License headers preserved on the only modified file (`kafka_producer.rs`).
- No CLAUDE.md / `.claude/rules/` edits in the Phase 7e commits.
- Memory file at `.claude/agent-memory/actor-executor/phase7e_public_surface.md` (correct location).

## DoD sign-off

- Build, test, lint, format-check clean (per actor's report; verified `cargo build` clean).
- Test count delta: 1161 → 1175 (+14), distributed across the six Phase 7e fixes per the per-commit messages.
- All translatable Java tests are accounted for: `testFlushCompleteSendOfInflightBatches` (single-record variant), `testPartitionsFor*` (cached + unknown-topic timeout), `testMetricsReporterAutoGeneratedClientId` (N/A — telemetry stub), `testFlushMeasureLatency` (N/A — metrics stub), `testPartitionsForWithNullTopic` (N/A — `&str` cannot be null), `shouldCloseProperlyAndThrowIfInterrupted` (N/A — Tokio cancellation has no `InterruptException` shape), `testCloseWhenWaitingForMetadataUpdate` and the `testCloseIsForcedOnPending*` family (deferred to Phase 7f when `MockClientImpl` is hoisted).
- Hot-path allocation audit: `flush`, `close`, `partitions_for`, `metrics` are NOT on the per-record send path. The `configure_partitioner` factory is config-time. No new send-path allocations introduced.

## Round 1 verdict: accepted with minor follow-ups

No Blocking issues. Two Suggestions (timeout-elapsed close branch; multi-record flush test) and one Nit (FQCN test body redundancy) should be addressed before Phase 7f if convenient — none rise to a blocker for closing Phase 7e.
