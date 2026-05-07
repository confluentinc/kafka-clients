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
