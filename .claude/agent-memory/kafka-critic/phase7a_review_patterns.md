---
name: Phase 7a review patterns
description: ProducerConfig + ConfigDef helper translation patterns. Public-DOC-constant divergence trap, skip-rationale-too-broad pattern, validator-name-vs-bound mismatch, schema parity audit checklist.
type: project
---

Phase 7a translated `ProducerConfig` + `ProducerConfigTest` plus three
ConfigDef helpers (`ValidList`, `CaseInsensitiveValidString`,
`NonEmptyString`) and the `addClientSslSupport` /
`addClientSaslSupport` schema extensions. The translation was very
clean — 0 Blocking, 4 Suggestion, 4 Nit. Recurring patterns:

## 1. Schema parity audit checklist (high-yield)

For any translation of a Java class with a `static { CONFIG = new
ConfigDef().define(...).define(...) ... }` block:

- Count `.define(` invocations in Java vs Rust — must match.
- Count public string-literal `*_CONFIG` constants in Java vs Rust —
  must match (modulo documented Milestone deviations).
- Spot-check 5-8 keys for: exact Java string literal, exact default
  value (Long vs Int boundaries are a frequent foot-gun — Java's `60 *
  1000` is `int`, Rust's `Long(60 * 1000)` must match), validator type
  (`Range::between(0, 5)` vs `Range::at_least(0)` vs
  `ValidString::in_set(...)`), Type tag (Boolean/Int/Long/Short/
  Double/String/List/Class/Password), and Importance (LOW/MEDIUM/HIGH).
- Verify `addClientSslSupport` / `addClientSaslSupport` insertion
  *order* — IndexMap-backed config_keys preserves insertion order, and
  user-facing iteration (e.g. `config_names()`) depends on it.

## 2. Public-DOC-constant divergence trap

Java's `ProducerConfig.public static final String *_DOC` constants are
part of the public API surface (callers like the HTML doc generator
read them directly). The Rust convention has been to translate the
*config string* (e.g. `KEY_SERIALIZER_CLASS_DOC`) but **paraphrase /
shorten** the doc text. Per CLAUDE.md Rule #4 ("Keep similar comments
as the Java source ... Never change the contract of public API") this
is a divergence.

**How to apply**: when a `pub const FOO_DOC: &str = ...` exists, diff
the byte content vs Java line-for-line. File a Suggestion if the text
diverges semantically (typo-fix, wording change, or stale Java-version
text). For *file-private* `const FOO_DOC: &str = ...` (no `pub`), the
shortening is acceptable since it's not on the public API surface.

The `SSL_*_DOC` constants in `ssl_configs.rs` are particularly prone
to staleness because Apache Kafka updates these doc strings between
versions. When reviewing, always confirm the Java reference is the
*current* `kafka/` 4.2 source, not an older version.

## 3. Skip-rationale-too-broad pattern

When a Java test combines several behaviors that are individually
rejected in Milestone-1 (e.g. `enable.idempotence=true` AND
`transactional.id=...` AND `2pc=true` AND `transaction.timeout.ms=...`),
a verbatim port is impossible. The wrong move is to skip the entire
test. The right move is to identify which **subset** of the test's
assertions exercises logic that IS reachable in Milestone-1.

**Example from Phase 7a**: `testTwoPhaseCommitIncompatibleWithTransactionTimeout`
sets all four configs above and asserts the 2pc/timeout error message
contains both keys. The Milestone-1-reachable subset is just `2pc=true`
+ `timeout=set` (without idempotence or transactional.id). The Rust
test was skipped entirely with a "verbatim port impossible" comment;
the right disposition is to write a Rust-adapted test for the
reachable subset and defer only the rest. Otherwise the underlying
production logic (8 lines at `producer_config.rs:1346-1354`) is
untested.

**How to apply during review**: when a Rust skip-rationale says
"verbatim port impossible because of Milestone deviations", check
whether the underlying invariant has a Milestone-reachable variant.
If yes → file a Suggestion to add the variant and narrow the skip.
This carries forward Phase-6d/6e's "skip rationale must name what's
*not* covered" pattern.

## 4. Validator-name-vs-bound mismatch

A frequent pattern: a chain of `Range::at_least(...)` validators is
bound to descriptive variables (`zero_or_more_i32`,
`zero_or_more_i64`). This is a fine pattern UNTIL one of the bound
constants is **not zero**: `Range::at_least(SEND_BUFFER_LOWER_BOUND)`
where `SEND_BUFFER_LOWER_BOUND = -1`. The variable name `zero_or_more`
falsely advertises a `>= 0` constraint while the actual constraint is
`>= -1`. Harmless but confusing. Watch for Java's `atLeast(BOUND)`
where BOUND is a named constant — verify the Rust binding name
matches the actual numeric value, not the documentation-style name.

## 5. `Range` validator with `f64` storage and integer types

The Rust `ConfigDef::Range` stores both `lower` and `upper` as `f64`,
even when the actual ConfigValue is `Int`/`Long`/`Short`. This
introduces no precision loss for the Producer's value ranges (max is
`i32::MAX = 2147483647`, fits exactly in f64). The producer's lone
i64 default `RECONNECT_BACKOFF_MAX_MS = 9223372036854775807` (i64::MAX)
would lose precision but only the user-supplied raw value is parsed
as i64; the validator only checks `>= 0_f64`, which is a
direction-preserving comparison. Don't flag this as a precision bug
unless a translation actually compares an `i64` value > 2^53 against
a non-zero bound. Pattern: when Java passes `Long.MAX_VALUE` to a
`Range` upper bound, that's the time to investigate.

## 6. AtomicI32 vs AtomicInteger ordering

Java's `AtomicInteger.getAndIncrement()` is sequentially consistent.
Rust's `fetch_add(1, Relaxed)` is not. For uniqueness counters
(client-id sequence, message-id sequence) this is fine — the only
invariant is that no two callers see the same value. For counters
that participate in happens-before (publication of a flag protected
by the counter) `Relaxed` is wrong. When reviewing, verify the
counter is *only* read for its uniqueness property, not as a fence.

## 7. `appendSerializerToConfig` Rust API shape

Java's `appendSerializerToConfig(Map<String, Object>, Serializer<?>,
Serializer<?>)` is an awkward fit for Rust because it relies on Java
reflection (`keySerializer.getClass()`). The Rust API takes
`Option<&str>` (FQCN) for the second/third args and
`HashMap<String, Option<String>>` for the first. The semantic
preservation hinges on:

- `Some(name)` → overwrite the entry with that FQCN string.
- `None` → keep the existing entry; if it's missing or `None`, error.
- Error message: `config_exception::new(name, "null", "must be non-null.")`
  produces `"Invalid value null for configuration <name>: must be non-null."`
  byte-exact with Java's `ConfigException(name, null, msg)` formatter.

The Rust tests should pass FQCN string constants matching what
`Serializer.getClass().toString()` would produce in Java
(`"org.apache.kafka.common.serialization.ByteArraySerializer"` etc.).

## 8. Milestone-deviation rejection ordering matters for error-message contracts

The `postProcessParsedConfig` step ordering in `ProducerConfig.java`
(line 569-577) is an ordered fold:

1. `postValidateSaslMechanismConfig`
2. `warnDisablingExponentialBackoff`
3. `postProcessReconnectBackoffConfigs`
4. `postProcessAndValidateIdempotenceConfigs`
5. `maybeOverrideClientId`

When inserting Milestone-1 hard rejections, *where* you insert them
changes which error message a user sees. The Phase 7a translation
inserts:

- `transactional.id` rejection between steps 3 and 4 (so the user
  sees "Transactional producer not supported" instead of step 4's
  "Cannot set transactional.id without enabling idempotence", which
  Milestone-1's `enable.idempotence=false` default would make
  misleading).
- `enable.idempotence=true` rejection between steps 4 and 5 (so the
  user explicitly opting into idempotence with a too-large
  `max.in.flight=6` sees Java's canonical upper-bound error verbatim
  before the Milestone-1 message; this preserves
  `testUpperboundCheckOfEnableIdempotence` byte-exact).

When reviewing rejection ordering, verify each placement against the
specific Java tests that depend on the canonical error messages. The
ordering rationale should be in a comment at the rejection callsite
naming the test by name.

## 9. Test-count integrity check

When a phase brief reports "+30 net new tests but +12 + 3 + 27 = +42
gross" with vague "pre-existing tests being replaced/refactored",
verify by:

```sh
git diff <phase-start>~1..<phase-end> -- <test-file> | grep -E '^- *(#\[test\]| *fn [a-z_]+)'
```

Confirm any `-fn` was either (a) a non-test helper renamed/refactored,
or (b) a test renamed (paired with a `+fn <new_name>`). If a
`-#[test]` appears without a corresponding `+#[test]` for the same
behavior, file a Blocking comment — silently dropped tests are a DoD
violation.
