---
name: Phase-7d Round-2 patterns
description: Verified-good fix shapes for Java-catch-arm fan-out parity in Rust + small lessons from Round 2 verification
type: project
---

Phase 7d Round 2 (commit `67ea5df` + `bfba26c` + `0e2dd8e`) cleared 2
Suggestions and 1 Nit cleanly. The patterns worth carrying forward:

## 1. Java `catch (ApiException) | catch (KafkaException) | catch (Exception)` fan-out → Rust classifier method

**Context:** Java's `KafkaProducer.doSend` catch chain has different
fire-up rules per arm — the `ApiException` arm fires the user callback,
the others don't (they rethrow). Rust's `Result<_, KafkaError>` has no
rethrow-vs-return distinction, so the parity rule has to be expressed
as a per-variant predicate.

**Verified-good shape:** add a `KafkaError::is_api_exception(&self) -> bool`
classifier whose match arm enumerates **every** variant explicitly
(no `_ =>` wildcard). Then gate the user-callback fire on
`err.is_api_exception()` while always firing the interceptor:

```rust
if err.is_api_exception() && let Some(user_cb) = append_cb.user_callback.as_ref() {
    user_cb.on_completion(Some(&null_metadata), Some(&err));
}
self.interceptors.on_send_error(Some(&record), Some(tp), &err);
```

**Why exhaustive matters:** a `_ => false` (or `_ => true`) wildcard
silently mis-classifies any future variant. The exhaustive match
forces a deliberate classification at compile time when a new variant
lands.

**Order of fire:** Java fires the user callback BEFORE the interceptor
(line 1058-1064). Rust must match this. When auditing, look at the
Rust block and verify the order matches the Java line numbers cited
in the comment.

**Truth-table verification protocol:** when reviewing such a classifier,
spot-check 4-5 of the most-likely-to-be-wrong variants against Java
source under `kafka/`:

```bash
grep -h "extends" kafka/.../BufferExhaustedException.java \
                  kafka/.../SerializationException.java \
                  kafka/.../ConfigException.java \
                  kafka/.../InterruptException.java
```

Watch for variants whose Rust name is ambiguous about the Java parent:
`BufferExhaustedException` extends `TimeoutException` (not directly
`KafkaException`), so it IS an ApiException. If the actor classified
it as `false`, that's a real bug.

## 2. Test-pinning two variants of the same catch path

For a per-arm fan-out fix, **one positive and one negative test** form
the pin:

- Positive: trigger an `ApiException` subclass (e.g. RecordTooLarge),
  assert user callback fires exactly once, interceptor fires exactly
  once.
- Negative: trigger a non-`ApiException` (e.g. close-then-send →
  `IllegalState`), assert user callback fires **zero** times,
  interceptor fires exactly once.

The negative test is the genuine pin against the original bug. Mental
simulation: if the pre-fix code had `if let Some(cb) = … { cb.fire(); }`
without the `is_api_exception()` gate, the negative test's
`assert_eq!(user_callback_count, 0)` would have failed → the test
catches the original bug. Confirm this before accepting.

## 3. log::warn! for "deferred-config silently ignored" deferrals

When a config key (e.g. `partitioner.class`) is currently a no-op
because the milestone deferred it, the operator-visible warn shape:

```rust
if config.inner().originals().contains_key(PARTITIONER_CLASS_CONFIG) {
    warn!(
        "Phase 7e: '{}' loading is not yet implemented; using built-in adaptive partitioning. \
         Configured value '{}' is ignored.",
        PARTITIONER_CLASS_CONFIG,
        config.inner().originals().get(PARTITIONER_CLASS_CONFIG).map(String::as_str).unwrap_or(""),
    );
}
```

Use `originals().contains_key()` (not `get_string()` or default-aware
accessors) — `originals()` returns only operator-supplied entries, so
the warn fires iff the user explicitly set the key. This avoids the
false positive of warning on every default-config construction.

The `Phase 7e:` prefix in the message is a useful operator hint that
ties the warning to a tracked carry-over item.

## 4. New-defect scan checklist for fan-out fixes

When a fixup adds a classifier method + gates a fire on it:

- Are all variants enumerated in the match (no wildcard hiding mis-classifications)?
- Does the order of fire match Java?
- Does the test cover both arms (positive + negative)?
- Does the negative test actually fail on pre-fix code? (mental simulation)
- Does any production code path widen visibility unnecessarily?
- Does the rustdoc cite Java line numbers so future readers can re-verify?

This 6-point check caught nothing on Phase 7d Round 2 — the fix was
clean. But the protocol is the right baseline for similar Java-catch
parity work in later phases.
