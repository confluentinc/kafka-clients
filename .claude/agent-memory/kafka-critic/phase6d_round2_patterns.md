---
name: Phase 6d Round 2 review patterns
description: Verified-good fix shapes from RecordAccumulator review — hot-path Arc<str> reuse via get_key_value, checked_add overflow translation, cancellation Drop test scaffolding
type: feedback
---

Patterns confirmed correct in Round 2 of Phase 6d. Apply pre-emptively when reviewing future producer-internals translations.

## Verified-good fix shapes

### Hot-path `Arc<str>` reuse via `HashMap::get_key_value`
Pattern confirmed safe and zero-allocation on the fast path:

```rust
fn get_or_create_topic_info(&self, topic: &str) -> (Arc<str>, Arc<TopicInfo>) {
    let mut map = self.topic_info_map.lock().unwrap();
    if let Some((key, info)) = map.get_key_value(topic) {
        return (Arc::clone(key), Arc::clone(info));  // 2 refcount bumps, no alloc
    }
    let topic_arc: Arc<str> = Arc::from(topic);  // alloc only on cold path
    let info = Arc::new(TopicInfo::new(...));
    map.insert(topic_arc.clone(), info.clone());
    (topic_arc, info)
}
```

Why it works: `HashMap<Arc<str>, V>::get_key_value(&str)` returns `Option<(&Arc<str>, &V)>` because `Arc<str>: Borrow<str>` is implemented in std. The returned `&Arc<str>` is a borrow of the **stored** key (not the lookup parameter), so `Arc::clone(key)` bumps the refcount on the interned value. **Result: 1 alloc per topic per producer, 0 allocs per `append`.** Mirrors Java's `Map.computeIfAbsent` reuse pattern.

**Critic note**: If you see `Arc::from(topic)` per-`append` even when a `HashMap<Arc<str>, _>` already holds the key, flag it as Performance — the fix is `get_key_value`.

### `checked_add` for Java overflow-detection idioms
Java's `(int) ((a + b) > 0)` relies on silent integer wrap-around. Rust debug builds panic on integer overflow; the safe translation is:

```rust
match a.checked_add(b) {
    Some(candidate) if candidate > 0 => { /* use candidate */ }
    _ => { /* warn-skip — Java wrap-to-negative path */ }
}
```

`None` arm covers exactly the cases where Java wraps to ≤ 0. Boundary: `i64::MAX.checked_add(0) = Some(i64::MAX) > 0` matches Java; `i64::MAX.checked_add(1) = None` matches Java's wrap-to-`i64::MIN`. **Use `checked_add`, not `wrapping_add` — the latter literally mirrors Java but still requires the comparison and is no faster.**

**Critic note**: If you see raw `a + b > 0` translating a Java overflow check, flag it. Rust will panic in dev builds before reaching the warn branch.

### Cancellation-Drop test scaffolding
Pattern for testing `Drop` fires on `tokio::task::JoinHandle::abort()`:

```rust
let blocked_accum = accum.clone();
let blocked = tokio::spawn(async move {
    blocked_accum.append(...).await  // blocks on BufferPool::allocate.await
});

// Wait for the task to register itself (poll baseline counter).
for _ in 0..100 {
    if accum.appends_in_progress_count() == 1 { break; }
    tokio::time::sleep(Duration::from_millis(2)).await;
}
assert_eq!(1, accum.appends_in_progress_count());

blocked.abort();
let _ = blocked.await;  // resolves with Err once Drop has run.

// Poll for Drop's bookkeeping update.
for _ in 0..100 {
    if accum.appends_in_progress_count() == 0 { break; }
    tokio::time::sleep(Duration::from_millis(2)).await;
}
assert_eq!(0, accum.appends_in_progress_count());
```

Inspectors via `#[cfg(test)] pub(crate) fn appends_in_progress_count()` — keeps production API clean. **Verify by grep that no non-test caller exists.**

**Critic note**: A weaker version of this test (no abort, no counter polling) does not exercise the cancellation Drop. The poll-with-deadline pattern is necessary because Drop runs on a different scheduler tick than the abort signal.

## Round-2 verification checklist (carry forward)

When verifying fixups for previously-filed issues:

1. **Per-issue "verified" line** — read the post-fix code, confirm the diff matches the disposition table.
2. **No-regression scan** — re-run patterns from past phases to confirm no false-positives re-introduced (e.g., `Pin<Box<dyn Future>>`, per-record `tokio::spawn`, `String::from(topic)`).
3. **Flake check** — run new concurrency / cancellation tests 3-5× to confirm stability.
4. **Inspector visibility scan** — `#[cfg(test)]` only? Verify by grep for non-test callers.
5. **DoD sign-off** — count tests pre/post, lint clean, format clean, no Cargo.toml/lock changes (unless explicitly authorized).
6. **Boundary-math sanity** — for any overflow/wrap-around translation, mentally walk i64::MAX + 0, i64::MAX + 1, i64::MIN cases through both Java and Rust to confirm equivalence.

## Anti-patterns to keep flagging

- Per-`append` `Arc::from(topic)` when the topic is already interned in a `HashMap<Arc<str>, _>` (Issue 8 archetype).
- Stress tests with `seen > 0` lower bounds instead of EXACT counts (Issue 2 archetype — masks record loss).
- Drained-batch tests that don't iterate `Records::records(&records)` to assert key/value byte fidelity (Issue 6 archetype).
- Raw `+` translating Java `(a + b) > 0` overflow checks (Issue 9 archetype).
- "Covered by Phase X" skip rationales without a specific test name / file:line (skip-rationale tightening pattern).
