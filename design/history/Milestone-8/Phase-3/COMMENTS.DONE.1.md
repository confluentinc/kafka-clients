# Milestone-8 Phase-3 review (N=1) — RESOLVED items

Issues from `COMMENTS.1.md` that have been addressed. Original review text
is preserved verbatim; each entry ends with a **Resolution** note.

---

## Issue 1: `current_lag` silently returns `Some(0)` for unassigned partitions instead of erroring

- **File**: `src/consumer/mock_consumer.rs:414-432`
- **Severity**: Behavior Mismatch (low — not exercised by Java tests)
- **Java Reference**: `MockConsumer.java:681-688` calls `position(topicPartition)` which on line 422-423 throws
  `IllegalArgumentException("You can only check the position for partitions assigned to this consumer.")`
  when the partition is unassigned.

**Description**: For a `topic_partition` that has an entry in
`end_offsets` but is **not currently assigned** (and therefore has no
`SubscriptionState` entry / no position), the Rust `current_lag` did:

```rust
let pos = self.subscriptions.position_or_null(topic_partition).map(|p| p.offset);
Some(end - pos.unwrap_or(end))   // → Some(end - end) = Some(0)
```

It returned `Some(0)`, indistinguishable from the "caught up" case
(`end_offsets` not present at all). Java's call chain would surface an
`IllegalArgumentException` from `position(tp)`.

**Resolution**: `current_lag` now returns `None` when the partition is
not assigned. The rustdoc spells out the full decision tree
(unassigned → `None`; assigned with no end offset known → `Some(0)`;
assigned with end known and no position → `Some(0)`; otherwise
`Some(end - position)`) and the divergence from Java (Java throws,
Rust returns `None` because `&self` can't mutate or throw).

---

## Issue 2: Unnecessary clone in `commit_async_impl`

- **File**: `src/consumer/mock_consumer.rs:951-962`
- **Severity**: Minor (allocation, not correctness)
- **Java Reference**: `MockConsumer.java:353-358`

**Description**: `commit_async_impl` cloned the offsets map before
extending `self.committed`, then passed `&offsets` to the callback. The
clone was avoidable by inverting the order: call the callback first with
`&offsets`, then move `offsets` into `self.committed.extend(offsets)`.

**Resolution**: Reordered so the callback is invoked with `&offsets`
first, then `self.committed.extend(offsets)` moves the map.

---

## Issue 3: `commit_async()` skips the explicit `ensureNotClosed` ordering of Java's `commitAsync()`

- **File**: `src/consumer/mock_consumer.rs:673-677`
- **Severity**: Minor (behavior is preserved; ordering of side effects differs)
- **Java Reference**: `MockConsumer.java:367-369` calls `commitAsync(null)` → line 372-375 which calls `ensureNotClosed()` **before** `allConsumed()`.

**Description**: Java's `commitAsync()` (no-args) calls
`commitAsync(callback=null)` which `ensureNotClosed()`s **before**
computing `allConsumed()`. Rust's `commit_async()` computed
`subscriptions.all_consumed()` first, then entered `commit_async_impl`
which called `ensure_not_closed()`.

**Resolution**: `commit_sync` and `commit_async` (the no-args variants)
now call `self.ensure_not_closed()?` at the top, before reading
`subscriptions.all_consumed()`. Matches Java's ordering.

---

## Issue 4: `commit_async_with_callback` double-checks `ensure_not_closed`

- **File**: `src/consumer/mock_consumer.rs:679-684`
- **Severity**: Cosmetic / redundancy

**Description**: `commit_async_with_callback` called `ensure_not_closed()`
at line 681, then delegated to `commit_async_impl` (line 683) which
called it AGAIN at line 956.

**Resolution**: Removed the outer `ensure_not_closed()` from
`commit_async_with_callback`; `commit_async_impl` handles it. Single
check remains.

---

## Issue 5: Commit message claims "THREE null-input assertions" dropped — only TWO exist

- **File**: Commit `eff6200` message, lines 24-26
- **Severity**: Cosmetic (doc accuracy)

**Description**: The Phase 3 (3/3) commit message stated "THREE
null-input assertions"; `MockConsumerTest.java:192-204` only has **two**.
The test docstring in `tests/consumer/mock_consumer_test.rs:316-329`
correctly lists only two — only the commit message text overstated.

**Resolution**: No code change. The body of the final resolutions
commit notes the commit-message overstatement.

---

## Issue 6: `PollTask<K, V>` re-export is publicly visible — confirm intent

- **File**: `src/consumer/mod.rs:48`
- **Severity**: API surface review

**Description**: `src/consumer/mod.rs:48` re-exported `PollTask`
alongside `MockConsumer`. Callers writing
`consumer.schedule_poll_task(Box::new(|c| ...))` do NOT need to reference
`PollTask` directly — type inference handles the coercion. The
re-export widened the public API surface without a demonstrated need.

**Resolution**: Dropped `PollTask` from the `pub use` in
`src/consumer/mod.rs`. The alias remains `pub` inside `mock_consumer.rs`
because it appears in the parameter type of `pub fn schedule_poll_task`,
but it is no longer re-exported at the consumer module root.

Reviewed at SHA `eff6200`.

---

# Re-review at SHA `7340f58` — RESOLVED items

## Issue 7: `commit_sync` inline comment traces the wrong Java call chain

- **File**: `src/consumer/mock_consumer.rs:669-672`
- **Severity**: Behavior Mismatch (documentation) — minor, comment contradicts the actual Java behavior the fix is justified against
- **Java Reference**: `MockConsumer.java:378-380` (`commitSync()` →
  `commitSync(allConsumed())` → line 362-364 →
  `commitAsync(offsets, null)` line 353-358)

**Description**: The inline comment introduced by `1912761` traced
the `commitAsync()` (no-args) chain, not the `commitSync()` chain.
Java's `commitSync()` actually reads `allConsumed()` BEFORE the
closed check (which only happens deep inside `commitAsync(offsets,
null)` at `MockConsumer.java:353-358`). The Rust code deliberately
tightens by checking closed first. The commit message for `1912761`
correctly acknowledged the tightening, but the inline comment claimed
the opposite — Java already checks closed first via the wrong call
chain. A future reader trusting the comment would believe Rust matches
Java exactly when in fact Rust is a deliberate behavior tightening.

**Resolution**: Rewrote the inline comment on `commit_sync` to match
the resolution-commit language: explicitly states Java reads
`allConsumed()` BEFORE the closed check, traces the correct chain
(`378-380 → 362-364 → 353-358`), and notes end behavior is identical
(both error when closed) but Rust skips the wasted subscription-map
traversal. Fixup commit `a452926` targets `1912761`.

---

## Issue 8: `commit_async_with_callback` comment references stale "same trade-off as `commit_sync()`"

- **File**: `src/consumer/mock_consumer.rs:710-715`
- **Severity**: Cosmetic (doc accuracy) — comment cross-reference became stale after fix #3
- **Java Reference**: `MockConsumer.java:372-375`

**Description**: The inline comment introduced by `4ebaa92` said "same
trade-off as `commit_sync()`", but fix #3 (`1912761`) added an
`ensure_not_closed()?` check at the top of `commit_sync()`. After fix
#3, `commit_sync()` no longer has the "wasted `all_consumed()`
traversal when closed" trade-off; only `commit_async_with_callback` (and
the other `*_offsets` / callback variants) does. The cross-reference
became misleading.

**Resolution**: Picked approach (a) — comment-only fix. Approach (b)
(also pre-checking `ensure_not_closed` here) would still leave
`commit_sync_offsets` and `commit_async_offsets_with_callback` relying
on `commit_async_impl`'s check, so adding a pre-check only on
`commit_async_with_callback` would introduce new asymmetry rather
than removing it. The no-args pre-checks on `commit_sync` /
`commit_async` exist as a targeted optimization (skip the
subscription-map traversal when closed); the `*_offsets` / callback
variants don't benefit from it because the caller already supplies
the offsets map.

The rewritten comment is now specific to `commit_async_with_callback`,
notes the asymmetry with `commit_sync` / `commit_async` explicitly,
and stops claiming parity that no longer exists. Fixup commit
`ca5d6cf` targets `4ebaa92`.

---

Re-reviewed at SHA `7340f58`. Both follow-up findings resolved.
