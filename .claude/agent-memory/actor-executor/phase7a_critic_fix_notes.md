---
name: phase7a-critic-fix-notes
description: Milestone-8 Phase-7a critic fix patterns — Arc<str> per-instance vs per-record, peek-by-ref, Notify::notified() race, parameterized test loops, KafkaError::unsupported_version for missing branches
metadata:
  type: feedback
---

Lessons from resolving COMMENTS.1.md (16 findings) on Phase 7a.

## §27 zero-copy on the receive path

**Rule**: Per-record code path may NOT allocate an `Arc<str>` from
`&str` (the allocation + UTF-8 copy is the same cost as `String::new`).
Allocate the `Arc<str>` ONCE per container (e.g. `CompletedFetch`) and
clone the `Arc` per record — that's a cheap atomic pointer bump.

**Why**: `Arc::from(&str)` allocates a new control block and copies
bytes. `Arc::clone(&existing_arc)` does neither. The Critic flagged
this as a per-record violation of `consumer-threading.md` §27.

**Apply when**: Any per-record code path in `consumer/internals/`,
especially `CompletedFetch::fetch_records`.

## Borrow-by-reference peek, mutate cursor index after

**Rule**: When iterating records out of `cursor.current_records[i]`,
prefer a peek function that returns `Option<(&Record, &BatchMetadata)>`
over one that returns owned tuples. Read all needed fields inside a
scoped borrow, drop the borrow, then mutate the cursor's
`record_index` (or any other `&mut self` mutation).

**Why**: Cloning a `DefaultRecord` deep-copies its key + value `Vec<u8>`
plus every `RecordHeader` — exactly what §27 forbids on the receive
path. The Critic flagged that the previous `peek_last_fetched_record`
returned `(DefaultRecord, BatchMetadata)` (cloned).

**Apply when**: Translating any Java method that takes a `Record`
reference and reads fields off it. In Rust, mirror the borrow shape
rather than the call-by-value shape (Java's calls look like values but
are object references).

## Notify::notified() registers on first poll — race window

**Rule**: When using `tokio::sync::Notify::notified()` for a wait
loop with a pre-check flag, always pin the future and call
`enable()` BEFORE re-checking the flag. The pattern:

```rust
if self.flag.swap(false, Ordering::SeqCst) { return; }
let notified = self.notify.notified();
tokio::pin!(notified);
notified.as_mut().enable(); // register as waiter
if self.flag.swap(false, Ordering::SeqCst) { return; } // re-check
let _ = tokio::time::timeout(timeout, notified).await;
```

**Why**: `notify_waiters()` is fire-and-forget — if no waiter is
registered when it fires, the wakeup is lost. The first poll registers
the waiter; in a `tokio::time::timeout(_, fut).await` the future is
polled inside the timeout, so a `notify_waiters` between flag-clear
and timeout-poll is lost. `enable()` registers the waiter eagerly.

**Apply when**: Any `Notify`-based wakeup that has a pre-check
fast-path AND can race with the notifier.

## Translate Java's `@ParameterizedTest` as loops, not single tests

**Rule**: When translating a `@ParameterizedTest` with `@ValueSource`
or `@MethodSource`, write a SINGLE `#[test]` function in Rust that
loops over the parameter tuples. Use `assert_*!` messages that embed
the parameter values so failures point to the failing combination.

**Why**: DoD §3 says "Parameterized tests in Java with N parameter
combinations become N tests in Rust". A single `#[test]` with a `for`
loop counts as N tests (test runs N assertions, fails on any). The
Critic flagged that the previous translation only covered ONE of the
N combinations.

**Apply when**: Translating any test class with `@ParameterizedTest`.

## When a Java code path depends on a missing type, fail loudly

**Rule**: If a translation needs a type that's not yet implemented
(e.g. `ControlRecordType::parse_key`), do not silently drop the path.
Instead:
1. Document the limitation in the module's docstring.
2. Return `KafkaError::unsupported_version(...)` when production code
   actually hits the path.
3. Track the missing translation as Phase X+1 work in the doc.

**Why**: Silently incomplete paths become correctness bugs in
production. The Critic flagged that the missing `containsAbortMarker`
translation could cause records to be silently dropped under
READ_COMMITTED with producer-ID reuse.

**Apply when**: Translating a complex method where a sub-branch
requires a not-yet-translated dependency.

## Match Java's package-private with `pub(crate)`, not `pub`

**Rule**: Java's package-private types (no modifier) become
`pub(crate)` in Rust, NOT `pub`. The containing module's visibility
doesn't promote the type; you have to opt in.

**Why**: `pub` in a `pub` module leaks the type to library users; Java
package-private restricts to one package, which in Rust is the crate.
The Critic flagged `FetchSessionRequestData` as overly public.

**Apply when**: Any type that's an "inner class" or
package-private type in Java. The struct AND its fields AND any
returning function need `pub(crate)`.

## Drop silent-no-op parameters; document the divergence

**Rule**: When the Java translation produces an unused / silently
ignored parameter (e.g. `copy_session_partitions: bool` that does
nothing in the Rust implementation), drop the parameter entirely
rather than keeping it as a no-op. Document the Java divergence in
the rustdoc so future readers understand why the Rust API differs.

**Why**: A silent no-op `bool` is worse than no parameter — Java
callers might depend on the implied behavior, and a Rust caller has
no way to discover the parameter does nothing without reading the
implementation.

**Apply when**: Any Java parameter whose Rust implementation makes it
a no-op (often happens when the Rust translation restructures the
underlying data flow).
