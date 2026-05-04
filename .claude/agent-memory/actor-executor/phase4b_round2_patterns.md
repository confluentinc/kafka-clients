---
name: Phase 4b Round 2 review patterns
description: Patterns and lessons from Phase 4b Round 2 fixups (Issues 5-16 in metadata stack)
type: feedback
---

# Phase 4b Round 2 review patterns

Concrete lessons from the Round 2 metadata-stack fixups that should
shape future review responses.

## ArcSwap as Java `volatile Arc<T>` analogue

**Why:** Issue 8 — `Metadata::fetch()` was taking the writer mutex
on the producer hot path. Java's `metadataSnapshot` field is
`volatile` and `fetch()` is not `synchronized`.

**How to apply:** Whenever Java has a `volatile T` field that's
read on a hot path and written by `synchronized` methods, the Rust
analogue is `arc_swap::ArcSwap<T>` on the parent struct, **outside**
the inner `Mutex`. Writers take the inner mutex (preserving Java's
writer-vs-writer serialization) and call `.store(Arc::new(new))`.
Readers do `.load_full()` lock-free. arc-swap is in Cargo.toml; do
not hand-roll. Also remember to plumb `&Arc<T>` through helpers
that previously read from `inner.<field>` so all reads in a single
write pass see a consistent view.

## Listener invocation under lock

**Why:** Issue 12 — Java's `synchronized` is reentrant, so `Metadata`
can dispatch listeners while holding the monitor. `std::sync::Mutex`
is not reentrant, but matching Java's listener-dispatch ordering
matters (a listener observing post-update state must not see a later
writer's interleaved state).

**How to apply:** When a Java method calls a listener inside a
`synchronized` block, the Rust translation should also call the
listener while holding the inner mutex AND add a type-level rustdoc
documenting the constraint that the listener must NOT call back into
the same instance (which would deadlock on a non-reentrant mutex).
The supported access pattern is reading the argument passed to the
callback.

## "Covered elsewhere" deferrals are a Round 1 trap

**Why:** Issues 6, 7, 13 — three rounds of Critic feedback have
caught the same anti-pattern: "covered by `MetadataSnapshotTest`"
where the deferred test exercises an integration path
(`Metadata.update`, `updatePartitionLeadership`) that the
unit-level test does not.

**How to apply:** When deferring a Java test:
1. Run a 1:1 grep for the Java method names the test exercises
   against the alternative test file.
2. Cite the specific assertions that are covered and which Rust
   test covers them.
3. If the alternative test is at the unit level (e.g. snapshot
   merge in isolation) but the deferred test is at the integration
   level (e.g. `Metadata.update` driving the merge), do NOT defer —
   translate it.
4. If still deferring, name a specific Phase to revisit and a
   trigger condition (e.g. "when Cluster::leader_for lands").

## `update_with_current_request_version` is a useful test surface

**Why:** Producer test code uses Java's
`updateWithCurrentRequestVersion(response, isPartial, now)` heavily
because it auto-resolves `request_version`. Without this helper,
Rust tests need to manually call `new_metadata_request_and_version`
before each `update`, which is verbose and error-prone (cached
versions go stale across `add` / `request_update_for_new_topics`
calls).

**How to apply:** When a Java class inherits a
"with_current_*-version" helper from a parent, translate the helper
on the Rust child as well, even if it's just a thin wrapper that
fetches the current version then calls `update`. Cheap to add,
big readability win for tests.

## Avoid `count()` on `&[T]` slices

**Why:** Many Cluster API methods (`partitions_for_topic`, `nodes`)
return `&[T]` not `Iterator`, so `.count()` doesn't compile and
clippy points to `.len()`. Easy mistake when reading existing
tests that use `.topics()` (which IS an iterator) and assuming
`.partitions_for_topic()` is too.

**How to apply:** Default to `.len()` on slices; use `.count()` only
on actual iterators. The Rust API surface for `Cluster` returns
`&[T]` for cached collections and `impl Iterator<Item = &str>` for
projections like `topics()`.
