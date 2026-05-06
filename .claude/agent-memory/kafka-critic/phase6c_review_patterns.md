---
name: Phase 6c review patterns
description: Recurring traps in producer leaf classes — null-vs-empty topic guard, owned-record interceptor clone, package-private→Box<dyn Fn> seam, race-loser path testability
type: project
---

Phase 6c reviewed: ProducerRecord, Partitioner trait + RoundRobinPartitioner,
ProducerInterceptor + ProducerInterceptors, BuiltInPartitioner.

## Patterns to carry forward

### Null-vs-empty constructor guard (#1, Behavior Mismatch)

When Java rejects `null` for a `String` parameter and the Rust
translation models the parameter as `impl Into<Arc<str>>` (non-null at
the type level), the actor often adds a runtime `is_empty()` check
"to mirror null". This is a divergence — Java accepts `""` at
construction, only the broker rejects it later. The Rust runtime
guard rejects `""` AND maps the variant name to "Null…" which is
misleading.

**Always check**: if the Rust `is_empty()` guard rejects an input
that the Java constructor accepts, file as Behavior Mismatch. The
fix is usually "drop the runtime guard, lean on the type system",
or rename the error variant to reflect the actual condition.

### Owned-in / owned-out interceptor signature → unavoidable clone

Java passes records by reference into interceptor chains; Rust's
"owned in, owned out" trait shape forces the wrapper to `.clone()`
the previous good record before each interceptor call so a panic
preserves it (the moved-in value is dropped during unwind).

**Hot-path cost analysis**:
- `K = V = &[u8]` → fat-pointer clone, cheap.
- `K = V = String/Vec<u8>` → real per-message allocation.
- `RecordHeaders` → deep-clones `Vec<RecordHeader>` and inner
  `Vec<u8>` per header — always a real allocation.

**Mitigation menu** (suggest, don't block):
1. Skip the clone when `interceptors.len() <= 1`.
2. `Arc<RecordHeaders>` to make the clone refcount-only.
3. Restructure trait to `&record → Option<modified>` for the
   no-modification fast path.

This trade-off is fundamental to Rust panic safety; raise as
Suggestion only — blocking implies a trait-shape change that
ripples into multiple later phases.

### `Box<dyn Fn>` test seams

Java's package-private overrideable methods (e.g.
`BuiltInPartitioner.randomPartition()`) translate to
`Box<dyn Fn() -> T + Send + Sync>` field. Always check:

1. Is the boxed closure always allocated in production, or only
   under test? (Almost always: always, even in production.) One
   allocation per partitioner instance. Acceptable if not on the
   hot path.
2. Does the field name and rustdoc point at the Java method it
   mocks? Look for "mirroring Java's package-private override
   seam used by ..." comment.

### Race-loser branch testability

`peek_current_partition_info`-style lock-free CAS race-resolve
patterns have a "race-loser" branch that is not exercised by
single-threaded tests. The actor's correctness depends entirely on
manual reasoning about `arc-swap` (or equivalent) semantics. File
as Suggestion if the production caller will exercise it under load.

**Verification checklist**:
- Does `compare_and_swap(None, Some(new))` return `None` on
  success, `Some(prev)` on failure? Confirm by checking
  `~/.cargo/registry/.../arc-swap-X/src/lib.rs`. The answer is yes
  for `arc-swap 1.x`.
- Does the loser-branch use `expect()` or `unwrap()`? An
  `expect()` panic in production from a misunderstanding of the
  CAS semantics is a real hazard — file the test gap as Suggestion.

### Java-`%`-vs-Rust-`rem_euclid` divergence in modular arithmetic

When Java does `random % n` where `random = Utils.toPositive(...)`
(masked non-negative), Rust translation as `random.rem_euclid(n)`
or `random % n` produces identical results — both because
`rem_euclid` ≡ `%` for non-negative dividends.

The divergence only matters if `random` can be negative.
`Utils.toPositive` masks the sign bit, so it can't. Skip filing
unless the random source is overridable in tests AND tests inject
negative values.

### Java `assert` → Rust `assert!` always-on divergence

Java's `assert` is `-ea`-gated (debug only); Rust's `assert!`
always runs. When the actor translates Java's `assert X` to Rust
`assert!(X)`, production behavior diverges:
- Java production: silently disabled, may proceed and crash later
  (e.g. ArrayIndexOutOfBounds).
- Rust production: panics with the assertion message.

Usually an improvement (clearer error). Don't file unless the
Java behavior on the unchecked path is *load-bearing* (very rare).

### `unwrap_or(-1)` for "no partitions" → silent invalid output

When Java's `random % numPartitions` would `ArithmeticException` on
zero partitions, Rust often translates with a guard returning `-1`.
Trade-off: hard failure vs silent invalid output that the caller
must handle. File as Suggestion to nudge for either:
1. `Result<i32, KafkaError>` propagation (invasive trait change).
2. Trait rustdoc documenting `-1` as a possible return (cheap fix).

### Java-test consolidation

Watch for "Java test X collapses two assertions; we split for
cleanliness". Verify:
- Both Java assertions are preserved in the Rust split.
- No edge case is dropped between halves.
- Naming clearly attributes both halves to the original Java test.

Phase 6c's `testStickyBatchSizeMoreThatZero` split is correct: both
`panic on 0` and `no-panic on 1` assertions present in separate
tests.

## Verified-good patterns from Phase 6c (don't reflag)

- `Arc<str>` topic storage in `ProducerRecord` with `topic()`
  returning `&str` and `topic_arc()` returning `&Arc<str>`. The
  `topic_arc` accessor has no Java analogue but is justified by
  CLAUDE.md rule 11 (avoid `Arc::<str>::from(record.topic())`
  which allocates). Document in rustdoc.
- `RoundRobinPartitioner` uses `Mutex<HashMap<Arc<str>,
  Arc<AtomicI32>>>` and clones the inner Arc out before
  `fetch_add`, so the hot path is lock-free. Verified by reading
  `next_value()` slow/fast paths.
- `ProducerInterceptors::on_send_error` mirrors Java's headers
  read-only-clone path including the `is_read_only()` short-circuit.
  Minor over-clone in the already-readonly branch (extra clone,
  no Java analogue) but not a behavior bug.
- `arc-swap::compare_and_swap` returns the previous Guard. To
  detect success, compare against the `current` parameter passed
  in. Phase 6c uses `prev.is_none()` correctly when expected
  current was `None`.
