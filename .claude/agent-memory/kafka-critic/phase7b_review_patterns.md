---
name: Phase-7b review patterns
description: Producer trait skeleton review — when the brief contradicts the Java source, and sync-vs-async fan-out for non-async stub methods
type: project
---

Phase 7b reviewed `c63329c` (Producer trait + UnsupportedOperation),
`26073a3` (test gap audit), `f222f20` (memory). Two Suggestion-level
findings; no Blocking. Phase 7a verdict-accepted carried into 7b
cleanly.

## Pattern 1 — brief-vs-Java-source mismatch

The user-provided brief listed three methods as expected on the Java
`Producer` interface that are NOT actually present in the bundled
Apache Kafka 4.2 source at `kafka/clients/.../producer/Producer.java`:

- `initTransactions(boolean keepPreparedTxn)` — only `void initTransactions()` exists
- `prepareTransaction()` — exists only on `internals/TransactionManager.java` (package-private)
- `listTopics()` — does not exist on `Producer` or `KafkaProducer`

The Actor followed two of the three brief items (added
`init_transactions_with_keep_prepared` and `prepare_transaction` to
the trait) and ignored the third (`list_topics` not on the trust —
correct).

**Why:** The brief evidently came from a newer KIP / Confluent-internal
producer version, but the bundled `kafka/` source is the single
source of truth per CLAUDE.md.

**How to apply:** When the user's brief lists Java methods, **always
verify against the actual `kafka/` source** before reporting
test-coverage / API-coverage issues. If a brief says "Java has
methods X, Y, Z" but `kafka/clients/.../Producer.java` only has X
and Y, do not flag the absence of Z as a missing-requirement —
instead flag the *presence* of Z in the Rust translation as a
DoD #7 violation (struct/trait not in Java codebase). The brief is
guidance; the bundled `kafka/` is contract.

## Pattern 2 — sync-Result vs async-Result for stub methods

The Actor declared all transactional methods as **sync** `fn ... -> Result<(), KafkaError>`
because their Milestone-1 bodies just return `Err(UnsupportedOperation)`.
But CLAUDE.md rule 9.1 requires Java-blocking → Rust-async
translation, and the Java methods (`initTransactions`, `commitTransaction`, etc.)
are blocking. So the trait shape is wrong even though the bodies
work fine today.

**Why:** Sync-vs-async stub methods are silently OK at the milestone
where they always error, but become **source-breaking** when the
real impl lands and the body needs to `.await`. Any caller who took
a dependency on the sync signature will have to add `.await` later.

**How to apply:** When reviewing trait-skeleton commits where most
methods are stubs returning `UnsupportedOperation` / unimplemented,
still verify the sync-vs-async shape matches the Java blocking
contract per CLAUDE.md rule 9.1 — don't let "they all return
errors right now" mask a future API-break.

Counter-example where sync-shape IS correct: methods like `metrics()`
that are non-blocking in Java (read-only registry access). Those
can stay sync.

## Pattern 3 — async-fn-in-trait (no `#[async_trait]`)

CLAUDE.md rule 11 ("avoid `Pin<Box<dyn Future>>` per call on hot
paths") drives the Phase 7b decision to use bare `async fn` in trait
syntax (Rust 1.75+) rather than the older `#[async_trait]` macro.
The cost: trait is not dyn-compatible. `Box<dyn Producer<K, V>>`
will not compile.

**How to apply:** When reviewing producer/consumer trait skeletons,
verify:
1. No `#[async_trait]` attribute on the trait.
2. Each `async fn` returns `impl std::future::Future<Output = ...> + Send`
   (the desugared form), or uses bare `async fn` syntax.
3. Module-level rustdoc explains the dyn-incompatibility trade-off
   and points to the generic-bound dispatch alternative
   (`fn run<P: Producer<K, V>>(...)`).
4. Tests use a hand-rolled stub impl with generic-bound dispatch,
   not `Box<dyn Producer>`.

## Pattern 4 — placeholder type alias for deferred return types

`metrics()` returns `Map<MetricName, ? extends Metric>` in Java;
the Rust trait returns `ProducerMetrics = HashMap<String, ()>` as a
placeholder. The alias documents the future swap.

**How to apply:** When a Java return type depends on a not-yet-translated
type, accept a `pub type Foo = HashMap<K, ()>;` placeholder if and
only if:
1. The alias has rustdoc citing what it will become.
2. Existing call sites only use `is_empty()` / `len()` / iteration
   — anything that inspects the value type would silently break
   when the type swaps.
3. The placeholder is in the same file as the trait method that
   returns it, so the swap is local.

## Pattern 5 — test gap audit commits with no new tests added

Commit `26073a3` is a "gap audit" commit: it compares Java `@Test`
cases against existing Rust translations and adds **no new tests**,
only strengthens an existing assertion (error-message text check on
`invalid_records`). This is a legitimate commit shape if:
1. The audit is thorough — every `@Test` is mapped to a Rust test
   citation with line numbers.
2. Any Java tests that lack Rust equivalents are documented (none
   in this commit).
3. The strengthening is per DoD #3 (error message content asserted).

**How to apply:** Don't reflexively flag "no new tests added in a
test-audit commit" as a finding — verify the audit is complete and
the rationale for any deferrals is documented. Then verify the test
strengthening is correct (e.g., the error message string matches
the Java production code byte-exact, not a paraphrase).
