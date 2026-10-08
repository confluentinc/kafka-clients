# Critic 71 — Review of commit 253e85cc (resolved items)

Fixup for `253e85cc` (`fix(producer): record transactional latency metrics`).
Issues 1 and 2 from `COMMENTS.71.md` are resolved here. Issue 3 (pre-existing,
out of scope by the Critic's own statement) and the Rules suggestion remain in
`COMMENTS.71.md`.

Gates after the fix: `cargo build` OK; `cargo test --lib` 3917 passed / 0 failed
/ 3 ignored (incl. `kafka_producer::tests::test_measure*` 2/2 and
`kafka_producer_metrics` 8/8); `cargo xtask format` + `format-check` clean;
`cargo xtask lint` (clippy + doc-hygiene) clean.

---

## Issue 1: New docs mislabel `now_nanos()` as "the wall clock" (it is a monotonic Instant clock) — RESOLVED
- **File**: `src/producer/kafka_producer.rs:802`, `:4993`, `:5027`, `:5073`, `:5079`, `:5662`; also the commit message ("kept wall-clock timing via now_nanos()")
- **Severity**: Behavior Mismatch (documentation) — LOW
- **Java Reference**: `KafkaProducer.java:662` (`time.nanoseconds()` == `System.nanoTime()`, monotonic)
- **Description**: The commit's new comments repeatedly describe the clock behind
  `now_nanos()` as "the wall clock". It is not. `now_nanos()` (line 690) calls
  `crate::common::metrics::time::SystemTime::nanoseconds()`, which is
  `NANO_ORIGIN.elapsed().as_nanos()` — a **monotonic `Instant`** reading.
  `now_nanos()`'s own rustdoc, `src/common/metrics/time.rs:27` ("Do NOT treat it
  as a wall-clock timestamp."), and the enforced codebase convention (where "wall
  clock" means `System.currentTimeMillis()` / millis, which can step backwards)
  all contradict the new text. The soundness of the `>= 1.0` / strict-growth
  floors depends on the clock being **monotonic**; the mislabel undercuts the
  very reasoning that makes those floors safe. Violates CLAUDE.md §4.

**Resolution:** Corrected all six mislabeled comment sites in
`src/producer/kafka_producer.rs` — `:802` (cross-reference), the
`get_and_assert_duration_at_least` helper doc, the
`test_measure_abort_transaction_duration` doc, the
`test_measure_transaction_durations` doc + in-body comment, and the module-tail
summary comment. Each now names the source a **monotonic clock (`Instant`, the
analog of `System.nanoTime()`)**. At `:802`, where the point is the contrast with
Java's injected `time_provider` / `MockTime`, it reads "the real, un-mocked
monotonic clock (`Instant`, the analog of `System.nanoTime()`)", so the
cross-reference no longer contradicts `now_nanos`'s rustdoc. Lines **198** and
**235** (the *millisecond* `time_provider`, genuinely `System.currentTimeMillis()`
wall-clock) were left unchanged, per the brief. `grep "wall clock\|wall-clock"`
over `src/producer/internals/kafka_producer_metrics.rs` found nothing — no leak
there. The `253e85cc` commit message still reads "kept wall-clock timing"; that
cannot be changed without rewriting history (no amend), so the correction is
stated plainly in this fixup's commit message instead.

---

## Issue 2: `txn-begin-time-ns-total` floor assertion is a latent flake on the synchronous begin path — RESOLVED (option a)
- **File**: `src/producer/kafka_producer.rs` — `test_measure_transaction_durations` (asserted `txn-begin` `>= 1.0`, then round-2 strict growth `>` first); floor logic + doc in `get_and_assert_duration_at_least`
- **Severity**: Bug (test fragility) — LOW
- **Java Reference**: `KafkaProducerTest.getAndAssertDurationAtLeast` (1841-1845), which asserts `>= tick.toNanos()` (1e9) under MockTime auto-tick — deterministic; the Rust floor cannot use that and previously fell back to `> 0`.
- **Description**: Four of the five metrics bracket an **awaited round trip**
  (pumped by `drive`), so their delta is comfortably `> 0`. `txn-begin` is the
  outlier: `begin_transaction` times a purely **synchronous** body (one
  uncontended mutex acquire + an enum state transition, tens of ns). Rust's
  `Instant` is guaranteed **non-decreasing, not strictly increasing**; on a
  platform whose monotonic tick granularity exceeds the bracketed work, two reads
  can return the same value → 0-ns delta → `assert!(value >= 1.0)` fails. The
  helper doc's "always advances" was an assumption about clock *resolution*, not a
  guarantee. (Critic's own 40-run sweep: 0 failures — a latent, low-probability
  risk, not a demonstrated failure.)

**Resolution — option (a):** For `txn-begin-time-ns-total` only,
`test_measure_transaction_durations` now asserts `>= 0.0` in round 1 and
non-strict `>= begin_first` in round 2 — the only levels `Instant` actually
guarantees. On Apple Silicon `mach_absolute_time` ticks at 24 MHz (~41.7 ns) and
the begin body is tens of ns, so a 0-ns delta is plausible; the old `>= 1.0` /
strict-growth asserted a guarantee that does not exist. The other four metrics
bracket an awaited round trip (microseconds of real work) and keep their `>= 1.0`
/ strict-growth floors. The `get_and_assert_duration_at_least` helper doc and the
`test_measure_transaction_durations` doc were rewritten to state the true
guarantee level (non-decreasing, resolution-dependent) and why begin is special.

**Honest trade-off (stated in the test doc):** with `>= 0.0` / non-strict, this
test no longer gives a deterministic *runtime* proof of the begin call-site
wiring — a broken `begin_transaction` → `record_begin_txn` wire would leave the
cumulative metric at 0.0 and still pass. That sensor plumbing is independently
proven by `kafka_producer_metrics::tests::should_record_tx_begin_time`, which
drives `record_begin_txn` directly and asserts the metric.

**Why (a) not (b):** (b) keeps `>= 1.0` and bets on clock resolution; running the
test 200× with zero failures cannot disprove a rare same-tick event the analysis
itself calls "plausible" — it only masks the flake. (a) removes the root cause
deterministically (assert only what the clock guarantees). The (b) measurement
was deliberately not run.

**Principled long-term fix (out of scope for this fixup):** thread an injectable
nanosecond clock through the producer — give the ms-only `time_provider` a
`nanoseconds()` and make `now_nanos()` read it — so a test `MockTime` with
`set_auto_tick` could drive it and restore Java's deterministic
`>= tick.toNanos()` floor for begin. That changes `time_provider`'s type, every
constructor, and `flush` / `metadata-wait` timing semantics, so it belongs in its
own piece of work, not here.

---

## Issue 4 (introduced by `33c07717`): ordinary-English "exception" in a non-Java doc comment — RESOLVED
- **Resolution**: Reworded the `get_and_assert_duration_at_least` rustdoc (`kafka_producer.rs:5008`, "The exception is `txn-begin-time-ns-total`" → "The outlier is …") and the mirror phrasing in this file's Issue 2 note ("`txn-begin` is the exception:" → "outlier:"), so the only remaining "exception" tokens are Java-type / `TestUtils.retryOnExceptionWithTimeout` references permitted by CLAUDE.md §2; a working-tree sweep of Rust source (`git diff master -- '*.rs' | grep '^+.*exception'`) is clean — the moved finding text below necessarily quotes its own subject word.
## Issue 4 (NEW, introduced by `33c07717`): ordinary-English "exception" in a non-Java doc comment
- **File**: `src/producer/kafka_producer.rs:5008` (in `get_and_assert_duration_at_least`'s rustdoc)
- **Severity**: Behavior Mismatch (convention) — LOW / borderline
- **Rule**: CLAUDE.md §2 — "the word 'exception' MUST never appear in Rust code, except in comments about the Java client." (DoD #1.)
- **Description**: The fixup's helper-doc rewrite added
  `/// realistic tick ... are safe for them. The`
  `/// exception is `txn-begin-time-ns-total`: ...`.
  Here "exception" is ordinary English ("the special case / outlier is
  txn-begin"), and the sentence is about the *Rust* test's clock behaviour, not
  the Java client — so it is not covered by the §2 carve-out. Every other
  "exception" in this file (lines 414, 700, 1295, 1304, 2086, …) names a Java
  type, which the carve-out permits; this is the only non-Java-referring use,
  and it is newly introduced. The doc-hygiene lint does not catch it.
- **Note on polarity**: This is the *letter* of §2. The rule's *intent* is to
  keep Java error-terminology out of Rust error handling, and "the special case
  is" is not that — so the Manager may reasonably read §2 narrowly and waive
  this. Flagged only because the review brief explicitly asked to check for
  "'exception' outside Java-referring comments", and this is one, newly added.
- **Expected**: Reword to avoid the banned word, e.g. "The **one outlier** is
  `txn-begin-time-ns-total`" or "The **special case** is …".
- **Actual**: "The exception is `txn-begin-time-ns-total`: …".
