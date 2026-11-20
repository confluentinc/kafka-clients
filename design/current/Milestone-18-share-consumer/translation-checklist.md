# Milestone 18 translation checklist (pilot rules R1–R8)

**Scope.** Same as [`.claude/rules/share-consumer.md`](../../../.claude/rules/share-consumer.md):
files under `rust/src/consumer/` (including `internals/`, `internals/events/`,
`internals/metrics/`) whose names match `*share*` or `acknowledg*` (so
`kafka_share_consumer.rs`, `mock_share_consumer.rs`, `dyn_share_consumer.rs`,
`internals/metrics/kafka_share_consumer_metrics.rs`, every `share_*` file and
`node_acknowledgements*`), the share request wrappers under
`rust/src/common/requests/`, and the share edits to shared consumer
infrastructure. Nothing else.

**Status: pilot.** These rules come from PLAN §6.2 and are adopted per PLAN §6.4.
They deliberately live outside `.claude/rules/` so they do not bind agents working
on producer, admin, .NET or JavaScript. CLAUDE.md and `definition-of-done.md` stay
unchanged until the milestone ends (P11 promotes the rules that held up, with team
review). A Critic comment that relies on one of these rules is tagged with its
number (`[R3]`), so the P1/P2 canary can count rule-driven false positives
(`COMMENTS.FP.md`).

Evidence IDs refer to `findings[].id` in the consumer parity audit
(`consumer-audit-prs/consumer-findings.json`, field `why_missed`).

## R1 — Assertion fidelity

**Rule.** Every Java assertion in a translated test has a Rust counterpart. A
translated test does not swap inputs (record magic, list order, error class), widen
the set of outcomes a fixture accepts, or drop an `assertFalse(future.isDone())`,
request-count or timing check, unless a deviation note at the test says why. The
Critic diffs assertions per test, Java against Rust.

**Applies to:** every test translated from a Java share test file (unit and
integration), and to share cases added to shared test files.
**Does not apply to:** Rust-only tests (R8 covers them); assertions on Java-only
mechanics with no Rust analogue (thread interruption, JMX), which are skipped with a
note.

**Over-application to avoid:** a Java timing assertion (`time.sleep`, a timer
expiry) may be adapted to the Rust test clock with a note at the test. It is not
ported as a literal wall-clock sleep. A Java test that cannot compile in Rust
(it relies on a Java runtime type error, e.g. `FetchSessionHandlerTest#testDoubleBuild`
against a builder that `build_request` consumes) is skipped with a one-line reason;
no compile-fail test is required.

**Evidence:** C1-27 (fixture widened to accept `NetworkError`), C2-01 (dropped
`assertFalse(leaveResult.isDone())`), C3-02 (v0 input swapped for v2), C3-03
(dropped `timeToNextUpdate` assertion), C4-02 (dropped backoff step), C4-06 (fixture
tolerates `UnknownServerError`), C4-07 (loops sending retries instead of asserting 0
requests), TX3-02 (test exercised a twin, not the trait method).

## R2 — Deferral ledger

**Rule.** "Deferred to Phase N", "documented carry-over", a stub closure, or a
setter production never calls counts as a FIXME (DoD §8). Each one gets a row in the
**Deferral ledger** at the end of `ledger.md` (item, where in the code, owning phase,
reason). A phase cannot close while it owns an open row.

**Applies to:** any share-scope code that leaves a Java path unimplemented or
partially wired.
**Does not apply to:** pending design decisions listed in PLAN §7 (they are tracked
there), and to Java paths that are out of scope by an explicit rule (e.g. KIP-714
until D2 decides).

**Over-application to avoid:** a CLAUDE.md §7 "fail with an explicit `Error`" stub
for a genuinely out-of-scope path still needs a row; it is not forbidden.

**Evidence:** C1-04, C1-12, C3-03, C3-05 (FIXME behind injected closure never
revisited), C3-06 (constructor parameter became a never-called setter), C5-01,
C5-04, C5-09 ("deferred to Phase 11" never picked up).

## R3 — Production-path construction

**Rule.** Every config key and every Java constructor parameter that the share
code reads is exercised by at least one test that builds the object through its
production constructor (the one production code calls), not through a test-only
setter or a hand-built field.

**Applies to:** `ShareConsumerImpl`, the share request managers, `ShareFetchConfig`,
`ShareConsumerConfig`, metrics managers — anything whose production constructor
takes config or collaborators.
**Does not apply to:** pure value types whose only constructor is the test's.

**Over-application to avoid:** "production constructor" means the public/crate
constructor driven over `MockClient` (or the existing mock network fixture). It does
not mean adding broker-backed unit tests, or adding test seams to production code.
A test-only setter **is** allowed when it is the faithful port of a Java test-only
override (a subclass or anonymous override in the Java test, e.g.
`FetchCollectorTest#testErrorInInitialize`), with a note at the setter.

**Evidence:** C2-07 (`client.rack` parsed, never wired; tests passed the rack
directly), C3-06, C1-16 (fixture calls a setter production never calls), C5-01.

## R4 — Callback → async checklist

**Rule.** When a Java callback, `CompletableFuture` chain or thread loop becomes
async Rust, check each of:

  1. read the clock at completion, not when the task is spawned;
  2. preserve completion order. Flag an unordered per-callback `spawn` only when
     the completion order is observable and nothing downstream restores Java's
     order; trace where the order is restored (e.g.
     `AsyncKafkaConsumer::register_async_commit` chains each commit continuation
     on its predecessor) before reporting;
  3. wake the background task when re-enqueuing work it must see;
  4. do not collapse a `CompletableFuture` chain so that when `isDone()` becomes
     true changes;
  5. map lock **order**, not only reentrancy: for every pair of locks held at the
     same time, check both acquisition orders across all paths (ABBA), including
     a lock taken inside a getter that Java leaves unsynchronized; and every
     nested path where a Java reentrant monitor becomes a non-reentrant
     `std::sync::Mutex`;
  6. a loop translated from a Java thread loop that can iterate without awaiting
     anything that can pend must yield, and is tested on a `current_thread`
     runtime;
  7. racing continuations: reason about interleavings at every `.await` point and
     at loop-iteration boundaries. The runtime flavour is **not** an exemption.
     In production the consumer bg loop runs on a dedicated `current_thread`
     runtime (`ConsumerNetworkThread::spawn_dedicated`, `new_current_thread`);
     with `BgJoin::Spawned` (unit tests, `with_components`) it is spawned on the
     caller's runtime, which may be multi-threaded. Even on `current_thread`, a
     spawned continuation interleaves with the loop at each await and across
     iterations;
  8. a Java `finally` (or `try/finally` state reset) around a call that becomes
     an `.await` needs a drop guard, because the future can be cancelled; test it
     by dropping the future mid-await **and asserting what the next call
     returns**, not only what the guard restored. M18 P3 (Critic 251 Issues 1
     and 2): both drop tests passed while the next call skipped the interrupted
     record (acknowledging it as a gap, which the broker archives) or lost the
     renewals the dropped call had not redelivered.

**Applies to:** share code translating a Java callback, `whenComplete` /
`thenApply` chain, `synchronized` section reached re-entrantly, or a Java thread
loop.
**Does not apply to:** straight-line synchronous code.

**Over-application to avoid:**
  - item 6 does not apply to per-record decode or send loops (never add
    `yield_now()` there), nor to a loop that awaits I/O on every iteration
    (e.g. `ConsumerNetworkThread::run`, which awaits the selector poll each
    time);
  - item 2: per-callback spawns whose order is restored downstream are not a
    finding;
  - item 3 uses `ApplicationEventHandler::wake_background_task`, never
    `WakeupTrigger`: the wrong primitive returns a spurious `Wakeup` to the caller
    (consumer-threading.md §31).

**Evidence:** C1-01 (busy loop froze a `current_thread` runtime), C2-01 (chain
collapsed), C2-05 (racing reconciliations after a spawn; the backtest withdrew it
on a wrong "`current_thread` cannot race" assumption, hence item 7), the #277
poll-vs-metadata ABBA deadlock (`FetchCollector::update_partition_state` held
`SubscriptionState` then took the `Metadata` lock; `Metadata::update` holds
`Metadata` then locks `SubscriptionState`; Java's `Metadata#metadataExpireMs` is
an unsynchronized getter — item 5), M18 P2 review (`AcknowledgementCommitCallbackHandler#onComplete`'s `finally` reset of `enteredCallback`, lost when the future was dropped — item 8), C2-06 and C4-09 (clock read
at spawn), C4-03 (reentrant monitor → `Mutex` deadlock), C4-08 (callbacks out of
order), C4-14 (missing wake after a channel hop), C3-04 (value moved out under a
concurrent reader).

## R5 — Java semantics table

**Rule.** Follow Java's exact collection and value semantics:

| Java | Rust |
|---|---|
| `LinkedHashMap` (incl. `remove`) | `IndexMap` + `shift_remove` (never `swap_remove`) |
| `TreeMap` / `TreeSet` / `SortedSet` | `BTreeMap` / `BTreeSet` |
| a sorted output | stays sorted |
| `long` ms arithmetic that Java clamps | saturating arithmetic; test `Duration::MAX` |
| `Math.addExact` and other throwing arithmetic | return the error |
| `String.trim` | trims chars `<= ' '`, not `str::trim` (Unicode whitespace) |
| `Matcher.matches` | anchored full match, not `Regex::find` |
| `equalsIgnoreCase` / case-sensitive `equals` | the same case rule (e.g. `ShareAcknowledgementMode#fromString` is case-sensitive, `ShareAcquireMode#of` is not) |

Tests use non-trivial orders (not only ascending, not only `[unchanged, altered,
new]`).

**Applies to:** share-scope code. In the share closure this concerns at least
`ShareSessionHandler` (`LinkedHashMap` session/next partitions),
`Acknowledgements` (`TreeMap`), `ShareInFlightBatch` (`TreeMap`, `TreeSet`), and
`ShareFetch#takeAcknowledgedRecords` (`LinkedHashMap`).
**Does not apply to:** existing code outside the share files. No retrofitting.

**Over-application to avoid:** saturate only where Java clamps; where Java throws,
return the error. Do not replace a Java `HashMap` with an ordered map "to be safe".

**Evidence:** C3-01 (`swap_remove`), C2-03 (`SortedSet` → `Vec`), C2-04 (unsorted
callback partitions), C1-03 (`as i64` wrap), C1-08 (`trim`), C4-11
(`Matcher.matches` vs `find`), C5-06 (`equalsIgnoreCase`).

## R6 — `close()` is explicit

**Rule.** Every step of Java's `close()` happens in Rust `close()`, in Java's order
(`ShareConsumerImpl#close(Duration, boolean)`): disable wakeups, send
acknowledgements and leave the group, stop finding the coordinator, handle completed
acknowledgements, process background events, close the application-event handler,
reap, then close metrics, deserializers and the telemetry reporter.

**Applies to:** `ShareConsumerImpl`, `KafkaShareConsumer`, `MockShareConsumer`, and
any share manager with a Java `close`.
**Does not apply to:** resources Java does not close explicitly.

**Over-application to avoid:** a `Drop` safety net may stay; it is just not where
Java's steps live.

**Evidence:** C1-30 (deserializers closed only by `Drop`).

## R7 — Rules are checked against Java

**Rule.** A rule's claim about Java (call sites, messages, behaviour) cites
`Class#method` and is verified against Java 4.4.0 before adoption. Three kinds of
claim need the method body read, not just the method name:
  - "X does / does not call Y" names X, the Java method whose body was read;
  - "X exists" / "X overrides Y" is checked in the class itself, not inferred from
    a base class or a sibling;
  - a quantified statement ("every method …", "every other …", "all paths …")
    is written as an explicit list of the Java methods it covers.

If Java contradicts a rule, the agent escalates to the Manager and does not follow
the rule.

**Applies to:** `share-consumer.md`, this checklist, phase PLANs, Critic
comments that assert Java behaviour, and **deviation notes in code and tests**. A
deviation note is a claim about Java (and about the Rust code it excuses) and must
stay true; a stale note is a finding (backtest S5-1: a test note claimed the
`CompletedFetch` abort-marker path was untranslated after it had been
translated).
**Does not apply to:** the *choice* behind a documented deviation that carries a
*Why* (the async API, the panics policy, Rust-only types). "Java wins" is about
claims of Java behaviour, not about those choices; the facts a deviation note
states are still checked (see above).

**Over-application to avoid:** on a contradiction the agent escalates; it does not
rewrite the rule or the code on its own.

**Evidence:** VC-01 (consumer-threading.md §31 misstated Java's call sites), C5-03
(§20's "accept silently" contradicted Java 4.3.1). P0 itself found two plan claims
that the 4.4.0 code contradicts (see the P0 report): `unsubscribe` does not call
`processBackgroundEvents()`, and `ShareAcknowledgementMode` parsing is
case-sensitive. The P0 review (Critic 201 items 1, 3, 4) found three more:
a cited `ShareConsumeRequestManager#pollOnClose` that does not exist, an
"every other method" closed-check claim that was false for `#metrics` / `#wakeup`,
and a close path described as acknowledging records it actually releases. This
prompted the three sub-rules above.

## R8 — Rust-only types get a test plan

**Rule.** A type with no Java body to diff (a handle, an adapter, a sealed dyn
companion) needs an explicit test plan in the phase PLAN, reviewed by the Critic
before the code. The plan names each behaviour the type must have and the test that
pins it, including the mock variant.

**Applies to:** e.g. `DynShareConsumer`, a `ShareConsumerHandle` if D6 adopts one,
any share adapter between Java's interface shape and Rust's.
**Does not apply to:** types translated from a Java class.

**Over-application to avoid:** a small private helper struct inside a translated
method does not need a plan.

**Evidence:** C1-15 (`ConsumerHandle` commits stall without `group.id`), C5-24
(the `MockConsumer` handle variant was stubbed).
