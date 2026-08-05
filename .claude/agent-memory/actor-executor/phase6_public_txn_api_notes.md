---
name: phase6-public-txn-api-notes
description: Milestone-11 Phase 6 lessons — shared PendingRequests, the 4.2 prepareTransaction correction, tokio timer starvation from a non-yielding task, and the two test-driver shapes
metadata:
  type: project
---

Milestone 11 Phase 6 (`Producer` trait + `KafkaProducer` transactional API + Sender
`sendProduceRequest` + tests). Landed as five commits on
`milestone11-producer-transactions`.

**A rule premise can be true of every existing caller and false of the class.**
`.claude/rules/producer-transactions.md` §2 called `pendingRequests` Sender-confined.
It is, for every caller that existed through Phase 5b — and wrong for the class: all
four blocking public methods enqueue into it from the *application* thread. The check
that would have caught it earlier is "grep the Java field's writers across threads",
not "grep the Rust callers". Resolution: `Arc<Mutex<PendingRequests>>` kept *outside*
the manager, lock order `pending_requests` → manager.

**Why to bind a lock guard to a local.** Rust evaluates a method *receiver* before its
arguments, so `manager.lock().unwrap().m(&mut other.lock().unwrap())` acquires in the
opposite order from how it reads. Any two-lock site must bind the first guard to a
local.

**Re-derive plan claims against the corpus, not against the inference.** §Phase-6 said
`prepare_transaction` was "on `KafkaProducer` only, not the `Producer` interface —
verified: grep Producer.java returns nothing". The grep was real; the conclusion was
not. 4.2 has `prepareTransaction` only on `TransactionManager`. Implementing it would
have added a method with no Java counterpart (DoD §7). Same failure class as PLAN
§9.19's stale rationales.

**Tokio: a task that never awaits anything pending starves every timer in the
runtime.** `Sender::run` over a `MockClient` never yields (`MockClient::poll` returns
immediately). Spawned with `tokio::task::spawn` onto a test's multi-thread runtime, it
deadlocked the whole runtime: only a *worker* parks on the time driver, and the sole
awake worker was inside that task, so no timer anywhere fired — the test's own `sleep`
never returned. Diagnosed with `sample <pid>`: one worker spinning in `run_once`, the
other in `park_condvar`. Fix for tests: give such a task its own OS thread and its own
current-thread runtime via `spawn_blocking` + `Builder::new_current_thread`. A real
`NetworkClient` cannot trigger it — its `poll` awaits the selector. `sample` on a hung
`cargo test` process is the fastest way to tell "spinning" from "blocked".

**Two test-driver shapes, pick by what the test's subject is.**
  - *Subject inside `run_once`*: keep the `Sender` test-owned and run the application
    call concurrently with a `run_once` loop via `tokio::join!` (never `select!`, which
    drops the loser). Full control of the `MockClient` between iterations.
  - *Subject inside `Sender::run`'s tail* (shutdown / force-close): the Sender must
    actually be spawned, so every response has to be queued up front and the mock is
    unreachable afterwards.

**`MockClient::prepare_response` vs `respond`.** `prepare_response` is matched when a
request is *sent*; a request already in flight can only be answered with `respond`.
Preparing after the send silently never matches and the test hangs.

**A "close returns" test can be much weaker than it looks.** Java's three
`testCloseIsForcedOn*` end with `assertionDoneLatch.await(5000, ..)` and **discard the
boolean**, so they do not require the pending call to have returned — and it cannot
have, because the in-flight request was already dequeued from `pendingRequests` and
`TransactionManager.close` has nothing to fail. Read what an ignored return value
implies before asserting more than Java does.

**Bug the port had and Java did not:** `await_sender_handle` passed the `JoinHandle` by
value to `tokio::time::timeout`, so an expiry dropped it and the follow-up
"indefinite join" had nothing to join — `close(Duration)` returned with the Sender still
running. Await `&mut handle` (`JoinHandle` is `Unpin`) and put it back.

**And the sharper lesson, from Critic 46: "the tests that reach the path" is not "the
tests that pin it".** The three `testCloseIsForcedOn*` translations reach the
force-close-after-timeout path and were claimed as the bug's cover — but they *pass with
the bug reintroduced*, because losing the handle makes `close` return **sooner**, and
every assertion there is an upper bound on elapsed time. When claiming a test covers a
fix, reintroduce the fix's inverse and watch it fail; a test that merely executes the
line proves nothing. The pins now are a mechanism test (an expired wait leaves the handle
`Some`) and a contract test (`close` cannot return before the Sender's exit hook has
run), both mutation-checked.

**Error assertions:** `KafkaError::error()` returns `UnknownServerError` for the
non-`Generic` variants (`Timeout(String)`, `RecordTooLarge(String)`, `InvalidTopic(..)`).
Assert the variant with `matches!` plus the message, not `error()`.
And a `commitTransaction` after a failed send reports Java's *bare* `KafkaException`
("Cannot execute transactional method because we are in an error state"), not the send's
own error — `maybeFailWithError` wraps `lastError`.

**Outstanding at hand-off:** most of the transactional `SenderTest` group, owner Phase 8,
with per-method reasons in the accounting block at the end of
`src/producer/internals/sender.rs`. That block derives its counts from a shipped program
and is the only place to read them from — an earlier version of this note carried "11 of
the 15", which its own phase then falsified by adding a sixteenth (Critic 46 pass 2
issue 2). **Do not copy a derived count into a note; cite where it is derived.** Two
suggested rule amendments are recorded in PLAN §10.9 (rules §2's `pendingRequests`
classification; CLAUDE.md §9.6.6's timer-starvation pitfall).

**Three sweep lessons, each learned by failing it in this phase's review loop.** Changing
a derived count is never a one-site edit, and "I swept for it" is only as good as the
shapes the sweep enumerated:

  - *A correction has to be swept too.* I refused to propagate a false fact into the
    section where it was disputed and then restated it in the section a reader hits first.
    Grep the claim, not the section you are editing.
  - *Restatements hide in cross-references, not only in headline repeats.* Sweeping for
    the numbers themselves (`27|28|15|16`) found four sites and missed a fifth phrased as
    "for the same reason **the 15 above** are" — a reference to a *group's size*, which
    matched no group even before the renumber. Also sweep `the <N> above|below`, `(N)`
    group headers, and `<N>-arithmetic`-style labels.
  - *The checker inherits the blind spot the checked text warns about.* I wrote a header
    sweep whose range regex required a closing paren, reported "40 headers", and three
    sentences below that count the same paragraph explains that Critic 44 issue 10 escaped
    an earlier sweep **because its range is followed by a clause inside the same
    parentheses rather than closing them**. Exactly one header has that shape, and my
    sweep could not see it — the population was 41. When a paragraph documents an escape
    shape, the first thing to test the new checker against is that shape; the warning is a
    ready-made adversarial input sitting in the same file.

Concretely, for any sweep over rustdoc/comment text: make it **wrap-tolerant** (join the
contiguous `///` block — "Translated from" routinely ends a line with the name on the next,
and a single-line pattern saw 24 of 41 here), **paren-tolerant** (never require the closing
delimiter after the payload), and have it check **every field the convention constrains**
(a start-only range check makes end errors structurally invisible — two of the five real
defects were end-only or found only by checking ends). Then paste the pattern *and* run it
literally as written, so the shipped recipe reproduces the shipped number.

The durable fix for both is the same and is now applied throughout: **cite where a number
is derived instead of restating it.** A cross-reference that names the group ("the STILL
OWED group above") cannot go stale; one that states its size can.
