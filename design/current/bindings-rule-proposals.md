# Proposed rule updates from the Admin bindings work

Three suggested changes to the agent rules, raised by Critic 1 and Actor 1
during Milestone 11 bindings slice B0+B1 and consolidated here by the Manager.

Per `CLAUDE.md`, agents do not edit the prompt or the rules directly —
suggestions go through the `agent-roles.md` process. This document is that
suggestion; applying it is a human decision.

Two of the three would have prevented a review finding in the slice that
produced them. That is the argument for adopting them rather than leaving the
knowledge in a phase record.

---

## 1. `.claude/rules/admin-client.md` §11 — refresh the branch-state caveat

**Status: describes a tree that no longer exists.**

§11 was written while the consumer bindings were unmerged. It states that the
only FFI is `src/ffi/producer.rs`, that it is "fully synchronous", that there is
no `consumer.py` and no async C dispatcher, and no `_ProducerBase`/
`AsyncProducer` split or `_run_sync`/`_run_async` helpers. On that basis it
directs an implementer to mirror the producer's synchronous patterns and to
treat the plan's async wording as an unresolved design conflict, with an
explicit caution not to "invent an unreviewed async dispatcher".

PR #116 merged as `origin/master` `9a35034`. Current state:

| §11 records | Current state |
|---|---|
| no `src/ffi/consumer.rs` | present, ~3785 lines, ~135 exported fns |
| no `consumer.py` | present, ~754 lines |
| no async C dispatcher | `src/ffi/common.rs`: `CompletionJob`, `spawn_dispatcher` |
| producer synchronous only | producer also has 6 `_async` fns and a dispatcher thread |
| no `_ProducerBase`/`AsyncProducer` split | `producer.py` provides all three |
| no `_run_sync`/`_run_async` | present in both `producer.py` and `consumer.py` |

**Proposed:** replace §11's caveat with a statement that the shared async
dispatcher exists in `src/ffi/common.rs` and should be reused, and that the
**consumer** binding shape is the one to mirror (Admin is RPC-oriented rather
than per-record, so the consumer is the closer precedent). Retire the caution
about introducing an async dispatcher.

**Cost of leaving it:** the Manager had to issue a written override in
`design/history/Milestone-11/PLAN-bindings.md` §7/D4 and repeat it in every
Actor and Critic brief for the slice, because otherwise the Actor builds the
wrong shape and the Critic cites the rule to defend it.

---

## 2. New rule — every C-visible `_async` entry point states its callback
   thread contract in full

**Proposed home:** `CLAUDE.md` §3 (C FFI conventions), or a shared FFI section
in the rules if one is created.

> Each exported `*_async` function's rustdoc must state, self-contained, which
> thread its callback may run on and any consequence for the caller. Do not
> factor the explanation into module-level rustdoc and cross-reference it:
> cbindgen copies only item-level rustdoc into the generated header, so a
> "see the module documentation" pointer becomes a dangling reference for the
> only audience that reads the header. Verify the wording in
> `target/include/confluent_kafka.h`, not in the source.

**Why:** this cost a full review round in B0+B1. The first fix used a module
section plus pointers, which read correctly in the source and left nine
dangling references in the header. Producer, consumer and admin currently
document this inconsistently; a rule keeps the three FFIs uniform.

---

## 3. `CLAUDE.md` §9.4 — a Java *timed* join is `tokio::time::timeout`, not a
   bare `.await`

§9.4 currently says:

> When Java uses `thread.join()` or `Future.get()` to block until completion,
> the Rust translation must actually `.await` the corresponding handle —
> setting a flag or dropping a channel is not equivalent to joining.

That is correct for the untimed form and is what the sentence was written
against. It does not cover the *timed* overloads, and the natural reading —
"must actually `.await`" — produces an unbounded wait where Java has a bounded
one.

**Proposed addition:**

> The timed overloads (`thread.join(ms)`, `Future.get(timeout, unit)`) carry a
> bound as part of their contract. Translate them as
> `tokio::time::timeout(duration, handle).await`, not a bare `.await`. Losing
> the bound is a behaviour change even when the untimed translation looks
> faithful — and it is not always benign: an `.await` that Java could never
> block on indefinitely may do so in Rust if the awaited future contains an
> uninterruptible operation.

**Why:** this was a real defect. `KafkaAdminClient.close` uses
`thread.join(waitTimeMs)`; the Rust translation used `let _ = handle.await`, so
`close(timeout)` ignored its own timeout while both the rustdoc and the exported
C header promised otherwise. The parenthetical matters: the overrun path was
`send_eligible_calls` awaiting `Selector::connect` with no timeout, which Java's
non-blocking NIO cannot do — so the Rust translation was unbounded in a way the
Java original never was.

---

## Provenance

Raised in `COMMENTS.1.md` (Critic 1, rounds 1–4) and the Actor's fix-cycle
reports during Milestone 11 bindings slice B0+B1. Items 2 and 3 each
correspond to a defect found in that slice; item 1 to a rule that had to be
overridden in writing before work could start.
