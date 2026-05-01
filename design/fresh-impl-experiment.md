# Fresh Implementation Experiment — AI Setup Changes

**Date**: 2026-05-01  
**Purpose**: Evaluate whether updated AI rules and prompts reduce the class of bugs found in the first implementation (PR #10). Two parallel branches run simultaneously so results can be compared fairly.

---

## Motivation

PR #10 (first producer implementation) revealed a recurring pattern of bugs that weren't purely "hard to translate" — they were bugs the AI should have caught or avoided given the right guidance:

- Zero-copy violations (intermediate copy buffers on the send path)
- `tokio::select!` cancellation dropping side effects silently
- `thread.join()` translated as a flag-set instead of `.await`
- Lifecycle tracking objects with `add()` never called (always-false `has_incomplete()`)
- Resource pools returning `new T` instead of the original object on dealloc
- Ownership transfer issues on retry paths (`&mut T` can't be moved into a collection)
- Wire encoding tests only doing round-trips (misses consistently wrong encodings)
- Nullable fields defaulting to `None` instead of empty when spec lacks `"default": "null"`

Rather than relying on the Critic catching these case-by-case, the goal is to encode them as explicit rules so Actor and Critic both know to look for them before code is committed.

---

## What Changed (Applied to Both Branches)

### CLAUDE.md

| Rule | Change |
|------|--------|
| **Rule 2 — Naming** | Added: `i64` (not `u64`) for comparison fields (Uuid, offsets, producer IDs) — signed vs unsigned ordering differs for values with the high bit set |
| **Rule 2 — Naming** | Added: nullable `string`/`bytes` fields default to `Some("")`/`Some(vec![])` unless spec explicitly sets `"default": "null"` |
| **Rule 2 — Naming** | Added: generator must use `field_flexible_versions(field, msg_flex)` per-field, not the raw message-level value — some fields override to `"none"` |
| **Rule 5 — Completeness** | Added: unimplemented Java code paths must fail with a `KafkaError`, not silently complete or hang — explicit error is better than silent wrong behavior |
| **Rule 9 — Concurrency** | Added item 4: `thread.join()` / `Future.get()` → must actually `.await` the handle — setting a flag or dropping a channel is not equivalent |
| **Rule 9 — Concurrency** | Added item 5: translating a callback to async does not drop the callback obligation — fire at the same lifecycle point as Java |
| **Rule 9 — Concurrency** | Added item 6 (Tokio pitfalls): `select!` cancels the losing branch — no side-effectful ops in arms unless cancellation-safe; never hold `MutexGuard` across `.await` |
| **Rule 11 — Optimizations** | Replaced single-line stack note with scoped hot-path guidance: `Arc<str>` for cheap identifier clones, `AtomicI64` over `Mutex<i64>`, no `Box<dyn Future>` per call, no per-message `spawn`. **Qualifier added**: "Outside hot paths, prefer the simpler type (`String`, `Mutex`) unless profiling shows otherwise." |
| **Rule 12 — Public API** | Extended zero-copy requirement through the entire write path: serialize directly into batch buffer, no finalization copies, use `IoSlice`/`write_vectored` for wire sends |

### `.claude/rules/definition-of-done.md`

| Item | Change |
|------|--------|
| **Item 3 — Tests** | Added four sub-checks: (1) per-message-type test files not missed, (2) `@RepeatedTest(N)` → loops not single calls, (3) assert error message content not just `is_err()`, (4) wire protocol types need byte-level encoding tests against known vectors — not just round-trips |
| **Item 10 — Hot-path audit** | New item: for classes on the producer send path, audit for avoidable per-message heap allocations |

### `.claude/agents/kafka-critic.md`

| Change | Detail |
|--------|--------|
| **Model** | `opus` → `claude-opus-4-7` |
| **Performance bar** | Lowered specifically for performance issues: "report if you can point to a specific avoidable allocation or copy, even without measuring the impact. Flag as **Performance** severity." (Correctness bar unchanged — still high) |
| **New design issue checks** | Lifecycle tracking completeness (add/remove/query all wired), resource pool return-paths (dealloc must return original, not `new T`), ownership transfer in retry paths (`&mut T` can't be moved into a collection) |

### `.claude/agents/actor-executor.md`

| Change | Detail |
|--------|--------|
| **Model** | `opus` → `claude-opus-4-7` |
| **Sub-agent spawning guidance** | For large phases with multiple independent classes, Actor should consider spawning a sub-agent per class/group to keep sessions focused and avoid context exhaustion mid-phase. Judgment call: small phases with related classes stay in one session; large phases with unrelated classes benefit from parallelism. |
| **Self-review checklist** | Added: send-path hot-path audit bullet; Tokio pitfalls bullet (select! cancellation, MutexGuard across .await) |
| **Key Translation Rules summary** | Fixed wrong example (`clients::consumer` → `consumer`); added: select! cancellation safety, zero-copy write path, callback lifecycle point requirement |

### `.claude/agents/project-manager.md`

| Change | Detail |
|--------|--------|
| **Model** | `opus` → `claude-opus-4-7` |
| **Step 7 cleanup** | Removed stale references to `design/current` and `marked_classes.txt` (both deleted); kept `design/history/` references |

### Agent memory deleted

Both Actor and Critic memory files (`.claude/agent-memory/`) were deleted before starting. Rationale: keeping prior memories would confound the evaluation — we couldn't tell whether improvements came from the updated rules or from accumulated memory. Any generalizable patterns from those memories were first extracted into the rule files above.

---

## Branch A: `fresh-impl` (Baseline)

**Worktree**: `/Users/shivsundarr/dev/njc-spike/example-confluent-kafka-rust`  
**Agents**: project-manager, actor-executor, kafka-critic  
**Setup**: Updated rules above applied. No Java Analyzer. No prior implementation code or agent memory.

This is the control branch — measures the effect of the updated rules alone.

---

## Branch B: `fresh-impl-java-analyzer` (With Java Analyzer)

**Worktree**: `/Users/shivsundarr/dev/njc-spike/example-confluent-kafka-rust-java-analyzer`  
**Agents**: project-manager, actor-executor, kafka-critic, **java-analyzer** (new)

**Additional changes vs. Branch A:**

### New agent: `.claude/agents/java-analyzer.md`

A new subagent that reads Java source and produces a structured translation brief **before** the Actor starts. The brief covers:

- Source file location and Rust target path
- Fields table (name, type, access, notes)
- Per-method table (Java signature, Javadoc summary, translation notes)
- Dependencies (Kafka classes used, flagging any not yet translated)
- Test files with every test method listed by name
- Translation risks: async/callbacks, thread safety, wire protocol, ownership, Java-specific behavior

Output: `design/history/<Milestone>/<Phase>/java-brief.md`

**Why**: The Actor spends significant effort reading unfamiliar Java source mid-task. The brief front-loads this into a dedicated pass that can be more thorough and that the Actor then uses as a checklist — reducing missed methods, missed tests, and missed risks.

### project-manager.md: Step 0.5

Before spawning the Actor for each Phase, the Manager now spawns the Java Analyzer first and waits for the brief to complete. The brief path is passed to the Actor.

### actor-executor.md: Brief-first reading

The Actor is told to read `java-brief.md` first if it exists, and open Java source directly only when the brief is unclear on something.

---

## What Was Considered But Not Applied

| Option | Decision |
|--------|----------|
| **Multiple Critics in parallel** | Not applied — adds coordination complexity, risk of duplicate/conflicting comments. May revisit if single Critic proves too slow. |
| **Critic specialization** (one for correctness, one for perf) | Not applied — too much overhead for a two-branch comparison. Single Critic with a lowered performance bar is simpler to evaluate. |
| **Pre-seeding Manager with milestone plan** | Not applied — Manager reads Java source and creates the plan from scratch each run. Keeps the experiment clean; avoids carrying over assumptions from the first implementation. |
| **Persisting session state across restarts** | Not possible — Claude sessions die on process exit. Recovery relies on git history + COMMENTS files. Plan approval is the last human gate; Actor/Critic loop is fully autonomous after that. |

---

## How to Evaluate Results

After both runs complete, compare:

1. **Bug count from Critic** — how many issues did the Critic find per Phase?
2. **Fix cycle count** — how many Actor→Critic loops per Phase before COMMENTS.N.md cleared?
3. **Bug categories** — do the specific bug classes targeted by new rules (zero-copy, select! cancellation, lifecycle, pool return-paths) appear less or more?
4. **Brief quality** (Branch B only) — did the java-brief.md actually surface risks the Actor used?
5. **Total time** — how long did each branch take end-to-end?

Results land in `design/history/Milestone-X/Phase-Y/COMMENTS.DONE.N.md` per phase.
