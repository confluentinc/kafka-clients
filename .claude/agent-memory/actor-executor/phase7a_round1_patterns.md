---
name: Phase 7a Round 1 review patterns
description: Lessons from Phase 7a Round 1 fixups — anchor-import reasoning, doc-string parity granularity, fixup-splitting workflow
type: project
---

Phase 7a Round 1 (Suggestions 1-4 + Nits 2/3) fixup patterns:

**`#[allow(unused_imports)] use foo as _anchor;` is a smell.** Round 1
landed two such "anchor" bindings in `producer_config.rs` that the
Critic flagged as dead noise. The deeper issue: when the Critic said
"sasl_configs/ssl_configs are already brought into scope at line 52
and used by the test module's full-path references", the test-module
references actually used full crate paths (`crate::common::config::*`),
which do **not** consume the file-level `use`. Removing the anchors
exposed that the bare module names in the line-52 `use` were never
referenced — they only stayed in scope because the anchors held them.
Lesson: `#[allow(unused_imports)] use ... as _foo;` indicates either
the original `use` is dead, or there is a real macro/trait-resolution
dependency that should be documented in a comment. If neither is
true, drop both.

**Doc-string parity has a public/private gradient.** Java exposes some
DOC strings via `public static final String *_DOC` and others via
`private static final String *_DOC` (used only inside the schema's
`.define(...)` call). The public ones are part of the API surface
(IDE tooltips, HTML doc generators, user code). For the Rust
translation:
- `pub const *_DOC: &str = ...` → must be Java-verbatim per CLAUDE.md
  Rule #4. Use `concat!()` with `<code>` markup preserved.
- file-private `const *_DOC: &str = ...` → can be condensed;
  acceptable to drop `<code>` markup since it's not exposed.

**Milestone-1 deviation in a verbatim doc**: append, don't replace.
For `ENABLE_IDEMPOTENCE_DOC` we kept Java's full text + appended a
`<p>` break + a `Milestone-1 deviation: ...` paragraph pointing at
`Milestone-1/PLAN.md`. Truncating Java's text loses contract; replacing
loses Java's intent. Append-with-`<p>`-separator preserves both.

**Skip rationales must scope to the reachable subset.** Java's
`testTwoPhaseCommitIncompatibleWithTransactionTimeout` sets
`enable.idempotence=true` AND `transactional.id=...` — both rejected
in Milestone-1, so the Round-1 brief skipped the whole test. But the
underlying invariant (2pc=true ⇒ explicit transaction.timeout.ms is
forbidden) is gated on neither rejected setter; it's reachable in
Milestone-1. Pattern: when skipping a Java test, ask "what is the
**production code path** the test exercises? Is *that path* reachable
in this milestone, or is it only the test fixture that's blocked?".
If the path is reachable, port the Milestone-1 subset and update the
TODO to scope the deferred variants precisely. (See also the
Phase-6e/6d echo of this same pattern.)

**Fixup commit splitting via stash.** When a single working-tree diff
spans multiple unrelated fixup targets (e.g. doc-string parity vs
cleanup vs SSL-doc), `git stash push -- <files>` lets you bracket
multi-file changes. For multiple fixups against the **same** file
against **different** parent commits, sequence the edits one fixup at
a time, run all four DoD checks per commit, then move to the next.
Don't try to commit them all at once and split with `--patch`; the
test-per-commit guarantee is easier with sequential editing.

**Validator naming should reflect actual semantics, not Java pattern.**
Java inlines `atLeast(SEND_BUFFER_LOWER_BOUND)` in the `.define()`
call; Rust hoists it to a named local. The Round-1 name
`zero_or_more_send_buffer` was misleading because the bound is `-1`.
Rename to match the **value semantics** (`at_least_send_buffer_lower_bound`)
rather than guessing at a Java-side equivalent that doesn't exist.
Also leave a 4-line comment that mirrors Java's call site so a future
reviewer can find the Java reference.
