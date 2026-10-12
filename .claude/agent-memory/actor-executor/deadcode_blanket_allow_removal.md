---
name: deadcode-blanket-allow-removal
description: How to remove a stale file-level #![allow(dead_code)] under #![deny(warnings)] without deleting translated methods
metadata:
  type: feedback
---

Removing a blanket `#![allow(dead_code)]` from a file under `#![deny(warnings)]`
(the crate sets it in `src/lib.rs:16`): do NOT re-add a blanket allow, and do NOT
delete translated methods (DoD #2 keeps them). Put a **targeted** `#[allow(dead_code)]`
on each surfaced item with an accurate justification.

**Why:** stale blanket allows hide the real dead-code picture and carry false
"callers aren't landed yet" comments (the M7 finding). Targeted allows are honest
and reviewable.

**How to apply:**
- `cargo build --all-features` catches the LIB build; `cargo xtask lint` (clippy
  `--all-targets`) also builds `lib test` + examples and catches items dead there
  (e.g. an unused `#[cfg(test)]` constant that the blanket allow was masking).
- rustc surfaces dead items in **cascading passes**: silence one batch and the
  next batch appears on the next build. Rebuild until clean — do not assume the
  first build listed everything.
- Every flagged item has **zero production callers by definition** (the lint only
  fires when unreachable in that build config). So categorize each: has unit-test
  callers → justification "exercised only by this crate's tests"; no caller
  anywhere → "translated for parity (DoD #2); no caller in this crate yet — <Java
  method>". Distinguish with `rg -n '\.method_name\(' src/ tests/` and check the
  caller line is below the `#[cfg(test)] mod tests` boundary.
- Genuinely-dead **test scaffolding** (an unused test-local const referenced by no
  test) is not a translated method — remove it rather than keep it alive behind an
  allow.
- Removing `ok &= report(...)` reassignments can make a `let mut ok` no longer need
  `mut` — clippy `-D unused_mut` will flag it.

**Cherry-picking such a commit across a feature revert (M11-fixes → CFFI, loop 64):**
the dead-code SET differs between source and target branch, so the picked allow-set
is wrong on arrival. A method live on the source branch (there the original commit
correctly left it *un*-annotated) can be dead on the target if the target reverted
the feature that called it. Concretely: milestone-12/AK-4.3.1 reverted KIP-939 2PC,
removing the production caller of `TransactionManager::is_prepared`; the M7 pick
auto-merged clean but the lib-only `#[deny(warnings)]` build then failed on
`is_prepared` (test-only callers don't count in a non-test build). Fix = add the
targeted allow the revert newly requires, mirroring its already-annotated sibling
(`prepared_transaction_state`), as a labeled follow-up `fixup!` — do NOT fold it into
the pick (keeps each pick a 1:1 replay for review). `cargo build` (lib) catches this;
the background-task "exit 0" can lie (see [[background_task_exit_masking]]).
