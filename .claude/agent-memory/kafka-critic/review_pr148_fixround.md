---
name: review-pr148-fixround
description: PR #148 human-reviewer fix round — Py_BuildValue 'N'-steal-on-OOM leak pattern, off-by-one cleanup-bound fix, i32-vs-usize negative-partition guard class, and a doc-comment-sweep gap left by an otherwise-thorough fix
metadata:
  type: project
---

Round reviewing 6 commits (`bc7996fa..39eb23ca`) that fixed real human-reviewer
feedback from Ankith-Confluent on PR #148, plus one genuine CI failure. All 5
substantive fixes were correct; found exactly one issue, a doc-comment leftover.
Verdict: safe to push. Full findings in `COMMENTS.61.md` (repo root, gitignored).

## Patterns worth carrying forward

- **`Py_BuildValue`'s `'N'` format only leaks on OOM, not on any later-item
  failure.** `do_mktuple` allocates the tuple first (`PyTuple_New`) and only
  *then* walks format items consuming each `'N'` argument's reference — so a
  later plain-scalar item failing (e.g. bad UTF-8 in an `"s"` slot) does NOT
  leak an earlier `'N'`'s reference; only failure of `PyTuple_New` itself
  (OOM, before any item is processed) leaks all `'N'` args. This was verified
  empirically in the actor's session with a refcount-tracking dealloc test —
  worth trusting without re-deriving, since it changes what severity this
  bug class deserves (OOM-only, no unit test expected) and how to fix it
  (`'O'` + unconditional `Py_DECREF` after the call, regardless of success,
  is the correct universal fix — see `node_to_py` in
  `bindings/python/_confluentkafka.c` for this file's own established
  precedent).
- **Ternary-branch `Py_BuildValue` fixes: check BOTH arms got the decref, not
  just one.** `py_DescribeFeaturesResult_drain`'s `has_epoch` ternary is the
  precedent case — the fix correctly hoisted the decref to run once, after
  the ternary, covering both `(OLO)` and `(OOO)` arms uniformly. A common way
  to get this wrong would be decrefing inside only one arm.
- **`i32`-vs-`usize` negative-index guard bug class**: `(len as i32) >
  some_i32` (or `<=`) is asymmetric around negative values — any
  non-negative `len` satisfies `>` a negative RHS, so the guard passes and a
  subsequent `as usize` cast on the negative value wraps to `usize::MAX`-ish,
  panicking on the index. The correct fix is `usize::try_from(x).ok()` (or
  `.filter`/`.and_then` chains on it), which rejects negative inputs
  outright rather than comparing them as signed integers against an
  always-non-negative length. Grep for `as i32) [<>]=?` next to a later
  `as usize` cast as a heuristic for finding more instances of this class
  elsewhere in the admin mock/client code — this PR fixed two sibling
  instances (`alter_replica_log_dirs`, `describe_replica_log_dirs`) that
  had the *identical* shape, suggesting the pattern may recur wherever a
  `TopicPartition`/`TopicPartitionReplica`-style unvalidated `i32` partition
  number is used as a `Vec` index.
- **Off-by-one cleanup-loop bound fix: verify by asking "does the failure
  branch's `break` skip the counter increment the success path gets for
  free?"** `py_Admin_alter_client_quotas_async`'s `for (; built < n;
  built++)` gives every *successful* iteration a free increment via the
  for-loop's own increment clause, but a `break` on failure skips that
  clause — so the fix must manually increment before breaking. Confirmed
  safe here because every row's five buffer-slots are NULL-initialized at
  loop-top before any fallible work, making `PyMem_Free(NULL)` (a no-op) the
  fallback for a row that failed before allocating.
- **A "fix every doc comment stating claim X" commit can still miss an
  instance of X.** `36b85ad3` explicitly set out to correct two named doc
  comments carrying the false "4.2 broker rejects earliestPendingUpload"
  claim, and did fix both — but missed a third, near-identical claim on the
  `offset_of` helper function's doc comment (`admin_elections_reassignments_offsets_test.rs:105-106`),
  9 lines above the function that's actually called. When a fix commit's
  own message enumerates "both X and Y", grep the whole file for the
  disproven claim's key phrase rather than trusting the commit's own count —
  the same technique COMMENTS.60.md itself used to find Ankith's uncounted
  7th test-assertion site (`describe_user_scram_credentials`) applies
  symmetrically to doc-comment sweeps, not just code-assertion sweeps.
- **Verifying exact Python error-message assertions is cheap and worth doing
  for real, not just via grep.** `venv/bin/pytest` (this repo keeps a venv at
  repo root with pytest installed, separate from the system/mise Python)
  actually runs the built `.so` — `bindings/python/build/.../` and the
  platform `.so` files already existed in this environment, so
  `cd bindings/python && venv/bin/python -m pytest test/unit/test_admin.py -k
  "..."` is a fast (~1s), high-confidence check beyond grepping the Rust
  error-message source strings. Use the full path to the repo-root `venv`,
  not a bare `pytest`/`python3 -m pytest` (system Python has neither pytest
  nor the built extension on `sys.path`).
