---
name: workflow-autosquash-append-only-logs
description: A fixup that appends to a plan's append-only log (status/notes list) extended by later commits conflicts under autosquash; how to place notes and verify with a scratch-clone rebase
metadata:
  type: feedback
---

A `fixup!` of an early commit must only edit text that exists at its target
and is not adjacent to where later commits append. A plan file whose §5
(status) and §6 (notes) lists grow one entry per commit is the trap: loop 78's
round-2 fixups appended to those tails, and `git rebase --autosquash` conflicted
because the fixup moved before the commits that created the tail.

**Why:** git's 3-way merge treats an insertion at the same place (or next to a
modified line) as a conflict. The code files auto-merged; only the markdown log
conflicted.

**How to apply:**
- Put a fixup's note into an existing item on the same topic (mid-list, away
  from the tail); put new status lines and new items in the tip (non-fixup)
  commit; say so in the messages.
- Verify before handing back: `git clone -q --no-checkout <repo> <scratch>`,
  `git checkout -B verify <tip>`, then
  `GIT_SEQUENCE_EDITOR=: git -c core.hooksPath=/dev/null rebase -q -i --autosquash <base>`
  and `git diff --stat <tip> HEAD` (must be empty).
- The fixup subject must be `fixup! <exact target subject>` (or a bare hash with
  no spaces). `fixup! <hash> <subject>` matches nothing.
- If the commits are already made, rebuild them: `git reset --mixed <parent>`
  keeps the tree; restage per commit by path, and confirm each commit's code
  equals the gated state with `git diff --stat <old> <new> -- src`.
- A lock like `COMMENTS.<N>.md.lock` is a directory: `ls` of it prints nothing
  when empty — test with `test -e`, not `ls`. Related: [[workflow-teeth-check-mtime]].
