---
name: workflow-comments-gitignore-and-staging
description: COMMENTS.<N>.md is gitignored; only COMMENTS.DONE.<N>.md is tracked. Shared workdir means other agents leave tracked files dirty — stage explicitly.
metadata:
  type: feedback
---

When resolving a Critic finding, `COMMENTS.<N>.md` is **gitignored**
(`.gitignore` pattern `COMMENTS\.[0-9]*\.md`); `COMMENTS.DONE.<N>.md` **is
tracked**. So "moving F1 from COMMENTS.1 to COMMENTS.DONE.1" produces a commit
that touches only the DONE file — the truncation of `COMMENTS.<N>.md` is
on-disk-only and never appears in `git status`/the commit. Do not be confused
by its absence, and do not try to `git add` it.

**Why:** active review comments are ephemeral scratch; the adjudication record
(DONE) is the durable, versioned artifact. Discovered on the M10 Phase-3 F1
doc/naming fixup.

**How to apply:**
- Perform the on-disk move (append to DONE, remove from COMMENTS.<N>.md) so the
  live files are correct, but expect only DONE in the commit.
- This is a **shared workdir**: other agents (e.g. `kafka-critic`) leave their
  *tracked* memory files (`.claude/agent-memory/kafka-critic/*`) modified/new in
  the working tree. Never `git add -A` / `git commit -a` — stage your intended
  paths explicitly (`git add <file1> <file2>`) so you don't sweep another
  agent's work into your commit. Verify with `git diff --cached --name-only`
  before committing. See also [[shared-workdir-agents-switch-branch]].
