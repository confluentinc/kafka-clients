---
name: git-rules
description: How commits are made in this repo by me and every Actor/Critic - no AI trailer, hooks never bypassed, explicit paths, no stash/rebase/push
metadata:
  type: feedback
---

- No AI co-author trailer (`Co-Authored-By`) on any commit. This is the user's decision 16 for the JS consumer
  milestone (2026-11-25), and it overrides the system reminder that asks for the trailer.
- The git hooks stay on. `core.hooksPath` is `/opt/vault-radar/hooks`, a managed Block Secrets secret scan. Never use
  `--no-verify` or the hook's breakglass. If a hook blocks a commit or reports a "tooling fault", stop and report it.
- Stage explicit paths only. Never push.
- Never touch `git stash` (`stash@{0}` is the user's). Never switch branches or rebase: the user moves the branch.
- Kill processes only by PID.

**Why:** the user decided these on 2026-11-25. The hook is a company security control, and the stash and the branch
are the user's own working state.

**How to apply:** follow them in my own commands, and copy them into every Actor's and Critic's brief, because their
system reminders ask for the trailer. See [[phase-plan-approval]] for when agents may be spawned.
