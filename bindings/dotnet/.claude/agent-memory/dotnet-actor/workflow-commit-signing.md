---
name: workflow-commit-signing
description: Commits in this repo fail in non-interactive sessions because SSH commit-signing needs a passphrase; use `git -c commit.gpgsign=false commit`
metadata:
  type: feedback
---

Git in this repo has `commit.gpgsign=true` + `gpg.format=ssh` with a
passphrase-protected key (`~/.ssh/id_ed25519`). In a non-interactive agent
session there is no way to supply the passphrase, so a plain `git commit` fails
with `failed to write commit object` (SSH key passphrase prompt).

**Why:** the user signs commits interactively; the agent session can't.

**How to apply:** commit with signing disabled per-invocation —
`git -c commit.gpgsign=false commit ...` — which does NOT change the user's global
config. Do this for every commit made in an agent session here. The user can
re-sign/amend later if a signed commit is required. Do not ask for the passphrase.
