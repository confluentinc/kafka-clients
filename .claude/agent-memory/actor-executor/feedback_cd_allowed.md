---
name: Always allow cd
description: User permits unrestricted cd; do not phrase commands defensively or break them into avoidance dances.
type: feedback
---

`cd` to any path is pre-approved.

**Why:** User stated "always allow cd *" during Phase 4a follow-up work. Project guidance discourages prepending `cd <wd>` to git commands but does not forbid `cd` outright; user wants the freedom to use it without prompts.

**How to apply:** Use `cd` freely when it makes a multi-step command clearer or shorter. Still prefer absolute paths for one-shot file operations (Read/Edit/Write) and avoid `cd <current-wd> && git ...` (per Bash tool guidance — that pattern triggers permission prompts and is redundant). The point is: don't go out of your way to *avoid* `cd` for this user.
