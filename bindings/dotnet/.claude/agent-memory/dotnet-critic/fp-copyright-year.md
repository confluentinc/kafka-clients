---
name: fp-copyright-year
description: FALSE-POSITIVE trap — repo copyright headers are fixed "Copyright 2025 Confluent Inc."; do NOT flag year 2025 as stale/wrong even in a later calendar year
metadata:
  type: feedback
---

Do **not** report a `.cs`/`.rs` Apache header reading `Copyright 2025 Confluent
Inc.` as a stale/wrong year, even when the current date is later (e.g. 2026).

**Why:** `2025` is the repo-wide convention — verified across 240+ `.rs` files and
the new `.cs` files all use exactly `// Copyright 2025 Confluent Inc.`. It is a
fixed project convention, not a per-file creation year. Flagging it would be a
false positive.

**How to apply:** When checking the Apache-2.0 header requirement, verify (a) the
header block is present and (b) the holder is `Confluent Inc.` — do not judge the
year. Only the *presence* of the Apache-2.0 header and `Confluent Inc.` as holder
is required (root CLAUDE.md rule 7). If ever in doubt, `grep -rh "Copyright"`
across the repo to confirm the reigning convention before commenting.

Related: [[review-verify-mechanics]].
