---
name: workflow-teeth-check-mtime
description: After restoring a corrupted-for-teeth-check source via `mv` of an older backup, `touch` it or cargo uses the stale corrupted binary
metadata:
  type: feedback
---

When verifying a test has "teeth" by temporarily corrupting a source arm
(`cp backup && perl -i corrupt && cargo test && mv backup back`), the restored
file's mtime is OLDER than the artifact cargo just compiled from the corrupted
version. Cargo's fingerprint sees no change and re-runs the STALE corrupted
binary, so the next `cargo test` shows a spurious failure on correct source.

**Why:** cargo change-detection keys on mtime; `mv` of a `cp`-made backup gives
the restored file an mtime earlier than the last compile.

**How to apply:** after restoring, run `touch <file>` (or edit it via the Edit
tool, which bumps mtime) before re-running `cargo test`. Better: do the
teeth-check with the Edit tool (corrupt → test → Edit-revert) instead of shell
`cp`/`mv`, so mtime is always fresh. Confirmed while resolving the M11 Tier 3
Phase 1 ACL exhaustive-test fix — one wasted cycle chasing a stale-binary
"failure" that the source had already fixed.
