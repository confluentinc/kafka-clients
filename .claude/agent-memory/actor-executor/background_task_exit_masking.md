---
name: background-task-exit-masking
description: A trailing echo after a backgrounded `cmd > log` makes the task-completion notification report the echo's exit (0), not cmd's — masking real cargo failures
metadata:
  type: feedback
---

Running cargo in the background is the standard workflow here (watchdog kills
foreground cargo > 600s). But the naive one-liner lies about success.

**The trap:** `cargo build ... > log 2>&1` followed by `echo "EXIT=$?"` — when
launched as a background Bash task — makes the harness's completion notification
report **the shell's final exit code**, which is the `echo`'s (always 0). So a
`cargo build` that FAILED (e.g. `#[deny(warnings)]` dead_code) reports
`<status>completed</status> exit code 0`. In loop 64 this nearly hid a real
lib-build failure; the log tail (not the notification) showed the error.

**Why:** the background wrapper reports the exit of the whole command line, and
`;`-chaining a trailing command overwrites `$?`.

**How to apply:**
- Capture cargo's OWN exit INTO the log and read it there, never trust the
  notification's "exit code":
  `cargo build ... > "$LOG" 2>&1; echo "REAL_EXIT=$?" >> "$LOG"`
  then `grep REAL_EXIT "$LOG"`.
- Always `grep -nE 'error|error\[|test result:|REAL_EXIT'` the log after a
  background cargo run; do not conclude "green" from the completion summary alone.
- Same for `${PIPESTATUS[0]}` in a piped foreground run — under zsh it can print
  empty; prefer the log-captured exit. (User memory `master-ffi-*` notes a related
  "pipe masks clippy exit" hazard.)
