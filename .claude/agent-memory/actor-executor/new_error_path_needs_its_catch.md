---
name: new-error-path-needs-its-catch
description: Making a silent path loud is half a translation — find who catches the new error before shipping the throw
metadata:
  type: feedback
---

When a change makes a previously-silent code path start returning errors, the change is
not done until you have found **who consumes that error** and confirmed it is a handler,
not an `.expect`/`unwrap`. Trace it in the Java source first: if Java throws there, some
Java caller catches it, and that catch is part of the same contract.

**Why:** PLAN §9.1 added the generator's non-default-at-unsupported-version guard, which
made `write()` fail for 100 fields. `network_client.rs` read
`request.to_send(&header).expect("Failed to serialize request")`, so every one of them
became a **panic on the I/O task**. Java wraps both `builder.build(version)` *and*
`request.toSend(header)` in one `try` (`NetworkClient.java:582-583` + `:608`); the port
had translated only the build half of that catch, which was harmless while the serialize
half could not fail. The guard turned a silent-wrong-value into a crash — strictly worse
than the bug it fixed — and the full unit suite stayed green throughout, because no test
drove a guarded field at a sub-gate version.

"No test failed" and "no caller regressed" are different questions. The whole point of
such a change is to make a path loud, so the first thing to check is what the newly-loud
path does when it fires.

**How to apply:**

  - Grep the new error's propagation path for `.expect(`, `.unwrap(`, and `panic!`
    *before* claiming a blast radius. A `Result` that no one is ready for is a defect.
  - Find the Java `catch` and check its **extent**, not just its existence — here the
    `try` spanned two calls in two different methods, and the second was easy to miss by
    reading only the method that throws.
  - Teeth-check the fix by restoring the old line and confirming the new test fails *on
    its assertion*. Watch for the narrow-check trap: deleting a parameter's only use
    makes the run fail on `unused_variable` under `#![deny(warnings)]`, which looks like
    a passing teeth-check but proves nothing. Keep the parameter consumed
    (`let _ = x;`) so the failure is the assertion.

See also [[generator_version_gate_guard]] and [[skips_recorded_in_prose]].
