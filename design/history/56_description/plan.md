# PR #56: Fix typo in rustdoc in AbstractHeartbeatRequestManager

## AK Commit

- **Commit**: `02fd9b1ad97cb04dddafd8f2102d142b402cd616`
- **Branch**: `trunk`
- **Title**: `MINOR: Fix typo in JavaDoc in AbstractHeartbeatRequestManager (#20956)`
- **Author**: Anton Vasanth

## Summary

This AK commit fixes a single-word typo in the class-level JavaDoc of
`AbstractHeartbeatRequestManager`:

```
- * <p>If the coordinator not is not found, we will skip sending the heartbeat …
+ * <p>If the coordinator is not found, we will skip sending the heartbeat …
```

No functional changes. No API surface changes. No logic changes.

## Java Source Reference

- `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractHeartbeatRequestManager.java`
  (line 56, class-level JavaDoc paragraph)

## Rust Target File

```
src/consumer/internals/abstract_heartbeat_request_manager.rs
```

Per project naming conventions (`org.apache.kafka.clients.consumer.internals` →
`consumer::internals`, `clients` omitted from module path).

## Translation Plan

### Scope

This is a documentation-only change. No new types, traits, methods, or modules are
introduced by this AK commit.

**In scope:**
- Correct the corresponding rustdoc comment in the Rust translation of
  `AbstractHeartbeatRequestManager` to match the fixed JavaDoc.

**Out of scope:**
- Translating the rest of `AbstractHeartbeatRequestManager.java` (524 lines) — that
  is a separate, larger translation effort not triggered by this commit.
- Any functional behaviour changes (there are none in this AK commit).

### Dependency Assessment

- **Plan dependency**: none — this change is self-contained.
- **Implementation dependency**: the Rust file
  `src/consumer/internals/abstract_heartbeat_request_manager.rs` must already exist
  for the rustdoc to be updated. If it does not yet exist, the fix is a no-op until
  that file is translated; the implementation step records this state explicitly.

### Implementation Steps

| Step | Action | File |
|------|--------|------|
| 1 | Check whether `abstract_heartbeat_request_manager.rs` exists in `src/consumer/internals/` | — |
| 2a | **If the file exists**: locate the rustdoc paragraph that corresponds to the fixed line and correct the wording from "coordinator not is not found" to "coordinator is not found" | `src/consumer/internals/abstract_heartbeat_request_manager.rs` |
| 2b | **If the file does not exist yet**: add a comment in the plan noting that the fix will be applied when the file is first translated; no code change needed now | `design/history/56_description/plan.md` (update) |
| 3 | Run `cargo xtask format-check` and `cargo xtask lint` to confirm no regressions | — |
| 4 | Commit with message referencing this PR | — |

### Definition of Done

- The rustdoc for `AbstractHeartbeatRequestManager` (when it exists) reads
  "coordinator is not found" (not "coordinator not is not found").
- `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint` all pass.
