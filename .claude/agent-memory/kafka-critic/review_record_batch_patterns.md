---
name: Record batch translation patterns
description: Common issues found in Milestone 2 Phase 1 RecordBatch wire format translation - silent truncation, debug_assert vs runtime check, panic vs Result
type: feedback
---

Record batch wire format (DefaultRecord, DefaultRecordBatch, MemoryRecordsBuilder) has recurring issues with:

1. **Silent integer truncation**: i64-to-i32 casts for offset deltas lack overflow validation that Java provides. The `as i32` cast wraps silently, producing corrupt wire data. Always check before casting when Java has a corresponding bounds check.

2. **debug_assert vs runtime check**: Java's `if (condition) throw new IllegalArgumentException(...)` was translated to `debug_assert!` which disappears in release builds. For precondition checks on public API parameters, use `assert!` or return `Result::Err`.

3. **Silent error swallowing**: Java's `throw IllegalArgumentException` in `recordWritten` was translated to a silent `return`. When Java throws, Rust should return `Err(...)` per CLAUDE.md rule 10.

4. **Panic vs Result in internal functions called from public API**: `compute_attributes` uses `assert_ne!` (panic) but is reachable from public `set_max_timestamp`. All paths from public API to panic should be reviewed.

**Why:** These are the same patterns seen in Layer 3 (panic vs Result in builders). The Actor tends to convert Java runtime exceptions to either panics or silent no-ops instead of Result errors.

**How to apply:** When reviewing any code that translates Java `throw new IllegalArgumentException` or `throw new IllegalStateException`, verify it maps to `Result::Err`, not panic or silent return.
