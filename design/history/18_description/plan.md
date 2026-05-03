# PR #18: KAFKA-20536 Fix empty heading — Upgrade instructions for 4.1

## Apache Kafka Commit

- **Commit:** `be36babf5567d34de5ec8aaf67de16c5fbc8932d`
- **JIRA:** [KAFKA-20536](https://issues.apache.org/jira/browse/KAFKA-20536)
- **Author:** Murali Basani
- **Date:** 2026-04-30
- **AK PR:** apache/kafka#22153

## What the Commit Does

This is a **documentation-only** change. It modifies
`docs/getting-started/upgrade.md` in the Apache Kafka repository to add
missing upgrade guide sections for Kafka 4.1.x patch releases:

1. **Upgrading to 4.1.2** — adds a "Notable changes in 4.1.2" section
   noting the fix for KAFKA-19012 (rare producer bug where a record could
   end up on the incorrect topic).

2. **Upgrading to 4.1.1** — adds a "Notable changes in 4.1.1" section
   noting fixes for:
   - KAFKA-19748: critical Kafka Streams memory leak affecting range scans,
     session/sliding windows, stream-stream joins, and foreign-key joins.
   - KAFKA-19479: critical Kafka Streams potential data loss bug.

3. **Upgrading to 4.1.0** — adds a note about the rolling upgrade procedure,
   pointing readers to the 4.0 upgrade section for step-by-step instructions.

No Java source code is changed. The diff is entirely within one Markdown
file (`docs/getting-started/upgrade.md`, +19 lines).

## Translation Analysis

### Code Changes Required: None

This commit contains no changes to Java source files. There is no class,
interface, method, or constant to translate into Rust. The Rust client
codebase does not include or mirror the upstream Kafka upgrade guide.

### Documentation Impact: None

The Rust client project maintains its own `README.md`, `CLAUDE.md`, and
`design/` documentation that reflects the Rust implementation. The upstream
upgrade guide for Kafka server and Java client is not replicated here.

The producer bug mentioned (KAFKA-19012 — record routed to wrong topic) is
a `KafkaProducer` bug in the Java client's full producer implementation.
The Rust project's current milestone scope does not include a full
`KafkaProducer` (only `MockProducer`), so this bug and its fix are not
relevant to any existing Rust code.

## Implementation Plan

**No implementation is required.**

This PR is a no-op for the Rust translation. The commit is tracked in the
`pr_commit` table to advance the AK cursor, but no Actor or Critic cycle
needs to be run.

### Checklist

- [x] AK commit analysed
- [x] Confirmed documentation-only (no Java source changes)
- [x] Confirmed no Rust code changes needed
- [x] No new modules, structs, traits, or tests required
- [x] No dependency on other open PRs

## Definition of Done

The PR is complete once this design document is committed and pushed.
No build, test, format, or lint steps are required because no source code
is modified.
