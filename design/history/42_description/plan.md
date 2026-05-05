# PR #42 — MINOR: Require --dry-run or --execute for reset-offsets in streams-groups tool

## AK Commit

- **Hash:** `ffab7e6a5d59f83ea247db080f3f66fa04066672`
- **Branch:** `trunk`
- **Author:** Ming-Yen Chung
- **Date:** 2025-11-22

## Commit Summary

This commit enforces that the `reset-offsets` sub-command in the
Kafka command-line tools requires either `--dry-run` or `--execute`
to be explicitly specified.  Previously the streams-groups tool
silently fell back to a dry-run when neither flag was given and
printed only a warning.  The commit makes that a hard error instead.

Affected Java sources (all under `tools/`):

| File | Change |
|------|--------|
| `tools/…/consumer/group/ConsumerGroupCommandOptions.java` | Update warning message wording to mention "version 5.0" deadline |
| `tools/…/consumer/group/ShareGroupCommandOptions.java` | Clarify `RESET_OFFSETS_DOC`: remove "(the default)" and add failure note |
| `tools/…/streams/StreamsGroupCommandOptions.java` | Replace warning + continue with `CommandLineUtils.printUsageAndExit()` when neither flag is present; also updates doc string |
| `tools/…/streams/ResetStreamsGroupOffsetTest.java` | Add `testResetOffsetsWithoutDryRunOrExecuteOption`; fix existing tests to always pass `--dry-run` explicitly |

The behavioral change is in `StreamsGroupCommandOptions`: calling
`reset-offsets` without `--dry-run` or `--execute` now exits with a
non-zero status code and prints a usage error, instead of printing a
warning and proceeding as a dry-run.

## Scope Assessment

All four changed files live inside the `tools/` Maven module of
Apache Kafka.  These are **administrative CLI tools** (consumer-group
tool, share-group tool, streams-group tool), not part of the Kafka
client library.

The Rust project translates **the Kafka client library only** (per
`CLAUDE.md`: "Rust Kafka client implementation translated from the
Java Kafka client (client only)").  The `tools/` package is
explicitly outside the translation scope.

The current Rust codebase has no equivalent of
`StreamsGroupCommandOptions`, `ConsumerGroupCommandOptions`,
`ShareGroupCommandOptions`, or any `reset-offsets` CLI tool.

**Conclusion:** This commit is **out of scope**.  No Rust translation
work is required.

## Decision

No implementation phases are needed for this PR.

The branch exists only to record this design document so the
translation-agent pipeline can mark the PR as processed and advance
the `branch_commit` cursor past this commit.

## Definition of Done

- [x] Design document written and committed — no further action needed.
