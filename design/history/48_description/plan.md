# PR #48 Translation Design Document

## Source Commit

- **AK Commit**: `bbb9220452f757a7fe1706f8bd2a9e97f9cd836b`
- **AK Branch**: `trunk`
- **Title**: MINOR: Fix em-dash in command option documentation (#20975)

## Summary of Changes

This commit is a purely cosmetic documentation fix. It replaces an em-dash character (`–`, U+2013)
with two ASCII hyphens (`--`) in the `--execute` option description strings of two Java tool
classes:

1. `tools/src/main/java/org/apache/kafka/tools/consumer/group/ShareGroupCommandOptions.java`
2. `tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java`

In both files, the string literal:
```
"Fails if neither '--dry-run' nor '–execute' is specified."
```
was corrected to:
```
"Fails if neither '--dry-run' nor '--execute' is specified."
```

No logic, API, behaviour, or test changes are included.

## Translation Assessment

**No Rust translation is required for this commit.**

Reasons:

1. **Documentation-only change**: The diff is limited to a single character replacement in two
   string constants used for command-line help text. There is no logic, algorithm, protocol, or
   API surface change to translate.

2. **Out-of-scope module**: Both affected classes reside in the `tools` module
   (`org.apache.kafka.tools`), which provides standalone administrative command-line utilities
   (e.g. `kafka-share-groups.sh`, `kafka-streams-application-reset.sh`). The current Rust
   translation project targets the Kafka **client library** (`org.apache.kafka.clients` and
   related packages), not the command-line tools.

3. **No equivalent Rust code exists**: `ShareGroupCommandOptions` and
   `StreamsGroupCommandOptions` have not been translated and are not planned as part of the
   current milestones. There is therefore no Rust file to update.

## Decision

Skip — no action required in the Rust repository.

This PR will be marked as a no-op translation with this design document as the sole artifact.
