# Translation Design: MINOR — Fix em-dash in command option documentation

**AK commit:** `bbb9220452f757a7fe1706f8bd2a9e97f9cd836b`
**AK branch:** trunk
**PR:** #82
**Rust branch:** `kafka-translate/bbb9220452f757a7fe1706f8bd2a9e97f9cd836b`

---

## Summary of the Java Commit

This is a pure documentation fix. In two CLI tool option classes, a
Unicode em-dash character (–, U+2013) was mistakenly used instead of a
double hyphen (--) inside a help-text string literal. The fix replaces
the em-dash with `--` so that the `--execute` flag is displayed
correctly in the command's help output.

### Files changed

| File | Change |
|---|---|
| `tools/src/main/java/org/apache/kafka/tools/consumer/group/ShareGroupCommandOptions.java` | Replace `'–execute'` → `'--execute'` in `RESET_OFFSETS_DOC` |
| `tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java` | Replace `'–execute'` → `'--execute'` in `RESET_OFFSETS_DOC` |

The change is a one-character cosmetic fix per file (em-dash → two
ASCII hyphens) with no behavioral, logic, or API impact.

---

## Applicability to the Rust Client Library

### Out-of-scope Java changes

Both changed files are part of the **Kafka CLI tooling** layer
(`kafka-share-groups.sh` / `kafka-streams-groups.sh`). They contain
only user-facing help text string constants embedded in
`CommandDefaultOptions` subclasses.

The Rust project is a **client library** (not a CLI tool suite). It
contains no equivalents of:

- `ShareGroupCommandOptions` — share-group management CLI
- `StreamsGroupCommandOptions` — streams-group management CLI

Neither class has a Rust counterpart, and no Rust file contains
analogous help-text string constants.

### Conclusion: no translation required

The commit makes no changes to:
- Any protocol logic
- Any client-facing API
- Any configuration handling
- Any data structures

It is entirely confined to help-text strings in CLI tools that have no
presence in the Rust library. There is nothing to translate.

---

## Rust Implementation Plan

**No code changes are required.**

This commit is a no-op for the Rust translation. The design document is
recorded for completeness and audit trail purposes only.

### Files NOT changed

| Java file | Reason not translated |
|---|---|
| `ShareGroupCommandOptions.java` | CLI tool, no Rust equivalent |
| `StreamsGroupCommandOptions.java` | CLI tool, no Rust equivalent |

---

## Test Plan

No tests are required. No production code changes are made.
