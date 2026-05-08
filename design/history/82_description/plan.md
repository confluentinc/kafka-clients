# Translation Design: MINOR — Fix em-dash in command option documentation

**AK commit:** `bbb9220452f757a7fe1706f8bd2a9e97f9cd836b`
**AK branch:** trunk
**PR:** #82
**Rust branch:** `kafka-translate/bbb9220452f757a7fe1706f8bd2a9e97f9cd836b`

---

## Summary of the Java Commit

This is a pure documentation fix. Two Java CLI tool classes had a Unicode
em-dash character (`–`, U+2013) in a help-text string constant where a
double hyphen (`--`) was intended. The affected files and lines:

| File | Change |
|------|--------|
| `tools/src/main/java/org/apache/kafka/tools/consumer/group/ShareGroupCommandOptions.java` | `'–execute'` → `'--execute'` in `RESET_OFFSETS_DOC` |
| `tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java` | `'–execute'` → `'--execute'` in `RESET_OFFSETS_DOC` |

The change ensures the `--execute` option name is displayed consistently
and correctly in CLI help output. No logic, behaviour, or API surface is
modified; only two string literals are corrected.

---

## Applicability to the Rust Client Library

### No CLI tool layer in Rust

The Rust library contains no equivalent of `ShareGroupCommandOptions` or
`StreamsGroupCommandOptions`. Looking at the project structure:

- `src/` contains only wire-protocol types and a schema code-generator.
- `tests/` contains unit and integration tests against a live broker.
- There is no CLI tooling, no help-text, and no option-parsing layer.

The concepts of share-group CLI and streams-group CLI do not exist in
this codebase. There is therefore no string to fix and no analogous
change to make.

### No other indirect effects

The commit touches no public API, no configuration mechanism, no
protocol message, and no test helper that could have a Rust counterpart.

---

## Rust Implementation Plan

**No changes required.**

This commit is entirely out of scope for the Rust client library. The
affected Java classes are CLI administration tools; the Rust library is
a client-side protocol library with no CLI layer.

### Files NOT changed

| Java file | Reason not translated |
|-----------|----------------------|
| `ShareGroupCommandOptions.java` | CLI tool, no Rust equivalent |
| `StreamsGroupCommandOptions.java` | CLI tool, no Rust equivalent |

---

## Test Plan

No tests are required because no production code is changed. The
existing test suite should continue to pass without modification.
