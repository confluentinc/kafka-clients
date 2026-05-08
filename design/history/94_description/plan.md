# Translation Plan: MINOR — Fix em-dash in command option documentation

**AK commit:** `bbb9220452f757a7fe1706f8bd2a9e97f9cd836b`
**AK branch:** trunk
**PR:** #94
**Rust branch:** `kafka-translate/bbb9220452f757a7fe1706f8bd2a9e97f9cd836b`

---

## Summary of the Apache Kafka Commit

This is a **documentation-only fix** in Java CLI tool classes. No functional
code was changed.

The commit replaces an em-dash character (`–`, U+2013) with a proper double
hyphen (`--`) in the `--execute` option description string for two command
option classes:

- `ShareGroupCommandOptions.java`
- `StreamsGroupCommandOptions.java`

The affected string is:
```
"Fails if neither '--dry-run' nor '–execute' is specified."
```
Changed to:
```
"Fails if neither '--dry-run' nor '--execute' is specified."
```

This ensures the `--execute` flag renders correctly in help text output.

**Changed files:**
```
tools/src/main/java/org/apache/kafka/tools/consumer/group/ShareGroupCommandOptions.java
tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommandOptions.java
```

---

## Rust Translation Analysis

### Is there equivalent code in Rust?

No. The Rust codebase is a client library implementation. It does not include
CLI tools such as `kafka-share-groups.sh` or `kafka-streams.sh`. The
`ShareGroupCommandOptions` and `StreamsGroupCommandOptions` classes are part of
the Kafka server-side tooling (`tools/` module), which is entirely out of scope
for this Rust client library.

### Is there production code to translate?

No.

### Is there test code to translate?

No.

---

## Implementation Plan

**No action required.**

This commit is a trivial typo fix in server-side CLI tool documentation strings
that have no equivalent in the Rust client library. There are no files to
create, modify, or translate.

---

## Files to Create / Modify

None.

---

## Out of Scope

- Server-side CLI tools (`kafka-share-groups`, `kafka-streams`) are not part of
  the Rust client translation effort.

---

## Definition of Done

- [x] Confirmed commit is not applicable to the Rust codebase.
- [x] No translation work needed.
