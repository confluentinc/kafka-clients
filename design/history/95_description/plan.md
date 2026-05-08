# Translation Plan: MINOR — Add 4.0.1 to system tests (#20970)

**AK commit:** `ea8babe6990924465b7957ed0746cb9253eea55a`
**AK branch:** trunk
**PR:** #95
**Rust branch:** `kafka-translate/ea8babe6990924465b7957ed0746cb9253eea55a`

---

## Summary of the Apache Kafka Commit

This is a **test infrastructure / CI-only change**. No production code was modified.

The commit bumps the Kafka 4.0 version reference from `4.0.0` to `4.0.1` across
system-test infrastructure files, reflecting the new 4.0.1 release:

**Changed files:**
```
gradle/dependencies.gradle      — version constant kafka_40: "4.0.0" → "4.0.1"
tests/docker/Dockerfile         — Docker image downloads 4.0.1 instead of 4.0.0
tests/kafkatest/version.py      — adds V_4_0_1, updates LATEST_4_0
vagrant/base.sh                 — Vagrant provisioning fetches 4.0.1
```

All changes are limited to:
- Version strings in build/test configuration
- Docker/Vagrant image provisioning scripts
- Python system-test version constants

---

## Rust Translation Analysis

### Does this affect any Rust code?

**No.** The commit modifies only Java/Scala build infrastructure
(`gradle/dependencies.gradle`), Docker/Vagrant provisioning scripts, and Python
system-test utilities (`tests/kafkatest/version.py`). None of these have Rust
equivalents.

### Is there production code to translate?

No. Zero production (library) source files were changed.

### Is there test code to translate?

No. The only "test code" change is a version constant addition in
`tests/kafkatest/version.py`, which is a Python-based system-test framework with
no Rust counterpart.

### What needs to be done?

**Nothing.** This commit is entirely infrastructure/CI-focused and has no
bearing on the Rust translation. The Rust project:

- Does not use Gradle for builds (uses Cargo).
- Does not have Docker-based system tests that pull specific Kafka release
  tarballs.
- Does not have a Vagrant-based development environment.
- Does not maintain a Python `version.py` for cross-version testing.

---

## Implementation Plan

### No action required

This commit is a **no-op** for the Rust translation. There are no files to
create or modify.

---

## Files to Create / Modify

| File | Action | Reason |
|------|--------|--------|
| (none) | — | No Rust-relevant changes in this commit |

---

## Out of Scope

- Cross-version compatibility testing infrastructure for the Rust project (if
  needed in the future, it would be designed independently from the
  Java/Python-based system tests).

---

## Definition of Done

- [x] Design document written and committed.
- [ ] PR closed as no-op, or merged with only this design document.
