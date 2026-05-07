# Translation Design: MINOR — Add 4.0.1 to system tests

**AK commit:** `ea8babe6990924465b7957ed0746cb9253eea55a`
**AK branch:** trunk
**PR:** #83
**Rust branch:** `kafka-translate/ea8babe6990924465b7957ed0746cb9253eea55a`

---

## Summary of the Java Commit

This commit updates the Apache Kafka system test infrastructure to reference
the newly released Kafka 4.0.1 patch release in place of 4.0.0.

Four files are changed:

| File | Change |
|---|---|
| `gradle/dependencies.gradle` | Bump `kafka_40` version constant from `"4.0.0"` to `"4.0.1"` |
| `tests/docker/Dockerfile` | Download and install `/opt/kafka-4.0.1` instead of `/opt/kafka-4.0.0`; update the kafka-streams test jar reference accordingly |
| `tests/kafkatest/version.py` | Add `V_4_0_1 = KafkaVersion("4.0.1")`; update `LATEST_4_0` pointer from `V_4_0_0` to `V_4_0_1` |
| `vagrant/base.sh` | Update `get_kafka` call and `chmod` target from `4.0.0` to `4.0.1` |

No production source code is modified; this is a pure CI / system-test
infrastructure bump to pick up the 4.0.1 patch release.

---

## Applicability to the Rust Client Library

### Entirely out of scope

All four changed files belong to the Apache Kafka system test and build
infrastructure:

- **`gradle/dependencies.gradle`** — Gradle build dependency version
  table. The Rust project uses Cargo, not Gradle. No equivalent file exists.
- **`tests/docker/Dockerfile`** — Docker image used by the Kafka system
  test harness (ducktape). The Rust project has no equivalent Docker-based
  integration test infrastructure tied to specific Kafka release tarballs.
- **`tests/kafkatest/version.py`** — Python ducktape version registry. The
  Rust project has no Python test harness.
- **`vagrant/base.sh`** — Vagrant VM provisioning script used by the Java
  system tests. The Rust project has no Vagrant setup.

None of these files have a Rust counterpart, and none affect client-facing
behaviour, APIs, or internal logic in any way.

---

## Rust Implementation Plan

**No changes required.**

This commit contains no logic changes, API changes, configuration
changes, or client-behaviour changes. It is solely a system test /
build-tooling version bump for the Apache Kafka project's own CI
infrastructure, which has no equivalent in the Rust client library.

### Files to change

_None._

### Files NOT changed

| Java file | Reason not translated |
|---|---|
| `gradle/dependencies.gradle` | Gradle build file; Rust project uses Cargo |
| `tests/docker/Dockerfile` | Java system-test Docker image; no Rust equivalent |
| `tests/kafkatest/version.py` | Python ducktape harness; no Rust equivalent |
| `vagrant/base.sh` | Vagrant provisioning for Java tests; no Rust equivalent |

---

## Test Plan

No tests to add or update. The commit introduces no testable client
behaviour.
