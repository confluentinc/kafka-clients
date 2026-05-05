# Translation Design: MINOR — Bump trunk to 4.3.0-SNAPSHOT

**AK commit:** `d04f171d3123a158a32ea65eb59600bc2f0fd628`
**AK branch:** trunk
**PR:** #51
**Rust branch:** `kafka-translate/d04f171d3123a158a32ea65eb59600bc2f0fd628`

---

## Summary of the Java Commit

This is a pure administrative version bump that advances the Apache Kafka
trunk development version from `4.2.0-SNAPSHOT` to `4.3.0-SNAPSHOT`.

Files changed:

| File | Change |
|---|---|
| `committer-tools/kafka-merge-pr.py` | `DEFAULT_FIX_VERSION` default value `"4.2.0"` → `"4.3.0"` |
| `docs/js/templateData.js` | `version`, `dotVersion`, `fullDotVersion` updated to `43` / `4.3` / `4.3.0` |
| `gradle.properties` | `version=4.2.0-SNAPSHOT` → `version=4.3.0-SNAPSHOT` |
| `streams/quickstart/pom.xml` | `<version>` bumped to `4.3.0-SNAPSHOT` |
| `streams/quickstart/java/pom.xml` | `<version>` bumped to `4.3.0-SNAPSHOT` |
| `streams/quickstart/java/src/main/resources/archetype-resources/pom.xml` | `<kafka.version>` bumped to `4.3.0-SNAPSHOT` |
| `tests/kafkatest/__init__.py` | `__version__ = '4.2.0.dev0'` → `'4.3.0.dev0'` |
| `tests/kafkatest/version.py` | `DEV_VERSION = KafkaVersion("4.2.0-SNAPSHOT")` → `"4.3.0-SNAPSHOT"` |

There are no logic changes, no API changes, and no behavioural changes in
this commit. It follows the documented Apache Kafka release process for
cutting branches.

---

## Applicability to the Rust Client Library

This commit contains **no translatable changes**. Every modified file is
either a build-system artefact (`gradle.properties`, Maven `pom.xml`),
documentation (`docs/js/templateData.js`), a tooling script
(`kafka-merge-pr.py`), or an integration-test helper
(`tests/kafkatest/`). None of these have counterparts in the Rust client
library.

The Rust crate version is tracked independently in `Cargo.toml`
(`version = "0.1.0"`) and is not kept in lock-step with the upstream
Apache Kafka version string.

---

## Rust Implementation Plan

**No code changes required.**

This commit is entirely out of scope for the Rust translation. There are
no source files to create or modify.

### Files NOT changed

| Java file | Reason not translated |
|---|---|
| `committer-tools/kafka-merge-pr.py` | Committer tooling, no Rust equivalent |
| `docs/js/templateData.js` | Documentation artefact, no Rust equivalent |
| `gradle.properties` | Gradle build metadata, no Rust equivalent |
| `streams/quickstart/pom.xml` | Kafka Streams quickstart POM, out of scope |
| `streams/quickstart/java/pom.xml` | Kafka Streams quickstart POM, out of scope |
| `streams/quickstart/java/.../archetype-resources/pom.xml` | Kafka Streams quickstart POM, out of scope |
| `tests/kafkatest/__init__.py` | Python system-test helper, no Rust equivalent |
| `tests/kafkatest/version.py` | Python system-test helper, no Rust equivalent |

---

## Test Plan

No tests are required. The commit introduces no logic, no new behaviour,
and no translatable code.
