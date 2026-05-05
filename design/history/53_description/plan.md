# Translation Design: KAFKA-19876 — Use eclipse-temurin as the base image to align with 3.9

**AK commit:** `99fafc44bae130f59abe896413641ce6538e9eab`
**AK branch:** trunk
**PR:** #53
**Rust branch:** `kafka-translate/99fafc44bae130f59abe896413641ce6538e9eab`

---

## Summary of the Java Commit

KAFKA-19876 updates the Docker-based system-test infrastructure
(`tests/docker/`) to switch the default JDK base image from
`sapmachine:17-jdk-ubuntu-jammy` to `eclipse-temurin:17-jdk-jammy`.

The motivation is two-fold:

1. **Alignment with AK 3.9**: `sapmachine` does not support JDK 8, so
   switching to `eclipse-temurin` allows the same image provider to be
   used across all supported Kafka versions.

2. **Java PATH fix for `eclipse-temurin` on Ubuntu**: After switching to
   `eclipse-temurin`, a "java not found" error was encountered via SSH
   because PAM (`/etc/pam.d/sshd`) re-reads `/etc/environment` _after_
   `~/.ssh/environment`, overwriting the `PATH` that had been set there.
   The chosen solution is to symlink all JDK binaries into `/usr/bin/` so
   that `java` is reachable regardless of `PATH` manipulation:

   ```dockerfile
   RUN cp -sn $JAVA_HOME/bin/* /usr/bin/
   ```

   The previous approach of appending to `~/.ssh/environment` and
   `~/.profile` is removed in favour of this distribution-agnostic symlink
   solution.

### Files changed

| File | Change |
|---|---|
| `tests/docker/ducker-ak` | `default_jdk` changed from `sapmachine:17-jdk-ubuntu-jammy` to `eclipse-temurin:17-jdk-jammy` |
| `tests/docker/Dockerfile` | Remove PATH env writes; add `RUN cp -sn $JAVA_HOME/bin/* /usr/bin/` |

---

## Applicability to the Rust Client Library

### Entirely out of scope

This commit touches **only** the Ducktape/Docker system-test
infrastructure used by the Apache Kafka project for running integration
tests against the Java broker and other JVM components. Specifically:

- `tests/docker/Dockerfile` — Dockerfile for the `ducker-ak` test
  container; has no counterpart in the Rust library.
- `tests/docker/ducker-ak` — Shell script managing the test cluster; has
  no counterpart in the Rust library.

The Rust client library:
- Does not use Docker for its own testing (tests are run with `cargo test`
  and the integration tests target a live or embedded Kafka cluster).
- Has no JVM dependency and no Java PATH management.
- Does not ship or reference any `ducker-ak` scripts or Dockerfiles.

There is no client-side behaviour change, no API change, no protocol
change, and no configuration change in this commit. Nothing needs to be
translated to Rust.

---

## Rust Implementation Plan

**No changes required.**

This commit is entirely about the Java system-test Docker infrastructure.
There is no Rust equivalent of `ducker-ak` or the `tests/docker/`
Dockerfile, and no client logic was modified.

### Files NOT changed

| Java file | Reason not translated |
|---|---|
| `tests/docker/Dockerfile` | Docker test infrastructure, no Rust equivalent |
| `tests/docker/ducker-ak` | Ducktape test-cluster shell script, no Rust equivalent |

---

## Test Plan

No tests need to be added or changed. The commit introduces no client
behaviour, no new configuration, and no observable difference in the
operation of the Rust client library.
