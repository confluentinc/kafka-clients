# PR #38 Translation Design: KAFKA-19876 Use eclipse-temurin as base image

## AK Commit

- **Commit:** `99fafc44bae130f59abe896413641ce6538e9eab`
- **Title:** KAFKA-19876 Use eclipse-temurin as the base image to align with 3.9 (#20945)
- **Author:** Ming-Yen Chung
- **Branch:** trunk

## Summary of Changes

This commit modifies two files in the Apache Kafka ducktape test infrastructure:

### `tests/docker/Dockerfile`

Removes PATH-based JDK exposure and replaces it with symlinks:

```diff
-  && echo "PATH=$(runuser -l ducker -c 'echo $PATH'):$JAVA_HOME/bin" >> /home/ducker/.ssh/environment \
-  && echo 'PATH=$PATH:'"$JAVA_HOME/bin" >> /home/ducker/.profile \
+# Symlink all JDK binaries (java, jcmd, jps, etc.) to /usr/bin.
+# We don't add PATH env to ~/.ssh/environment because on some Linux distributions,
+# PAM (Pluggable Authentication Modules) may re-read /etc/environment, overwriting PATH.
+RUN cp -sn $JAVA_HOME/bin/* /usr/bin/
```

**Reason:** On some Linux distributions, PAM re-reads `/etc/environment` after
`~/.ssh/environment`, overwriting the `PATH` set there. The symlink approach avoids
this problem entirely.

### `tests/docker/ducker-ak`

Changes the default JDK Docker base image:

```diff
-default_jdk="sapmachine:17-jdk-ubuntu-jammy"
+default_jdk="eclipse-temurin:17-jdk-jammy"
```

**Reason:** `sapmachine` does not support JDK 8, so switching to `eclipse-temurin`
allows the same image provider to be used across all AK versions (including AK 3.9
which requires JDK 8 support).

## Relevance to Rust Translation

This commit is **entirely confined to Apache Kafka's ducktape distributed test
framework** (`tests/docker/`). Ducktape is a Python-based framework used to run
multi-node integration and system tests against the Java Kafka broker and clients.

The Confluent Kafka Rust client does **not** use ducktape or the ducktape Docker
infrastructure. The Rust client's test infrastructure is completely independent:

- **Unit tests:** standard Rust `cargo test`
- **Integration tests:** [`testcontainers`](https://github.com/testcontainers/testcontainers-rs)
  crate spins up a real Kafka broker in Docker via the standard Testcontainers API

There is no `tests/docker/Dockerfile` or `tests/docker/ducker-ak` equivalent in
the Rust repository, and none is needed.

## Translation Decision: No Changes Required

**This commit does not translate to any code change in the Rust repository.**

The change is exclusively about the JDK base image and the method of exposing JDK
binaries inside the ducktape Docker container. Neither of these concepts has any
analog in the Rust client codebase.

### Checklist

| Item | Applicable? | Action |
|------|------------|--------|
| Docker test infrastructure (`tests/docker/`) | No — Rust uses testcontainers | None |
| JDK base image selection | No — Rust has no JVM dependency | None |
| PATH / symlink setup for JDK binaries | No — Rust has no JVM dependency | None |
| Protocol changes | No | None |
| Client API changes | No | None |
| Configuration changes | No | None |
| New request/response types | No | None |

## Conclusion

PR #38 requires **no implementation work**. The AK commit is a maintenance change
to Kafka's internal CI/test Docker setup that has no corresponding concept in the
Rust client.

No Actor/Critic cycle is needed. The branch can be merged as-is (containing only
the submodule bump commit and this design document).
