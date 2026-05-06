# Translation Design: MINOR: Add 4.0.1 to system tests (#20970)

## AK Commit

**Commit**: `ea8babe6990924465b7957ed0746cb9253eea55a`
**Branch**: `trunk`
**PR**: #60

## Summary of AK Change

This is a minor Apache Kafka maintenance commit that updates version references
in AK's own system test infrastructure from Kafka 4.0.0 to Kafka 4.0.1. The
affected files are:

| File | Change |
|---|---|
| `gradle/dependencies.gradle` | `kafka_40: "4.0.0"` → `kafka_40: "4.0.1"` |
| `tests/docker/Dockerfile` | Docker image installs `kafka-4.0.1` instead of `kafka-4.0.0` |
| `tests/kafkatest/version.py` | Adds `V_4_0_1 = KafkaVersion("4.0.1")` constant; updates `LATEST_4_0 = V_4_0_1` |
| `vagrant/base.sh` | Vagrant provisioner downloads `kafka-4.0.1` instead of `kafka-4.0.0` |

None of these files are part of the Kafka Java client library code. They
belong exclusively to AK's integration/system test harness and build tooling.

## Translation Decision: No-Op

**No Rust code changes are required for this commit.**

Rationale:

1. **Test-infrastructure only**: All four changed files are AK's internal
   system-test plumbing (ducktape/system tests, Docker/Vagrant provisioners,
   Gradle dependency versions). The Rust client project does not replicate AK's
   system-test infrastructure.

2. **No client library changes**: The Kafka client library source under
   `clients/src/main/java/org/apache/kafka/` is untouched. There are no new
   classes, no changed interfaces, no new behaviour to translate.

3. **No protocol changes**: No new Kafka protocol message types, no request/
   response schema changes, no configuration key additions.

4. **Version constant not mirrored**: The Rust client does not maintain a
   `LATEST_4_0` version constant or equivalent — version negotiation is handled
   at runtime via `ApiVersions`.

## Implementation Plan

This PR is a **no-op translation**. The only deliverable is this design
document explaining why no Rust changes are needed.

### Steps

1. Write this design document (`plan.md`). ✓
2. No code changes.
3. No test changes.

### Definition of Done

- [ ] This design document committed to the PR branch.
- [ ] PR description updated to reflect no-op status.

## Dependencies

- **Plan dependency**: none
- **Implementation dependency**: none
