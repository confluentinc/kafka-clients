# Translation Design: MINOR — Address flaky wait for producer acks logging

**AK commit:** `db336c3ff8287da6a9db84f727d1623885b1ac8a`
**AK branch:** trunk
**PR:** #107
**Rust branch:** `kafka-translate/db336c3ff8287da6a9db84f727d1623885b1ac8a`

---

## Summary of the Java Commit

This is a minor commit (no JIRA ticket) that improves the reliability and
debuggability of Kafka system tests written in Python (using the
`kafkatest` framework). The changes are authored by Bill Bejeck and
address flakiness in `test_fencing_consumer`.

### Changes

1. **`tests/kafkatest/services/kafka/kafka.py`** — adds a new method
   `describe_consumer_group_members(group, node, command_config)` that
   runs `kafka-consumer-groups.sh --describe --members` and returns the
   parsed output lines. This is used solely for diagnostic logging in
   failing test assertions.

2. **`tests/kafkatest/tests/client/consumer_test.py`** — modifies
   `test_fencing_consumer`:
   - Increases `await_produced_messages` timeout from the default to
     120 seconds.
   - Wraps several assertions in `try/except` blocks to log
     `describe_consumer_group_members` output before re-raising, aiding
     diagnosis of flaky failures.
   - Increases `wait_until` timeouts from 50s to 60s for waiting on
     consumers to leave the group.
   - Adds descriptive error messages that include group membership info
     to `wait_until` calls.

---

## Applicability to the Rust Client Library

### Verdict: No translation required

This commit modifies **Python system test infrastructure** only. The
files changed are:

| File | Purpose |
|---|---|
| `tests/kafkatest/services/kafka/kafka.py` | Python test harness service wrapper |
| `tests/kafkatest/tests/client/consumer_test.py` | Python system test for consumer fencing |

These are integration/system tests that exercise the Kafka broker and
Java clients via the `ducktape` testing framework. They have no
equivalent in the Rust client library because:

1. **No `kafkatest` framework in Rust** — The Rust project uses standard
   Rust testing (`#[cfg(test)]`, integration tests) and does not have a
   `ducktape`-based system test suite.

2. **No consumer group protocol implementation** — The Rust client
   library does not currently implement the consumer group protocol,
   static membership, or consumer fencing. The test being fixed
   (`test_fencing_consumer`) tests behavior that has no Rust counterpart.

3. **No behavioral change** — The commit does not change any library
   code, protocol handling, or configuration parsing. It only adds
   logging and adjusts timeouts in test scaffolding.

---

## Rust Implementation Plan

**No files to change.** This commit is entirely out of scope for the
Rust translation.

### Files NOT changed

| Java/Python file | Reason not translated |
|---|---|
| `tests/kafkatest/services/kafka/kafka.py` | Python test infrastructure, no Rust equivalent |
| `tests/kafkatest/tests/client/consumer_test.py` | Python system test, no Rust equivalent |

---

## Test Plan

No tests to add or modify. The original commit contains no behavioral
changes to translate.
