# Translation Plan: MINOR — Fix rare flaky behaviour in testPreferredReplicaElection

**AK commit:** `ce152e7395b520cfbe76ce944aa7920b61ae769e`
**AK branch:** trunk
**PR:** #109
**Rust branch:** `kafka-translate/ce152e7395b520cfbe76ce944aa7920b61ae769e`

---

## Summary of the Apache Kafka Commit

This is a **test-only** commit (tagged MINOR). It fixes a rare flaky test
`testPreferredReplicaElection` in `LeaderElectionCommandTest`. The test was
failing intermittently with the error "X replica(s) could not be elected"
because it used `--all-topic-partitions`, which attempts preferred leader
election on all internal topics in addition to the test topic. While the test
waited for the test topic to join the ISR, it did not wait for all internal
topics to synchronize their ISR membership after broker restart.

The fix narrows the election scope to target only the specific test topic
partition using `--topic` and `--partition` flags instead of
`--all-topic-partitions`.

**Changed files:**

| File | Change |
|------|--------|
| `tools/src/test/java/org/apache/kafka/tools/LeaderElectionCommandTest.java` | Replace `--all-topic-partitions` with `--topic`/`--partition` targeting specific partition |

**Diff:**
```java
// Before:
"--election-type", "preferred",
"--all-topic-partitions"

// After:
"--election-type", "preferred",
"--topic", topic,
"--partition", Integer.toString(partition)
```

---

## Rust Translation Analysis

### Equivalent Rust code

The Rust codebase does **not** have a translation of `LeaderElectionCommand` or
its associated test `LeaderElectionCommandTest`. A search for leader election,
election command, or tools-related modules yields no results in the Rust `src/`
or `tests/` directories.

### Assessment

This commit modifies only a Java integration test for a CLI tool
(`kafka-leader-election`) that has not been translated to Rust. The fix is
purely about test reliability (narrowing partition scope to avoid timing issues
with internal topics) and does not affect any client library logic, protocol
handling, or API behavior.

---

## Implementation Plan

### No action required

This commit is **out of scope** for the Rust translation because:

1. `LeaderElectionCommand` is a server-side CLI admin tool, not a client library
   component.
2. The changed file is a test for that tool, which does not exist in the Rust
   codebase.
3. No client-facing API, protocol, or behavioral change is introduced by this
   commit.

---

## Files to Modify

| File | Action | Reason |
|------|--------|--------|
| (none) | — | No Rust equivalent exists for this test-only server tooling change |

No files need to be created or modified.

---

## Out of Scope

- Translating `LeaderElectionCommand` — this is a server-side admin CLI tool,
  not part of the client library translation scope.
- Translating `LeaderElectionCommandTest` — requires a full KRaft cluster test
  harness with broker restart capabilities.

---

## Definition of Done

- [x] Design document written acknowledging this commit is out of scope.
- [x] No code changes required for the Rust codebase.
