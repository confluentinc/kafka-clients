# Translation Design: MINOR — Cleanup DelayedOperationKey

**AK commit:** `7d54f7b036c15553558ef5638f559622d7b23d4a`
**AK branch:** trunk
**PR:** #85
**Rust branch:** `kafka-translate/7d54f7b036c15553558ef5638f559622d7b23d4a`

---

## Summary of the Java Commit

This is a pure refactoring commit that converts several `DelayedOperationKey`
subclasses from plain Java classes to Java `record`s, removing boilerplate
`equals()`, `hashCode()`, and `toString()` implementations that Java records
provide automatically.

### Files changed

| File | Change |
|---|---|
| `server-common/.../purgatory/TopicPartitionOperationKey.java` | Convert class → record; remove manual `equals`, `hashCode` |
| `server-common/.../purgatory/DelayedOperationTest.java` | Convert inner `MockKey` class → record in test |
| `server/.../share/fetch/DelayedShareFetchGroupKey.java` | Convert class → record; remove manual `equals`, `hashCode`, `toString` |
| `server/.../share/fetch/DelayedShareFetchPartitionKey.java` | Convert class → record; remove manual `equals`, `hashCode`, `toString` |
| `storage/.../purgatory/DelayedRemoteListOffsetsTest.java` | Update field access from `key.topic` / `key.partition` to `key.topic()` / `key.partition()` (record accessor syntax) |

### Before/After example (`TopicPartitionOperationKey`)

**Before** (plain class, ~30 lines):
```java
public class TopicPartitionOperationKey implements DelayedOperationKey {
    public final String topic;
    public final int partition;

    public TopicPartitionOperationKey(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public boolean equals(Object o) { ... }

    @Override
    public int hashCode() { return Objects.hash(topic, partition); }
}
```

**After** (record, ~10 lines):
```java
public record TopicPartitionOperationKey(String topic, int partition) implements DelayedOperationKey {
    public TopicPartitionOperationKey(TopicPartition tp) {
        this(tp.topic(), tp.partition());
    }

    @Override
    public String keyLabel() {
        return topic + "-" + partition;
    }
}
```

The net effect is the same runtime semantics with ~103 fewer lines of boilerplate.

---

## Applicability to the Rust Client Library

### All changed classes are server-side

Every class modified by this commit lives in the **server** or
**server-common** Maven module (package `org.apache.kafka.server.*`),
not in the `clients` module:

| Class | Maven module | Package |
|---|---|---|
| `TopicPartitionOperationKey` | `server-common` | `org.apache.kafka.server.purgatory` |
| `DelayedShareFetchGroupKey` | `server` | `org.apache.kafka.server.share.fetch` |
| `DelayedShareFetchPartitionKey` | `server` | `org.apache.kafka.server.share.fetch` |

These are Kafka **broker** internals — part of the delayed-operation
purgatory used for producer/fetch request management on the broker side.
The Confluent Kafka Rust library is a **client** library; it does not
implement or translate any broker-side purgatory logic.

There are no Rust counterparts for any of these classes in the current
codebase, and there is nothing to translate.

### Test file changes are also server-side

`DelayedOperationTest.java` and `DelayedRemoteListOffsetsTest.java` are
in `server-common` and `storage` test modules respectively. They test
broker purgatory functionality that is out of scope for the Rust client.

---

## Rust Implementation Plan

**No changes required.**

This commit is entirely composed of server-side refactoring that has no
client-side equivalent. The Rust Kafka client library does not implement
the delayed-operation purgatory, share-fetch infrastructure, or any
related broker internals.

### Files NOT changed

| Java file | Reason not translated |
|---|---|
| `TopicPartitionOperationKey.java` | Broker purgatory, no Rust equivalent |
| `DelayedShareFetchGroupKey.java` | Broker share-fetch purgatory, no Rust equivalent |
| `DelayedShareFetchPartitionKey.java` | Broker share-fetch purgatory, no Rust equivalent |
| `DelayedOperationTest.java` | Server-side test, out of scope |
| `DelayedRemoteListOffsetsTest.java` | Storage-module test, out of scope |

---

## Test Plan

No new tests are needed. No production or test Rust code is changed by
this translation.
