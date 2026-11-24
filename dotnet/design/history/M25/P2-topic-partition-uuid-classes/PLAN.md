# Plan: TopicPartition and Uuid become classes (M25/P2)

Status: **approved 2026-11-22** (Pranav, main session: "I've gone through the plan and I approve it with all
recommendations"): decisions D1–D17 of §13 as recommended, recorded as ledger rows 609–625 (Agreed).
N = 134.
Branch `prashah_dev_dotnet_additional_rules`, HEAD `0091556a6`, main checkout, same PR (#356), no push.
Mode A: .NET, plus one commit in the shared error-class generator (`rust/xtask`, §10 S3). No change to the Rust
core, the C ABI, the C header, Python or the gRPC proto.

Path prefixes: `N/` = `dotnet/src/Confluent.Kafka/`, `NT/` = `dotnet/tests/Confluent.Kafka.UnitTests/`,
`D/` = `dotnet/`, `J/` = `kafka/clients/src/main/java/org/apache/kafka/`,
`JM/` = `kafka/clients/src/testFixtures/java/org/apache/kafka/clients/admin/MockAdminClient.java`,
`P/` = `python/confluent_kafka/`. "NPE" = Java's `NullPointerException`; "ISE" / "IAE" = Java's
`IllegalStateException` / `IllegalArgumentException` (and the .NET classes of the same names).

## 0. Words used in this plan

**struct and class.** In C#, a *struct* is a value. A *class* is an object you reach through a reference.

```csharp
public readonly struct PointS { public PointS(int x) { _x = x; } private readonly int _x; public int X() => _x; }
public sealed class PointC  { public PointC(int x) { _x = x; } private readonly int _x; public int X() => _x; }

PointS s = default;        // a real value: X() is 0. A struct variable can never be null.
PointS s2 = new PointS();  // compiles: every struct has a constructor with no arguments.
PointC c = null;           // compiles: a class variable can hold "no object".
// new PointC();           // does not compile: PointC has no such constructor.
```

- Rust: a struct is like a plain Rust value; a class is like `Option<Arc<T>>`.
- Python: every object is like a class. A variable can be `None`.
- Java: `TopicPartition` and `Uuid` are classes. A variable can be `null`. `new TopicPartition()` does not exist.

**`T?`.** For a struct, `TopicPartition?` is a different type, `Nullable<TopicPartition>`, with `.HasValue` and
`.Value`. For a class, `TopicPartition?` is only a note to the compiler: "this reference may be null". It has no
`.HasValue` and no `.Value`.

**sealed.** No class can derive from a sealed class. Java's `final` means the same.

## 1. TL;DR

1. `TopicPartition` and `Uuid` change from `public readonly struct` to `public sealed class`. Their members,
   names and constructors stay the same.
2. `default(TopicPartition)` and `default(Uuid)` become `null`. `new TopicPartition()` and `new Uuid()` stop
   compiling, as in Java. `Uuid.ZERO_UUID`, today `= default`, becomes `new Uuid(0L, 0L)`.
3. A `Uuid` the binding reads from the core is never `null` (the core always gives a value, `ZERO_UUID` when the
   broker sent none, as Java). `TopicPartition?` / `Uuid?` annotations follow Python's stubs (D15).
4. Every public method that takes a `TopicPartition` or a `Uuid` (alone, in a collection, or as a map key or
   value) handles `null` before the C ABI, by the five cases of §4.1. §4.2 lists every such method, checked
   against Java and Python. Never a `NullReferenceException`.
5. `RecordMetadata` keeps the topic and partition itself, so a delivered record allocates no extra object. A new
   test, written first on today's struct, proves it.
6. `==` and `!=` keep comparing values, and accept `null`.
7. The shared error-class generator learns that `TopicPartition` is a class (today it bails or skips the check).
8. The two analyzer baseline lines (`kind T:Confluent.Kafka.Common.TopicPartition`, `kind T:…Uuid`) go away.
9. Tests: 19 `default(TopicPartition)` sites keep testing a null topic; a test per §4.2 row; four admin
   "null-key" test rows come back; Java's `UuidTest` is completed.
10. A breaking change for users (the client is in preview). §9 shows what breaks and how to fix it.

## 2. Why

Emanuele Sabellico's review of PR #356:

- A struct can never be `null`. Where Java returns `null`, C# can only return a default value
  (`default(TopicPartition)` = topic `null`, partition 0; or `Uuid.ZERO_UUID`). Users must remember to check
  it. The nullable-reference warnings do not help with structs.
- A struct always has a constructor with no arguments. Java has none.
- The structs were chosen for speed. That may not be worth the cost above.

The analyzer rule of `0091556a6` (CKRDA0013 `kind`) says the same: "a Java class is a C# class, never a struct".
Today's two structs are in its baseline, to fix in this phase.

### What Java, Python and .NET do today

| Question | Java | Python | .NET today (struct) | .NET after (class) |
|---|---|---|---|---|
| Kind | `TopicPartition`: `final class`. `Uuid`: `class` (not final) | classes (Python has no `final`) | `readonly struct` | `sealed class` (both, D2) |
| Can a variable be null? | yes | yes (`None`) | no | yes |
| Constructor with no arguments | no | no (keyword arguments required) | yes, always | no |
| `new TopicPartition(null, 0)` | stored as given | stored as given | stored as given | stored as given (unchanged) |
| `equals` / `==` | `equals`: value, checks `getClass()`; `==` is identity | `__eq__`: value, checks `type()`; `== None` is `False` | `==` and `Equals`: value | `==` and `Equals`: value; `tp == null` is false, `null == null` is true (D3) |
| `toString` of a null topic | `"null-5"` | `"None-5"` (Python's spelling) | `"null-5"` | `"null-5"` (unchanged) |
| hash code | `TopicPartition` caches it in a field | computed | computed each call | cached, as Java (D5) |
| `Uuid.compareTo(null)` | NPE | raises (`AttributeError`) | impossible | `ArgumentNullException` (D4) |
| `Uuid.equals(null)` | `false` | `NotImplemented` → `False` | impossible | `false` |
| `Uuid.ZERO_UUID` | `new Uuid(0L, 0L)` | `Uuid(0, 0)` | `default` | `new Uuid(0L, 0L)` |

Sources: `J/common/TopicPartition.java` (`hashCode`, `equals`, `toString`), `J/common/Uuid.java` (`equals`,
`compareTo`, constants), `P/common/topic_partition.py`, `P/common/uuid.py`, `N/Common/TopicPartition.cs`,
`N/Common/Uuid.cs`.

## 3. The new types

### 3.1 TopicPartition

```csharp
// before                                              // after
public readonly struct TopicPartition                   public sealed class TopicPartition
    : IEquatable<TopicPartition>                            : IEquatable<TopicPartition>
{                                                       {
    private readonly string _topic;                         private readonly string _topic;
    private readonly int _partition;                        private readonly int _partition;
                                                            private int _hash;          // Java's cached hash (D5)
    public TopicPartition(string topic, int partition)      public TopicPartition(string topic, int partition)
    public string Topic();                                  public string Topic();
    public int Partition();                                 public int Partition();
    public static bool operator ==(TopicPartition l,        public static bool operator ==(TopicPartition? l,
                                   TopicPartition r);                                      TopicPartition? r);
    public bool Equals(TopicPartition other);               public bool Equals(TopicPartition? other);
    …                                                       …
}                                                       }
```

The null-safe operator and `Equals` (today's `==` calls `left.Equals(right)` and `Equals` reads
`other.Partition()`, so both would throw on `null` if left as they are):

```csharp
public static bool operator ==(TopicPartition? left, TopicPartition? right) =>
    ReferenceEquals(left, right) || (left is not null && left.Equals(right));

public bool Equals(TopicPartition? other) =>
    other is not null
    && (ReferenceEquals(this, other)
        || (_partition == other._partition && string.Equals(_topic, other._topic, StringComparison.Ordinal)));
```

- The object is 32 bytes on 64-bit with or without `_hash`, so caching costs nothing.
- `Topic()` keeps its non-null annotation, as today (`PublicTopicPartitionTests.TheTopicAnnotation_StaysNonNullable`);
  Python types it `str` too. The constructor still stores a `null` topic, as Java's does.
- `ToString` moves its text into one `internal static string Format(string? topic, int partition)`, so
  `RecordMetadata` (§5) prints exactly the same text without building a `TopicPartition`.

### 3.2 Uuid

```csharp
// before                                              // after
public readonly struct Uuid                             public sealed class Uuid
    : IEquatable<Uuid>, IComparable<Uuid>                   : IEquatable<Uuid>, IComparable<Uuid>
{                                                       {
    public static readonly Uuid ZERO_UUID = default;        public static readonly Uuid ZERO_UUID = new Uuid(0L, 0L);
    public int CompareTo(Uuid other);                       public int CompareTo(Uuid? other);   // null → ArgumentNullException (D4)
    public bool Equals(Uuid other);                         public bool Equals(Uuid? other);     // null → false
    public static bool operator ==(Uuid l, Uuid r);         public static bool operator ==(Uuid? l, Uuid? r);
    …                                                       …
}                                                       }
```

- `ZERO_UUID` must be built, not `default`: as a class, `default` is `null`, which would make `ZERO_UUID` `null`,
  put a `null` in `RESERVED`, and let `RandomUuid` return `(0, 0)`.
- `ONE_UUID`, `METADATA_TOPIC_ID`, `RESERVED` stay `static readonly` fields in the same order (static fields start
  in text order).
- `FromString(null)` keeps `ArgumentNullException` (Java: NPE; Python: `TypeError("str must not be None")`).
- `ToArray` / `ToList` copy `null` elements, as Java's and Python's do. Their annotations stay as they are.
- Sorting a list with `null`s through .NET's own comparer still works: `Comparer<Uuid>.Default` handles `null`
  before it calls `CompareTo`.

## 4. Where `null` can now come in

### 4.1 The rule: five cases

A method that takes a `TopicPartition` or a `Uuid` (alone, as a collection element, or as a map key or value)
handles `null` before any pin, marshal or P/Invoke (`ffi-marshalling.md` §A5, `D/CLAUDE.md` §3 "Call-site
checks"). Checks keep their place in Java's order: the closed and owner checks that run first today still run
first. For each method, find the first case that fits:

1. **Java checks for null and throws a Java class** → the same class and message.
   `Assign([null])`: Java checks `tp != null` and throws IAE "Topic partitions to assign to cannot have null or
   empty topic"; Python raises the same.
2. **Java treats `null` as a partition it does not know** (a map or set lookup that finds nothing, giving a Java
   class or an answer) → the same class and message, `null` printed as `null`, or the same answer.
   `Pause([null])`: Java's `SubscriptionState#assignedState` throws ISE "No current assignment for partition
   null", as for any partition the consumer does not hold. `ConsumerRecords.Records(null)`: Java's
   `records.get(null)` finds nothing and returns an empty list; Python returns `[]`.
   Today .NET already answers a *null topic* this way (`NativeConsumer.SnapshotPartitions`, "No current assignment
   for partition null-5"); a null partition prints `null`, as Java's.
3. **Java dereferences the `null`** (an NPE at the call, or later on its background thread or in the returned
   future) → `ArgumentNullException` at the call, the .NET-only precondition of §A5. Python raises its
   `TypeError("<param> must not contain None")` at the call in the same places. Messages:
   - a single argument: `new ArgumentNullException(nameof(partition))`;
   - an element or a key: `new ArgumentNullException(nameof(partitions), "partitions must not contain null")`
     (the text the binding already uses, for example "topics must not contain null" in `NativeConsumer`);
   - a key of a map whose values may be `null`: `"reassignments must not contain a null key"` (Python's
     "must not contain a None key").
4. **Java accepts the `null` but .NET cannot carry it** (the C ABI has no null partition, or a .NET `Dictionary`
   cannot hold a null key) → the same `ArgumentNullException` as case 3, recorded as a deviation, like today's
   "Admin null names" bullet (`D/CLAUDE.md` §3). Example: `MockConsumer.Commit` with a null key; Java's mock
   stores it with `putAll`.
5. **Java accepts it and .NET can too** → accept it. The class's own `Equals`, `GetHashCode` and `ToString` then
   handle the `null` as Java's do (`Objects.hash`, `"null"` in the text).

Never let a `null` reach `.Topic()` (a `NullReferenceException`) or a BCL `Dictionary` / `HashSet` key (the BCL's
own `ArgumentNullException("key")`). A `HashSet<TopicPartition>` can hold one `null`, and a user's own
`IReadOnlyDictionary` can yield a `null` key (BCL `Dictionary`, `SortedDictionary` and `ImmutableDictionary`
cannot), so both are checked.

A check before the call, in the shared helper (`NativeConsumer.SnapshotPartitions`):

```csharp
// before: a null element is impossible; a null topic is checked
foreach (TopicPartition tp in partitions)
{
    if (tp.Topic() is null)
    {
        throw nullTopicIsUnassigned
            ? new IllegalStateException(NoCurrentAssignment(tp.Partition()))
            : new ArgumentNullException(paramName, paramName + " must not contain a TopicPartition with a null topic");
    }
    …
}

// after: the element itself first
foreach (TopicPartition? tp in partitions)
{
    if (tp is null)
    {
        throw nullIsUnassigned                                              // case 2 (Pause, Resume, SeekTo…)
            ? new IllegalStateException("No current assignment for partition null")
            : new ArgumentNullException(paramName, paramName + " must not contain null");   // cases 3 and 4
    }

    if (tp.Topic() is null) { … as today … }
    …
}
```

### 4.2 Every entry point

Every row was checked against the Java source and, where Python has the method, the Python source (✓). Each
row says what Java's real client and Java's mock do, what Python does, what .NET does today once the type is a
class (before this phase adds checks: "NRE" = `NullReferenceException`), and the planned .NET exception and
message, with the case of §4.1. "4 clients" = the sync and async real client and the sync and async mock; they
share one implementation (`NativeConsumer`, `NativeProducer`, `NativeAdminClient`), so they behave the same
unless a row says otherwise.

Today's .NET answer for a *null topic* (not a null partition) stays as it is in every row.

#### Consumer (`IConsumer`, `IAsyncConsumer`; `KafkaConsumer`, `AsyncKafkaConsumer`, `MockConsumer`, `AsyncMockConsumer`)

| ID | .NET entry point (shape) | Java | Python | .NET today | .NET planned | ✓ |
|---|---|---|---|---|---|---|
| C1 | `Assign(partitions)`, real, null element | IAE "Topic partitions to assign to cannot have null or empty topic" (`AsyncKafkaConsumer#assign` checks `tp != null`) | same IAE (`consumer/_base.py` blanks a None) | NRE in `SnapshotAssignPartitions` | case 1: same IAE and message | ✓ |
| C2 | `Assign(partitions)`, mocks, null element | NPE, no message (`MockConsumer#assign` → `SubscriptionState#assignFromUser` reads `partition.topic()`) | `AttributeError` (`_subscription_state.py#assign_from_user`) | NRE in `SnapshotPartitions` | case 3: `ArgumentNullException("partitions", "partitions must not contain null")` | ✓ |
| C3 | `Pause` / `Resume(partitions)`, 4 clients, null element | ISE "No current assignment for partition null" (`SubscriptionState#assignedState`; real client rethrows it unwrapped; mock the same) | mock: `IllegalStateError` "No current assignment for partition None"; real: `AttributeError` in `_conversions.py#tp_to_spec` (an accident, not a rule) | NRE, after the closed and owner checks | case 2: ISE "No current assignment for partition null", after the closed and owner checks | ✓ |
| C4 | `SeekToBeginning` / `SeekToEnd(partitions)`, 4 clients, null element (a null collection keeps IAE "Partitions collection cannot be null") | ISE "No current assignment for partition null" (`SubscriptionState#requestOffsetReset`) | mock: `IllegalStateError` "…partition None"; real: `AttributeError` | NRE | case 2: ISE "No current assignment for partition null" | ✓ |
| C5 | `Position(partition)` / `Position(partition, timeout)`, null | real: ISE "You can only check the position for partitions assigned to this consumer."; mock: IAE, same text (`MockConsumer#position`) | real: `IllegalStateError`, same text; mock: `IllegalArgumentError`, same text | NRE, after the closed and owner checks | case 2: real ISE, mocks IAE, Java's text | ✓ |
| C6 | `Seek(partition, offset)`, null | a negative offset gives IAE first; then ISE "No current assignment for partition null" (real: background `seekUnvalidated`; mock: the same ISE) | `IllegalStateError` "No current assignment for partition None", after the negative check (`_base.py#_seek_args`) | NRE in the public wrapper, before the negative-offset and closed checks | case 2: ISE "No current assignment for partition null", after the negative-offset and closed checks | ✓ |
| C7 | `Seek(partition, offsetAndMetadata)`, null partition | a null `offsetAndMetadata` gives NPE first. Real, with a leader epoch: `KafkaException` wrapping NPE "TopicPartition cannot be null" (`Metadata#updateLastSeenEpochIfNewer`); without one: ISE as C6. Mock: ISE | null offset-and-metadata `TypeError` first; with an epoch: `KafkaError(cause=TypeError("partition must not be None"))`; without: `IllegalStateError` | NRE, before the null-`offsetAndMetadata` check | null `offsetAndMetadata` first (unchanged); then real with an epoch: `new KafkaException(new ArgumentNullException("partition"))` (message from `KafkaException(Exception)`, Java's wrap form, D17); otherwise ISE as C6 | ✓ |
| C8 | `CurrentLag(partition)`, null | real: ISE "No current assignment for partition null" (`SubscriptionState#partitionLag`); mock: `OptionalLong.of(0)` | real: a NULL partition reaches the core, which answers its precondition error "topic_partition must not be null" (`require_ref`), raised as `TypeError`; mock: 0 | NRE in the public wrapper | case 2: real ISE as Java; mocks return 0 | ✓ |
| C9 | `Commit(offsets)`, `Commit(offsets, timeout)`, `CommitAsync(offsets, callback)` (sync and async forms), null key | real: `new HashMap` accepts; the background thread NPEs (`CommitRequestManager`, or `Metadata` "TopicPartition cannot be null" with an epoch); mock: `putAll` accepts | real: `AttributeError` (`_conversions.py#offsets_to_spec`); mock: accepts | NRE in `OffsetsSnapshot.Of` | real: case 3, mocks: case 4: `ArgumentNullException("offsets", "offsets must not contain null")` | ✓ (Java's caller sees the background NPE wrapped; not run, does not change the plan) |
| C10 | `Committed(partitions)` / `(partitions, timeout)`, null element | real: background NPE (`CommitRequestManager`, `groupingBy(TopicPartition::topic)`); mock: filters by `committed::containsKey`, so no entry for null | real: `AttributeError`; mock: accepts, no entry | NRE | real: case 3, `ArgumentNullException("partitions", "partitions must not contain null")`; mocks: case 2, the null is skipped (no entry), as Java's mock | ✓ |
| C11 | `OffsetsForTimes(timestampsToSearch[, timeout])`, null key | real: background NPE (`OffsetFetcherUtils#topicsForPartitions`); mock: always `UnsupportedOperationException` "Not implemented yet." | real: `AttributeError`; mock: `UnsupportedOperationError` "Not implemented yet." | NRE (on the mock, before the core's unsupported error) | real: case 3, `ArgumentNullException("timestampsToSearch", "timestampsToSearch must not contain null")`; mocks: Java's `UnsupportedOperationException("Not implemented yet.")`, which the binding throws itself for a null key since the core cannot receive one | ✓ |
| C12 | `BeginningOffsets` / `EndOffsets(partitions[, timeout])`, null element | real: background NPE; mock: ISE "The partition null does not have a beginning offset." / "… an end offset." | real: `AttributeError`; mock: `IllegalStateError` "The partition None does not have …" | NRE | real: case 3, `ArgumentNullException("partitions", "partitions must not contain null")`; mocks: case 2, Java's ISE text | ✓ |
| C13 | mock `Rebalance(newAssignment)`, null element | IAE first if the mock was assigned manually; otherwise NPE with no message, after clearing records and calling `onPartitionsRevoked` (`PartitionStates#update`) | accepts, stores `None` | NRE | case 3: `ArgumentNullException("newAssignment", "newAssignment must not contain null")`, after the existing manual-assignment check and before any side effect *(deviation: Java's side effects before its NPE are not repeated)* | ✓ |
| C14 | mock `LosePartitions(partitionsLost)`, null element | ISE "Cannot lose partitions that are not currently assigned: [null]" (the list is every element not assigned) | `IllegalStateError`, same with `[None]` | NRE | case 2: ISE with Java's text; the binding builds the list itself from `Assignment()`, every element that is null or not assigned, in order, printed as Java prints a list | ✓ |
| C15 | mock `UpdateDurationOffsets(newOffsets)`, null key | `putAll` accepts | accepts | NRE, before the closed check | case 4: `ArgumentNullException("newOffsets", "newOffsets must not contain null")` | ✓ |
| C16 | `ConsumerRecords(records, nextOffsets)`, null key in `records` | stored as given (accepts) | accepts | BCL `ArgumentNullException("key")` from `Dictionary.Add` | case 4: `ArgumentNullException("records", "records must not contain null")` | ✓ |
| C17 | `ConsumerRecords(records, nextOffsets)`, null key in `nextOffsets` | NPE from `Map.copyOf` | `TypeError("next_offsets must not contain None")` | BCL `ArgumentNullException("key")` | case 3: `ArgumentNullException("nextOffsets", "nextOffsets must not contain null")` | ✓ |
| C18 | `ConsumerRecords.Records(partition)`, null | empty list (`records.get(null)`; an NPE only if the user's map is immutable) | `[]` | BCL `ArgumentNullException("key")` from `TryGetValue` | case 2: empty list | ✓ |
| C19 | `OnPartitionsLost` default (`ConsumerRebalanceListenerExtensions`, async twin), null element | the default calls `onPartitionsRevoked`, accepts | the default calls `on_partitions_revoked` | passes through | case 5: unchanged | ✓ |

Not a `TopicPartition` input, so not in the table (checked): `Assignment()`, `Paused()` (built from the core,
never null); `UpdateBeginningOffset` / `UpdateEndOffset(string topic, int partition, long offset)` (a
pre-existing shape difference from Java's `Map` form, out of scope here); the listener and commit callbacks
(they receive only partitions the binding built, never null).

#### Producer

| ID | .NET entry point | Java | Python | .NET today | .NET planned | ✓ |
|---|---|---|---|---|---|---|
| P1 | `SendOffsetsToTransaction(offsets, groupMetadata)`, real producers, null key | after the group-metadata, transaction and closed checks: NPE, no message (`KafkaProducer#sendOffsetsToTransaction`, `TopicPartition::topic`) | `AttributeError` (`producer/_base.py#offsets_to_spec`) | NRE in `OffsetsSnapshot.Of`, after the closed check | case 3: `ArgumentNullException("offsets", "offsets must not contain null")`, in Java's place (after those checks) | ✓ |
| P2 | `SendOffsetsToTransaction`, mock producers, null key | `putAll` accepts | accepts | NRE | case 4: the same `ArgumentNullException` | ✓ |
| P3 | `new RecordMetadata(null, …)` | stored; `topic()` / `partition()` / `toString()` NPE | stored; `TypeError("topic_partition must not be None")` at `topic()` | stored; `Topic()`, `Partition()`, `ToString()` throw `InvalidOperationException("topicPartition must not be null")` (ledger 281, 309) | case 5: unchanged (§5) | ✓ |

Outputs: `MockProducer.UncommittedOffsets()` / `ConsumerGroupOffsetsHistory()` cannot hold a null key, since P2
rejects it (Java's can). Checked.

#### Exception classes (`N/Common/Errors/`, `N/Consumer/`; all generated by `cargo xtask generate-error-codes` except `RecordDeserializationException`)

| ID | .NET entry point | Java | Python | .NET today | .NET planned | ✓ |
|---|---|---|---|---|---|---|
| E1 | `RecordDeserializationException(…, TopicPartition partition, …)`, null; getter `TopicPartition()` | stored; getter returns `null` | stored | stored | case 5: unchanged; annotation as Python's stub (D15) | ✓ |
| E2 | `NoOffsetForPartitionException(TopicPartition partition)`, null | `Collections.singleton(null)` accepts; message "…partitions: [null]" | accepts | accepts; message prints `null` (`JavaText.ValueOf`) | case 5: unchanged | ✓ |
| E3 | `NoOffsetForPartitionException(IReadOnlyCollection<TopicPartition> partitions)`, null element | NPE, no message (`Set.copyOf`) | `TypeError("partitions must not contain None")` (`_throwable.py#checked_set`) | accepts (`new HashSet` holds a null) | case 3: `ArgumentNullException("partitions", "partitions must not contain null")`, emitted by the generator (§10 S3) | ✓ |
| E4 | `OffsetOutOfRangeException(map)` / `(message, map)`, null key; `Partitions()` / `OffsetOutOfRangePartitions()` | stored as given | accepts | stored | case 5: unchanged | ✓ |
| E5 | `LogTruncationException(…, divergentOffsets)`, null key | `Collections.unmodifiableMap` accepts | accepts | stored (a null map is `ArgumentNullException`) | case 5: unchanged | ✓ |
| E6 | `RecordTooLargeException(message, recordTooLargePartitions)`, null key | stored | accepts | stored | case 5: unchanged | ✓ |

`InvalidOffsetException` takes only a string. Checked.

#### Admin (`IAdmin`, `IAsyncAdmin`; `KafkaAdminClient`, `AsyncKafkaAdminClient`, `MockAdminClient`, `AsyncMockAdminClient`)

For admin RPCs the plan keeps today's rule for a null topic name, "Admin null names" (`D/CLAUDE.md` §3): check at
the call, on all four clients, as Python's real client does. Java's real client mostly NPEs at the call (the
partition-leader lookup puts the keys in an immutable map, `PartitionLeaderStrategy.PartitionLeaderFuture`, which
rejects a null key), or fails a group's future later. Java's mock throws `UnsupportedOperationException("Not
implemented yet")` for several of these RPCs whatever the input; the .NET mocks check the argument first, as
Python's mock does for `DeleteRecords` (recorded).

| ID | .NET entry point | Java real | Java mock (`JM`) | Python | .NET today | .NET planned | ✓ |
|---|---|---|---|---|---|---|---|
| A1 | `DeleteRecords(recordsToDelete[, options])`, null key | NPE at the call (immutable map) | non-empty map: `UnsupportedOperationException("Not implemented yet")` | `TypeError("records_to_delete must not contain None")` at the call, before options; mock the same, then unsupported | NRE (`NativeAdminClient.Partitions`) | case 3: `ArgumentNullException("recordsToDelete", "recordsToDelete must not contain null")` | ✓ |
| A2 | `ListOffsets(topicPartitionOffsets[, options])`, null key | NPE at the call (immutable map) | NPE at the call (`endOffsets.get(null)` unboxed) unless a null key was seeded | `TypeError("topic_partition_offsets must not contain None")`; mock: `IllegalStateError("MockAdminClient has no end offset for null")` | NRE | case 3: `ArgumentNullException("topicPartitionOffsets", "topicPartitionOffsets must not contain null")`, after the existing isolation-level check | ✓ |
| A3 | `ElectLeaders(electionType, partitions[, options])`, null element (a null collection keeps meaning "all partitions", as Java) | no error at the call; the request build NPEs inside the network send, which ends the admin thread (read in `KafkaAdminClient#sendEligibleCalls`; not run) | `UnsupportedOperationException("Not implemented yet")` | `TypeError("partitions must not contain None")` after options | NRE (`NativeAdminClient.DistinctPartitions`) | case 3: `ArgumentNullException("partitions", "partitions must not contain null")` | ✓ |
| A4 | `AlterPartitionReassignments(reassignments[, options])`, null key (values may be null) | NPE at the call (`entry.getKey().topic()`) | NPE at the call (`partition.topic()`) | `TypeError("reassignments must not contain a None key")`; mock the same | NRE | case 3: `ArgumentNullException("reassignments", "reassignments must not contain a null key")` | ✓ |
| A5 | `ListPartitionReassignments(partitions[, options])`, null element (a null collection keeps meaning "all") | NPE at the call (`tp.topic()` in the validation loop) | accepts: `reassignments.get(null)` finds nothing, skipped | `TypeError("partitions must not contain None")`; mock accepts | NRE (`DistinctPartitions`; with null options, earlier in `AdminEarlyReturns`) | case 3 (real) and case 4 (mocks): `ArgumentNullException("partitions", "partitions must not contain null")` | ✓ |
| A6 | `AlterConsumerGroupOffsets(groupId, offsets[, options])`, null key | the group's future fails with NPE (`AlterConsumerGroupOffsetsHandler#buildBatchedRequest`) | `UnsupportedOperationException("Not implement yet")` (Java's typo) | `TypeError("offsets must not contain None")` | NRE | case 3: `ArgumentNullException("offsets", "offsets must not contain null")` at the call | ✓ |
| A7 | `DeleteConsumerGroupOffsets(groupId, partitions[, options])`, null element | the group's future fails with NPE (`groupingBy(TopicPartition::topic)`) | `UnsupportedOperationException("Not implemented yet")` | `TypeError("partitions must not contain None")` | NRE | case 3: `ArgumentNullException("partitions", "partitions must not contain null")` | ✓ |
| A8 | `ListConsumerGroupOffsets(groupSpecs[, options])`, a null element in a spec's `TopicPartitions()` | the group's future fails with NPE (`ListConsumerGroupOffsetsHandler#buildBatchedRequest`) | accepts (`topicPartitions.contains(key)` never matches null) | `AttributeError` at the call (no element check, unlike the other RPCs; a Python gap) | NRE | case 3 (real) and case 4 (mocks): `ArgumentNullException("groupSpecs", "groupSpecs must not contain null")`, the text the binding uses today for the other nulls in `groupSpecs` | ✓ |
| A9 | `DescribeProducers(partitions[, options])`, null element | NPE at the call (immutable map) | `UnsupportedOperationException("Not implemented yet")` | `TypeError("partitions must not contain None")`, before options | NRE | case 3: `ArgumentNullException("partitions", "partitions must not contain null")` | ✓ |
| A10 | `AbortTransaction(spec[, options])`, `spec.TopicPartition()` null | NPE at the call (`Collections.singleton(null)` into the immutable map) | `UnsupportedOperationException("Not implemented yet")` | `AttributeError` at the call (a Python gap) | NRE (`spec.TopicPartition().Topic()`) | case 3: `ArgumentNullException("spec", "spec must not have a null TopicPartition")`, beside today's "spec must not have a TopicPartition with a null topic" | ✓ |
| A11 | mock `UpdateBeginningOffsets` / `UpdateEndOffsets` / `UpdateConsumerGroupOffsets(newOffsets)`, null key | — | `putAll` accepts | accepts | NRE | case 4: `ArgumentNullException("newOffsets", "newOffsets must not contain null")` | ✓ |
| A12 | `AlterConsumerGroupOffsetsResult.PartitionResult(partition)` (async twin too), null | returns a future that fails with IAE "Alter offset for partition \"null\" was not attempted" once the call ends (or with the call's error) | — (the RPC is unsupported) | `TypeError("partition must not be None")` (a recorded Python deviation) | NRE; a null *topic* today returns a future already failed with IAE "…\"null-N\"…" (a recorded .NET deviation) | case 2: a future already failed with IAE "Alter offset for partition \"null\" was not attempted", as today's null-topic answer (D8) | ✓ |
| A13 | `DeleteConsumerGroupOffsetsResult.PartitionResult(partition)`, null | IAE at the call "Partition null was not included in the original request" | — | `IllegalArgumentError`, same text | NRE | case 2: IAE, Java's text | ✓ |
| A14 | `ListOffsetsResult.PartitionResult(partition)` (async twin too), null | client-built result: NPE (immutable map); a result a user built from a `HashMap`: IAE "List Offsets for partition \"null\" was not attempted" | same IAE | `IllegalArgumentError`, same text | BCL `ArgumentNullException("key")` from `TryGetValue` | case 2: IAE, Java's text (D8) | ✓ |
| A15 | `DescribeProducersResult.PartitionResult(partition)` (async twin too), null | Java's message is `"Topic partition " + partition + " was not included in the request"`, so "Topic partition null was not included in the request"; but Java's constructor is package-private and its client builds the map immutable, so a null lookup NPEs first | — | `IllegalArgumentError("Topic partition null was not included in the request")` | BCL `ArgumentNullException("key")` | case 2: IAE "Topic partition null was not included in the request", as Python (D8) | ✓ |
| A16 | public constructors `DeleteRecordsResult(futures)`, `ListOffsetsResult(futures)` (async twins too), null key | stored as given | — | accepts (`dict(futures)`) | BCL `ArgumentNullException("key")` from `Dictionary.Add` | case 4: `ArgumentNullException("futures", "futures must not contain null")` | ✓ |
| A17 | `new AbortTransactionSpec(topicPartition, …)`, null; getter `TopicPartition()` | stored; `equals` / `hashCode` null-safe (`Objects.hash`), `toString` prints `null` | stored | stored; `GetHashCode` NRE, `Equals` NRE via `==`, `ToString` prints "" | case 5: stored; `Equals`, `GetHashCode`, `ToString` null-safe, printing `null` | ✓ |
| A18 | `ListConsumerGroupOffsetsSpec.TopicPartitions(partitions)`, null element | stored; `Objects.equals` / `hash` null-safe | stored | stored; `GetHashCode` and `ToString` NRE | case 5: stored; `GetHashCode` / `ToString` null-safe (the RPC rejects it, A8) | ✓ |
| A19 | `new MemberAssignment(topicPartitions)`, null element | NPE (`Set.copyOf`) | — | `TypeError("topic_partitions must not contain None")` | stored (`HashSet`); `GetHashCode` / `ToString` NRE | case 3: `ArgumentNullException("topicPartitions", "topicPartitions must not contain null")` | ✓ |
| A20 | `new TransactionDescription(…, topicPartitions)`, null element | stored as given; null-safe `equals` / `hashCode` | — | accepts | stored; `GetHashCode` NRE, `ToString` prints "" | case 5: stored; `GetHashCode` null-safe, `ToString` prints `null` | ✓ |
| A21 | `new LogDirDescription(error, replicaInfos[, …])`, null key | stored as given | — | accepts | stored; `ToString` prints "" for it | case 5: stored; `ToString` prints `null` | ✓ |

Not a `TopicPartition` input (checked): `ReplicaInfo`, `NewPartitionReassignment`, `ListOffsetsResultInfo`,
`RecordsToDelete`, `OffsetSpec`, `PartitionReassignment`, `DeletedRecords`, every `*Options` class (they name
`TopicPartition` only in docs), `AlterReplicaLogDirs` / `DescribeReplicaLogDirs` (they take
`TopicPartitionReplica`), `MockAdminClient.AddTopic` (takes `TopicPartitionInfo`). No admin Result has an
indexer keyed by `TopicPartition`; the maps `Values()`, `LowWatermarks()`, `Partitions()` return are built from
the core, so their keys are never null.

#### Uuid

| ID | .NET entry point | Java real | Java mock (`JM`) | Python | .NET today | .NET planned | ✓ |
|---|---|---|---|---|---|---|---|
| U1 | `TopicCollection.OfTopicIds(topicIds)`, null element | accepted | — | accepted (`tuple(topic_ids)`) | accepted | case 5: unchanged | ✓ |
| U2 | `DeleteTopics(topics[, options])` with a topic-id collection, null element | NPE at the call (`topicId.equals(Uuid.ZERO_UUID)`) | the key's future fails with `UnknownTopicOrPartitionException("Topic null does not exist.")` | `TypeError("topics must not contain None")` at the call; mock: `UnknownTopicOrPartitionError("Topic null does not exist.")` | NRE (`AdminEarlyReturns.AnyTopicId`, or `NativeAdminClient.Topics`) | case 3 (real) and case 4 (mocks): `ArgumentNullException("topics", "topics must not contain null")` | ✓ |
| U3 | `DescribeTopics(topics[, options])` with a topic-id collection, null element | the key's future fails with `InvalidTopicException("The given topic id 'null' cannot be represented in a request.")` | `UnknownTopicIdException("Topic idnull not found.")` | `TypeError("topics must not contain None")`; mock: `UnknownTopicIdError("Topic idnull not found.")` | NRE | case 4 ("Admin null names": a null the C ABI cannot carry, checked at the call, as Python): `ArgumentNullException("topics", "topics must not contain null")` | ✓ |
| U4 | `new TopicListing(name, topicId, internal)`, null id | stored | — | stored; prints `null` | stored; `ToString` prints `topicId=` (empty) | case 5: stored; `ToString` prints `topicId=null`, as Java | ✓ |
| U5 | `new TopicDescription(…, topicId)` (five-argument form), null id; the shorter forms pass `ZERO_UUID` | stored; not used by `equals` / `hashCode` / `toString` | — | stored | stored | case 5: unchanged | ✓ |
| U6 | `new CreateTopicsResult.TopicMetadataAndConfig(topicId, …)`, null id | stored | — | stored (`topic_id: Uuid \| None`) | stored | case 5: unchanged; the error form's `_topicId` set to `ZERO_UUID`, as Java and Python (§6) | ✓ |
| U7 | `Uuid.CompareTo(null)`, `Equals(null)`, `==` / `!=` with null | `compareTo`: NPE; `equals`: `false` | — | comparisons raise; `== None` is `False` | NRE in all four | `CompareTo`: `ArgumentNullException("other")` (D4); `Equals`: `false`; operators null-safe | ✓ |
| U8 | `Uuid.ToArray(list)` / `ToList(array)` with null elements | copied | — | copied | copied | unchanged | ✓ |

## 5. RecordMetadata: no new object per record

The producer builds one `RecordMetadata` per delivered record, on the producer's completion thread
(`Internal/Interop/RecordMetadataMarshal.cs` `CopyOut`, called from `DeliveryState.Arrive`). Today it holds a
`TopicPartition?` field (a struct inside the object, no extra allocation). With a class, storing a
`TopicPartition` would add one 32-byte object per record.

Java's `RecordMetadata` has no getter that returns its `TopicPartition`: only `topic()`, `partition()` and
`toString()` read it. So .NET can keep the two values instead. Nobody can see the difference.

```csharp
// before
private readonly TopicPartition? _topicPartition;
public RecordMetadata(TopicPartition? topicPartition, long baseOffset, int batchIndex, long timestamp,
    int serializedKeySize, int serializedValueSize)
{
    _topicPartition = topicPartition;
    …
}
internal RecordMetadata(TopicPartition topicPartition, long offset, long timestamp, int keySize, int valueSize)
public string Topic() => StoredTopicPartition().Topic();

// after
private readonly string? _topic;
private readonly int _partition;
private readonly bool _hasTopicPartition;       // false only when the public constructor got null
public RecordMetadata(TopicPartition? topicPartition, long baseOffset, int batchIndex, long timestamp,
    int serializedKeySize, int serializedValueSize)
{
    if (topicPartition is not null)
    {
        _topic = topicPartition.Topic();         // may be null, as Java's topic() may return null
        _partition = topicPartition.Partition();
        _hasTopicPartition = true;
    }
    …
}
internal RecordMetadata(string topic, int partition, long offset, long timestamp, int keySize, int valueSize)
public string Topic() => _hasTopicPartition
    ? _topic!
    : throw new InvalidOperationException("topicPartition must not be null");
public override string ToString() => /* same check */ TopicPartition.Format(_topic, _partition) + "@" + …;
```

- The public constructor keeps Java's parameter. `TopicPartition?` now means "a reference that may be null"
  instead of `Nullable<TopicPartition>`; the behaviour with `null` is unchanged (ledger 281, 309).
- The object gets smaller: 56 bytes instead of 64 (the `Nullable<TopicPartition>` field was 24 bytes).
- The three internal callers (`RecordMetadataMarshal.CopyOut`, `DeliveryState.Arrive`'s placeholder,
  `FutureFailure.Placeholder`) call the internal `(string topic, int partition, …)` constructor.
- Python builds a `TopicPartition` per record (`RecordMetadata._from_ffi` calls `TopicPartition._of`). .NET does
  not; the difference cannot be observed.

### 5.1 The allocation tests

Today no budget test measures the completion thread, where `RecordMetadata` is built. The producer budget tests
measure only the caller's part of `Send` (`GetAllocatedBytesForCurrentThread` around `Send`). So:

- **S1 adds `NT/PublicProducerDeliveryAllocationBudgetTests.cs`, on today's struct.** It sends N records with an
  `ICallback` on an auto-completing `MockProducer`. In each `OnCompletion`, on the completion thread, it reads
  `GC.GetAllocatedBytesForCurrentThread()` and `Environment.CurrentManagedThreadId`. Per-record cost =
  (last − first) / (N − 1), best of several attempts; an attempt whose callbacks ran on more than one thread is
  thrown away. The constant is today's measured value plus 8 bytes (less than one object), with the measured
  number and what it holds written beside it, as the other budget tests do.
- **The constant never changes after S1.** If S2 makes it fail, the class added a per-record object, and S2 is
  wrong. That is what keeps the test honest: it was written and passed before the change.

What the existing budget tests do (none should change; the Actor runs them all in S2 and S7):

| Test file | What it measures | Moves? |
|---|---|---|
| `PublicProducerSendAllocationBudgetTests`, `PublicSyncProducerSendAllocationBudgetTests`, `PublicProducerDeliveryCallbackAllocationBudgetTests`, `PublicProducerHeadersAllocationTests` | caller thread inside `Send`; no `TopicPartition` is built there | no |
| `PublicSyncConsumerAllocationBudgetTests`, `PublicConsumerTypedAllocationBudgetTests`, `PublicConsumerTopicAllocationBudgetTests` | per record: a large poll minus a small one, so a per-poll cost cancels | no. The consumer builds `TopicPartition`s per partition per poll (`ConsumerRecordsMarshal` `AddPartition`, `NextOffsets`), not per record. `ConsumerRecord` stores `string _topic` and `int _partition`, not a `TopicPartition` |
| `PublicAsyncConsumerAllocationBudgetTests`, `PublicConsumerMemoryAllocationBudgetTests` | per record over many records; their notes say a per-poll cost cancels | no |
| `PublicSyncConsumerQueryAllocationBudgetTests` | a 4096-byte sanity ceiling per query | no (one 32-byte object per entry) |

If any budget test fails after S2 or S7, the Actor stops and reports. Changing a budget constant is a ruling, not a
fix.

## 6. Uuid: places that relied on the zero default

The most important line: `N/Common/Uuid.cs:48` is `public static readonly Uuid ZERO_UUID = default;`. As a class,
that makes `ZERO_UUID` itself `null`. It becomes `new Uuid(0L, 0L)`, as Java's.

| Site | Today | After |
|---|---|---|
| `N/Admin/CreateTopicsResult.cs` `TopicMetadataAndConfig._topicId`, never set by the `ApiException` constructor | zero by default | set to `Uuid.ZERO_UUID`, as Java's and Python's constructors do (not observable: `TopicId()` throws first) |
| `N/Internal/AdminEarlyReturns.cs` `AnyTopicId` (`id.Equals(Uuid.ZERO_UUID)`), `NativeAdminClient.Topics.cs` (`id.GetMostSignificantBits()`) | cannot meet `null` | the U2 / U3 check runs first |
| `N/Internal/Interop/UuidMarshal.cs` (`IntPtr.Zero` → `Uuid.ZERO_UUID`) | returns the constant | unchanged; correct once the constant is built |
| `Uuid.ToArray` (`new Uuid[list.Count]`) | every slot assigned | unchanged |

**The core never hands .NET an unset id other than zero** (checked): the Rust `Uuid` is a plain value
(`rust/src/admin/topic_listing.rs`, `topic_description.rs`: `topic_id(&self) -> Uuid`, never `Option`), the
generated messages default it to zero, and each C accessor (`kafka_admin_TopicListing_topic_id`,
`kafka_admin_TopicDescription_topic_id`, `kafka_admin_CreateTopicsResult_topic_id`, the describe-topics map keys)
returns `box_uuid(id)`, which is never NULL (`rust/src/ffi/common/uuid.rs`). So a `Uuid` read from the core is
never `null`, as in Java.

There is no `default(Uuid)`, `new Uuid()`, `Uuid?` or `TryGetValue(…, out Uuid)` in src, tests, grpc-server,
examples, soak or tools. 42 files mention `Uuid` (src 18, tests 20, grpc-server 3, examples 1). `TopicIdPartition`
does not exist in .NET (out of scope with the share consumer).

## 7. Tests

### 7.1 Tests that change

The 19 `default(TopicPartition)` sites today mean "a partition with a `null` topic". After the change they would
mean "no partition at all", which is a different test. Each site becomes `new TopicPartition(null!, 0)`, so it
keeps testing what it tested; a separate new test covers the `null` element (§7.2).

| File | Sites | Change |
|---|---|---|
| `NT/PublicTopicPartitionTests.cs` | 8 (`NullTopic_IsStoredAsGiven_AndEqualsDefault`, `ToString_OfANullTopic_IsJavasConcatenation`) | rewrite: a null-topic partition equals another null-topic partition; `default(TopicPartition)` is `null`; drop "equals default"; rename the test |
| `NT/PublicAdminListConsumerGroupOffsetsTests.cs` | 2 (226 comment, 234) | `new TopicPartition(null!, 0)` |
| `NT/PublicAdminListPartitionsRecordsTests.cs` | 1 comment (274) | reword |
| `NT/PublicConsumerRecordsTests.cs` | 1 (99, `Records(default)`) | becomes `Records(null!)` → empty (C18), plus a null-topic case |
| `NT/Interop/AdminP4SubmitArgumentTests.cs` | 2 (142, 153) | `new TopicPartition(null!, 0)` |
| `NT/Interop/AdminP5SubmitArgumentTests.cs` | 1 (717) | same |
| `NT/Interop/AdminConstructedNullTopicTests.cs` | 1 doc comment (31) | reword: the two paths are now one |
| `NT/PublicSyncConsumerQueryTests.cs` | 1 (384) | `new TopicPartition(null!, 0)` |
| `NT/PublicSyncConsumerPreconditionTests.cs` | 2 (101 comment, 105) | same |
| `NT/PublicConsumerOffsetQueryTests.cs` | 1 (266) | same |
| `NT/PublicAdminP14DifferentialTests.cs` | 2 (`TopicPartition nullTopic = default;` at 958, 1317) | `new TopicPartition(null!, 0)` |
| `NT/PublicAdminP8Tests.cs` | `AbortTransaction_RejectsADefaultTopicPartition` | rename; keep the null-topic case, add A10 |
| `NT/PublicUuidTests.cs` | `Shape_…` (interfaces) | add: it is a sealed class |

The compiler finds the rest: with `<Nullable>enable` and `TreatWarningsAsErrors`, a `default(TopicPartition)`
where a non-null one is expected is an error.

`AdminP14SyncCompositionTests.AssertValue<T>` uses `Assert.Equal` for a value type and `Assert.Same` for a class.
If a `Uuid` or `TopicPartition` flows through it, the test now checks that the same object comes back, which is
stricter. If it fails, the Actor reports it rather than loosening it.

### 7.2 Tests that are added

- **One test per row of §4.2** (C1–C19, P1–P3, E1–E6, A1–A21, U1–U8), with the exact class and message, and "no
  native call" (as the existing precondition tests do), on every client the row names, sync and async, real and
  mock. Rows whose answer is "accepted" assert that the value is kept and that `Equals`, `GetHashCode` and
  `ToString` do not throw.
- **The four admin "null-key" rows** in `NT/Interop/AdminP14SyncCompositionTests.cs`, left out today with the note
  "TopicPartition is a value type, so a null key cannot be expressed" (`DeleteRecordsRows`,
  `AlterPartitionReassignmentsRows`, `ListOffsetsRows`, `DescribeProducersRows`). Each asserts the §4.2 answer for
  a `null` (A1, A4, A14, A15), and the four "Omitted" notes lose "null-key".
- **`TopicPartition`**: `==` / `!=` / `Equals` with `null` on either side; the hash is stable across calls; it is a
  sealed class with one constructor.
- **`Uuid`**: U7; `ZERO_UUID` is not `null` and equals `new Uuid(0L, 0L)`; `RESERVED` holds no `null`; it is a
  sealed class.
- **RecordMetadata**: the delivery allocation test (§5.1); `new RecordMetadata(null, …)` still throws on
  `Topic()`, `Partition()`, `ToString()`; `new RecordMetadata(new TopicPartition(null!, 3), …)` returns a `null`
  topic and partition 3, and prints `null-3@…`, as Java.
- **The generator** (S3): an xtask unit test that `Set.copyOf` of a reference element emits the element check.

A test of a null element, as written:

```csharp
[Fact]
public void Pause_NullElement_ThrowsJavasIllegalStateException_BeforeAnyNativeCall()
{
    using MockConsumer<byte[], byte[]> consumer = NewMockConsumer();
    consumer.Assign(new[] { new TopicPartition("t", 0) });

    IllegalStateException e = Assert.Throws<IllegalStateException>(
        () => consumer.Pause(new TopicPartition[] { null! }));

    Assert.Equal("No current assignment for partition null", e.Message);
    Assert.Empty(consumer.Paused());
}
```

### 7.3 Java's tests (definition-of-done §3)

| Java test | Today in .NET | After |
|---|---|---|
| `TopicPartitionTest.testSerializationRoundtrip` | not translated | stays out: it tests Java's `Serializable`; .NET's class is not `[Serializable]` and `BinaryFormatter` is gone in .NET 9. Python translates it as a `pickle` round trip, which .NET has no peer of. Written in the header of `PublicTopicPartitionTests.cs` |
| `TopicPartitionTest.testTopiPartitionSerializationCompatibility` | not translated | stays out: it reads a checked-in Java-serialized file (Python skips it too) |
| `UuidTest.testSignificantBits` (`new Uuid(34L, 98L)`) | missing | add |
| `UuidTest.testUuidEquality` | `EqualityAndHashing_ComparesBothHalves` | check it covers `ZERO_UUID` and the three ids; add what is missing |
| `UuidTest.testHashCode` (23, 19064, −2011255899) | missing (no exact values) | add, with Java's three values |
| `UuidTest.testStringConversion` | `FromString_InvertsToString` | check the `ZERO_UUID` round trip |
| `UuidTest.testRandomUuid` (`@RepeatedTest(100)`) | `RandomUuid_IsVersion4_…` (a 1000-pass loop) | keep; it covers 100 |
| `UuidTest.testCompareUuids` | `CompareTo_IsJavasSignedOrder_MostSignificantFirst` | check it has Java's nine assertions |
| `UuidTest.testFromStringWithInvalidInput` | `FromString_RejectsWhatJavaRejects` | check the 32-byte and 4-byte inputs |
| `UuidTest.testToArray` / `testToList` | `ToArray_…`, `ToList_…` | covered |

The Uuid tests stay in `PublicUuidTests.cs`, with Java's names in a comment above each translated one.

## 8. The other projects

Each must build and pass its tests after S2 and S7.

| Project | Uses | Expected change |
|---|---|---|
| `D/grpc-server` (12 files) | `TopicPartition? _partition` in `ListenerAction.cs:300`, cast `(TopicPartition?)null` at 324, read with `is not TopicPartition partition` at 341; `Uuid` in `TranslateAdmin.cs`, `AdminServiceImpl.cs`, `AsyncAdminServiceImpl.cs` (built with `Uuid.FromString` or read from the core, never null) | none: all compile for a class (checked by reading). Run `make -C dotnet test-grpc-server-dotnet` |
| `D/soak` (`SoakClient.cs`) | builds and reads partitions | none expected; `make -C dotnet test-soak-dotnet` |
| `D/examples` (16 files, mostly `AdminApis/`; `CreateTopics.cs` names `Uuid`) | builds partitions, prints them | none expected; `make -C dotnet test-examples-dotnet` (build and format) |
| `D/tools/AotSmoke` | one use | none expected; `make -C dotnet test-aot-dotnet` (Native AOT; a class is fine for trimming) |
| `D/tools/VerifiableClients` (5 files) | consumer and test doubles | none expected; `make -C dotnet test-verifiable-clients-dotnet` |
| perf (`D/tests/Performance`) | builds partitions | none expected; `make -C dotnet perf-unit-test-dotnet` |

## 9. What breaks for a user

The client is in preview, so a break is allowed. It goes in the PR description.

```csharp
// 1. Nullable<T> is gone: .HasValue and .Value do not compile.
TopicPartition? tp = FindPartition();
if (tp.HasValue) { Use(tp.Value); }   // before
if (tp is not null) { Use(tp); }      // after

// 2. default is null.
TopicPartition tp = default;          // before: topic null, partition 0. After: null.
int p = tp.Partition();               // before: 0. After: NullReferenceException.

// 3. No constructor without arguments.
var tp = new TopicPartition();        // before: compiles. After: error CS7036.
var tp = new TopicPartition("orders", 0);   // after: write the topic and partition

// 4. Uuid's default is null, not ZERO_UUID.
Uuid id = default;
bool zero = id == Uuid.ZERO_UUID;     // before: true. After: false (id is null).
Uuid id = Uuid.ZERO_UUID;             // after: say what you mean

// 5. Passing null is now possible, and gets Java's answer at the call (§4.2).
consumer.Pause(new TopicPartition[] { null! });   // after: IllegalStateException "No current assignment for partition null"
admin.DeleteRecords(map with a null key);         // after: ArgumentNullException "recordsToDelete must not contain null"
```

Nothing else changes: `new TopicPartition("t", 0)`, `Topic()`, `Partition()`, `==`, `Equals`, dictionary keys,
`ToString` and every `Uuid` member work as before.

## 10. Steps for the Actor

One commit per step. Each .NET commit builds on all four TFMs (`net462`, `netstandard2.0`, `net8.0`, `net10.0`),
and passes the unit tests on net8.0 and net10.0, `dotnet format --verify-no-changes` and the analyzers
(CKRDA0013's lists only shrink). The Critic reviews after each commit, and checks each step's rows against §4.2
and the Java source, not only the code. .NET-only commits: build, format and tests first, then `--no-verify`
(the hook is for Rust and C changes). S3 is a Rust commit and runs the hook.

Build from `dotnet/`: `dotnet build -c Release -p:CargoProfileDir=debug Confluent.Kafka.sln` (the native library
is built under `rust/target/debug`).

**S1 — the delivery allocation test, on today's struct.** Test only (§5.1). Gate: the test passes three runs in a
row on net8.0 and net10.0. (dotnet-actor, dotnet-critic.)

**S2 — `TopicPartition` is a sealed class.** (dotnet-actor, dotnet-critic.)
- `N/Common/TopicPartition.cs` as in §3.1 (cached hash, null-safe operators, `internal static Format`).
- `RecordMetadata` keeps topic and partition (§5); its three internal callers use the internal constructor.
- Fix every compile error the change causes (nullable warnings are errors). No new null checks yet, except where
  the compiler needs one.
- The 19 `default(TopicPartition)` sites (§7.1) become null-topic partitions, so they still test what they tested.
- Delete `kind T:Confluent.Kafka.Common.TopicPartition` from `D/analyzers/java-name-baseline.txt` in this same
  commit (a stale baseline line fails the build as CKRDA0014). Confirm with `cargo xtask dotnet-java-name-baseline`
  that nothing else changes.
- Gate: as above, plus the S1 test unchanged and green, every budget test of §5.1 green, and
  `make -C dotnet test-grpc-server-dotnet test-soak-dotnet test-examples-dotnet test-verifiable-clients-dotnet
  test-aot-dotnet perf-unit-test-dotnet`.

**S3 — the error-class generator learns that `TopicPartition` is a class.** Rust tooling (`rust/xtask`), so
actor-executor and kafka-critic, as M25/P1's S1. (D16.)
- `rust/xtask/src/error_hierarchy/csharp.rs`: `cs_type` maps `TopicPartition` as a reference type; `is_value_type`
  no longer lists it. Today the `Set.copyOf` branch stops generation for a reference element ("would need a
  null-element check"); it now emits that check, throwing `ArgumentNullException(nameof(x), "x must not contain
  null")`, as Python's generator emits `checked_set(..., elements=True)`.
- Regenerate (`cargo xtask generate-error-codes`): `NoOffsetForPartitionException(IReadOnlyCollection<…>)` gains
  E3's check; any annotation change in the other generated classes follows Python's stubs (D15).
- Gate: `cargo test` in `rust/xtask` (with the new unit test), `cargo xtask check-generated`,
  `cargo xtask format-check`, `cargo xtask lint`, the hook; then the .NET gate.

**S4 — consumer null handling.** Rows C1–C19, in the shared helpers (`SnapshotPartitions`,
`SnapshotAssignPartitions`, `OffsetsSnapshot.Of`, `SnapshotTimestamps`, `RequirePositionPartition`, the `Seek` /
`CurrentLag` wrappers, `ConsumerRecords`). The doc comments that say "an element's topic is null" also say "or an
element is null", with the row's exception: `IConsumer.cs` (lines 235, 286, 307, 514, 534, 557, 574, 594, 610,
629, 645), `IAsyncConsumer.cs` (396, 457, 483, 798, 824, 859, 881, 905, 927, 951, 973), `MockConsumer.cs` (347,
389, 406), `AsyncMockConsumer.cs` (462, 489). One test per row (§7.2). Gate: as above.

**S5 — producer and exception classes.** Rows P1–P3, E1–E6. Gate: as above.

**S6 — admin.** Rows A1–A21: the RPC checks, the Result lookups, the value classes' null-safe `Equals` /
`GetHashCode` / `ToString`, the four "null-key" rows, and the two doc comments that say "a
`default(TopicPartition)`" (`N/Admin/IAdmin.cs:1880`, `N/Admin/IAsyncAdmin.cs:1706`), reworded to "a null
partition or a partition whose topic is null". Gate: as above.

**S7 — `Uuid` is a sealed class.** §3.2, §6, rows U1–U8, the Uuid tests of §7.3. Delete
`kind T:Confluent.Kafka.Common.Uuid` from the baseline in this commit. Gate: as S2.

**S8 — docs and records.**
- `D/CLAUDE.md` §3 "Call-site checks": a bullet with §4.1's five cases, and the case-4 deviations (C9 mocks, C15,
  C16, P2, A5 and A8 mocks, A11, A16, U2 and U3 mocks); the "Admin null names" bullet names a null partition and a
  null topic id too. The Java-names rule 1 (`kind`) needs no change; the `RecordMetadata` sketch keeps
  `TopicPartition? topicPartition`, now a nullable reference.
- `ffi-marshalling.md` §A5 "Precondition" and its consumer twin: a null element of a collection of value classes
  is checked like a null collection; the "Tests required" lists name it.
- `D/design/current/STATUS.md`: the M25/P2 entry; the D4 entry (ledger 281, 309) says `TopicPartition?` is now a
  nullable reference.
- The proposed ledger rows (§13), for the PM to add after Pranav's rulings.

**S9 — close-out.** The full gate (§11), then archive `COMMENTS.DONE.134.md` in this folder and reset
`COMMENTS.134.md`.

## 11. The close gate

- `make -C dotnet check-dotnet` and `make -C dotnet test-dotnet-macos` (unit and soak tests, net8.0 and net10.0).
- The other projects of §8.
- The native gRPC arms (`make test-integration-dotnet-native`) only after checking Docker's free memory first: the
  M25/P1 run at 6 threads with 11 containers up killed the user's `kafka-perf` container. `kafka-perf` (port 9092)
  must never be stopped or removed. Four unit tests (`PublicAdminNonNumericBrokerNameTests.RealClient_*`) fail
  locally only because `kafka-perf` holds 9092; they are not caused by this phase.
- CI runs the Linux gate (`verify-except-grpc-arm`, the gRPC arms) on the PR.

## 12. Open branches that will need a rebase

Scanned read-only on 2026-11-22: 297 refs (84 local, 213 `origin/`). 14 are already in HEAD, 208 have no .NET
`TopicPartition`. For the other 75, the scan diffed each branch against its merge base with HEAD and looked for
added lines with `default(TopicPartition)`, `default(Uuid)`, `TopicPartition?`, `Uuid?`, `ZERO_UUID = default`,
`new TopicPartition()`, `new Uuid()`, or `TopicPartition x = default;`.

- **No branch adds a struct-only use that HEAD does not already have.** Every added line found is already in HEAD
  (so this phase fixes it), or is an older wording of a test HEAD has in a newer form.
- These branches touch the same lines, so they will conflict on rebase:

| Branch (local and/or `origin/`) | Last commit | What it has | Note |
|---|---|---|---|
| `prashah_dev_dotnet_review_followups` | 2026-11-17 | the two admin doc comments naming `default(TopicPartition)` | the most recent of these local branches; HEAD has the same two comments, which S6 rewords, so a rebase conflicts on them |
| `origin/dev_multilanguage-conversion` | 2026-11-18 | `TopicPartition? _partition` in `grpc-server/ListenerAction.cs` | compiles unchanged with a class |
| `prashah_dev_dotnet_admin_options_overloads` (+ `-presquash`) | 2026-11-16 | two `default(TopicPartition)` admin tests | not in HEAD's history, but HEAD has the same two lines word for word, so §7.1 covers them; a rebase conflicts on them |
| `prashah_dev_dotnet_java_shape_gaps` | 2026-11-12 | `Records(default(TopicPartition))` test | HEAD has the same line (§7.1, C18); a rebase conflicts on it |
| `origin/dev_dotnet-getters-as-methods` | 2026-11-07 | `ZERO_UUID = default`, a spec test with `default(TopicPartition)`; edits `TopicPartition.cs`, `Uuid.cs`, `RecordMetadata.cs` | old; its lines are in HEAD |
| `prashah_dev_dotnet_cffi_port`, `prashah_dev_dotnet_namespace_alignment` | 2026-11-04 / 11-01 | older wordings of the same admin tests; edit all three type files | squash-merged into master as #259 |
| `origin/prashah_dev_dotnet_java_name_fixes`, `origin/dev_dotnet-error-categorization` | 2026-11-08 / 11-05 | edit `Uuid.cs` (and `RecordMetadata.cs`) | old; conflict only on those files |

## 13. Decisions for Pranav

Each has a recommendation. Proposed ledger rows follow the same numbers (609 = D1, …).

1. **Do it: both types become classes.** *Recommended: yes.* Java and Python have classes; the analyzer rule asks
   for it; the speed cost is one object per partition per poll and none per record (§5).
2. **Sealed.** `TopicPartition` is `final` in Java, so `sealed`. Java's `Uuid` is open, but its `equals` checks
   `getClass()`, so a subclass gains nothing. *Recommended: both `sealed`*, as every .NET value class here (ledger
   601). Unsealing later is not a break; sealing later is.
3. **`==` and `!=`.** Keep them, comparing values and accepting `null` (Python's `==` compares values; Java's
   `==` compares identity). *Recommended: keep.* Removing them would silently turn every user's `==` into an
   identity check.
4. **`Uuid.CompareTo(null)`.** Java throws NPE; Python raises. .NET's usual rule is "any object is greater than
   null". *Recommended: `ArgumentNullException`* (ffi §A5: a .NET-only precondition where Java throws NPE).
   Sorting with `Comparer<Uuid>.Default` still works, since it handles `null` first.
5. **Cache the hash in `TopicPartition`**, as Java does. *Recommended: yes* (free: the object is 32 bytes either
   way).
6. **`RecordMetadata` keeps topic and partition**, not a `TopicPartition` (§5). The public constructor keeps
   `TopicPartition? topicPartition` and today's `InvalidOperationException` for `null` (ledger 281, 309).
   *Recommended: yes.*
7. **The five cases of §4.1** (Java's check; Java's "unknown partition" answer; `ArgumentNullException` where Java
   dereferences; the same where Java accepts but .NET cannot carry it, recorded; accept where both can), applied
   as §4.2's rows say. *Recommended: yes.* Changed from the first draft: case 2 is new. It gives Java's own answer
   (`IllegalStateException "No current assignment for partition null"`, an empty list, …) where Java treats `null`
   as a partition it does not know, instead of an `ArgumentNullException`; Python's mocks answer the same way, and
   .NET already answers a null *topic* this way.
8. **Admin Result lookups with `null`** (A12–A15): Java's IAE text, which prints `null`; `AlterConsumerGroupOffsets`
   returns a future already failed with it, as .NET does today for a null topic. *Recommended: yes.* Python agrees
   for A13–A15; for A12 Python raises `TypeError` (its own recorded deviation), and .NET keeps Java's answer.
9. **The delivery allocation test first, on the struct, its constant frozen after.** *Recommended: yes.*
10. **Java's `TopicPartitionTest`** stays out (Java serialization), the reason in the test file; **`UuidTest`** is
    completed in Java order (§7.3). *Recommended: yes.*
11. **`TopicPartition.Topic()` stays annotated non-null** although the constructor stores a `null` topic, as
    today and as Python's `str`. *Recommended: keep.*
12. **No compatibility shim, no Python change.** Python already has classes. Two Python gaps found on the way
    (A8 and A10 raise `AttributeError` instead of `TypeError`) go to the Python owners as a note. *Recommended:
    yes.*
13. **`Uuid` from the core**: never `null` (checked, §6). *Recommended: yes, no special handling.*
14. **Base**: `prashah_dev_dotnet_additional_rules` at `0091556a6`, PR #356, no push. *Recommended: yes.*
15. **New: annotations follow Python's stubs.** A `TopicPartition` / `Uuid` parameter or return is `?` exactly where
    Python's stub says `| None` (for example `RecordMetadata`'s constructor, `ElectLeaders`' and
    `ListPartitionReassignments`' "all partitions" collections, `TopicMetadataAndConfig`'s `topic_id`); elements
    stay non-null. *Recommended: yes* (one checkable rule instead of a judgement per member).
16. **New: the error-class generator** (`rust/xtask`) changes in S3, by the actor-executor with the kafka-critic,
    so that generated exceptions check a null element where Java's `Set.copyOf` does (E3). The alternative, an
    allowlisted difference, keeps `NoOffsetForPartitionException` accepting what Java and Python reject.
    *Recommended: change the generator.*
17. **New: `Seek(null, offsetAndMetadata)` with a leader epoch** on the real consumers (C7): `KafkaException`
    wrapping `ArgumentNullException("partition")`, as Java wraps its NPE and Python wraps its `TypeError`; without
    an epoch, and on the mocks, Java's ISE. *Recommended: yes.*

Proposed ledger rows (to be filled with the rulings):

| # | ID in the evaluation doc | Decision | My pick | Status | Ruling and notes | Date |
|---|---|---|---|---|---|---|
| 609 | new (PM 134, M25/P2 PLAN D1; Emanuele's review of PR #356) | `TopicPartition` and `Uuid`: struct or class | sealed classes | Evaluate | | |
| 610 | new (PM 134, D2) | Sealing | both `sealed` | Evaluate | | |
| 611 | new (PM 134, D3) | `==` / `!=` | value equality, null-safe | Evaluate | | |
| 612 | new (PM 134, D4) | `Uuid.CompareTo(null)` | `ArgumentNullException` | Evaluate | | |
| 613 | new (PM 134, D5) | `TopicPartition` hash | cached, as Java | Evaluate | | |
| 614 | new (PM 134, D6) | `RecordMetadata` storage | topic + partition, no per-record object; supersedes the storage part of 309 | Evaluate | | |
| 615 | new (PM 134, D7) | Null handling for `TopicPartition` / `Uuid` inputs | §4.1's five cases, §4.2's rows | Evaluate | | |
| 616 | new (PM 134, D8) | Admin Result lookups with `null` | Java's IAE text | Evaluate | | |
| 617 | new (PM 134, D9) | The delivery allocation test | first, on the struct; constant frozen | Evaluate | | |
| 618 | new (PM 134, D10) | Java's `TopicPartitionTest` / `UuidTest` | out (serialization) / completed | Evaluate | | |
| 619 | new (PM 134, D11) | `Topic()` annotation | stays non-null | Evaluate | | |
| 620 | new (PM 134, D12) | Shim and Python | none / unchanged; two Python gaps noted | Evaluate | | |
| 621 | new (PM 134, D13) | `Uuid` from the core | never null; no special handling | Evaluate | | |
| 622 | new (PM 134, D14) | Base branch | `prashah_dev_dotnet_additional_rules` at `0091556a6` | Evaluate | | |
| 623 | new (PM 134, D15) | Nullable annotations | as Python's stubs | Evaluate | | |
| 624 | new (PM 134, D16) | Error-class generator | changed in S3 (actor-executor, kafka-critic) | Evaluate | | |
| 625 | new (PM 134, D17) | `Seek(null, offsetAndMetadata)` with an epoch | `KafkaException(ArgumentNullException)` | Evaluate | | |

## 14. Risks

1. **A meaning change in tests.** `default(TopicPartition)` changes from "null topic" to "null". Mitigation: §7.1
   changes every site in S2, before any check is added.
2. **Many small answers.** §4.2 has 57 rows, and three cases give different exceptions for one method on the real
   client and on the mock (C10, C11, C12). Mitigation: one test per row; the Critic checks each step's rows
   against the Java source.
3. **Budget tests.** A per-poll object is expected to cancel in every consumer budget. If one fails, the Actor
   stops (§5.1).
4. **Shared tooling.** S3 edits `rust/xtask`. Mitigation: `check-generated` guards the generated files; only one
   generated class is expected to change.
5. **Branch conflicts** (§12): text conflicts only, no new struct-only use; the newest are
   `prashah_dev_dotnet_review_followups` and `origin/dev_multilanguage-conversion`.
6. **Docker memory** at the close gate (§11).

## 15. What cannot be checked before the work starts

- **The completion thread's per-record allocation today**, and whether every budget test stays green after S2 and
  S7. Why: these are measurements of code that S1, S2 and S7 write; S1 measures the first, the gates check the
  second.
- **How Java's real client reports some background NPEs to the caller** (C9–C12: probably a `KafkaException`
  wrapping the NPE, from `ConsumerUtils#getResult`) and **the end of Java's `ElectLeaders` with a null element**
  (A3: the NPE in the network send seems to end the admin thread). Why: both need a running Java client and broker;
  the source was read, not run. The plan does not depend on them: in both, .NET throws `ArgumentNullException` at
  the call, as Python does.
