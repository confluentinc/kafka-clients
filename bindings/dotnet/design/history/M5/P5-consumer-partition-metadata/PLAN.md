# M5/P5 — "Consumer partition-metadata queries" (N=14)

**Milestone/Phase:** M5 / P5
**Review number:** N = 14 (last completed: N=13, M5/P4 "Consumer offset-map query
siblings" = Category E1).
**Category:** **E2** — partition-metadata queries. **Completes Category E** (the
consumer query family). **Mode A** (all ABI verified present in
`target/include/confluent_kafka.h`).
**Personas:** `dotnet-actor` (steps 5–7, header-down C# only), `dotnet-critic`
(C# review). No Rust-core work (Mode A).

---

## 1 · Scope

Two **async** query methods on `IAsyncConsumer`, both reusing the **owned-handle
completion bridge** E1 established (`SubmitOwnedHandleOperation<T>` +
`OperationCompletionSource<T>` + a per-container `OnPoll`-clone trampoline), plus
the binding's **first nested public value types** (`PartitionInfo`, `Node`).

| Java | .NET | ABI async fn → owned container | New value type |
|---|---|---|---|
| `partitionsFor(String[, Duration])` → `List<PartitionInfo>` | `Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken = default)` | `kafka_consumer_Consumer_partitions_for_async(consumer, topic, cb, ud)` → `PartitionInfoList_t` | `PartitionInfo`, `Node` |
| `listTopics([Duration])` → `Map<String,List<PartitionInfo>>` | `Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(CancellationToken = default)` | `kafka_consumer_Consumer_list_topics_async(consumer, cb, ud)` (no input) → `TopicPartitionInfoMap_t` | (same) |

Both on `IAsyncConsumer` (blocking-in-Java / callback-at-ABI → async, §4). One
method each — **NO `TimeSpan` overload** (the async ABI carries no timeout);
`CancellationToken` = user-initiated cancellation only (→ `wakeup()`), **not** a
timeout/deadline (the `Position` / `Close` / E1 precedent).

### ABI ground truth (verified in the generated header — line refs)

- **Callbacks — owned-handle shape** (`(container*, error*, ud)`), identical to
  E1's offset-map family:
  - `kafka_consumer_Consumer_partitions_for_callback_t(PartitionInfoList_t*, KafkaError*, void*)` (h.209)
  - `kafka_consumer_Consumer_list_topics_callback_t(TopicPartitionInfoMap_t*, KafkaError*, void*)` (h.220)
  - Success: container non-null + error null; failure: container null + error
    non-null. The callback **owns whichever is non-null** — `_destroy` the
    container, or `FromHandle` the error. Same contract as `OnCommitted` /
    `OnLongOffsets`.
- **Root containers (Category-3 owned borrow-roots):**
  - `PartitionInfoList_t`: `_count` (h.1405), `_get(list, i)` → `const PartitionInfo_t*` (h.1415, **borrowed**), `_destroy` (h.1419).
  - `TopicPartitionInfoMap_t`: `_count` (h.1436), `_get_topic(map, i)` → `const char*` **NUL-terminated, handle-owned** (h.1439–1447), `_get_partitions(map, i)` → `const PartitionInfoList_t*` (h.1459, **nested borrowed** container), `_destroy` (h.1470).
- **`PartitionInfo_t` accessors** (all `const *` returns → **borrowed views**):
  - `_topic(info)` → `const char*` **NUL-terminated, handle-owned** (h.1160–1167)
  - `_partition(info)` → `int32_t` (h.1177)
  - `_leader(info)` → `const kafka_common_Node_t*` (h.1188) — **null if no leader**
  - `_replica_count` + `_replica(info, i)` → `const Node_t*` (h.1198/1208)
  - `_in_sync_replica_count` + `_in_sync_replica(info, i)` (h.1219/1230)
  - `_offline_replica_count` + `_offline_replica(info, i)` (h.1241/1252)
  - `PartitionInfo_destroy` (h.1264) exists **only** for a standalone-owned info;
    as a `PartitionInfoList_get` / `TopicPartitionInfoMap_get_partitions` element
    it is a **borrowed Category-4 view** — **NEVER freed**. Classify by the
    accessor's const-ness (§B2 note): `_get` returns `const *` → borrowed.
- **`Node` = `kafka_common_Node_t`** (the `common` namespace, h.88–92): `_id(node)`
  → `int32_t` (h.1124); `_host(node, out_len)` → `const char*` (h.1135);
  `_port(node)` → `int32_t` (h.1146); `_rack(node, out_len)` → `const char*` or
  **(null, -1) if absent** (h.1149–1156). **No `Node_destroy`** — a Node is a
  borrowed view, freed with its owning `PartitionInfo` (which is itself freed with
  the root container). See §3 for the string-form divergence.

---

## 2 · Nested copy-out plan (the central risk — §B2 Cat-3/4, §B4, consumer-threading §27)

The **whole tree is owned by the ROOT container** — `PartitionInfoList_t` for
`PartitionsFor`; `TopicPartitionInfoMap_t` (→ nested `PartitionInfoList_t`) for
`ListTopics`. Everything below the root — every `PartitionInfo`, its leader `Node`,
the three `Node` lists, and every `Node`'s `host`/`rack` string — is a **borrowed
view** and becomes a dangling pointer the instant the root `_destroy`s. So the
trampoline must **copy the entire tree into owned managed values on the dispatcher
thread BEFORE the single root `_destroy`** (in the `finally`), exactly as
`OnCommitted` copies the offset map before `OffsetMapDestroy`.

**Borrow discipline (hard invariant, Critic-checkable):** the trampoline destroys
**only the root container, exactly once, in the `finally`**. It **NEVER** calls
`PartitionInfo_destroy`, and there is no `Node_destroy` to call. Freeing any
`_get` element or any `Node` is a double-free/UAF and a review-blocker.

### Marshaller layering (model on E1's `OffsetMapMarshal` + `ConsumerRecordsMarshal`)

Four shared `internal static` copy-out marshallers in `Internal/Interop/`, layered
so the two public methods reuse the same core:

1. **`NodeMarshal.CopyOut(IntPtr node)` → `Node`** — reads `_id` / `_host(out len)`
   / `_port` / `_rack(out len)`; **null node → `null`** (leader may be absent).
   The single place the length-delimited string form is used (§3).
2. **`PartitionInfoMarshal.CopyOut(IntPtr info)` → `PartitionInfo`** — reads
   `_topic` (NUL-scan), `_partition`, `_leader` (→ `NodeMarshal`, may be null), and
   the three replica lists (`_replica_count` + `_replica(i)` → `NodeMarshal` per
   element, likewise ISR + offline), producing owned `IReadOnlyList<Node>` each.
3. **`PartitionInfoListMarshal.CopyOut(IntPtr list)` → `IReadOnlyList<PartitionInfo>`**
   — `_count` + `_get(i)` → `PartitionInfoMarshal` per element; `count ≤ 0` →
   `Array.Empty<PartitionInfo>()` (§8 resolved — no new `EmptyReadOnlyList<T>` surface).
4. **`TopicPartitionInfoMapMarshal.CopyOut(IntPtr map)` →
   `IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>`** — `_count` +
   `_get_topic(i)` (NUL-scan) + `_get_partitions(i)` → **`PartitionInfoListMarshal`
   per entry** (the nested borrowed list is copied out too, before the root
   destroy); `count ≤ 0` → `EmptyReadOnlyDictionary` (E1's singleton).

All four take a borrowed `IntPtr`, retain no native pointer after returning, and
run entirely on the dispatcher thread before the root `_destroy`. The nesting is
the only new dimension vs E1: E1 copied a flat map of scalars; E2 copies a
2-to-3-level tree (map → list → info → 4× node-or-node-list → node strings).

---

## 3 · The `Node` length-delimited string finding (the one shape difference from E1)

**`Node_host` and `Node_rack` are LENGTH-DELIMITED `(const char*, out int32_t len)`
— NOT NUL-terminated** (h.1127–1156: "returns … as a (ptr, len) pair (NOT
NUL-terminated; borrows into the owning `PartitionInfo`)"). This is the §B3
**receive-path** string form (like `ConsumerRecord_topic` / header keys) — use
`Utf8Marshal.PtrToString(ptr, len)` (both overloads already exist:
`Utf8Marshal.cs` L54 NUL-scan, L99 length form). **NEVER NUL-scan a Node string** —
the slice borrows into the container with no terminator, so a scan over-reads into
the next field (the classic §B3 over-read AV).

Contrast — **verified NUL-terminated** (handle-owned; use `PtrToString(ptr)`,
NUL-scan): `PartitionInfo_topic` (h.1160 "as a NUL-terminated C string (owned by
the handle)") and `TopicPartitionInfoMap_get_topic` (h.1439 same). So within one
tree the two string forms **coexist** — topic names NUL-scan, Node host/rack use
`out_len`. The marshaller must not use one form uniformly. All copied out before
the root `_destroy`.

**`rack` absence:** `_rack` returns **(null, -1)** when absent (h.1149). Map that to
`Node.Rack == null` (Java's `rack()` is nullable). `PtrToString(IntPtr.Zero, …)` →
`null` (verify the length-overload's null-ptr guard covers the `-1` case; if not,
gate on `ptr == IntPtr.Zero` before the call).

---

## 4 · The two new nested public value types (the binding's FIRST nested types)

Both `public sealed class` in the `Confluent.Kafka` root namespace, getter-only
properties, full XML docs, Java-faithful (`org.apache.kafka.common.{Node,
PartitionInfo}`), `internal` constructor (fields are owned copies read out of a
borrowed ABI element). Style mirrors the shipped `OffsetAndTimestamp` /
`OffsetAndMetadata` (`sealed class`, not `readonly struct`; result payloads, not
hot-path map keys).

```csharp
public sealed class Node          // org.apache.kafka.common.Node
{
    internal Node(int id, string host, int port, string? rack);
    public int Id { get; }
    public string Host { get; }
    public int Port { get; }
    public string? Rack { get; }   // Java rack() is nullable → null when absent (§3)
    public override string ToString();   // e.g. "host:port (id: N rack: R)"
}

public sealed class PartitionInfo // org.apache.kafka.common.PartitionInfo
{
    internal PartitionInfo(string topic, int partition, Node? leader,
        IReadOnlyList<Node> replicas, IReadOnlyList<Node> inSyncReplicas,
        IReadOnlyList<Node> offlineReplicas);
    public string Topic { get; }
    public int Partition { get; }
    public Node? Leader { get; }          // ABI _leader may be null → nullable
    public IReadOnlyList<Node> Replicas { get; }
    public IReadOnlyList<Node> InSyncReplicas { get; }
    public IReadOnlyList<Node> OfflineReplicas { get; }
    public override string ToString();
}
```

**`Leader` nullability:** the ABI `_leader` accessor may return null (h.1180 "or
null if the partition has no leader"); Java `PartitionInfo.leader()` can also be
null. Model as `Node? Leader`. (The mock always populates a leader — §6 — but the
type must not assume it.)

**`IEquatable` / `ToString` decision:** take **`ToString` only, NO `IEquatable`**,
per the E1 value-type precedent (`OffsetAndTimestamp` shipped `ToString`, no
`IEquatable` — they're results, not dict keys). Note Java overrides `equals`/
`hashCode` on both `Node` and `PartitionInfo`; add `IEquatable` (+ `GetHashCode`)
**only if a test needs value equality** — flag as an open question (§8) rather than
gold-plating. The dictionary in `ListTopics` is keyed by `string` (topic), not by
these types, so equality is not required for the result shape.

**Nested-type note:** `PartitionInfo` holds `Node` and lists of `Node` — the
binding's first public type composed of another public type. Both live flat in the
`Confluent.Kafka` root (§3 project layout — public types stay at the root while the
surface is small; a topical folder is not yet warranted).

---

## 5 · Bridge reuse — do NOT re-litigate the proven paths

Reuse E1's owned-handle bridge verbatim (`NativeConsumer.cs`
`SubmitOwnedHandleOperation<TResult>`, L1572 — verified callback-type-agnostic: it
allocs the `GCHandle`, wires cancellation → `Wakeup`, and calls a
`NativeOwnedHandleSubmit submit` that captures its own strongly-typed rooted
callback at the call site, passing only `(consumer, userData)`). Add:

- **Two `OnPoll`-clone trampolines** in `ConsumerCallbacks.cs`, one per container,
  each differing from `OnCommitted` only in (a) the `OperationCompletionSource<T>`
  result type, (b) the copy-out marshaller called on success
  (`PartitionInfoListMarshal` / `TopicPartitionInfoMapMarshal`), and (c) which
  `_destroy` runs in the `finally` (`PartitionInfoListDestroy` /
  `TopicPartitionInfoMapDestroy`). Every `OnPoll` invariant preserved verbatim:
  no-throw boundary, copy-out on the dispatcher thread **before** `_destroy`,
  container `_destroy` in the `finally` on every path (null-safe), `GCHandle` freed
  exactly once, `RunContinuationsAsynchronously`. Each needs its own delegate type
  matching the header typedef (`OperationCallback`-family precedent).
- **`PartitionsFor` submit** pins its one topic string **call-scoped** (§A3/§B3 —
  the core copies it synchronously during the async submit; verify against
  `src/ffi/consumer.rs partitions_for_async`), P/Invokes
  `partitions_for_async(consumer, topicPtr, cb, ud)`, unpins in a `finally`. E1's
  `WithPinnedTopics` is array-shaped; for a single topic a scoped `Utf8Marshal.Pin`
  (its `PinnedUtf8String` disposable) is the simpler fit — do not force the array
  helper.
- **`ListTopics` submit** takes **no input** — just P/Invokes
  `list_topics_async(consumer, cb, ud)`.

**Do NOT disturb** the proven poll / void / scalar / E1 offset-map paths. New
`NativeMethods` `[DllImport]` decls: the two `_async` fns, the two callback
delegate typedefs, and the container accessors/destroys
(`PartitionInfoList_{count,get,destroy}`,
`TopicPartitionInfoMap_{count,get_topic,get_partitions,destroy}`,
`PartitionInfo_{topic,partition,leader,replica_count,replica,in_sync_replica_count,
in_sync_replica,offline_replica_count,offline_replica}`,
`Node_{id,host,port,rack}`) — each with `EntryPoint` set to the full symbol (§0.1).
`Node_host`/`_rack` take an `out int` length. **No `SafeHandle`** for any of these —
the root containers are Category-3 read-and-destroyed-in-the-trampoline (not
long-lived), and the elements/Nodes are Category-4 borrowed views.

---

## 6 · Mock wiring & reachability (`MockConsumer_update_partitions`)

**Wire the last unwired mock helper.** `kafka_consumer_MockConsumer_update_partitions(
consumer, topic, partition_count, leader_id, leader_host, leader_port)` (h.928) sets
partition metadata broker-free — closing the last Python-mock-parity gap. Add an
`UpdatePartitions(string topic, int partitionCount, int leaderId, string leaderHost,
int leaderPort)` **inherent** forwarder on `AsyncMockConsumer` (mock-only, not on
`IAsyncConsumer`) — same pattern as M5/P3's `Update*Offset`. Pin both strings
call-scoped, check the returned `KafkaError*` (non-null → throw / faulted per §B5),
`illegal_state` on a non-mock/async-consumer per the header.

**Reachability — verified against `src/consumer/mock_consumer.rs` +
`src/ffi/consumer.rs` L1301-1306.** The FFI builds, for each of `partition_count`
partitions `p ∈ [0, count)`:
`PartitionInfo::new(topic, p, Some(Node::new(leader_id, host, leader_port)),
replicas=[leader], in_sync_replicas=[leader])` — and `PartitionInfo::new` sets
`offline_replicas = []`. `Node::new(id, host, port)` is the 3-arg form → **rack =
None**.

So, broker-free after `UpdatePartitions`:

| Field | Reachable (assert in tests) | Documented-empty (mock limit) |
|---|---|---|
| `PartitionInfo.Topic` | ✅ = the topic | |
| `PartitionInfo.Partition` | ✅ = 0..count-1 | |
| `PartitionInfo.Leader` | ✅ `Node{Id=leaderId, Host=leaderHost, Port=leaderPort, Rack=null}` | |
| `PartitionInfo.Replicas` | ✅ `[leader]` (single, == Leader) | |
| `PartitionInfo.InSyncReplicas` | ✅ `[leader]` (single, == Leader) | |
| `PartitionInfo.OfflineReplicas` | | ⚠ always empty (mock never sets offline) |
| `Node.Rack` | | ⚠ always null (mock uses 3-arg `Node::new`) |

Tests assert the reachable fields (topic / partition / leader id-host-port /
replicas[0] / isr[0]) and **document** `OfflineReplicas` empty + `Rack` null as a
**reachable-slice limit of the mock, not a silent gap** — the marshaller code paths
for offline replicas and rack are still exercised structurally (empty list / null),
and their non-empty behavior is a broker/integration concern. This is a
reachability disclosure, not a skipped requirement.

---

## 7 · Deliverables

**New files** (`Internal/Interop/`): `NodeMarshal.cs`, `PartitionInfoMarshal.cs`,
`PartitionInfoListMarshal.cs`, `TopicPartitionInfoMapMarshal.cs` (+ optional
`EmptyReadOnlyList.cs` if not reusing `Array.Empty<T>()`).
**New public types** (root): `Node.cs`, `PartitionInfo.cs`.
**Edited:** `NativeMethods.cs` (the `[DllImport]` block of §5), `ConsumerCallbacks.cs`
(two `OnPoll`-clone trampolines + delegate typedefs), `NativeConsumer.cs`
(`PartitionsForWithCallback` / `ListTopicsWithCallback` submit paths +
`UpdatePartitions` forwarder plumbing), `IAsyncConsumer.cs` (the two methods +
**remove `partitionsFor`/`listTopics` from the "not-yet-wired" remark** at L59 — the
remaining not-wired list becomes: commit family + pattern subscribe + rebalance
listener), `AsyncKafkaConsumer.cs` + `AsyncMockConsumer.cs` (impls; `UpdatePartitions`
inherent on the mock).

**Tests** (`tests/Confluent.Kafka.UnitTests/`):
- Marshaller unit tests mirroring `OffsetMapMarshalTests` — nested copy-out
  correctness, empty container → empty result, borrowed-view-not-freed (the tree
  survives a simulated root destroy because it's copied).
- `PartitionsFor`: after `UpdatePartitions`, returns the reachable
  `PartitionInfo`+`Node` (assert §6 fields); resolves via the `Task`.
- `ListTopics`: multiple topics → dictionary keyed by topic; each value the
  per-topic list; empty → empty non-null dictionary.
- **§B3 non-ASCII round-trip**: a non-ASCII `Node.Host` marshalled via the
  **length** form (guards against NUL-scanning the length-delimited slice); a
  non-ASCII NUL-terminated topic via the scan form; a multi-byte char at the slice
  boundary.
- **Errors/preconditions (§B5):** `PartitionsFor(null)` → `ArgumentNullException`;
  **`PartitionsFor("")` is forwarded to the core (NOT rejected)** — Java/Python-faithful
  (§8 resolved: Python does no topic validation; the binding guards only `null` for FFI
  panic-safety); operational failure (mock `set_poll_error`) → faulted `Task` with
  `KafkaException` (**assert message**); concurrent async op → faulted
  `KafkaException`; post-`Dispose` → `ObjectDisposedException`; `wakeup()`/pre-canceled
  token → `KafkaException`(Wakeup) / `OperationCanceledException`.
- **Handle lifecycle (§B2):** the root container is `_destroy`d exactly once; no
  element/Node freed; `GCHandle` + error handle freed exactly once on every path
  (success / operational error / concurrent-reject).
- **Allocation-budget** on the copy-out (DoD §10 / consumer-threading §27 precedent):
  the per-`PartitionInfo` copy allocates only the owned managed tree (info + node +
  strings + lists) — no allocation attributable to native traversal beyond the
  unavoidable managed copies.
- **TFM-matrix smoke:** the two methods work on net462(via ns2.0)/net8.0/net10.0.

**DoD:** `cargo build --features ffi` (native present) → `dotnet build` →
`dotnet test` green on the TFM matrix; `dotnet format` clean; ffi-marshalling
anti-patterns satisfied; every Java method in scope translated with its tests;
error messages asserted (DoD §3).

---

## 8 · Resolved (user review, 2026-08-06)

All four confirmed:

1. **`IEquatable` → NO; `ToString` only** on `Node`/`PartitionInfo` (the E1 value-type
   precedent; they are query results, not dict keys). Non-breaking to add later.
2. **`partitionsFor("")` → forward the empty topic to the core.** Java- and
   Python-faithful: the Python sibling does **zero** topic validation — `partitions_for`
   just `_check_closed()` then forwards `topic` straight to `Consumer_partitions_for_async`
   (verified `bindings/python/consumer.py:577,431`); Java does not reject empty either. The
   binding rejects only **`null`** → `ArgumentNullException` (the one guard .NET must add for
   FFI panic-safety — ffi §B5 / CLAUDE.md §3; the core does not null-check). Do **NOT** reject
   empty/whitespace client-side. **This OVERRIDES any body text proposing an empty-topic
   `ArgumentException`.**
3. **Empty result → `Array.Empty<T>()`** (no new `EmptyReadOnlyList<T>` surface).
4. **`Node.ToString()` → mirror Java's format** (`"host:port (id: … rack: …)"`).

**Awaiting:** final user approval to run the N=14 Actor → Critic loop.
