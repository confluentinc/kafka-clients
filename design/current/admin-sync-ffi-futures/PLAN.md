# Admin sync FFI: return the `*Result` immediately, block only on get

Branch `fix/admin-sync-ffi-futures`, stacked on PR #201 (`feat/admin-per-key-python` @ e15df4c3).
Scope: `rust/src/ffi/admin.rs` (so the generated `target/include/confluent_kafka.h` changes too),
plus the C callers that use the sync entry points (`c/grpc_server/server.cc`, `c/tests/test_mock_admin.c`,
`c/tests/test_kafka_admin.c`). These callers must keep compiling and passing. Python is unaffected
because `python/_confluentkafka.c` only calls the `_async` entry points. The `_async` entry points
keep their current behaviour.

## 1. Problem

Java (`kafka/clients/.../admin/*Result.java`, 4.3.1):

```java
CreateTopicsResult r = admin.createTopics(topics);   // returns at once; work is queued
r.values().keySet();                                  // known at once, no wait
r.values().get("a").get();                            // waits for topic "a" only
r.all().get();                                        // waits for every topic
```

Rust FFI today: each of the 48 sync entry points goes through `admin_sync_future_op`. That function
submits the RPC, then runs `runtime.block_on(KafkaFuture::join_map_results(per_key_futures))`, and
only then writes a fully resolved, flattened `kafka_admin_*Result_t`. The C call therefore blocks
until every key has resolved. There is nothing to get, and per-key timing is lost: a key that is
already done waits for the slowest key.

## 2. Design (recommended: option A)

### 2.1 Entry points return immediately

`kafka_admin_AdminClient_<rpc>(admin, ..., out_result) -> kafka_common_Error_t*` keeps its signature.
It calls the `Admin` method on the calling thread (as today), wraps the core `*Result`'s
`KafkaFuture`s in the result handle, and returns **without waiting**.

The return value carries only failures that happen before the RPC is submitted. Java would throw
these from the call itself:

- a NULL `admin`;
- input marshalling (an unparseable base64 `Uuid`, as Java's `Uuid.fromString` throws at the caller;
  an unknown `OpType` code);
- anything the core `Admin` method itself rejects synchronously (e.g. an empty `updateFeatures` map).

Every other failure (a timeout, authorisation, a broker error code, or a request-wide failure that
Java delivers by completing every future exceptionally) now surfaces from the result's accessors,
as in Java. **This is a behaviour change for C callers:** a request-wide failure no longer comes back
from the entry point.

### 2.2 The result handle is the "future holder"; reading it is `get()`

One backing struct per result type holds the **unjoined** `KafkaFuture`s (one per key for Java's
per-key maps, one for single-future results), plus a lazily filled cache of the flattened values
(`OnceLock` per key / per future). Borrowed pointers the accessors return (`_get_key`, `_get_value`,
`_get_error`, ...) therefore stay valid until `_destroy`, as they do today.

| Java | C | Blocks? |
|---|---|---|
| `values().keySet()` (per-key maps) | `_count`, `_get_key(i)`, `_get_topic(i)` / `_get_partition(i)` etc. | **No.** Keys are fixed at submission. |
| `values().get(k).get()` | `_get_value(i)`, `_get_error(i)`, and the per-entry scalar getters | Waits for **key i's future only**. |
| `all().get()` | `_all(result) -> kafka_common_Error_t*` (owned, NULL = success); **new** where missing | Waits for every key. |
| `isDone()` | `_is_done(result, i)` / `_all_is_done(result)`; **new** | No. |
| single-future results (`listTopics`, `describeCluster`, `describeAcls`, `listGroups`, `describeFeatures`, ...) | every accessor, including `_count` | Waits for the one future. Its error is read through the Java-named method returning `kafka_common_Error_t*`, e.g. `kafka_admin_ListTopicsResult_names_to_listings`, `kafka_admin_DescribeClusterResult_nodes` / `_controller` / `_cluster_id` / `_authorized_operations`. |

Existing precedents already use this naming: `AlterConsumerGroupOffsetsResult_all` / `_partition_result`,
`RemoveMembersFromConsumerGroupResult_all` / `_member_result`, and
`DescribeUserScramCredentialsResult_all_error` / `_users` / `_description`. They become blocking with
no signature change.

The doc comment on every accessor states whether it blocks. Because cbindgen copies per-item rustdoc
only, the rule is restated in each item's own doc rather than in the module docs.

**One implementation for sync and async.** The `_async` callbacks for the non-per-key RPCs already
deliver the same handle types. They will build the handle from the same unjoined futures after
`all()` resolves, so on that path every accessor returns at once. The async behaviour does not
change, and there is one builder per type instead of two.

### 2.3 Blocking mechanics

- Accessors block through the admin handle's `tokio::runtime::Handle::block_on`, as the sync entry
  points do today. Calling one from inside a tokio runtime still panics. `#[ffi_guard]` turns that
  panic into a `LOCAL_ILLEGAL_STATE` error, exactly as it does for today's sync entry points.
- **The admin handle is destroyed while a future is still unresolved.** `kafka_admin_AdminClient_destroy`
  runs `shutdown_background()`, which aborts the background task, so that future would never
  complete. The handle will fire a `CancellationToken` on destroy, and each result handle holds a
  clone of it. An accessor waits on `select(future.get(), token.cancelled())`. If the token wins, the
  accessor returns an error instead of hanging. Neither branch uses a timer, so this works after the
  runtime has shut down. Java has no `destroy`. `close()` already resolves every pending future as
  Java's does, so this path is only reachable when `destroy` runs without `close`. It is a documented
  deviation (DoD §7), not a Java behaviour.
- Thread safety: `OnceLock::get_or_init` serialises concurrent first reads of one key. Reads of other
  keys proceed independently. `Admin` is `Send + Sync`, and the result handle is `Sync` like Java's
  futures.

### 2.4 No timed `get`

Every RPC already takes `timeout_ms`, and the core fails each future at that deadline. So no get
waits past the call's own timeout, which is the same bound Java's `get()` has with
`default.api.timeout.ms`. `_is_done` covers polling. If a timed get is wanted, it can be added later
as a `timeout_ms` parameter on `_all` without breaking anything.

## 3. Option B (not recommended): a C `KafkaFuture` handle

`kafka_common_KafkaFuture_t` with `_get` / `_is_done` / `_destroy`, returned by functions named after
every Java accessor (`kafka_admin_CreateTopicsResult_values(r, "a")`, `_config(r, "a")`, `_topic_id(r, "a")`, ...).
This mirrors Java's *syntax* more closely. But C has no generics, so the value either comes back as
an untyped `void*` the caller must cast correctly, or needs one future type per value type (about 30
types). Either way every accessor in the 42 result types is renamed, and every C caller is rewritten.
Option A gives the same Java *semantics* (immediate return, per-key waits, `all()`, `isDone()`) and
keeps the existing typed accessors.

## 4. Work breakdown

1. Shared machinery: an unjoined future holder with a lazy per-key cache and destroy cancellation;
   a new `admin_sync_submit` that replaces `admin_sync_future_op` / `admin_sync_value_op` and does
   not wait; `_destroy` remains the only owner release.
2. Convert the 48 entry points and 42 result types, grouped as in PR #201's phases: topics; cluster
   and configs; log dirs and partitions; offsets and groups; ACLs, quotas and SCRAM; tokens,
   features, transactions and producers.
3. Add `_all` / `_is_done` and the Java-named error getters for single-future results where they are
   missing.
4. Rust FFI unit tests: entry points return before resolution, keys are readable at once, a per-key
   accessor waits for its own key only, `all()` returns the first error, `destroy` before resolution
   gives an error rather than a hang, and the async path is unchanged. Driven by an unresolved
   `KafkaFutureImpl` and the mock.
5. Update `c/tests/test_mock_admin.c`, `c/tests/test_kafka_admin.c` and `c/grpc_server/server.cc`
   where they read a request-wide failure from the entry point's return value. They read it from
   `_all` / the per-key error instead.
6. Gate: `cargo test --features ffi --lib`, `cargo xtask lint`, `format-check`, the C tests, and
   `make verify` once before the push.

## 5. Decisions (approved 2026-10-13)

1. Option A: the lazy result handle with blocking accessors.
2. Keep today's accessor names; add Java-named `_all` / `_is_done`, and Java-named error getters for
   single-future results.
3. Request-wide errors move off the entry point's return value, as in Java.
4. A follow-up branch stacked on #201.
