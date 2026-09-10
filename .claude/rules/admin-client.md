# Admin client translation rules

This file consolidates design decisions specific to translating the Kafka
admin client (`org.apache.kafka.clients.admin.*`) from Java to Rust. It
supplements `CLAUDE.md` — when these rules conflict with general translation
guidance, the admin-specific rule wins inside the admin module. It mirrors
the role `consumer-threading.md` plays for the consumer.

Rules are grouped by topic. Each numbered section is a single design
decision: the rule itself, **Why** (rationale, often referencing the Java
contract), and **How to apply** (concrete guidance for Actor / Critic).

## 1. API surface: sync methods returning futures, not `async fn`

Every Java `Admin` RPC method (`createTopics`, `deleteTopics`,
`listTopics`, `describeTopics`, ...) **returns immediately** with a
`*Result` object wrapping one `KafkaFuture<T>` per key (e.g. one per topic
for `createTopics`). The network I/O happens later on the background task;
the *caller* decides whether/when to block, by awaiting the returned
`KafkaFuture`(s). Per CLAUDE.md §9.1 ("if a method is blocking in Java it
should be async in Rust"), these methods are **not** blocking in Java, so
they must **not** become `async fn` in Rust — they stay plain sync `fn`
that enqueue a `Call` (or driver invocation) onto the background task and
return a result struct holding one `KafkaFuture<T>` handle per key.

The one exception is `close()`: Java's `close(Duration timeout)` blocks
joining the background thread (CLAUDE.md §9.4: `thread.join()` → must
actually `.await` in Rust), so `close()` / `close_timeout()` are the only
`async fn`s on the `Admin` trait.

Java declares each RPC as a pair — a `default xxx(args)` forwarding to
`xxx(args, new XxxOptions())` — so each Rust RPC is a pair too, named per
CLAUDE.md §2: the no-options form owns the plain name and the options-taking
form carries the `_options` suffix. The no-options form is a **trait default
method** whose body passes `XxxOptions::default()`, exactly as Java's
`default` body passes a fresh options instance, so an implementor writes only
the `_options` form.

```rust
pub trait Admin: Send + Sync + 'static {
    fn create_topics(&self, new_topics: &[NewTopic]) -> CreateTopicsResult {
        self.create_topics_options(new_topics, CreateTopicsOptions::default())
    }
    fn create_topics_options(&self, new_topics: &[NewTopic], options: CreateTopicsOptions) -> CreateTopicsResult;

    fn delete_topics(&self, topics: TopicCollection) -> DeleteTopicsResult { /* forwards */ }
    fn delete_topics_options(&self, topics: TopicCollection, options: DeleteTopicsOptions) -> DeleteTopicsResult;

    fn list_topics(&self) -> ListTopicsResult { /* forwards */ }
    fn list_topics_options(&self, options: ListTopicsOptions) -> ListTopicsResult;

    fn describe_topics(&self, topics: TopicCollection) -> DescribeTopicsResult { /* forwards */ }
    fn describe_topics_options(&self, topics: TopicCollection, options: DescribeTopicsOptions) -> DescribeTopicsResult;
    // ... all other RPCs: sync, return a *Result holding KafkaFuture<T> per key ...

    async fn close(&self) { /* forwards to close_timeout(Long.MAX_VALUE ms) */ }
    async fn close_timeout(&self, timeout: Duration);   // blocks in Java -> must await in Rust
}
```

**Why:** The distinguishing signal vs. the consumer is *where* the blocking
happens. Consumer's `poll()` itself performs I/O and blocks (so it is
`async`). Java's `Admin.createTopics()` hands work to a background thread
and returns instantly — blocking is opt-in, at the `KafkaFuture.get()` call
site, which in Rust means "the caller awaits the future when it chooses,"
not "the method is `async`."

**How to apply:**

  - Do NOT copy the consumer's `#[async_trait]`-everything shape (§2 of
    `consumer-threading.md`). Only `close()` / `close_timeout()` are
    `async fn` on `Admin`.
  - The `#[async_trait]` attribute is acceptable on the trait solely to
    make the two `close` methods dispatchable through `Box<dyn Admin>`; it
    must NOT turn the per-RPC methods into `async fn`.

## 2. Dispatch engine: preserve Java's two-pattern split

`KafkaAdminClient` uses two distinct mechanisms, both translated per
CLAUDE.md's "preserve original architecture" rule:

1. **`Call`/retry-list** (`internals`: `Call`, plus `NodeProvider` variants
   `ControllerNodeProvider` / `LeastLoadedNodeProvider` /
   `ConstantNodeIdProvider` / `MetadataUpdateNodeIdProvider` /
   `LeastLoadedBrokerOrActiveKController`) — a simple per-request retry unit
   with `tries` / `deadline_ms`, used for most single-request RPCs. All four
   Phase-1 RPCs use this pattern.
2. **`AdminApiDriver` / `AdminApiHandler` / `AdminApiLookupStrategy`** — a
   multi-step engine for RPCs needing a coordinator or per-partition-leader
   lookup before the real request can be sent. First real use is Tier 1
   Phase 5 (`listOffsets` via `PartitionLeaderStrategy`) and Tier 2
   (group RPCs via `CoordinatorStrategy`). In Phase 1 this is a *skeleton*
   only — the four topic RPCs do not exercise it.

Both are driven by **one `tokio::spawn`ed background task per `AdminClient`
instance** (mirrors `AdminClientRunnable` — Java has one thread, one
pending-calls queue, no separate managers). This is simpler than the
consumer's `RequestManagers` list. Reuse `NetworkClient` / `KafkaClient`
(`src/network_client.rs`, `src/kafka_client.rs`) exactly as Producer's
`Sender` and Consumer's `NetworkClientDelegate` already do.

**How to apply:**

  - Translate `AdminClientRunnable.run()` / `processRequests()`
    phase-for-phase: drain new calls → handle timeouts → choose nodes for
    pending calls → maybe make a metadata call → send eligible calls →
    `client.poll(...)` → unassign calls to disconnected nodes → handle
    responses. The Java source is the contract for phase ordering.
  - Java uses `synchronized`/`newCalls` + `client.wakeup()` to hand calls
    from the app thread to the I/O thread. In Rust use a channel (mpsc) of
    "new call" messages plus the selector's wakeup primitive
    (`delegate.wakeup_handle()` → `Arc<Notify>`), NOT a raw `select!` that
    cancels the poll (§10 of `consumer-threading.md` applies: the network
    poll is not cancel-safe — wake it, do not drop it).
  - `Call`, `CallsInFlight`, `NodeProvider` and the driver types are **plain
    structs / enums driven by the background task**, NOT `#[async_trait]`
    traits. Do not let `#[async_trait]` bleed into them.
  - The per-`Call` `create_request` / `handle_response` / `handle_failure`
    hooks are Java abstract methods on an anonymous subclass. In Rust model
    them as boxed closures or a small trait object owned by the `Call`
    struct — whichever keeps the call sites readable — but keep them sync
    (they run on the background task and complete `KafkaFuture`s; they do
    not await).

## 3. `AdminMetadataManager`: the Admin-specific metadata holder

A thin `AdminMetadataManager` (bootstrap + controller/broker list refresh)
is the Admin analog of `ConsumerMetadata`. It tracks the current `Cluster`,
the controller node, readiness, and the metadata-fetch backoff/deadline
state used by `AdminClientRunnable` to decide when to issue a
`Metadata` (or KIP-919 `DescribeCluster`) refresh call.

**How to apply:** mirror `AdminMetadataManager.java` field-for-field:
`isReady()`, `nodeById(id)`, `controller()`, `requestUpdate()`,
`metadataFetchDelayMs(now)`, `transitionToUpdatePending(now)`,
`update(cluster, now)`, `updateFailed(e)`, `usingBootstrapControllers()`.
`bootstrap.controllers` support may be stubbed to `false` in Phase 1 if not
needed by the four topic RPCs — note the deferral in the phase self-review.

## 4. `KafkaFuture` reuse and required extension

Admin's `*Result` types hold `KafkaFuture<T>` (the public, already-present
`src/common/kafka_future.rs` type). But on the current branch that type is
**pre-resolved-only** — it exposes `KafkaFuture::completed(result)`,
`get`, `get_timeout`, `is_done`, and is `Clone`. It has NO completable
handle, no `all_of`, no `then_apply`, no `when_complete`.

The Admin client fundamentally needs completable-later futures: every RPC
creates empty futures, returns them to the caller synchronously (§1), and
the background task completes them when the response arrives. So the
`KafkaFuture` extension is a **hard Phase-1 prerequisite** (DoD #4), not an
optional nicety:

  - A crate-internal completable handle (Java's
    `common.internals.KafkaFutureImpl`; per CLAUDE.md `internal` package →
    `pub(crate)`) with `complete(value)` / `complete_exceptionally(err)`
    and a `future()` accessor returning the public `KafkaFuture<T>` view.
    Both share one state via `Arc`, so completing the handle resolves every
    outstanding `get()`.
  - `KafkaFuture::all_of(futures)` (Java `KafkaFuture.allOf`) — completes
    when all inputs complete; yields the first error if any failed.
  - `KafkaFuture::then_apply(f)` (Java `thenApply`) for infallible
    transforms, plus a fallible variant where the Java transform can throw
    (e.g. `CreateTopicsResult.TopicMetadataAndConfig` accessors call
    `ensureSuccess()`).
  - `KafkaFuture::when_complete(action)` (Java `whenComplete`).

**Why `Arc<Mutex<Option<Result>>>` + `Notify`, not a `oneshot`:** the future
is `Clone` and awaitable by multiple consumers and multiple times (Java
`Future.get()` is re-callable), which a `oneshot::Receiver` cannot model.
The `Notify::notified()`-before-check pattern avoids lost wakeups.

## 5. Per-key batch-result shape

Multi-key RPCs return one `KafkaFuture<T>` per key inside the `*Result`
struct, mirroring Java exactly:

  - `CreateTopicsResult`: `Map<String, KafkaFuture<TopicMetadataAndConfig>>`
    keyed by topic name; `values()`/`config()`/`topic_id()`/
    `num_partitions()`/`replication_factor()` are `then_apply` refinements;
    `all()` is `all_of`.
  - `DeleteTopicsResult` / `DescribeTopicsResult`: keyed by **either** topic
    id **or** topic name (never both — the "exactly one non-null" invariant
    is enforced in the constructor, matching Java's
    `IllegalArgumentException`).
  - `ListTopicsResult`: a single `KafkaFuture<Map<String, TopicListing>>`
    plus `names()` / `listings()` `then_apply` views.

Do NOT flatten per-key futures into one aggregate future on the public
surface; callers rely on per-key granularity.

## 6. Module layout

```
src/admin/
  mod.rs                     # Admin trait, new_admin_client() factory -> Box<dyn Admin>
  admin_client_config.rs     # AdminClientConfig (plain struct + from_properties, no AbstractConfig)
  kafka_admin_client.rs      # KafkaAdminClient: owns NetworkClient + bg task handle
  mock_admin_client.rs       # MockAdminClient: in-memory fake, immediately-ready futures
  new_topic.rs, topic_listing.rs, topic_description.rs, config.rs, config_entry.rs, ...
  options/                   # one file per *Options class, re-exported from admin::
  <rpc>_result.rs            # one file per *Result class
  internals/
    call.rs, node_provider.rs, admin_metadata_manager.rs, admin_client_runnable.rs,
    admin_api_driver.rs, admin_api_handler.rs, admin_api_lookup_strategy.rs, ...
```

`TopicCollection` lives in `org.apache.kafka.common` (not `admin`) →
`src/common/topic_collection.rs`. `Node` / `Cluster` / `Uuid` /
`TopicPartitionInfo` are `common` types (reuse or add under `src/common/`).
Wire wrappers go in `src/common/requests/`.

## 7. Wire wrappers require enum wiring

`ConcreteRequest` / `ConcreteResponse` (`src/common/requests/`) are **enums**
(one variant per API), not traits. Adding a new admin RPC's wire type means:

  1. Add a variant to `ConcreteRequest` and `ConcreteResponse`.
  2. Wire every `match` arm (`version`, `api_key`, `to_send`,
     `serialize_with_header`, `serialize`, `get_error_response`,
     `parse_request` / `parse_response`).
  3. Write the typed builder (a `RequestBuilder` impl, following
     `MetadataRequestBuilder`).

`MetadataRequest`/`MetadataResponse` are already wired and back
`listTopics`/`describeTopics`(metadata fallback). Net-new for Phase 1:
`CreateTopics`, `DeleteTopics`, `DescribeCluster` (KIP-919 metadata refresh
+ describe-cluster prerequisite of `describeTopics`-by-names), and
`DescribeTopicPartitions` (primary `describeTopics`-by-names path).

## 8. `AclOperation` / `AclPermissionType` and `from_32_bit_field` are Phase-1 deps

`TopicDescription` (Phase 1) carries `Set<AclOperation>` for
`authorizedOperations`, and `KafkaAdminClient` decodes it via
`AdminUtils.validAclOperations` which needs `Utils.from32BitField`.

**Phase-1 action:** land `common::acl::AclOperation` and
`common::acl::AclPermissionType` (the two enums only — NOT `AclBinding` /
`AclBindingFilter`, which arrive in Tier 3 Phase 1) and
`common::utils::from_32_bit_field`.

## 9. `MockAdminClient`: mirror Java, unsupported → error not panic

`MockAdminClient` is a user-facing test helper (mirrors Java's
`MockAdminClient`), with in-memory state and immediately-resolved
`KafkaFuture`s.

**The governing principle: mirror Java's `MockAdminClient` method-for-method.**
Whether a Rust mock method gets a real in-memory implementation or an
"unsupported" error is decided **solely by what the Java `MockAdminClient`
does for that same method** — not by which tier/phase the method belongs to:

  - **If Java's `MockAdminClient` implements the method with real in-memory
    logic** (against `allTopics`, `brokerConfigs`, `clientMetricsConfigs`,
    `groupConfigs`, etc.), the Rust mock MUST translate that logic faithfully,
    seeding whatever in-memory maps/fields Java uses. This applies regardless
    of tier — e.g. the topic methods (`createTopics`/`deleteTopics`/
    `listTopics`/`describeTopics`), `describeCluster`, **and** the config
    methods `describeConfigs` / `incrementalAlterConfigs` /
    `listConfigResources` are ALL fully implemented by Java's mock, so all
    must be implemented in Rust. "No in-scope test exercises it" is NOT a
    licence to stub a method Java's mock implements — implement it anyway.

  - **Only** for the methods Java's own `MockAdminClient` leaves as
    `throw new UnsupportedOperationException("Not implemented yet")` (e.g.
    `createPartitions`, and the non-empty `deleteRecords` path) may the Rust
    mock return an `Error::unsupported_version("Not implemented yet")`
    (NOT a `panic!` — CLAUDE.md §10.1). This is a faithful translation of the
    Java behavior, not a scope deferral, and every such site MUST cite the
    exact Java line that throws so the claim is verifiable. Do NOT attach a
    "Java throws unsupported" justification to a method Java actually
    implements — that is a false statement of the Java contract and will be
    flagged.

Mock-specific configuration methods (e.g. seeding brokers/topics, `set_*`)
are inherent methods on the concrete `MockAdminClient`, not on the `Admin`
trait.

## 10. Definition-of-Done adjustments for Admin

  - **DoD #10 (hot-path allocation audit): N/A.** `AdminClient` calls are
    batch/administrative, not per-record — there is no per-message hot path
    to audit. State this explicitly in each phase's self-review rather than
    skipping silently.
  - **DoD #11 (consumer trait surface check): does not apply** to Admin's
    trait, but its *spirit* does — verify per-RPC methods stay plain `fn`,
    only `close()` / `close_timeout()` are `async fn`, and no
    `#[async_trait]` bleeds into the internal `Call`/driver types (§1, §2).
  - All other DoD items apply in full: exact error-message assertions,
    byte-level wire encoding tests for the net-new request/response types,
    `@RepeatedTest`/`@ParameterizedTest` → loops with the exact Java bounds,
    and dedicated per-class test files (`NewTopicTest`, `TopicCollectionTest`,
    `*ResultTest`) translated alongside the `KafkaAdminClientTest` slices.

## 11. Bindings (C FFI / Python): branch-state caveat

The Milestone-11 plan's bindings sections assume a consumer FFI
(`src/ffi/consumer.rs`), a `consumer.py`, and an **async C API with a
`CompletionJob`/dispatcher** to mirror. On the
`dev/admin-client-implementation` branch **none of these exist**: the only
FFI is `src/ffi/producer.rs`, which is **fully synchronous** (`block_on`
under a `Mutex`), and the only Python binding is `producer.py`
(`concurrent.futures.Future` + a completion-callback closure; the batching
dispatcher lives in the hand-written C extension `_confluentkafka.c`, not
in Rust). There is no `_ProducerBase`/`AsyncProducer` split or
`_run_sync`/`_run_async` helper to reuse.

**How to apply:** the Admin FFI/Python layers must mirror the **producer**
patterns actually present on this branch (opaque `kafka_admin_AdminClient_t`
wrapping `Box<Mutex<AdminKind>> + Runtime`, sync `block_on` methods with
`out_error` out-params, copy-out callbacks for result structs; Python
`AdminClient`/`MockAdminClient` over `concurrent.futures.Future`). The
plan's "async C API"/"mirror consumer" wording is a design conflict to be
resolved with the Manager before the bindings slice — flag it loudly, do
not invent an unreviewed async dispatcher (CLAUDE.md: preserve architecture;
the plan itself warns against papering over gaps with a hack).
