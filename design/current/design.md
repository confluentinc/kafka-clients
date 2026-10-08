# Current Design

This document describes the architecture of the Confluent Kafka Rust client as
it stands. The dated, step-by-step record of how it got here lives in
`design/history/`. What is done and what is not is tracked in
[status.md](status.md).

**Last verified against the tree:** 2026-09-27
**Coverage:** Producer (including idempotence and transactions), Consumer
(KIP-848) and AdminClient are all translated. The C FFI and the Python bindings
cover all three.

> **Paths:** the repository has three top-level directories, `rust/`, `python/`
> and `c/` (see [structure.md](structure.md#top-level)). Paths in this document
> such as `src/…`, `tests/…`, `generator/…`, `xtask/…`,
> `multilanguage-test-server/` and `consumer-perf/` are relative to `rust/`.
> Run `cargo` commands from there. Binding paths are given from the repository
> root. Line-number citations are as of the "last verified" date above.

---

## Overview

A Rust Kafka client translated from the Java Kafka client (Apache Kafka 4.3.1).
It keeps the Java architecture and logical structure and adapts them to Rust
idioms. All I/O is async and runs on Tokio.

**Crate stats:** 777 source files under `src/`, ~334,000 lines.
- Per area (files / lines): `common` 466 / 98.6k, `consumer` 82 / 81.5k,
  `producer` 31 / 53.3k, `admin` 161 / 44.5k, `ffi` 6 / 38.6k, plus 23
  crate-root files.
- **4144 lib tests** (`cargo test --lib -- --list`), or 4361 with
  `--features ffi`.
- 109 `#[tokio::test]`s across 27 feature-gated integration files. The
  multilanguage test macros generate further cases.

> **Diagrams:** every image lives in `img/`. Each `![...](img/*.svg)` below is
> rendered from the `.d2` file of the same name beside it. The `.d2` is the
> source of truth, so never hand-edit an `.svg`.

![Component & ownership structure](img/component-ownership.svg)

*Takeaway: there are three independent clients. Each owns exactly one
background task and one `NetworkClient`. The app side talks to its background
task through channels plus a wakeup `Notify`, never by sharing a lock.*

---

## Layered Architecture

![Layered architecture](img/network-stack.svg)

*Takeaway: each layer depends only on the one below it, so a change to the
transport never reaches the request/response types. The `generator/` crate is
the one input from outside the stack; it emits `Message` impls into the
protocol layer at build time.*

> **Path note:** the client layer has no `src/clients/` directory. CLAUDE.md
> §2 forbids `clients` in a folder name, so `kafka_client.rs`,
> `network_client.rs`, `metadata.rs`, `api_versions.rs`,
> `in_flight_requests.rs`, `cluster_connection_states.rs` and the rest live
> flat at the crate root. They are private modules (`mod`, `src/lib.rs:28-57`),
> and their types are re-exported only as `pub(crate) use`. Any
> `src/clients/...` path elsewhere in this directory should be read as
> `src/...`.

Above this stack sit the three client modules, `src/producer/`,
`src/consumer/` and `src/admin/`. Each has its own background task (see
[Client Modules](#client-modules) below).

---

## Network Stack

### TransportLayer trait (`common/network/transport_layer.rs`)

The core abstraction that bridges Java NIO and Tokio async I/O. It is an
object-safe trait (`transport_layer.rs:99`), used as `Box<dyn TransportLayer>`
for runtime dispatch.

**Key methods:** `read`, `try_read`, `try_read_append`, `write`,
`write_vectored`, `try_write_vectored`, `handshake`, `finish_connect`,
`disconnect`, `close`

**Implementations:**
- `PlaintextTransportLayer`: wraps `tokio::net::TcpStream`.
- `SslTransportLayer`: wraps `tokio_rustls::TlsStream<TcpStream>`. Its state
  machine runs Handshaking -> Ready -> Closed.

### Selector (`common/network/selector.rs`)

Multiplexes I/O across multiple `KafkaChannel` instances and implements the
`Selectable` trait. The struct is at `selector.rs:194`.

**NIO to Tokio mapping:**
- Java `SelectionKey`: eliminated. Channels are keyed by `Arc<str>` ID in an
  `FxHashMap`.
- Java `selectedKeys()`: replaced by a shared **fired queue**
  (`ready_queue: Arc<ReadyQueue>`, `selector.rs:262`). Per-channel
  `ChannelWaker`s push into it when the tokio reactor fires them.
- Wakeup: a `tokio::sync::Notify`.
- Idle timeout: `IdleExpiryManager`.

**Poll cycle (readiness-driven, not a full sweep; `async fn poll`,
`selector.rs:1301`):**
1. The WAIT future drains the fired queue. It translates `(token, is_write)`
   pairs back to channel ids through `token_to_id` (`selector.rs:272`).
2. Pass 1 processes only the *ready* ids, plus channels with buffered reads
   and channels that connected immediately. It does not issue a `recv` syscall
   on every registered channel.
3. A readable channel is drained in a **tight `try_read` loop** until the
   socket is empty (`selector.rs:791-806`). Without the loop it would read one
   chunk per async round-trip.
4. `interest_dirty` (`selector.rs:281`) forces a re-arm for any channel whose
   interest may have been switched on since it was last armed. A missed arm is
   a data-stall bug, so the rule is "when in doubt, mark dirty".
5. The poll returns completed sends and receives and the disconnected
   channels.

![Selector poll cycle](img/selector-poll-cycle.svg)

*Takeaway: the poll touches only the channels the tokio reactor fired. A wakeup
returns the WAIT future without losing state: the scratch sets are struct
fields, not future-locals, so a cancelled WAIT costs nothing.*

`FxHashMap`/`FxHashSet` (not SipHash) are used for the per-poll channel maps.
The keys are internal connection ids, and hashing them showed up as ~10% of CPU
in a TLS profile (`selector.rs:203-209`). The raw flat profile behind that
measurement is no longer kept here. The `selector.rs` comment records the
figure it produced.

> The `Selector` doc comment's own "Key adaptations" summary
> (`selector.rs:188-192`) still says *"NIO `Selector.select()` → sequential
> try_read/try_write + tokio timeout"* and *"Metrics deferred (no-op stubs)"*.
> That describes a sweep-every-channel design that no longer exists. The
> struct's fields and the poll body implement the readiness-driven design
> described above. The comment is stale, not the code.

### KafkaChannel (`common/network/kafka_channel.rs`)

A per-connection state machine that combines transport, authentication and I/O
buffers.

**Contains:**
- `Box<dyn TransportLayer>`: the underlying transport.
- `Box<dyn Authenticator>`: the authentication handler.
- `Option<NetworkSend>` / `Option<NetworkReceive>`: the I/O buffers.
- `ChannelState` (`channel_state.rs:96`): a struct holding the `State`, an
  optional error and the remote address.
  - The lifecycle enum it carries is `State` (`channel_state.rs:49`):
    `NotConnected`, `Authenticate`, `Ready`, `Expired`, `FailedSend`,
    `AuthenticationFailed`, `LocalClose`.
  - The file also exposes the `NOT_CONNECTED` / `AUTHENTICATE` / `READY` /
    `EXPIRED` consts that Java has as static fields
    (`channel_state.rs:120-128`).
- `ChannelMuteState`: read muting for backpressure.

### ChannelBuilder trait (`common/network/channel_builder.rs`)

A factory that creates `KafkaChannel` instances from a `TcpStream`.

**Implementations:**
- `PlaintextChannelBuilder`: creates plaintext channels.
- `SslChannelBuilder`: creates SSL/TLS channels with rustls.
- `SaslChannelBuilder`: creates SASL-authenticated channels (SASL_PLAINTEXT,
  SASL_SSL).

**Factory:**
- `ChannelBuilders::client_channel_builder()` (`channel_builders.rs:68`)
  dispatches on `SecurityProtocol` to create the right builder. All three
  clients call it (see [Security](#security)).

### Network I/O Primitives

- `Send` trait (`send.rs:35`): async send. Its implementation is
  `ByteBufferSend`. Java's name is kept, so inside the crate it shadows
  `std::marker::Send` only where it is imported by name.
- `Receive` trait (`receive.rs:34`): async receive. Its implementation is
  `NetworkReceive`.
- `NetworkSend` (`network_send.rs`): wraps a `Send` with a destination ID.
- `NetworkReceive` (`network_receive.rs`): a size-delimited receive (4-byte
  header plus payload).

---

## Protocol Framework

### Serialization Traits (`common/protocol/`)

- `Readable`: deserializes Kafka types from byte streams.
- `Writable`: serializes Kafka types to byte streams.
- `ByteBufferAccessor`: a concrete impl of both traits over `&mut [u8]`.
- `Message`: versioned serialization, with `size()`, `add_size()`, `write()`
  and `read()`.
- `ApiMessage`: extends Message with `api_key()`.

Serialization takes two passes: calculate the size first, then serialize.

### Wire Format

- Big-endian for all multi-byte integers
- Varint/varlong: Protocol Buffers unsigned and zig-zag signed encoding
- Strings: i16 length prefix (standard) or varint(len+1) (flexible)
- Bytes: i32 length prefix (standard) or varint(len+1) (flexible)
- Arrays: i32 length prefix (standard) or varint(len+1) (flexible)
- Tagged fields at end of struct in flexible versions

Flexibility is decided **per field**, not per message. The generator calls
`field_flexible_versions(field, msg_flex)`, so overrides such as
`RequestHeader`'s `ClientId` (`"none"`, always length-prefixed) encode
correctly (CLAUDE.md §2).

---

## Request/Response Framework (`common/requests/`)

The whole module is crate-private (`pub(crate) mod requests`,
`common/mod.rs:59`), because Java's `org.apache.kafka.common.requests` is not
public API.

### RequestBuilder trait (`abstract_request.rs`)

Constructs requests at a specific API version. It replaces Java's
`AbstractRequest.Builder`.

Methods: `api_key()`, `oldest_allowed_version()`, `latest_allowed_version()`,
`build()`, `build_version()` (`abstract_request.rs:145-169`).

### Dispatch Enums

`AbstractRequest` (`abstract_request.rs:182`) and `ConcreteResponse`
(`abstract_response.rs:122`) are **enums** with one variant per API. Adding an
RPC means wiring every `match` arm: `version`, `api_key`, `to_send`,
`serialize_with_header`, `serialize`, `get_error_response` and `parse_*`.

**52 variants** are wired today in each enum:

| Group | Variants |
|-------|----------|
| Core | ApiVersions, Metadata, Produce, Fetch |
| Security | SaslHandshake, SaslAuthenticate |
| Coordination | FindCoordinator, ConsumerGroupHeartbeat, LeaveGroup |
| Groups | ListGroups, DescribeGroups, ConsumerGroupDescribe, DeleteGroups |
| Offsets | ListOffsets, OffsetsForLeaderEpoch, OffsetCommit, OffsetFetch, OffsetDelete |
| Topics | CreateTopics, DeleteTopics, CreatePartitions, DeleteRecords |
| Configs | DescribeConfigs, IncrementalAlterConfigs, ListConfigResources |
| Cluster | DescribeCluster, DescribeLogDirs, AlterReplicaLogDirs, ElectLeaders, AlterPartitionReassignments, ListPartitionReassignments, UpdateFeatures |
| ACLs / quotas | DescribeAcls, CreateAcls, DeleteAcls, DescribeClientQuotas, AlterClientQuotas |
| Credentials | DescribeUserScramCredentials, AlterUserScramCredentials, Create/Renew/Expire/DescribeDelegationToken |
| Transactions | InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit, WriteTxnMarkers, DescribeProducers, DescribeTransactions, ListTransactions |

The variant list is exactly the set of RPCs that can be sent. Anything missing
from it cannot be sent, however complete its generated message type is.
`ListOffsets` is shared: the consumer's wrapper also backs the admin
`list_offsets`, so no second wire type was added.

### Supporting Types

- `RequestHeader` / `ResponseHeader`: Kafka frame headers.
- `SendBuilder`: constructs a `NetworkSend` from header plus body (zero-copy).

---

## Client Layer (`src/*.rs`)

### KafkaClient trait (`kafka_client.rs`)

Defines the client interface (`kafka_client.rs:39`): `is_ready`, `ready`,
`send`, `poll`, `disconnect`, `least_loaded_node`, `in_flight_request_count`,
`wakeup_handle`, `wakeup_notify` and more. `wakeup_handle()` (`:233`) returns
the selector's `Arc<Notify>`. Every background task is interrupted through it,
so an in-flight poll is never cancelled.

### NetworkClient<S: Selectable, H: HostResolver> (`network_client.rs`)

The core async client implementation (`network_client.rs:88`). It is generic
over `Selectable`, so tests can inject a `MockSelector`, and over
`HostResolver`, whose default is `default_host_resolver.rs`. Its module is
private (`lib.rs:54`), and the type is re-exported only as `pub(crate)`.

**Manages:**
- `InFlightRequests`: the per-destination request queue.
- `ClusterConnectionStates<H>`: the connection state machine with exponential
  backoff.
- `ApiVersions`: the cached per-node API version registry.
- Metadata updates and recovery.

### Connection State Machine

![Connection state machine](img/connection-lifecycle.svg)

*Takeaway: a node is marked `Connecting` before its socket exists, so dropping
the network-poll future strands it. That is why no background task races its
poll in a `select!`.*

`ConnectionState` (`src/connection_state.rs:30-35`) has five states. The
transitions are the `ClusterConnectionStates` methods of the same names, and
each one carries the exponential-backoff bookkeeping:
- `connecting` (`cluster_connection_states.rs:175`)
- `checking_api_versions` (`:286`)
- `ready` (`:298`)
- `authentication_failed` (`:321`)
- `disconnected` (`:229`)

`initiate_connect` marks a node `Connecting` (`network_client.rs:498`)
**before** it awaits `current_address()` (`:499`) and `selector.connect()`. So
the poll that drives it is *not* cancellation-safe; see the note under
[Consumer](#consumer-srcconsumer).

### Request/Response Flow

1. `NetworkClient::send(ClientRequest)` queues the request.
2. `RequestBuilder::build_version()` produces an `AbstractRequest`.
3. `Message::write()` produces the bytes. They are wrapped in `SendBuilder`,
   then `ByteBufferSend`, then `NetworkSend`.
4. `Selector::poll()` drives the I/O through `KafkaChannel` and
   `TransportLayer`.
5. The response travels `TransportLayer::read()` -> `NetworkReceive` ->
   `Message::read()` -> `ClientResponse`.
6. The `RequestCompletionHandler` callback is invoked.

### Supporting Types

- `ClientRequest` / `ClientResponse`: request/response wrappers with metadata.
- `RequestCompletionHandler`: `Box<dyn FnOnce(&mut ClientResponse) + Send>`
  (`lib.rs:74`).
- `Metadata`: a thread-safe metadata cache with epoch tracking.
- `MetadataSnapshot`: an immutable cluster metadata snapshot.
- `NodeApiVersions`: per-node API version tracking.

---

## Client Modules

### Producer (`src/producer/`)

`Producer<K, V>` (`producer/producer.rs:45`) declares twelve methods:
- `init_transactions`, `begin_transaction`, `send_offsets_to_transaction`,
  `commit_transaction`, `abort_transaction`
- `send`, `send_with_callback`, `flush`, `partitions_for`, `metrics`
- `close`, `close_with_timeout`

`begin_transaction` and `metrics` are sync. The others return
`impl Future<Output = …> + Send`. `KafkaProducer<K, V>` (struct at
`kafka_producer.rs:109`, impl at `:2491`) and `MockProducer<K, V>` (struct
`mock_producer.rs:183`, impl `:999`) implement it.

Unlike `Consumer` and `Admin`, this trait does **not** use `#[async_trait]`,
because `send` is on the hot path and a boxed future per record is what
CLAUDE.md §13 rules out. The cost is that `Producer` is not dyn-compatible.
CLAUDE.md §3 asks for a dyn companion in that case, and `DynProducer`
(`dyn_producer.rs:79`) is it:
- It is sealed.
- It is blanket-implemented for every `Producer` (`:199`).
- It boxes the futures.
- `impl Producer for dyn DynProducer` (`:300`) lets a `Box<dyn DynProducer>`
  be used wherever a `Producer` is expected.

Only code that asks for dynamic dispatch pays the allocation.

![Producer send path](img/producer-send-path.svg)

*Takeaway: `send()` never touches the network. It appends into the
`RecordAccumulator` and returns a future. The `Sender` task does all the I/O
and completes the future later.*

The app-side path is:
1. `do_send` (`kafka_producer.rs:1670`)
2. `wait_on_metadata(max_block_ms)`
3. serialize key and value
4. `compute_partition` (`:2275`)
5. `ensure_valid_record_size` (`:2212`)
6. `accumulator.append(...)` (`:1859`)

How the partition is chosen:
- An explicit partition wins.
- Otherwise a present, non-ignored key is hashed through
  `BuiltInPartitioner::partition_for_key` by the configured `KeyHasher`. The
  default is CRC-32, for co-partitioning parity with librdkafka's
  `consistent_random`. Murmur2 gives exact Java parity. `partitioner.type`
  selects between them.
- Failing both, `UNKNOWN_PARTITION` defers to the sticky partitioner.

The Sender is woken only when the append filled a batch or started a new one
(`kafka_producer.rs:1948-1955`), matching Java.

> The rationale for the CRC-32 default used to be written up in
> `design/current/partitioner.md`. **That file no longer exists**: it was
> deleted in commit `eff912f5` and survives in git history. The code still
> cites it at `producer_config.rs:213`, as do
> `python/test/performance/partitioner.py:10` and
> `producer_performance_test.py:514`. Those references now dangle; see
> [status.md](status.md).

The `Sender<C>` task (`internals/sender.rs:416`) loops `run()` (`:915`) →
`run_once()` (`:1118`). Each iteration:
1. For a transactional producer only, `run_transaction_phase()` (`:1347`). It
   drives `maybe_send_and_poll_transactional_request` (`:1519`), and it may
   send one transactional request and return early.
2. `send_producer_data(now)` (`:1918`) drains the accumulator and builds one
   `ProduceRequest` per ready node in `send_produce_requests` (`:2688`).
3. `poll_and_dispatch` runs `client.poll(...)` and hands the responses to
   `handle_client_responses` (`:1212`).

On shutdown the Sender keeps looping while the accumulator has undrained
batches or requests are in flight. A transactional producer with an open
transaction aborts it first, and a forced close aborts the incomplete batches.

Rust closures cannot capture `&mut self`. So Java's `RequestCompletionHandler` →
`handleProduceResponse` callback becomes code that processes the responses
`poll()` returns. It then applies the per-batch outcome afterwards, as a
`BatchAction` of `Done`, `Reenqueue` or `SplitAndReenqueue` (`sender.rs:108`).

**Idempotence and transactions.** `TransactionManager`
(`internals/transaction_manager.rs`) is shared three ways — by `KafkaProducer`,
`Sender` and `RecordAccumulator` (`record_accumulator.rs:314`) — as
`Arc<Mutex<TransactionManager>>` (`sender.rs:462`).

Not everything sits behind that lock:
- The fields Java confines to the Sender thread stay plain fields on the
  `Sender`. These are the coordinator nodes (`sender.rs:531`) and the
  in-flight correlation id (`:540`).
- The pending transactional request queue has its own
  `Arc<Mutex<PendingRequests>>` (`sender.rs:523`). `KafkaProducer` enqueues
  into it.

Every path takes the locks in the same order: partition deque →
`pending_requests` → manager. No guard is held across an `.await`.

An invalid state transition either returns an error or poisons the manager
into `FATAL_ERROR`. Java decides which by inspecting the current thread. Here
the decision is carried by an explicit `Caller { App, Sender }` argument
(`transaction_manager.rs:362`). `producer-transactions.md` holds the full set
of rules.

![Producer transaction locks](img/producer-txn-locks.svg)

*Takeaway: one lock, shared three ways, always taken after the deque and the
pending-request queue. The fields Java confines to the Sender thread never
enter it.*

### Consumer (`src/consumer/`)

`Consumer<K, V>` is a single `#[async_trait]` trait (`src/consumer/mod.rs:133`)
with 47 methods. The 38 async ones are the ones that block in Java. The nine
sync ones are `assignment`, `subscription`, `paused`, `group_metadata`,
`client_id`, `current_lag`, `metrics`, `wakeup` and `handle`.

`KafkaConsumer::new(config, key_deserializer, value_deserializer)`
(`kafka_consumer.rs`) is the factory, and it returns
`Box<dyn Consumer<K, V>>`. There is no free `new_consumer` function. The
implementations are:
- `AsyncKafkaConsumer<K, V>`, which is crate-private (`mod.rs:44`) and
  reachable only through that box.
- `MockConsumer<K, V>`.

A compile-time guard on this shape lives at
`tests/consumer/trait_surface_check.rs`.

Only the KIP-848 group protocol is translated. The factory rejects
`group.protocol=classic` with an `unsupported_version` error rather than
silently degrading. The client-side assignors (`RangeAssignor`,
`StickyAssignor`, …) have no Rust counterpart because KIP-848 assigns
server-side (`consumer-threading.md` §20). Two pieces of the classic protocol
*are* present, because the admin group-describe path needs them:
`ConsumerProtocol` (`internals/consumer_protocol.rs`) and the `Assignment` /
`Subscription` data holders (`consumer_partition_assignor.rs`).

![Consumer background-task loop](img/consumer-bg-loop.svg)

*Takeaway: one `run_once()` iteration walks the phases of Java's `runOnce()`
in order. Phase 4, the network poll, runs to completion; it is never raced in
a `select!`.*

`run_once` is at `consumer_network_thread.rs:343`. Its phase labels are the
ones the code itself uses, and they follow Java's
`ConsumerNetworkThread.runOnce()`.

There is no phase 3. Java visits the membership manager inside its `entries()`
walk. Rust's `entries()` (`request_managers.rs:211`) skips the three
`Arc`-shared managers (coordinator, commit, membership), because a
`&mut dyn RequestManager` cannot be produced from a shared `Arc`. Their side
effects are supplied explicitly instead, at phases 2, 2.4, 2.5 and 2.6
(`consumer_network_thread.rs:374-564`).

The order matches Java's: coordinator → commit → heartbeat → *reconcile* →
offsets → topic_metadata → fetch. Because `reconcile` sits in the middle,
`offsets.poll()` and `fetch.poll()` see the post-reconcile subscription state
within the same iteration.

**Phase 4 is a plain `.await`, deliberately.** Suppose a `tokio::select!`
raced the poll against a wakeup token and an application-event `Notify`:
1. The losing poll is cancelled in the middle of `initiate_connect`.
2. That leaves a node marked `Connecting` with no socket.
3. The node stays stuck until the ~10 s connection-setup timeout, which
   stalls group joins intermittently and indefinitely.

So wakeups go to the selector's own `Arc<Notify>`
(`KafkaClient::wakeup_handle()`), and the poll returns at a safe boundary
instead of being dropped. `notify_one()` stores a permit, so a poke that lands
between the top-of-loop drain and the await still returns the poll immediately.
The phase runs from `consumer_network_thread.rs:565`, with the poll itself at
`:602`.

> The root-cause analysis behind that rule was written up in
> `design/current/consumer-join-stall-rootcause.md`. **That file no longer
> exists.** Its content is in git history, and the fix is commit `9966df19`.
> Its conclusion is preserved in `consumer-threading.md` §10: *the network
> poll performs side-effecting connection setup before an `await`, so it must
> never be a `select!` loser*. That rule file still cites the deleted
> document, as do several source comments; [status.md](status.md) lists them.

> The module docstring at `consumer_network_thread.rs:42-45` still describes
> phase 4 as *"wrapped in `tokio::select!` against the current wakeup token
> and the shutdown signal"*. That is the shape that caused the stall. The body
> at line 602 is authoritative.

![Consumer event model](img/consumer-event-model.svg)

*Takeaway: `ConsumerRebalanceListener` and `OffsetCommitCallback` run inline
on the caller's task. The background loop keeps spinning during the callback
and gates only the membership state transition on the ack.*

The background-to-app half of the handshake is `BackgroundEvent`
(`internals/events/background_event.rs`). It has three variants, mirroring the
Kafka 4.3.1 event classes:
- `Error { error }`
- `PartitionsRemoved { method_name, partitions, ack }` (`:53`), which carries
  revoked and lost partitions
- `PartitionsAssigned { assigned_partitions, added_partitions, ack }` (`:76`)

The app side drains them in `process_background_events`
(`async_kafka_consumer.rs:3287`), invokes the listener, sends the ack and pokes
the bg `Notify`. The bg side stores the receiver and `try_recv`s it on each
iteration, in `ConsumerMembershipManager::drive_pending_release`
(`consumer_membership_manager.rs:1289`) and `drive_pending_reconcile`
(`:1369`). As a result, a listener that reenters the consumer through a
`ConsumerHandle` (`async_kafka_consumer.rs:187`) cannot deadlock.

If the bg loop blocked on that ack instead, any consumer call the listener
makes would deadlock:
- The listener runs on the app task.
- That task's `poll()` cannot return until the listener does.
- So the loop that has to service the reentrant call must stay free.

Driving the ack with `try_recv` across iterations keeps heartbeats and fetches
flowing while a listener runs.

The assign side has one more step, which comes from Kafka 4.3.1 (KAFKA-20106).
The app task answers `PartitionsAssigned` with
`ApplicationEvent::ApplyAssignment` (`application_event.rs:254`) and awaits
it:
1. The bg task installs the new assignment.
2. The app task then runs `on_partitions_assigned`.
3. The ack finally releases the reconcile.

So `SubscriptionState` is still mutated on the bg task, but only at a point the
app task chooses, inside `poll()`.

![Consumer assign handshake](img/consumer-assign-handshake.svg)

*Takeaway: the assignment is applied on the bg task, but only when the app
task asks for it from inside `poll()`. Fetching for the added partitions starts
only after the listener has returned.*

> `background_event.rs:58` and `:82` still say the bg task "awaits" the ack.
> That describes the earlier blocking shape. The membership manager code is
> authoritative.

![Consumer receive path](img/consumer-receive-path.svg)

*Takeaway: one buffer owns the fetch payload, and everything downstream
borrows slices from it. The only per-record deep copy is the one the user's
`Deserializer` chooses to make.*

Receive-path zero-copy holds:
- `CompletedFetch` owns the fetched bytes once.
- The buffer is *moved*, never cloned, into a lazy `BatchCursor`
  (`completed_fetch.rs:250`).
- Compressed batches are decompressed once per batch.
- The topic name is one `Arc<str>` per fetch (`topic_arc`, `:180`), cloned
  per record.
- `Deserializer<T>` is a sync trait taking `&[u8]`
  (`common/serialization/deserializer.rs:53`).

Headers are the one deliberate owned copy per record, matching Java's
allocation behaviour.

### Admin (`src/admin/`)

The `Admin` trait (`src/admin/mod.rs:242`) has **94 sync `fn` RPC methods
and exactly two `async fn`s (`close` / `close_with_timeout`)**. That is the
shape `.claude/rules/admin-client.md` §1 requires. The trait covers 44 RPCs.

Only 45 methods are required: the 44 options-taking `*_with_options` forms
plus `close_with_timeout`. The other 51 are **trait default methods**. They
translate Java's `default xxx(args)` bodies, which forward to
`xxx(args, new XxxOptions())`.

The naming follows CLAUDE.md §2:
- Where Java declares a no-options overload, it keeps the plain name. So
  `create_topics(&[NewTopic])` forwards to
  `create_topics_with_options(&[NewTopic], CreateTopicsOptions::default())`.
- Where Java has several non-options overloads, each is discriminated by its
  parameter: `describe_topics_with_topic_names` / `describe_topics_with_topics`
  and `list_consumer_group_offsets_with_group_id` /
  `list_consumer_group_offsets_with_group_specs`. Each has its own `…_options`
  partner.
- `close()` is the default that forwards to the required
  `close_with_timeout(Duration)`.

The factories:
- `AdminClient::create(config)` (`admin_client.rs:56`) returns the real client
  as `Box<dyn Admin>`. It calls the crate-private `KafkaAdminClient::new`
  (`kafka_admin_client.rs:284`), which wraps construction errors as "Failed to
  create new KafkaAdminClient".
- `MockAdminClient::create(num_brokers)` (`mock_admin_client.rs:193`)
  constructs the fake.

Both implement `Admin` (`kafka_admin_client.rs:3030`,
`mock_admin_client.rs:654`) and can be boxed as `Box<dyn Admin>`.

**Why sync methods, when the consumer's are async.** The deciding factor is
*where* the blocking happens in Java:
- `Consumer.poll()` itself performs I/O and blocks, so it becomes `async`.
- `Admin.createTopics()` hands the work to a background thread and returns
  immediately with a `*Result` wrapping one `KafkaFuture<T>` per key.
  Blocking is opt-in, at `KafkaFuture.get()`. So the per-RPC methods stay
  plain `fn`s that enqueue a `Call` and return a result struct, and the caller
  awaits the futures it cares about.
- `close()` is `async` because Java's `close(Duration)` joins the background
  thread.

`#[async_trait]` sits on the trait (`mod.rs:238`) only so that `close()` and
`close_with_timeout()` can be dispatched through `Box<dyn Admin>`. It does not
reach the internal `Call` / driver types, which are plain structs driven by
the background task.

`KafkaFuture<T>` (`common/kafka_future.rs:64`) is the type that makes this
work:
- The public view is `Clone` and can be awaited more than once. Its methods
  are `get`, `get_with_timeout`, `is_done`, `all_of`, `then_apply`,
  `then_apply_try`, `join_map` and `join_map_results`.
- The completable handle is the crate-internal `KafkaFutureImpl<T>`
  (`common/internals/kafka_future_impl.rs:150`), with `complete`,
  `complete_with_error`, `when_complete` and `future()`.
- It is built on shared `Mutex` state plus a `Notify`, not on a `oneshot`,
  because Java's `Future.get()` can be called again by several callers. The
  waiter creates its `notified()` future before checking the state, so a
  completion that lands between the check and the await is not lost.

![Admin dispatch patterns](img/admin-dispatch.svg)

*Takeaway: both dispatch patterns share one background task and one
`NetworkClient`. They differ only in whether the target node is known up front
(`Call`) or must be looked up first (`AdminApiDriver`).*

Both patterns run on one `tokio::spawn`ed `AdminClientRunnable<C: KafkaClient>`
(`admin/internals/admin_client_runnable.rs:84`), which receives new `Call`s
over an mpsc channel. It mirrors the producer's `Sender<C>` and walks the
phases of Java's `AdminClientRunnable.run()`:
1. drain new calls
2. time out expired calls
3. choose nodes
4. maybe issue a metadata call
5. send the eligible calls
6. `client.poll(...)`
7. unassign calls whose node disconnected
8. handle the responses

`AdminMetadataManager` is the admin analog of `ConsumerMetadata`. It tracks
the cluster, the controller and the metadata backoff/deadline state.

Which pattern an RPC uses is a property of the RPC:

- **`Call` / `NodeProvider`**: one request to a node that is known up front.
  Topic CRUD, `create_partitions`, cluster and config administration, log-dir
  administration, elections and reassignments all take this path.
- **`AdminApiDriver` / `AdminApiHandler` / `AdminApiLookupStrategy`**: a
  two-stage engine (lookup, then fulfillment) for RPCs whose target must be
  resolved first.
  - `src/admin/internals/` holds 16 `*_handler.rs` files.
  - It has four concrete strategies: `PartitionLeaderStrategy` (plus
    `PartitionLeaderCache`) for `delete_records` / `list_offsets`,
    `CoordinatorStrategy` for the group and transaction RPCs, and
    `AllBrokersStrategy` / `StaticBrokerStrategy`.
  - The driver batches fulfillment requests per node. On a stale-leader or
    disconnect error it unmaps the affected keys and reruns the lookup,
    through the `Call::set_maybe_retry_fn` / `MaybeRetryOutcome` hook
    (`call.rs:276`, `:61`).

Two behaviours are worth knowing because they are easy to get wrong:
- `describe_configs` and `incremental_alter_configs` route **per resource
  type**. Broker and broker-logger resources go to that specific broker; topic
  and other resources go to the controller or the least-loaded node.
- Quota-exceeded retries carry `ThrottlingQuotaExceeded` / `throttle_time_ms`
  forward and complete again on the final timeout, matching Java's
  `maybeCompleteQuotaExceededException` (`maybe_complete_quota_exceeded`,
  `kafka_admin_client.rs:1100`).

A note on names: the Rust `NodeProvider` enum (`admin/internals/call.rs:86`)
has the variants `Controller`, `LeastLoaded`,
`LeastLoadedBrokerOrActiveKController`, `MetadataUpdate` and
`ConstantNodeId(i32)`. Prose in `status.md` sometimes uses the Java class
names (`ControllerNodeProvider`, `LeastLoadedNodeProvider`,
`ConstantNodeIdProvider`, `MetadataUpdateNodeIdProvider`). They refer to these
variants.

`MockAdminClient` is the user-facing in-memory fake, and it mirrors Java's
method for method:
- A method that Java's mock implements against its in-memory maps is
  implemented here too.
- A method that Java leaves as
  `UnsupportedOperationException("Not implemented yet")` fails with
  `unsupported_version("Not implemented yet")` on the returned future, not
  with a panic, since a public API must not panic (CLAUDE.md §12.1).

Java ships the mock in its test jar. Here it stays public through
`xtask/public-audience-allowlist.txt`.

The admin client builds its channel builder from `security.protocol` like the
other two clients (`kafka_admin_client.rs:325`), so SSL and SASL/PLAIN work.
The one visible gap is `bootstrap.controllers` (KIP-919), which is passed to
`AdminMetadataManager` as `false` (`kafka_admin_client.rs:313`); the
`using_bootstrap_controllers()` branches exist but are never taken.

Beyond the 44 RPCs there is no `ForwardingAdmin`, and there are no
Streams-group, share-group or KRaft raft-voter RPCs (`add_raft_voter`,
`remove_raft_voter`, `describe_metadata_quorum`, `unregister_broker`). Java's
deprecated `listConsumerGroups` and `listClientMetricsResources` are
deliberately not translated (CLAUDE.md §3).

---

## Security

### SecurityProtocol (`common/security/auth/security_protocol.rs`)

```
PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
```

### SSL/TLS

- `SslFactory` (`common/security/ssl/ssl_factory.rs`): the TLS configuration
  factory, built on rustls.
- `SslConfigs` (`common/config/ssl_configs.rs`): certificate, key and
  truststore configuration.
- `SslTransportLayer`: async TLS transport through tokio-rustls.
- `SslChannelBuilder`: the TLS channel factory.
- TLS 1.2 and 1.3 are supported.

### SASL (PLAIN mechanism only)

- `SaslConfigs` (`common/config/sasl_configs.rs`): mechanism and JAAS
  configuration.
- `SaslHandshakeRequest/Response`: mechanism negotiation.
- `SaslAuthenticateRequest/Response`: the authentication exchange.
- `SaslClientAuthenticator`
  (`common/security/authenticator/sasl_client_authenticator.rs`): the state
  machine that runs handshake → initial token → intermediate → complete.
  - It implements **PLAIN (RFC 4616) only**
    (`sasl_client_authenticator.rs:28-29`).
  - The state machine is shaped for challenge-response mechanisms, but SCRAM,
    OAUTHBEARER and GSSAPI are not implemented.

All three clients build their channel builder from `security.protocol` through
`ChannelBuilders::client_channel_builder`: the producer
(`kafka_producer.rs:968`), the consumer (`async_kafka_consumer.rs:1899`) and
the admin client (`kafka_admin_client.rs:325`).

> Earlier the admin client passed `SecurityProtocol::Plaintext`
> unconditionally. Comments that give that as the reason are now stale, for
> example `tests/integration/admin_scram_test.rs:66-71`, which also cites
> `status.md:606-609`. The conclusion they draw still holds, but for a
> different reason. The SCRAM and delegation-token suites can create
> credentials but not authenticate with them, because the client implements
> SASL/PLAIN only and both kinds of credential are used through SASL/SCRAM.

### Other security types

- `KafkaPrincipal` (`common/security/auth/kafka_principal.rs`)
- `DelegationToken` / `TokenInformation` (`common/security/token/delegation/`)
- `ScramFormatter` and the internal `ScramMechanism`
  (`common/security/scram/internals/`): RFC 5802 `Hi` through
  `aws_lc_rs::pbkdf2`. This is a narrow helper for SCRAM *credential
  administration*, NOT a SASL/SCRAM client.

### Authenticator trait (`network/authenticator.rs`)

The trait is at `common/network/authenticator.rs:40`. It has two
implementations:
- `PlaintextAuthenticator`: a no-op for PLAINTEXT and SSL connections.
- `SaslClientAuthenticator`: full SASL PLAIN authentication, with the
  handshake/authenticate exchange.

---

## Error Handling

![Error types](img/error-types.svg)

*Takeaway: Java's exception hierarchy flattens into one `Error` enum with a
variant per Java exception. Java's intermediate classes survive as `is_*_error`
predicates, and `KafkaException` is the `KafkaError` struct that every variant
embeds.*

- `Error` (`common/error.rs:863`) is the single error type that every fallible
  API returns.
  - It is a `#[non_exhaustive]` flat enum with **162 variants**, one per Java
    exception class. Examples: `TopicAuthorization`,
    `ProducerBufferExhausted`, `ConsumerOffsetOutOfRange`,
    `CorrelationIdMismatch`.
  - It includes the `java.lang` / `java.util` runtime exceptions, under the
    `Local` prefix: `LocalIllegalArgument`, `LocalIllegalState`,
    `LocalConcurrentModification`, `LocalTimeout` and others.
  - `KafkaError(KafkaError)` is the variant for a bare `KafkaException`.
- Each variant's payload lives in its own file: 146 under `common/errors/`,
  plus `consumer/consumer_*_error.rs` and
  `producer/producer_buffer_exhausted_error.rs`, as CLAUDE.md §2 requires.
  Module-prefixed names (`Consumer…`, `Producer…`) avoid collisions with
  `common` errors of the same Java name.
- `KafkaError` (`common/kafka_error.rs:84`) is Java's `KafkaException` base,
  `{ error: Errors, custom_message: Option<String>, source: Option<Box<Error>> }`.
  Every payload embeds one.
- The ambassador-delegated traits forward to that embedded base: `ErrorCode`
  (`error.rs:318`), `ErrorName` (`:344`), `ErrorMessage` (`:509`) and
  `ErrorSource` (`:531`).
- The crate-private `ErrorHierarchy` trait (`error.rs:160`) encodes Java's
  `extends` chain. Each intermediate Java class is a predicate on `Error`,
  with public forms from `error.rs:1702`:
  - `is_kafka_error`, `is_api_error`, `is_retriable_error`,
    `is_refresh_retriable_error`, `is_timeout_error`
  - `is_invalid_metadata_error`, `is_invalid_configuration_error`,
    `is_application_recoverable_error`
  - `is_invalid_offset_error`, `is_consumer_invalid_offset_error`,
    `is_consumer_offset_out_of_range_error`
  - `is_out_of_order_sequence_error`, `is_serialization_error`
  - `is_authentication_error`, `is_authorization_error`

  They are not complements of each other. For example, `Serialization` is a
  `KafkaException` but not an `ApiException`. `is_transaction_abortable_error`
  (`:1650`) tests a single leaf class.
- Fatality is **not** a predicate on `Error`, because the same class is fatal
  in one context and recoverable in another. It is
  `RequestUtils::is_fatal_error(&Error)` (`common/requests/request_utils.rs:63`),
  crate-private like its Java home.
- `Errors` (`common/protocol/errors.rs:37`) is the crate-private wire-code
  enum. It has **135 variants** covering codes `-1..=133`. `Errors::error()`
  (`errors.rs:583`) builds the `Error` variant for a code.

---

## Code Generation

![Code generation pipeline](img/codegen-pipeline.svg)

*Takeaway: no generated code is checked in. `build.rs` re-derives every
message type from the JSON specs on each build, and the crate sees them
through one crate-private `generated` module.*

### Pipeline

1. JSON specifications live in `generator/messages/`: 198 Kafka protocol
   definitions, the same set as Apache Kafka 4.3.1.
2. The generator code in `generator/src/` parses the specs and emits Rust
   code.
3. `build.rs` invokes the generator at build time.
4. The output is `OUT_DIR/generated/`, with one `.rs` file per message type.
   It is re-exported through the `pub(crate) mod generated` in `lib.rs`
   (`lib.rs:109-113`). Java's generated `org.apache.kafka.common.message`
   package is not public API, so neither is this module.

The generator's own translation of `org.apache.kafka.message` (`CodeBuffer`,
`Versions`, `FieldType`, `FieldSpec`, `StructSpec`, `MessageSpec`,
`SchemaGenerator`, …) lives in `generator/src/message/`, not `src/message/`.

### Generated Artifacts

- Concrete data structs (e.g. `ApiVersionsRequestData`, `MetadataResponseData`)
- `Message` trait implementations (versioned read/write/size)
- `ApiMessage` implementations with `api_key()`
- Builder setters, Eq/Hash/Display derives
- 4 test-only message types from `generator/test-messages/`, generated into a
  `#[cfg(test)]` `test_generated` module (`lib.rs:120`)

---

## Key Dependencies

Verified against `Cargo.toml`.

| Crate | Version | Purpose |
|-------|---------|---------|
| tokio | 1.x | Async runtime (net, io-util, rt, rt-multi-thread, macros, time, sync, signal) |
| tokio-util | 0.7 | `CancellationToken` for `wakeup()` |
| async-trait | 0.1 | `Consumer` / `Admin` dispatch traits, `ConsumerRebalanceListener`, `OffsetCommitCallback` (not `Producer`; see above) |
| ambassador | 0.5 | Delegates the `Error*` traits from `Error` to each variant's payload |
| futures-util | 0.3 | Future combinators |
| tokio-rustls | 0.26 | Async TLS |
| rustls | 0.23 | TLS implementation (TLS 1.2 + 1.3); PEM cert/key parsing via its re-exported `pki_types::pem::PemObject` |
| webpki-roots | 0.26 | Root CA certificates |
| aws-lc-rs | 1 | rustls crypto provider, and PBKDF2-HMAC for `ScramFormatter::hi` (already a transitive dependency through rustls, so no extra compiled crate) |
| bytes | 1 | Buffer handling on the receive path |
| flate2 / snap / lz4_flex / zstd | 1 / 1 / 0.11 / 0.13 | Record-batch compression codecs |
| crc32c | 0.6 | Record-batch CRC |
| crc32fast | 1 | CRC-32 key hashing for the default partitioner |
| indexmap | 2 | Ordered HashMap |
| rustc-hash | 2 | FxHash for the Selector's per-poll channel maps |
| dashmap | 6 | Concurrent maps |
| uuid | 1 | UUID generation (v4) |
| rand | 0.9 | Randomization |
| regex | 1.0 | Consumer internal-topic and pattern matching, generator |
| serde / serde_json | 1.0 | Serialization for config and generator |
| generator | path | The code generator crate (also a build-dependency) |
| log | 0.4 | Logging facade |
| env_logger | 0.11 | Optional (`ffi` feature) / dev |
| cbindgen | 0.29 | Optional build-dependency: C header generation for the `ffi` feature |
| testcontainers(-modules) | 0.27 / 0.15 | Dev-only: Dockerised broker fixtures |
| rcgen | 0.13 | Dev-only: self-signed certificate generation |
| tonic / prost / tokio-stream / paste | 0.12 / 0.13 / 0.1 / 1 | Dev-only: multilanguage gRPC tests |

Release profile: `lto = "fat"`, `codegen-units = 1`, and **not**
`panic = "abort"`. `std::sync::Mutex` poisoning (`consumer-threading.md` §16)
needs unwinding.

Cargo features: `integration-tests`, `multilanguage-tests`, `ffi`.

---

## Test Infrastructure

### Unit Tests (4144 lib tests; 4361 with `ffi`)

- Message serialization/deserialization round-trips
- Protocol encoding (varint, flexible versions, tagged fields)
- Generated message type validation
- Network client with MockSelector
- Per-record allocation-budget assertions via `src/test_alloc_tracker.rs`
- `src/integration_tests/` (`#[cfg(all(test, feature = "integration-tests"))]`,
  `lib.rs:125-127`): the Docker-backed suites that need crate-private types —
  ApiVersions, connection, metadata and SSL/SASL. The public-API suites stay
  in `tests/integration/`.

### Integration Tests (27 files, 109 `#[tokio::test]`s, feature-gated)

- They require `--features integration-tests` and Docker.
- They use `testcontainers` with Kafka 4.2.0 (`tests/common/kafka_cluster.rs:36`),
  one minor version behind the 4.3.1 source the client is translated from.
- The consumer suites mirror Java's `PlaintextConsumerTest` family (assign,
  callback, commit, fetch, poll, subscription), plus
  `sasl_ssl_consumer_test`.
- The producer has `producer_transactions_test`.
- There are admin suites for topics, configs, groups, ACLs/quotas, SCRAM,
  delegation tokens, log dirs, elections/reassignments/offsets and
  transactions. Several need special fixtures: a broker with two
  `KAFKA_LOG_DIRS`, a 3-broker cluster, or an authorizer-enabled broker.
- The multilanguage cases come from macro expansions, not from
  `#[tokio::test]` attributes: `multilanguage_admin_test.rs` and
  `multilanguage_consumer_test.rs` for admin and consumer, and the
  `multilanguage_test!` uses inside `producer_test.rs` /
  `producer_transactions_test.rs` for the producer. So the attribute count
  above understates the suite.
- Infrastructure lives in `tests/common/`: `ClusterConfig`, `ClusterPool`,
  `KafkaCluster`, `TestContext`, `BackendFactory` / `BackendPool`, and
  `test_certs` for certificate generation.
- The `performance` target is `test = false` in `Cargo.toml`, so latency
  budgets never run under full-suite load. Invoke it explicitly.

### Cargo test targets

There are four test targets, three of which run under a plain `cargo test`:
- `producer` (`tests/producer/main.rs`)
- `consumer` (`tests/consumer/main.rs`)
- `integration`, auto-discovered from `tests/integration/main.rs` and gated
  behind `#![cfg(feature = "integration-tests")]`
- `performance`, which is `test = false`

`tests/common/` is a shared module included by the targets, not a target of
its own.

### Test commands

`make verify` (repository root) runs
`build format-check lint test check-bindings`. `build` and `test` cover the
Rust, C and Python sides. For Rust only, use `make verify-rust`.

The xtask subcommands are `format`, `format-check`, `check-generated`,
`generate-error-codes`, `java-deprecated`, `lint`, `lint-custom`,
`doc-hygiene`, `lint-fix`, `coverage`, `coverage-lcov`, `coverage-all`,
`test-multilanguage` and `producer-perf-test`.

---

## Bindings and Auxiliary Components

Everything below is in this repository and built by `make build`, but sits
outside the Rust client library proper.

![Bindings layering](img/bindings-layering.svg)

*Takeaway: every blocking operation crosses the C ABI twice over: once as a
`block_on` sync entry point and once as an `*_async` entry point completed on a
dispatcher thread. Both bindings are thin layers over that pair.*

### C FFI (`src/ffi/`, feature `ffi`)

The module is gated on the `ffi` feature, which also turns on the `cbindgen`
build-dependency that emits the C header. Only crate types that are public
have bindings (CLAUDE.md §4). It has five files:

| File | Exported symbols | `*_async` |
|------|-----------------:|----------:|
| `common.rs` (the `kafka_common_Error_t` surface, error predicates, the shared callback machinery) | 57 | 0 |
| `producer.rs` | 62 | 12 |
| `consumer.rs` | 156 | 22 |
| `consumer_handle.rs` | 23 | 1 |
| `admin.rs` | 490 | 47 |

Every operation that can block is exposed twice:
- The sync form drives the async API with `block_on` on a runtime the handle
  owns. It returns `*mut kafka_common_Error_t` (`common.rs:77`), with null
  meaning success.
- The `*_async` form takes a `*_callback_t` plus a `void *user_data` and
  spawns the future on the same runtime. It delivers the result through a
  dispatcher thread, using `CompletionJob` (`src/ffi/common.rs:1922`),
  `spawn_dispatcher` (`:1931`) and `enqueue_or_run_inline` (`:1949`).

`kafka_consumer_Consumer_subscribe` / `_subscribe_async`
(`src/ffi/consumer.rs:2935`, `:2953`) are the canonical pair.

Error classification crosses the boundary as predicates. C cannot see enum
variants, so every `is_*_error` predicate on `Error` has a
`kafka_common_Error_is_*` counterpart. Errors with extra payload expose an
opaque accessor type, such as `kafka_common_ResourceNotFoundError_t`.

The consumer handle keeps its `ConsumerKind` in an `UnsafeCell` behind a
non-reentrant single-owner guard, an `AtomicU64` owner id. A second thread
that enters while another holds the guard gets a `ConcurrentModification`
error rather than being serialized. That is Java's
`KafkaConsumer.acquire()/release()` contract. `wakeup()` deliberately bypasses
the guard, because it has to work *while* another thread holds it
(`src/ffi/consumer.rs:166-167`, `:553`).

`consumer_handle.rs` exposes the reentrant-safe `ConsumerHandle` operations to
C: `assign`, `seek`, `pause`, `resume`, `position`, `committed`, the offset
queries and `commit_*`. So a C rebalance listener can call back into the
consumer.

The producer handle also has an async submission outbox for
`send_async` / `send_batch_async`:
- `SubmitRequest::{Send, Barrier}` (`producer.rs:714-720`).
- `drain_submitted_sends_await` (`:3108`) pushes a FIFO barrier.
- `flush`, `close` and every transaction-control operation drain the outbox
  first, through `with_txn_control` (`:3172`) and `with_txn_control_async`
  (`:3593`). So a send that has returned is always included in the next
  commit, abort or flush (`producer-transactions.md` §13).

The admin handle, `kafka_admin_AdminClient_t` (`src/ffi/admin.rs:278`), wraps
either a `KafkaAdminClient` or a `MockAdminClient`. It exposes each RPC as a
sync call plus an `*_async` variant, and copies result structs out through
accessor functions.

> The module docs at `src/ffi/consumer.rs:37` cite
> `design/current/consumer-ffi-plan.md`. That plan has moved under
> `design/history/`, so the path in the code is stale.

### Python bindings (`python/`)

`producer.py`, `consumer.py` and `admin.py` sit over a hand-written CPython
extension (`_confluentkafka.c`) that links the `confluent_kafka` cdylib.
`python/setup.py` resolves the library under `../rust/target/`, and
`CONFLUENT_KAFKA_LIB_DIR` overrides that.

Each module offers both shapes, matching the two C entry points underneath:
- A synchronous family returning `concurrent.futures.Future`.
- An async family whose methods are coroutines.

For the producer these are `Producer` / `KafkaProducer` / `MockProducer` and
`AsyncProducer` / `AsyncKafkaProducer` / `AsyncMockProducer`, all on a shared
`_ProducerBase`. The consumer and admin modules follow the same pattern: the
admin module has `Admin` / `AdminClient` / `MockAdminClient` and their `Async*`
twins, and the consumer module also has `ConsumerHandle`.

`grpc_server.py`, `grpc_server_async.py` and `grpc_translate.py` back the
Python arm of the multilanguage tests.

### Multilanguage test harness

`rust/multilanguage-test-server/` (a Rust gRPC server with `admin`, `consumer`
and `producer` services), `c/grpc_server/` (the C backend) and the Python
servers above let the same producer, consumer and admin scenarios run against
three targets: the native Rust client, the Python binding and the C binding.
The harness is enabled by the `multilanguage-tests` feature, which implies
`integration-tests`, and driven by `cargo xtask test-multilanguage`.

### Benchmark harness

`consumer-perf/` is a workspace member that holds the consumer benchmark
driver. `tests/performance/` is the latency-budget cargo target, kept out of
ordinary runs by `test = false`. The Python and C producer perf tests live
under `python/test/performance/` and `c/tests/producer_perf_test.c`.

---

## Where the history lives

This document deliberately carries no step-by-step record. For how and when
each piece arrived, including the plans and reviews, see `design/history/`.
For what is done and what is still open, see [status.md](status.md).
