# Phase 12: Production constructor wiring + integration test

## Goal

Close out Milestone 8 by translating the **350-line Java primary
constructor** of `AsyncKafkaConsumer` end-to-end, wiring it through the
`new_consumer` factory, and proving the result against a real Kafka
4.2.0 broker via a `tests/integration/consumer_test.rs` analog of
`producer_test.rs`.

After Phase 12, callers running `cargo test --features integration-tests
--test integration_main consumer_test` can:

1. `let mut consumer = new_consumer::<String, String>(cfg, …)?;`
2. `consumer.subscribe(vec![topic])?;`
3. Produce records via the existing `KafkaProducer`.
4. `consumer.poll(Duration::from_secs(5)).await?` → assert key / value /
   topic / partition / offset.

Phase 11 already shipped the full struct + trait impl + `MockClient`-
backed unit tests via `AsyncKafkaConsumer::new_with_components`. Phase 12
adds:

- (a) **Production constructor.** `AsyncKafkaConsumer::new(config,
  key_de, value_de)` — translates `AsyncKafkaConsumer.java:355-518`
  (primary ctor) line-for-line, builds the full dependency closure,
  spawns the bg task, returns the consumer.
- (b) **Factory swap.** `new_consumer()`'s
  `GroupProtocol::Consumer => Err(...)` arm becomes
  `Ok(Box::new(AsyncKafkaConsumer::new(cfg, kd, vd)?))`.
- (c) **State-notifier registration.** The
  `ConsumerStateNotifier` (already built inside `new_with_components`,
  Phase 11) is registered on `ConsumerMembershipManager` via
  `register_member_state_listener` so the bg task observes group
  metadata + assignment-snapshot updates. The wire-up was deferred per
  the Phase-11 PLAN.md note at `async_kafka_consumer.rs:548`
  ("production wire-up (Phase 12) clones `state_notifier`…").
- (d) **Integration test.** `tests/integration/consumer_test.rs` with
  4 end-to-end flows (subscribe / assign / commit-and-resume /
  seek-to-beginning). Module wired into `tests/integration/main.rs`.
- (e) **Unblocked unit tests.** Every Phase-11-deferred test marked
  "Deferred to Phase 12 (integration tests)" or "needs MockClient"
  becomes translatable via the new production ctor (which still uses
  the in-process `NetworkClient` + a real socket — the unit tests use
  `mock_broker`). If a test cannot be reasonably translated even with
  the production ctor wired, carry a one-line `// SKIP: <reason>`
  rationale per DoD §3.

## Branch

Lands on `consumer-impl` directly. **No worktree.** Phase 12 is the
final Milestone-8 phase; no further phases follow on this branch.

## Java sources

### Phase 12 production (translate)

- `org/apache/kafka/clients/consumer/internals/AsyncKafkaConsumer.java`
  lines **285-518** (primary public constructor + the fields it
  initializes). Pinned commit
  `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.
- The `memberStateListener` anonymous-inner-class at lines 343-353
  is **already translated** (`ConsumerStateNotifier` in
  `async_kafka_consumer.rs:383`). Phase 12 only registers it.
- `initializeGroupMetadata(ConsumerConfig, GroupRebalanceConfig)` at
  line 856 (currently called only by the test ctors).
- `closeQuietly(...)` helper chain inside the catch block at lines
  509-517 (partial-ctor cleanup). Translate via Rust's `?` +
  explicit drop ordering — see "Error path" below.

### Out of scope (deferred Milestone-8-wide, do NOT touch)

- `AsyncConsumerMetrics`, `KafkaConsumerMetrics`, `ConsumerMetrics`
  field initialization. The Phase-11 PLAN already deferred these;
  Phase 12 inherits the deferral. The corresponding fields stay as
  no-op placeholders.
- `ClientTelemetryReporter` (line 400-401). Pass `None` /
  no-op placeholder; matches Phase-11 PLAN deferral #6.
- `AppInfoParser.registerAppInfo(...)` at line 507. JMX registration
  is not part of the Rust translation (no JMX in scope).
- `StreamsRebalanceData` and the `streamsRebalanceData.map(...)` arms
  at lines 488-489 — Streams out of scope per
  `consumer-threading.md` §20.
- `ConsumerInterceptor` reflection-based instantiation. Rust's
  `ConsumerConfig` carries already-constructed
  `Box<dyn ConsumerInterceptor<K, V>>` per the Phase-2 decision —
  the production ctor reads them out, no `Class.forName` equivalent.
- `ClusterResourceListeners` plumbing (line 412-414). Translation
  exists for the producer (`ClusterResourceListeners::new()`); use
  the same here. The deserializer/interceptor `onUpdate` notifier
  wiring stays a no-op for Phase 12 (matches Phase-11 deferral —
  interceptors are wired structurally but `onUpdate` is not driven).
- `ChannelBuilder` SSL/SASL escape hatches. **PLAINTEXT only** for
  Phase 12. The integration test uses Kafka 4.2.0 with the PLAINTEXT
  listener (matches the producer integration test). SSL/SASL ctor
  wiring is left for a future cross-cutting commit.

### Tests in scope

#### New integration test file

`tests/integration/consumer_test.rs`, gated behind
`#![cfg(feature = "integration-tests")]`, wired into
`tests/integration/main.rs`. Four `#[tokio::test(flavor =
"multi_thread")]` flows:

1. **`test_subscribe_and_poll_records`** — subscribe to a fresh topic,
   produce 10 records with deterministic keys via the existing
   `KafkaProducer`, poll until 10 records observed, assert each
   `ConsumerRecord` carries the expected key / value / topic /
   non-negative partition / monotonic offset.
2. **`test_assign_partitions_and_poll`** — `assign(vec![TopicPartition
   ::new(topic, 0)])` to a topic with 1 partition, produce 5 records,
   poll, assert assignment skipped the coordinator path (group_id is
   absent from this consumer's config).
3. **`test_commit_sync_then_resume_in_same_group`** — subscribe in
   `group.id = G1`, produce 10, poll 5, `commit_sync().await`,
   `close().await`. Create a fresh consumer in the same `G1`, poll,
   assert the remaining 5 records are visible (offset committed).
4. **`test_seek_to_beginning_re_reads_records`** — subscribe + poll a
   few records to establish position, `seek_to_beginning(&assigned)`,
   poll again, assert all records re-read from offset 0.

Each test uses `TestContext` / `ClusterConfig::default()` (PLAINTEXT
listener). Topics are created via the existing
`ctx.topic("…")` helper. Group IDs via `ctx.group_id("…")`.

#### Group-rebalance multi-consumer tests

**Skipped from Phase 12** unless they fit into a single deterministic
flow. Rationale carried in the test file header. KIP-848 server-side
assignment makes multi-consumer rebalance flakiness common in
integration tests; we'd rather ship 4 green tests than 5 with one
flaky.

#### Unit tests unblocked by the production ctor

Translate the AsyncKafkaConsumerTest cases marked **"Deferred to
Phase 12 (integration tests)"** or **"needs MockClient"** in
`async_kafka_consumer.rs` (5 occurrences at the time of Phase-11
close-out). Each unblocked test becomes a new `#[tokio::test]` inside
`tests/consumer/async_kafka_consumer_test.rs` if and only if it can be
expressed without a real broker. The ones that genuinely need a real
broker (`testSubscribePatternAgainstBrokerNotSupportingRegex`) move
into the integration test file as a separate `#[ignore]`-gated case OR
remain explicitly skipped with rationale.

The five Phase-11 deferral markers to revisit:

- `async_kafka_consumer.rs:3875` —
  `testSubscribePatternAgainstBrokerNotSupportingRegex`.
  Verdict: keep as integration test (regex broker rejection requires
  a real network path).
- `async_kafka_consumer.rs:3967` — group_metadata bg-task wire-up
  assertion. Phase 12 wires the bg-task; the test becomes translatable
  if Phase 12 also calls `register_member_state_listener` on the
  membership manager. (We do — see (c) above.)
- `async_kafka_consumer.rs:5295` — `wakeup` mid-fetch path. Phase 12
  ctor's bg task makes this exercisable; translate.
- `async_kafka_consumer.rs:5655` — close events observed by the bg
  task. Translate against the production ctor; if the test requires
  a `MockClient`-injected handle, keep the existing skip.
- `async_kafka_consumer.rs:6203` —
  `awaitPendingAsyncCommits` race. Same as above — translate if
  possible, otherwise keep skipped with rationale referencing the
  Java line.

For each, the Actor either (a) commits the translated test or (b)
leaves the existing `// SKIP:` marker in place with an updated
rationale referencing the Phase-12 production ctor.

## Rust outputs

### NEW files

```
tests/integration/consumer_test.rs                  # NEW — 4 end-to-end flows
```

### Updated files

```
src/consumer/async_kafka_consumer.rs                # +AsyncKafkaConsumer::new(config, kd, vd)
src/consumer/mod.rs                                 # new_consumer arm: Box::new(AsyncKafkaConsumer::new(...)?)
src/consumer/internals/consumer_membership_manager.rs   # NO CHANGE — already exposes register_member_state_listener (Phase 8b)
tests/consumer/async_kafka_consumer_test.rs         # +translated Phase-11-deferred tests (where reasonable)
tests/integration/main.rs                           # mod consumer_test;
design/history/Milestone-8/PLAN.md                  # row 12 → CLOSED
.claude/agent-memory/...                            # Phase 12 patterns + memory updates
```

## Behavior parity

### `AsyncKafkaConsumer::new(config, key_de, value_de)`

A single `pub fn new(...) -> Result<Self, KafkaError>` that mirrors
the Java primary ctor at `AsyncKafkaConsumer.java:355-518`. Implements
the **same field-init order** as Java; each step references the Java
line via inline comments. Builds and returns a fully-spawned consumer.

Step-by-step (Java line → Rust action):

| Java line | Java action | Rust action |
|---|---|---|
| 390 | `clientId = config.getString(CLIENT_ID_CONFIG)` | `let client_id: Arc<str> = Arc::from(config.client_id())` |
| 391 | `autoCommitEnabled = config.getBoolean(ENABLE_AUTO_COMMIT_CONFIG)` | `let auto_commit_enabled = config.enable_auto_commit()` |
| 392 | `LogContext logContext = createLogContext(...)` | Drop — Rust uses `log` crate. |
| 393-394 | `backgroundEventQueue`, logger init | Build `mpsc::unbounded_channel` for bg events. |
| 397 | `defaultApiTimeoutMs = Duration.ofMillis(...)` | Read `config.default_api_timeout_ms` into `i64`. |
| 398 | `time = time` | `Arc::new(SystemThreadTime)`. |
| 399-401 | `reporters`, `clientTelemetryReporter` | Drop (telemetry deferred). |
| 402 | `metrics = createMetrics(...)` | Drop (metrics deferred). Stub `None`. |
| 403-404 | `asyncConsumerMetrics`, `kafkaConsumerMetrics` | Drop. |
| 405 | `retryBackoffMs = config.getLong(RETRY_BACKOFF_MS_CONFIG)` | `config.retry_backoff_ms()`. |
| 406 | `requestTimeoutMs = config.getInt(REQUEST_TIMEOUT_MS_CONFIG)` | `config.request_timeout_ms()`. |
| 408-409 | `interceptorList`, `interceptors = new ConsumerInterceptors<>(...)` | Read `config.take_interceptors()` (Phase 2 contract — moved out of config), wrap in `Arc<Mutex<ConsumerInterceptors<K,V>>>`. |
| 410 | `deserializers = new Deserializers<>(...)` | `Arc::new(Deserializers::new(key_de, value_de))`. |
| 411 | `subscriptions = createSubscriptionState(...)` | `Arc::new(Mutex::new(SubscriptionState::new(config.auto_offset_reset())))`. |
| 412-414 | `ClusterResourceListeners` | Drop notifier wiring (Phase-11 deferral); pass `ClusterResourceListeners::new()`. |
| 415 | `metadata = metadataFactory.build(...)` | `Arc::new(ConsumerMetadata::new(config, Arc::clone(&subscriptions)))`. |
| 416-417 | `addresses = parseAndValidateAddresses(...)`, `metadata.bootstrap(addresses)` | `let addrs = client_utils::parse_and_validate_addresses(&config.bootstrap_servers)?; metadata.bootstrap(addrs);`. |
| 419 | `fetchMetricsManager = createFetchMetricsManager(...)` | Drop (metrics deferred); pass `None` / stub. |
| 420 | `fetchConfig = new FetchConfig(config)` | `FetchConfig::from_consumer_config(&config)`. |
| 421 | `isolationLevel = fetchConfig.isolationLevel` | `let isolation_level = fetch_config.isolation_level;`. |
| 423 | `apiVersions = new ApiVersions()` | `Arc::new(ApiVersions::new())`. |
| 424 | `applicationEventQueue = new LinkedBlockingQueue<>()` | `mpsc::unbounded_channel::<ApplicationEventEnvelope>()`. |
| 425-429 | `backgroundEventHandler = new BackgroundEventHandler(...)` | `Arc::new(BackgroundEventHandler::new(bg_event_tx))`. |
| 432 | `fetchBuffer = new FetchBuffer(logContext)` | `Arc::new(FetchBuffer::new())`. |
| 433 | `positionsValidator = new PositionsValidator(...)` | Phase 12 may stub `None` if not yet wired into the bg task; otherwise construct via the Phase-7 type. |
| 434-445 | `networkClientDelegateSupplier = NetworkClientDelegate.supplier(...)` | Build the `NetworkClient` (PLAINTEXT, mirror producer ctor lines 268-289), wrap in `NetworkClientDelegate::new(...)`, store in `Arc<AsyncMutex<NetworkClientDelegate<NetworkClient>>>`. |
| 446 | `offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(interceptors)` | `Arc::new(OffsetCommitCallbackInvoker::new(consumer_interceptors_clone))`. |
| 447 | `groupMetadata.set(initializeGroupMetadata(config, groupRebalanceConfig))` | Translate `initialize_group_metadata(&config)` returning `Option<ConsumerGroupMetadata>`; store inside the per-ctor `Arc<Mutex<Option<ConsumerGroupMetadata>>>` that the `ConsumerStateNotifier` shares. |
| 448-465 | `requestManagersSupplier = RequestManagers.supplier(...)` | Build each RM manually via the Phase-6/7/8/9 `RequestManagers::new(...)` — coordinator, commit, heartbeat, membership, offsets, topic_metadata, fetch. Group-protocol-conditional: heartbeat / membership / commit only if `group.id` is present. |
| 466-470 | `applicationEventProcessorSupplier` | `ApplicationEventProcessor::new(metadata, subscriptions, request_managers)`. |
| 471-481 | `applicationEventHandler = new ApplicationEventHandler(...)` | `Arc::new(ApplicationEventHandler::new(app_event_tx))`. |
| 482-487 | `rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(...)` | `ConsumerRebalanceListenerInvoker::new(Arc::clone(&subscriptions))`. |
| 488-489 | `streamsRebalanceListenerInvoker` | Drop (Streams out of scope). |
| 490 | `backgroundEventProcessor = new BackgroundEventProcessor()` | The `process_background_events` method already exists on `AsyncKafkaConsumer` (Phase 11); no separate processor struct. |
| 491 | `backgroundEventReaper = backgroundEventReaperFactory.build(logContext)` | `Arc::new(Mutex::new(CompletableEventReaper::new()))`. |
| 494-500 | `fetchCollector = fetchCollectorFactory.build(...)` | `Arc::new(FetchCollector::new(metadata, subscriptions, fetch_config, deserializers, fetch_collector_time))`. |
| 502-505 | `groupMetadata.get().isPresent() && groupProtocol == CONSUMER` → `config.ignore(GROUP_REMOTE_ASSIGNOR_CONFIG)` | Translate — silently ignore the classic-protocol-only config. Rust currently doesn't track "ignored" config keys; comment-only is fine here. |
| 506 | `config.logUnused()` | `log::debug!("Kafka consumer initialized")`. |
| 507 | `AppInfoParser.registerAppInfo(...)` | Drop (JMX out of scope). |
| 508 | `log.debug("Kafka consumer initialized")` | Same. |

After all components are built, **register the state-notifier** on the
membership manager (only if `group.id` is present):

```rust
if let Some(membership) = request_managers.lock().unwrap().consumer_membership.as_ref() {
    membership.register_member_state_listener(Arc::clone(&state_notifier));
}
```

Then **spawn the bg task**:

```rust
let app_processor = ApplicationEventProcessor::new(
    Arc::clone(&metadata),
    Arc::clone(&subscriptions),
    Arc::clone(&request_managers),
);
let mut network_thread = ConsumerNetworkThread::new(
    Arc::clone(&time),
    app_event_rx,
    Arc::clone(&completable_event_reaper),
    app_processor,
    Arc::clone(&network_client_delegate),
    Arc::clone(&request_managers),
    membership_opt.clone(),
    wakeup_trigger.clone(),
);

let signal_close_fn = {
    let thread_ref = thread_inner.clone();  // shared Arc<NetworkThreadShared> if needed
    Box::new(move || thread_ref.signal_close())
};
// … wakeup_fn similarly …

let join_handle = tokio::spawn(async move {
    loop {
        if !network_thread.is_running() { break; }
        network_thread.run_once().await;
    }
    network_thread.cleanup().await;
});

let network_thread_close = NetworkThreadCloseHandle::new(
    signal_close_fn,
    wakeup_fn,
    join_handle,
);
```

Finally, build the `AsyncKafkaConsumerComponents` struct and call the
existing `Self::new_with_components(...)`. This preserves the
Phase-11 test seam exactly — no duplicated init code. The
`new_with_components` body already does the `ConsumerStateNotifier`
creation; we just additionally register it onto the membership
manager **before** spawning.

### Error path

Java's try-catch at lines 509-517 calls
`close(Duration.ZERO, LEAVE_GROUP, true)` on any partial-ctor error.
Rust translates this via:

- Each construction step uses `?` to early-return.
- On early-return, fields already built (`NetworkClient`, channels,
  `FetchBuffer`) are dropped in reverse construction order via Rust's
  drop glue. Channels close naturally.
- The bg task is **NOT** spawned until after every other component
  exists. If the spawn line itself fails (it cannot — `tokio::spawn`
  doesn't return `Result`), there's nothing to close.
- If a later step fails after spawn (none should — spawn is the last
  step), the caller would observe a partial consumer; we guarantee
  no construction step after `tokio::spawn` returns an error.

### Group-protocol gate

`new_consumer()` already enforces `GroupProtocol::Classic =>
Err(unsupported_version)`. The `Consumer` arm calls
`AsyncKafkaConsumer::new(...)`. For assignment-only consumers
(`group.id` absent), the ctor builds without heartbeat / commit /
membership managers (`None` in the corresponding `RequestManagers`
slots) — the Phase-6/8 `RequestManagers::new` already supports
`None` for these.

### State-notifier registration

`async_kafka_consumer.rs:548` builds
`state_notifier: Arc<ConsumerStateNotifier>` and stores it on `self`.
Phase 11 marked the **production registration on the membership
manager** as a Phase-12 deliverable. Phase 12 adds:

```rust
membership.register_member_state_listener(Arc::clone(&state_notifier));
```

inside the new production ctor, before bg-task spawn. The Phase-11
test rig stores `state_notifier` but never registers it (no
membership manager wired); the production ctor does both.

The `register_member_state_listener` method already exists on
`ConsumerMembershipManager` (Phase 8b). The Phase-11 TODO at
`consumer_membership_manager.rs:538` is already closed (per Phase-11
PLAN commit (7/N)); no further changes there.

### Integration test wiring

`tests/integration/consumer_test.rs` follows the producer test
pattern exactly:

```rust
#![cfg(feature = "integration-tests")]   // implicit via main.rs

use std::time::Duration;
use confluent_kafka::common::serialization::StringDeserializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};
use crate::common::cluster_config::ClusterConfig;
use crate::common::test_context::TestContext;

fn make_consumer_config(bootstrap: &str, group_id: &str) -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".into(), bootstrap.into()),
        ("group.id".into(), group_id.into()),
        ("group.protocol".into(), "consumer".into()),  // KIP-848
        ("auto.offset.reset".into(), "earliest".into()),
        ("client.id".into(), "integration-test-consumer".into()),
        ("enable.auto.commit".into(), "false".into()),
    ]);
    ConsumerConfig::from_properties(&props).expect("invalid test config")
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_and_poll_records() {
    let mut ctx = TestContext::new(ClusterConfig::default()).await;
    let topic = ctx.topic("subscribe_poll");
    let group_id = ctx.group_id("g1");

    // Produce 10 records first so the consumer has something to fetch.
    let producer = KafkaProducer::from_config(
        producer_config(ctx.bootstrap_servers()),
        Box::new(StringSerializer),
        Box::new(StringSerializer),
    )?;
    for i in 0..10 {
        let rec = ProducerRecord::with_key(topic.clone(), Some(format!("k{}", i)), Some(format!("v{}", i)));
        producer.send(rec).await?.get_timeout(Duration::from_secs(30)).await?;
    }
    producer.close().await?;

    let mut consumer = new_consumer::<String, String>(
        make_consumer_config(ctx.bootstrap_servers(), &group_id),
        Box::new(StringDeserializer),
        Box::new(StringDeserializer),
    )?;
    consumer.subscribe(vec![topic.clone()]).await?;

    let mut collected = Vec::new();
    let start = std::time::Instant::now();
    while collected.len() < 10 && start.elapsed() < Duration::from_secs(30) {
        let records = consumer.poll(Duration::from_secs(5)).await?;
        for r in records.iter() { collected.push((r.key().clone(), r.value().clone(), r.partition(), r.offset())); }
    }
    assert_eq!(collected.len(), 10);
    // … assert keys / values / monotonic offsets …
    consumer.close(CloseOptions::default()).await?;
}
```

The other three tests follow the same pattern.

## Commit plan

Each commit is independently buildable and has its own tests passing.

| # | Title | Files touched | Approx LOC |
|---|---|---|---|
| 1 | `Phase 12 (1/N): scaffold AsyncKafkaConsumer::new(config, kd, vd) — build channels + subscriptions + metadata + NetworkClient` | `async_kafka_consumer.rs` ctor scaffold (no bg-task spawn yet, no factory wire-up) | ~250 prod |
| 2 | `Phase 12 (2/N): build RequestManagers + group-protocol gate + state-notifier registration` | `async_kafka_consumer.rs` RM wiring; conditional heartbeat/commit/membership; `register_member_state_listener` call | ~200 prod |
| 3 | `Phase 12 (3/N): spawn bg task + assemble NetworkThreadCloseHandle + call new_with_components` | `async_kafka_consumer.rs` bg-task spawn + `signal_close_fn` / `wakeup_fn` closures | ~150 prod |
| 4 | `Phase 12 (4/N): factory swap in new_consumer + smoke test against mock_broker` | `src/consumer/mod.rs` factory arm flip; unit smoke test in `async_kafka_consumer_test.rs` that builds a real `AsyncKafkaConsumer::new` against a localhost listener that refuses-connection (verifies ctor succeeds without a broker) | ~50 prod / ~100 test |
| 5 | `Phase 12 (5/N): integration consumer_test — subscribe + poll flow` | `tests/integration/consumer_test.rs` (test 1) + `tests/integration/main.rs` wire-up | ~150 test |
| 6 | `Phase 12 (6/N): integration consumer_test — assign + commit + seek flows` | `tests/integration/consumer_test.rs` (tests 2-4) | ~250 test |
| 7 | `Phase 12 (7/N): unblock Phase-11-deferred unit tests + close out PLAN.md row 12` | `tests/consumer/async_kafka_consumer_test.rs` (per-deferral-marker translation or rationale update) + `design/history/Milestone-8/PLAN.md` status flip + agent-memory entry | small |

**Granularity rationale:** Commits (1)-(3) build the production ctor
in three buildable slices so each step adds runnable code without
holding back. Commit (4) flips the public factory but also adds a
unit-level smoke test so we don't depend on docker availability to
prove the ctor works at all. Commits (5)-(6) split the integration
tests at the natural docker-boundary so a single docker session can
exercise both halves. Commit (7) is the close-out.

**Inter-commit dependencies:** (1)→(2)→(3)→(4) strictly serial.
(5) and (6) can run in parallel once (4) lands. (7) must come last.

## Definition of Done

Per `definition-of-done.md` plus Milestone-8 phase additions:

- `cargo build` clean.
- `cargo test` (unit + non-integration tests) clean. No skipped tests
  beyond the ones explicitly carrying a `// SKIP: <reason>` rationale.
- `cargo xtask format-check` clean.
- `cargo xtask lint` clean (clippy-warnings-as-errors).
- `cargo xtask check-generated` clean.
- `cargo test --features integration-tests --test integration_main
  consumer_test` — runs end-to-end against the testcontainers Kafka
  4.2.0 broker. **The Actor must run this locally before declaring
  done.** If docker is unavailable on the Actor's machine, the Actor
  must say so explicitly in the close-out message — do NOT claim
  success without exercising the test.
- `new_consumer<K, V>(config, kd, vd)` returns
  `Ok(Box::new(AsyncKafkaConsumer::new(...)?))` for
  `GroupProtocol::Consumer`. The `GroupProtocol::Classic` arm
  continues to return the unsupported-version error.
- The factory comment block at `src/consumer/mod.rs:428-442` is
  rewritten to reflect that Phase 12 has landed the production ctor.
  The "deferred to Phase 12" wording is removed.
- The 5 Phase-11 `Deferred to Phase 12 / needs MockClient` markers
  in `async_kafka_consumer.rs` are addressed individually — either
  the corresponding test is translated, or the marker is rewritten to
  point at a more specific reason (e.g. "needs `--features
  integration-tests`" or "covered by `tests/integration/consumer_test`").
- State-notifier registration: the `ConsumerStateNotifier` built in
  `new_with_components` is registered on the
  `ConsumerMembershipManager` via `register_member_state_listener`
  before bg-task spawn (production ctor only — test rig is unchanged).
- The Phase-11 PLAN.md `state_notifier`-related TODO comments
  inside `async_kafka_consumer.rs` (currently at lines 220, 240,
  546-549, 698, 745) are updated to reflect that the wire-up has
  shipped.
- **Trait surface check (DoD §11):**
  - `Consumer<K, V>` factory returns `Box<dyn Consumer<K, V>>` —
    verified by `tests/consumer/trait_surface_check.rs`.
  - `AsyncKafkaConsumer::new` (production ctor) returns `Result<Self,
    KafkaError>`, not `Result<Box<dyn Consumer>, _>` — the factory
    wraps. No double-Box.
  - No `block_on`-wrapped sync façade.
- **Hot-path allocation audit (DoD §10):**
  - The production ctor allocates **at construction time only** —
    nothing allocated per-poll or per-record by the ctor changes.
    The receive-path zero-copy contract from Phase 7 stays intact.
  - Integration test does not enable any "copy bytes" code path —
    `StringDeserializer` returns owned `String`, but that allocation
    is the user's choice (per §27).
- **Phase-12-specific audits:**
  - Production ctor does NOT spawn more than one `tokio::spawn`
    (single bg task per consumer instance, `consumer-threading.md`
    §10).
  - Production ctor's bg-task spawn happens **after** every other
    component is built (no partial-spawn-on-error).
  - State-notifier is registered **before** bg-task spawn so the
    very first heartbeat cycle drives `update_group_metadata`.
  - Group-protocol gate: if `config.group.id` is None, the
    `RequestManagers` built has `consumer_heartbeat = None`,
    `consumer_membership = None`, `commit = None`. Verified by the
    Phase-12 (4) smoke test.
- **Carry-overs to a future milestone** (NOT addressed in Phase 12):
  - `AsyncConsumerMetrics` / `KafkaConsumerMetrics` translation —
    deferred Milestone-8-wide.
  - SSL/SASL `ChannelBuilder` escape hatch — Phase 12 ships PLAINTEXT
    only.
  - C FFI for `AsyncKafkaConsumer` — out of milestone.
  - Classic-protocol path — out of milestone per §20.

## Workflow

Per `.claude/rules/agent-roles.md`:

1. Actor agent N=1 spawned to implement commits (1)-(7) sequentially.
2. After each batch of 2-3 commits, Critic agent N=1 reviews via
   `cargo xtask await-commit` and writes comments to
   `design/history/Milestone-8/Phase-12/COMMENTS.1.md`.
3. Actor reads `COMMENTS.1.md`, fixes each issue, moves resolved
   comments to `COMMENTS.DONE.1.md`, commits each fix as `fixup!`
   referencing the original commit.
4. Loop until `COMMENTS.1.md` is empty AND every DoD checkbox passes.
5. Phase 12 closes with a Status section appended to this PLAN.md
   listing the final commit-hash table, the row-12 flip in
   `Milestone-8/PLAN.md`, and an agent-memory note for Phase-12
   patterns.

## Risks

- **Docker / testcontainers unavailable in the Actor's environment.**
  Mitigation: commit (4) ships a unit-level smoke test against
  localhost-refuses-connection so the ctor is exercised at unit-test
  time. The Actor must explicitly state docker availability in the
  close-out message; the Manager re-runs integration tests in a
  docker-enabled environment before phase close-out is final.
- **KIP-848 broker compatibility.** Kafka 4.2.0 supports the new
  consumer-group protocol on the broker side, but the integration
  test cluster config may need
  `--override group.coordinator.rebalance.protocols=classic,consumer`
  or similar to enable it. Verify against the existing testcontainers
  setup before committing test (1); if the broker rejects
  `group.protocol=consumer`, fall back to setting the broker config
  override in `ClusterConfig` (existing infrastructure).
- **Integration test flakiness.** Producer-then-consume timing can
  race — tests use a 30-second deadline with a poll-until-N-records
  loop to absorb broker initialization jitter. Same pattern as the
  producer integration tests.
- **State-notifier registration timing.** Must happen before bg-task
  spawn so the first heartbeat cycle drives metadata updates. If
  registered after spawn, the bg task may briefly hold a stale
  `None` listener Arc; the Phase-8b membership manager handles `None`
  gracefully (no panic) but the test assertion observing
  `group_metadata().member_epoch() > 0` would be flaky.

## Status — CLOSED

Phase 12 is **CLOSED** as of commit (7/N). The production constructor
ships, the `new_consumer` factory flows through `AsyncKafkaConsumer::new`
for the `GroupProtocol::Consumer` arm, the state-notifier is wired
single-source-of-truth onto the membership manager + consumer struct,
and the shared `Arc<AtomicI64>` for `max_time_to_wait_ms` connects the
bg task to the app-side accessor.

### Final commit table

| # | SHA | Title |
|---|---|---|
| 0 | `2eb875a` | `Phase 12 (0/N): PLAN.md — AsyncKafkaConsumer ctor wire-up + factory swap + integration smoke` |
| 1 | `a4378ce` | `Phase 12 (1/N): scaffold AsyncKafkaConsumer::new(config, kd, vd) — build channels + subscriptions + metadata + NetworkClient` |
| 2 | `143f30a` | `Phase 12 (2/N): build RequestManagers + group-protocol gate + state-notifier registration` |
| 2-fixup | `db3f793` | `fixup! Phase 12 (2/N): Issues 1 + 4 — commit poll wire-up + auth-closure FIXME` |
| 2-fixup | `8e2540c` | `fixup! Phase 12 (2/N): Issues 2 + 5 — single ConsumerStateNotifier wiring` |
| 3 | `d3e9e15` | `Phase 12 (3/N): spawn bg task + assemble NetworkThreadCloseHandle + call new_with_components` |
| 3-fixup | `6114cb0` | `fixup! Phase 12 (3/N): Issue 3 — shared Arc<AtomicI64> for max_time_to_wait_ms` |
| 4 | `188ddf0` | `Phase 12 (4/N): factory swap in new_consumer + smoke test + Issue 6` |
| 5 | `4b48abe` | `Phase 12 (5/N): integration consumer_test — subscribe + poll flow` |
| 6 | `7bea0a4` | `Phase 12 (6/N): integration consumer_test — assign + commit + seek flows` |
| 6-doc | `b0738a1` | `Phase 12: response-routing audit + Critic round-3 + agent-memory` |
| 7 | TBD | `Phase 12 (7/N): close-out — PLAN status + Phase-11 deferral cleanup + Phase 12.5 pointer` |

### What shipped

- **Production constructor.** `AsyncKafkaConsumer::new(config, kd, vd)`
  translates Java's primary ctor (`AsyncKafkaConsumer.java:355-518`)
  line-for-line. Builds the full dependency closure (subscriptions,
  metadata, `NetworkClient`, `BackgroundEventHandler`, `FetchBuffer`,
  `Deserializers`, `ConsumerInterceptors`,
  `OffsetCommitCallbackInvoker`, `RequestManagers`,
  `ConsumerStateNotifier`, `ApplicationEventHandler`,
  `CompletableEventReaper`, `FetchCollector`), registers the
  state-notifier on the membership manager, and spawns the bg task.
- **Factory swap.** `new_consumer()`'s `GroupProtocol::Consumer` arm
  now flows through `AsyncKafkaConsumer::new`. The
  `GroupProtocol::Classic` arm continues to return
  `unsupported_version`.
- **State-notifier single-source-of-truth.** `AsyncKafkaConsumerComponents`
  carries `group_metadata`, `group_assignment_snapshot`, and
  `state_notifier` as mandatory Arcs; the SAME Arcs are registered on
  the membership manager AND stored on the consumer struct (Issue 2
  resolution).
- **Shared `Arc<AtomicI64>` for `max_time_to_wait_ms`.** Bg-task writes
  via `cached_max_time_to_wait_ms.store(...)`; app-side reads via
  `maximum_time_to_wait_ms()` accessor (Issue 3 resolution).
- **Smoke test (docker-free).** `tests/consumer/async_kafka_consumer_test.rs`
  exercises the production ctor against a refused-connection
  `127.0.0.1:1` peer. Runtime: ~100ms (smoke test does NOT take 30s —
  Critic Issue 9 ruled moot by measurement).
- **4 integration tests written but `#[ignore]`-gated at end of
  Phase 12.** `tests/integration/consumer_test.rs` carries
  `test_subscribe_and_poll_records`,
  `test_assign_partitions_and_poll`,
  `test_commit_sync_then_resume_in_same_group`,
  `test_seek_to_beginning_re_reads_records`. Each was `#[ignore]`d on
  the response-routing gap (see RESPONSE-ROUTING-AUDIT.md) at
  Phase-12 close. **Phase 12.5 has since shipped and un-ignored all
  four** — they pass end to end against a real broker.

### Phase 12.5 — SHIPPED

`design/history/Milestone-8/Phase-12/RESPONSE-ROUTING-AUDIT.md` audited
all 6 `RequestManager`s and identified **4 BROKEN** ones whose
`UnsentRequest` build sites did NOT call
`take_response_receiver()`, so broker responses were dropped silently:

- `coordinator_request_manager.rs:240`
- `consumer_heartbeat_request_manager.rs:278`
- `fetch_request_manager.rs:266`
- `topic_metadata_request_manager.rs:344`

The other 2 (`commit_request_manager`, `offsets_request_manager`) were
already WIRED. The gap was structural carry-over from Phase 10.

**Phase 12.5 CLOSED** (`design/history/Milestone-8/Phase-12.5/PLAN.md`):
all four BROKEN RMs now use either the canonical `Arc<Mutex<Inner>>`
interior-mutability pattern (`coordinator_request_manager`,
`topic_metadata_request_manager`) or an mpsc channel-back pattern
where the RM has async transitions the sync `poll(now)` can't drive
inline (`consumer_heartbeat_request_manager` for
`transition_to_fenced/_fatal`; `fetch_request_manager` for response
dispatch via the bg-task pending-set). The 4 integration tests are
un-ignored and run green end to end.

Final shape: 14 commits (`ba37a51..9662a77`), ~600 LOC production +
~300 LOC tests, 4 Critic review rounds all closed (see
`Phase-12.5/COMMENTS.DONE.1.md`).

### Carry-overs (NOT in Phase 12 or Phase 12.5 scope)

- `AsyncConsumerMetrics` / `KafkaConsumerMetrics` translation —
  deferred Milestone-8-wide.
- SSL/SASL `ChannelBuilder` escape hatch — Phase 12/12.5 ship
  PLAINTEXT only.
- C FFI for `AsyncKafkaConsumer` — out of milestone.
- Classic-protocol path — out of milestone per consumer-threading.md
  §20.

### Phase-11 deferred unit tests

The 5 markers listed at `PLAN.md:148-166` were re-examined at
Phase-12 close-out (commit 7/N). Each Phase-11 deferral was confirmed
to depend on response routing (Phase 12.5), not on the production
ctor itself. Markers at end of Phase 12 pointed at Phase 12.5.
**With Phase 12.5 CLOSED**, the response-routing dependency is met
and these unit tests can be revisited in a future milestone — the bg
task now drives `FindCoordinator` → `Heartbeat` → `Fetch` →
`OffsetCommit` end-to-end against a real broker, and the 4
`tests/integration/consumer_test.rs` tests prove the loop.
