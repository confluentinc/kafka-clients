# Milestone 8: AsyncKafkaConsumer (KIP-848)

## Goal

Translate the Java `AsyncKafkaConsumer<K, V>` and its dependency closure from
`org.apache.kafka.clients.consumer.*` (Apache Kafka, pinned commit
`a18251bae0b825c69794a50dffd4c3100cf5ca5b` — `kafka/` submodule HEAD) to Rust,
exposing a `Box<dyn Consumer<K, V>>` public API governed by
[`consumer-threading.md`](../../../.claude/rules/consumer-threading.md).

## Scope

**In scope (per `consumer-threading.md` §20):**

- Public API: `Consumer` trait, `AsyncKafkaConsumer`, `MockConsumer`,
  `ConsumerConfig`, `ConsumerRecord`, `ConsumerRecords`, `ConsumerGroupMetadata`,
  `ConsumerRebalanceListener`, `OffsetCommitCallback`, `OffsetAndMetadata`,
  `OffsetAndTimestamp`, `OffsetResetStrategy`, `GroupProtocol`, `CloseOptions`,
  `SubscriptionPattern`, consumer exception hierarchy
  (`CommitFailedError`, `InvalidOffsetError`, `LogTruncationError`,
  `NoOffsetForPartitionError`, `OffsetOutOfRangeError`,
  `RetriableCommitFailedError`).
- Internals: `SubscriptionState`, `ConsumerMetadata`, `Deserializers`,
  `ConsumerInterceptors`, `AutoOffsetResetStrategy`,
  `ConsumerRebalanceListenerInvoker`, `WakeupTrigger` (rotating
  `CancellationToken` per §11), all KIP-848 request managers (`Coordinator`,
  `Commit`, `Fetch`, `Heartbeat`, `Offsets`, `TopicMetadata`),
  `MembershipManager`, `NetworkClientDelegate`, `ApplicationEventHandler`,
  `BackgroundEventHandler`, `ApplicationEventProcessor`,
  `CompletableEventReaper`, all events under `internals/events/`, `Fetcher`,
  `FetchBuffer`, `FetchCollector`, `CompletedFetch`, `FetchConfig`,
  `OffsetFetcher`, `ConsumerNetworkThread`.

**Out of scope (deferred to future milestones):**

- `ClassicKafkaConsumer`, `ConsumerCoordinator`, `AbstractCoordinator`,
  `ConsumerNetworkClient`, `LegacyKafkaConsumer`, `BaseHeartbeatThread`.
- Classic-protocol assignors: `AbstractPartitionAssignor`, `RangeAssignor`,
  `RoundRobinAssignor`, `StickyAssignor`, `AbstractStickyAssignor`,
  `CooperativeStickyAssignor`, `ConsumerProtocol`, `ConsumerPartitionAssignor`
  trait.
- `ConsumerDelegate`, `ConsumerDelegateCreator` (collapses per `consumer-threading.md` §2).
- All share-consumer files (KIP-932): `KafkaShareConsumer`, `MockShareConsumer`,
  `ShareConsumer`, `ShareConsumerConfig`, `Acknowledge*`,
  `ShareCompletedFetch`, `ShareConsumeRequestManager`, related tests.
- C FFI for consumer (separate milestone).

## Java source root

`kafka/clients/src/main/java/org/apache/kafka/clients/consumer/` (production) and
`kafka/clients/src/test/java/org/apache/kafka/clients/consumer/` (tests), at
submodule commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

## Phases

| # | Phase | Java sources (LOC) | Rust output | Depends on |
|---|---|---|---|---|
| 1 | **Public foundation types** | `ConsumerConfig` (815), `ConsumerRecord` (274), `ConsumerRecords` (147), `OffsetAndMetadata` (127), `OffsetAndTimestamp` (85), `OffsetResetStrategy` (33), `AutoOffsetResetStrategy` (182), `GroupProtocol` (43), `ConsumerGroupMetadata` (100), `CloseOptions` (113), `SubscriptionPattern` (59), 6 exception classes (~285). Tests: `ConsumerConfigTest` (295), `ConsumerRecordTest` (101), `ConsumerRecordsTest` (220), `OffsetAndMetadataTest` (83), `ConsumerGroupMetadataTest` (89), `CloseOptionsTest` (51), `AutoOffsetResetStrategyTest` (121). | `src/consumer/{consumer_config, consumer_record, consumer_records, offset_and_metadata, offset_and_timestamp, offset_reset_strategy, group_protocol, consumer_group_metadata, close_options, subscription_pattern, errors}.rs` + `src/consumer/internals/auto_offset_reset_strategy.rs` + matching `tests/consumer/` files. | — |
| 2 | **Public traits & deserializers** | `Consumer<K,V>` (interface), `ConsumerRebalanceListener`, `OffsetCommitCallback`, `Deserializer<T>`, `ConsumerInterceptor<K,V>`, `Deserializers`, `ConsumerInterceptors`. Tests: `ConsumerInterceptorsTest`, any `DeserializersTest` if present. | `src/consumer/mod.rs` (`Consumer` trait + factory), `consumer_rebalance_listener.rs`, `offset_commit_callback.rs`, `deserializer.rs`, `interceptor.rs`, `internals/{deserializers, consumer_interceptors}.rs` + matching tests | Phase 1 |
| 3 | **`MockConsumer`** | `MockConsumer.java` + `MockConsumerTest.java` (192) | `src/consumer/mock_consumer.rs` + `tests/consumer/mock_consumer_test.rs` | Phase 1–2 |
| 4 | **`SubscriptionState` + `ConsumerMetadata`** | `SubscriptionState` (1323) + `SubscriptionStateTest` (970), `ConsumerMetadata` (99) + `ConsumerMetadataTest` | `src/consumer/internals/{subscription_state, consumer_metadata}.rs` + tests | Phase 1 |
| 5 | **Event channels, wakeup, reaper** | All `consumer/internals/events/` (~25 files), `WakeupTrigger`, `CompletableEventReaper`. Tests: `WakeupTriggerTest`, `CompletableEventReaperTest`, event-type tests. | `src/consumer/internals/events/` (`ApplicationEvent`/`BackgroundEvent` enums, `ApplicationEventHandler`, `BackgroundEventHandler`), `internals/{wakeup_trigger, completable_event_reaper}.rs` + tests | Phase 1, 4 |
| 6 | **`NetworkClientDelegate` + `RequestManager` + `CoordinatorRequestManager`** | `NetworkClientDelegate` (408), `RequestManager`, `RequestState`, `TimedRequestState`, `CoordinatorRequestManager`, `RequestManagers`. Tests: `NetworkClientDelegateTest`, `RequestStateTest`, `TimedRequestStateTest`, `CoordinatorRequestManagerTest`, `RequestManagersTest`. | `src/consumer/internals/{network_client_delegate, request_manager, request_state, timed_request_state, coordinator_request_manager, request_managers}.rs` + tests | Phase 1, 5 |
| 7 | **Fetch path** | `AbstractFetch` (497), `FetchConfig`, `FetchBuffer` (277), `CompletedFetch` (390), `FetchCollector` (393), `Fetcher` (210), `FetchRequestManager` (121), `OffsetFetcher`, `OffsetFetcherUtils`, `TopicMetadataFetcher`, `OffsetsRequestManager`, `OffsetsForLeaderEpochClient`, `TopicMetadataRequestManager`, `FetchMetricsAggregator/Manager/Registry`, `SensorBuilder` (subset for fetch). Tests: `FetcherTest` (~4K), `FetchBufferTest`, `FetchCollectorTest`, `CompletedFetchTest`, `FetchRequestManagerTest`, `OffsetsRequestManagerTest`, `TopicMetadataRequestManagerTest`, `OffsetFetcherTest`, `TopicMetadataFetcherTest`, plus the per-record allocation-budget test required by `consumer-threading.md` §27. | `src/consumer/internals/{fetch_config, fetch_buffer, completed_fetch, fetcher, fetch_collector, fetch_request_manager, offset_fetcher, offsets_request_manager, offsets_for_leader_epoch_client, topic_metadata_fetcher, topic_metadata_request_manager}.rs` + tests | Phase 4, 5, 6 |
| 8 | **Membership & heartbeat (KIP-848)** | `MembershipManager` interface + `MembershipManagerImpl` (1511), `MemberState`, `MemberStateListener`, `Heartbeat`, `HeartbeatRequestState`, `HeartbeatRequestManager` (629), `AbstractHeartbeatRequestManager`, `ConsumerHeartbeatRequestManager`. Tests: `MembershipManagerImplTest`, `HeartbeatRequestManagerTest`, `HeartbeatTest`. | `src/consumer/internals/{membership_manager, member_state, heartbeat, heartbeat_request_manager, consumer_heartbeat_request_manager}.rs` + tests | Phase 4, 5, 6 |
| 9 | **Commit** | `CommitRequestManager` (1282), `OffsetCommitCallbackInvoker`. Tests: `CommitRequestManagerTest`. | `src/consumer/internals/{commit_request_manager, offset_commit_callback_invoker}.rs` + tests | Phase 4, 5, 6 |
| 10 | **Background task & event processor** | `ConsumerNetworkThread` (321), `ApplicationEventProcessor`. Tests: `ConsumerNetworkThreadTest`, `ApplicationEventProcessorTest`. | `src/consumer/internals/{consumer_network_thread, application_event_processor}.rs`. Single `tokio::spawn` per consumer (§10) with `runOnce()` phase ordering; `select!` over shutdown/wakeup/network-poll. | Phase 5–9 |
| 11 | **`AsyncKafkaConsumer` public impl** | `AsyncKafkaConsumer.java` (2368), `ConsumerRebalanceListenerInvoker`, `ConsumerUtils`. Tests: `AsyncKafkaConsumerTest` (~2K) plus the two `ConsumerRebalanceListener` regression tests required by `consumer-threading.md` §31. | `src/consumer/async_kafka_consumer.rs` implementing `Consumer<K,V>`; `process_background_events` invocation in every blocking-style API (§31) | Phase 2, 3, 5, 7, 8, 9, 10 |
| 12 | **Integration test** — CLOSED [^p12] | analog of producer Phase 6 integration | `tests/integration/consumer_test.rs`: 4 end-to-end flows + production ctor wired through `new_consumer` factory. **Integration tests un-ignored in Phase 12.5** once the response-routing gap was closed — see footnote. | Phase 11 |
| 12.5 | **Response routing for the 4 BROKEN RMs** — CLOSED [^p12_5] | `CoordinatorRequestManager`, `TopicMetadataRequestManager`, `ConsumerHeartbeatRequestManager`, `FetchRequestManager` | per-RM `take_response_receiver` + spawned-task dispatch wired through. Heartbeat uses mpsc channel-back since `transition_to_fenced/_fatal` is async and `poll(now)` is sync. Integration tests un-ignored. | Phase 12 |

[^p12]: Phase 12 production ctor wired end-to-end. The 4 integration
tests (`tests/integration/consumer_test.rs`) were `#[ignore]`-gated
pending **Phase 12.5** which wired response routing for the 4 BROKEN
RequestManagers (coordinator, consumer_heartbeat, fetch,
topic_metadata) per `Phase-12/RESPONSE-ROUTING-AUDIT.md`. With
Phase 12.5 CLOSED, the gap is closed; integration tests are
un-ignored and run green end to end.

[^p12_5]: Phase 12.5 CLOSED with 14 commits (`ba37a51..9662a77`).
All four `tests/integration/consumer_test.rs` tests pass against
testcontainers Kafka 4.2.0. The structural Phase-10 carry-over
(per-RM `whenComplete` translation) is resolved: `Inner` types
are `Arc<Mutex<...>>` (`CoordinatorRequestManager`,
`TopicMetadataRequestManager`) or `Arc<Inner>` with interior
mutability + mpsc side-channel for async transitions
(`ConsumerHeartbeatRequestManager`), and `make_*_request` paths
take the response receiver and spawn dispatch tasks. Charter at
`Phase-12.5/PLAN.md`. Critic rounds 1-4 all closed; see
`Phase-12.5/COMMENTS.DONE.1.md`.

## Module structure

```
src/consumer/
├── mod.rs                       # Consumer<K,V> trait, new_consumer factory
├── async_kafka_consumer.rs
├── mock_consumer.rs
├── consumer_config.rs
├── consumer_record.rs
├── consumer_records.rs
├── consumer_group_metadata.rs
├── consumer_rebalance_listener.rs
├── offset_and_metadata.rs
├── offset_and_timestamp.rs
├── offset_commit_callback.rs
├── offset_reset_strategy.rs
├── group_protocol.rs
├── close_options.rs
├── subscription_pattern.rs
├── deserializer.rs
├── interceptor.rs
├── errors.rs                    # CommitFailedException → CommitFailedError, etc.
└── internals/                   # pub(crate) per CLAUDE.md §2
    ├── mod.rs
    ├── auto_offset_reset_strategy.rs
    ├── subscription_state.rs
    ├── consumer_metadata.rs
    ├── deserializers.rs
    ├── consumer_interceptors.rs
    ├── consumer_rebalance_listener_invoker.rs
    ├── wakeup_trigger.rs
    ├── completable_event_reaper.rs
    ├── network_client_delegate.rs
    ├── request_manager.rs
    ├── request_managers.rs
    ├── request_state.rs
    ├── timed_request_state.rs
    ├── coordinator_request_manager.rs
    ├── commit_request_manager.rs
    ├── offset_commit_callback_invoker.rs
    ├── heartbeat.rs
    ├── heartbeat_request_manager.rs
    ├── consumer_heartbeat_request_manager.rs
    ├── member_state.rs
    ├── membership_manager.rs
    ├── fetch_config.rs
    ├── fetch_buffer.rs
    ├── completed_fetch.rs
    ├── fetcher.rs
    ├── fetch_collector.rs
    ├── fetch_request_manager.rs
    ├── offset_fetcher.rs
    ├── offsets_request_manager.rs
    ├── offsets_for_leader_epoch_client.rs
    ├── topic_metadata_fetcher.rs
    ├── topic_metadata_request_manager.rs
    ├── consumer_network_thread.rs
    ├── application_event_processor.rs
    └── events/
        ├── mod.rs               # ApplicationEvent / BackgroundEvent enum trees
        ├── application_event_handler.rs
        └── background_event_handler.rs
```

## Workflow per phase

Per `.claude/rules/agent-roles.md`: each phase runs an Actor → Critic → fix
loop. Comment files live at
`design/history/Milestone-8/Phase-N/COMMENTS.<critic-id>.md`; resolved comments
are moved to `COMMENTS.DONE.<critic-id>.md`. All work lands on the
`consumer-impl` branch.

## Definition of Done (per phase)

Beyond `definition-of-done.md`:

- `cargo build`, `cargo test`, `cargo xtask format-check`, `cargo xtask lint` clean.
- **Unit tests for every class translated in the phase ship in the same
  phase** — Java test files listed in the phase row are translated and
  committed before the phase closes. Each skipped Java test method must
  carry a one-line rationale per DoD §3.
- Consumer trait surface check (DoD §11) verified on phases that touch the
  public trait (2, 3, 11).
- Receive-path zero-copy / per-record allocation-budget test (§27) lives
  with Phase 7.
- The two `ConsumerRebalanceListener` regression tests required by §31 live
  with Phase 11.

## Parallelism plan

Dependency graph allows real concurrency in two windows:

1. After Phase 1 lands: Phase 4 and Phase 5 can run in parallel (2 actors).
2. After Phase 6 lands: Phases 7, 8, 9 can run in parallel (3 actors). This
   is the biggest compression — these three carry the largest test loads
   (`FetcherTest` ~4K, `MembershipManagerImplTest`, `CommitRequestManagerTest`).

Phases 10, 11, 12 are serial bottlenecks.
