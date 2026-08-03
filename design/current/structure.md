# Project Structure

**Last verified:** 2026-08-03. 252 files / ~146 000 lines under `src/`.

Java package → Rust module mapping follows CLAUDE.md §2: `clients` never appears
in a path (`org.apache.kafka.clients.producer` → `producer`), and any package
containing `internal` is `pub(crate)` only.

```
src/
├── lib.rs                  # Crate root; includes the generated message types
│
├── *.rs                    # org.apache.kafka.clients — the client layer
│   ├── network_client.rs       # NetworkClient: connection mgmt, request dispatch
│   ├── kafka_client.rs         # KafkaClient trait
│   ├── metadata.rs             # Metadata cache + epoch tracking
│   ├── api_versions.rs         # Per-node API version registry
│   ├── in_flight_requests.rs   # In-flight request tracking
│   ├── cluster_connection_states.rs
│   ├── fetch_session_handler.rs
│   └── ...                     # 22 files, ~14 000 lines
│
├── common/                 # org.apache.kafka.common — 139 files, ~43 900 lines
│   ├── protocol/               # Readable/Writable, Message, varint, ApiKeys, Errors
│   ├── requests/               # Request/response wrappers + dispatch enums
│   ├── network/                # Selector, KafkaChannel, TransportLayer,
│   │                           #   plaintext/SSL/SASL channel builders
│   ├── security/               # SecurityProtocol, SslFactory, SASL authenticator
│   ├── record/                 # DefaultRecordBatch, MemoryRecords, record iteration
│   ├── compress/               # gzip / snappy / lz4 / zstd
│   ├── serialization/          # Serializer / Deserializer traits and impls
│   ├── config/                 # SSL and SASL configuration
│   ├── internals/, memory/, feature/, header/, utils/
│   ├── kafka_error.rs          # KafkaError: is_retriable / is_fatal / txn_requires_abort
│   ├── kafka_future.rs, uuid.rs, cluster.rs, node.rs, topic_partition.rs
│   └── ...
│
├── producer/               # org.apache.kafka.clients.producer — 21 files, ~15 900 lines
│   ├── kafka_producer.rs       # KafkaProducer
│   ├── mock_producer.rs        # MockProducer
│   ├── producer_trait.rs       # Producer trait (native async fn in trait)
│   ├── producer_config.rs, producer_record.rs, record_metadata.rs, callback.rs
│   └── internals/              # pub(crate)
│       ├── record_accumulator.rs   # Batching, per-partition deques
│       ├── sender.rs               # Drain loop, in-flight batches
│       ├── producer_batch.rs, buffer_pool.rs, built_in_partitioner.rs
│       ├── transactional_request_result.rs   # Milestone 11
│       ├── txn_partition_entry.rs            # Milestone 11
│       └── txn_partition_map.rs              # Milestone 11
│
├── consumer/              # org.apache.kafka.clients.consumer — 64 files, ~64 500 lines
│   │                     # Largest module. KIP-848 protocol only (see
│   │                     # .claude/rules/consumer-threading.md §20)
│   ├── async_kafka_consumer.rs # AsyncKafkaConsumer
│   ├── mock_consumer.rs        # MockConsumer
│   ├── mod.rs                  # Consumer trait (#[async_trait])
│   ├── consumer_config.rs, consumer_record.rs, consumer_records.rs
│   ├── consumer_rebalance_listener.rs, offset_commit_callback.rs
│   └── internals/              # pub(crate)
│       ├── consumer_network_thread.rs      # The single background task
│       ├── consumer_membership_manager.rs  # KIP-848 membership state machine
│       ├── consumer_heartbeat_request_manager.rs
│       ├── commit_request_manager.rs, fetch_request_manager.rs
│       ├── offsets_request_manager.rs, coordinator_request_manager.rs
│       ├── fetch_buffer.rs, fetch_collector.rs, completed_fetch.rs
│       ├── subscription_state.rs, consumer_metadata.rs
│       └── events/                         # Application/background event plumbing
│
├── ffi/                  # C FFI (feature = "ffi") — 4 files, ~7 700 lines
│   ├── producer.rs, consumer.rs   # ~135 KB each
│   ├── common.rs                  # Shared completion/dispatch/error machinery
│   └── mod.rs
│
└── bin/                  # message_generator, consumer_test

generator/                # Code generator: 197 JSON specs → Rust message types
├── messages/                 # 197 production specs
├── test-messages/            # 3 test-only specs
└── src/                      # The generator itself

build.rs                  # Runs the generator before compilation
xtask/                    # Rust task runner (format, lint, coverage, perf)

bindings/
├── c/                    # CMake build, Unity tests, gRPC server
└── python/               # CPython extension + sync/asyncio wrappers

multilanguage-test-server/  # gRPC protos shared by the Rust/Python/C++ harness
consumer-perf/             # E2E latency / CPU / RSS benchmark
tools/
├── translation_agent/     # Milestone 7: watches upstream Kafka, opens PRs
└── ...

kafka/                    # Java source submodule, pinned at 4.2.0 (a18251b)

tests/
├── main.rs + common/     # Protocol and generated-message tests
├── producer/             # Producer unit tests
├── consumer/             # Consumer unit tests
└── integration/          # --features integration-tests; needs Docker
```

## Related documents

| For | See |
|---|---|
| Milestone scope and progress | `design/history/MILESTONES.md` |
| Per-phase plans | `design/history/Milestone-N/**/PLAN.md` |
| Performance vs Java and librdkafka | `design/current/client-comparison-results.md` |
| Architecture of the network/protocol layers | `design/current/design.md` (Milestones 1-3 only) |
| Consumer threading rules | `.claude/rules/consumer-threading.md` |
| Producer transaction rules | `.claude/rules/producer-transactions.md` |
