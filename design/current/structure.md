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
├── common/                 # org.apache.kafka.common — 148 files, ~47 600 lines
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
├── producer/               # org.apache.kafka.clients.producer — 23 files, ~42 100 lines
│   ├── kafka_producer.rs       # KafkaProducer, incl. the transaction API
│   ├── mock_producer.rs        # MockProducer, incl. its transactional surface
│   ├── producer_trait.rs       # Producer trait (native async fn in trait)
│   ├── producer_config.rs, producer_record.rs, record_metadata.rs, callback.rs
│   └── internals/              # pub(crate)
│       ├── record_accumulator.rs   # Batching, per-partition deques, sequence assignment
│       ├── sender.rs               # Drain loop, in-flight batches, transactional loop
│       ├── producer_batch.rs, buffer_pool.rs, built_in_partitioner.rs
│       ├── transaction_manager.rs            # Milestone 11 — idempotence + txn state machine
│       ├── transactional_request_result.rs   # Milestone 11
│       ├── txn_partition_entry.rs            # Milestone 11
│       ├── txn_partition_map.rs              # Milestone 11
│       └── producer_test_utils.rs            # Milestone 11, #[cfg(test)] — ProducerTestUtils
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
## Admin module (Milestone 11 Tier 1 Phase 1 — org.apache.kafka.clients.admin)

> The tree above is stale (it predates the Producer, Consumer and Admin work).
> The Admin module added in Milestone 11 Phase 1:

```
src/admin/
├── mod.rs                    # Admin trait + new_admin_client() factory
├── admin_client_config.rs    # AdminClientConfig
├── kafka_admin_client.rs     # KafkaAdminClient (owns NetworkClient + bg task)
├── mock_admin_client.rs      # MockAdminClient (in-memory fake)
├── config.rs, config_entry.rs
├── new_topic.rs, topic_listing.rs, topic_description.rs
├── create_topics_result.rs, delete_topics_result.rs,
│   list_topics_result.rs, describe_topics_result.rs
├── options/                  # {create,delete,list,describe}_topics_options.rs
└── internals/
    ├── call.rs               # Call + NodeProvider retry engine
    ├── admin_metadata_manager.rs
    ├── admin_utils.rs
    └── admin_client_runnable.rs   # single tokio::spawn background task (AdminClientRunnable<C>)

# New common/wire types added this phase:
src/common/acl/{acl_operation.rs, acl_permission_type.rs}   # AclOperation, AclPermissionType (enums)
src/common/{topic_collection.rs, topic_partition_info.rs}
src/common/requests/{create_topics_request,create_topics_response,
                     delete_topics_request,delete_topics_response}.rs
tests/integration/admin_topics_test.rs                      # real-broker topic-CRUD tests
```

### Phase 2 additions — Partitions & records + AdminApiDriver engine

```
src/admin/
├── new_partitions.rs, records_to_delete.rs, deleted_records.rs
├── create_partitions_result.rs, delete_records_result.rs
├── options/{create_partitions_options.rs, delete_records_options.rs}
└── internals/          # multi-step lookup→fulfillment dispatch engine (pulled fwd from Phase 5)
    ├── admin_api_driver.rs          # AdminApiDriver
    ├── admin_api_handler.rs         # AdminApiHandler
    ├── admin_api_lookup_strategy.rs # AdminApiLookupStrategy
    ├── admin_api_future.rs          # AdminApiFuture
    ├── api_request_scope.rs         # ApiRequestScope (SingleLookup | Fulfillment)
    ├── partition_leader_strategy.rs # PartitionLeaderStrategy
    ├── partition_leader_cache.rs    # PartitionLeaderCache
    └── delete_records_handler.rs    # DeleteRecordsHandler

src/common/requests/{create_partitions_request,create_partitions_response,
                     delete_records_request,delete_records_response}.rs
tests/integration/admin_partitions_records_test.rs
```

### Phase 3 additions — Cluster & configs

```
src/admin/
├── alter_config_op.rs        # AlterConfigOp (+ OpType)
├── describe_cluster_result.rs, describe_configs_result.rs,
│   alter_configs_result.rs, list_config_resources_result.rs
└── options/{describe_cluster_options, describe_configs_options,
            alter_configs_options, list_config_resources_options}.rs

src/common/config/config_resource.rs                        # ConfigResource (+ resource-type enum)
src/common/requests/{describe_cluster_request,describe_cluster_response,
                     describe_configs_request,describe_configs_response,
                     incremental_alter_configs_request,incremental_alter_configs_response,
                     list_config_resources_request,list_config_resources_response}.rs
tests/integration/admin_cluster_configs_test.rs
```

### Phase 4 additions — Log dirs

```
src/admin/
├── log_dir_description.rs, replica_info.rs
├── describe_log_dirs_result.rs, alter_replica_log_dirs_result.rs,
│   describe_replica_log_dirs_result.rs
└── options/{describe_log_dirs_options, alter_replica_log_dirs_options,
            describe_replica_log_dirs_options}.rs

src/common/topic_partition_replica.rs                       # TopicPartitionReplica
src/common/requests/{describe_log_dirs_request,describe_log_dirs_response,
                     alter_replica_log_dirs_request,alter_replica_log_dirs_response}.rs
tests/integration/admin_log_dirs_test.rs                    # incl. real cross-dir move (2 KAFKA_LOG_DIRS)
```

### Phase 5 additions — Elections, reassignment, offsets (completes Tier 1)

```
src/admin/
├── offset_spec.rs, new_partition_reassignment.rs, partition_reassignment.rs
├── elect_leaders_result.rs, alter_partition_reassignments_result.rs,
│   list_partition_reassignments_result.rs, list_offsets_result.rs
├── options/{elect_leaders_options, alter_partition_reassignments_options,
│           list_partition_reassignments_options, list_offsets_options}.rs
└── internals/list_offsets_handler.rs   # ListOffsetsHandler on the AdminApiDriver engine

src/common/election_type.rs                                 # ElectionType
src/common/requests/{elect_leaders_request,elect_leaders_response,
                     alter_partition_reassignments_request,alter_partition_reassignments_response,
                     list_partition_reassignments_request,list_partition_reassignments_response}.rs
# (ListOffsetsRequest/Response reused from the Consumer module — not duplicated)
tests/integration/admin_elections_reassignments_offsets_test.rs  # incl. real 3-broker reassignment
```
