# Project Structure

> **Verified against the tree on 2026-08-13.** File counts below are
> `ls`-derived and will drift; the module *shape* is the durable part.
>
> Diagrams live in `img/` and are rendered from the sibling `.d2` sources with
> `make -C design/current d2`; the `.d2` is the source of truth, so never
> hand-edit an `.svg`. `*.svg` is gitignored repo-wide, so run that command
> once in a fresh clone to make the images appear.

## Top level

```
src/                    # 566 .rs files, ~219k lines
├── lib.rs              # crate root: module list + generated-code inclusion
│
│   # === Client layer: Java's org.apache.kafka.clients, FLAT in src/ ===
│   # CLAUDE.md §2 forbids `clients` in a folder name, so these sit at the
│   # crate root rather than under a src/clients/ directory.
├── kafka_client.rs         # KafkaClient trait
├── network_client.rs       # NetworkClient<S, H>  (pub(crate))
├── network_client_utils.rs # (pub(crate))
├── metadata.rs, metadata_snapshot.rs, metadata_updater.rs,
│   metadata_recovery_strategy.rs
├── api_versions.rs, node_api_versions.rs
├── client_request.rs, client_response.rs, client_utils.rs
├── cluster_connection_states.rs, connection_state.rs
├── in_flight_requests.rs, least_loaded_node.rs, host_resolver.rs
├── fetch_session_handler.rs, common_client_configs.rs
├── mock_client.rs          # #[cfg(test)] only
├── test_alloc_tracker.rs   # #[cfg(test)] only — hot-path allocation budgets
│
├── admin/              # org.apache.kafka.clients.admin (see below)
├── consumer/           # org.apache.kafka.clients.consumer (see below)
├── producer/           # org.apache.kafka.clients.producer (see below)
├── ffi/                # C FFI, feature-gated on `ffi` (see below)
├── common/             # org.apache.kafka.common (see below)
└── bin/
    ├── consumer_test.rs
    └── message_generator.rs

generator/              # Code generation — was src/message/ in earlier drafts
├── messages/           # 197 production JSON RPC specs (Apache Kafka 4.2)
├── test-messages/      # 3 test-only specs
└── src/
    ├── lib.rs
    └── message/        # org.apache.kafka.message
        ├── code_buffer.rs, versions.rs, field_type.rs, entity_type.rs,
        ├── field_spec.rs, struct_spec.rs, message_spec.rs,
        ├── message_spec_type.rs, request_listener_type.rs
        └── schema_generator.rs

build.rs                # invokes the generator before compilation
xtask/                  # format, format-check, check-generated, lint, lint-fix,
                        # coverage{,-lcov,-all}, test-multilanguage,
                        # producer-perf-test
bindings/               # C and Python bindings (see below)
consumer-perf/          # consumer benchmark harness (workspace member)
multilanguage-test-server/  # gRPC server backing the multilanguage tests
kafka/                  # Java source reference (Apache Kafka 4.2)
design/{current,history}/
```

> **Three paths that do not exist**, though documents and commit messages in
> this repository still name them: `src/clients/` (its contents are the flat
> files listed above — CLAUDE.md §2 forbids the folder name), `src/message/`
> and a top-level `message-specs/` directory (both live in the `generator/`
> crate now: `generator/src/message/` and `generator/messages/`).

![Layered network stack](img/network-stack.svg)

*The layer boundaries still match Java's; only the client layer (flattened
into `src/`) and code generation (moved into `generator/`) changed location.*

![Code generation pipeline](img/codegen-pipeline.svg)

*Nothing under `generated/` is checked in — `build.rs` rebuilds it from the
JSON specs, which is why the specs and the generator, not the output, are the
things to read.*

## common/ — org.apache.kafka.common

```
src/common/
├── protocol/       # Readable, Writable, ByteBufferAccessor, Message,
│                   # ApiMessage, varint, api_keys.rs, errors.rs (135 codes),
│                   # message_size_accumulator.rs, object_serialization_cache.rs,
│                   # message_util.rs
├── requests/       # 107 files: RequestBuilder trait, ConcreteRequest /
│                   # ConcreteResponse (48 API variants), RequestHeader,
│                   # ResponseHeader, SendBuilder, one file per wire type
├── network/        # 27 files: Selector, Selectable, KafkaChannel,
│                   # TransportLayer (+ plaintext / ssl), ChannelBuilder
│                   # (+ plaintext / ssl / sasl), channel_builders.rs,
│                   # send.rs, receive.rs, network_send.rs, network_receive.rs,
│                   # byte_buffer_send.rs, mock_selector.rs
├── security/       # auth/ (SecurityProtocol, KafkaPrincipal),
│                   # ssl/ (SslFactory), authenticator/, token/delegation/,
│                   # scram/
├── config/         # ssl_configs, sasl_configs, ssl_client_auth,
│                   # config_resource
├── record/         # record batches, compression framing
├── compress/       # gzip / snappy / lz4 / zstd
├── serialization/  # Serializer<T>, Deserializer<T> (sync, &[u8])
├── metrics/        # Metrics, Sensor, KafkaMetric, MetricName
├── acl/            # AclOperation, AclPermissionType, AclBinding(+Filter),
│                   # AccessControlEntry(+Filter/Data)
├── resource/       # Resource, ResourcePattern(+Filter), PatternType,
│                   # ResourceType
├── quota/          # ClientQuotaEntity, ClientQuotaFilter(+Component),
│                   # ClientQuotaAlteration
├── feature/, header/, memory/, internals/, utils/
└── node.rs, cluster.rs, topic_partition.rs, topic_id_partition.rs,
    topic_partition_info.rs, topic_partition_replica.rs,
    topic_collection.rs, uuid.rs, kafka_error.rs, kafka_future.rs,
    partition_info.rs, isolation_level.rs, election_type.rs,
    group_state.rs, group_type.rs, classic_group_state.rs,
    consumer_group_state.rs, cluster_resource{,_listener}.rs,
    metric{,_name,_name_template}.rs
```

![Error types](img/error-types.svg)

*Java's exception hierarchy is flattened into one `KafkaError` enum; the
`Errors` code underneath answers every retriable/fatal question.*

## producer/ — org.apache.kafka.clients.producer

```
src/producer/
├── mod.rs                  # module re-exports
├── producer_trait.rs       # Producer<K, V> trait (#[async_trait])
├── kafka_producer.rs       # KafkaProducer<K, V>
├── mock_producer.rs        # MockProducer
├── producer_config.rs, producer_record.rs, record_metadata.rs, callback.rs
└── internals/
    ├── record_accumulator.rs   # per-partition batch deques
    ├── producer_batch.rs, incomplete_batches.rs, buffer_pool.rs
    ├── built_in_partitioner.rs
    ├── future_record_metadata.rs, produce_request_result.rs
    ├── producer_metadata.rs
    └── sender.rs               # the producer background task
```

![Producer send path](img/producer-send-path.svg)

*The app task only appends into the accumulator and returns a future; all
network work happens on the `Sender` task.*

## consumer/ — org.apache.kafka.clients.consumer

```
src/consumer/               # 19 files + 48 internals + 9 events
├── mod.rs                  # Consumer<K, V> trait + new_consumer() factory
├── async_kafka_consumer.rs # AsyncKafkaConsumer<K, V> + ConsumerHandle
├── mock_consumer.rs        # MockConsumer<K, V>
├── consumer_config.rs, consumer_record.rs, consumer_records.rs,
│   consumer_group_metadata.rs, consumer_rebalance_listener.rs,
│   consumer_rebalance_listener_method_name.rs, offset_commit_callback.rs,
│   offset_and_metadata.rs, offset_and_timestamp.rs,
│   offset_reset_strategy.rs, group_protocol.rs, close_options.rs,
│   subscription_pattern.rs, interceptor.rs, errors.rs,
│   consumer_partition_assignor.rs   # Assignment/Subscription holders only
└── internals/
    ├── consumer_network_thread.rs   # the background task (run_once)
    ├── network_client_delegate.rs   # the bg task's handle on NetworkClient
    ├── request_managers.rs, request_manager.rs
    ├── coordinator_request_manager.rs, commit_request_manager.rs
    ├── consumer_heartbeat_request_manager.rs,
    │   abstract_heartbeat_request_manager.rs, heartbeat_request_state.rs
    ├── consumer_membership_manager.rs, abstract_membership_manager.rs,
    │   member_state.rs, member_state_listener.rs
    ├── fetch_request_manager.rs, abstract_fetch.rs, fetch_buffer.rs,
    │   fetch_collector.rs, completed_fetch.rs, fetch_config.rs,
    │   fetch_utils.rs
    ├── offsets_request_manager.rs, offset_fetcher_utils.rs,
    │   offsets_for_leader_epoch_client.rs, offset_and_timestamp_internal.rs
    ├── topic_metadata_request_manager.rs, consumer_metadata.rs
    ├── subscription_state.rs, auto_offset_reset_strategy.rs
    ├── consumer_rebalance_listener_invoker.rs,
    │   offset_commit_callback_invoker.rs
    ├── consumer_protocol.rs         # decodes classic members' assignment
    │                                # bytes for the admin group-describe path
    ├── deserializers.rs, consumer_interceptors.rs, consumer_utils.rs
    ├── wakeup_trigger.rs, request_state.rs, timed_request_state.rs
    ├── async_consumer_metrics.rs, kafka_consumer_metrics.rs,
    │   fetch_metrics_manager.rs, fetch_metrics_aggregator.rs,
    │   fetch_metrics_registry.rs, heartbeat_metrics_manager.rs,
    │   offset_commit_metrics_manager.rs,
    │   consumer_rebalance_metrics_manager.rs,
    │   rebalance_callback_metrics_manager.rs, sensor_builder.rs
    └── events/
        ├── application_event.rs, application_event_handler.rs,
        │   application_event_processor.rs
        ├── background_event.rs, background_event_handler.rs
        ├── completable_event.rs, completable_event_reaper.rs
        └── event_processor.rs
```

![Consumer background-task loop](img/consumer-bg-loop.svg)

*`run_once()` phase order is Java's `runOnce()`; phase 4 is a plain
`.await` that wakeups interrupt via the selector's `Notify` rather than
cancelling.*

![Consumer event model](img/consumer-event-model.svg)

*Rebalance listeners and commit callbacks always run inline on the app
task; the bg loop gates only the membership transition on their ack.*

![Consumer receive path](img/consumer-receive-path.svg)

*`fetch_buffer.rs` / `completed_fetch.rs` / `fetch_collector.rs` are one
pipeline over a single owned buffer — read them in that order.*

## ffi/ and bindings/

```
src/ffi/                # feature-gated: --features ffi
├── mod.rs
├── common.rs           # kafka_common_KafkaError_t (5 exported symbols)
│                       # + the callback dispatcher: CompletionJob,
│                       # spawn_dispatcher, enqueue_or_run_inline
├── producer.rs         # 34 exported symbols (7 of them *_async)
└── consumer.rs         # 149 exported symbols (20 of them *_async)

bindings/
├── c/
│   ├── CMakeLists.txt, Makefile, Dockerfile.grpc
│   ├── tests/{test_kafka_producer,test_kafka_consumer,test_mock_producer,
│   │          test_mock_consumer,producer_perf_test}.c + unity/ (submodule)
│   └── grpc_server/     # C backend for the multilanguage tests
└── python/
    ├── producer.py, consumer.py
    ├── _confluentkafka.c   # hand-written C extension
    ├── grpc_server.py, grpc_server_async.py, grpc_translate.py
    └── setup.py, pyproject.toml
```

There is **no Admin FFI**: `grep -r kafka_admin_ src/ffi/` returns nothing.

Both bindings expose each operation twice: a synchronous entry point that
`block_on`s the async API on the runtime the handle owns, and an `*_async`
one that takes a `*_callback_t` + `user_data`, spawns the future and delivers
the result through a dispatcher thread (`src/ffi/consumer.rs:155-199` for the
handle fields; `spawn_dispatcher` / `enqueue_or_run_inline` in
`src/ffi/common.rs:214-241`). 20 of the consumer's 149 exported symbols and 7
of the producer's 34 are these `*_async` variants; the rest are the sync forms,
the accessors and the destructors.

## tests/

```
tests/                  # 69 .rs files across 5 cargo test targets
├── main.rs             # target `common`
├── common/             # shared infrastructure + common-module tests
│   ├── mod.rs                  # re-exports library types for test-generated messages
│   ├── cluster_config.rs       # ClusterConfig, SecurityMode,
│   │                           # authorizer_single_broker() fixture
│   ├── cluster_pool.rs         # process-global pool of shared containers
│   ├── kafka_cluster.rs        # Docker wrapper + SecureKafka custom image
│   ├── backend_factory.rs, backend_pool.rs
│   ├── multilanguage_{producer,consumer}.rs + *_test_macro.rs
│   ├── test_certs.rs, test_context.rs, test_utils.rs
│   ├── message/    # mod.rs + message_test.rs, simple_example_message_test.rs,
│   │               # nullable_struct_message_test.rs,
│   │               # simple_arrays_message_test.rs, generated_messages_test.rs,
│   │               # message_round_trip_test.rs, message_serialization_test.rs
│   └── protocol/   # mod.rs + flexible_version_test.rs, tagged_fields_test.rs
├── producer/           # target `producer`: main.rs + mock_producer_test.rs
├── consumer/           # target `consumer`: async_kafka_consumer_test.rs,
│                       # mock_consumer_test.rs, trait_surface_check.rs,
│                       # consumer_config_test.rs, consumer_record(s)_test.rs,
│                       # offset_and_metadata_test.rs, close_options_test.rs,
│                       # auto_offset_reset_strategy_test.rs,
│                       # consumer_group_metadata_test.rs
├── performance/        # target `performance`, `test = false` in Cargo.toml —
│                       # excluded from plain `cargo test`; run explicitly
└── integration/        # 28 test files + main.rs, Docker required
    ├── connection_test.rs, api_versions_test.rs, metadata_test.rs,
    │   ssl_sasl_test.rs
    ├── producer_test.rs
    ├── consumer_test.rs, consumer_topic_creation_test.rs,
    │   plaintext_consumer{,_assign,_callback,_commit,_fetch,_poll,
    │   _subscription}_test.rs, sasl_ssl_consumer_test.rs,
    │   multilanguage_consumer_test.rs
    └── admin_{topics,partitions_records,cluster_configs,log_dirs,
        elections_reassignments_offsets,groups,group_offsets,acls,quotas,
        features,transactions,scram}_test.rs
```

The per-message-type test directory uses `mod.rs`, not `main.rs`: the
single entry point for that target is `tests/main.rs`.

Four of the five targets are declared in `Cargo.toml`; `integration` is not —
Cargo auto-discovers `tests/integration/main.rs`. That file is guarded by
`#![cfg(feature = "integration-tests")]`, so the target still compiles (to
nothing) without the feature.

---

# Admin module (org.apache.kafka.clients.admin)

**88 top-level files + 31 `internals/` + 47 `options/`**, covering 46 RPCs.
The layout is regular: for each RPC there is one `*_result.rs` at the top
level, one `options/*_options.rs`, and — if the RPC needs a lookup before it
can be sent — one `internals/*_handler.rs`. The listings below group the files
by what they administer.

The 46 RPCs, in `Admin` trait declaration order:
`create_topics`, `delete_topics`, `list_topics`, `describe_topics`,
`create_partitions`, `delete_records`, `describe_producers`,
`abort_transaction`, `describe_transactions`, `fence_producers`,
`list_transactions`, `force_terminate_transaction`, `describe_cluster`,
`describe_configs`, `incremental_alter_configs`, `list_config_resources`,
`list_client_metrics_resources`, `describe_log_dirs`, `alter_replica_log_dirs`,
`describe_replica_log_dirs`, `elect_leaders`, `alter_partition_reassignments`,
`list_partition_reassignments`, `list_offsets`, `list_groups`,
`list_consumer_groups`, `describe_consumer_groups`, `describe_classic_groups`,
`list_consumer_group_offsets`, `alter_consumer_group_offsets`,
`delete_consumer_group_offsets`, `delete_consumer_groups`,
`remove_members_from_consumer_group`, `create_acls`, `describe_acls`,
`delete_acls`, `describe_client_quotas`, `alter_client_quotas`,
`describe_user_scram_credentials`, `alter_user_scram_credentials`,
`create_delegation_token`, `renew_delegation_token`,
`expire_delegation_token`, `describe_delegation_token`, `describe_features`,
`update_features` — plus `async fn close`.

## Core: trait, client, dispatch

```
src/admin/
├── mod.rs                    # Admin trait (46 sync fn + async close)
│                             # + new_admin_client() factory
├── admin_client_config.rs    # AdminClientConfig (no security settings)
├── kafka_admin_client.rs     # KafkaAdminClient (owns NetworkClient + bg task)
├── mock_admin_client.rs      # MockAdminClient (in-memory fake)
└── internals/
    ├── call.rs                     # Call + NodeProvider + MaybeRetryOutcome
    ├── admin_metadata_manager.rs   # cluster / controller / backoff state
    ├── admin_utils.rs
    ├── admin_client_runnable.rs    # the single tokio::spawn background task
    │                               # AdminClientRunnable<C: KafkaClient>
    ├── admin_api_driver.rs         # lookup -> fulfillment engine
    ├── admin_api_handler.rs, admin_api_lookup_strategy.rs,
    │   admin_api_future.rs, api_request_scope.rs
    └── partition_leader_strategy.rs, partition_leader_cache.rs,
        coordinator_strategy.rs, coordinator_key.rs,
        all_brokers_strategy.rs, static_broker_strategy.rs
```

The `Call` path serves RPCs whose node is known up front; the driver path
serves the ones that must resolve a partition leader, a group/transaction
coordinator, or the broker set first. Both run on the one background task.

## Topics, partitions, records

```
src/admin/
├── new_topic.rs, topic_listing.rs, topic_description.rs,
│   new_partitions.rs, records_to_delete.rs, deleted_records.rs
├── {create,delete,list,describe}_topics_result.rs,
│   create_partitions_result.rs, delete_records_result.rs
├── options/{create,delete,list,describe}_topics_options.rs,
│           create_partitions_options.rs, delete_records_options.rs
└── internals/delete_records_handler.rs        # PartitionLeaderStrategy

src/common/topic_collection.rs, src/common/topic_partition_info.rs
src/common/requests/{create_topics,delete_topics,create_partitions,
                     delete_records}_{request,response}.rs
tests/integration/admin_{topics,partitions_records}_test.rs
```

`describe_topics` (by name and by id) goes through the Metadata API, mirroring
Java's `generateDescribeTopicsCallWithMetadataApi` / `handleDescribeTopicsByIds`
rather than `DescribeTopicPartitions` cursor pagination.

## Cluster, configs, log dirs

```
src/admin/
├── config.rs, config_entry.rs, alter_config_op.rs
├── log_dir_description.rs, replica_info.rs
├── describe_cluster_result.rs, describe_configs_result.rs,
│   alter_configs_result.rs, list_config_resources_result.rs,
│   describe_log_dirs_result.rs, alter_replica_log_dirs_result.rs,
│   describe_replica_log_dirs_result.rs
└── options/{describe_cluster,describe_configs,alter_configs,
            list_config_resources,describe_log_dirs,alter_replica_log_dirs,
            describe_replica_log_dirs}_options.rs

src/common/config/config_resource.rs, src/common/topic_partition_replica.rs
src/common/acl/{acl_operation.rs, acl_permission_type.rs}   # authorized_operations
src/common/requests/{describe_cluster,describe_configs,
                     incremental_alter_configs,list_config_resources,
                     describe_log_dirs,alter_replica_log_dirs}_{request,response}.rs
tests/integration/admin_{cluster_configs,log_dirs}_test.rs
```

All on the plain `Call` path, but not all to the same node: config requests are
routed per resource type (broker / broker-logger resources to that broker,
everything else to the controller or least-loaded node), and log-dir requests
fan out per broker. `describe_replica_log_dirs` is built on
`DescribeLogDirsRequest` and reshaped into current/future-dir `ReplicaLogDirInfo`.
The log-dirs integration suite drives a real cross-directory replica move
against a broker fixture with two `KAFKA_LOG_DIRS`.

## Elections, reassignments, offsets

```
src/admin/
├── offset_spec.rs, new_partition_reassignment.rs, partition_reassignment.rs
├── elect_leaders_result.rs, alter_partition_reassignments_result.rs,
│   list_partition_reassignments_result.rs, list_offsets_result.rs
├── options/{elect_leaders,alter_partition_reassignments,
│           list_partition_reassignments,list_offsets}_options.rs
└── internals/list_offsets_handler.rs          # PartitionLeaderStrategy

src/common/election_type.rs
src/common/requests/{elect_leaders,alter_partition_reassignments,
                     list_partition_reassignments}_{request,response}.rs
# ListOffsets{Request,Response} is the consumer module's wrapper, reused
tests/integration/admin_elections_reassignments_offsets_test.rs
```

## Groups and group offsets

```
src/admin/
├── group_listing.rs, consumer_group_listing.rs,
│   consumer_group_description.rs, classic_group_description.rs,
│   member_description.rs, member_assignment.rs, member_to_remove.rs,
│   list_consumer_group_offsets_spec.rs
├── {list_groups,list_consumer_groups,describe_consumer_groups,
│    describe_classic_groups,list_consumer_group_offsets,
│    alter_consumer_group_offsets,delete_consumer_group_offsets,
│    delete_consumer_groups,remove_members_from_consumer_group}_result.rs
└── internals/                                  # all CoordinatorStrategy
    ├── describe_consumer_groups_handler.rs, describe_classic_groups_handler.rs
    ├── list_consumer_group_offsets_handler.rs,
    │   alter_consumer_group_offsets_handler.rs,
    │   delete_consumer_group_offsets_handler.rs
    └── delete_groups_handler.rs, delete_consumer_groups_handler.rs,
        remove_members_from_consumer_group_handler.rs

src/common/{group_state.rs, group_type.rs, classic_group_state.rs,
            consumer_group_state.rs}
src/consumer/internals/consumer_protocol.rs   # decodes a classic member's
src/consumer/consumer_partition_assignor.rs   # assignment bytes (data holders
                                              # only; no assignor trait)
src/common/requests/{list_groups,consumer_group_describe,describe_groups,
                     delete_groups,leave_group,offset_delete}_{request,response}.rs
tests/integration/admin_{groups,group_offsets}_test.rs
```

## ACLs, quotas, SCRAM, tokens, features, transactions, client metrics

```
src/admin/
├── {create,describe,delete}_acls_result.rs
├── {describe,alter}_client_quotas_result.rs
├── {describe,alter}_user_scram_credentials_result.rs,
│   scram_credential_info.rs, scram_mechanism.rs,
│   user_scram_credential_{alteration,deletion,upsertion}.rs,
│   user_scram_credentials_description.rs
├── {create,renew,expire,describe}_delegation_token_result.rs
├── feature_metadata.rs, feature_update.rs, finalized_version_range.rs,
│   supported_version_range.rs, {describe,update}_features_result.rs
├── producer_state.rs, transaction_state.rs, transaction_description.rs,
│   transaction_listing.rs, abort_transaction_spec.rs,
│   {describe_producers,describe_transactions,abort_transaction,
│    fence_producers,list_transactions,terminate_transaction}_result.rs
├── client_metrics_resource_listing.rs,
│   list_client_metrics_resources_result.rs
└── internals/{describe_producers,describe_transactions,abort_transaction,
              fence_producers,list_transactions}_handler.rs

src/common/acl/{acl_binding.rs, acl_binding_filter.rs,
                access_control_entry{,_filter,_data}.rs}
src/common/resource/{resource.rs, resource_pattern{,_filter}.rs,
                     pattern_type.rs, resource_type.rs}
src/common/quota/{client_quota_entity, client_quota_filter,
                  client_quota_filter_component, client_quota_alteration}.rs
src/common/security/{auth/kafka_principal.rs,
                     token/delegation/{delegation_token,token_information}.rs,
                     scram/internals/{scram_formatter,scram_mechanism}.rs}
tests/integration/admin_{acls,quotas,scram,features,transactions}_test.rs
```

`list_client_metrics_resources` reuses the `ListConfigResources` wire type
rather than adding one. The transaction RPCs are the users of
`AllBrokersStrategy` and `StaticBrokerStrategy`. `force_terminate_transaction`
is the one RPC whose files are named for the Java `terminateTransaction`
spelling (`terminate_transaction_{result,options}.rs`).

Because `AdminClientConfig` carries no security settings, the delegation-token
and SCRAM integration suites can create and describe credentials but cannot
then authenticate with them.

![Component & ownership structure](img/component-ownership.svg)

*Each client owns its own background task and its own `NetworkClient`;
nothing is shared between a producer, a consumer and an admin client.*

![Admin dispatch patterns](img/admin-dispatch.svg)

*Which `internals/` file you need depends on this fork: a plain `Call` needs
no handler, a lookup-first RPC has one `*_handler.rs` plus a strategy.*
