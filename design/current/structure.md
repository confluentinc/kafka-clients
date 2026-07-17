# Project Structure

```
src/
├── lib.rs              # Root library module with generated code inclusion
├── common/             # Common types (org.apache.kafka.common)
│   ├── mod.rs
│   ├── uuid.rs         # UUID type with base64 URL encoding
│   └── protocol/       # Wire protocol serialization (org.apache.kafka.common.protocol)
│       ├── mod.rs
│       ├── readable.rs             # Readable trait for deserialization
│       ├── writable.rs             # Writable trait for serialization
│       ├── byte_buffer_accessor.rs # ByteBufferAccessor implementation
│       └── varint.rs               # Varint/varlong encoding
└── message/            # Schema generation (org.apache.kafka.message)
    ├── mod.rs
    ├── code_buffer.rs  # CodeBuffer - buffered code generation
    ├── versions.rs     # Versions - version range management
    ├── field_type.rs   # FieldType - type hierarchy for message fields
    ├── entity_type.rs  # EntityType - entity types (transactionalId, topicName, etc.)
    ├── field_spec.rs   # FieldSpec - field specifications with JSON deserialization
    ├── struct_spec.rs  # StructSpec - structure specifications
    ├── message_spec.rs # MessageSpec - top-level message specification
    ├── message_spec_type.rs      # MessageSpecType enum
    ├── request_listener_type.rs  # RequestListenerType enum
    └── schema_generator.rs       # SchemaGenerator - generates schemas from specs

message-specs/          # 197 Kafka JSON RPC definitions copied from Apache Kafka 4.1
build.rs                # Build script that generates Rust code before compilation
tests/
├── common/             # Mirrors org.apache.kafka.common
│   ├── mod.rs          # Shared infra: re-exports library types for test-generated messages
│   ├── cluster_config.rs   # ClusterConfig + SecurityMode for integration tests
│   ├── cluster_pool.rs     # Process-global pool of shared Kafka containers
│   ├── kafka_cluster.rs    # Docker Kafka wrapper + SecureKafka custom image
│   ├── test_certs.rs       # Certificate generation utility (rcgen)
│   ├── test_context.rs     # Per-test isolation context
│   ├── message/        # Tests for org.apache.kafka.common.message
│   │   ├── main.rs
│   │   ├── message_test.rs             # MessageTest.java
│   │   ├── simple_example_message_test.rs
│   │   ├── nullable_struct_message_test.rs
│   │   ├── simple_arrays_message_test.rs
│   │   ├── generated_messages_test.rs
│   │   ├── message_round_trip_test.rs
│   │   └── message_serialization_test.rs
│   └── protocol/      # Tests for org.apache.kafka.common.protocol
│       ├── main.rs
│       ├── flexible_version_test.rs
│       └── tagged_fields_test.rs
└── integration/        # Integration tests (--features integration-tests)
    ├── main.rs
    ├── connection_test.rs
    ├── api_versions_test.rs
    ├── metadata_test.rs
    └── ssl_sasl_test.rs    # SSL and SASL PLAIN integration tests
```

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
