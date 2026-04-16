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
