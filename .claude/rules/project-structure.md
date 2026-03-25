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
└── generated_messages_test.rs  # Integration tests for generated messages
```
