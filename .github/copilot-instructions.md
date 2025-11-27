# Copilot Instructions

## Project Overview
This is a Rust kafka client implementationt that needs to transpile with AI
the Java Kafka client (only the client!) instruction by instruction by keeping the same
architecture and the same namespace and names, just adapted to Rust case
naming conventions. If you need to implement a class from the Java standard library that is not
present in Rust you should implement it from scratch in Rust by transpiling the
code from OpenJDK, otherwise you can use the Rust standard library if it
provides the same functionality and equal or better performance.
Keep the same tests and similar comments as well.
Ensure you don't leave any TODO or FIXME and do anything that should be done.
Don't stop until the entire project builds and all tests pass and the linter as
well.
Avoid using sh scripts but use xtask Rust programs instead.
Don't change this line or the description above this line.

## Transpilation Rules
1. **Java Standard Library**: If a Java standard library class isn't available in Rust, implement it from scratch by transpiling from OpenJDK
2. **Rust Standard Library**: Use Rust standard library when it provides equivalent or better functionality/performance
3. **Naming Conventions**: 
   - Java package `org.apache.kafka.message` → Rust module `message`
   - Java class names (PascalCase) → Rust struct/enum names (PascalCase)
   - Java method names (camelCase) → Rust function names (snake_case)
   - Preserve original architecture and logical structure

## Project Structure
```
src/
├── lib.rs              # Root library module with generated code inclusion
├── common/             # Common types (org.apache.kafka.common)
│   ├── mod.rs
│   ├── uuid.rs         # ✅ UUID type with base64 URL encoding
│   └── protocol/       # ✅ Wire protocol serialization (org.apache.kafka.common.protocol)
│       ├── mod.rs
│       ├── readable.rs             # ✅ Readable trait for deserialization
│       ├── writable.rs             # ✅ Writable trait for serialization
│       ├── byte_buffer_accessor.rs # ✅ ByteBufferAccessor implementation
│       └── varint.rs               # ✅ Varint/varlong encoding
└── message/            # Schema generation (org.apache.kafka.message)
    ├── mod.rs
    ├── code_buffer.rs  # ✅ CodeBuffer - buffered code generation
    ├── versions.rs     # ✅ Versions - version range management
    ├── field_type.rs   # ✅ FieldType - type hierarchy for message fields
    ├── entity_type.rs  # ✅ EntityType - entity types (transactionalId, topicName, etc.)
    ├── field_spec.rs   # ✅ FieldSpec - field specifications with JSON deserialization
    ├── struct_spec.rs  # ✅ StructSpec - structure specifications
    ├── message_spec.rs # ✅ MessageSpec - top-level message specification
    ├── message_spec_type.rs      # ✅ MessageSpecType enum
    ├── request_listener_type.rs  # ✅ RequestListenerType enum
    └── schema_generator.rs       # ✅ SchemaGenerator - generates schemas from specs

message-specs/          # ✅ 197 Kafka JSON RPC definitions copied from Apache Kafka 4.1
build.rs                # ✅ Build script that generates Rust code before compilation
tests/
└── generated_messages_test.rs  # ✅ Integration tests for generated messages
```

## Current Status: Wire Protocol Serialization Foundation Complete ✅
All core components transpiled + build infrastructure + wire protocol serialization layer implemented + **flexible version and tagged field support complete**.

### Completed Components
**Core Message Generation:**
- **CodeBuffer** (`code_buffer.rs`): Manages indented code generation with line buffering
- **Versions** (`versions.rs`): Handles version ranges (e.g., "3-7", "4+", "none")
- **FieldType** (`field_type.rs`): Type hierarchy for all field types (primitives, arrays, structs)
- **EntityType** (`entity_type.rs`): Entity type validation (transactionalId, topicName, brokerId, etc.)
- **FieldSpec** (`field_spec.rs`): Field specifications with JSON deserialization and validation
- **StructSpec** (`struct_spec.rs`): Structure specifications with field validation
- **MessageSpec** (`message_spec.rs`): Top-level message specification with comprehensive validation
- **MessageSpecType** (`message_spec_type.rs`): Enum for message types (Request, Response, Header, etc.)
- **RequestListenerType** (`request_listener_type.rs`): Enum for request listener types
- **SchemaGenerator** (`schema_generator.rs`): Generates Rust schema code from MessageSpec
- **build.rs**: Build script that parses JSON specs and uses SchemaGenerator to generate code

**Wire Protocol Serialization:**
- **Uuid** (`common/uuid.rs`): 128-bit UUID with base64 URL encoding matching Java
- **Readable** (`common/protocol/readable.rs`): Trait for reading wire protocol data
- **Writable** (`common/protocol/writable.rs`): Trait for writing wire protocol data
- **ByteBufferAccessor** (`common/protocol/byte_buffer_accessor.rs`): Buffer implementation for Readable/Writable
- **varint** (`common/protocol/varint.rs`): Protocol Buffers varint/varlong encoding (unsigned and zig-zag)
- **RawTaggedField** (`common/protocol/readable.rs`): Tagged field support for forward compatibility

**Flexible Version Support:**
- **read_unsigned_varint/write_unsigned_varint**: Compact encoding for flexible versions (varint length + 1)
- **RawTaggedField**: Stores unknown tagged fields for forward compatibility
- **read_unknown_tagged_field**: Reads and accumulates unknown tagged fields
- **Flexible string encoding**: varint(length+1) + UTF-8 bytes (null = varint(0), empty = varint(1))
- **Flexible bytes encoding**: varint(length+1) + raw bytes
- **Flexible array encoding**: varint(length+1) + elements
- **Tagged fields serialization**: num_fields (varint) + [tag (varint), size (varint), data]*

**Tagged Field Support (COMPLETE):**
- **generate_tagged_field_read**: Generates code to read tagged fields based on JSON specs
  * Reads tag (varint) and size (varint) from stream
  * Matches on tag ID to dispatch to correct field reader
  * Supports: Bool, Int8/16/32/64, Uuid, String, Array, Struct types
  * For Struct: reads bytes into buffer, creates ByteBufferAccessor, calls Struct::read()
  * For unknown tags: skips size bytes for forward compatibility
- **generate_tagged_field_write**: Generates code to write tagged fields
  * Two-pass approach: first counts non-default fields, then writes them
  * Writes num_tagged_fields (varint)
  * For each field: writes tag (varint), size (varint), data
  * For Struct: writes struct to temp ByteBufferAccessor, calculates size, writes bytes
  * For String: writes varint(length+1) + UTF-8 bytes
  * For primitives: writes fixed-size data (1/2/4/8/16 bytes)
- **Field filtering**: Separates tagged fields from normal fields based on taggedVersions
  * Fields with non-empty taggedVersions are written in tagged section only
  * Fields with empty taggedVersions are written in normal section
- **Version-aware**: Tagged fields only written/read in flexible version ranges

### Test Coverage
- **76 tests passing** covering all transpiled components, generated messages, wire protocol, flexible versions, and **full tagged field support**
- **197 message types** generated from JSON specifications
- Comprehensive test suites matching Java originals
- All components have unit tests with edge case coverage
- Integration tests verify generated code accessibility
- Wire protocol tests verify round-trip serialization/deserialization
- **Flexible version tests (11 tests)**: varint encoding, tagged fields, compact encoding
- **Tagged field tests (4 tests)**: String tagged fields, Struct tagged fields, multiple tagged fields, empty tagged fields

### Build System Details
- build.rs reads 197 JSON message specs from message-specs/
- Parses JSON with comment stripping using serde_json
- Uses MessageSpec to parse specifications (including flexibleVersions and taggedVersions)
- Uses SchemaGenerator infrastructure to generate proper Rust structs
- Handles Rust keyword escaping (e.g., `type` → `r#type`)
- Generates complete field definitions with types, docs, and version constants
- **Generates tagged field read/write code based on JSON tag and taggedVersions fields**
- Output: 197 properly formatted Rust modules in target/.../out/generated/

### Wire Protocol Implementation
- **Big-endian** byte order for all multi-byte integers (matching Java ByteBuffer)
- **Varint encoding**: Protocol Buffers unsigned and zig-zag signed encoding
- **UUID serialization**: Most significant 64 bits first, least significant 64 bits second
- **String encoding**: 
  - Standard: i16 length prefix + UTF-8 bytes
  - Flexible: varint(length+1) + UTF-8 bytes (null=0, empty=1)
- **Bytes encoding**:
  - Standard: i32 length prefix + raw bytes
  - Flexible: varint(length+1) + raw bytes
- **Array encoding**:
  - Standard: i32 length prefix + elements
  - Flexible: varint(length+1) + elements
- **ByteBufferAccessor**: Provides mutable byte buffer with position tracking
- All primitive types: byte, short, int, long, double, arrays
- **Tagged fields**: Encoded at end of struct with tag and size varints
- **Tagged field format**: tag (varint) | size (varint) | data (size bytes)
- **Struct tagged fields**: Serialized to temp buffer, size calculated, then written with tag and size
- Full compatibility with Java's Readable/Writable interfaces

### Next Steps
- Implement Message trait with size(), addSize(), read(), write() methods
- Add support for nullable fields and version-specific field serialization
- Begin transpiling the Kafka client implementation (org.apache.kafka.clients)

### Key Java Classes to Transpile (Reference)
Reference: `kafka/generator/src/main/java/org/apache/kafka/message/`
- ✅ `FieldType.java` - Field type hierarchy
- ✅ `FieldSpec.java` - Field specifications
- ✅ `StructSpec.java` - Structure specifications
- ✅ `MessageSpec.java` - Top-level message spec with JSON parsing
- ✅ `SchemaGenerator.java` - Schema code generator
- ✅ `MessageSpecType.java` - Message type enum
- ✅ `RequestListenerType.java` - Request listener enum

## Development Workflow
- **Build**: `cargo build`
- **Test**: `cargo test`
- **Format**: `cargo xtask format` (formats all code including generated files)
- **Format Check**: `cargo xtask format-check` (CI-friendly format checking)
- **Check Generated**: `cargo xtask check-generated` (validates generated code formatting only, no modifications)
- **Source Reference**: Java source in `kafka/` directory (Apache Kafka 4.1)
- All transpiled code includes Apache 2.0 license header
- Comprehensive unit tests for each component