# Basic Kafka Connection Implementation Roadmap

## Overview

This document outlines the minimal set of classes needed to implement basic TCP connection to Kafka brokers and send/receive requests without SSL/SASL authentication.

## Summary Statistics

- **Total classes in KafkaProducer dependency graph**: 784
- **Classes needed for basic connection (no SSL/SASL)**: 628
- **Already completed (marked)**: 177 (28%)
- **Remaining to implement**: 451
- **Critical path classes**: 41 (only 3 completed so far)

## Critical Path Implementation (7 Layers)

Implement these layers in order from bottom to top. Each layer builds on the previous one.

### Layer 1 - Core Protocol Types (5 classes)
Foundation types representing Kafka cluster entities.

- ○ `Node` - Represents a Kafka broker node
- ○ `TopicPartition` - Topic and partition identifier
- ○ `Cluster` - Cluster metadata and node information
- ○ `ApiKeys` - Enum of all Kafka API request types
- ○ `Errors` - Kafka protocol error codes

### Layer 2 - Wire Protocol (5 classes)
Binary serialization/deserialization framework.

- ✓ `Readable` - Interface for reading from wire protocol *(DONE)*
- ✓ `Writable` - Interface for writing to wire protocol *(DONE)*
- ✓ `ByteBufferAccessor` - Buffer-based implementation *(DONE)*
- ○ `Message` - Base interface for protocol messages
- ○ `ApiMessage` - API-specific message interface

### Layer 3 - Request/Response Framework (8 classes)
High-level request/response abstraction.

- ○ `RequestHeader` - Request header with correlation ID, client ID
- ○ `ResponseHeader` - Response header with correlation ID
- ○ `AbstractRequest` - Base class for all requests
- ○ `AbstractResponse` - Base class for all responses
- ○ `ApiVersionsRequest` - Query broker API versions
- ○ `ApiVersionsResponse` - Broker API version information
- ○ `MetadataRequest` - Request cluster metadata
- ○ `MetadataResponse` - Cluster metadata (topics, partitions, leaders)

### Layer 4 - Network Transport (7 classes)
Low-level TCP I/O and framing.

- ○ `TransportLayer` - Abstract network transport interface
- ○ `PlaintextTransportLayer` - Unencrypted TCP transport
- ○ `Send` - Interface for outgoing data
- ○ `Receive` - Interface for incoming data
- ○ `NetworkSend` - Outgoing network frame
- ○ `NetworkReceive` - Incoming network frame
- ○ `ByteBufferSend` - Send implementation using ByteBuffer

### Layer 5 - Channel & Selection (5 classes)
Non-blocking I/O channel management (equivalent to Java NIO Selector).

- ○ `Selectable` - Interface for selectable I/O operations
- ○ `Selector` - Non-blocking I/O multiplexer (like Java NIO Selector)
- ○ `KafkaChannel` - Single connection to a broker
- ○ `ChannelBuilder` - Factory for creating channels
- ○ `PlaintextChannelBuilder` - Builder for plaintext (non-SSL) channels

### Layer 6 - Client Infrastructure (7 classes)
High-level client connection and request management.

- ○ `KafkaClient` - Interface for Kafka network client
- ○ `NetworkClient` - Main implementation of KafkaClient
- ○ `ClientRequest` - Wrapper for outgoing requests
- ○ `ClientResponse` - Wrapper for received responses
- ○ `RequestCompletionHandler` - Callback for async requests
- ○ `InFlightRequests` - Track pending requests
- ○ `ClusterConnectionStates` - Per-node connection state tracking

### Layer 7 - Metadata & Version Management (4 classes)
Cluster metadata caching and API version negotiation.

- ○ `Metadata` - Cached cluster metadata with refresh logic
- ○ `MetadataSnapshot` - Immutable metadata snapshot
- ○ `ApiVersions` - Track broker API version capabilities
- ○ `NodeApiVersions` - Per-broker API version information

## Implementation Strategy

### Phase 1: Wire Protocol Foundation
**Goal**: Establish request/response serialization

1. Implement Layer 1 (Core Protocol Types)
2. Complete Layer 2 (Wire Protocol) - 60% done
3. Implement generated message types (ApiVersionsRequestData, MetadataRequestData, etc.)

### Phase 2: Network Transport
**Goal**: Establish TCP connections and I/O

4. Implement Layer 4 (Network Transport)
5. Implement Layer 5 (Channel & Selection)
   - Use Tokio for async I/O instead of Java NIO Selector
   - Map Java Selector → Tokio TcpStream + mio poll

### Phase 3: Request/Response Framework
**Goal**: High-level request handling

6. Implement Layer 3 (Request/Response Framework)
7. Implement Layer 6 (Client Infrastructure)

### Phase 4: Metadata Management
**Goal**: Cluster discovery and API negotiation

8. Implement Layer 7 (Metadata & Version Management)

### Phase 5: Integration Testing
**Goal**: Verify end-to-end connection

9. Test basic connection flow:
   - Connect to broker
   - Send ApiVersionsRequest
   - Receive and parse ApiVersionsResponse
   - Send MetadataRequest
   - Receive and parse MetadataResponse

## Key Dependencies Beyond Critical Path

### Additional Support Classes (587 classes total)
While the critical path has only 41 classes, the complete implementation requires supporting classes in these areas:

- **All Request/Response types** (182 classes) - Most requests won't be needed initially
- **Configuration** (`AbstractConfig`, `ConfigDef`) - For client configuration
- **Metrics** (`Metric`, `MetricName`) - For monitoring
- **Compression** (`GzipCompression`, `Lz4Compression`, etc.) - For record compression
- **Error handling** (21 exception classes) - For proper error reporting
- **Utilities** (`ByteUtils`, `Checksums`, `Crc32C`) - Helper functions

## Rust-Specific Adaptations

### Java NIO → Tokio Mapping
- `java.nio.channels.Selector` → `tokio::net::TcpStream` + `mio::Poll`
- `java.nio.channels.SocketChannel` → `tokio::net::TcpStream`
- `java.nio.ByteBuffer` → `bytes::BytesMut` or `Vec<u8>`

### Concurrency
- Java callbacks → Rust async/await
- `CompletableFuture` → `tokio::spawn` for fire-and-forget
- Thread-per-request → Single Tokio runtime with tasks

### Error Handling
- Java checked exceptions → `Result<T, KafkaError>`
- Retriable errors → `KafkaError::is_retriable()`
- Fatal errors → `KafkaError::is_fatal()`

## Generated Files

- `basic_connection_classes.json` - All 628 classes needed for basic connection
- `critical_path_classes.json` - 41 critical path classes organized by layer
- `dependency_graph.json` - Full dependency tree (784 classes)
- `dependency_graph_flat.json` - Flat dependency list with completion tracking

## Next Steps

1. Review the critical path and prioritize Layer 1 implementation
2. Start with `Node`, `TopicPartition`, `Cluster` as they have minimal dependencies
3. Complete Layer 2 by implementing `Message` and `ApiMessage` interfaces
4. Work through layers 3-7 in sequence

## Progress Tracking

Use the `marked_classes.txt` file to track completed classes. Regenerate the dependency graph with:

```bash
python3 tools/dependency_graph/dependency_graph.py \
  --kafka-dir kafka/ \
  --root-class KafkaProducer \
  --mark-file marked_classes.txt \
  --json-only \
  --output dependency_graph
```

This will update completion statistics and the `remaining_classes.txt` file.
