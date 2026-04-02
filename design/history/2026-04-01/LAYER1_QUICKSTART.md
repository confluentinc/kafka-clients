# Layer 1 Quick Start - Core Protocol Types

## Goal
Implement the 5 foundational types that represent Kafka cluster entities.

## Classes to Implement

### 1. Node (`org.apache.kafka.common.Node`)
**File**: `kafka/clients/src/main/java/org/apache/kafka/common/Node.java`

Represents a Kafka broker node (id, host, port, rack).

**Key methods**:
- `id()` - Broker ID
- `host()` - Hostname
- `port()` - Port number
- `idString()` - String representation of ID
- `isEmpty()` - Check if this is an empty node
- `hasRack()` - Check if rack is specified
- `rack()` - Get rack ID

**Rust implementation**:
```rust
pub struct Node {
    id: i32,
    host: String,
    port: i32,
    rack: Option<String>,
}
```

---

### 2. TopicPartition (`org.apache.kafka.common.TopicPartition`)
**File**: `kafka/clients/src/main/java/org/apache/kafka/common/TopicPartition.java`

Identifies a specific partition of a topic.

**Key methods**:
- `topic()` - Topic name
- `partition()` - Partition number

**Rust implementation**:
```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    topic: String,
    partition: i32,
}
```

---

### 3. Cluster (`org.apache.kafka.common.Cluster`)
**File**: `kafka/clients/src/main/java/org/apache/kafka/common/Cluster.java`

Immutable snapshot of cluster metadata (nodes, topics, partitions, leaders).

**Key methods**:
- `nodes()` - All broker nodes
- `nodeById(id)` - Get node by ID
- `topics()` - All topic names
- `partitionsForTopic(topic)` - Get partitions for topic
- `leaderFor(partition)` - Get leader node for partition
- `availablePartitionsForTopic(topic)` - Get available (online) partitions

**Rust implementation**:
```rust
pub struct Cluster {
    nodes: Vec<Node>,
    nodes_by_id: HashMap<i32, Node>,
    partitions_by_topic: HashMap<String, Vec<PartitionInfo>>,
    // ... more fields
}
```

---

### 4. ApiKeys (`org.apache.kafka.common.protocol.ApiKeys`)
**File**: `kafka/clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java`

Enum of all Kafka API request types with their ID and version ranges.

**Key methods**:
- `id` - Numeric API key
- `name` - API name
- `latestVersion()` - Latest supported version
- `oldestVersion()` - Oldest supported version
- `forId(id)` - Lookup ApiKey by ID

**Important APIs** (for basic connection):
- `API_VERSIONS (18)` - Query broker capabilities
- `METADATA (3)` - Fetch cluster metadata
- `PRODUCE (0)` - Send records
- `FETCH (1)` - Fetch records

**Rust implementation**:
```rust
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ApiKeys {
    Produce = 0,
    Fetch = 1,
    // ... 60+ API types
    Metadata = 3,
    // ...
    ApiVersions = 18,
    // ...
}

impl ApiKeys {
    pub fn id(&self) -> i16 { *self as i16 }
    pub fn latest_version(&self) -> i16 { /* ... */ }
    pub fn oldest_version(&self) -> i16 { /* ... */ }
}
```

---

### 5. Errors (`org.apache.kafka.common.protocol.Errors`)
**File**: `kafka/clients/src/main/java/org/apache/kafka/common/protocol/Errors.java`

Enum mapping error codes to exception types and retry behavior.

**Key methods**:
- `code()` - Numeric error code
- `message()` - Error message
- `exception()` - Convert to exception
- `forCode(code)` - Lookup error by code

**Common errors** (for basic connection):
- `NONE (0)` - No error
- `UNKNOWN_SERVER_ERROR (1)` - Generic server error
- `INVALID_REQUEST (42)` - Invalid request
- `UNSUPPORTED_VERSION (35)` - API version not supported
- `TOPIC_AUTHORIZATION_FAILED (29)` - Not authorized
- `NETWORK_EXCEPTION (-1)` - Network error (client-side)

**Rust implementation**:
```rust
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Errors {
    None = 0,
    UnknownServerError = 1,
    // ... 100+ error codes
    UnsupportedVersion = 35,
    // ...
}

impl Errors {
    pub fn code(&self) -> i16 { *self as i16 }
    pub fn is_retriable(&self) -> bool { /* ... */ }
    pub fn exception(&self) -> KafkaError { /* ... */ }
}
```

---

## Implementation Order

1. **TopicPartition** - Simplest, no dependencies
2. **Node** - Simple struct, no dependencies
3. **Errors** - Enum with error code mappings
4. **ApiKeys** - Enum with API metadata
5. **Cluster** - Uses Node and TopicPartition

## Testing

Create tests for each class:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let node = Node::new(1, "localhost".to_string(), 9092, None);
        assert_eq!(node.id(), 1);
        assert_eq!(node.host(), "localhost");
        assert_eq!(node.port(), 9092);
    }

    #[test]
    fn test_topic_partition() {
        let tp = TopicPartition::new("test".to_string(), 0);
        assert_eq!(tp.topic(), "test");
        assert_eq!(tp.partition(), 0);
    }
    
    // ... more tests
}
```

## Next Layer

After completing Layer 1, move to Layer 2 to complete the wire protocol implementation (Message, ApiMessage interfaces).

## Reference Files

- Java source: `kafka/clients/src/main/java/org/apache/kafka/common/`
- Dependencies: None for Layer 1 (these are foundation classes)
