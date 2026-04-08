# Actor 0 Session - Layer 1: Core Protocol Types

**Date:** 2026-04-02  
**Branch:** `dev/network_connection_and_request_response`  
**Role:** Actor 0 (per agent-roles.md)

## Task

Implement Layer 1 from BASIC_CONNECTION_ROADMAP.md: translate foundational Java Kafka client types to Rust following CLAUDE.md translation rules.

## Commits

### 1. `62be2dd` - Implement Layer 1: Core Protocol Types

Created 7 new files translating Java Kafka client types:

- **TopicPartition** (`src/common/topic_partition.rs`) - topic+partition pair with Hash/Eq/Display
- **Node** (`src/common/node.rs`) - Kafka broker node info with rack/fenced support, NO_NODE sentinel
- **PartitionInfo** (`src/common/partition_info.rs`) - per-partition metadata (leader, replicas, ISR, offline)
- **ClusterResource** (`src/common/cluster_resource.rs`) - cluster ID wrapper
- **Cluster** (`src/common/cluster.rs`) - immutable cluster metadata snapshot with indexed lookups
- **Errors** (`src/common/protocol/errors.rs`) - 134 Kafka error codes with retriable/fatal classification
- **ApiKeys** (`src/common/protocol/api_keys.rs`) - 85+ API key definitions wrapping auto-generated ApiMessageType

Also:
- Extended generator (`generator/src/lib.rs`) to auto-generate `ApiMessageType` enum from 197 JSON specs
- Added `uuid` crate for UUID v4 generation (`Uuid::random_uuid()`)
- Modified 13 existing files (mod.rs, build.rs, Cargo.toml, etc.)

### 2. `36d864c` - fixup! (Review Round 1 - COMMENTS.0.md)

Addressed 6 review issues:

| Issue | Fix |
|-------|-----|
| 1. Cluster missing PartialEq/Eq/Hash | Manual implementations matching Java equals/hashCode |
| 2. Cluster missing node shuffling | Added `rand` crate, shuffle in constructor for load balancing |
| 3. Missing Cluster tests | Translated testEquals and testNotEquals from ClusterTest.java |
| 4. ApiKeys missing methods | Added all_versions(), in_scope(), broker/controller/client_apis(), apis_for_listener() |
| 5. Missing ApiKeys tests | Translated 5 tests: invalid ID, cluster action, valid versions, API scope |
| 6. Truncated error messages | Completed 7 messages to match Java source (InvalidSessionTimeout, InvalidRequest, etc.) |

### 3. `4b76ada` - fixup! (Review Round 2 - COMMENTS.0.md)

Addressed 2 review issues:

| Issue | Fix |
|-------|-----|
| 7. ApiKeys missing toApiVersion methods | Implemented to_api_version() and to_api_version_for_api_response() with PRODUCE API special handling |
| 8. Missing Schema infrastructure | Created Schema/BoundField/Field/SchemaType types, generated schema() methods on all 197 message structs, added request_schema/response_schema to ApiMessageType, translated testResponseThrottleTime |

## Key Decisions

- **ApiMessageType auto-generated, ApiKeys hand-written** - Follows Java architecture where ApiMessageType is generated from JSON specs and ApiKeys wraps it with additional metadata (cluster_action, forwardable flags)
- **Removed APIs (keys 4-7)** - Handled via fallback JSON parsing since full MessageSpec parsing fails for `validVersions: "none"`
- **uuid crate for UUID v4** - User suggested over getrandom; provides `Uuid::random_uuid()` matching Java's `Uuid.randomUuid()`
- **rand crate for node shuffling** - Java shuffles nodes in Cluster constructor for load balancing
- **Schema as runtime type** - Generated `schema(version)` methods instead of const statics (vec! isn't const-compatible); pragmatic approach enabling field introspection

## Final State

- **131 tests passing** (91 unit + 40 integration)
- `cargo build` - success
- `cargo test` - all pass
- `cargo xtask format-check` - clean
- `cargo xtask lint` - clean

## Dependencies Added

- `uuid = { version = "1", features = ["v4"] }` - UUID v4 generation
- `rand = "0.9"` - Node shuffling in Cluster constructor

## Files Created (8)

| File | Lines | Description |
|------|-------|-------------|
| `src/common/topic_partition.rs` | ~70 | TopicPartition struct |
| `src/common/node.rs` | ~140 | Node struct with NO_NODE sentinel |
| `src/common/partition_info.rs` | ~130 | PartitionInfo with Display |
| `src/common/cluster_resource.rs` | ~50 | ClusterResource wrapper |
| `src/common/cluster.rs` | ~580 | Cluster with indexed lookups, equality, tests |
| `src/common/protocol/errors.rs` | ~785 | 134 error codes, retriable classification |
| `src/common/protocol/api_keys.rs` | ~620 | ApiKeys with all methods and tests |
| `src/common/protocol/types.rs` | ~170 | Schema infrastructure for field introspection |

## Files Modified (14)

generator/src/lib.rs, generator/src/message/*.rs, build.rs, Cargo.toml, Cargo.lock, src/common/mod.rs, src/common/protocol/mod.rs, src/common/uuid.rs, xtask/src/main.rs, .claude/rules/agent-roles.md
