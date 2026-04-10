# Layer 3 Phase 3: Request/Response Framework (ApiVersions + Metadata Only)

## 1. Scope

Translate ONLY the request/response classes needed for ApiVersions and Metadata RPCs.

### Classes to Translate

| # | Java Class | Rust File | Complexity |
|---|-----------|-----------|------------|
| 1 | `RequestHeader` | `requests/request_header.rs` | Medium |
| 2 | `ResponseHeader` | `requests/response_header.rs` | Medium |
| 3 | `AbstractRequest` + `Builder` | `requests/abstract_request.rs` | High |
| 4 | `AbstractResponse` | `requests/abstract_response.rs` | High |
| 5 | `RequestAndSize` | `requests/request_and_size.rs` | Low |
| 6 | `RequestUtils` (subset) | `requests/request_utils.rs` | Low |
| 7 | `SendBuilder` | `requests/send_builder.rs` | Medium |
| 8 | `ApiVersionsRequest` + Builder | `requests/api_versions_request.rs` | Medium |
| 9 | `ApiVersionsResponse` + Builder | `requests/api_versions_response.rs` | High |
| 10 | `MetadataRequest` + Builder | `requests/metadata_request.rs` | Medium |
| 11 | `MetadataResponse` + inner types | `requests/metadata_response.rs` | High |

### Explicitly Out of Scope
- All other request/response types (Produce, Fetch, etc.)
- Full `ApiMessageType` dispatch for 197 types
- `RequestContext`, `EnvelopeRequest`
- `Features`/`SupportedVersionRange` (server-side builder fields)
- `NodeApiVersions` (server-side)
- Zero-copy record handling in `SendBuilder`

## 2. Dependencies

### Already Translated (Layers 1-2)
- `ApiKeys`, `Errors`, `Message`/`ApiMessage` traits
- `ByteBufferAccessor`, `Readable`/`Writable`, `ObjectSerializationCache`
- `MessageUtil`, `MessageSizeAccumulator`
- `Node`, `Cluster`, `TopicPartition`, `PartitionInfo`, `Uuid`
- Generated data types: `RequestHeaderData`, `ResponseHeaderData`, `ApiVersionsRequestData`, `ApiVersionsResponseData`, `MetadataRequestData`, `MetadataResponseData`

### Modifications to Existing Code

1. **`ByteBufferAccessor::snapshot_remaining()`** — Returns a new `ByteBufferAccessor` with a copy of remaining bytes. Needed by `ApiVersionsResponse::parse()` for fallback parsing.

2. **`NO_PARTITION_LEADER_EPOCH` const** — Value `-1i32`, used by `RequestUtils::get_leader_epoch()`. Add to `src/common/record.rs` or requests module.

## 3. Rust-Specific Design Decisions

### Java Inheritance → Rust Enums

`AbstractRequest`/`AbstractResponse` are abstract base classes in Java. In Rust:

```rust
// Enum dispatch instead of trait objects
pub enum ConcreteRequest {
    ApiVersions(ApiVersionsRequest),
    Metadata(MetadataRequest),
}

pub enum ConcreteResponse {
    ApiVersions(ApiVersionsResponse),
    Metadata(MetadataResponse),
}
```

Common methods (`api_key()`, `version()`, `to_send()`, `data()`) implemented on the enum with `match` delegation. `data()` returns `&dyn ApiMessage`.

### Java Builder → Rust RequestBuilder Trait

```rust
pub trait RequestBuilder {
    type Request;
    fn api_key(&self) -> &ApiKeys;
    fn oldest_allowed_version(&self) -> i16;
    fn latest_allowed_version(&self) -> i16;
    fn build(&self) -> Self::Request { self.build_version(self.latest_allowed_version()) }
    fn build_version(&self, version: i16) -> Self::Request;
}
```

### Lazy Holder in MetadataResponse

Java uses `volatile` + double-checked locking. Rust: `OnceLock<Holder>` for thread-safe lazy initialization.

### AppInfoParser.getVersion()

Replaced with `env!("CARGO_PKG_VERSION")` compile-time constant.

### Error Handling

- `UnsupportedVersionException` → `KafkaError::UnsupportedVersion` or `io::Error::new(InvalidData, ...)`
- `InvalidRequestException` → `io::Error::new(InvalidData, ...)`
- `CorrelationIdMismatchException` → Custom error type or `io::Error`

### SendBuilder

Only the simple contiguous-buffer path (no zero-copy records). Serializes header + body into `ByteBufferSend`.

## 4. Implementation Order

### Step 0: Prerequisites
- Add `snapshot_remaining()` to `ByteBufferAccessor`
- Add `NO_PARTITION_LEADER_EPOCH` const

### Step 1: Headers + Utilities
- `src/common/requests/mod.rs` — Module declaration, re-exports
- `src/common/requests/request_utils.rs` — `serialize()`, `get_leader_epoch()`
- `src/common/requests/response_header.rs` — `ResponseHeader` struct
- `src/common/requests/request_header.rs` — `RequestHeader` struct

### Step 2: Framework
- `src/common/requests/send_builder.rs` — `SendBuilder` for serialization
- `src/common/requests/abstract_response.rs` — `ConcreteResponse` enum, common methods
- `src/common/requests/abstract_request.rs` — `ConcreteRequest` enum, `RequestBuilder` trait
- `src/common/requests/request_and_size.rs` — Simple struct

### Step 3: ApiVersions
- `src/common/requests/api_versions_request.rs` — Struct + Builder
- `src/common/requests/api_versions_response.rs` — Struct + Builder + fallback parse logic

### Step 4: Metadata
- `src/common/requests/metadata_request.rs` — Struct + Builder
- `src/common/requests/metadata_response.rs` — Struct + TopicMetadata + PartitionMetadata + Holder

## 5. Tests to Translate

| Java Test File | Tests | Notes |
|---|---|---|
| `RequestHeaderTest.java` | 4 tests | Skip Mockito-based `verifySizeMethodsReturnSameValue`; verify caching through correctness |
| `ResponseHeaderTest.java` | (none found) | Write basic roundtrip tests |
| `ApiVersionsResponseTest.java` | 7 tests | `shouldHaveCorrectDefaultApiVersionsResponse` needs minimal `TestUtils` helper |
| `MetadataRequestTest.java` | 4 tests | |
| `MetadataResponseTest.java` | 1 test (`buildClusterTest`) | |

### Test Helpers Needed
- `RequestTestUtils::serialize_request_header()`
- `RequestTestUtils::serialize_response_with_header()`
- Minimal `metadata_response()` / `metadata_update_with()` helpers

## 6. Files to Create

```
src/common/
  requests/
    mod.rs
    request_utils.rs
    response_header.rs
    request_header.rs
    send_builder.rs
    abstract_response.rs
    abstract_request.rs
    request_and_size.rs
    api_versions_request.rs
    api_versions_response.rs
    metadata_request.rs
    metadata_response.rs
```

## 7. Files to Modify

- `src/common/mod.rs` — Add `pub mod requests;`
- `src/common/protocol/byte_buffer_accessor.rs` — Add `snapshot_remaining()` method

## 8. Risks

1. **Generated type API surface** — Depends on generated types having methods like `set_request_api_key()`, `find()` on collections. Verify by building after Step 1.

2. **SendBuilder simplification** — Only contiguous-buffer path. Zero-copy for ProduceRequest/FetchResponse deferred.

3. **Enum extensibility** — Adding new request types means adding variants. Conscious trade-off for type safety; all variants known at compile time.

4. **ApiVersionsResponse.Builder** — Server-side `Features`/`SupportedVersionRange` params simplified to `HashMap<String, (i16, i16)>` or minimal stubs.
