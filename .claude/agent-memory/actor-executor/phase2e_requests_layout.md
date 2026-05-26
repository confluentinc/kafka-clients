---
name: Phase 2e requests-wrapper layout
description: Where the request/response wrapper classes live after Phase 2e and the trait shape they expose
type: project
---

Phase 2e (final phase of Phase 2) translates
`org.apache.kafka.common.requests.*` into `src/common/requests/`.

**Trait shape (key non-obvious choices):**

- `AbstractRequestResponse::data()` returns `&dyn Message`, not
  `&dyn ApiMessage`. Header data structs (`RequestHeaderData`,
  `ResponseHeaderData`) implement `Message` but not `ApiMessage` (Java's
  headers are bare `Message`s). Using `Message` as the common bound lets
  headers and request bodies share the same trait surface.
- `AbstractResponse::error_counts()` returns `HashMap<Errors, i32>`, not
  `BTreeMap` — `Errors` doesn't impl `Ord` (it derives `Hash` only). All
  call sites in `MetadataResponse`/`ProduceResponse` use `HashMap`.
- `AbstractResponse` cannot expose an associated `const`
  (`DEFAULT_THROTTLE_TIME`) because it then becomes dyn-incompatible.
  The constant lives at module scope:
  `crate::common::requests::abstract_response::DEFAULT_THROTTLE_TIME`.

**`ApiKey` accessor surface added in Phase 2e:**

`ApiKey` (in `protocol::api_keys`) gained these delegating methods that
forward to the generated `ApiMessageType`:

- `oldest_version()`, `latest_version()`, `latest_version_unstable(bool)`
- `is_version_supported(version)`, `has_valid_version()`
- `is_version_deprecated(version)`
- `request_header_version(version)`, `response_header_version(version)`
- `message_type()` → `ApiMessageType`

These are required by `RequestHeader::new(api_key, version, …)` and
`ResponseHeader::new(...)` to derive the correct header version per
`ApiKeys.requestHeaderVersion(short)` / `responseHeaderVersion(short)`.
The generated `ApiMessageType` enum (Phase 2d-1) is the authoritative
source for header-version logic.

**`KafkaError::InvalidRequest(String)` added (wire code 42):**

Java's `RequestHeader.parse` throws `InvalidRequestException` (wire code
42) on malformed input. We added the variant + `from_code` mapping +
`java_class_name` so the parser can return a proper `Result`.

**`*Data::new()` not `::default()`:**

The generator emits `pub fn new() -> Self` but no `Default` impl. Wrapper
construction uses `*Data::new()` and the struct-update syntax
`..*Data::new()` for partial-init.

**Tests deferred to later phases:**

- `MetadataResponseTest#buildClusterTest` — needs `Cluster`/`Node`/
  `PartitionInfo` translation (Phase 4).
- `RequestContextTest` (all four tests) — needs `KafkaPrincipal`,
  `ListenerName`, `SecurityProtocol`, `ClientInformation`,
  `ByteBufferChannel`, `Send`, and the `EnvelopeRequest`/`Response`
  apis. Producer client doesn't construct `RequestContext`; it's a
  broker-side concern.
- `ProduceRequestTest` `MemoryRecords`-driven tests
  (`shouldBeFlaggedAsTransactional…`, `testV3AndAboveShouldContainOnly…`,
  `testMixedTransactionalData`, etc.) — need `MemoryRecords` (Phase 3).
- `ApiVersionsResponseTest`'s parameterized tests over
  `messageType.requestSchemas()[i]` — the Phase 2d-1 stub returns an
  empty Schema; valid translation requires the Phase 2d follow-up to
  populate `request_schema(version)`/`response_schema(version)` from
  `crate::common::message::*_data::*Data::schema(version)`.

**Tests translated:**

- `RequestHeaderTest` — all 5 tests.
- `RequestUtilsTest#testIsFatalException` — translated; comment notes
  the four producer-irrelevant exception types skipped.
- `MetadataRequestTest` — 4 of 4 tests (constructor / version validation).
- `ProduceRequestTest`'s non-MemoryRecords assertions:
  `testBuildWithCurrentMessageFormat` (only the version-bound part),
  `testBuilderOldestAndLatestAllowed` (state preservation).
- `ProduceResponseTest#produceResponseRecordErrorsTest` translated as
  `produce_response_record_errors_test` — loops over all PRODUCE
  versions verifying the v8+ recordErrors / errorMessage fields.

**Wrapper-framing byte tests** (Phase 2e DoD):

`src/common/requests/tests.rs` carries 6 framing tests that lock
`serialize_with_header() == header bytes ++ body bytes` for all 3 API
pairs. These do not need new Java fixtures: they reuse the per-`*Data`
fixtures already locked in `src/common/message/tests.rs` and assert
the wrapper layer's concatenation is byte-identical.
