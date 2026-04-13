---
name: Layer 3 Request/Response patterns
description: Review patterns for the request/response framework translation — validation gaps and panic vs Result for public API
type: feedback
---

When reviewing request/response framework translations:

1. **Header validation in serialize methods**: Java `AbstractRequest.serializeWithHeader` validates apiKey and apiVersion match before serializing. Rust enum dispatch makes this easy to miss since each variant delegates directly.

2. **Builder panic vs Result**: Java builder `build(short version)` throws `UnsupportedVersionException` for invalid versions. CLAUDE.md rule 10.2 requires `Result` for such cases. Watch for `assert!` in public-facing builder methods — these should return errors.

3. **Generator per-field flexibleVersions**: The generator now supports per-field `flexibleVersions` overrides (e.g., `"flexibleVersions": "none"` on RequestHeaderData.clientId). This was a necessary fix for correct RequestHeader serialization.

4. **Nullable array defaults**: Java generated code defaults nullable arrays to empty lists; Rust defaults to `None`. Tests must explicitly set `Some(Vec::new())` to match Java default behavior. This is a known issue (see nullable_default_mismatch.md).

5. **Version validation in constructors**: Java `AbstractRequest(ApiKeys, short)` validates `apiKey.isVersionSupported(version)` in the constructor. Rust request structs (`ApiVersionsRequest::new`, `MetadataRequest::new`) skip this validation since Rust doesn't have constructor patterns with inheritance. Builder-level validation covers the common path; parse-level validation catches deserialization. Acceptable gap — not a bug in practice.

6. **getErrorResponse Throwable vs Errors**: Java `getErrorResponse(int throttleTimeMs, Throwable e)` maps exceptions to error codes via `Errors.forException(e)`. Rust passes `&Errors` directly, which is a valid adaptation. Callers must map their errors to `Errors` codes before calling.

7. **isFatalException not yet translated**: `RequestUtils.isFatalException` and its test depend on exception classes not yet in the codebase. Will be handled by `KafkaError::is_fatal()` when those error types are translated.

**Why:** These patterns appear across the request/response layer and will recur as more request types are translated.
**How to apply:** Check every new request/response type for validation parity, builder error handling, and generated field encoding.
