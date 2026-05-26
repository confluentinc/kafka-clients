---
name: phase9-0-sasl-wire-types
description: SASL handshake/authenticate wrappers — generator behavior, flex-boundary fixture process, error-mapping decision
metadata:
  type: project
---

Phase 9.0 closed (4 commits). Key learnings for downstream phases.

**Generator works out of the box for SASL.** The 4 JSON specs
(`SaslHandshake{Request,Response}.json`, `SaslAuthenticate{Request,Response}.json`)
already exist in `generator/messages/` and the build.rs already runs
the generator over them. Output lands in
`$OUT_DIR/generated/sasl_*_data.rs`. Did NOT need to modify
generator or build.rs — just add `pub mod sasl_*_data { include!(...) }`
to `src/common/message/mod.rs`.

**Hex-fixture hand-derivation matched generator output byte-for-byte.**
All 14 hex fixtures in commit 3 (`884c8c1`) were hand-derived from the
JSON specs and then asserted against `AbstractRequest::serialize()`
output. They all passed first try. This is **evidence the
generator-vs-spec alignment is correct** for `flexibleVersions: "none"`
and `flexibleVersions: "2+"` patterns — at least to the resolution
of "two independent encodings of the same spec agree." Residual risk:
Java-runtime cross-verification still pending; carry to 9c.

**Why: When 9c stands up a SASL Testcontainer broker, capture real
wire bytes (wireshark + or pipe proxy) for the same inputs the
fixtures use and assert byte-identical. If divergent, the bug is
spec-misinterpretation shared by generator and fixtures.**

**How to apply: trust the generator for these specs going forward in
9a/9b; the v2 compact-bytes + varint tagged-trailer encoding is
verified at the spec level.**

**Error mapping decision: did NOT introduce
`KafkaError::UnsupportedSaslMechanism`.** Java's
`UnsupportedSaslMechanismException` extends `AuthenticationException`,
and our `KafkaError::Authentication(String)` already covers wire code
58 + the broader semantic class. Wire code 33
(`UNSUPPORTED_SASL_MECHANISM`) is preserved in `Errors` enum at
`protocol/errors.rs:75`. If 9b's config validator needs a more
specific variant for `sasl.mechanism = SCRAM-SHA-512` rejection at
construction time, add it then.

**parse_response_body extended in 9.0, not deferred.** Added api keys
17 and 36 to the match arm in `abstract_response::parse_response_body`.
Mechanical change, keeps 9a focused on `SaslChannelBuilder` /
state-machine work. Downstream phases can `parse_response()` for
SASL replies without additional plumbing.

**Builder parity.** Both message types have Java
`public static class Builder` — translated to
`SaslHandshakeRequestBuilder` and `SaslAuthenticateRequestBuilder`,
each implementing `AbstractRequestBuilder` with
`oldest_allowed_version` / `latest_allowed_version` reading from the
`ApiKey` registry. Pattern is identical to
`ApiVersionsRequestBuilder`.

**Debug masking for credential-carrying types.** Java overrides
`toString()` on `SaslAuthenticateRequest`/`Response` to hide
`authBytes`. Translated as manual `impl fmt::Debug` rather than
deriving — emits `auth_bytes: "<redacted>"`. There's no `Display`
override because Rust callers will typically `{:?}` for logging.
Generated `*_data.rs` Display impl uses `{:?}` which would leak the
bytes; users should print the wrapper, not the data struct.

**Test count: 1233 → 1275 (+42).** 28 round-trip + 14 hex-fixture.
