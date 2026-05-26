---
name: phase9_0-review-patterns
description: Phase 9.0 (SASL wire-types) review patterns — hand-derived hex fixtures, credential redaction at wrapper-vs-data-class boundary, generator Display/Debug leak vector
metadata:
  type: project
---

# Phase 9.0 SASL wire-type review patterns

Phase 9.0 added hand-written wrappers + 14 hex-byte fixtures + 28 round-trip tests over the generator-emitted SASL message types. Recurring review patterns to carry forward:

## Pattern 1: Hand-derived hex fixtures — verify generator/spec common-mode risk

When fixtures are hand-derived from the JSON spec (not captured from a running Java client), the test asserts that the generator and the hand-derivation **agree** on the spec interpretation. If both share a misreading, the test passes but is wire-incompatible with Java.

**Review action**: independently re-derive the bytes from the JSON spec + Kafka protocol-guide encoding rules. Walk byte-by-byte. If your derivation matches the fixture, you've at minimum proven the fixture is internally consistent with the spec — the residual risk is that the spec interpretation itself is wrong, which only a runtime capture against Java will resolve.

The flex-boundary fixtures (v2 of both `SaslAuthenticate*`) are the highest-priority manual verifications because the encoding differs entirely from v0/v1.

## Pattern 2: Credential-redaction in Debug — wrapper-level only protects the wrapper type

Java's `SaslAuthenticateRequest.toString()` overrides clear `authBytes` via `data.duplicate()`. The Rust translation puts `impl fmt::Debug` on the wrapper type only — `{:?}` on the wrapper is safe. **But:**

- `format!("{:?}", req.request_data())` calls the derived Debug on `SaslAuthenticateRequestData` — **leaks**.
- `format!("{}", req.request_data())` goes through the generator-emitted `impl fmt::Display { write!(f, "{:?}", self) }` — **also leaks** via the derived Debug.

Java has the same leak (Java's `*Data.toString()` prints `authBytes`); Rust adds the Display variant. Future SASL state machine work (9a) will likely log via `tracing::debug!("data = {}", data)` and silently leak. Flag as Suggestion to address before 9a's logging callsites land.

## Pattern 3: Java tests using SASL as transport for codec corruption

`testInvalidSaslHandShakeRequest`, `testInvalidSaslAuthenticateRequest`, `testInvalidTaggedFieldsWithSaslAuthenticateRequest` in `RequestResponseTest.java` use SASL message types but test the underlying `Readable` corruption error path. Equivalent Rust coverage exists at `byte_buffer_accessor.rs:380-410` + `types/type.rs:433-525`. Acceptable skip per DoD #3 "not relevant to the Rust codebase" — but the close stanza should explicitly enumerate these skips for audit trail.

## Pattern 4: Builder/registry version range coupling

Both SASL builders source `oldest_allowed_version()` / `latest_allowed_version()` from `ApiKey::oldest_version()` / `latest_version()`, which in turn sources from the generator-emitted `ApiMessageType::lowest_supported_version()` (JSON-derived). So a JSON-spec update auto-propagates through the build to the builder bounds — no hand-coded version literals in the wrapper layer.

Verify by checking the JSON `validVersions`: SaslHandshakeRequest = `"0-1"` → builder `0..=1`; SaslAuthenticateRequest = `"0-2"` → builder `0..=2`. Tests assert these literals explicitly — good regression pin.

## Pattern 5: Nullable string default trap (CLAUDE.md rule)

`SaslAuthenticateResponse.json` has `ErrorMessage` field with `"nullableVersions": "0+"` but no `"default": "null"`. Per CLAUDE.md rule 2, default must be `Some(String::new())`, not `None`. Generated code does this correctly (line 63 of `sasl_authenticate_response_data.rs`). **But:** fixtures that test the null branch (e.g. `hex_fixture_v0_success_null_message`) construct `error_message: None` explicitly and emit `FF FF` — which is a valid wire encoding but is NOT what `new()` produces. Both behaviours are correct; just worth being aware that the fixture exercises a non-default path.

## Pattern 6: `parse_response_body` dispatch arm verification

When new API keys are added to the dispatch table, verify by inspection:
1. arm key id matches the JSON spec's `apiKey`
2. arm dispatches to the **matching** response type (key 17 → `SaslHandshakeResponse::parse`, NOT `SaslAuthenticateResponse::parse`)
3. no off-by-one — a swap would compile, pass round-trip on the data type, and only fail at runtime against a real broker.

## Pattern 7: Test panic-message leak vector

When `assert!(!debug_str.contains(secret), "leaked: {debug_str}")`, the panic message echoes the leaked content. In CI logs this re-leaks. Use a sentinel-only check: `assert!(!dbg.contains(SENTINEL), "leaked! len={}", dbg.len())` so the failure mode doesn't surface the secret.
