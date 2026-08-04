---
name: m11-tier3-phase2-client-quotas
description: M11 Tier3 P2 client quotas — ClientQuotaMatch tri-state enum, generated quota message data pre-existed, manual Hash for map-keyed entity
metadata:
  type: project
---

Milestone 11 Tier 3 Phase 2 (client quotas): describeClientQuotas +
alterClientQuotas. Plain `Call` with `NodeProvider::LeastLoaded`. Landed on
`dev/adminclient_translation_and_bindings` (base was d2b6e36).

**Key translation decisions:**
- `ClientQuotaFilterComponent.match()` is `Optional<String>` with a
  meaningful tri-state (present=exact, empty=default, null=any). Modeled as a
  new `ClientQuotaMatch { Exact(String), Default, Any }` enum — folding
  empty/null together would break equals + wire match-type encoding. Wire
  match types: EXACT=0, DEFAULT=1, SPECIFIED(any)=2; DEFAULT and ANY both send
  a null match string.
- `ClientQuotaAlteration.Op` value is `Option<f64>`; `None` = removal, encoded
  as `remove=true` + placeholder `value=0.0`, decoded back to `None`.
- `ClientQuotaEntity` wraps `HashMap<String, Option<String>>` (fix cycle 1:
  was `HashMap<String,String>`) and is used as a HashMap key → manual
  order-independent `Hash` impl (HashMap isn't Hash). PartialEq compares the
  map directly. **`None` value = the built-in DEFAULT entity** (Java's nullable
  `Map<String,String>` value; `null`→default per javadoc). `None` encodes to
  wire-null entity name (`0x00`), `Some(name)` to non-null (possibly empty →
  `0x01`). Decode preserves the distinction verbatim — do NOT `unwrap_or_default()`
  the wire `entity_name` (that collapses default vs `""`). 3 encode + 3 decode
  sites across alter req + alter/describe resp. Byte-level test asserts
  `bytes[7]==0x00` for default vs `0x01` for `Some("")`.
- `ApiError` has no Rust equivalent; `AlterClientQuotasResponse.from_quota_entities`
  takes `&[(ClientQuotaEntity, Errors, Option<String>)]` instead of
  `Map<ClientQuotaEntity, ApiError>` (matches the `api_error(code,msg)` convention).

**Gotcha:** the generated wire message data (`describe_client_quotas_request_data`
etc.) ALREADY existed under OUT_DIR/generated (JSON specs live in
`generator/messages/`), so no generator work was needed — only the hand-written
`common/requests/*_client_quotas_{request,response}.rs` wrappers + enum wiring.

**Mock:** Java `MockAdminClient` throws `UnsupportedOperationException("Not
implement yet")` (note the typo — NOT "implemented") for both RPCs
(MockAdminClient.java:1243-1250); Rust returns exceptional futures with that
exact message per admin-client.md §9.

Tests: 33 new lib tests (2709→2742), incl. byte-level wire vectors for
match-type + remove-flag. 3 integration tests pass against real broker.
`cargo xtask lint` does NOT compile integration tests (`--all-targets` without
`--features integration-tests`) — pre-existing clippy issues in OTHER
integration files surface only under the feature flag, none in new files.
