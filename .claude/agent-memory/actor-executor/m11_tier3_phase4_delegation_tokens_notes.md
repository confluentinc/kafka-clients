---
name: m11-tier3-phase4-delegation-tokens
description: M11 Tier 3 Phase 4 delegation tokens — prereqs, wire, admin+mock, SASL integration gap
metadata:
  type: project
---

Milestone 11 Tier 3 Phase 4 (delegation tokens) landed in 4 commits on
`dev/adminclient_translation_and_bindings` (parent of ac4ca50).

**Why / how to apply:** context for the delegation-token slice and the admin
SASL gap that blocks its integration coverage.

- Prereqs (finding #5 confirmed absent, translated new): `common::security::auth::KafkaPrincipal`,
  `common::security::token::delegation::{DelegationToken,TokenInformation}`. Reused generated
  `*_delegation_token_{request,response}_data` structs (already emitted by the message generator).
- `TokenInformation` Java `equals` ignores `expiryTimestamp` but `hashCode` includes it (Java bug);
  translated eq faithfully and made Hash consistent with eq (excludes expiry) — documented deviation.
- `DelegationToken.hmacAsBase64String` uses STANDARD base64 (padding, `+/`), not the URL-safe
  encoder in `uuid.rs`; wrote a from-scratch encoder with RFC-4648 known vectors.
- Wire: 4 request + 4 response wrappers; all `ConcreteRequest`/`ConcreteResponse` match arms wired.
  Latest versions: Create=3, Renew=2, Expire=2, Describe=3 (all flexible `2+`). Create request's
  unset owner principal fields encode as EMPTY compact string (0x01), not null (0x00) — no explicit
  `default:null` in the spec, so generator defaults to `Some(String::new())`.
- Admin RPCs use plain `Call` + `NodeProvider::LeastLoaded`; sync `fn` returning `*Result`.
- MockAdminClient mirrors Java ~641-725: in-memory `all_tokens: Vec<DelegationToken>`,
  USER_TYPE renewer validation (InvalidPrincipalType), owner = first renewer, HMAC = token-id bytes,
  `-1` expire sentinel removes token, owners describe filter (null/empty → all).
- Unit tests are NEW (finding #10: zero Java client-side delegation-token tests exist), written
  against the mock and labeled as such.

**Admin SASL gap (blocks delegation-token integration test):** `AdminClientConfig` has NO
security.protocol/sasl fields and `KafkaAdminClient::from_config` hardcodes
`SecurityProtocol::Plaintext` for its channel builder. The low-level Selector supports SASL/PLAIN
(see ssl_sasl_test.rs) but the AdminClient can't be configured to use it. Delegation-token CREATE
requires a SASL-authenticated (non-token) connection, so the integration test is not feasible
without first wiring SASL into the admin client — deferred, not hacked. Behavioral contract is
covered by the mock unit tests.

`cargo test --lib`: 2747 → 2808.
