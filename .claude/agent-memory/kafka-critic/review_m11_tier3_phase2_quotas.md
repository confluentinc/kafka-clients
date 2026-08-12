---
name: review-m11-tier3-phase2-quotas
description: M11 Tier3 Phase2 client-quotas review — nullable map-value fidelity gap; match-type/remove wire codes verified clean
metadata:
  type: project
---

M11 Tier 3 Phase 2 (client quotas: describeClientQuotas / alterClientQuotas) review, commits `d2b6e36..85c6c44`.

**Confirmed CLEAN (verified against Java + specs):**
- Match-type wire codes EXACT=0 / DEFAULT=1 / SPECIFIED=2 match `DescribeClientQuotasRequest.java:32-34`. Rust `Any→SPECIFIED`, `Exact→EXACT`, `Default→DEFAULT` correct; decode inverse correct.
- `Op` null-value=removal: encode `value=0.0 + remove=true`, decode `remove?None:Some(value)` — byte-faithful to Java `AlterClientQuotasRequest.java:58-59,100`. `Some(0.0)` survives distinct from `None` (remove=false vs true).
- `ClientQuotaMatch{Exact,Default,Any}` NEW enum is a justified translation of Java `Optional<String>` tri-state (Default=Optional.empty ≠ Any=null); preserves equals + wire. DoD #7 OK.
- Enum wiring (ConcreteRequest/ConcreteResponse) fully wired all arms. Byte-level known-vector tests hand-computed, non-self-referential, all 33 tests pass.
- Mock stubs faithful: Java `MockAdminClient` throws `UnsupportedOperationException("Not implement yet")` (note Java typo "implement") — Rust returns `unsupported_version("Not implement yet")` matching typo, cites line. §9 OK.
- alter `handle_response` skip-unknown-entity (vs Java `IllegalArgumentException`) is documented + safer (no hang for requested entities) — defensible deviation, not a defect.
- DoD #3: no dedicated Java test files for `common/quota/*` — genuine upstream gap (verified), compensating equals/round-trip tests adequate. Java `testAlterClientQuotas` asserts exception TYPE not message → Rust `.error()` code assertion equivalent fidelity.

**THE finding (Behavior Mismatch):** `ClientQuotaEntity` uses `HashMap<String,String>` but Java is `Map<String,String>` where **null value = default quota entity** (`--entity-default`). Rust cannot represent null → (1) can't express default entity, (2) encodes `Some("")` not wire-null → broker sees entity named "" not default = wire-incompatible, (3) decode coerces null→"" losing distinction. Spec `EntityName` is `nullableVersions 0+`. Fix: `HashMap<String, Option<String>>`.

**Lesson:** When a Java POJO stores `Map<String,String>` (or any collection with null values) as a domain type, check whether null is *semantically meaningful* on the wire (here: default entity). A `HashMap<String,String>` translation silently drops that. Same trap likely in future admin types carrying nullable map/entity names.
