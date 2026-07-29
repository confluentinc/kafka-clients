---
name: m11-tier3-phase4-delegation
description: M11 Tier3 Phase4 delegation tokens review — mock-as-spec fidelity, Eq/Hash Java-bug deviation, nullable-owner default, hand-rolled base64 adjudications (CLEAN)
metadata:
  type: project
---

M11 Tier 3 Phase 4 (delegation tokens: create/renew/expire/describe, plain `Call` +
`LeastLoadedNodeProvider`) reviewed CLEAN — no COMMENTS.1.md issues. Key adjudications
worth reusing on future security/POJO-heavy admin phases:

**Java Eq/hashCode inconsistency → Rust must diverge, and that's correct.**
`TokenInformation.equals` excludes `expiryTimestamp` but `hashCode` INCLUDES it (a Java
bug). Rust's Eq/Hash contract forbids replicating this. Actor made `Hash` consistent with
`eq` (both exclude expiry). This is *forced* by Rust, documented, and the type is never a
map key — defensible, NOT a defect. Verify `eq` itself matches Java's field set exactly
(it did: owner, tokenRequester, renewers, issue/max ts, tokenId — no expiry).

**Nullable string default = empty `""` (0x01 compact), not null (0x00).**
Confirmed against Java generator `FieldSpec.java` `fieldDefault()`: StringFieldType returns
`"null"` ONLY when spec has explicit `"default":"null"`; otherwise `"\"\""` (empty). So a
`nullableVersions`-without-`default:null` field (e.g. Create's OwnerPrincipalType/Name)
serializes as empty compact string 0x01, matching CLAUDE.md §2 AND Java. When an Actor
claims "unset field serializes as 0x01 not null," verify via FieldSpec, not intuition.

**Mock-as-de-facto-spec: scrutinize the mock hardest when Java has zero client unit tests.**
Verified Rust mock method-by-method vs MockAdminClient.java:641-725. One documented
deviation: Java `describeDelegationToken` NPEs on null owners (`options.owners().isEmpty()`
on null); Rust maps `None`→all-tokens. Defensible because it matches the REAL
DescribeDelegationTokenOptions javadoc contract ("null → describe all") — replicating an
untested Java-mock NPE would be pointless. §9 "mirror exactly, don't fix" yields to the
documented real-client contract here.

**Hand-rolled standard base64 (padding, +/) — adjudicated OK.** uuid.rs has a base64 but
it's URL-safe + unpadded + private, so not reusable. New ~25-line encoder verified correct
against RFC 4648 vectors incl. the `+`/`/` alphabet (`[0xFB,0xFF,0xFE]`→`+//+`). §1.1 would
prefer asking for a crate, but hand-rolling matches uuid.rs precedent — not a defect.

**Non-reported minor:** mock `create_delegation_token` panics on empty renewers via `[0]`
indexing, faithfully mirroring Java `renewers().get(0)` IndexOutOfBounds. §9 (mirror mock)
vs §10.1 (no panic) conflict; §9 wins for a test helper faithfully mirroring a Java runtime
exception. Flagged to Manager as awareness, not written to COMMENTS.

**Convention confirmed:** Rust `Admin` trait omits Java's no-arg convenience default
overloads (`createDelegationToken()`, etc.) — same as all prior phases (list_topics,
create_acls). Do NOT flag this per-phase; it's an established project-wide decision.
