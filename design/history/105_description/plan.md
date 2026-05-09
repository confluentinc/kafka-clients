# Translation Plan: MINOR — Bump LATEST_PRODUCTION to 4.2-IV1

**AK commit:** `fb68ada1a23a6563945249b6bccf531a8c97bd12`
**AK branch:** trunk
**PR:** #105
**Rust branch:** `kafka-translate/fb68ada1a23a6563945249b6bccf531a8c97bd12`

---

## Summary of the Apache Kafka Commit

This commit bumps the `LATEST_PRODUCTION` metadata version constant from
`IBP_4_1_IV1` to `IBP_4_2_IV1`. This promotes two previously-unstable metadata
versions to production status:

- **IBP_4_2_IV0** (version 28) — Enables share groups by default for new
  clusters (KIP-932).
- **IBP_4_2_IV1** (version 29) — Enables "streams" groups by default for new
  clusters (KIP-1071).

Additionally, a new unstable version **IBP_4_3_IV0** (version 30) is introduced
for the upcoming Kafka 4.3.0 release cycle.

**Changed files:**

| File | Change |
|------|--------|
| `server-common/.../MetadataVersion.java` | Move 4.2-IV0/IV1 above the unstable marker; add 4.3-IV0; bump `LATEST_PRODUCTION` |
| `tests/kafkatest/version.py` | Update `LATEST_STABLE_METADATA_VERSION` to `"4.2-IV1"` |
| `metadata/.../FormatterTest.java` | Update test expectations for new latest production version |
| `server-common/.../MetadataVersionTest.java` | Update test expectations |
| `test-common/.../ClusterTest.java` | Update default annotation metadata version |
| `tools/.../FeatureCommandTest.java` | Update test expectations |

---

## Rust Translation Analysis

### Does this code exist in Rust?

No. The Rust codebase is a **client library**. It does not implement the
server-side `MetadataVersion` enum, KRaft metadata formatting, or broker
feature-level management. The concept of `LATEST_PRODUCTION` is exclusively a
broker/controller concern — it determines which inter-broker protocol features
are considered stable for production clusters.

The Rust client interacts with metadata versions only indirectly:
- It sends `ApiVersions` requests and parses `ApiVersionsResponse` to negotiate
  protocol versions with brokers.
- It does not need to know which metadata version a cluster is running at;
  protocol negotiation handles compatibility automatically.

### Is there production code to translate?

No. There is no Rust equivalent of `MetadataVersion.java` and no client-side
logic that references `LATEST_PRODUCTION` or specific `IBP_*` constants.

### Are there tests to translate?

No. The changed test files (`FormatterTest`, `MetadataVersionTest`,
`FeatureCommandTest`, `ClusterTest`) are all server-side tests with no
client-library equivalents.

---

## Implementation Plan

### Verdict: No translation required

This commit is **not applicable** to the Rust client library. It is a
server-side-only change that:

1. Promotes metadata versions to production status — a broker/controller concept.
2. Adds a new unstable metadata version for future development.
3. Updates server-side tests and tooling.

None of these changes affect client protocol behavior, message schemas, or
client-side logic.

---

## Files to Create / Modify

| File | Action | Reason |
|------|--------|--------|
| (none) | — | No Rust code changes needed |

---

## Out of Scope

- Implementing a `MetadataVersion` enum in Rust — not needed for a client
  library; protocol negotiation is handled via `ApiVersions`/`ApiKeys`.
- Share groups (KIP-932) or streams groups (KIP-1071) client support — these
  are separate features that will be translated when their client-facing APIs
  are implemented.

---

## Definition of Done

- [x] Design document written confirming no translation is needed.
- [ ] `cargo build` succeeds (no changes to verify, but confirms no regression).
- [ ] `cargo test` passes (no changes to verify, but confirms no regression).
