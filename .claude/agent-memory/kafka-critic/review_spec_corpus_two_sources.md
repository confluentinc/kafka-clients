---
name: review-spec-corpus-two-sources
description: Wire-protocol spec claims must name which of the TWO spec corpora they measured — generator/messages (pre-4.2, what the Rust build reads) vs kafka/ (4.2.0, the CLAUDE.md Source Reference); they disagree on 36 of 197 files
metadata:
  type: project
---

This repo has **two** message-spec corpora and they are not in sync. Any review
claim about a spec property (`latestVersionUnstable`, `validVersions`,
`ignorable`, field versions) is ambiguous until it names which one.

  - `generator/messages/` — 197 JSON specs, **what `build.rs:44` actually compiles**
    (`generator::generate_messages(Path::new("generator/messages"), ...)`).
  - `kafka/clients/src/main/resources/common/message/` — Apache Kafka 4.2.0, the
    CLAUDE.md "Source Reference". 197 specs plus a `README.md`.

**Why:** `generator/messages/` is a pre-4.2 snapshot, never refreshed since the
initial branch commit (`git log --oneline -- generator/messages/` → one commit,
`6cd275c`). 36 of 197 files differ, consistently *older* — e.g.
`ListOffsetsRequest` `validVersions` 1-10 vs 4.2's 1-11 (KIP-1023 v11 absent).
The consequential ones are four `latestVersionUnstable: true` flags that 4.2
does **not** set: `OffsetCommitRequest`, `OffsetFetchRequest`,
`StreamsGroupHeartbeatRequest`, `StreamsGroupDescribeRequest`. Only
`InitProducerIdRequest` carries the flag in both. So Rust's
`ApiKeys::OFFSET_COMMIT.latest_version_with_unstable(false)` returns 9 where
Java 4.2 returns 10, and the Streams pair returns -1 where Java returns 0.
Tracked as PLAN §9.9 (to be sequenced with the §9.2 4.3.1 migration).

**Latent, and pass 6 verified why** — the full set of flag consumers is small:

  - `latest_version_unstable()` has no caller outside the generated file.
  - `latest_version_with_unstable(false)` in production appears only in the five
    Phase 2 txn builders, whose specs are flag-identical across both corpora.
  - `ApiKeys::is_version_enabled` and `to_api_version_internal` are the only other
    readers; they are reached solely from `filter_apis` / `collect_apis` /
    `intersect_forwardable_apis` / `default_api_versions_response*` in
    `api_versions_response.rs`, and **every caller in the tree passes `true`**.
  - `NodeApiVersions` goes through `ApiVersionsResponse::to_api_version`, which uses
    the unstable-**inclusive** `latest_version()` — faithful to Java's
    `ApiVersionsResponse.toApiVersion(ApiKeys)`.

So a "does this flag divergence bite?" question resolves by auditing that list, not
by reasoning about the accessor in the abstract. It becomes live the moment a new
builder or negotiation path asks for the *released* ceiling of an affected API.

**How to apply:**

  - Before reporting or accepting a spec-flag claim, grep **both** trees and say
    which one the number came from. Three findings in the Phase 2 loop (Critic 42
    issues 9, 11, 12) were variants of the same error: a flag claim not tied to an
    artifact.
  - The generated table is authoritative for *Rust behaviour*:
    `target/debug/build/confluent-kafka-rust-*/out/generated/api_message_type.rs`,
    functions `latest_version_unstable()` and `highest_supported_version(bool)` —
    the latter ends `if !self.latest_version_unstable() || enable_unstable { highest }
    else { highest - 1 }`. That matches Java's
    `ApiMessageTypeGenerator.java:427-438` exactly — divergences come from the
    spec input, never the arithmetic.
  - `design/current/design.md` claims the generator specs are "from Apache Kafka
    4.2". They are not. Do not treat that line as verification.
  - Check the flag's **value**, not its presence: an absent property means
    `false` (`#[serde(default)]`), and `"latestVersionUnstable": false` is
    explicitly written in several specs — see [[review-m11-phase2-docs-fix-loop]]
    for the txn specs where this asymmetry actually caused a defect.

See also [[review-m11-phase2-docs-fix-loop]].
