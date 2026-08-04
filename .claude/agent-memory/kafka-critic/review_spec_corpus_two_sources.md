---
name: review-spec-corpus-two-sources
description: Wire-protocol spec claims must name which of the TWO spec corpora they measured — generator/messages (pre-4.2, what the Rust build reads) vs kafka/ (4.2.0, the CLAUDE.md Source Reference); they disagree on 36 of 197 files
metadata:
  type: project
---

This repo has **two** message-spec corpora and they are not in sync. Any review
claim about a spec property (`latestVersionUnstable`, `validVersions`,
`ignorable`, field versions) is ambiguous until it names which one.

  - `generator/messages/` — 197 JSON specs, **what `build.rs` actually compiles**
    (`generator::generate_messages(Path::new("generator/messages"), ...)`).
  - `kafka/clients/src/main/resources/common/message/` — Apache Kafka 4.2.0, the
    CLAUDE.md "Source Reference".

**Why:** `generator/messages/` is a pre-4.2 snapshot, never refreshed since the
initial branch commit. 36 of 197 files differ, consistently *older* — e.g.
`ListOffsetsRequest` `validVersions` 1-10 vs 4.2's 1-11 (KIP-1023 v11 absent).
The consequential ones are four `latestVersionUnstable: true` flags that 4.2
does **not** set: `OffsetCommitRequest`, `OffsetFetchRequest`,
`StreamsGroupHeartbeatRequest`, `StreamsGroupDescribeRequest`. Only
`InitProducerIdRequest` carries the flag in both. So Rust's
`ApiKeys::OFFSET_COMMIT.latest_version_with_unstable(false)` returns 9 where
Java 4.2 returns 10. Latent, because no production site consults that accessor
for those APIs — but PLAN §9.7 proposes work that would.

**How to apply:**

  - Before reporting or accepting a spec-flag claim, grep **both** trees and say
    which one the number came from. Three findings in the Phase 2 loop (Critic 42
    issues 9, 11, 12) were the same error: a flag claim not tied to an artifact.
  - The generated table is authoritative for *Rust behaviour*:
    `target/debug/build/confluent-kafka-rust-*/out/generated/api_message_type.rs`,
    functions `latest_version_unstable()` and `highest_supported_version(bool)`.
    The `highest - 1` formula matches Java's
    `ApiMessageTypeGenerator.java:427-438` exactly — divergences come from the
    spec input, never the arithmetic.
  - `design/current/design.md` claims the generator specs are "from Apache Kafka
    4.2". They are not. Do not treat that line as verification.
  - Check the flag's **value**, not its presence: an absent property means
    `false` (`#[serde(default)]`), and `"latestVersionUnstable": false` is
    explicitly written in several specs.

See also [[review_m11_phase2_docs_fix_loop]].
