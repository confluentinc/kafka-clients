---
name: review-spec-corpus-two-sources
description: Wire-protocol spec claims must still name which of the TWO spec corpora they measured — generator/messages (what the Rust build reads) vs kafka/ (the CLAUDE.md Source Reference); both are synced to 4.3.1 as of Milestone-13 Phase 0
metadata:
  type: project
---

This repo has **two** message-spec corpora. As of **Milestone-13 Phase 0**
they are **in sync** — both track **Apache Kafka 4.3.1**. Any review claim
about a spec property (`latestVersionUnstable`, `validVersions`, `ignorable`,
field versions) should still name which corpus it measured, because the two are
maintained separately and could drift again in a future milestone.

  - `generator/messages/` — 197 JSON specs plus `ControlRecordTypeSchema.json`,
    **what `build.rs:44` actually compiles**
    (`generator::generate_messages(Path::new("generator/messages"), ...)`).
    Synced to 4.3.1 in Milestone-13 Phase 0.
  - `kafka/clients/src/main/resources/common/message/` — the `kafka/` submodule
    at tag `4.3.1` (`26b251a451`), the CLAUDE.md "Source Reference".

**History / correction:** an earlier version of this memory claimed
`generator/messages/` was a *pre-4.2 snapshot* with *36 of 197 files* drifting
older (and four extra `latestVersionUnstable: true` flags). **That claim was
stale/incorrect.** Milestone-13 Phase 0 verified (and the measured 4.2.0→4.3.1
delta corroborated) that the corpus matched **4.2.0 exactly** before that phase
— the diff against the 4.3.1 source was exactly the 10 modified + 1 new
(`ControlRecordTypeSchema.json`) + README delta, with no older-version drift.
Phase 0 then copied those 4.3.1 specs in, so the corpus now matches 4.3.1
exactly. Do not cite the old "36/197 drift" or "four flags differ" figures.

**Flag-consumer audit (still useful):** whether a `latestVersionUnstable`
divergence would bite is answered by auditing the small set of flag consumers,
not by reasoning about the accessor in the abstract:

  - `latest_version_unstable()` has no caller outside the generated file.
  - `latest_version_with_unstable(false)` in production appears only in the five
    Phase 2 txn builders.
  - `ApiKeys::is_version_enabled` and `to_api_version_internal` are the only other
    readers; they are reached solely from `filter_apis` / `collect_apis` /
    `intersect_forwardable_apis` / `default_api_versions_response*` in
    `api_versions_response.rs`, and **every caller in the tree passes `true`**.
  - `NodeApiVersions` goes through `ApiVersionsResponse::to_api_version`, which uses
    the unstable-**inclusive** `latest_version()` — faithful to Java's
    `ApiVersionsResponse.toApiVersion(ApiKeys)`.

**How to apply:**

  - Before reporting or accepting a spec-flag claim, grep **both** trees and say
    which one the number came from. Three findings in the Phase 2 loop (Critic 42
    issues 9, 11, 12) were variants of the same error: a flag claim not tied to an
    artifact. This discipline survives the sync — the corpora are separate files
    that can diverge again.
  - The generated table is authoritative for *Rust behaviour*:
    `target/debug/build/confluent-kafka-rust-*/out/generated/api_message_type.rs`,
    functions `latest_version_unstable()` and `highest_supported_version(bool)` —
    the latter ends `if !self.latest_version_unstable() || enable_unstable { highest }
    else { highest - 1 }`. That matches Java's
    `ApiMessageTypeGenerator.java` exactly — divergences come from the
    spec input, never the arithmetic.
  - Check the flag's **value**, not its presence: an absent property means
    `false` (`#[serde(default)]`), and `"latestVersionUnstable": false` is
    explicitly written in several specs — see [[review-m11-phase2-docs-fix-loop]]
    for the txn specs where this asymmetry actually caused a defect.

See also [[review-m11-phase2-docs-fix-loop]].
