---
name: review-m11-phase1-admin
description: Milestone 11 (AdminClient) Phase 1 review-only skeleton patterns — derive-vs-Java-equals, §12 by-value inputs, trait-as-map-value
metadata:
  type: project
---

Milestone 11 = AdminClient translation (`org.apache.kafka.clients.admin` -> `admin`),
branch `dev/adminclient_translation_and_bindings`. Phase 1 was a review-only API
skeleton (unimplemented! bodies, cataloged types as comments, not wired into lib.rs).

**Why:** Admin methods are sync-returning-futures, NOT async — every RPC returns a
`*Result` holding one `KafkaFuture<T>` per key immediately; only `close()` and
`client_instance_id()` block in Java, so only those two are `async fn`. `#[async_trait]`
belongs on the `Admin` trait ONLY (for those two), never on POJO/Result/Options types.

**How to apply — recurring findings to check in Admin (and any POJO) translation:**

- **Derived `PartialEq`/`Eq` vs Java's hand-written equals/hashCode.** Java Admin
  POJOs frequently EXCLUDE a field from equals/hashCode. Confirmed case:
  `TopicDescription.java:40-54` excludes `topicId`; Rust `#[derive(PartialEq,Eq)]`
  wrongly includes it. Always diff the Java `equals()`/`hashCode()` field list
  against Rust's derived-all-fields. (NewTopic includes all 5 fields → derive OK.
  TopicListing/CreateTopicsOptions have NO Java equals (identity) → deriving is a
  benign additive enhancement, NOT a finding.)
- **§12 borrowing inconsistency in the trait.** Slice/map args were borrowed
  (`&[NewTopic]`, `&HashMap`) but single filters/collections were by-value
  (`TopicCollection`, `AclBindingFilter`, `ClientQuotaFilter`, `AbortTransactionSpec`,
  `Option<HashSet<..>>`). Flag by-value non-Copy read-only inputs.
- **`metrics()` return** `HashMap<MetricName, Metric>` uses bare unsized trait as
  map value — needs `Box<dyn Metric>`. Java `Map<MetricName, ? extends Metric>`.
- **Catalog completeness (comment-only design record).** Verify against Java: caught
  `OffsetSpec` catalog missing `EarliestPendingUploadSpec` (7 subclasses, not 6).

**Trait surface was complete & faithful:** all Tier 1-3 canonical (options-taking,
abstract) methods present; all Tier 4 (Streams-groups, Share/KIP-932, raft-voter:
addRaftVoter/removeRaftVoter/describeMetadataQuorum/unregisterBroker) correctly
excluded. KafkaFuture reused from `crate::common::KafkaFuture`.
