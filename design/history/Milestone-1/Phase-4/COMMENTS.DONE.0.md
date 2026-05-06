# Phase 4a Review — Resolved Items (Critic N=0)

Reviewed commit: `2739f7f` ("Phase 4a: cluster data types
(Node, Cluster, TopicPartition, ...)")

Java reference tree: `kafka/clients/src/main/java/org/apache/kafka/common/`
(plus `clients/admin/TopicCollectionTest.java` for tests).

---

## Findings

### Issue 1 — DEFERRED-OK (Minor) — `TopicIdPartition::to_string` for empty topic does not match Java's `null-1` literal

- **File:** `src/common/topic_id_partition.rs:67-70` (Display impl) and
  `src/common/topic_id_partition.rs:144-149` (test expectation)
- **Java reference:**
  `kafka/clients/src/test/java/org/apache/kafka/common/TopicIdPartitionTest.java:79`
  asserts the literal string
  `"vDiRhkpVQgmtSLnsAZx7lA:null-1"` for the null-topic case.
- **Behavior delta:** The translated Rust test asserts
  `"vDiRhkpVQgmtSLnsAZx7lA:-1"` — i.e. an empty topic name renders
  as nothing between the colon and the dash, not the literal text
  `null`. Java prints `null` because Java's `String + null` operator
  inserts the four-character string `"null"`.
- **Why DEFERRED-OK:** The Actor's choice to encode "no topic" as
  `Arc::from("")` rather than introducing an `Option<Arc<str>>` topic
  field on every `TopicPartition` (which would propagate an extra
  branch through every hot-path lookup) is consistent with CLAUDE.md
  rule 11. The Phase 4a `NOTES.md` is silent on this case but the
  `Arc<str>` choice is mandatory there.
  No in-tree caller of `TopicIdPartition::to_string()` (logs,
  diagnostic strings) currently asserts on the literal `null`
  substring. If a future `Metadata` log line or admin-tool output
  expects `null`, this becomes a real Bug — flag for re-check in
  Phase 4b when `Metadata.java` is translated.
- **Suggestion:** Document this delta in
  `src/common/topic_id_partition.rs` rustdoc above `impl Display`
  ("Java's null-topic case prints `null-N`; Rust uses `-N` since the
  topic is `Arc<str>`."). The Actor already has a comment in the
  test, but it is not visible in the Display rustdoc itself.

**Resolution:** Fixed in `e109708` (`fixup! Phase 4a: cluster data
types ...`). Added a rustdoc block above `impl Display for
TopicIdPartition` documenting the Java/Rust delta with concrete
example output (`vDiRhkpVQgmtSLnsAZx7lA:null-1` vs
`vDiRhkpVQgmtSLnsAZx7lA:-1`) and a flag for future readers that
this `Display` impl is the only place to fix if Java parity is
later required.

### Issue 2 — Performance — `TopicPartition::hashCode` is not cached

- **File:** `src/common/topic_partition.rs:70-79`
- **Java reference:**
  `kafka/clients/src/main/java/org/apache/kafka/common/TopicPartition.java:28,46-54`:

      private int hash = 0;
      ...
      @Override public int hashCode() {
          if (hash != 0) return hash;
          ...
          this.hash = result;
          return result;
      }

  Java caches the int hash because the comment in `Node.java:35`
  notes `hashCode` "is called in performance sensitive parts of the
  code (e.g. RecordAccumulator.ready)" — and the same applies to
  `TopicPartition`, the *key* of the producer's
  `Map<TopicPartition, Deque<ProducerBatch>>`.
- **Description:** Rust derives `Hash` through `(*self.topic).hash`
  which iterates the entire topic-name string each call. For a
  busy `RecordAccumulator` keying on `TopicPartition` thousands of
  times per second, this is real CPU. Java caches the int into a
  benign-data-race instance field; Rust would need
  `OnceLock<u64>` or a `Cell<Option<u64>>` (the latter requires
  `unsafe` interior mutability through `&self`).
- **Severity:** Performance, not Bug. The Rust hash is correct,
  just slower than Java for repeated lookups of the same key.
- **Suggestion:** Defer to a later perf pass once the producer is
  wired end-to-end and we can measure. Note that benchmarking
  must use a realistic topic-name length (Java's cache pays off
  most for long topic names). Add a `// TODO(perf): consider caching`
  marker if desired.
- **Why not BLOCKER:** CLAUDE.md rule 11 explicitly calls out
  hot-path costs Rust makes explicit; this is one of those costs.
  The Phase 4 DoD does not mandate caching the hash.

**Resolution:** Tracked in `5d99bf5` (`fixup! Phase 4a: cluster
data types ...`). Documented the deferred optimization opportunity
in `design/history/Milestone-1/Phase-4/NOTES.md` under a new
"Future perf opportunities" section instead of leaving a `TODO`
marker in the source — CLAUDE.md rule 5 forbids `TODO`/`FIXME` in
code, and this optimization is measurement-blocked (it needs an
end-to-end-wired `RecordAccumulator.ready` to benchmark
realistically). The note records why the optimization was deferred,
the candidate implementations (`OnceLock<u64>` vs `Cell<Option<u64>>`),
and the trigger condition (post producer wire-up benchmark).

### Issue 3 — Minor — `Cluster::bootstrap(&[SocketAddr])` silently loses hostnames

- **File:** `src/common/cluster.rs:245-270`
- **Java reference:**
  `kafka/clients/src/main/java/org/apache/kafka/common/Cluster.java:211-218`
  builds nodes from `address.getHostString()` which is the *textual*
  hostname, not the resolved IP. `ClusterTest.testBootstrap`
  (line 49–62) passes `www.example.com` and asserts the host is
  preserved as `www.example.com` in `cluster.nodes()`.
- **Description:** The `bootstrap(&[SocketAddr])` method in Rust
  calls `addr.ip().to_string()` which always returns the numeric IP.
  This is *fundamentally* different from Java when the caller
  constructed the original `InetSocketAddress` from a hostname.
  Rust's `SocketAddr` cannot store a hostname, so the user must
  call `bootstrap_with_hosts(&[(String, u16)])` to preserve names.
  The two-method split is well-motivated (Rust's type system),
  and the translated `test_bootstrap` correctly uses
  `bootstrap_with_hosts` to mirror Java's behavior.
- **Concern:** A future caller (Phase 4b `Metadata.bootstrap` or
  Phase 4c `ClientUtils.parseAndValidateAddresses`) is likely to
  call `Cluster::bootstrap(&[SocketAddr])` because it is the more
  Rust-idiomatic name — and silently lose the hostname. Java's
  `Metadata.update` then fails to match the bootstrap entry against
  the `MetadataResponse`'s broker hostnames.
- **Suggestion:** Add a strong rustdoc warning on `Cluster::bootstrap`
  ("This loses hostnames — if your input came from a hostname string
  use `bootstrap_with_hosts` instead, which is the actual analogue of
  Java's `Cluster.bootstrap(List<InetSocketAddress>)`"). Optionally
  rename the methods so `bootstrap` matches Java semantics and the
  IP-only variant is `bootstrap_resolved` or similar — but renaming
  may be out of scope for Phase 4a.
- **Why not BLOCKER:** No in-tree caller invokes `Cluster::bootstrap`
  yet. Phase 4b/4c will introduce them; flag this before then.

**Resolution:** Fixed via the rename path in `2673983` (`fixup!
Phase 4a: cluster data types ...`). The hostname-preserving
constructor (formerly `bootstrap_with_hosts`) is now the default
`Cluster::bootstrap`, matching Java's
`Cluster.bootstrap(List<InetSocketAddress>)` semantics. The IP-only
variant is renamed to `Cluster::bootstrap_with_addresses` and carries
a strong rustdoc warning that it loses hostnames, with a
cross-reference to the new `Cluster::bootstrap` as the Java analogue.
Renaming was the cleaner long-term fix because it removes the
footgun outright (callers writing the obvious
`Cluster::bootstrap(...)` get Java-equivalent behavior); the only
in-tree caller (`test_bootstrap`) was updated in the same commit.

### Issue 4 — Performance — `Cluster::with_partitions` undoes Arc sharing

- **File:** `src/common/cluster.rs:298-318`
- **Java reference:**
  `kafka/clients/src/main/java/org/apache/kafka/common/Cluster.java:223-229`
  passes the existing `String` keys straight through to the new
  `Cluster` constructor.
- **Description:** The Rust `with_partitions` materializes
  `HashSet<String>` from `HashSet<Arc<str>>` (3 sets) and
  `HashMap<String, Uuid>` from `HashMap<Arc<str>, Uuid>` for
  `unauthorized`, `invalid`, `internal`, and `topic_ids`, then the
  `new_with_topic_ids` constructor turns them back into `Arc<str>`
  via `Arc::<str>::from(k)` — i.e. allocates *fresh* `Arc<str>`
  buffers identical to the existing ones. The original `Arc<str>`
  refcount-sharing is destroyed.
- **Severity:** Performance. `with_partitions` is on the metadata
  refresh path (called from `MetadataSnapshot` / `Metadata.update`
  in Phase 4b), not the message send path, so the impact is
  bounded. It still strikes me as wasted work.
- **Suggestion:** Add overloads on `Cluster::build` (or new
  `Cluster::new_*_arc` constructors) that accept
  `HashSet<Arc<str>>` / `HashMap<Arc<str>, Uuid>` directly, so
  `with_partitions` can pass through the existing allocations.
  Or have `with_partitions` reach into `Cluster::build` directly
  by exposing it as `pub(crate)`.
- **Why not BLOCKER:** Producer correctness is unaffected.

**Resolution:** Fixed in `97d5a12` (`fixup! Phase 4a: cluster data
types ...`). Refactored to push the `String → Arc<str>` conversion
to the public-API boundary (one `into_arc_set` helper invoked by
each `pub fn new_*` constructor) so the inner `Cluster::build`
carries `Arc<str>` end-to-end. Added a `pub(crate)
Cluster::from_arc_inputs` constructor that accepts already-shared
`Arc<str>` collections directly, and rewrote `with_partitions` to
call it with `Arc::clone` (refcount bumps, no reallocation). Added
a regression test (`with_partitions_shares_topic_arc`) that pins
the `as_ptr()` addresses of every `Arc<str>`-keyed entry across
`with_partitions` so a future refactor cannot silently reintroduce
the round-trip. Test count 627 → 628.

---

# Phase 4b Review — Resolved Items (Critic N=0)

Reviewed commits: `95c1175` (metadata stack), `da923a0` (test
extensions), `96cf27c` (actor memory note).

Java reference tree:
- `kafka/clients/src/main/java/org/apache/kafka/clients/Metadata.java`
- `kafka/clients/src/main/java/org/apache/kafka/clients/producer/internals/ProducerMetadata.java`
- `kafka/clients/src/test/java/org/apache/kafka/clients/MetadataTest.java`
- `kafka/clients/src/test/java/org/apache/kafka/clients/producer/internals/ProducerMetadataTest.java`

DoD checks all green at end:
- `cargo build --lib` clean
- `cargo test --lib` — 684 passed (was 673; +11 net: +12 added, -1 dedupe)
- `cargo xtask format-check` clean
- `cargo xtask lint` clean
- `cargo xtask check-generated` clean (199 generated files)

---

### Issue 5 — `testTopicExpiry` was missing

**Resolution:** Fixed in `4f56fbd` (Fixup D). Translated
`ProducerMetadataTest.java:182-213` directly as
`producer_metadata::tests::topic_expiry`. Three-phase contract
covered: idle-window expiry (add → wait `METADATA_IDLE_MS` → next
update drops the topic), in-window re-add keeps the topic alive
(loop adding inside the window), late update on a newly-added topic
still retains it (no chance to expire). The
`retain_topic_inner` `expire_ms <= now_ms` branch
(`producer_metadata.rs:182-189`) is now exercised end-to-end. Added
`ProducerMetadata::update_with_current_request_version` to mirror
Java's inherited helper so the test reads naturally.

### Issue 6 — `testConcurrentUpdateAndFetchForSnapshotAndCluster` was unjustifiably deferred

**Resolution:** Fixed in `4f56fbd` (Fixup D). Translated
`MetadataTest.java:1145-1232` as
`metadata::tests::concurrent_update_and_fetch_for_snapshot_and_cluster`
using `std::thread::spawn` + `std::sync::Barrier` (the synchronous-mutex
analogue of Java's `ExecutorService` + `CountDownLatch`). 6 threads:
3 writers update with progressively larger node count / partition
count / leader epoch; 3 readers wait on the barrier then snapshot
both `fetch_metadata_snapshot()` and `fetch()`. Same post-test
assertions as Java: snapshot/cluster reflect strictly-greater node
count, partition counts, and leader epoch. The Round 1 deferral
("Java-specific stress test, no Rust analogue") was wrong — the
property is language-independent and the lock-free `ArcSwap` from
Issue 8 makes this exact test load-bearing.

### Issue 7 — Three tests claimed "covered by `MetadataSnapshotTest`" were not

**Resolution:** Fixed in `4f56fbd` (Fixup D). All three translated
directly as separate `metadata::tests` cases:

- `testEpochUpdateOnChangedTopicIds` (`MetadataTest.java:413-452`) →
  `epoch_update_on_changed_topic_ids`. Covers the topic-id-change
  branch in `update_latest_metadata` for both lower-epoch (wins
  because id changed) and higher-epoch transitions.
- `testMetadataMergeOnIdDowngrade` (`MetadataTest.java:1019-1064`) →
  `metadata_merge_on_id_downgrade`. Drives the topic-id downgrade
  through the Metadata stack with a `set_retain_topic_fn` predicate
  (Rust composition analogue of Java's anonymous-subclass
  `retainTopic` override).
- `testTopicMetadataOnUpdatePartitionLeadership`
  (`MetadataTest.java:1066-1139`) →
  `topic_metadata_on_update_partition_leadership`. Verifies that
  `update_partition_leadership` changes a partition's leader id
  without losing other partition data.

The Round 1 "covered elsewhere" claim was incorrect: those tests
exercise integration paths through `Metadata.update` /
`updatePartitionLeadership` that `MetadataSnapshotTest` (which
exercises only `MetadataSnapshot.mergeWith` in isolation) does not
touch.

### Issue 8 — `Metadata::fetch()` and `fetch_metadata_snapshot()` were taking the writer lock

**Resolution:** Fixed in `4ec14cd` (Fixup A). Hoisted
`metadata_snapshot` out of `MetadataInner` into an
`arc_swap::ArcSwap<MetadataSnapshot>` on `Metadata` itself. Readers
(`fetch`, `fetch_metadata_snapshot`, `topic_ids`, `topic_names`,
`partition_metadata_if_current`, `current_leader`) now do a
lock-free atomic Arc load instead of acquiring the inner mutex.
Writers (`bootstrap`, `update`, `update_partition_leadership`)
still run inside the inner mutex (so writer-vs-writer
serialization preserves Java's `synchronized` semantics) and
publish via `ArcSwap::store`. `handle_metadata_response_locked`
and `update_latest_metadata` now take an explicit
`&Arc<MetadataSnapshot>` plumbed in from the writer's single
`load_full()` so all reads in a single update pass see a
consistent view.

Added `arc-swap = "1"` as a direct dependency. `arc-swap` is the
established Rust crate for the "volatile `Arc<T>`" pattern (CLAUDE.md
rule 1.2: prefer popular Rust crates over hand-rolling). The
dependency is justified for this exact `volatile`-replacement
pattern matching `Metadata.java:79,129-138`.

The producer hot path (`KafkaProducer.send` partitioning lookup) is
now lock-free.

### Issue 9 — `ProducerMetadata::update` silently swallowed `MetadataResponse::errors()` failure

**Resolution:** Fixed in `e513cd9` (Fixup B). Replaced
`response.errors().unwrap_or_default()` with
`response.errors().expect("...")` documenting the assumed invariant.
This matches Java's `ProducerMetadata.update` semantics
(`ProducerMetadata.java:136`): the `IllegalArgumentException` from
`errors()` is only thrown for topic-id-only responses, and the
producer client always operates on name-keyed responses, so a
topic-id-only response here is a programming error — Java propagates
it as an unchecked exception, Rust panics via `expect()`. CLAUDE.md
rule 5 violation (silent failure-completion) is now fixed.

### Issue 10 — `cluster_listener_fires_on_close` asserted nothing

**Resolution:** Fixed in `8d8fd4a` (Fixup E). Replaced the misleading
no-op-listener test with `cluster_listener_notified_on_update_not_on_bootstrap`,
a faithful translation of `MetadataTest.java:299-322`. Captures the
most-recent `on_update` argument via
`Arc<Mutex<Option<ClusterResource>>>`, asserts the listener is
**not** notified after `bootstrap` and **is** notified with cluster
id `"dummy"` after `update`. The `on_update` wiring in
`Metadata::update` is now covered.

### Issue 11 — `epoch_update_after_topic_deletion` skipped phase 3

**Resolution:** Fixed in `4f56fbd` (Fixup D). Replaced the partial
2-phase test with the full 3-phase translation matching
`MetadataTest.java:388-411`. New test
(`epoch_update_after_topic_deletion`) covers: empty → topic with
topic-id A epoch 10 → `UNKNOWN_TOPIC_OR_PARTITION` error response
keeps last-seen at 10 → topic recreated with **different topic-id
B and lower epoch 5** → last-seen = 5 (lower epoch wins because
topic id changed). The previously uncovered branch
`Some(current_epoch) if topic_id.is_some() && topic_id != old_topic_id =>`
in `metadata.rs` is now exercised.

### Issue 12 — Listener invocation outside lock diverged from Java

**Resolution:** Fixed in `0708bc6` (Fixup C). Moved the listener
dispatch back into the locked section in both `Metadata::update`
and `Metadata::update_partition_leadership`, matching Java's
`Metadata.java:367` ordering. Added a type-level "Listener
constraint" rustdoc on `Metadata` documenting the non-reentrancy
adaptation: cluster resource listeners must NOT call back into the
same `Metadata` instance from `on_update` (Java's `synchronized`
is reentrant; `std::sync::Mutex` is not, so a re-entry would
deadlock). Reading the `cluster_resource` argument is the
supported access pattern.

### Issue 13 — Several `MetadataTest` cases beyond the deferral list were silently missing

**Resolution:** Fixed in `4df52cc` (Fixup F). Translated the four
MetadataTest cases the Critic explicitly named:

- `testStaleMetadata` (`MetadataTest.java:232-280`) →
  `stale_metadata_with_older_epoch_ignored`.
- `testRequestVersion` (`MetadataTest.java:612-639`) →
  `request_version_in_flight_bump`.
- `testPartialMetadataUpdate` (`MetadataTest.java:641-702`) →
  `partial_metadata_update_full_vs_partial`.
- `testMetadataTopicErrors` (`MetadataTest.java:749-782`) →
  `metadata_topic_errors_per_topic`.

Per-test justifications for the remaining MetadataTest cases that
stayed deferred (no longer the generic "covered elsewhere" pattern;
each names a specific reason and a Phase to revisit):

- `testTimeToNextUpdateRetryBackoff` (`MetadataTest.java:155-176`):
  partially covered by existing
  `failed_update_resets_attempts_on_subsequent_success` and
  `failed_update_bumps_attempts`. Full `requestUpdate`-vs-backoff
  interaction stays deferred to Phase 5 (producer wire-up).
- `testIgnoreLeaderEpochInOlderMetadataResponse`
  (`MetadataTest.java:184-230`): requires `MetadataResponse.parse`
  with version<9. Wire-format harness today is at v12; rolling back
  is a generator change deferred to Phase 5.
- `testOutOfBandEpochUpdate` (`MetadataTest.java:511-550`): asserts
  `partitionMetadataIfCurrent` returns None after
  `updateLastSeenEpochIfNewer`. Filter logic exercised by
  `update_last_seen_epoch_if_newer_contract`; full integration
  deferred to Phase 5/6.
- `testClusterCopy` (`MetadataTest.java:574-605`): exercised by
  `metadata_merge_partial_update_retains_old_topics` and
  `cluster_listener_notified_on_update_not_on_bootstrap`.
- `testLeaderMetadataInconsistentWithBrokerMetadata`
  (`MetadataTest.java:839-886`): requires `Cluster::leader_for(tp)`
  not yet present. Translate when the API lands (Phase 4a follow-up).

### Issue 14 — Several `ProducerMetadataTest` cases were missing; two duplicates

**Resolution:** Fixed in `4f56fbd` (Fixup D for the new tests) and
`4df52cc` (Fixup F for the duplicate consolidation). Translated:

- `testMetadataWaitAbortedOnFatalException`
  (`ProducerMetadataTest.java:216-219`) →
  `metadata_wait_aborted_on_fatal_error`.
- `testTimeToNextUpdateOverwriteBackoff`
  (`ProducerMetadataTest.java:163-180`) →
  `time_to_next_update_overwrite_backoff`.
- `testMetadataPartialUpdate` (`ProducerMetadataTest.java:222-267`)
  → `metadata_partial_update_lifecycle`.

Removed the duplicate `await_update_returns_after_close_synchronously`
test in `producer_metadata.rs` (it exercised exactly the same path
as `await_update_throws_after_close`). Left an inline comment so a
future reader doesn't re-add it.

Per-test justifications for the remaining ProducerMetadataTest cases
that stayed deferred:

- `testMetadata` / `testMetadataAwaitAfterClose` /
  `testMetadataEquivalentResponsesBackoff`
  (`ProducerMetadataTest.java:60-131`): all three drive a Java
  `Thread`-based `asyncFetch` worker that calls
  `metadata.fetch().partitionsForTopic` in a loop, awaiting an
  update from the test thread. The async/sync split between Rust
  threads and `tokio::time::timeout` doesn't map cleanly without
  rewiring `await_update` to use the abstract `Time` trait (Phase 4b
  explicitly chose to bind the deadline to wall time — see
  `producer_metadata.rs:218-222`). Deferred to the Phase that adds
  abstract-clock support.

### Issue 15 — `failed_update_resets_attempts_on_subsequent_success` asserted too weakly

**Resolution:** Fixed in `8d8fd4a` (Fixup E). Strengthened to mirror
`MetadataTest.java:282-297`: bump `attempts` to 3 via repeated
failures, do a successful update at t=100, then issue another
`failed_update` at t=1100 and assert `time_to_next_update(1100)`
is in the **base** [80, 120] band. An un-reset `attempts=4` would
put the backoff at [640, 960], far outside the base band, so the
assertion now genuinely proves attempts was reset on success.

### Issue 16 — `is_invalid_metadata_kafka_error` incompletely matched Java's hierarchy

**Resolution:** Fixed in `8aeeebf` (Fixup G). Added a rustdoc
comment listing the 6 missing `InvalidMetadataException` subclasses
(`FencedLeaderEpoch`, `ReplicaNotAvailable`, `ListenerNotFound`,
`ElectionNotNeeded`, `InconsistentTopicId`, `PreferredLeaderNotAvailable`,
`EligibleLeadersNotAvailable`) inline with their Java semantics so
a future reader extending `KafkaError` knows exactly what to add to
the `matches!` arm. Comment-only — no behavior change.

---

All Phase 4b BLOCKER, MAJOR, and MINOR issues resolved. Test count
673 → 684. New `arc-swap` dependency added (Cargo.toml change is
the only structural change; reviewer should re-validate the dep
choice in Round 2).

---

# Phase 4c Review — Resolved Items (Critic N=0)

Reviewed commits: `6713037` (4c-1: ClientDnsLookup, HostResolver,
DefaultHostResolver, CommonClientConfigs), `b64b30a` (4c-2: dedupe
RETRY_BACKOFF constants), `e932fd9` (4c-3: ClientUtils) against
base `e6b4858`. Round 1 verdict: APPROVED with 1 MINOR.

---

### Issue 17 — `ClientDnsLookup::ResolveCanonicalBootstrapServersOnly` rustdoc did not flag the reverse-DNS fallback

- **File:** `src/client_dns_lookup.rs:32-34` (Round 1 line range)
- **Severity:** MINOR
- **Java reference:** `kafka/clients/src/main/java/org/apache/kafka/clients/ClientUtils.java:76-86`
  — canonical-name lookup uses `InetAddress.getCanonicalHostName()`,
  which performs reverse DNS.
- **Description:** The Phase 4c deviation — Rust always falls back to
  the IP textual form for the canonical-name slot because `std::net`
  exposes no reverse-DNS API — was documented at the call site
  (`client_utils.rs:195-200`) and on the regression test
  (`test_parse_and_validate_addresses_with_reverse_lookup`) but not
  on the public-API surface of the `ClientDnsLookup` variant itself.
  A future SASL/Kerberos consumer reading only the variant doc would
  not learn about the deviation; the resulting SPN would silently be
  `kafka/<ip>@REALM` instead of `kafka/<canonical-hostname>@REALM`,
  which fails Kerberos auth unless every broker IP has a host
  principal in the KDC.

**Resolution:** Fixed in `7e201af`. Added a
`# Note — deviation from Java behavior` paragraph on the
`ResolveCanonicalBootstrapServersOnly` variant rustdoc in
`src/client_dns_lookup.rs` covering all four points the Round 1
review asked for:

1. What Java does — `InetAddress.getCanonicalHostName()` reverse
   DNS.
2. What Rust does in Phase 4c — IP-literal fallback because
   `std::net` exposes no reverse-DNS API; cross-references the
   call-site implementation in
   `client_utils::parse_and_validate_addresses_with_resolver`.
3. The implication for SASL/Kerberos — wrong SPN
   (`kafka/<ip>@REALM` vs `kafka/<canonical-hostname>@REALM`),
   plus a note that no caller is currently affected because the
   SASL stack is not yet implemented.
4. Where to revisit — Phase 5+ SASL stack — with two candidate
   crates (`dns-lookup`, `hickory-resolver`) and the fix shape:
   route the resolver through reverse-DNS only for this variant,
   preserving the cheap synchronous `to_socket_addrs` path for
   `UseAllDnsIps`.

Comment-only change. No public API change. 720 library tests still
passing; format-check, lint, check-generated all clean.

---

All Phase 4c BLOCKER, MAJOR, and MINOR issues resolved.
