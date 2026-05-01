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
