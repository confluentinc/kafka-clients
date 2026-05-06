---
name: Phase 4a review patterns
description: Cluster data-types translation: Arc<str> end-to-end checks, sentinel caching, Java-vs-Rust hostname semantics in bootstrap
type: project
---

Phase 4a translated `Node`, `TopicPartition`, `TopicIdPartition`,
`PartitionInfo`, `TopicPartitionInfo`, `Cluster`,
`TopicCollection`, `ClusterResource`, `ClusterResourceListener` (trait),
`ClusterResourceListeners` (pub(crate) aggregator), and 5 of the
matching Java tests. Commit `2739f7f` on `fresh-impl`.

**Why:** Build up institutional notes on what to verify when reviewing
Java cluster-topology translations and recurring traps.

**How to apply:** When reviewing future cluster/metadata-adjacent
phases, focus on these:

1. **`Arc<str>` end-to-end audit.** For any topic-name-bearing type,
   verify all four legs:
   - field type is `Arc<str>` (not `String`),
   - constructor takes `impl Into<Arc<str>>`,
   - getter returns `&str` (not `&String`),
   - `HashMap<Arc<str>, _>` lookups pass through `&str` (relies on
     `Arc<str>: Borrow<str>`).
   Missing any one of the four undoes the optimization.

2. **Sentinel caching.** Java `public static final` → Rust
   `&'static T` via `OnceLock<T>`. Easy bug: forgetting to wrap in
   `OnceLock` and re-allocating on every `T::default()` call.
   Pointer-equality test (`a as *const T == b as *const T`) is the
   way to verify caching.

3. **Java toString edge cases.** Java's `String + null` produces the
   literal `"null"` substring — Rust's `Option`-using Display impls
   need an explicit `None => "null"` branch. Conversely, when a
   Java field that was nullable is translated as `Arc::from("")`,
   the empty render no longer says `null` — flag if any test or
   downstream caller asserts on the literal.

4. **Hash-vs-equals scope.** Java sometimes does primary-key
   hashing (e.g. `Objects.hashCode(id)`) with full-struct equals,
   which violates the contract but is intentional in Java. Rust's
   `derive(Hash, PartialEq)` always hashes everything that equals
   uses. If Java does primary-key hashing, `derive(Hash)` is
   *inconsistent* with Java's hashing layout (different bucket
   for two equal-by-equals objects). For Phase 4a Node/TopicPartition
   this is NOT an issue — Java hashes the same fields it equals
   on. Always verify by reading `equals` AND `hashCode` together.

5. **`Cluster::bootstrap` hostname trap.** Rust `SocketAddr` can't
   hold a hostname. A direct `bootstrap(&[SocketAddr])` always
   produces numeric IPs in the `Node.host()` field, which differs
   from Java when the user constructed the `InetSocketAddress`
   from a hostname. The Phase 4a Actor split into two methods
   (`bootstrap` and `bootstrap_with_hosts`); future callers must
   pick the right one. Watch Phase 4c (`ClientUtils.parseAndValidateAddresses`)
   and Phase 4b (`Metadata.bootstrap`).

6. **Java `Cluster.partitionsByNode` requires non-null leader to be
   in the nodes set.** Java does
   `Objects.requireNonNull(tmpPartitionsByNode.get(p.leader().id()))` —
   NPE if violated. Rust translates to `unwrap_or_else(|| panic!())`.
   Acceptable per CLAUDE.md (programming error, panic OK).

7. **`available_partitions_by_topic` filter.** Java filters
   `partition.leader != null`. Rust must filter `partition.leader().is_some()`.
   Watch for accidental filtering on `is_empty()` instead — `Node::no_node()`
   has a non-null leader value in Java that returns `isEmpty()=true`,
   so Java's check on `null OR isEmpty` matters. The Phase 4a Rust
   code does `if let Some(leader) = p.leader() { if leader.is_empty() {
   continue; } ... }` — correct.

8. **TopicPartition hashCode caching.** Java uses
   `private int hash = 0` lazy cache; `RecordAccumulator.ready` is
   a hot caller. Rust's derive recomputes the hash through
   `(*Arc<str>).hash()` — content hash, slower than a cached int
   for repeated lookups of the same key. Performance concern,
   not correctness.

9. **`with_partitions` Arc preservation.** When a method rebuilds a
   Cluster from an existing one + new partitions, the existing
   `Arc<str>` topic identifiers should pass through, not get
   stringified-and-re-allocated. The Phase 4a `with_partitions`
   does the round-trip; flag as performance.

**Tests not in the Java tree (Phase 4a):** `NodeTest`,
`TopicPartitionInfoTest`, `ClusterResourceListenersTest` — verified by
`find kafka -iname '*Test.java'`. Do not require translation. Bonus
unit tests are OK to add for Rust-specific concerns (e.g. sentinel
pointer-equality).
