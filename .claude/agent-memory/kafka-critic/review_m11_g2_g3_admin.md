---
name: review-m11-g2-g3-admin
description: Milestone 11 G2/G3 admin multilanguage review — how to test whether a null-vs-empty distinction really survives the C FFI, why "unreachable on this fixture" is usually a false claim in this harness, the bare-raise-inherits-generic-variant divergence class, and all_of's cardinality blindness
metadata:
  type: project
---

Round 14 reviewed `17f28136..61857016` (G2 = 8 cluster/config/log-dir RPCs,
G3 = 4 election/reassignment/offset RPCs). See also
[[review_m11_g1_admin_topics]] and [[review_m11_g0_multilanguage_admin]].

## The mechanical test for "is this null-vs-empty distinction really preserved?"

G3 proposed it and it is sound — adopt it as the standing check:

  - **Preserved** iff the C entry point takes a *dedicated boolean argument*.
    `read_optional_partition_set(all_partitions, ...)` returns `None` **before**
    touching the arrays; `read_reassignments` short-circuits on `cancel[i]`
    without reading the replica columns (and has a unit test that supplies a
    non-empty replica list on a cancelled row to prove the flag wins).
  - **Collapsed** iff the FFI decides *by emptiness* —
    `NewPartitionsBuilder::build`'s `if self.new_assignments.is_empty()`.

Then always check the wire spec too, because a distinction the broker cannot see
is not worth carrying: `grep nullableVersions
kafka/clients/src/main/resources/common/message/<Rpc>Request.json`. That is what
separates the harmless `NewTopic` caveat (`CreateTopicsRequest.json` has none)
from the real `NewPartitions` one.

And demand the third step: **preserved ≠ observable.** Make the author name which
of {absent, `Some(empty)`, `Some(non-empty)`} the fixture actually reaches.
`electLeaders` selection is only *half* observable — the controller's null branch
omits `ELECTION_NOT_NEEDED` (`ReplicationControlManager.java:1507` branch,
`:1525` filter), so explicit `{tp}` → 1 entry vs absent → 0 catches the dangerous
direction, but absent and `Some(empty)` both give 0 on a healthy cluster.

## "Unreachable on this fixture" is the weakest claim in this harness — challenge every one

Twice now a state was recorded as unreachable when it was one `ClusterConfig`
argument away. `backend_pool` keys containers by `(kind, broker_network)`, so a
distinct cluster just starts its own — the *harness authors' own note* says "no
extra work". `ClusterConfig::with_brokers(3)` and a two-`KAFKA_LOG_DIRS` cluster
are both in use already. So:

  - Ask: does any expressible fixture reach it? If yes it is a coverage gap, not
    an unreachable state.
  - Cross-check against the Java test's own broker count. `IntegrationTestHarness`
    defaults to 3, and several Java admin tests iterate `0 until brokerCount` —
    so a single-broker Rust fixture is often *narrower than Java*, which is a
    DoD #3 issue, not a fixture limitation.
  - Genuinely unreachable, verified: `LogDirDescription.error` (needs a disk to
    fail mid-run via `LogDirFailureChannel`); a cross-*broker*
    `alterReplicaLogDirs` (`AlterReplicaLogDirsRequest` is per-broker — that is
    reassignment's job); `PREFERRED` vs `UNCLEAN` on a healthy partition
    (`ReplicationControlManager.java:1576-1578` returns `ELECTION_NOT_NEEDED`
    for both); a *successful* leader election (needs a broker outage).

Also: G2 had to retract its own reachability guess about
`describeCluster().authorizedOperations` being `None`. Real mechanism —
`AuthHelper.authorizedOperations` (`kafka/core/.../AuthHelper.scala:62-76`)
returns `supportedOps.toSet` when `authorizer == None`, **never consulting the
principal**. So "ANONYMOUS is a super user" is the wrong explanation for the right
answer. General lesson the actor drew and I endorse: for "the broker omits field
X" claims, run it before writing the assertion.

## Divergence class: a bare `raise` in a Python handler inherits the generic variant

Recurred three slices after being fixed once. `_kafka_error_to_proto`'s
non-`KafkaError` fallback stamps `ILLEGAL_STATE`; C++ hand-writes
`VARIANT_ILLEGAL_ARGUMENT` at the equivalent site. So any new server-side
*rejection of a malformed request* written as `raise ValueError(...)` silently
1-vs-2 diverges. `_admin_constructor_error` exists precisely to fix this for the
constructor case — it was written as a one-off rather than as the rule.

Review heuristic: grep the Python handlers for `raise` that is not routed through
a dedicated translation, and diff the variant against the C++ `make_synthetic_error`
call at the same logical site. **A comment asserting the two servers agree is not
evidence** — twice now both sides carried such a comment that was true about the
*level* (top-level vs per-key) and silent about the *variant*, which is where they
part. Check the variant, not the level.

## Conversion-audit traps specific to this harness

  - **`all_of` is cardinality-blind.** `for outcome in outcomes.values()` returns
    `Ok(())` for an **empty** map. Any conversion that replaced `result.values()[&key]`
    (which panics on a missing key) with `all_of(&outcomes)` dropped the only
    response-completeness check — and on the gRPC arms `keyed` builds the map
    purely from the response's `entries` with no request reconciliation, so a
    short `entries` list now passes. Replacing `.all()` with `all_of` *is*
    faithful; replacing per-key indexing is not.
  - **A helper that reads a different field is a predicate change.** A poll for
    `replicas() == [target]` became a poll for `leader() == target` via a helper
    named `sole_replica_of` that actually returns `.leader().id()`, while the
    failure message still said "replica set" — and the commit claimed every
    assertion was kept and filed it as a *strengthening*. Always open the helper.
  - **Watch for newly-vacuous assertions.** `assert!(map.contains_key(&k))` is
    dead if `map[&k]` was already indexed earlier in the same body; check whether
    the original assertion was over a *different* map (here: the aggregate
    `all_descriptions()` future, which now has no real-broker caller at all).

## Where the fixture is right, say so — the throttle recipe is not flaky

In-flight `PartitionReassignment{replicas, adding, removing}` is reachable and
deterministic: 3 brokers, RF-1 topic, `{leader,follower}.replication.throttled
.replicas=*` on the **topic** plus `...throttled.rate=1024` on **every broker**,
then ~2 MiB produced. Both halves required — a rate with no replica list
throttles nothing (which is also why the never-unset broker rate does *not* leak
onto sibling scenarios sharing the pooled cluster: the replica list is per-topic).
A too-fast move does not make the assertions vanish, because entry to the
observable state is gated by a **panicking** `wait_until_true_with_timeout`.

## Verification shortcuts that paid off

  - `git diff <base>..<tip> -- src/ bindings/c/include/` empty is the one-command
    proof that no production visibility was widened for a `*View` type. Then check
    the `pub(crate)` really is Java-faithful: `ConfigSynonym` is package-private at
    `ConfigEntry.java:243`, `ReplicaLogDirInfo` at
    `DescribeReplicaLogDirsResult.java:71,75`.
  - `is_default`-style derived booleans are **not** tautological assertions when
    they cross the wire as their own proto field alongside the field they derive
    from — the check then proves the encoder populated both consistently.
  - `flock` does not exist on macOS. Use
    `python3 -c "import fcntl; ... fcntl.flock(f.fileno(), fcntl.LOCK_EX)"` to
    append to `COMMENTS.N.md` under an exclusive lock.
  - A one-off `cargo test ... -- --list` compile failure that does not reproduce
    on three re-runs is a concurrent-build artefact (shared `target/` with the
    Actor), not a tree defect — but say so in the report rather than omitting it.
