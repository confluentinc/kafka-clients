---
name: integration-flake-hunting
description: How to prove an integration-suite flake is fixed — full-target run cost, why a green suite does not exercise cluster eviction, and the before/after teeth-check technique that does
metadata:
  type: project
---

Proving a fix for a rare integration-suite race needs a different method than
"run it again and it passed". Two facts make the difference.

## The full target is cheap — repeated runs are the right proof

`cargo test --features multilanguage-tests --test integration` is **534 tests
(524 + 10 ignored) in ~75s**, ~80s wall including the between-run Docker reap.
So 20 repetitions cost ~30 minutes, not a day.

**Why it matters:** several briefs this milestone assumed a full run was
expensive and accepted a *filtered* run or a single sample as evidence. It is
not expensive. Never substitute a filtered subset for the full target when the
claim is about flakiness — a filtered run does not reproduce the suite's
concurrency, which is the variable that drives these races.

**How to apply:** build the test binary once, then loop. Reap orphaned
`kafka-net-*` networks and stray `kafka-*` / grpc-server containers *between*
runs — leaked networks exhaust Docker's address pool and then every run fails
for an infrastructure reason that looks like your bug.

## A green full-suite run does NOT exercise cluster eviction

`cluster_pool`'s eviction only reclaims clusters that are **idle**, and under
the suite's real parallelism (18 test threads) the clusters are all checked out
whenever a new one is requested — so eviction finds no candidates and does not
fire. Measured: **0 eviction lines in a complete 534-test run.**

**Why it matters:** eviction is load-dependent, so it fires on some machines /
some runs and not others. Any number of green suite runs is therefore silent
about eviction-path bugs. Do not report "N clean runs" as evidence for a fix in
that path.

**How to apply:** force the path with a targeted temporary test instead. To make
eviction fire deterministically, create `TARGET_LIVE_CLUSTERS + 1` distinct
`ClusterConfig`s **sequentially**, dropping each `TestContext` before the next —
distinct configs are cheap to mint via
`ClusterConfig::with_properties` with one differing server property. Sequential
+ dropped means every cluster is idle, so the 6th request evicts the LRU one.

## Assert the fix from Docker, and prove the check has teeth by reverting

For a resource-lifetime fix, assert against `docker network ls` /
`docker ps -a` rather than against an in-process value — the bug *is* that the
Rust value and the Docker resource disagreed.

Then run the same check against the pre-fix code (`git show HEAD~1:<file> >
<file>`, `touch`, rebuild) and confirm it **fails**. That single before/after
pair establishes the diagnosis and the fix together, and is far stronger than
any number of green runs. Observed on this branch: pre-fix the evicted
cluster's backend container survived and its network leaked; post-fix both were
reaped.

Two traps when reverting: `touch` the restored files or cargo may run a stale
binary; and verify the rebuilt binary by `strings`-grepping for a message only
the fixed code contains, rather than trusting mtimes (`cargo test --no-run`
prints which `Executable` is current).

## This host's Docker VM is 8 GiB

`--memoryMiB 8192`, 18 CPUs. `cluster_pool`'s doc comment reasons about a
"15 GB host" and a measured 4.8 GiB peak at `TARGET_LIVE_CLUSTERS = 5`; on 8 GiB
that is much tighter, so leaked containers translate quickly into
OOM-killed-on-startup symptoms (e.g. `get_host_port_ipv4` reporting "does not
expose port <p>/tcp", which is what a *stopped* container looks like).

Also: `docker version` itself can take 60s+ to answer right after a heavy suite
run. Poll for readiness before concluding Docker is broken.

See [[integration_metadata_propagation_races]] for the other half of this
branch's flakiness — the describe-after-create window and the
`retry_on_exception_with_timeout` idiom that closes it.
