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

## `cargo test` hides the harness's own `eprintln!` — grep results can be vacuous

`cluster_pool`/`backend_pool` report evictions and warnings with `eprintln!`.
`cargo test` **captures stdout/stderr of passing tests and prints it only for
failing ones**, so grepping a green run's log for `INFO: evicted` finds nothing
and it looks like the eviction path never fired. I concluded exactly that, and
it was wrong: re-running with `-- --nocapture` showed **9 eviction events per
run, reaping 12 clusters and 24 gRPC backend containers**. The path fires
constantly.

**Why it matters:** this is a vacuous-check trap of the same family as a
green wrapper hiding zero work. A grep over captured output is not evidence
about an internal code path.

**How to apply:** any claim about harness-internal logging needs
`-- --nocapture`. And when a measurement contradicts a mechanism you believe in,
suspect the measurement before the mechanism.

To force eviction deterministically in a *targeted* test instead, create
`TARGET_LIVE_CLUSTERS + 1` distinct `ClusterConfig`s **sequentially**, dropping
each `TestContext` before the next — distinct configs are cheap to mint via
`ClusterConfig::with_properties` with one differing server property.

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

## Fixing a real bug on the path is not the same as fixing the flake

The eviction leak was real and measurable — one pre-fix full run leaks exactly
**8** `kafka-net-*` networks (matching what was found lying around on the host)
and leaves 24 dead backend containers resident for the run. Fixing it took the
leak to 0. But the flake it was supposed to explain
(`get_host_port_ipv4` → "does not expose port <p>/tcp") **still reproduced
afterwards**, at roughly 1 in 40 full runs.

**Why it matters:** a plausible mechanism plus a real defect on the same code
path is not proof of causation. Say "leak fixed, flake not proven fixed" rather
than collapsing the two.

**How to apply:** when a rare failure's error message admits several underlying
states, make the failure *attributable* before trying to fix it — capture the
state at the failure site (for a container: `docker inspect` status/exit
code/OOMKilled/`State.Error`/`NetworkSettings.Ports` plus `docker logs --tail`).
Resist a fallback that merely relocates the failure: falling back from the IPv4
to the IPv6 port mapping is wrong here because the endpoint is `127.0.0.1`.
Also note `WaitFor::message_on_stderr` is satisfied by the line appearing in the
log stream, so a container that logs its readiness message and then dies still
gets past `start()`.

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
