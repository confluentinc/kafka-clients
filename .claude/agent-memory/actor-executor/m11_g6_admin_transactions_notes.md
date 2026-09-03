---
name: m11-g6-admin-producers-transactions-multilanguage
description: Milestone 11 G6 (admin multilanguage producers & transactions, the final slice) — the four routes that turn an "unreachable" verdict over, why a docker-exec console producer is a legitimate fixture, the StatusResponse envelope for void-only results, and the script bug that silently truncated both Python servers
metadata:
  type: project
---

Slice G6 of `design/history/Milestone-11/PLAN-multilanguage-admin.md`: the last
six RPCs (`describeProducers`, `describeTransactions`, `abortTransaction`,
`forceTerminateTransaction`, `listTransactions`, `fenceProducers`) plus the whole
branch's final sweep. **46/46 RPCs, 78 scenarios, 312 admin entries**; `__rust`
64 → 77 in the thirteen `admin_*_test.rs` files (78 with
`multilanguage_admin_test.rs`, which is feature-gated).

## Five "unreachable" states fell in one slice; the four questions that find them

Every earlier slice recorded some state as unreachable by *reasoning* about the
fixture. Six such claims were re-tested at G6 and five were wrong. The questions
that actually decide it, in cost order — and which claim each one closed:

 1. **Is it unreachable by construction, or only on this fixture?** Only
    `state()`/`group_state()` is the former (no cluster configuration reaches
    `NOT_READY` outside STREAMS).
 2. **Have you tried a purpose-built `ClusterConfig`?** Closed G3's reassignment
    and G5's ACL denial.
 3. **Can another RPC *in the same slice* produce the server state?** Closed
    `describeClassicGroups`' value arm: `alterConsumerGroupOffsets` on a
    never-consumed group id **creates a simple classic group**, because a negative
    generation id on an unknown group hits
    `getOrMaybeCreateClassicGroup(id, true)`
    (`OffsetMetadataManager.java:458-467`). Same cluster, an RPC already
    implemented.
 4. **Have you asked `MockAdminClient`?** Closed `updateFeatures`' success arm.
    On an *unseeded* mock a feature is `cur = min = max = 0`, which is already
    enough: SafeDowngrade→0 succeeds, SafeDowngrade→1 fails with a message *only*
    that branch emits ("Can't downgrade to newer version." vs UPGRADE's "Can't
    upgrade above 0"), and UNKNOWN is a third. No seeding plumbing needed.

The delegation-token claim held, and only because it had been *measured* both
ways rather than reasoned about.

## A `docker exec` console producer is the right fixture, not a hack

`ProducerState`'s six fields were dead end to end because a producer state entry
exists only for a producer with a real producer id, and this client's send path
always writes `RecordBatch::NO_PRODUCER_ID` (`enable.idempotence` defaults to
true in the config but nothing calls `set_producer_state`). No admin RPC creates
one either — the transaction and group coordinators' own log writes carry no
producer id.

`docker exec <broker> kafka-console-producer.sh --producer-property
enable.idempotence=true` populates it in **under a second**, and it works
identically for all four backends because the exec runs from the test process.
Two traps: find the container by `--filter network=<net>` + an image-name match
rather than by tag (so the helper does not restate `KAFKA_TAG`), and bootstrap
from `ctx.container_bootstrap_servers()` — the PLAINTEXT listener advertises a
host-mapped port that is unreachable from inside the container, and the run
otherwise spends two minutes retrying `127.0.0.1:<host port>`.

Four of six `ProducerState` fields then carry real values; the two `Optional`s
need an in-progress transaction and are asserted on their `None` side, which is
what catches a backend defaulting them to 0.

## `abortTransaction` needs no hanging transaction, and its observable is the log

Measured: the broker accepts a `WriteTxnMarkers` for a producer id it has never
seen, for a mismatched epoch, and for a fabricated id — **all three `Ok`**. So the
`#[ignore]`d skeleton was unnecessary, and there is no reachable error arm.
`Ok(())` alone is a weak predicate though (a backend that never sent the request
also returns it), so the assertion is the *effect*: an abort marker is a control
record, so `list_offsets(latest)` advances by exactly one.

## Two RPCs whose result carries nothing: reuse `StatusResponse`

`AbortTransactionResult` exposes only `all()` and `TerminateTransactionResult`
only `result()`, both over `Void`, with no reachable per-key granularity. Both
bindings already model that — the C entry points return **no result handle at
all** and `admin.py` resolves to `None` — so the wire shape is the shared
`StatusResponse` that `Close` uses, i.e. whole-value degenerated to void. No new
envelope shape was needed for the whole slice; the other four are ordinary per-key
`oneof`s.

`listTransactions` is keyed by **broker**, from Java's `byBrokerId()` — the only
one of its three views that keeps a per-broker error. That is also the only route
to its per-broker error arm: a malformed id pattern makes the broker throw
`InvalidRegularExpression` → `INVALID_REGULAR_EXPRESSION(128)`
(`TransactionStateManager.scala:359-368`).

## An `*Options` timeout that is readable back out of a response

`FenceProducersHandler` writes the option's timeout into
`InitProducerIdRequest.transactionTimeoutMs`, so fencing with 45 000 ms and then
reading `describeTransactions(id).transactionTimeoutMs()` is the **only** place in
this harness where an options value is observable in a later response. Worth using
rather than asserting a shape.

## Per-backend ids are mandatory when the state is global and persistent

All four arms of a scenario share one broker (the pool keys on `ClusterConfig`),
and transaction state persists. A shared literal transactional id would make the
second arm see the first arm's transaction and `epoch == 0` would fail. Deriving
the id from `TestContext::group_id` (thread name + random suffix) fixes it. The
same reasoning is why the "no transactions on a quiet cluster" scenario needs its
own `ClusterConfig` — its four arms share a container that nothing else touches.

## The script that silently truncated both Python servers

A Python rewriter that reassembles a file from `lines[:start]` + per-method
blocks and never appends `lines[end:]` **deleted `def main()` and the
`__main__` guard** from both servers. The images built fine and the containers
exited 0 with no output; the failure surfaced as
`failed to wait for container log: End of stream reached before finding message`
in `backend_pool`. Lesson: after any whole-file rewrite, check the *tail* (and the
line count) before rebuilding an image — a server that starts and exits looks like
a wait-strategy problem, not a truncation.

## Teeth check

Three transpositions in the **sync** Python encoder, sync image only:
`TransactionDescription.coordinator_id ↔ .transaction_timeout_ms`,
`ProducerState.last_sequence ↔ .last_timestamp`,
`ClassicGroupDescription.group_id ↔ .protocol`. Predicted 3 red `__grpc_python`
arms, got **4** — the extra one was a *consequence* worth having: the huge
timestamp does not fit `last_sequence`'s int32, so the encoder raised, and because
round-15 LOW 2's fix put the encode inside the handler's guard it crossed as
`IllegalStateError: python server: ValueError: Value out of range: 1786473747359`
instead of a bare gRPC UNKNOWN. The classic-group mutation printed the mechanism
exactly — *"reported is_simple_consumer_group=true but Java derives false from
protocol=..."* — which is the derivation check that was **dead code** until this
slice. All `__grpc_python_async` arms stayed green on the same shared file, and the
error-arm siblings (`describe_classic_groups_rejects_a_kip848_group`,
`force_terminate_transaction_fresh_id`) stayed green because they never decode the
mutated fields. Restore returned the file to sha256 `6fb48845…` and the image to
the exact pre-mutation id `853558d0e776`.

## Environment

  - `cargo test --features multilanguage-tests` (one invocation, 8 targets) is
    the whole gated suite: **3734 passed, 0 failed, 15 ignored**. All 10 ignored
    integration tests are Milestone-8 consumer items (Issue 8 / broker shutdown);
    **zero admin `#[ignore]`s**.
  - `cargo xtask check-generated` **now passes** (199 files). Every slice from G1
    on disclosed it as failing on a pre-existing `join_group_response_data.rs`
    blank-line diff; that is stale.
  - The Bash tool caps at 600 s regardless of the `timeout` argument. A full-suite
    run needs `run_in_background`. Interrupting one leaves ~40 containers and 20
    `kafka-net-*` networks behind and **killed the Docker daemon**; `open -a
    Docker`, then `docker network rm` / `container prune`, then re-run.
  - Leftover test binaries from an interrupted run keep running and compete for
    Docker. `pgrep -fl integration-` and kill the *old* binary hash before
    re-running.
