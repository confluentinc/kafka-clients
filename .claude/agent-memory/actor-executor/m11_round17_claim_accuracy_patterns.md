---
name: m11-round17-claim-accuracy-patterns
description: Critic round-17 closing patterns for the admin multilanguage branch — entailed assertions, one-directional observability, empty-citation "unreachable" rows, errata arithmetic, and the check-generated exit-code trap
metadata:
  type: feedback
---

Round 17 (the branch's merge verdict) found **no correctness defect** — every
item was a claim that overstated what the code proves. Five recurring shapes,
each of which cost a review round somewhere in M11:

1. **Expand a predicate before adding it as a "strengthening."** `is_valid()`
   turned out to be `NO_PRODUCER_ID < producer_id` one file away from an
   assertion of `producer_id >= 0`. Round-15 Issue 3 (`assert_ne!(Some(5),
   Some(3))`) was the same shape and was re-introduced *two commits after being
   removed*, in the same range.
2. **Name the arm a strengthening can fail on.** `all_of_exactly`'s key-set half
   is inert on `__rust`, because `RustNativeAdmin` builds its `Outcomes` map from
   the request's own keys. A ledger entry citing it is only true under
   `--features multilanguage-tests` (now documented on the helper itself).
3. **For a present/absent request field, say which direction is caught.** On a
   single-broker fixture, "unset routes to the leader" and "set to the only
   broker" are the *same route*, so a dropped field is invisible while
   "absent → 0" is caught. "Fully observable" was false in the dropped direction
   for both `DescribeProducersOptions.broker_id` and (round 16) `strict`.
4. **An "unreachable" row with an empty citation is not a record**, and
   cross-referencing a *different* RPC's correction discharges nothing. Ask
   §5.5's four questions and, if the purpose-built `ClusterConfig` was not tried,
   write "fixture limit, not attempted" instead of "unreachable".
5. **Errata need arithmetic discipline.** §5.9 said "five" over six rows and
   omitted three claims. If a table's count is in prose, recount it on every
   edit.

**Why:** the plan and the commit messages are the branch's permanent record and
the PR description is generated from §5.4/§5.6/§5.8/§5.9 verbatim, so a
non-discriminating assertion described as discriminating is a false statement
about coverage, not a wording nit.

**How to apply:** before writing "STRONGER", name an input the old assertions
accept and the new ones reject, and name the backend arm on which it fails. Add
MIXED to the vocabulary (strengthened on gRPC, unchanged natively).

## Two host traps confirmed here

  - **`cargo xtask check-generated` passes** (199 files, exit 0) under the nix
    1.97.1 triple. Without `rustfmt` on `PATH` it exits **1** with
    `Error: No such file or directory (os error 2)` — indistinguishable by exit
    code from a real formatting diff. Five slices disclosed a phantom
    "pre-existing blank-line diff in `join_group_response_data.rs`" that does not
    reproduce. Never conclude "misformatted" from exit 1 alone; check the
    formatter exists first.
  - **A fast admin multilanguage run is a real run.** The Kafka KRaft container
    boots in about a second, so all 44 `admin_transactions_test` entries across
    four backends finish in ~4.4 s and the 48 `admin_groups_test` entries in
    ~8 s. Confirm with a teeth-check mutation rather than assuming the suite
    skipped (`docker ps` mid-run shows `kafka-1-<suffix>`, gone seconds later via
    the atexit teardown).
