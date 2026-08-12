---
name: review-m11-g5-acls-quotas-scram-tokens-features
description: M11 G5 admin review (13 ACL/quota/SCRAM/token/feature RPCs) — MockAdminClient as a fourth reachability surface, the "non-discriminating assertion" verdict distinct from tautology, and the whole-module cfg gate that explains the __rust count
metadata:
  type: project
---

Slice G5 of `design/history/Milestone-11/PLAN-multilanguage-admin.md`, reviewed
as round 16 in `COMMENTS.1.md`. Verdict: merge-ready, no production defect; the
findings are one coverage gap and a cluster of conversion-ledger accuracy
problems.

## "Unreachable" now has a fourth surface to check: `MockAdminClient`

Rounds 14/15 established: check other *fixtures*, then other *in-slice RPCs*.
G5 adds a third: **grep `src/admin/mock_admin_client.rs` for the method.** The
mock is NOT uniformly unsupported — per `admin-client.md` §9 it mirrors Java's
per-method, and Java implements all four delegation-token RPCs
(`MockAdminClient.java:641-722`) and **both feature RPCs** (`:1268`, `:1285`)
with real logic, while throwing for ACLs/quotas/SCRAM (`:806`, `:811`, `:816`,
`:1243`, `:1248`, `:1253`, `:1258`).

G5 discovered this and applied it to tokens (closing a real marshaling gap) and
**not** to features — so `updateFeatures`' per-key success arm and every
`UpgradeType` except `UPGRADE` still never cross. Fourth consecutive round of
"recorded unreachable is reachable", and the first where the slice itself owned
the surface. When reviewing, ask the reachability question once per RPC family,
not once per slice.

Mock seeding exists on all four backends (`set_feature_levels` at
`mock_admin_client.rs:237`, `kafka_admin_MockAdminClient_set_feature_levels` at
`ffi/admin.rs:14879`, Python `_MockAdminClientMixin`) — but reaching it from a
scenario needs a new proto RPC + 3 handlers, so price the fix before filing MED.

## Verifying "the mock path exercises the same encode/decode sites"

The claim is checkable structurally, and it held: Python's
`MockAdminClient(_MockAdminClientMixin, Admin)` mixin contains only seeding
setters and overrides no RPC, so mock and real share the FFI call and the
`*Result_drain` decode; C++ gets a handle from `MockAdminClient_new` and every
handler then calls the same `kafka_admin_AdminClient_*` entry points with no mock
branch; the harness decoder is shared. Check the mixin's method list and grep the
C++ handlers for a mock branch — that is the whole audit.

## A third ledger-defect verdict: "non-discriminating" ≠ "tautological"

Round 15 caught a tautology (entailed by neighbouring assertions). G5 has a
distinct failure mode worth naming separately: an assertion nothing *prior*
entails, but which no input in the fixture can falsify. The `strict` /
`contains_only` case: strict makes the result a **subset**, so dropping the flag
degrades to `contains` (a superset) and a pure single-component entity is
reported either way. The companion "only single-component entities" loop would
discriminate — but only if a multi-component quota entity existed, and
`alter_client_quotas` is called from one file with one entity shape.

Generalisable check: for a subset/superset filter flag, "our thing is still
there" can never catch a dropped flag. You need an item the wider filter includes
and the narrower one excludes.

Two more ledger shapes seen this round: a rationale that is **inverted**
(`unwrap()` + `assert!(epoch >= 0)` claimed to catch absent-decoded-as-0, which
is `Some(0)` and passes both) and a claim that describes a **different
scenario's** code (`create_then_describe_acls` credited with a `binding()`
assertion that lives in the next scenario — and the swap dropped the per-ACL
`exception()` check that the original's `DeleteAclsResult::all()` performed,
`delete_acls_result.rs:120-127`).

## Count reconciliation: look for whole-module cfg gates before filing off-by-one

65 `multilanguage_admin_test!` registrations vs a claimed `__rust` 64 looked like
an error. It is not: `mod multilanguage_admin_test;` is
`#[cfg(feature = "multilanguage-tests")]`-gated as a whole module in
`tests/integration/main.rs`, so its G0 lifecycle scenario contributes 0 `__rust`
arms and 4 gated entries. Hence `__rust` 64 / 260 entries are *both* exact. Same
gate explains round-15's measured 50. Check `main.rs` mod attributes before
disputing a count.

## The proto-invariant-vs-FFI trap

`admin_service.proto` states the SCRAM salt's absent-vs-present-empty distinction
is preserved ("Not derivable from emptiness"). All three servers honour it
(`HasField` / `has_salt()`), and then `ffi/admin.rs:15600` decides by
`salt.is_empty()`. One field over, the same FFI carries a dedicated
`op_has_values[i][j]` bool column for the identical problem on quota values. When
a proto comment asserts a presence distinction, trace it to the Rust API that
consumes it — the servers are usually not where it dies.

## Server-vs-server asymmetry class, still recurring

Round 15's shape (one server stamps a synthetic per-entry error, the other
raises or silently shortens) recurred twice in G5, both unreachable: C++ omits
`TokenInformation.owner`/`token_requester` on null and returns **success** where
Python raises; C++ `continue`s past an unreadable quota value where Python writes
unconditionally. Also: round-15 LOW 2 (response builder outside the handler's
`try`) was fixed for **exactly** G5's 13 handlers and not retrofitted — an AST
pass showed 13 inside / 29 outside in both Python servers. Use an AST pass, not
grep, for that check.

## Claims that held up under real scrutiny

- ACL denial reachability: DENY beats the implicit allow
  (`StandardAuthorizerData.java:221-224` — the implicit allow is the *default*,
  consulted only when nothing matches). The broker-denies-itself boot failure is
  real: `ControllerApis` `handleBrokerRegistration`/`handleControllerRegistration`
  both call `authHelper.authorizeClusterOperation(request, CLUSTER_ACTION)`,
  which throws (`AuthHelper.scala:57-59`), and the fixture maps
  CONTROLLER/BROKER to PLAINTEXT.
- Delegation-token unreachability: `KafkaApis.allowTokenRequests`
  (`KafkaApis.scala:2345-2354`) is tested **before** `tokenManager.isEnabled`
  (`:2320-2323`), so the broker's secret-key config is irrelevant; the gate is the
  client's protocol, and `kafka_admin_client.rs:283` hardcodes Plaintext.
- `describeUserScramCredentials` as a per-key `oneof` is NOT inconsistent with
  G4's `Listings`: its response data carries a per-user `errorCode`, whereas
  `ListGroupsResult.errors()` is an unkeyed `Collection<Throwable>`.
- Quota remove-vs-set: the catch is step 3 (`None` → a collapsed encoder sends
  `0.0` → the broker refuses → `all_of_exactly` fails), not step 2. Step 2 turns
  "the broker refuses zero" into a tested property. Both are needed.
