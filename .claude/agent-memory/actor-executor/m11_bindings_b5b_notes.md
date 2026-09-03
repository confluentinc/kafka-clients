---
name: m11-bindings-b5b-notes
description: M11 admin bindings B5b (SCRAM, delegation tokens, features) — when a collection-valued result mints a handle, nullable-bytes across CPython, and seeding a mock so a drain stops being dead code
metadata:
  type: project
---

Admin C FFI + Python bindings, slice B5b (`describeUserScramCredentials`,
`alterUserScramCredentials`, `createDelegationToken`, `renewDelegationToken`,
`expireDelegationToken`, `describeDelegationToken`, `describeFeatures`,
`updateFeatures`), landed on `dev/admin-bindings`. Builds on
[[m11_bindings_b5a_notes]].

## D2's fifth rule, and applying it the same day it was written

The rule (now in `PLAN-bindings.md` §7 D2): for a collection-valued per-key
value, **flatten to a second index when the element is scalar-only; mint a
handle as soon as that element itself contains a collection.** Two index levels
is the limit a C signature stays readable at.

B5b is the first slice decided by it rather than by taste:

  - `DelegationToken` → `TokenInformation` → `List<KafkaPrincipal>` is three
    levels once flattened, so **three handles** were minted
    (`kafka_common_DelegationToken_t`, `_TokenInformation_t`,
    `_KafkaPrincipal_t`) and the renewer list got its own index space at 0.
    `DelegationToken` is also the value of two results, which is B5a's
    independent reuse argument.
  - `ScramCredentialInfo` is two scalars → flattened to `(i, j)`.
  - `FeatureMetadata` is a single record with two maps of two-scalar ranges,
    keyed directly by the result → scalars and both maps live on the result
    handle, nothing minted. Its two maps are **independently indexed** (the
    `ListGroupsResult.valid()/errors()` shape), which the accessor rustdoc has
    to say or a caller will co-index them.

Do **not** use "is the Java type named / user-visible" as the discriminator —
`DeleteAclsResult.FilterResults` and `LogDirDescription` are both named public
classes and they went opposite ways. Index depth separates them.

## Nullable *bytes* has no PyArg_ParseTuple unit

`y#` rejects `None`; `z#` rejects `bytes` (it is the str-or-None unit). A
nullable byte string therefore crosses as `O` plus a manual
`PyBytes_AsStringAndSize` when it is not `Py_None`. Hit on the SCRAM password
and salt. Required (non-nullable) bytes like the delegation-token HMAC use `y#`
normally.

On the way out, raw bytes need `y#` in `Py_BuildValue` (two arguments, one
unit) — the HMAC can contain an interior NUL, so an `s` would truncate it.
Assert that in the test with a fixture whose bytes actually contain a `0x00`,
or the bug is invisible.

## Composing three Java views into one C result

`DescribeUserScramCredentialsResult` exposes `all()`, `users()` and
`description(user)` over one response future, with different failure semantics
(`all()` fails on the first hard per-user error; `users()` still lists them).
C has one result handle, so the FFI composes them into per-user rows using only
public API: take `all()`'s map when it succeeds; when it fails, walk `users()`
and call `description(u)` per user. Nothing Java can reach is lost, and the
result lands on D2's richest row (key + value + error). Guard the empty case —
if the composition yields no rows, return the `all()` error rather than an
empty success (the B4 `removeMembers` trap).

## Seeding the mock is what makes a drain testable

Six of B5b's eight RPCs *are* implemented by Java's `MockAdminClient`, but
`describeFeatures` / `updateFeatures` return nothing useful until the three
feature-level maps are seeded — and Java seeds them on its `Builder`, which the
Rust mock exposes as `set_feature_levels`. Exporting
`kafka_admin_MockAdminClient_set_feature_levels` (beside the existing
`update_beginning_offsets` setters) turned both drains from "empty map, epoch
only" into real end-to-end coverage, including `updateFeatures`'s
apply-vs-`validate_only` difference. **Before writing a drain, check whether the
mock has state to seed; if it does, exporting the setter is cheaper than
accepting a dead success path.**

## Which Java said that

`updateFeatures` is the one RPC with client-side validation before enqueue, but
only in `KafkaAdminClient` — `MockAdminClient.updateFeatures`
(`MockAdminClient.java:1285-1300`) goes straight to the per-feature loop and
accepts an empty map. A first draft attributed the throw to "Java" flatly,
which is the round-5 mistake. Name the class.

## Small mechanics

  - `cargo test` and `cargo clippy` at the root **do not build the `ffi`
    feature**, so `ffi::admin`'s ~100 unit tests are invisible to
    `make test-rust` even after it became `--workspace`. Run
    `cargo test --features ffi` explicitly.
  - Splicing a block into `_confluentkafka.c` must land **before
    `ProducerNativeMethods`**, not before the first `static PyMethodDef` in the
    file (that is `ConsumerRecords_methods`, ~4000 lines earlier, ahead of the
    shared helpers). The symptom is `implicit declaration of fire_handle_cb` and
    a wheel build that fails while pytest silently runs the stale extension.
  - `expireDelegationToken`'s `-1` means **expire immediately**, while
    `renewDelegationToken`'s `-1` means **use the broker default**. Same
    sentinel, opposite meanings, adjacent signatures — give them different
    values in every fixture.
  - `KafkaPrincipal.equals` ignores `tokenAuthenticated`; the request rows carry
    only type and name, so `_principal_rows` must not emit the flag.
