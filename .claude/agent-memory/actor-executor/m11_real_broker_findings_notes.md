---
name: m11-real-broker-findings-notes
description: M11 admin — the three real-broker bugs mock suites structurally cannot reach, and the count/presence FFI encoding rule adopted for nullable collections
metadata:
  type: project
---

Real-broker C+Python probes over all 46 Admin RPCs (`apache/kafka:4.2.0`) found six
issues the committed suites could not: **they only drive `MockAdminClient`**. Three
fixed on `dev/admin-bindings`, three deferred (written up in `COMMENTS.DONE.1.md`
under "Real-broker findings, deferred"). See [[m11-tier2-phase1-notes]] for the
group-describe layers involved.

## The structural blind spot to remember

`MockAdminClient` throws `UnsupportedOperationException` per group for
`describeConsumerGroups` (faithfully translated), so **the C and Python
`describe_consumer_groups` drain is unreachable from any mock-driven test**. Neither
`cargo xtask check-bindings` (arity only) nor the ctest/pytest suites can observe
anything on that path. When a bug lives behind a mock-unsupported RPC, the only
reachable coverage is a Rust-side FFI unit test that builds the `*Inner` flattener
directly and calls the `extern "C"` accessors — that pattern works and is now used
for the coordinator.

## Assertion anti-pattern that hid a real bug for a whole milestone

`assert_eq!(description.coordinator().map(Node::id), Some(0))` — an **id-only**
assertion on a struct whose whole point is an endpoint. A fabricated
`Node::new(id, String::new(), -1)` satisfies it. Whenever a test asserts one field
of a multi-field identity (Node, TopicPartition, endpoint, address), assert the
whole value or compare against the seeded fixture (`assert_eq!(x, &nodes[0])`).

## `Call.curNode()` → third closure parameter

Java's `Call` exposes `curNode()` to its anonymous subclasses; a Rust boxed closure
cannot reach the owning struct's fields. The translation is to widen the hook
signature: `HandleResponseFn` takes `Option<&Node>`, and `Call::handle_response`
destructures `let Self { cur_node, handle_response_fn, .. } = self;` so the `&mut`
hook borrow and the shared node borrow stay disjoint. Same shape applies to any
future hook that needs sibling `Call` state.

## Java's `Errors.exception(String)` lives on `Errors`, not in a helper module

`unwrap_or_default()` on a nullable broker `error_message` is a **bug**, not a
convenience: it turns a wire null into `Some("")`, which then shadows the error
code's default text (`KafkaGenericError.custom_message: Some("")` wins over
`Errors::message()`). Java tests `message == null` only — an empty-but-non-null
message is kept verbatim.

Now `Errors::exception(&self, Option<&str>) -> KafkaError` (`pub(crate)`,
`src/common/protocol/errors.rs`), matching Java's own class. Putting it there rather
than in `admin/internals/admin_utils.rs` is what let
`common::requests::elect_leaders_response` share it without an admin→common layering
inversion — and that file was the **fourth** site, missed by an admin-only grep.
**Lesson: sweep for a defect class crate-wide, not just in the module the report
named**; admin RPC response decoding lives partly under `src/common/requests/`.

## FFI rule adopted: counts are never negative; absence is a separate `bool`

A nullable Java collection (`authorizedOperations()`, `elr()`, `lastKnownElr()`)
must keep null distinct from empty across the C boundary. The rule now documented in
`src/ffi/admin.rs` module docs and `src/ffi/mod.rs`:

  - every `*_count` returns a **non-negative length**; absent and reported-empty
    both count 0;
  - presence is a sibling `*_has_<field>() -> bool` (six exist);
  - element accessors independently return -1 / null out of range.

Rationale: a count feeds `malloc(count * n)` and `for (size_t i = 0; i < count; i++)`,
so an in-band `-1` is a memory-safety hazard. Removing the sentinel from the return
*range* makes the mistake inexpressible — strictly better than documenting "remember
to check for -1". A sweep found only 3 of 70+ `*_count` fns were negative, so -1 was
already the outlier.

Prerequisite: the **core** types had to become nullable first —
`valid_acl_operations -> Option<BTreeSet<AclOperation>>` (Java's
`AdminUtils.validAclOperations` returns null on `AUTHORIZED_OPERATIONS_OMITTED`), and
`TopicDescription` / `ConsumerGroupDescription` / `ClassicGroupDescription` carry
`Option<BTreeSet<..>>`. Java's 3-arg `TopicDescription` ctor and `MockAdminClient`
both pass `Collections.emptySet()` → `Some(empty)`, **not** `None`.

Two things this uncovered: (1) `bindings/python/admin.py` already documented
`elr`/`last_known_elr` as `None`-when-unreported but `_confluentkafka.c` could never
produce it for `authorized_operations` — a dead branch; (2)
`DescribeConsumerGroupsHandlerTest`'s Java builders explicitly set
`.setAuthorizedOperations(Utils.to32BitField(emptySet()))` (= 0), which the Rust
translation had dropped, leaving the generated *omitted* default. Both were invisible
while null and empty collapsed. **When a translation collapses two Java values into
one, test-fidelity gaps hide behind it silently.**

## Deferred (do not re-report as new)

  1. **Lookup-stage metadata retries busy-spin** — 54k–108k Metadata attempts in
     10–20 s (~6k req/s). The no-backoff *decision* matches
     `AdminApiDriver.clearInflightRequest`, but Java is RTT-bound (one request per
     round trip) and the Rust re-send happens inside the same `run_once` sweep, so
     it is CPU-bound. Triggered by any unresolvable partition. Core scope, needs its
     own change.
  2. **`enable.idempotence` defaults true but is unimplemented** — `InitProducerId`
     appears nowhere under `src/producer/`, so `describeProducers` correctly returns
     0 producers.
  3. **Argument-validation errors surface as `UNSUPPORTED_VERSION(35)`** — Java
     raises `IllegalArgumentException`; 35 invites a version-downgrade retry loop.
     Related: Rust type names leak into user messages at `src/network_client.rs:471,480,507`.

## Toolchain notes for this worktree (`ckr-b2`)

`rustfmt`/`clippy`/`cmake` are not on PATH; only nix-store paths work, and clippy is
**1.97.1** while `rust-toolchain.toml` pins 1.95.0 (no rustup, so the pin is inert).
Run clippy with the whole 1.97.1 toolchain in a **separate `CARGO_TARGET_DIR`**, or
every dep hits `E0514`. rustfmt 1.97.1 agrees with the committed baseline (verified
by stashing). The C tests link `target/release/libconfluent_kafka.a`, so
`devel-build-c` needs a `cargo build --release --features ffi` first.

**`cargo clippy ... | tail` masks the exit status** — `$?` is `tail`'s. A gate script
must capture `${pipestatus[1]}` (zsh); I got a false "CLIPPY EXIT: 0" over a real
`single_element_loop` error this way. Same family as the
[[workflow-teeth-check-mtime]] false-green.
