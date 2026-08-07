---
name: m11-bindings-b5a-notes
description: M11 admin bindings B5a (ACLs + client quotas) — kafka_common_* namespacing for common-package types, the null-vs-absent decision rule by kind, and fixtures whose counts must differ
metadata:
  type: project
---

Admin C FFI + Python bindings, slice B5a (`createAcls`, `describeAcls`,
`deleteAcls`, `describeClientQuotas`, `alterClientQuotas`), landed on
`dev/admin-bindings`. Builds on [[m11_bindings_b0_b1_notes]],
[[m11_bindings_b2_notes]], [[m11_bindings_b3_notes]] and
[[m11_bindings_b4_notes]].

**Why:** B5b and B6 hit the first two of these directly, and the third is a
class of dead test that looks alive.

## The namespace rule finally produced a `kafka_common_*` type

Round 7 ruled that `kafka_consumer_TopicPartition_t` is mis-namespaced because
`TopicPartition` is `org.apache.kafka.common`. B5a is the first slice where the
rule *creates* types rather than just forbidding reuse: `AclBinding`,
`AclBindingFilter` (`.acl`), `ResourcePattern` (`.resource`) and
`ClientQuotaEntity` (`.quota`) are all `org.apache.kafka.common`, so they are
`kafka_common_AclBinding_t` etc., **not** `kafka_admin_*`. Precedent already in
the tree: `kafka_common_Node_t`, `kafka_common_KafkaError_t`.

**How to apply:** before naming any new FFI type, find the Java package. Only
`org.apache.kafka.clients.admin.*` earns `kafka_admin_*`.

## Output-only borrowed handle, input-only parallel arrays — never both

`AclBinding` is needed as a *value* in three results (createAcls key,
describeAcls listing, deleteAcls FilterResult) and as an *input* to createAcls.
Resist making one handle serve both: a type that is caller-owned on the request
side and borrowed on the result side is the ownership confusion round 7 flagged.
Resolution: the handle is output-only (`*const`, "do not free"), requests cross
as parallel arrays — the `alterConsumerGroupOffsets` /
`listConsumerGroupOffsets` shape. Say so explicitly in the module comment;
otherwise the asymmetry reads as an oversight.

Reuse across ≥2 results is what justifies a handle at all. `AclBindingFilter`
appears as a key in only one result but got a handle anyway, for symmetry with
`AclBinding` — worth stating as a deliberate choice, not letting it look
inconsistent with the flattened `ConfigResource` key.

## Null-versus-absent: decide **per kind**, do not apply one rule

B3's lesson was "give every Java `Optional` an explicit discriminant". B5a
shows that is too broad, and the sharper rule is:

  - **Nullable string → a null pointer, no discriminant.** A null pointer
    cannot collide with a pointer to `""`, and an empty resource name /
    principal / host / quota-entity name is a legal, *distinct* value. Adding
    a `_has_name()` beside it would be redundant noise.
  - **Nullable number → an explicit `bool`.** Every `double` (0 included) is a
    legal quota value, so no sentinel works. `op_has_values[i][j] == false` is
    Java's `Op(key, null)` = *remove the quota*.
  - **A tri-state that is not string-nullability → an explicit `int32`.** A
    quota filter component's match is EXACT / DEFAULT / ANY, and DEFAULT and
    ANY *both carry no name*, so a null name could not separate them — and they
    differ in equality **and** in the wire match-type byte.

The discriminant values must be real Kafka constants, never invented:
`MATCH_TYPE_EXACT/DEFAULT/SPECIFIED` (0/1/2) already exist in
`common::requests::describe_client_quotas_request`.

Also: an out-of-range accessor returning a `double` needs the
`bool fn(..., double *out)` shape for the same reason — there is no safe
sentinel. `-1` works for a partition id; nothing works for a quota value.

## `sorted_entries` needs `K: Ord`, and these keys do not have it

`AclBinding`, `AclBindingFilter` and `ClientQuotaEntity` are `Hash + Eq` in
Java and in Rust, but not `Ord`. Do not invent an `Ord` for a Java type that
has none — sort the *already-flattened* rows by their C-visible field tuple
instead. A generic `fn sort_rows_by_key<I, E, K: Ord>(rows, key: impl Fn(&I) -> K)`
does **not** compile when `K` borrows from `I` (needs GATs); write the
comparison closure out at each site.

## Fixture *shape* must be asymmetric, not just fixture *values*

Both the C and the Rust `alterClientQuotas` fixtures were first written with
entity counts `(2, 1)` and op counts `(2, 1)`. Substituting one `*const i32`
count array for the other compiles cleanly and would have passed both suites.
Changed to `(2, 1)` and `(1, 2)`, and the mutation then fails both.

**How to apply:** for any two same-typed parallel inputs, check the fixture
gives them *different* values. This extends the round-5 "call every option
builder twice with asymmetric flags" rule from flags to **counts, lengths and
indices** — the shape of the fixture, not only the values in it. Carry into
B5b/B6: whenever two arrays of the same C type sit side by side in a signature,
make their per-row lengths differ.

## A mutation nothing catches means a real coverage hole, not a bad mutation

The Python teeth run applied two mutations. The principal/host transposition
failed 3 tests. The second — collapsing a `None` op value into `0.0`, i.e.
turning "remove this quota" into "set it to zero" — **passed the entire
suite**, because Java's mock throws before echoing any op back, so the outbound
half of `alterClientQuotas` has no end-to-end observable at all.

The fix is not to pick a different mutation. It is to make the outbound
marshaling a pure function and test it directly: `_acl_binding_rows`,
`_acl_filter_rows` and `_quota_alteration_rows` are now static methods with
their own tests. This is the round-5 "unit-test the pure flatteners" rule
applied to the **request** direction, which earlier slices only ever applied to
the response direction.

**How to apply:** for every mock-unsupported RPC, ask what part of the *request*
the mock discards. That part needs a direct test of the row builder; no
end-to-end test can reach it.

## The teeth check must survive `#![deny(warnings)]`

Deleting a discriminant read (`let present = true;`) is rejected as
`unused_variable`, which proves nothing about the tests. *Inverting* it
(`let present = has_values.is_null() || !*has_values.add(index);`) keeps the
variable used, compiles, and is caught only by a test. Same lesson as B3's
"call-site argument swap, not field transposition", one level down.

## `cargo xtask check-bindings` caught its author

Written this slice (see [[m11_bindings_check_bindings_gate]]), it immediately
rejected `PyArg_ParseTuple(item, nullable ? "izizzii" : "isissii", ...)` in the
new code as a non-literal format. Split into two literal-format calls. Worth
knowing the gate treats an unverifiable site as a failure, not a pass.

## Small mechanics

  - `read_strings` **skips** NULL entries. For parallel arrays that is a bug
    generator: a dropped entry shifts every later row's fields onto the wrong
    record. Nullable string arrays need a `optional_string_at(ptr, i)` that
    preserves NULL as `None`.
  - Wrapping a nested `KafkaError`'s text: `e.message()`, not `{e}` (B3), and
    prefix the row index — C has no other way to say which entry was wrong:
    `acl at index 1: resourceType must not be ANY`.
  - The mock strings differ between the two domains and both are asserted
    verbatim: ACL RPCs throw `"Not implemented yet"`, quota RPCs
    `"Not implement yet"` (Java's own typo,
    `MockAdminClient.java:1243`/`:1248`).
  - Panic audit came back clean but only after tracing:
    `ResourcePatternFilter::matches` **can** panic on an unsupported pattern
    type, but nothing outside its own unit tests calls it.
    `AccessControlEntry::principal()`/`host()` `.expect()` a present value, and
    every construction site goes through `AccessControlEntry::new` (which
    always stores both) while the wire decoders propagate the constructor's
    `Result` rather than unwrapping. Do not assume — grep the callers.
  - Java's filter constructors accept exactly what the binding constructors
    reject (ANY on all four enums, MATCH pattern type, null strings). Test that
    asymmetry from **both** sides: `create_acls` rejecting with Java's exact
    message, and `describe_acls` with the same values reaching the mock
    instead. Only the second shows marshaling accepted them.
