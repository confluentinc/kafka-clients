# PR #16 — KAFKA-20072: Don't generate IDs with hyphens

## AK Commit

- **Commit**: `474798afbed318c7ff316b2ca38e559aa1a136ad`
- **Branch**: `trunk`
- **Title**: KAFKA-20072: Don't generate IDs with hyphens (#21313)

## Summary of the Java Change

The `Uuid#randomUuid()` method was changed to exclude any generated UUID
whose base64 string representation **contains** a hyphen (`-`) anywhere,
not just at the start. Previously the guard was:

```java
while (RESERVED.contains(uuid) || uuid.toString().startsWith("-")) {
```

After the fix:

```java
while (RESERVED.contains(uuid) || uuid.toString().contains("-")) {
```

The Javadoc comment was updated accordingly, and the unit test
`UuidTest#testRandomUuid` assertion was updated from `startsWith` to
`contains`.

Additionally, a new section "Recommendations for 3rd-party Clients:
Member ID Format" was appended to `docs/design/protocol.md`, stating
that generated member IDs should be URL-safe base64 UUIDs **without
hyphens**.

## Motivation

The base64-URL charset used for `Uuid.toString()` includes `-` at
character index 62. This means a UUID's base64 string can contain a
hyphen not only at the very first character but at any position. The
original `startsWith` check therefore failed to exclude such UUIDs,
resulting in IDs that contain hyphens in internal positions. The fix
closes this gap and aligns the implementation with the documented
guarantee.

## Rust Translation Scope

All changes are confined to **`src/common/uuid.rs`**.

### 1. Fix `random_uuid()` loop guard

**File**: `src/common/uuid.rs`

Current code:
```rust
if uuid.to_base64_string().starts_with('-') {
    continue;
}
```

Change to:
```rust
if uuid.to_base64_string().contains('-') {
    continue;
}
```

### 2. Update the doc comment on `random_uuid()`

Current:
```
/// This will not generate a UUID equal to `ZERO_UUID`, `ONE_UUID`, or one whose
/// string representation starts with a dash ("-").
```

Change to:
```
/// This will not generate a UUID equal to `ZERO_UUID`, `ONE_UUID`, or one whose
/// string representation contains a dash ("-").
```

### 3. Update the `test_random_uuid` test

**File**: `src/common/uuid.rs`, `tests` module

Current assertion:
```rust
assert!(!random_id.to_string().starts_with('-'));
```

Change to:
```rust
assert!(!random_id.to_string().contains('-'));
```

Also update the doc comment above the test:
```
/// and do not start with a dash.
```
becomes:
```
/// and do not contain a dash.
```

## Files Changed

| File | Change |
|------|--------|
| `src/common/uuid.rs` | Fix loop guard in `random_uuid`, update doc comment, update test |

## Out of Scope

The `docs/design/protocol.md` addition (recommendations for member ID
format in 3rd-party clients) is documentation for the Java broker
protocol. The equivalent guidance is already implied in the Rust
codebase by the fact that `random_uuid()` excludes hyphens. No separate
documentation file needs to be created in the Rust repo for this.

## Verification

After the change, run:

```
cargo test -p confluent-kafka-rust uuid
```

All existing UUID tests should pass. In particular `test_random_uuid`
(100 iterations) must pass with the stronger `contains` check.
