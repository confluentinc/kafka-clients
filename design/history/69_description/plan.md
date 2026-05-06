# PR #69: Introduce CloseOptions for Consumer close API

## AK Commit

`2dffe32c2a36dc40e0fbcec3d2438275b4f268be`
**KAFKA-19249: Replace Consumer#close(Duration) with Consumer#close(CloseOptions) (#20983)**

## Summary

The AK commit deprecates `Consumer#close(Duration)` and migrates all call sites to
`Consumer#close(CloseOptions)`. The `CloseOptions` type (introduced in AK 4.1) provides
a richer, extensible shutdown API:

- Optional timeout (`Optional<Duration>`, default: `DEFAULT_CLOSE_TIMEOUT_MS`)
- Optional group membership operation on close
  (`GroupMembershipOperation`: `LEAVE_GROUP`, `REMAIN_IN_GROUP`, `DEFAULT`)
- Builder-style fluent API with static factory methods `timeout()` and `groupMembershipOperation()`

The Rust codebase does not yet have a `Consumer` trait or `MockConsumer`. This PR
introduces `CloseOptions` and its nested `GroupMembershipOperation` enum so that when
the Consumer module is translated it starts with the non-deprecated API.

### Changed files in AK commit (for reference)

| File | Change |
|------|--------|
| `clients/.../consumer/MockConsumer.java` | `close()` delegates to `close(CloseOptions.timeout(...))` instead of `close(Duration...)` |
| `core/.../integration/kafka/api/IntegrationTestHarness.scala` | test teardown uses `CloseOptions.timeout(Duration.ZERO)` |
| `core/.../kafka/server/DynamicBrokerReconfigurationTest.scala` | test teardown uses `CloseOptions.timeout(Duration.ZERO)` |
| `storage/.../metadata/storage/ConsumerTask.java` | `closeConsumer()` uses `CloseOptions.timeout(...)`, removes `@SuppressWarnings("deprecation")` |

Test-only and server-side storage files are out of scope for the Rust client translation.

---

## Scope

**In scope:**
- `CloseOptions` struct with builder methods
- `GroupMembershipOperation` enum (nested in Java, top-level in Rust module)
- Module scaffolding: `src/consumer/mod.rs` (new top-level consumer module)

**Out of scope (deferred to later PRs):**
- `Consumer` trait
- `MockConsumer`
- `KafkaConsumer`
- Any consumer internals

---

## Java Source Reference

| Java class | Rust target |
|------------|-------------|
| `org.apache.kafka.clients.consumer.CloseOptions` | `src/consumer/close_options.rs` |
| `CloseOptions.GroupMembershipOperation` | `src/consumer/close_options.rs` (same file) |

---

## Rust Design

### `GroupMembershipOperation` (`close_options.rs`)

```rust
/// Specifies the group membership operation upon consumer close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupMembershipOperation {
    /// The consumer will leave the group.
    LeaveGroup,
    /// The consumer will remain in the group.
    RemainInGroup,
    /// Applies the default behavior:
    /// - static members remain in the group
    /// - dynamic members leave the group
    Default,
}

impl Default for GroupMembershipOperation {
    fn default() -> Self {
        Self::Default
    }
}
```

### `CloseOptions` (`close_options.rs`)

```rust
/// Options for closing a consumer.
///
/// Created via static factory methods; fields are mutated with fluent `with_*` setters.
///
/// ```
/// use std::time::Duration;
/// use kafka::consumer::{CloseOptions, GroupMembershipOperation};
///
/// // Close with a custom timeout
/// let opts = CloseOptions::timeout(Duration::from_secs(5));
///
/// // Stay in group on close
/// let opts = CloseOptions::group_membership_operation(GroupMembershipOperation::RemainInGroup);
///
/// // Combine both
/// let opts = CloseOptions::timeout(Duration::from_secs(5))
///     .with_group_membership_operation(GroupMembershipOperation::RemainInGroup);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseOptions {
    operation: GroupMembershipOperation,
    timeout: Option<Duration>,
}

impl Default for CloseOptions {
    fn default() -> Self {
        Self {
            operation: GroupMembershipOperation::Default,
            timeout: None,
        }
    }
}

impl CloseOptions {
    /// Creates a `CloseOptions` with a custom timeout.
    pub fn timeout(timeout: Duration) -> Self {
        Self::default().with_timeout(Some(timeout))
    }

    /// Creates a `CloseOptions` with a specified group membership operation.
    pub fn group_membership_operation(operation: GroupMembershipOperation) -> Self {
        Self::default().with_group_membership_operation(operation)
    }

    /// Sets the timeout. Pass `None` to restore the default.
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the group membership operation.
    pub fn with_group_membership_operation(mut self, operation: GroupMembershipOperation) -> Self {
        self.operation = operation;
        self
    }

    /// Returns the group membership operation.
    pub fn group_membership_operation(&self) -> GroupMembershipOperation {
        self.operation
    }

    /// Returns the optional timeout. `None` means use `DEFAULT_CLOSE_TIMEOUT_MS`.
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }
}
```

**Notes:**
- Java's `Optional<Duration>` maps to `Option<Duration>`.
- `withTimeout(null)` in Java resets to empty — translated as `with_timeout(None)`.
- The private constructor pattern becomes `Default` in Rust.
- The Java getter `timeout()` returning `Optional<Duration>` returns `Option<Duration>` by value
  (copy-cheap `Duration`).

### Module layout

```
src/consumer/
└── mod.rs              # pub mod close_options; pub use close_options::{CloseOptions, GroupMembershipOperation};
└── close_options.rs    # CloseOptions + GroupMembershipOperation
```

`src/lib.rs` gains `pub mod consumer;`.

---

## Tests

Unit tests in `src/consumer/close_options.rs` (inline `#[cfg(test)]`):

| Test | What it checks |
|------|----------------|
| `default_has_no_timeout_and_default_operation` | `CloseOptions::default()` fields |
| `timeout_factory_sets_timeout` | `CloseOptions::timeout(d).timeout() == Some(d)` |
| `group_membership_operation_factory` | `CloseOptions::group_membership_operation(LeaveGroup).group_membership_operation() == LeaveGroup` |
| `with_timeout_none_clears_timeout` | `opts.with_timeout(None).timeout() == None` |
| `with_group_membership_operation_overwrites` | fluent setter replaces previous value |
| `builder_chain` | `CloseOptions::timeout(d).with_group_membership_operation(RemainInGroup)` has both fields set |

---

## Implementation Steps

1. Add `src/consumer/mod.rs` with module declaration and re-exports.
2. Add `src/consumer/close_options.rs` with `GroupMembershipOperation` and `CloseOptions`.
3. Add `pub mod consumer;` to `src/lib.rs`.
4. Write unit tests (inline in `close_options.rs`).

## Definition of Done

- `cargo build` passes.
- `cargo test` passes (all existing tests + new unit tests).
- `cargo xtask format-check` passes.
- `cargo xtask lint` passes.
