# Logging Design Document

## Context

The Rust Kafka client needs structured logging that matches the Java client's messages, levels, and contextual prefixes. Java uses SLF4J with a custom `LogContext` wrapper that prepends `[Producer clientId=X]` to every message. The Rust client currently uses the `log` crate with 125 direct macro calls across 13 modules, but has no `LogContext` equivalent, no `log_enabled!` guards, and ~10-15% of messages diverge from Java wording.

## Goals

1. **Exact message parity with Java client** — same wording, same level, same arguments
2. **LogContext prefix injection** — `[Producer clientId=X]` prefix on every message, matching Java
3. **Guard expensive formatting** — use `log_enabled!` before computing expensive arguments
4. **RUST_LOG filtering** — works by default via module path targets (no extra setup needed)
5. **Zero overhead when disabled** — log macros already skip formatting; guards skip computation

## Non-Goals

- Choosing a log backend (env_logger, tracing-subscriber, etc.) — that's the user's responsibility
- Structured/key-value logging — `log` crate's kv feature is unstable
- Adding log calls to modules that don't have them in Java

---

## Architecture

### LogContext

Translated from `org.apache.kafka.common.utils.LogContext` (kafka/clients/src/main/java/org/apache/kafka/common/utils/LogContext.java).

```rust
// src/common/utils/log_context.rs

/// Provides contextual log message prefixes, matching Java's LogContext.
///
/// Created once per client instance and passed to all components.
/// The prefix is prepended to every log message automatically via
/// the kafka_* logging macros.
#[derive(Clone, Debug)]
pub struct LogContext {
    prefix: String,
}

impl LogContext {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    pub fn empty() -> Self {
        Self {
            prefix: String::new(),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}
```

**Java equivalence:**

| Java | Rust |
|------|------|
| `new LogContext("[Producer clientId=" + id + "] ")` | `LogContext::new(format!("[Producer clientId={}] ", id))` |
| `logContext.logger(KafkaProducer.class)` | Store `log_context: LogContext` as a field; use `kafka_*!` macros |
| `log.debug("message {}", arg)` | `kafka_debug!(self.log_context, "message {}", arg)` |

### Logging Macros

Define macros in `src/common/utils/log_macros.rs` that wrap the `log` crate macros and prepend the LogContext prefix.

```rust
// src/common/utils/log_macros.rs

/// Log at ERROR level with LogContext prefix.
macro_rules! kafka_error {
    ($ctx:expr, $($arg:tt)*) => {
        log::error!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}

/// Log at WARN level with LogContext prefix.
macro_rules! kafka_warn {
    ($ctx:expr, $($arg:tt)*) => {
        log::warn!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}

/// Log at INFO level with LogContext prefix.
macro_rules! kafka_info {
    ($ctx:expr, $($arg:tt)*) => {
        log::info!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}

/// Log at DEBUG level with LogContext prefix.
macro_rules! kafka_debug {
    ($ctx:expr, $($arg:tt)*) => {
        log::debug!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}

/// Log at TRACE level with LogContext prefix.
macro_rules! kafka_trace {
    ($ctx:expr, $($arg:tt)*) => {
        log::trace!("{}{}", $ctx.prefix(), format_args!($($arg)*))
    };
}
```

**Why this works:**

- `log::trace!` already checks `lvl <= log::max_level()` before evaluating `format_args!` — zero cost when disabled
- `format_args!` returns `Arguments<'_>` (implements `Display`) — no allocation, no formatting until output
- `module_path!()` inside `log::trace!` expands at the **call site**, so `target` is the caller's module (e.g., `confluent_kafka::producer::kafka_producer`), enabling correct `RUST_LOG` filtering
- The prefix is only read (not cloned) when the level is enabled

### Component Wiring

Each component stores a `LogContext` field, matching Java's pattern where LogContext is passed through constructors:

```
KafkaProducer (creates LogContext)
  |
  +-> Sender (receives LogContext)
  |     +-> NetworkClient (receives LogContext)
  |           +-> Metadata (receives LogContext)
  |
  +-> RecordAccumulator (receives LogContext)
        +-> BuiltInPartitioner (receives LogContext)
```

**Java reference** (KafkaProducer.java:354):
```java
LogContext logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
```

**Rust equivalent** (kafka_producer.rs):
```rust
let log_context = LogContext::new(format!("[Producer clientId={}] ", client_id));
let sender = Sender::new(log_context.clone(), ...);
let accumulator = RecordAccumulator::new(log_context.clone(), ...);
```

---

## RUST_LOG Filtering

Works automatically — no code changes needed. The `log` crate macros use `module_path!()` as the default `target`, which maps to the Rust module path. Users filter with `RUST_LOG` environment variable:

| Java Logger Name | Rust Module Target | RUST_LOG Filter |
|------------------|--------------------|-----------------|
| `o.a.k.clients.producer.KafkaProducer` | `confluent_kafka::producer::kafka_producer` | `confluent_kafka::producer::kafka_producer=debug` |
| `o.a.k.clients.producer.internals.Sender` | `confluent_kafka::producer::internals::sender` | `confluent_kafka::producer::internals::sender=trace` |
| `o.a.k.clients.NetworkClient` | `confluent_kafka::network_client` | `confluent_kafka::network_client=debug` |
| `o.a.k.clients.Metadata` | `confluent_kafka::metadata` | `confluent_kafka::metadata=info` |
| All Kafka client | `confluent_kafka` | `confluent_kafka=debug` |
| Producer only | `confluent_kafka::producer` | `confluent_kafka::producer=trace` |

**Example usage:**
```bash
# All Kafka logs at debug
RUST_LOG=confluent_kafka=debug ./my_app

# Only sender trace, rest at warn
RUST_LOG=confluent_kafka=warn,confluent_kafka::producer::internals::sender=trace ./my_app
```

The library does **not** initialize a log backend — that is the application's responsibility. Any `log`-compatible backend (env_logger, tracing-subscriber, fern, slog, etc.) works.

---

## log_enabled! Guard Pattern

The `log` crate macros already defer argument formatting via `format_args!`. Guards are only needed when **computing the arguments themselves** is expensive — not for the formatting.

### When NOT to guard (most cases)

```rust
// No guard needed: field access and Display are cheap
kafka_debug!(self.log_context, "Node {} disconnected.", node_id);
kafka_warn!(self.log_context, "Error connecting to node {}: {}", node, err);
kafka_trace!(self.log_context, "Sending request with correlation id {}", corr_id);
```

### When to guard

Guard with `log_enabled!` when the code must iterate collections, call expensive `.to_string()`, or perform computation to prepare arguments:

```rust
// Java (RecordAccumulator.java:799-810):
//   if (log.isTraceEnabled()) {
//       if (shouldBackoff)
//           log.trace("Skipping send since batch for partition {} ...");
//       else
//           log.trace("Backoff is not needed since last request ...");
//   }
//
// Rust equivalent:
if log::log_enabled!(log::Level::Trace) {
    if should_backoff {
        kafka_trace!(self.log_context,
            "Skipping send since batch for partition {} has active produce requests.", tp);
    } else {
        kafka_trace!(self.log_context,
            "Backoff is not needed since last request attempt on partition {} has an error ...", tp);
    }
}

// Java (KafkaProducer.java:1592):
//   recordLogString = log.isTraceEnabled() && record != null ? record.toString() : "";
//
// Rust equivalent:
let record_log_string = if log::log_enabled!(log::Level::Trace) {
    format!("{}", record)
} else {
    String::new()
};

// Java (Metadata.java:440-442):
//   if (log.isDebugEnabled()) {
//       updatePartitionMetadata.forEach(partMetadata ->
//           log.debug("For {} updating leader information ...", partMetadata.topicPartition, partMetadata));
//   }
//
// Rust equivalent:
if log::log_enabled!(log::Level::Debug) {
    for part_metadata in &update_partition_metadata {
        kafka_debug!(self.log_context,
            "For {} updating leader information, updated metadata is {}.",
            part_metadata.topic_partition, part_metadata);
    }
}

// Java (NetworkClient.java:604-606):
//   if (log.isDebugEnabled()) {
//       log.debug("Sending {} request with header {} ...", apiKey, header, timeout, dest, request);
//   }
//
// Rust equivalent — request.Display is expensive:
if log::log_enabled!(log::Level::Debug) {
    kafka_debug!(self.log_context,
        "Sending {} request with header {} and timeout {} to node {}: {}",
        client_request.api_key(), header, timeout, destination, request);
}
```

### Rule of thumb

| Argument type | Guard needed? |
|---------------|---------------|
| Primitive / integer / &str | No |
| Simple field access | No |
| Error enum Display | No |
| `format!()` to build a string | Yes |
| `.to_string()` on complex struct | Yes |
| Iterator / collection formatting | Yes |
| Conditional logic to pick message | Yes |
| Debug formatting (`{:?}`) on large struct | Yes |

---

## Log Level Guidelines

Match Java's level choices exactly. Reference: SLF4J levels map 1:1 to `log` crate levels.

| Level | Purpose | Java Example | Guard? |
|-------|---------|-------------|--------|
| ERROR | Unrecoverable failures, auth errors | `"Uncaught error in kafka producer I/O thread:"` | Never |
| WARN | Degraded state, retryable errors, config issues | `"Connection to node {} could not be established."` | Never |
| INFO | Lifecycle events, configuration | `"Starting the Kafka producer"`, `"Cluster ID: {}"` | Rarely |
| DEBUG | Connection state, request/response flow, metadata | `"Initiating connection to node {}"` | When args are expensive |
| TRACE | Per-record, per-batch, per-partition operations | `"Attempting to append record {}"` | Often |

---

## Message Alignment with Java

Existing log messages that diverge from Java must be updated. Key differences found:

| Rust (current) | Java (reference) | Action |
|----------------|------------------|--------|
| `"Starting Kafka producer I/O task."` | `"Starting Kafka producer I/O thread."` | Update |
| `"Beginning shutdown of Kafka producer I/O task"` | `"Beginning shutdown of Kafka producer I/O thread"` | Update |
| Various `{:?}` Debug formatting | Java uses `{}` (toString) | Implement Display, use `{}` |

When translating new log messages from Java:
- `{}` placeholder in Java SLF4J maps to `{}` in Rust's `format_args!`
- `Exception.toString()` maps to `err` (Display impl)
- `log.warn("msg", exception)` with throwable maps to `kafka_warn!(ctx, "msg: {}", err)`

---

## Files to Modify

### New files
- `src/common/utils/log_context.rs` — LogContext struct
- `src/common/utils/log_macros.rs` — kafka_trace!, kafka_debug!, etc. macros

### Modified files (add LogContext field + migrate to kafka_* macros)

| File | Log calls | LogContext source |
|------|-----------|-------------------|
| `src/producer/kafka_producer.rs` | 13 | Creates: `LogContext::new(format!("[Producer clientId={}] ", client_id))` |
| `src/network_client.rs` | 41 | Receives from KafkaProducer |
| `src/metadata.rs` | 20 | Receives from NetworkClient |
| `src/producer/internals/sender.rs` | 18 | Receives from KafkaProducer |
| `src/producer/internals/producer_batch.rs` | 9 | Receives from RecordAccumulator |
| `src/producer/internals/built_in_partitioner.rs` | 6 | Receives from RecordAccumulator |
| `src/producer/internals/record_accumulator.rs` | 5 | Receives from KafkaProducer |
| `src/common/network/selector.rs` | 5 | Receives from NetworkClient |
| `src/common/security/authenticator/sasl_client_authenticator.rs` | 3 | Receives from channel builder |
| `src/client_utils.rs` | 2 | Receives from caller |
| `src/cluster_connection_states.rs` | 1 | Receives from NetworkClient |
| `src/common/network/network_receive.rs` | 1 | No context (standalone warn) |
| `src/producer/producer_config.rs` | 1 | No context (config validation) |
| `src/producer/internals/producer_metadata.rs` | 1 | Receives from KafkaProducer |

### Modules to re-export
- `src/common/utils/mod.rs` — re-export LogContext
- `src/lib.rs` or crate root — `#[macro_use]` for log_macros

---

## Migration Strategy

1. **Add LogContext and macros** — new files, no existing code changes
2. **Wire LogContext through constructors** — KafkaProducer creates it, passes to children
3. **Migrate existing log calls module by module** — replace `debug!(...)` with `kafka_debug!(self.log_context, ...)`
4. **Add log_enabled! guards** — where Java uses `isTraceEnabled()` / `isDebugEnabled()`
5. **Align message wording** — compare each message against Java source, fix divergences
6. **Add missing log messages** — Java log calls not yet translated

Each step should be a separate commit. Steps 3-6 can be done per-module.

---

## Example: Full Module Migration

**Before (sender.rs):**
```rust
use log::{debug, trace, warn, error};

debug!("Starting Kafka producer I/O task.");
warn!("Cancelled request {} due to a version mismatch with node {}: {}",
      response, response.destination(), response.version_mismatch().unwrap_or("unknown"));
```

**After (sender.rs):**
```rust
use crate::common::utils::LogContext;

struct Sender {
    log_context: LogContext,
    // ...
}

kafka_debug!(self.log_context, "Starting Kafka producer I/O thread.");
kafka_warn!(self.log_context,
    "Cancelled request {} due to a version mismatch with node {}: {}",
    response, response.destination(), response.version_mismatch().unwrap_or("unknown"));
```

**Output with `RUST_LOG=confluent_kafka=debug`:**
```
[2025-01-15T10:30:00Z DEBUG confluent_kafka::producer::internals::sender] [Producer clientId=producer-1] Starting Kafka producer I/O thread.
```

---

## Verification

1. `cargo build` — macros compile correctly, LogContext wired through all constructors
2. `cargo test` — all existing tests pass (logging doesn't affect behavior)
3. `make verify` — format, lint, all test suites
4. Manual: `RUST_LOG=confluent_kafka=trace cargo test -- --nocapture` — verify prefix appears, messages match Java
5. Manual: `RUST_LOG=confluent_kafka::producer::internals::sender=trace,confluent_kafka=warn` — verify filtering works
6. Spot-check 5 messages per module against Java source
