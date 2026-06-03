# Phase 1: Foundation Types

## Goal

Translate the public foundation types in `org.apache.kafka.clients.consumer.*`
that the `Consumer<K, V>` trait surface (Phase 2) and `MockConsumer` (Phase 3)
will depend on. No trait, no networking, no background task — just the value
types, their tests, and the module skeleton.

## Branch

`consumer-impl`. All commits land here.

## Java sources

All paths relative to `kafka/clients/src/main/java/org/apache/kafka/clients/consumer/`,
at submodule commit `a18251bae0b825c69794a50dffd4c3100cf5ca5b`.

Production:

- `ConsumerRecord.java` (274)
- `ConsumerRecords.java` (147)
- `ConsumerConfig.java` (815)
- `ConsumerGroupMetadata.java` (100)
- `OffsetAndMetadata.java` (127)
- `OffsetAndTimestamp.java` (85)
- `OffsetResetStrategy.java` (33) — `@Deprecated` enum; translate to match public surface but mark deprecated
- `internals/AutoOffsetResetStrategy.java` (182)
- `GroupProtocol.java` (43)
- `CloseOptions.java` (113)
- `SubscriptionPattern.java` (59)
- `CommitFailedException.java` (43)
- `InvalidOffsetException.java` (38)
- `LogTruncationException.java` (60)
- `NoOffsetForPartitionException.java` (53)
- `OffsetOutOfRangeException.java` (54)
- `RetriableCommitFailedException.java` (37)

Tests (translate each — DoD §3):

- `ConsumerRecordTest.java` (101) → `tests/consumer/consumer_record_test.rs`
- `ConsumerRecordsTest.java` (220) → `tests/consumer/consumer_records_test.rs`
- `ConsumerConfigTest.java` (295) → `tests/consumer/consumer_config_test.rs`
- `ConsumerGroupMetadataTest.java` (89) → `tests/consumer/consumer_group_metadata_test.rs`
- `OffsetAndMetadataTest.java` (83) → `tests/consumer/offset_and_metadata_test.rs`
- `CloseOptionsTest.java` (51) → `tests/consumer/close_options_test.rs`
- `internals/AutoOffsetResetStrategyTest.java` (121) →
  `tests/consumer/auto_offset_reset_strategy_test.rs`

Any test not listed above doesn't apply to this phase's classes. There is no
`SubscriptionPatternTest`, `OffsetAndTimestampTest`, `GroupProtocolTest`,
`OffsetResetStrategyTest`, or per-exception test in the Java repo at this
commit; we don't invent ones that don't exist.

## Out of scope (deferred to later phases)

- `Consumer<K, V>` trait (Phase 2)
- `ConsumerRebalanceListener`, `OffsetCommitCallback`,
  `ConsumerInterceptor`, `Deserializer` (Phase 2)
- `ConsumerInterceptors`, `Deserializers` (Phase 2)
- `Consumer.java` interface, `KafkaConsumer.java`, `MockConsumer.java`,
  any `*Assignor*`, any share-consumer file, any classic-protocol class
- Anything in `internals/` other than `AutoOffsetResetStrategy.java`

## Module structure produced by this phase

```
src/consumer/
├── mod.rs                         # pub mod + pub use re-exports (see §"Module file" below)
├── consumer_config.rs
├── consumer_record.rs
├── consumer_records.rs
├── consumer_group_metadata.rs
├── offset_and_metadata.rs
├── offset_and_timestamp.rs
├── offset_reset_strategy.rs
├── auto_offset_reset_strategy.rs  # public re-export; defined in internals/
├── group_protocol.rs
├── close_options.rs
├── subscription_pattern.rs
├── errors.rs                      # consumer exception hierarchy
└── internals/
    ├── mod.rs                     # pub(crate) per CLAUDE.md §2
    └── auto_offset_reset_strategy.rs

tests/consumer/
├── mod.rs
├── consumer_record_test.rs
├── consumer_records_test.rs
├── consumer_config_test.rs
├── consumer_group_metadata_test.rs
├── offset_and_metadata_test.rs
├── close_options_test.rs
└── auto_offset_reset_strategy_test.rs
```

Update `src/lib.rs` to add `pub mod consumer;`. Update `tests/` entry point
to include the new `consumer/` test module.

## Type-by-type spec

### `ConsumerRecord<K, V>` (`consumer_record.rs`)

Per `consumer-threading.md` §27, **topic must be `Arc<str>`**, not `String`.
The same `Arc<str>` instance is cloned per record from `SubscriptionState`
(Phase 4) — for this phase we just establish the type. Headers are owned
(`Headers` = `Vec<RecordHeader>`) per §27 — do NOT add a lifetime parameter
to `ConsumerRecord<K, V>`.

```rust
pub struct ConsumerRecord<K, V> {
    topic: Arc<str>,
    partition: i32,
    offset: i64,
    timestamp: i64,
    timestamp_type: TimestampType,
    serialized_key_size: i32,
    serialized_value_size: i32,
    key: Option<K>,
    value: Option<V>,
    headers: Headers,
    leader_epoch: Option<i32>,
    delivery_count: Option<i16>,
}

pub const NO_TIMESTAMP: i64 = -1;       // RecordBatch.NO_TIMESTAMP
pub const NULL_SIZE: i32 = -1;
```

Constructors mirror Java's three (5-arg, 11-arg, 12-arg) — the 5-arg form
forwards to the 12-arg form with defaults `NO_TIMESTAMP`,
`TimestampType::NoTimestampType`, `NULL_SIZE`, empty headers,
`leader_epoch = None`, `delivery_count = None`.

Validation: matches Java — topic non-null is a Rust type-system invariant
already, but `Arc::from("")` is still legal and Java permits empty topic in
this class. Headers non-null is a Rust invariant. **No panic** in
constructors beyond what Java throws.

Getters return borrowed references:

- `fn topic(&self) -> &str` — returns `&str`, not `&Arc<str>`
- `fn partition(&self) -> i32`
- `fn offset(&self) -> i64`
- `fn timestamp(&self) -> i64`
- `fn timestamp_type(&self) -> TimestampType`
- `fn serialized_key_size(&self) -> i32`
- `fn serialized_value_size(&self) -> i32`
- `fn key(&self) -> Option<&K>`
- `fn value(&self) -> Option<&V>`
- `fn headers(&self) -> &Headers`
- `fn leader_epoch(&self) -> Option<i32>`
- `fn delivery_count(&self) -> Option<i16>`

`Debug` derive. `Display` matches Java `toString()` format (test asserts this).
No `PartialEq`/`Eq` — Java's `ConsumerRecord` doesn't override `equals`.
No `Clone` derive either — we don't need it on the receive path.

`TimestampType` already exists in this crate? **Check**: search for
`pub enum TimestampType` under `src/common/`. If it doesn't exist, add it as
`src/common/record/timestamp_type.rs` translating
`kafka/clients/src/main/java/org/apache/kafka/common/record/TimestampType.java`
(small enum: `CreateTime`, `LogAppendTime`, `NoTimestampType`). It is shared
with producer-side code, so it belongs in `common/record/`, not `consumer/`.

`Headers` already exists? **Check**: search for `pub struct Headers` /
`pub trait Headers`. If not, add `src/common/header/{mod.rs, header.rs,
headers.rs}` translating `org.apache.kafka.common.header.{Header, Headers}`
and `org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}`.
Per §27, `Headers` is `Vec<RecordHeader>` with each `RecordHeader` owning
`Arc<str>` key + `Vec<u8>` value.

### `ConsumerRecords<K, V>` (`consumer_records.rs`)

Java is `Iterable<ConsumerRecord<K,V>>` plus a per-partition map. Rust:

```rust
pub struct ConsumerRecords<K, V> {
    records: HashMap<TopicPartition, Vec<ConsumerRecord<K, V>>>,
    next_offsets: HashMap<TopicPartition, OffsetAndMetadata>,
}
```

Methods:

- `pub const fn empty<K, V>() -> ConsumerRecords<K, V>` — static `EMPTY`
- `pub fn new(records, next_offsets) -> Self`
- `pub fn records_for_partition(&self, p: &TopicPartition) -> &[ConsumerRecord<K, V>]`
- `pub fn records_for_topic(&self, topic: &str) -> impl Iterator<Item = &ConsumerRecord<K, V>>`
- `pub fn partitions(&self) -> impl Iterator<Item = &TopicPartition>`
- `pub fn count(&self) -> usize` — total record count across all partitions
- `pub fn is_empty(&self) -> bool`
- `pub fn next_offsets(&self) -> &HashMap<TopicPartition, OffsetAndMetadata>`
- `impl<'a, K, V> IntoIterator for &'a ConsumerRecords<K, V>` — iterate all records
  in partition order (Java iterates per-partition map insertion order).

Java's `Iterable` is borrowed iteration; the owned consumption variant on the
caller side will be a separate `into_iter` impl on the owned type. Translate
both: `for r in &records` (borrowed) and `for r in records` (owned).

### `OffsetAndMetadata` (`offset_and_metadata.rs`)

```rust
#[derive(Clone, Debug, Eq)]
pub struct OffsetAndMetadata {
    offset: i64,
    leader_epoch: Option<i32>,
    metadata: String,
}
```

Constructors:

- `pub fn new(offset: i64) -> Result<Self, KafkaError>` — leader_epoch=None, metadata=""
- `pub fn with_metadata(offset, metadata: impl Into<String>) -> Result<Self, KafkaError>`
- `pub fn with_leader_epoch(offset, leader_epoch: Option<i32>, metadata) -> Result<Self, KafkaError>`

Validation: offset < 0 returns `Err(KafkaError::InvalidArgument(...))` —
matches Java's `IllegalArgumentException`. Per CLAUDE.md §10.2: Java
unchecked exceptions that are recoverable become `Result`. Negative offset
is recoverable (caller mistake) → Result.

Per Java: `null` metadata is normalized to empty string. In Rust, accept
`Option<String>` in the most general form? — no, just `impl Into<String>`;
empty string is the canonical "no metadata" representation. The constructor
that accepts no metadata passes `""`.

`leader_epoch()` returns `Option<i32>` where the inner value is filtered to
`None` if negative — matches Java line 99–101.

`PartialEq`/`Hash`: only the offset, metadata, and the *filtered*
`leader_epoch()` participate (Java does this — `equals` calls `leaderEpoch()`
not the raw field). Hand-write these; do NOT derive directly on the field.

`Display`: format `OffsetAndMetadata{offset=N, leaderEpoch=X, metadata='Y'}`
where `X` is `null` if `None`. Tests assert this exactly.

### `OffsetAndTimestamp` (`offset_and_timestamp.rs`)

Trivial value type:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OffsetAndTimestamp {
    offset: i64,
    timestamp: i64,
    leader_epoch: Option<i32>,
}
```

Constructors with negative-offset / negative-timestamp validation matching
Java. `Display` impl matching Java `toString()`.

### `OffsetResetStrategy` (`offset_reset_strategy.rs`)

Translate the deprecated Java enum. Mark with `#[deprecated]`. Three
variants: `Latest`, `Earliest`, `None_` (rename clash with `Option::None` —
use `None_` and document; per CLAUDE.md naming convention, Java enum values
become PascalCase Rust enum variants — `None` is a reserved-feeling name but
not a Rust reserved word, so `None` is technically allowed; pick `None_`
explicitly to avoid confusing pattern matches against `Option::None`).

`Display` impl matches Java `toString().toLowerCase()`.

### `AutoOffsetResetStrategy` (`internals/auto_offset_reset_strategy.rs`,
re-exported at `consumer::AutoOffsetResetStrategy`)

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AutoOffsetResetStrategy {
    strategy_type: StrategyType,
    duration: Option<Duration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StrategyType {
    Latest,
    Earliest,
    None_,
    ByDuration,
}

impl AutoOffsetResetStrategy {
    pub const EARLIEST: AutoOffsetResetStrategy = ...;
    pub const LATEST: AutoOffsetResetStrategy = ...;
    pub const NONE: AutoOffsetResetStrategy = ...;

    pub fn from_string(s: &str) -> Result<Self, KafkaError>;
    pub fn type_(&self) -> StrategyType;
    pub fn name(&self) -> String;
    pub fn timestamp(&self) -> Option<i64>;
    pub fn duration(&self) -> Option<Duration>;
}
```

`from_string` accepts: `"earliest"`, `"latest"`, `"none"`, and
`"by_duration:<ISO-8601-duration>"`. Java parses the duration via
`Duration.parse`; in Rust, use `iso8601-duration` crate **or** implement a
small parser locally (CLAUDE.md §1 — prefer std/crate; if a new dep is
needed, ask before adding). For Phase 1, prefer implementing a tiny ISO-8601
parser inline (only `PnDTnHnMnS` form needed for Kafka semantics) to avoid a
new dep.

Skip `Validator` inner class (it's `ConfigDef.Validator`, which we don't
have a Rust equivalent for and which `ConsumerConfig` doesn't need yet —
config validation in Rust happens via builder method returns).

### `GroupProtocol` (`group_protocol.rs`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupProtocol {
    Classic,
    Consumer,
}
```

`Display` impl produces lowercase (`"classic"` / `"consumer"`) matching Java's
`toString().toLowerCase(Locale.ROOT)` pattern. Provide a `from_string` /
`FromStr` impl that round-trips with `Display`.

### `ConsumerGroupMetadata` (`consumer_group_metadata.rs`)

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ConsumerGroupMetadata {
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
}
```

Constructors:

- `pub fn new(group_id) -> Self` — defaults match
  `JoinGroupRequest.UNKNOWN_GENERATION_ID` and `UNKNOWN_MEMBER_ID`. The
  constants live in the request module — for Phase 1 hardcode the values
  (`generation_id = -1`, `member_id = ""`) with comments pointing to the
  Java source, and add a TODO-free note that Phase 2+ will wire them to
  the generated `JoinGroupRequest` constants.
- `pub fn with_details(group_id, generation_id, member_id, group_instance_id) -> Self`

Getters and `Display`/`PartialEq`/`Hash` match Java. Document and avoid the
TODO/FIXME word (CLAUDE.md §5) — use a "NOTE:" prefix.

### `CloseOptions` (`close_options.rs`)

```rust
#[derive(Clone, Debug)]
pub struct CloseOptions {
    operation: GroupMembershipOperation,
    timeout: Option<Duration>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupMembershipOperation {
    LeaveGroup,
    RemainInGroup,
    Default,
}

impl Default for CloseOptions {
    fn default() -> Self { ... } // operation=Default, timeout=None
}
```

Fluent builders mirror Java: `pub fn with_timeout(self, timeout: Duration) -> Self`,
`pub fn with_group_membership_operation(self, op) -> Self`. Static
constructors `CloseOptions::timeout(d)`, `CloseOptions::group_membership_operation(op)`.

Default timeout constant `DEFAULT_CLOSE_TIMEOUT_MS` lives in
`ConsumerUtils.java` (Phase 6); for Phase 1, expose a `pub const`
`DEFAULT_CLOSE_TIMEOUT_MS: u64 = 30_000` here with a NOTE that Phase 6 will
move it to the canonical location and re-export.

### `SubscriptionPattern` (`subscription_pattern.rs`)

Java wraps a string regex pattern for Google RE2 (broker-side validation).
Rust: hold the pattern string + lazily compiled `regex::Regex`. Since `regex`
is already a workspace dep (check `Cargo.toml`; if not, ask before adding —
this is a popular Rust crate per CLAUDE.md §1.2).

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionPattern {
    pattern: String,
}

impl SubscriptionPattern {
    pub fn new(pattern: impl Into<String>) -> Result<Self, KafkaError>;
    pub fn pattern(&self) -> &str;
}
```

Validation of regex syntax happens broker-side per Java javadoc. Don't
pre-compile; just store the string. (If `regex` is needed elsewhere in the
phase — it shouldn't be — confirm dep first.)

### Consumer exception hierarchy (`errors.rs`)

Translate the six exception classes to Rust error types. Per CLAUDE.md
§10.3, we already have a unified `KafkaError` enum; the question is whether
each consumer exception becomes:

(a) a new variant on the existing top-level `KafkaError` enum, or
(b) a dedicated error struct in `consumer/errors.rs` that converts into
    `KafkaError` via `From`.

**Decision for this phase:** add them as a new sub-enum
`ConsumerError` in `src/consumer/errors.rs`, plus a `From<ConsumerError>
for KafkaError` impl. Keeps the top-level enum small while preserving the
classification (`is_retriable`, etc.). Variants:

- `CommitFailed { message: String, cause: Option<Box<KafkaError>> }`
- `RetriableCommitFailed { message: String, cause: Option<Box<KafkaError>> }`
- `InvalidOffset { message: String }` (abstract in Java; concrete subclasses live in this enum)
- `NoOffsetForPartition { partitions: Vec<TopicPartition> }`
- `OffsetOutOfRange { offsets: HashMap<TopicPartition, i64> }`
- `LogTruncation { offset_out_of_range_partitions: HashMap<TopicPartition, i64>, divergent_offsets: HashMap<TopicPartition, OffsetAndMetadata> }`

Each variant exposes accessors mirroring Java getter methods
(`offset_out_of_range_partitions()`, `divergent_offsets()`,
`partitions_with_no_offsets()`, etc.). `Display` / `Error` impls produce
messages identical to Java's so test assertions on error message content
(DoD §3 sub-bullet) work.

The Critic should specifically check the message-content assertions: Java
constructs the message via `String.format` patterns that our `Display` must
match byte-for-byte for any test that asserts text.

### `ConsumerConfig` (`consumer_config.rs`)

Mirror the `ProducerConfig` shape — Rust struct with named fields, getters
returning borrowed references, fluent setters for builder use. Do NOT
translate the Java `ConfigDef`-based reflection machinery; just the
fields, defaults, and the validation that `ConsumerConfig` does in its
constructor.

Field list to extract from `ConsumerConfig.java` (all `public static final
String *_CONFIG` definitions plus their defaults and validators). Order
fields in the Rust struct by logical grouping (connection, group,
fetching, deserialization, security, …) mirroring `producer_config.rs`.

The Critic should check:

- Every `*_CONFIG` key from Java appears as a struct field (or is explicitly
  noted as out-of-scope-for-Phase-1 with a comment pointing to its later
  phase, e.g. `partition.assignment.strategy` — silently accepted per
  `consumer-threading.md` §20).
- Every default value matches Java's `define(... DEFAULT_X, ...)`.
- Numeric types match the value-comparison rules in CLAUDE.md §2 (i64 for
  fields used in comparison; specifically *offsets*, *generation ids* — none
  of which appear here, but `session.timeout.ms` etc. should be `i32`/`i64`
  matching Java).
- Group-protocol-specific overrides: `PARTITION_ASSIGNMENT_STRATEGY_CONFIG`,
  `INTERCEPTOR_CLASSES_CONFIG` — accept silently per §20.

For Phase 1 we do **not** translate `ConsumerConfig.postProcessParsedConfig`
runtime cross-field validation; that lives wherever `AsyncKafkaConsumer`
constructs its config (Phase 11). We translate construction-time validation
only (e.g., `group.id` non-empty when group-aware methods are called is a
Phase-11 concern; for now just store the value).

Methods to provide:

- `pub fn new(bootstrap_servers: Vec<String>) -> Self` — minimal constructor
  with all other fields at defaults.
- `pub fn with_*` fluent setters for every field.
- `pub fn from_map(props: HashMap<String, String>) -> Result<Self, KafkaError>`
  — parses Java-style property keys to the typed struct (matches Java
  constructor `ConsumerConfig(Map<?, ?> props)`). This is the only path
  that needs explicit string-to-type conversion. Unknown keys → log warning
  (mirror `mod log` usage in `producer_config.rs`), do not error.
- Getters for every field.

Tests (`tests/consumer/consumer_config_test.rs`) must translate Java
`ConsumerConfigTest`'s 295 LOC. The Java test exercises:

- Default values for each config
- Validators on `auto.offset.reset` (delegates to `AutoOffsetResetStrategy`)
- Validators on `group.protocol`
- `share.acknowledgement.mode` — silently accepted per scope
- Other share-consumer keys — silently accepted per scope

Skip tests that exercise Java reflection (`ConfigDef`) directly — they
don't apply to the Rust struct model. Where skipped, note in the test
module preamble with a one-line justification (DoD §3).

## Module file (`src/consumer/mod.rs`)

```rust
//! Consumer types (org.apache.kafka.clients.consumer)

pub mod close_options;
pub mod consumer_config;
pub mod consumer_group_metadata;
pub mod consumer_record;
pub mod consumer_records;
pub mod errors;
pub mod group_protocol;
pub mod offset_and_metadata;
pub mod offset_and_timestamp;
pub mod offset_reset_strategy;
pub mod subscription_pattern;

pub(crate) mod internals;

pub use close_options::{CloseOptions, GroupMembershipOperation};
pub use consumer_config::ConsumerConfig;
pub use consumer_group_metadata::ConsumerGroupMetadata;
pub use consumer_record::{ConsumerRecord, NO_TIMESTAMP, NULL_SIZE};
pub use consumer_records::ConsumerRecords;
pub use errors::ConsumerError;
pub use group_protocol::GroupProtocol;
pub use internals::auto_offset_reset_strategy::{AutoOffsetResetStrategy, StrategyType};
pub use offset_and_metadata::OffsetAndMetadata;
pub use offset_and_timestamp::OffsetAndTimestamp;
pub use offset_reset_strategy::OffsetResetStrategy;
pub use subscription_pattern::SubscriptionPattern;
```

`src/consumer/internals/mod.rs`:

```rust
pub(crate) mod auto_offset_reset_strategy;
```

And add `pub mod consumer;` to `src/lib.rs` in the same alphabetical
position as `pub mod producer;`.

## Cross-cutting requirements

- **License header**: every new `.rs` file starts with the 14-line Apache 2.0
  header used by existing files (CLAUDE.md §7). Copy from
  `src/producer/mod.rs`.
- **Naming**: snake_case fn/var, PascalCase types, `*_CONFIG` constants
  become `pub const *_CONFIG: &str = ...`. `clients` MUST NOT appear in the
  module path (CLAUDE.md §2).
- **No TODO/FIXME** anywhere in this phase's output (CLAUDE.md §5). Use
  `NOTE:` prefix for future-phase pointers if needed.
- **Tests**: translate every named Java test method. Loops for
  `@RepeatedTest`. Assert error message content, not just `is_err()`
  (DoD §3 sub-bullets).
- **No new dependency** without explicit user approval (CLAUDE.md §1.2).
  If you find `regex` or any other crate is needed, stop and ask via the
  Actor's reporting channel before adding.

## Verification

Per phase DoD plus this checklist (Actor runs these before declaring
done; Critic verifies):

1. `cargo build` — clean
2. `cargo test` — all existing tests still pass; new tests pass
3. `cargo xtask format-check` — clean
4. `cargo xtask lint` — clean
5. `cargo doc --no-deps` builds (catches rustdoc link breakage)
6. The 7 Java test files listed in §"Tests" each have a corresponding Rust
   test file with **at least** the same number of `#[test]` functions as
   the Java has `@Test` methods. Any skip is annotated with a one-line
   rationale referencing CLAUDE.md / DoD.
7. No `Box<dyn Future>` per record introduced anywhere (DoD §10) — this
   phase has no futures, so the audit is a sanity check.
8. The Critic verifies `ConsumerRecord::topic` is `Arc<str>` and `Headers`
   is owned (`consumer-threading.md` §27).

## Commit plan

Suggested per-Actor commit granularity (one commit per logical bundle):

1. `Phase 1 (1/N): common/record/TimestampType + common/header/{Header, Headers}` (only if missing)
2. `Phase 1 (2/N): consumer module skeleton + value types (offset_and_*, group_protocol, subscription_pattern, close_options)`
3. `Phase 1 (3/N): ConsumerRecord + ConsumerRecords`
4. `Phase 1 (4/N): ConsumerGroupMetadata + ConsumerError hierarchy`
5. `Phase 1 (5/N): AutoOffsetResetStrategy + OffsetResetStrategy`
6. `Phase 1 (6/N): ConsumerConfig`
7. `Phase 1 (7/N): test translations (one commit per test file is fine)`

Each commit must individually pass `cargo build` (CLAUDE.md §"Workflow per
phase" / Actor role). Adjust the split if a logical unit ends up too small
or too large.
