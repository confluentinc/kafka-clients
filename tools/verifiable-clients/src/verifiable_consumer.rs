// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Translated from `org.apache.kafka.tools.VerifiableConsumer`.
//!
//! Command-line consumer designed for system testing. It outputs consumer
//! events to stdout as JSON-formatted objects. The `name` field in each JSON
//! event identifies the event type. The following events are supported:
//!
//! - `partitions_revoked`: partitions revoked through
//!   [`ConsumerRebalanceListener::on_partitions_revoked`].
//! - `partitions_assigned`: partitions assigned through
//!   [`ConsumerRebalanceListener::on_partitions_assigned`].
//! - `records_consumed`: a summary of records consumed in a single call to
//!   [`Consumer::poll`].
//! - `record_data`: the key, value, and offset of an individual consumed record
//!   (only included if verbose output is enabled).
//! - `offsets_committed`: the result of every offset commit (only included if
//!   auto-commit is not enabled).
//! - `startup_complete` / `shutdown_requested` / `shutdown_complete`: lifecycle.
//!
//! # The JSON stdout contract
//!
//! Every printed event mirrors Java's Jackson output exactly — event `name`,
//! field names, and field order — because that stdout contract is what a
//! downstream ducktape-style harness parses (Milestone 14, PLAN §1).
//!
//! # Group-protocol scope
//!
//! This client supports only the KIP-848 (`consumer`) group protocol
//! (`consumer-threading.md` §20). The classic-protocol arguments
//! (`--assignment-strategy`, `--session-timeout` for classic) are still parsed
//! so ducktape command lines are accepted, but selecting
//! `--group-protocol classic` fails at consumer construction with the factory's
//! `unsupported_version` error — the faithful, documented behavior.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde::Serialize;

use confluent_kafka::common::Error;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::serialization::StringDeserializer;
use confluent_kafka::consumer::{
    Consumer, ConsumerConfig, ConsumerHandle, ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords,
    GroupProtocol, OffsetAndMetadata, OffsetCommitCallback, new_consumer,
};

/// Wall-clock milliseconds since the Unix epoch. The Rust analog of Java's
/// `System.currentTimeMillis()`.
fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

/// Minimum interval between emitting `startup_complete` and `shutdown_complete`
/// (enforced in [`VerifiableConsumer::run`]).
///
/// This is a test-harness accommodation with no Java counterpart; it does not
/// alter client behavior. The ducktape `VerifiableConsumer` service treats a
/// node as started only between its `startup_complete` and `shutdown_complete`
/// events, and polls for that state. A consumer fenced on its first heartbeat —
/// for example a conflicting static member whose `group.instance.id` is already
/// in use (`UNRELEASED_INSTANCE_ID`, as in
/// `OffsetValidationTest.test_fencing_static_consumer`) — closes within a few
/// milliseconds of startup, too briefly for the poller to observe, and the
/// service's start gate then times out. The Java client leaves an observable
/// window (~250 ms) only incidentally, through its slower shutdown pipeline;
/// the native client completes the same work in ~3 ms. Deferring the final
/// `shutdown_complete` makes the window explicit and deterministic. The consumer
/// is already fenced and closed before this delay, so it introduces no group
/// activity. Three seconds comfortably exceeds the service's poll backoff and
/// stdout-streaming latency.
const MIN_STARTUP_VISIBLE: Duration = Duration::from_secs(3);

/// Java's `printJson`: serialize `data` and print one line, or print a
/// diagnostic if it cannot be serialized. A free function (Java makes it an
/// instance method only to reach the shared `ObjectMapper`, which is stateless
/// here). `println!` locks stdout for the whole line, so each event is written
/// atomically.
fn print_json<T: Serialize>(data: &T) {
    match serde_json::to_string(data) {
        Ok(json) => println!("{json}"),
        Err(e) => println!("Bad data can't be written as json: {e}"),
    }
}

// ----------------------------------------------------------------------------
// JSON events (Java's `ConsumerEvent` hierarchy).
//
// `@JsonPropertyOrder({"timestamp","name"})` on the Java base class ⇒ every
// event serializes `timestamp` then `name` first. Subsequent fields follow in
// `@JsonProperty` method declaration order, which each struct reproduces via
// field order. (Field order within an event is not load-bearing for the
// downstream harness — ducktape parses each line into a dict — but we match
// Java's declared order regardless.)
// ----------------------------------------------------------------------------

/// The JSON shape Java's custom `TopicPartition` serializer emits:
/// `{"topic":..,"partition":..}` (Java `addKafkaSerializerModule`, lines
/// 123-135). Used wherever a bare `TopicPartition` is serialized (the
/// `partitions` lists of the rebalance events).
#[derive(Serialize)]
struct PartitionJson {
    topic: String,
    partition: i32,
}

impl PartitionJson {
    fn new(tp: &TopicPartition) -> Self {
        Self { topic: tp.topic().to_string(), partition: tp.partition() }
    }
}

/// `startup_complete` (Java `StartupComplete`).
#[derive(Serialize)]
struct StartupComplete {
    timestamp: i64,
    name: &'static str,
}

impl StartupComplete {
    fn new() -> Self {
        Self { timestamp: now_millis(), name: "startup_complete" }
    }
}

/// `shutdown_requested` (Java `ShutdownRequested`).
#[derive(Serialize)]
struct ShutdownRequested {
    timestamp: i64,
    name: &'static str,
}

impl ShutdownRequested {
    fn new() -> Self {
        Self { timestamp: now_millis(), name: "shutdown_requested" }
    }
}

/// `shutdown_complete` (Java `ShutdownComplete`).
#[derive(Serialize)]
struct ShutdownComplete {
    timestamp: i64,
    name: &'static str,
}

impl ShutdownComplete {
    fn new() -> Self {
        Self { timestamp: now_millis(), name: "shutdown_complete" }
    }
}

/// `partitions_revoked` (Java `PartitionsRevoked`).
#[derive(Serialize)]
struct PartitionsRevoked {
    timestamp: i64,
    name: &'static str,
    partitions: Vec<PartitionJson>,
}

impl PartitionsRevoked {
    fn new(partitions: &[TopicPartition]) -> Self {
        Self {
            timestamp: now_millis(),
            name: "partitions_revoked",
            partitions: partitions.iter().map(PartitionJson::new).collect(),
        }
    }
}

/// `partitions_assigned` (Java `PartitionsAssigned`).
#[derive(Serialize)]
struct PartitionsAssigned {
    timestamp: i64,
    name: &'static str,
    partitions: Vec<PartitionJson>,
}

impl PartitionsAssigned {
    fn new(partitions: &[TopicPartition]) -> Self {
        Self {
            timestamp: now_millis(),
            name: "partitions_assigned",
            partitions: partitions.iter().map(PartitionJson::new).collect(),
        }
    }
}

/// A per-partition record-set summary (Java `RecordSetSummary`, which extends
/// `PartitionData`). Java's getter-derived property names are camelCase
/// (`minOffset`/`maxOffset`), preserved with `#[serde(rename)]`.
#[derive(Serialize)]
struct RecordSetSummary {
    topic: String,
    partition: i32,
    count: i64,
    #[serde(rename = "minOffset")]
    min_offset: i64,
    #[serde(rename = "maxOffset")]
    max_offset: i64,
}

impl RecordSetSummary {
    fn new(topic: String, partition: i32, count: i64, min_offset: i64, max_offset: i64) -> Self {
        Self { topic, partition, count, min_offset, max_offset }
    }
}

/// `records_consumed` (Java `RecordsConsumed`).
#[derive(Serialize)]
struct RecordsConsumed {
    timestamp: i64,
    name: &'static str,
    count: i64,
    partitions: Vec<RecordSetSummary>,
}

impl RecordsConsumed {
    fn new(count: i64, partition_summaries: Vec<RecordSetSummary>) -> Self {
        Self {
            timestamp: now_millis(),
            name: "records_consumed",
            count,
            partitions: partition_summaries,
        }
    }
}

/// `record_data` (Java `RecordData`), emitted per record only when verbose.
/// `@JsonPropertyOrder({"timestamp","name","key","value","topic","partition","offset"})`.
#[derive(Serialize)]
struct RecordData {
    timestamp: i64,
    name: &'static str,
    // Java `record.key()` / `record.value()` are possibly-null Strings and are
    // serialized with no `NON_NULL` filter, so a null renders as `null`.
    key: Option<String>,
    value: Option<String>,
    topic: String,
    partition: i32,
    offset: i64,
}

impl RecordData {
    fn from_record(record: &ConsumerRecord<String, String>) -> Self {
        Self {
            timestamp: now_millis(),
            name: "record_data",
            key: record.key().cloned(),
            value: record.value().cloned(),
            topic: record.topic().to_string(),
            partition: record.partition(),
            offset: record.offset(),
        }
    }
}

/// A single committed offset entry (Java `CommitData`, which extends
/// `PartitionData`).
#[derive(Serialize)]
struct CommitData {
    topic: String,
    partition: i32,
    offset: i64,
}

/// `offsets_committed` (Java `OffsetsCommitted`).
#[derive(Serialize)]
struct OffsetsCommitted {
    timestamp: i64,
    name: &'static str,
    offsets: Vec<CommitData>,
    // Java `error()` carries `@JsonInclude(NON_NULL)`, so a null error is
    // omitted entirely (a successful commit prints no `error` field).
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    success: bool,
}

impl OffsetsCommitted {
    /// Build the event from a commit result, mirroring Java's `onComplete`
    /// (lines 181-194): list the committed offsets, and set `success`/`error`
    /// from whether an error occurred.
    fn from_commit(offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) -> Self {
        let committed: Vec<CommitData> = offsets
            .iter()
            .map(|(tp, om)| CommitData {
                topic: tp.topic().to_string(),
                partition: tp.partition(),
                offset: om.offset(),
            })
            .collect();
        // Java: `error = exception.getMessage()`, which may be null and is then
        // omitted by `@JsonInclude(NON_NULL)`. `Error::message()` is the
        // faithful translation of `getMessage()` (bare text); its "no message"
        // is the empty string, which we map to `None` so the field is omitted
        // exactly as Java omits a null message.
        let (success, error) = match error {
            None => (true, None),
            Some(e) => {
                let message = e.message();
                (
                    false,
                    if message.is_empty() {
                        None
                    } else {
                        Some(message.to_string())
                    },
                )
            },
        };
        Self {
            timestamp: now_millis(),
            name: "offsets_committed",
            offsets: committed,
            error,
            success,
        }
    }
}

/// The shared JSON-event sink that implements the consumer's
/// [`ConsumerRebalanceListener`] and [`OffsetCommitCallback`] traits.
///
/// # Why a separate type (Rust-necessitated split, not in Java)
///
/// Java's `VerifiableConsumer implements OffsetCommitCallback,
/// ConsumerRebalanceListener` — one object is both the poll/commit driver *and*
/// the listener/callback, because a Java object reference is freely shareable.
/// In Rust the driver ([`VerifiableConsumer`]) needs `&mut self` to call
/// `poll`/`commit`/`close`, while the client's
/// [`Consumer::subscribe_with_listener`] and
/// [`Consumer::commit_async_offsets_with_callback`] take
/// `Arc<dyn ConsumerRebalanceListener>` / `Arc<dyn OffsetCommitCallback>` shared
/// across the caller task (`consumer-threading.md` §31). A single `&mut` owner
/// cannot also be an `Arc<dyn …>`, so the shareable half is split out here.
///
/// The split is clean because the shared half is exactly the part that only
/// prints JSON: the callbacks carry no consumer state. `on_complete` builds an
/// `offsets_committed` event; the rebalance callbacks build
/// `partitions_assigned`/`partitions_revoked`. All are stateless, so
/// `EventReporter` is a unit struct.
#[derive(Debug, Default)]
pub struct EventReporter;

impl EventReporter {
    /// Print the `shutdown_requested` event (Java `close()`, line 262). Called
    /// from the ctrl-c task, which holds a shared clone of the reporter.
    pub fn print_shutdown_requested(&self) {
        print_json(&ShutdownRequested::new());
    }
}

#[async_trait]
impl OffsetCommitCallback for EventReporter {
    /// Java `onComplete` (lines 180-195): print `offsets_committed` with
    /// success/error derived from `exception`.
    async fn on_complete(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) {
        print_json(&OffsetsCommitted::from_commit(offsets, error));
    }
}

#[async_trait]
impl ConsumerRebalanceListener for EventReporter {
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java `onPartitionsRevoked` (lines 202-205).
        print_json(&PartitionsRevoked::new(partitions));
        Ok(())
    }

    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) -> Result<(), Error> {
        // Java `onPartitionsAssigned` (lines 197-200).
        print_json(&PartitionsAssigned::new(partitions));
        Ok(())
    }
    // Java's VerifiableConsumer does NOT override onPartitionsLost, so it
    // inherits the interface default that delegates to onPartitionsRevoked. The
    // Rust trait's default `on_partitions_lost` also delegates to
    // `on_partitions_revoked`, so we leave it — a `partitions_revoked` event is
    // printed on partition loss, matching Java.
}

/// The `VerifiableConsumer`.
///
/// Holds the owned consumer (driven with `&mut self`) plus the shared
/// [`EventReporter`] (the listener/commit-callback half — see
/// [`EventReporter`]). `Box<dyn Consumer<String, String>>` is used directly
/// rather than a type parameter (unlike the generic `VerifiableProducer`)
/// because the client's [`Consumer`] trait is `#[async_trait]` and therefore
/// `dyn`-compatible; tests substitute a boxed `MockConsumer`.
pub struct VerifiableConsumer {
    consumer: Box<dyn Consumer<String, String>>,
    // Java takes a `PrintStream out`; we always print to stdout via `print_json`
    // (Java always passes `System.out`).
    topic: String,
    use_auto_commit: bool,
    use_async_commit: bool,
    verbose: bool,
    max_messages: i32,
    consumed_messages: i32,
    reporter: Arc<EventReporter>,
}

impl VerifiableConsumer {
    /// Construct a `VerifiableConsumer`. Mirrors Java's constructor (minus the
    /// `PrintStream out`, which is always stdout here).
    pub fn new(
        consumer: Box<dyn Consumer<String, String>>,
        topic: String,
        max_messages: i32,
        use_auto_commit: bool,
        use_async_commit: bool,
        verbose: bool,
    ) -> Self {
        Self {
            consumer,
            topic,
            use_auto_commit,
            use_async_commit,
            verbose,
            max_messages,
            consumed_messages: 0,
            reporter: Arc::new(EventReporter),
        }
    }

    /// Java `hasMessageLimit()` (lines 137-139).
    fn has_message_limit(&self) -> bool {
        self.max_messages >= 0
    }

    /// Java `isFinished()` (lines 141-143).
    fn is_finished(&self) -> bool {
        self.has_message_limit() && self.consumed_messages >= self.max_messages
    }

    /// Java `onRecordsReceived` (lines 145-178): build the per-partition
    /// offsets to commit (`maxOffset + 1`) and the `records_consumed` summary,
    /// truncating at `maxMessages`, printing `record_data` per record when
    /// verbose, and stopping early once finished.
    ///
    /// Returns `Result` because Java's `new OffsetAndMetadata(offset)` throws on
    /// a negative offset (never reachable here, since `maxOffset + 1 >= 1`), and
    /// a thrown error in Java would propagate to `run`'s catch — mirrored by
    /// returning it here.
    fn on_records_received(
        &mut self,
        records: &ConsumerRecords<String, String>,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, Error> {
        let (offsets, records_consumed) = self.collect_records_consumed(records)?;
        // Java `onRecordsReceived` line 176: print the `records_consumed`
        // summary after building the offsets.
        print_json(&records_consumed);
        Ok(offsets)
    }

    /// The record-collection core of Java `onRecordsReceived` (lines 145-176),
    /// separated from the final `records_consumed` print so a unit test can pin
    /// the emitted event's fields without capturing process stdout. It returns
    /// the offsets to commit together with the exact [`RecordsConsumed`] event
    /// [`on_records_received`](Self::on_records_received) prints — whose `count`
    /// is the FULL `records.count()` (Java line 176), never the truncated
    /// per-partition size. Verbose `record_data` printing stays here so the
    /// caller's observable behavior is unchanged.
    #[allow(clippy::type_complexity)]
    fn collect_records_consumed(
        &mut self,
        records: &ConsumerRecords<String, String>,
    ) -> Result<(HashMap<TopicPartition, OffsetAndMetadata>, RecordsConsumed), Error> {
        let mut offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        let mut summaries: Vec<RecordSetSummary> = Vec::new();

        // Collect partitions up front so the immutable borrow of `records` for
        // `partitions()` does not overlap the later `records_for_partition`.
        let partitions: Vec<TopicPartition> = records.partitions().cloned().collect();
        for tp in &partitions {
            let all = records.records_for_partition(tp);

            // Java `subList(0, maxMessages - consumedMessages)` truncation.
            let partition_records: &[ConsumerRecord<String, String>] = if self.has_message_limit()
                && self.consumed_messages as usize + all.len() > self.max_messages as usize
            {
                &all[..(self.max_messages - self.consumed_messages) as usize]
            } else {
                all
            };

            if partition_records.is_empty() {
                continue;
            }

            let min_offset = partition_records[0].offset();
            let max_offset = partition_records[partition_records.len() - 1].offset();

            offsets.insert(tp.clone(), OffsetAndMetadata::new(max_offset + 1)?);
            summaries.push(RecordSetSummary::new(
                tp.topic().to_string(),
                tp.partition(),
                partition_records.len() as i64,
                min_offset,
                max_offset,
            ));

            if self.verbose {
                for record in partition_records {
                    print_json(&RecordData::from_record(record));
                }
            }

            self.consumed_messages += partition_records.len() as i32;
            if self.is_finished() {
                break;
            }
        }

        // Java line 176: the `records_consumed` count is the FULL poll count
        // (`records.count()`), NOT the possibly-truncated number added to
        // `consumed_messages` above.
        Ok((offsets, RecordsConsumed::new(records.count() as i64, summaries)))
    }

    /// Java `commitSync` (lines 215-228): commit synchronously and report the
    /// result via `onComplete`. On a wakeup, Java catches `WakeupException`,
    /// recurses to retry the commit, then rethrows the wakeup so `run`'s loop
    /// unwinds; a `FencedInstanceIdException` is rethrown; any other error is
    /// reported through `onComplete(offsets, error)`.
    ///
    /// The recursion is translated literally with a boxed future (async
    /// recursion needs an explicit indirection in Rust). Java's comment notes
    /// "we only call wakeup() once to close the consumer, so this recursion
    /// should be safe."
    async fn commit_sync(&mut self, offsets: HashMap<TopicPartition, OffsetAndMetadata>) -> Result<(), Error> {
        match self.consumer.commit_sync_offsets(offsets.clone()).await {
            Ok(()) => {
                self.reporter.on_complete(&offsets, None).await;
                Ok(())
            },
            Err(e) if matches!(e, Error::Wakeup(_)) => {
                // Retry the commit (Java's recursive `commitSync(offsets)`),
                // then propagate the wakeup (Java's `throw e`).
                Box::pin(self.commit_sync(offsets)).await?;
                Err(e)
            },
            Err(e) if matches!(e, Error::FencedInstanceId(_)) => Err(e),
            Err(e) => {
                self.reporter.on_complete(&offsets, Some(&e)).await;
                Ok(())
            },
        }
    }

    /// Java `run` (lines 230-257): subscribe, poll until finished, commit each
    /// batch (unless auto-commit is enabled), and on shutdown close the consumer
    /// and print `shutdown_complete`.
    ///
    /// Java's `WakeupException` (raised by `wakeup()` from the shutdown path) is
    /// caught and ignored; any other error is logged to stderr (not stdout, so
    /// the JSON contract on stdout stays clean); the `finally` block always
    /// closes and prints `shutdown_complete`. Java's `shutdownLatch` is not
    /// needed in Rust: `#[tokio::main]` awaits `run` to completion, so the
    /// process cannot exit before the close finishes.
    pub async fn run(&mut self) {
        // Marks (approximately) the instant `run_loop` emits `startup_complete`,
        // its first statement. Used to enforce `MIN_STARTUP_VISIBLE` below.
        let started_at = std::time::Instant::now();

        if let Err(e) = self.run_loop().await {
            if matches!(e, Error::Wakeup(_)) {
                // ignore, we are closing (Java catch WakeupException).
            } else {
                // Log the error to stderr so it goes to the service log and not
                // stdout (Java `log.error(...)`).
                eprintln!("Error during processing, terminating consumer process: {e}");
            }
        }

        // finally: close the consumer and announce shutdown.
        if let Err(e) = self.consumer.close().await {
            eprintln!("Error closing consumer: {e}");
        }

        // Defer the final `shutdown_complete` so the interval since
        // `startup_complete` is at least `MIN_STARTUP_VISIBLE`, ensuring the
        // ducktape service can observe the started state. Only runs that
        // terminate within that interval — such as a consumer fenced on its
        // first heartbeat — are affected; a normal consumer runs for seconds and
        // is unaffected. The consumer is already closed at this point, so the
        // delay introduces no group activity.
        let elapsed = started_at.elapsed();
        if elapsed < MIN_STARTUP_VISIBLE {
            tokio::time::sleep(MIN_STARTUP_VISIBLE - elapsed).await;
        }

        print_json(&ShutdownComplete::new());
    }

    /// The body of Java `run`'s `try` block, separated so `run` can translate
    /// the `catch`/`finally` uniformly regardless of which step failed.
    async fn run_loop(&mut self) -> Result<(), Error> {
        print_json(&StartupComplete::new());
        self.consumer
            .subscribe_with_listener(
                vec![self.topic.clone()],
                Arc::clone(&self.reporter) as Arc<dyn ConsumerRebalanceListener>,
            )
            .await?;

        while !self.is_finished() {
            // Java `poll(Duration.ofMillis(Long.MAX_VALUE))` — an effectively
            // unbounded poll, interrupted by `wakeup()` on shutdown.
            let records = self.consumer.poll(Duration::from_millis(i64::MAX as u64)).await?;
            let offsets = self.on_records_received(&records)?;

            if !self.use_auto_commit {
                if self.use_async_commit {
                    self.consumer
                        .commit_async_offsets_with_callback(
                            offsets,
                            Arc::clone(&self.reporter) as Arc<dyn OffsetCommitCallback>,
                        )
                        .await?;
                } else {
                    self.commit_sync(offsets).await?;
                }
            }
        }
        Ok(())
    }

    /// A cross-task [`ConsumerHandle`] whose `wakeup()` interrupts the blocking
    /// `poll`, translating Java `close()`'s `consumer.wakeup()` call (line 263)
    /// issued from the shutdown-hook thread.
    pub fn handle(&self) -> ConsumerHandle {
        self.consumer.handle()
    }

    /// A shared clone of the [`EventReporter`], so the ctrl-c task can print
    /// `shutdown_requested` (Java `close()`, line 262).
    pub fn reporter(&self) -> Arc<EventReporter> {
        Arc::clone(&self.reporter)
    }
}

/// The parsed command-line arguments, separated from consumer construction so
/// the parsing/validation logic is unit-testable without a live broker.
#[derive(Debug, PartialEq)]
struct ParsedArgs {
    bootstrap_server: String,
    topic: String,
    group_protocol: String,
    // Java default `DEFAULT_GROUP_REMOTE_ASSIGNOR` is `null` → `None`.
    group_remote_assignor: Option<String>,
    group_id: String,
    group_instance_id: Option<String>,
    max_messages: i32,
    session_timeout: Option<i32>,
    verbose: bool,
    use_auto_commit: bool,
    reset_policy: String,
    assignment_strategy: String,
    config_file: Option<String>,
    command_config_file: Option<String>,
}

/// Java class name of `RangeAssignor`, the `--assignment-strategy` default. Java
/// uses `RangeAssignor.class.getName()`; the assignor types themselves are not
/// translated (classic-protocol only, `consumer-threading.md` §20), so the
/// default is reproduced as a literal string.
const DEFAULT_ASSIGNMENT_STRATEGY: &str = "org.apache.kafka.clients.consumer.RangeAssignor";

/// An example assignor class name used only in the `--assignment-strategy` help
/// text (Java uses `RoundRobinAssignor.class.getName()`).
const EXAMPLE_ASSIGNMENT_STRATEGY: &str = "org.apache.kafka.clients.consumer.RoundRobinAssignor";

/// Build a command-line argument error. Java raises
/// `ArgumentParserException`; the word "exception" must not appear in Rust
/// identifiers (CLAUDE.md §2, §10), so this maps to a recoverable
/// `LocalIllegalArgument` error carrying the same message.
fn arg_error(message: impl Into<String>) -> Error {
    Error::local_illegal_argument(message)
}

fn next_value<'a>(args: &mut impl Iterator<Item = &'a String>, flag: &str) -> Result<String, Error> {
    args.next()
        .cloned()
        .ok_or_else(|| arg_error(format!("argument {flag}: expected one argument")))
}

fn parse_int(value: &str, flag: &str) -> Result<i32, Error> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|_| arg_error(format!("argument {flag}: could not convert '{value}' to integer")))
}

/// Reproduces Java's `argParser()` + the validation `createFromArgs` performs
/// before touching the network (required args, the `--consumer.config` /
/// `--command-config` mutual exclusion).
fn parse_args(args: &[String]) -> Result<ParsedArgs, Error> {
    let mut bootstrap_server: Option<String> = None;
    let mut topic: Option<String> = None;
    let mut group_protocol: String = ConsumerConfig::DEFAULT_GROUP_PROTOCOL.to_string();
    let mut group_remote_assignor: Option<String> = None;
    let mut group_id: Option<String> = None;
    let mut group_instance_id: Option<String> = None;
    let mut max_messages: i32 = -1;
    let mut session_timeout: Option<i32> = None;
    let mut verbose = false;
    let mut use_auto_commit = false;
    let mut reset_policy: String = "earliest".to_string();
    let mut assignment_strategy: String = DEFAULT_ASSIGNMENT_STRATEGY.to_string();
    let mut config_file: Option<String> = None;
    let mut command_config_file: Option<String> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--bootstrap-server" => bootstrap_server = Some(next_value(&mut it, "--bootstrap-server")?),
            "--topic" => topic = Some(next_value(&mut it, "--topic")?),
            "--group-protocol" => group_protocol = next_value(&mut it, "--group-protocol")?,
            "--group-remote-assignor" => group_remote_assignor = Some(next_value(&mut it, "--group-remote-assignor")?),
            "--group-id" => group_id = Some(next_value(&mut it, "--group-id")?),
            "--group-instance-id" => group_instance_id = Some(next_value(&mut it, "--group-instance-id")?),
            "--max-messages" => max_messages = parse_int(&next_value(&mut it, "--max-messages")?, "--max-messages")?,
            "--session-timeout" => {
                session_timeout = Some(parse_int(&next_value(&mut it, "--session-timeout")?, "--session-timeout")?)
            },
            "--verbose" => verbose = true,
            "--enable-autocommit" => use_auto_commit = true,
            "--reset-policy" => reset_policy = next_value(&mut it, "--reset-policy")?,
            "--assignment-strategy" => assignment_strategy = next_value(&mut it, "--assignment-strategy")?,
            "--consumer.config" => config_file = Some(next_value(&mut it, "--consumer.config")?),
            "--command-config" => command_config_file = Some(next_value(&mut it, "--command-config")?),
            other => return Err(arg_error(format!("unrecognized arguments: '{other}'"))),
        }
    }

    // The Java "Connection Group" mutually-exclusive group is `required(true)`
    // and contains only `--bootstrap-server`, so it is effectively required.
    let bootstrap_server =
        bootstrap_server.ok_or_else(|| arg_error("one of the arguments --bootstrap-server is required"))?;
    let topic = topic.ok_or_else(|| arg_error("argument --topic is required"))?;
    let group_id = group_id.ok_or_else(|| arg_error("argument --group-id is required"))?;

    // Java createFromArgs 649-651.
    if config_file.is_some() && command_config_file.is_some() {
        return Err(arg_error(
            "Options --consumer.config and --command-config are mutually exclusive.",
        ));
    }

    Ok(ParsedArgs {
        bootstrap_server,
        topic,
        group_protocol,
        group_remote_assignor,
        group_id,
        group_instance_id,
        max_messages,
        session_timeout,
        verbose,
        use_auto_commit,
        reset_policy,
        assignment_strategy,
        config_file,
        command_config_file,
    })
}

/// Read a Java-properties file into ordered key/value pairs.
///
/// Java's `Utils.loadProps` delegates to `java.util.Properties`. This is a
/// minimal translation covering the forms system-test config files use:
/// `key=value` / `key:value` / `key value`, `#` and `!` comment lines, and
/// blank lines, with surrounding whitespace trimmed. It intentionally omits
/// `java.util.Properties`' line continuations and `\uXXXX` escapes — no
/// system-test config relies on them.
fn load_props(filename: &str) -> Result<Vec<(String, String)>, Error> {
    // Java's IOException is caught in createFromArgs and rethrown as an
    // ArgumentParserException carrying the message; we mirror that mapping.
    let contents = std::fs::read_to_string(filename).map_err(|e| arg_error(e.to_string()))?;
    Ok(parse_properties(&contents))
}

fn parse_properties(contents: &str) -> Vec<(String, String)> {
    let mut props = Vec::new();
    for line in contents.lines() {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('!') {
            continue;
        }
        let key_end = trimmed
            .char_indices()
            .find(|(_, c)| *c == '=' || *c == ':' || c.is_whitespace())
            .map(|(i, _)| i)
            .unwrap_or(trimmed.len());
        let key = &trimmed[..key_end];
        let rest = trimmed[key_end..].trim_start();
        let value = rest
            .strip_prefix('=')
            .or_else(|| rest.strip_prefix(':'))
            .map(str::trim)
            .unwrap_or_else(|| rest.trim());
        props.push((key.to_string(), value.to_string()));
    }
    props
}

/// Construct a `VerifiableConsumer` from command-line arguments. Mirrors Java's
/// static `createFromArgs` (lines 640-718).
///
/// # Errors
///
/// Returns `Err` for any argument-parsing failure, an unreadable config file, an
/// invalid group protocol, an invalid consumer configuration, or a consumer that
/// cannot be constructed. In particular, `--group-protocol classic` fails at
/// [`new_consumer`] with an `unsupported_version` error
/// (`consumer-threading.md` §20).
pub fn create_from_args(args: &[String]) -> Result<VerifiableConsumer, Error> {
    let parsed = parse_args(args)?;

    // Build the consumer property map (Java createFromArgs 648-704). Config
    // files are applied first, then explicit args override them.
    let mut props: HashMap<String, String> = HashMap::new();

    if let Some(config_file) = &parsed.config_file {
        println!(
            "Option --consumer.config has been deprecated and will be removed in a future version. Use --command-config instead."
        );
        for (key, value) in load_props(config_file)? {
            props.insert(key, value);
        }
    }
    if let Some(command_config_file) = &parsed.command_config_file {
        for (key, value) in load_props(command_config_file)? {
            props.insert(key, value);
        }
    }

    // Java: `GroupProtocol.of(res.getString("groupProtocol"))` validates the
    // value, then stores its canonical (uppercase) `name()`.
    let group_protocol = GroupProtocol::of(&parsed.group_protocol)?;
    props.insert(
        ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(),
        group_protocol.name().to_string(),
    );

    // The two group protocols use different assignor configuration (Java
    // createFromArgs 676-684).
    if group_protocol == GroupProtocol::Consumer {
        if let Some(assignor) = &parsed.group_remote_assignor {
            props.insert(ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG.to_string(), assignor.clone());
        }
    } else {
        props.insert(
            ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG.to_string(),
            parsed.assignment_strategy.clone(),
        );
    }

    if let Some(session_timeout) = parsed.session_timeout {
        props.insert(
            ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG.to_string(),
            session_timeout.to_string(),
        );
    }

    props.insert(ConsumerConfig::GROUP_ID_CONFIG.to_string(), parsed.group_id.clone());

    if let Some(group_instance_id) = &parsed.group_instance_id {
        props.insert(ConsumerConfig::GROUP_INSTANCE_ID_CONFIG.to_string(), group_instance_id.clone());
    }

    props.insert(
        ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        parsed.bootstrap_server.clone(),
    );
    props.insert(
        ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG.to_string(),
        parsed.use_auto_commit.to_string(),
    );
    props.insert(
        ConsumerConfig::AUTO_OFFSET_RESET_CONFIG.to_string(),
        parsed.reset_policy.clone(),
    );

    let config = ConsumerConfig::from_properties(&props)?;
    let consumer = new_consumer::<String, String>(config, Box::new(StringDeserializer), Box::new(StringDeserializer))?;

    // Java always constructs with `useAsyncCommit = false` (createFromArgs line
    // 716), so async commit is never taken even though the field exists.
    Ok(VerifiableConsumer::new(
        consumer,
        parsed.topic,
        parsed.max_messages,
        parsed.use_auto_commit,
        false,
        parsed.verbose,
    ))
}

/// The `--help` text, listing every argument Java's `argParser()` declares.
pub fn help_text() -> String {
    format!(
        "usage: verifiable-consumer [-h] --bootstrap-server \
HOST1:PORT1[,HOST2:PORT2[...]] --topic TOPIC --group-id GROUP-ID\n\
                           [--group-protocol GROUP-PROTOCOL] [--group-remote-assignor GROUP-REMOTE-ASSIGNOR]\n\
                           [--group-instance-id GROUP-INSTANCE-ID] [--max-messages MAX-MESSAGES]\n\
                           [--session-timeout TIMEOUT-MS] [--verbose] [--enable-autocommit]\n\
                           [--reset-policy RESET-POLICY] [--assignment-strategy ASSIGNMENT-STRATEGY]\n\
                           [--consumer.config CONFIG-FILE] [--command-config CONFIG-FILE]\n\n\
This tool consumes messages from a specific topic and emits consumer events\n\
(e.g. group rebalances, received messages, and offsets committed) as JSON\n\
objects to STDOUT.\n\n\
NOTE: this client supports only the KIP-848 'consumer' group protocol; running\n\
with --group-protocol classic fails at startup with an unsupported-version error.\n\n\
required arguments:\n\
  --bootstrap-server HOST1:PORT1[,...]  The server(s) to connect to.\n\
  --topic TOPIC                 Consumes messages from this topic.\n\
  --group-id GROUP-ID           The group id of the consumer group.\n\n\
optional arguments:\n\
  --group-protocol GROUP-PROTOCOL    Group protocol (one of CLASSIC, CONSUMER). (default: {default_protocol})\n\
  --group-remote-assignor GROUP-REMOTE-ASSIGNOR  Group remote assignor; only used if the group protocol is CONSUMER.\n\
  --group-instance-id GROUP-INSTANCE-ID  A unique identifier of the consumer instance.\n\
  --max-messages MAX-MESSAGES   Consume this many messages. If -1, consume until killed. (default: -1)\n\
  --session-timeout TIMEOUT-MS  The consumer's session timeout; not supported when group protocol is CONSUMER.\n\
  --verbose                     Enable to log individual consumed records.\n\
  --enable-autocommit           Enable offset auto-commit on consumer.\n\
  --reset-policy RESET-POLICY   Set reset policy (earliest, latest, or none). (default: earliest)\n\
  --assignment-strategy ASSIGNMENT-STRATEGY  Set assignment strategy (e.g. {example_strategy}); only used if the group protocol is CLASSIC. (default: {default_strategy})\n\
  --consumer.config CONFIG-FILE (DEPRECATED) Consumer config properties file. Use --command-config instead.\n\
  --command-config CONFIG-FILE  Config properties file (mutually exclusive with --consumer.config).",
        default_protocol = ConsumerConfig::DEFAULT_GROUP_PROTOCOL,
        example_strategy = EXAMPLE_ASSIGNMENT_STRATEGY,
        default_strategy = DEFAULT_ASSIGNMENT_STRATEGY,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use confluent_kafka::consumer::{AutoOffsetResetStrategy, MockConsumer};
    use indexmap::IndexMap;

    // ---- construction helpers ----------------------------------------------

    fn mock_consumer() -> Box<dyn Consumer<String, String>> {
        Box::new(MockConsumer::<String, String>::new(AutoOffsetResetStrategy::EARLIEST))
    }

    fn verifiable_with(max_messages: i32, verbose: bool) -> VerifiableConsumer {
        VerifiableConsumer::new(mock_consumer(), "test-topic".to_string(), max_messages, false, false, verbose)
    }

    fn record(topic: &str, partition: i32, offset: i64) -> ConsumerRecord<String, String> {
        ConsumerRecord::new(topic.to_string(), partition, offset, None, Some(format!("v{offset}")))
    }

    fn records_of(
        entries: Vec<(TopicPartition, Vec<ConsumerRecord<String, String>>)>,
    ) -> ConsumerRecords<String, String> {
        let mut map: IndexMap<TopicPartition, Vec<ConsumerRecord<String, String>>> = IndexMap::new();
        for (tp, recs) in entries {
            map.insert(tp, recs);
        }
        ConsumerRecords::new(map, HashMap::new())
    }

    // ---- has_message_limit / is_finished -----------------------------------

    #[test]
    fn has_message_limit_false_for_negative_max() {
        let consumer = verifiable_with(-1, false);
        assert!(!consumer.has_message_limit());
        assert!(!consumer.is_finished());
    }

    #[test]
    fn has_message_limit_true_for_zero_or_positive_max() {
        assert!(verifiable_with(0, false).has_message_limit());
        assert!(verifiable_with(5, false).has_message_limit());
        // max=0 with 0 consumed is already finished.
        assert!(verifiable_with(0, false).is_finished());
    }

    // ---- on_records_received offset math -----------------------------------

    #[test]
    fn on_records_received_committed_offset_is_max_plus_one() {
        let mut consumer = verifiable_with(-1, false);
        let tp = TopicPartition::new("t", 0);
        let recs = vec![record("t", 0, 10), record("t", 0, 11), record("t", 0, 12)];
        let records = records_of(vec![(tp.clone(), recs)]);

        let offsets = consumer.on_records_received(&records).unwrap();
        // maxOffset (12) + 1.
        assert_eq!(offsets.get(&tp).unwrap().offset(), 13);
        assert_eq!(consumer.consumed_messages, 3);
    }

    #[test]
    fn on_records_received_truncates_at_max_messages() {
        // max=2, a batch of 5 => only the first 2 are consumed and committed.
        let mut consumer = verifiable_with(2, false);
        let tp = TopicPartition::new("t", 0);
        let recs = vec![
            record("t", 0, 100),
            record("t", 0, 101),
            record("t", 0, 102),
            record("t", 0, 103),
            record("t", 0, 104),
        ];
        let records = records_of(vec![(tp.clone(), recs)]);

        let offsets = consumer.on_records_received(&records).unwrap();
        // Truncated to offsets [100, 101] => committed maxOffset (101) + 1.
        assert_eq!(offsets.get(&tp).unwrap().offset(), 102);
        assert_eq!(consumer.consumed_messages, 2);
        assert!(consumer.is_finished());
    }

    #[test]
    fn on_records_received_stops_early_across_partitions_when_finished() {
        // max=1: the first partition's single record finishes the consumer, so
        // the second partition is never summarized.
        let mut consumer = verifiable_with(1, false);
        let tp0 = TopicPartition::new("t", 0);
        let tp1 = TopicPartition::new("t", 1);
        let records = records_of(vec![
            (tp0.clone(), vec![record("t", 0, 0)]),
            (tp1.clone(), vec![record("t", 1, 0)]),
        ]);

        let offsets = consumer.on_records_received(&records).unwrap();
        assert!(offsets.contains_key(&tp0));
        assert!(!offsets.contains_key(&tp1));
        assert_eq!(consumer.consumed_messages, 1);
        assert!(consumer.is_finished());
    }

    #[test]
    fn records_consumed_count_is_full_poll_count_when_truncated() {
        // Java `onRecordsReceived` line 176: the `records_consumed` event's
        // `count` is the FULL poll count (`records.count()`), even when the
        // batch is truncated by `maxMessages`. Pin the real path
        // (`collect_records_consumed`, which `on_records_received` prints
        // verbatim) so a regression that emitted the truncated count would fail.
        let mut consumer = verifiable_with(2, false);
        let tp = TopicPartition::new("t", 0);
        // 5 records, but only 2 fit under max_messages=2.
        let recs = vec![
            record("t", 0, 100),
            record("t", 0, 101),
            record("t", 0, 102),
            record("t", 0, 103),
            record("t", 0, 104),
        ];
        let records = records_of(vec![(tp.clone(), recs)]);

        let (offsets, event) = consumer.collect_records_consumed(&records).unwrap();

        // The emitted `records_consumed.count` is the full 5, NOT the truncated 2.
        assert_eq!(event.count, 5);
        // ... while the per-partition summary reflects the truncation (2 records,
        // offsets 100..=101).
        assert_eq!(event.partitions.len(), 1);
        let summary = &event.partitions[0];
        assert_eq!(summary.count, 2);
        assert_eq!(summary.min_offset, 100);
        assert_eq!(summary.max_offset, 101);
        // The committed offset is the truncated maxOffset (101) + 1, and only 2
        // messages were counted as consumed.
        assert_eq!(offsets.get(&tp).unwrap().offset(), 102);
        assert_eq!(consumer.consumed_messages, 2);
        assert!(consumer.is_finished());

        // The serialized event carries the full count on the wire.
        let mut wire = RecordsConsumed::new(event.count, event.partitions);
        wire.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&wire).unwrap(),
            r#"{"timestamp":42,"name":"records_consumed","count":5,"partitions":[{"topic":"t","partition":0,"count":2,"minOffset":100,"maxOffset":101}]}"#
        );
    }

    // ---- exact JSON vectors (the stdout wire contract) ---------------------
    //
    // Each vector goes through the real event-construction path (constructor /
    // `from_*`), then overwrites `timestamp` so the wire string is exact
    // (DoD §12: pin the mechanism, not a hand-built fixture).

    #[test]
    fn startup_complete_json() {
        let mut event = StartupComplete::new();
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"startup_complete"}"#
        );
    }

    #[test]
    fn shutdown_requested_json() {
        let mut event = ShutdownRequested::new();
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"shutdown_requested"}"#
        );
    }

    #[test]
    fn shutdown_complete_json() {
        let mut event = ShutdownComplete::new();
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"shutdown_complete"}"#
        );
    }

    #[test]
    fn partitions_assigned_json_with_topic_partition_shape() {
        let mut event = PartitionsAssigned::new(&[TopicPartition::new("t", 0), TopicPartition::new("t", 1)]);
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"partitions_assigned","partitions":[{"topic":"t","partition":0},{"topic":"t","partition":1}]}"#
        );
    }

    #[test]
    fn partitions_revoked_json_with_topic_partition_shape() {
        let mut event = PartitionsRevoked::new(&[TopicPartition::new("t", 3)]);
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"partitions_revoked","partitions":[{"topic":"t","partition":3}]}"#
        );
    }

    #[test]
    fn records_consumed_json() {
        let mut event = RecordsConsumed::new(3, vec![RecordSetSummary::new("t".to_string(), 0, 3, 10, 12)]);
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"records_consumed","count":3,"partitions":[{"topic":"t","partition":0,"count":3,"minOffset":10,"maxOffset":12}]}"#
        );
    }

    #[test]
    fn record_data_json_field_order() {
        let rec = ConsumerRecord::new("t".to_string(), 2, 9, Some("k".to_string()), Some("v".to_string()));
        let mut event = RecordData::from_record(&rec);
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"record_data","key":"k","value":"v","topic":"t","partition":2,"offset":9}"#
        );
    }

    #[test]
    fn record_data_json_null_key() {
        let rec: ConsumerRecord<String, String> =
            ConsumerRecord::new("t".to_string(), 0, 0, None, Some("v".to_string()));
        let mut event = RecordData::from_record(&rec);
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"record_data","key":null,"value":"v","topic":"t","partition":0,"offset":0}"#
        );
    }

    #[test]
    fn offsets_committed_json_success_omits_error() {
        let mut offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        offsets.insert(TopicPartition::new("t", 0), OffsetAndMetadata::new(5).unwrap());
        let mut event = OffsetsCommitted::from_commit(&offsets, None);
        event.timestamp = 42;
        // Success => `error` is omitted (Java `@JsonInclude(NON_NULL)`).
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"offsets_committed","offsets":[{"topic":"t","partition":0,"offset":5}],"success":true}"#
        );
    }

    #[test]
    fn offsets_committed_json_failure_includes_error() {
        let offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
        let error = Error::local_illegal_argument("boom");
        let mut event = OffsetsCommitted::from_commit(&offsets, Some(&error));
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"offsets_committed","offsets":[],"error":"boom","success":false}"#
        );
    }

    // ---- arg parsing --------------------------------------------------------

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn minimal() -> Vec<String> {
        args(&["--bootstrap-server", "b", "--topic", "t", "--group-id", "g"])
    }

    #[test]
    fn parse_args_minimal_ok() {
        let parsed = parse_args(&minimal()).unwrap();
        assert_eq!(parsed.bootstrap_server, "b");
        assert_eq!(parsed.topic, "t");
        assert_eq!(parsed.group_id, "g");
        // Defaults.
        assert_eq!(parsed.group_protocol, ConsumerConfig::DEFAULT_GROUP_PROTOCOL);
        assert_eq!(parsed.group_remote_assignor, None);
        assert_eq!(parsed.group_instance_id, None);
        assert_eq!(parsed.max_messages, -1);
        assert_eq!(parsed.session_timeout, None);
        assert!(!parsed.verbose);
        assert!(!parsed.use_auto_commit);
        assert_eq!(parsed.reset_policy, "earliest");
        assert_eq!(parsed.assignment_strategy, DEFAULT_ASSIGNMENT_STRATEGY);
    }

    #[test]
    fn parse_args_missing_bootstrap_server_errors() {
        let error = parse_args(&args(&["--topic", "t", "--group-id", "g"])).unwrap_err();
        assert_eq!(error.message(), "one of the arguments --bootstrap-server is required");
    }

    #[test]
    fn parse_args_missing_topic_errors() {
        let error = parse_args(&args(&["--bootstrap-server", "b", "--group-id", "g"])).unwrap_err();
        assert_eq!(error.message(), "argument --topic is required");
    }

    #[test]
    fn parse_args_missing_group_id_errors() {
        let error = parse_args(&args(&["--bootstrap-server", "b", "--topic", "t"])).unwrap_err();
        assert_eq!(error.message(), "argument --group-id is required");
    }

    #[test]
    fn parse_args_flags_and_values() {
        let mut a = minimal();
        a.extend(args(&[
            "--group-protocol",
            "consumer",
            "--group-remote-assignor",
            "uniform",
            "--group-instance-id",
            "inst-1",
            "--max-messages",
            "100",
            "--session-timeout",
            "30000",
            "--verbose",
            "--enable-autocommit",
            "--reset-policy",
            "latest",
        ]));
        let parsed = parse_args(&a).unwrap();
        assert_eq!(parsed.group_protocol, "consumer");
        assert_eq!(parsed.group_remote_assignor.as_deref(), Some("uniform"));
        assert_eq!(parsed.group_instance_id.as_deref(), Some("inst-1"));
        assert_eq!(parsed.max_messages, 100);
        assert_eq!(parsed.session_timeout, Some(30000));
        assert!(parsed.verbose);
        assert!(parsed.use_auto_commit);
        assert_eq!(parsed.reset_policy, "latest");
    }

    #[test]
    fn parse_args_config_file_mutual_exclusion_errors() {
        let mut a = minimal();
        a.extend(args(&["--consumer.config", "a.properties", "--command-config", "b.properties"]));
        let error = parse_args(&a).unwrap_err();
        assert_eq!(
            error.message(),
            "Options --consumer.config and --command-config are mutually exclusive."
        );
    }

    #[test]
    fn parse_args_missing_value_errors() {
        let error = parse_args(&args(&["--topic"])).unwrap_err();
        assert_eq!(error.message(), "argument --topic: expected one argument");
    }

    #[test]
    fn parse_args_unrecognized_argument_errors() {
        let mut a = minimal();
        a.push("--bogus".to_string());
        let error = parse_args(&a).unwrap_err();
        assert_eq!(error.message(), "unrecognized arguments: '--bogus'");
    }

    // ---- create_from_args: classic protocol is unsupported ------------------

    #[test]
    fn create_from_args_classic_protocol_is_unsupported() {
        // The client is KIP-848-only; classic fails at `new_consumer`
        // (consumer-threading.md §20). This is the documented, faithful
        // behavior. (The default protocol is `classic`, so the minimal args
        // already select it.)
        // `VerifiableConsumer` (the Ok type) is not `Debug`, so match rather
        // than `unwrap_err`.
        let error = match create_from_args(&minimal()) {
            Ok(_) => panic!("expected classic group protocol to be unsupported"),
            Err(e) => e,
        };
        assert_eq!(error.error(), confluent_kafka::common::protocol::Errors::UnsupportedVersion);
    }

    // ---- properties parsing -------------------------------------------------

    #[test]
    fn parse_properties_handles_common_forms() {
        let contents = "\
# a comment
! another comment

bootstrap.servers=localhost:9092
group.id : cg
auto.offset.reset earliest
";
        let props = parse_properties(contents);
        assert_eq!(
            props,
            vec![
                ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
                ("group.id".to_string(), "cg".to_string()),
                ("auto.offset.reset".to_string(), "earliest".to_string()),
            ]
        );
    }
}
