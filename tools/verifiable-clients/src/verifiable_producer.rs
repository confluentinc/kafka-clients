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

//! Translated from `org.apache.kafka.tools.VerifiableProducer`.
//!
//! Primarily intended for use with system testing, this producer prints
//! metadata in the form of JSON to stdout on each "send" request. For example,
//! this helps with end-to-end correctness tests by making externally visible
//! which messages have been acked and which have not.
//!
//! When used as a command-line tool, it produces increasing integers. It will
//! produce a fixed number of messages unless the default max-messages -1 is
//! used, in which case it produces indefinitely.
//!
//! If logging is left enabled, log output on stdout can be easily ignored by
//! checking whether a given line is valid JSON.
//!
//! # The JSON stdout contract
//!
//! Every printed event mirrors Java's Jackson output exactly — event `name`,
//! field names, and field order — because that stdout contract is what a
//! downstream ducktape-style harness parses. serde
//! serializes struct fields in declaration order, so each event struct declares
//! its fields in the same order Java's `@JsonPropertyOrder({"timestamp","name"})`
//! plus `@JsonProperty` method declaration order produces.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::{Callback, KafkaProducer, Producer, ProducerConfig, ProducerRecord, RecordMetadata};

use crate::throughput_throttler::ThroughputThrottler;

/// Wall-clock milliseconds since the Unix epoch. The Rust analog of Java's
/// `System.currentTimeMillis()`.
fn now_millis() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}

/// Java's `printJson`: serialize `data` and print one line, or print a
/// diagnostic if it cannot be serialized. A free function (Java makes it an
/// instance method only to reach the shared `ObjectMapper`, which is stateless
/// here). `println!` locks stdout for the whole line, so each event is written
/// atomically — the Rust equivalent of Java's `synchronized (System.out)`.
fn print_json<T: Serialize>(data: &T) {
    match serde_json::to_string(data) {
        Ok(json) => println!("{json}"),
        Err(e) => println!("Bad data can't be written as json: {e}"),
    }
}

/// The value for the JSON `exception` field.
///
/// Java's `FailedSend.exception()` is `Exception.getClass().toString()` — a JVM
/// class name that has no Rust equivalent. We emit the error's protocol
/// classification (`Errors` variant) as the closest stable analog. The JSON
/// *field name* `exception` is Java's wire contract and is preserved unchanged;
/// only this value differs, unavoidably. The Rust identifier deliberately
/// avoids the word "exception".
fn error_class_name(error: &Error) -> String {
    format!("{:?}", error.error())
}

// ----------------------------------------------------------------------------
// JSON events (Java's `ProducerEvent` hierarchy).
//
// `@JsonPropertyOrder({"timestamp","name"})` on the Java base class ⇒ every
// event serializes `timestamp` then `name` first. Subsequent fields follow in
// `@JsonProperty` method declaration order, which each struct reproduces via
// field order.
// ----------------------------------------------------------------------------

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

/// `producer_send_success` (Java `SuccessfulSend`).
#[derive(Serialize)]
struct SuccessfulSend {
    timestamp: i64,
    name: &'static str,
    // Java `key()` returns a possibly-null String.
    key: Option<String>,
    value: String,
    topic: String,
    partition: i32,
    offset: i64,
}

impl SuccessfulSend {
    fn new(key: Option<String>, value: String, record_metadata: &RecordMetadata) -> Self {
        Self {
            timestamp: now_millis(),
            name: "producer_send_success",
            key,
            value,
            topic: record_metadata.topic().to_string(),
            partition: record_metadata.partition(),
            offset: record_metadata.offset(),
        }
    }
}

/// `producer_send_error` (Java `FailedSend`).
#[derive(Serialize)]
struct FailedSend {
    timestamp: i64,
    name: &'static str,
    key: Option<String>,
    value: String,
    topic: String,
    // The serialized key `exception` is the Java event's field name (Jackson
    // `@JsonProperty` on `exception()`), part of the stdout wire contract. The
    // Rust field is named `error_class` because the word "exception" must not
    // appear in Rust identifiers; `#[serde(rename)]` restores the Java wire key.
    // See `error_class_name` for how the value is derived.
    #[serde(rename = "exception")]
    error_class: String,
    // Java `message()` is `exception.getMessage()`, which may be null and is
    // serialized with no `NON_NULL` filter, so a null renders as `"message":null`
    // (always present, never skipped). `Error::message()` is the faithful
    // translation of `getMessage()` (bare text, no class-name prefix — unlike
    // `Display`/`to_string()`, which is `Throwable.toString()`); it represents
    // "no message" as the empty string, which we map to `None` so serde renders
    // the JSON `null` Java produces.
    message: Option<String>,
}

impl FailedSend {
    fn from_error(key: Option<String>, value: String, topic: String, error: &Error) -> Self {
        // Faithful to Java `FailedSend.message()` = `exception.getMessage()`
        // (not `toString()`): the class/variant lives in the sibling `exception`
        // field, so `message` must not duplicate that prefix. An empty
        // `message()` (Rust's "no message") maps to `None` → JSON `null`, exactly
        // as Java surfaces a null `getMessage()`.
        let message = error.message();
        let message = if message.is_empty() {
            None
        } else {
            Some(message.to_string())
        };
        Self {
            timestamp: now_millis(),
            name: "producer_send_error",
            key,
            value,
            topic,
            error_class: error_class_name(error),
            message,
        }
    }
}

/// `tool_data` (Java `ToolData`).
#[derive(Serialize)]
struct ToolData {
    timestamp: i64,
    name: &'static str,
    sent: i64,
    acked: i64,
    // Java `@JsonProperty("target_throughput")` on `targetThroughput()`.
    #[serde(rename = "target_throughput")]
    target_throughput: i64,
    // Java `@JsonProperty("avg_throughput")` on `avgThroughput()`.
    #[serde(rename = "avg_throughput")]
    avg_throughput: f64,
}

impl ToolData {
    fn new(sent: i64, acked: i64, target_throughput: i64, avg_throughput: f64) -> Self {
        Self {
            timestamp: now_millis(),
            name: "tool_data",
            sent,
            acked,
            target_throughput,
            avg_throughput,
        }
    }
}

/// The `VerifiableProducer`.
///
/// Generic over the producer type `P` because Java stores the producer as the
/// `Producer<String, String>` *interface*. The Rust [`Producer`] trait uses
/// native `async fn` and so is not `dyn`-compatible; a type parameter is the
/// faithful (static-dispatch) analog, and lets tests substitute
/// [`MockProducer`](confluent_kafka::producer::MockProducer).
pub struct VerifiableProducer<P: Producer<String, String>> {
    topic: String,
    producer: P,
    // If maxMessages < 0, produce until the process is killed externally.
    max_messages: i64,
    // Number of messages for which acks were received. Shared with the send
    // callback (which runs on the producer's completion path), so it is an
    // `Arc<AtomicI64>` rather than a plain field: a counter shared across tasks
    // is atomic, not a lock.
    num_acked: Arc<AtomicI64>,
    // Number of send attempts.
    num_sent: i64,
    // Throttle message throughput if this is set >= 0.
    throughput: i64,
    // Hook to trigger the producing task to stop sending messages. Written by
    // the ctrl-c task (Java's shutdown hook), read by `run`; shared, so atomic.
    stop_producing: Arc<AtomicBool>,
    // Prefix (plus a dot separator) added to every value produced; `None` means
    // values are produced without a prefix.
    value_prefix: Option<i32>,
    // Send messages with a key of 0 incrementing by 1 for each message produced;
    // when the number specified is reached, the key is reset to 0.
    repeating_keys: Option<i32>,
    key_counter: i32,
    // The create time to set in messages, in milliseconds since epoch.
    create_time: Option<i64>,
    start_time: i64,
}

impl<P: Producer<String, String>> VerifiableProducer<P> {
    /// Construct a `VerifiableProducer`. Mirrors Java's constructor.
    pub fn new(
        producer: P,
        topic: String,
        throughput: i32,
        max_messages: i32,
        value_prefix: Option<i32>,
        create_time: Option<i64>,
        repeating_keys: Option<i32>,
    ) -> Self {
        Self {
            topic,
            producer,
            max_messages: i64::from(max_messages),
            num_acked: Arc::new(AtomicI64::new(0)),
            num_sent: 0,
            throughput: i64::from(throughput),
            stop_producing: Arc::new(AtomicBool::new(false)),
            value_prefix,
            create_time,
            start_time: now_millis(),
            repeating_keys,
            key_counter: 0,
        }
    }

    /// Returns a string to publish: either `valuePrefix.val` or `val`.
    pub fn get_value(&self, val: i64) -> String {
        match self.value_prefix {
            Some(value_prefix) => format!("{value_prefix}.{val}"),
            None => format!("{val}"),
        }
    }

    /// Returns the next key, or `None` when `--repeating-keys` was not set.
    pub fn get_key(&mut self) -> Option<String> {
        match self.repeating_keys {
            Some(repeating_keys) => {
                // Java uses post-increment: the key is the current counter, then
                // it advances and wraps back to 0 on reaching `repeatingKeys`.
                let key = self.key_counter.to_string();
                self.key_counter += 1;
                if self.key_counter == repeating_keys {
                    self.key_counter = 0;
                }
                Some(key)
            },
            None => None,
        }
    }

    /// Produce a message with the given key and value.
    pub async fn send(&mut self, key: Option<String>, value: String) {
        // Build the record (Java 285-290). Java performs this outside its
        // try/catch, so a negative create-time would propagate (crash). We
        // surface it as a FailedSend instead of aborting (no panic on a
        // recoverable path); `numSent` is left un-incremented,
        // matching Java's ordering (the increment follows construction).
        let record = if let Some(create_time) = self.create_time {
            let record = ProducerRecord::with_timestamp(
                self.topic.clone(),
                None,
                Some(create_time),
                key.clone(),
                Some(value.clone()),
            )
            .map_err(|e| Error::local_illegal_argument(e.message()));
            // Java 287: advance createTime by wall-clock progress since start.
            self.create_time = Some(create_time + (now_millis() - self.start_time));
            record
        } else {
            Ok(ProducerRecord::with_key(self.topic.clone(), key.clone(), Some(value.clone())))
        };
        let record = match record {
            Ok(record) => record,
            Err(e) => {
                print_json(&FailedSend::from_error(key, value, self.topic.clone(), &e));
                return;
            },
        };

        self.num_sent += 1;

        // PrintInfoCallback (Java 496-516): exactly one JSON line per completed
        // send — success increments numAcked and prints producer_send_success,
        // failure prints producer_send_error. The producer invokes this callback
        // exactly once when the send completes; we pass it through rather than
        // re-implementing it.
        let cb_key = key.clone();
        let cb_value = value.clone();
        let cb_topic = self.topic.clone();
        let num_acked = Arc::clone(&self.num_acked);
        let callback: Callback =
            Box::new(
                move |record_metadata: Option<&RecordMetadata>, error: Option<&Error>| match error {
                    None => {
                        num_acked.fetch_add(1, Ordering::AcqRel);
                        let record_metadata = record_metadata.expect("Expected non-null recordMetadata object.");
                        print_json(&SuccessfulSend::new(cb_key, cb_value, record_metadata));
                    },
                    Some(e) => {
                        print_json(&FailedSend::from_error(cb_key, cb_value, cb_topic, e));
                    },
                },
            );

        // Java 293-300 wraps producer.send in try/catch: a synchronous failure
        // (buffer exhaustion, closed producer, ...) prints a FailedSend here.
        // Asynchronous failures arrive through the callback above instead.
        if let Err(e) = self.producer.send_with_callback(record, Some(callback)).await {
            print_json(&FailedSend::from_error(key, value, self.topic.clone(), &e));
        }
    }

    /// Close the producer to flush any remaining messages, then print
    /// `shutdown_complete`.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the underlying producer fails to close. Java's `close()`
    /// is `void` but may throw unchecked; we surface that as a `Result`, and (as
    /// in Java) `shutdown_complete` is printed only on a clean close.
    pub async fn close(&self) -> Result<(), Error> {
        self.producer.close().await?;
        print_json(&ShutdownComplete::new());
        Ok(())
    }

    /// Produce messages until `maxMessages` is reached or stop is requested.
    pub async fn run(&mut self, throttler: &ThroughputThrottler) {
        print_json(&StartupComplete::new());
        // negative maxMessages (-1) means "infinite"
        let max_messages = if self.max_messages < 0 {
            i64::MAX
        } else {
            self.max_messages
        };

        let mut i: i64 = 0;
        while i < max_messages {
            if self.stop_producing.load(Ordering::Acquire) {
                break;
            }
            let send_start_ms = now_millis();

            let key = self.get_key();
            let value = self.get_value(i);
            self.send(key, value).await;

            if throttler.should_throttle(i, send_start_ms) {
                throttler.throttle().await;
            }
            i += 1;
        }
    }

    /// Number of send attempts so far (Java `numSent`).
    pub fn num_sent(&self) -> i64 {
        self.num_sent
    }

    /// Number of messages for which acks were received (Java `numAcked`).
    pub fn num_acked(&self) -> i64 {
        self.num_acked.load(Ordering::Acquire)
    }

    /// The configured target throughput (Java `throughput`).
    pub fn throughput(&self) -> i64 {
        self.throughput
    }

    /// A shared handle to the stop flag, for the ctrl-c task to flip (Java's
    /// shutdown hook sets `stopProducing = true`).
    pub fn stop_producing_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.stop_producing)
    }

    /// Print the `tool_data` summary event (Java's shutdown hook, lines 561-564).
    pub fn print_tool_data(&self, avg_throughput: f64) {
        print_json(&ToolData::new(
            self.num_sent(),
            self.num_acked(),
            self.throughput,
            avg_throughput,
        ));
    }
}

/// The parsed command-line arguments, separated from producer construction so
/// the parsing/validation logic is unit-testable without a live broker.
#[derive(Debug, PartialEq)]
struct ParsedArgs {
    topic: String,
    bootstrap_server: String,
    max_messages: i32,
    throughput: i32,
    acks: i32,
    config_file: Option<String>,
    command_config_file: Option<String>,
    // Java: `--message-create-time == -1L` means null (no explicit create time).
    create_time: Option<i64>,
    value_prefix: Option<i32>,
    repeating_keys: Option<i32>,
}

/// Build a command-line argument error. Java raises
/// `ArgumentParserException`; the word "exception" must not appear in Rust
/// identifiers, so this maps to a recoverable `LocalIllegalArgument` error
/// carrying the same message.
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

fn parse_long(value: &str, flag: &str) -> Result<i64, Error> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|_| arg_error(format!("argument {flag}: could not convert '{value}' to long")))
}

/// Reproduces Java's `argParser()` + the validation `createFromArgs` performs
/// before touching the network (required args, `--acks` choices, the
/// `--producer.config` / `--command-config` mutual exclusion).
fn parse_args(args: &[String]) -> Result<ParsedArgs, Error> {
    let mut topic: Option<String> = None;
    let mut bootstrap_server: Option<String> = None;
    let mut max_messages: i32 = -1;
    let mut throughput: i32 = -1;
    let mut acks: i32 = -1;
    let mut config_file: Option<String> = None;
    let mut command_config_file: Option<String> = None;
    let mut create_time_raw: i64 = -1;
    let mut value_prefix: Option<i32> = None;
    let mut repeating_keys: Option<i32> = None;

    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--topic" => topic = Some(next_value(&mut it, "--topic")?),
            "--bootstrap-server" => bootstrap_server = Some(next_value(&mut it, "--bootstrap-server")?),
            "--max-messages" => max_messages = parse_int(&next_value(&mut it, "--max-messages")?, "--max-messages")?,
            "--throughput" => throughput = parse_int(&next_value(&mut it, "--throughput")?, "--throughput")?,
            "--acks" => {
                let value = parse_int(&next_value(&mut it, "--acks")?, "--acks")?;
                // Java `.choices(0, 1, -1)`.
                if value != 0 && value != 1 && value != -1 {
                    return Err(arg_error(format!(
                        "argument --acks: invalid choice: '{value}' (choose from 0, 1, -1)"
                    )));
                }
                acks = value;
            },
            "--producer.config" => config_file = Some(next_value(&mut it, "--producer.config")?),
            "--command-config" => command_config_file = Some(next_value(&mut it, "--command-config")?),
            "--message-create-time" => {
                create_time_raw = parse_long(&next_value(&mut it, "--message-create-time")?, "--message-create-time")?
            },
            "--value-prefix" => {
                value_prefix = Some(parse_int(&next_value(&mut it, "--value-prefix")?, "--value-prefix")?)
            },
            "--repeating-keys" => {
                repeating_keys = Some(parse_int(&next_value(&mut it, "--repeating-keys")?, "--repeating-keys")?)
            },
            other => return Err(arg_error(format!("unrecognized arguments: '{other}'"))),
        }
    }

    let topic = topic.ok_or_else(|| arg_error("argument --topic is required"))?;
    // The Java "Connection Group" mutually-exclusive group is `required(true)`
    // and contains only `--bootstrap-server`, so it is effectively required.
    let bootstrap_server =
        bootstrap_server.ok_or_else(|| arg_error("one of the arguments --bootstrap-server is required"))?;

    // Java createFromArgs 253-255.
    if config_file.is_some() && command_config_file.is_some() {
        return Err(arg_error(
            "Options --producer.config and --command-config are mutually exclusive.",
        ));
    }

    // Java createFromArgs 234-235: `-1L` means null (no explicit create time).
    let create_time = if create_time_raw == -1 {
        None
    } else {
        Some(create_time_raw)
    };

    Ok(ParsedArgs {
        topic,
        bootstrap_server,
        max_messages,
        throughput,
        acks,
        config_file,
        command_config_file,
        create_time,
        value_prefix,
        repeating_keys,
    })
}

/// Read a Java-properties file into ordered key/value pairs.
///
/// Java's `VerifiableProducer.loadProps` delegates to `java.util.Properties`.
/// This is a minimal translation covering the forms system-test config files
/// use: `key=value` / `key:value` / `key value`, `#` and `!` comment lines, and
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
        // The key runs up to the first '=', ':' or whitespace. Then, as
        // `java.util.Properties` does, skip surrounding whitespace and a single
        // '=' or ':' separator; the remainder (trimmed) is the value. A '=' or
        // ':' inside the value is preserved because only the first is consumed.
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

/// Construct a `VerifiableProducer` from command-line arguments. Mirrors Java's
/// static `createFromArgs`.
///
/// # Errors
///
/// Returns `Err` for any argument-parsing failure, an unreadable config file, an
/// invalid producer configuration, or a producer that cannot be constructed.
pub async fn create_from_args(args: &[String]) -> Result<VerifiableProducer<KafkaProducer<String, String>>, Error> {
    let parsed = parse_args(args)?;

    // Build the producer property map (Java createFromArgs 237-271). The key/
    // value serializer class strings Java sets for reflection are omitted: Rust
    // passes StringSerializer *instances* to the constructor instead (no
    // reflection), so those config keys would only warn as "unknown".
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert(
        ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        parsed.bootstrap_server.clone(),
    );
    props.insert(ProducerConfig::ACKS_CONFIG.to_string(), parsed.acks.to_string());
    // No producer retries.
    props.insert(ProducerConfig::RETRIES_CONFIG.to_string(), "0".to_string());

    if let Some(config_file) = &parsed.config_file {
        println!(
            "Option --producer.config has been deprecated and will be removed in a future version. Use --command-config instead."
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

    let config = ProducerConfig::from_properties(&props)?;
    let producer =
        KafkaProducer::<String, String>::from_config(config, Box::new(StringSerializer), Box::new(StringSerializer))?;

    Ok(VerifiableProducer::new(
        producer,
        parsed.topic,
        parsed.throughput,
        parsed.max_messages,
        parsed.value_prefix,
        parsed.create_time,
        parsed.repeating_keys,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use confluent_kafka::common::protocol::Errors;
    use confluent_kafka::producer::MockProducer;

    fn mock_producer() -> MockProducer<String, String> {
        MockProducer::with_auto_complete(true)
    }

    fn verifiable_with(
        value_prefix: Option<i32>,
        repeating_keys: Option<i32>,
    ) -> VerifiableProducer<MockProducer<String, String>> {
        VerifiableProducer::new(
            mock_producer(),
            "test-topic".to_string(),
            -1,
            -1,
            value_prefix,
            None,
            repeating_keys,
        )
    }

    // ---- get_value ----------------------------------------------------------

    #[test]
    fn get_value_without_prefix_is_just_the_number() {
        let producer = verifiable_with(None, None);
        assert_eq!(producer.get_value(0), "0");
        assert_eq!(producer.get_value(42), "42");
    }

    #[test]
    fn get_value_with_prefix_uses_dot_separator() {
        let producer = verifiable_with(Some(7), None);
        assert_eq!(producer.get_value(0), "7.0");
        assert_eq!(producer.get_value(42), "7.42");
    }

    // ---- get_key ------------------------------------------------------------

    #[test]
    fn get_key_is_none_without_repeating_keys() {
        let mut producer = verifiable_with(None, None);
        assert_eq!(producer.get_key(), None);
        assert_eq!(producer.get_key(), None);
    }

    #[test]
    fn get_key_wraps_around_at_repeating_keys() {
        // repeatingKeys = 3 => keys cycle 0,1,2,0,1,2,...
        let mut producer = verifiable_with(None, Some(3));
        assert_eq!(producer.get_key().as_deref(), Some("0"));
        assert_eq!(producer.get_key().as_deref(), Some("1"));
        assert_eq!(producer.get_key().as_deref(), Some("2"));
        assert_eq!(producer.get_key().as_deref(), Some("0"));
        assert_eq!(producer.get_key().as_deref(), Some("1"));
    }

    // ---- send / callback obligation ----------------------------------------

    #[tokio::test]
    async fn send_increments_sent_and_acked_via_callback() {
        // With an auto-completing MockProducer the callback fires synchronously
        // on send, so exactly one ack is recorded per completed send.
        let mut producer = verifiable_with(None, None);
        assert_eq!(producer.num_sent(), 0);
        assert_eq!(producer.num_acked(), 0);

        producer.send(Some("k".to_string()), "v".to_string()).await;
        assert_eq!(producer.num_sent(), 1);
        assert_eq!(producer.num_acked(), 1);

        producer.send(None, "v2".to_string()).await;
        assert_eq!(producer.num_sent(), 2);
        assert_eq!(producer.num_acked(), 2);
    }

    // ---- exact JSON vectors (the stdout wire contract) ----------------------

    #[test]
    fn startup_complete_json() {
        let event = StartupComplete { timestamp: 42, name: "startup_complete" };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"startup_complete"}"#
        );
    }

    #[test]
    fn shutdown_complete_json() {
        let event = ShutdownComplete { timestamp: 42, name: "shutdown_complete" };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"shutdown_complete"}"#
        );
    }

    #[test]
    fn successful_send_json_with_key() {
        let event = SuccessfulSend {
            timestamp: 42,
            name: "producer_send_success",
            key: Some("k".to_string()),
            value: "v".to_string(),
            topic: "t".to_string(),
            partition: 3,
            offset: 7,
        };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"producer_send_success","key":"k","value":"v","topic":"t","partition":3,"offset":7}"#
        );
    }

    #[test]
    fn successful_send_json_with_null_key() {
        let event = SuccessfulSend {
            timestamp: 42,
            name: "producer_send_success",
            key: None,
            value: "v".to_string(),
            topic: "t".to_string(),
            partition: 0,
            offset: 0,
        };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"producer_send_success","key":null,"value":"v","topic":"t","partition":0,"offset":0}"#
        );
    }

    #[test]
    fn failed_send_json_with_message() {
        let event = FailedSend {
            timestamp: 42,
            name: "producer_send_error",
            key: Some("k".to_string()),
            value: "v".to_string(),
            topic: "t".to_string(),
            error_class: "RequestTimedOut".to_string(),
            message: Some("boom".to_string()),
        };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"producer_send_error","key":"k","value":"v","topic":"t","exception":"RequestTimedOut","message":"boom"}"#
        );
    }

    #[test]
    fn failed_send_json_with_null_message() {
        let event = FailedSend {
            timestamp: 42,
            name: "producer_send_error",
            key: None,
            value: "v".to_string(),
            topic: "t".to_string(),
            error_class: "RequestTimedOut".to_string(),
            message: None,
        };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"producer_send_error","key":null,"value":"v","topic":"t","exception":"RequestTimedOut","message":null}"#
        );
    }

    // These two go through the real `from_error` derivation path (not a
    // hand-built struct), so they prove `message` is `getMessage()` (bare text,
    // no class-name prefix) AND that an empty message renders as JSON `null`,
    // exactly as Java's `FailedSend` does. The timestamp is overwritten so the
    // wire string is exact.
    #[test]
    fn failed_send_from_error_present_message_json() {
        let error = Error::with_message(Errors::RequestTimedOut, "boom");
        let mut event = FailedSend::from_error(Some("k".to_string()), "v".to_string(), "t".to_string(), &error);
        event.timestamp = 42;
        // `message` is the bare text, NOT `to_string()` (which would prefix the
        // class name and duplicate the `exception` field).
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"producer_send_error","key":"k","value":"v","topic":"t","exception":"RequestTimedOut","message":"boom"}"#
        );
    }

    #[test]
    fn failed_send_from_error_null_message_json() {
        // An empty message is Rust's representation of "no message"; Java's
        // `getMessage()` would be null. Both render as JSON `null` (present, not
        // skipped).
        let error = Error::with_message(Errors::RequestTimedOut, "");
        let mut event = FailedSend::from_error(None, "v".to_string(), "t".to_string(), &error);
        event.timestamp = 42;
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"producer_send_error","key":null,"value":"v","topic":"t","exception":"RequestTimedOut","message":null}"#
        );
    }

    #[test]
    fn tool_data_json() {
        let event = ToolData {
            timestamp: 42,
            name: "tool_data",
            sent: 10,
            acked: 8,
            target_throughput: 100,
            avg_throughput: 50.0,
        };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"timestamp":42,"name":"tool_data","sent":10,"acked":8,"target_throughput":100,"avg_throughput":50.0}"#
        );
    }

    // ---- arg parsing --------------------------------------------------------

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_args_minimal_ok() {
        let parsed = parse_args(&args(&["--topic", "t", "--bootstrap-server", "localhost:9092"])).unwrap();
        assert_eq!(parsed.topic, "t");
        assert_eq!(parsed.bootstrap_server, "localhost:9092");
        // Defaults.
        assert_eq!(parsed.max_messages, -1);
        assert_eq!(parsed.throughput, -1);
        assert_eq!(parsed.acks, -1);
        assert_eq!(parsed.create_time, None);
        assert_eq!(parsed.value_prefix, None);
        assert_eq!(parsed.repeating_keys, None);
    }

    #[test]
    fn parse_args_missing_topic_errors() {
        let error = parse_args(&args(&["--bootstrap-server", "localhost:9092"])).unwrap_err();
        assert_eq!(error.message(), "argument --topic is required");
    }

    #[test]
    fn parse_args_missing_bootstrap_server_errors() {
        let error = parse_args(&args(&["--topic", "t"])).unwrap_err();
        assert_eq!(error.message(), "one of the arguments --bootstrap-server is required");
    }

    #[test]
    fn parse_args_invalid_acks_choice_errors() {
        let error = parse_args(&args(&["--topic", "t", "--bootstrap-server", "b", "--acks", "5"])).unwrap_err();
        assert_eq!(error.message(), "argument --acks: invalid choice: '5' (choose from 0, 1, -1)");
    }

    #[test]
    fn parse_args_valid_acks_choices() {
        for value in ["0", "1", "-1"] {
            let parsed = parse_args(&args(&["--topic", "t", "--bootstrap-server", "b", "--acks", value])).unwrap();
            assert_eq!(parsed.acks, value.parse::<i32>().unwrap());
        }
    }

    #[test]
    fn parse_args_config_file_mutual_exclusion_errors() {
        let error = parse_args(&args(&[
            "--topic",
            "t",
            "--bootstrap-server",
            "b",
            "--producer.config",
            "a.properties",
            "--command-config",
            "b.properties",
        ]))
        .unwrap_err();
        assert_eq!(
            error.message(),
            "Options --producer.config and --command-config are mutually exclusive."
        );
    }

    #[test]
    fn parse_args_message_create_time_minus_one_is_none() {
        let parsed = parse_args(&args(&[
            "--topic",
            "t",
            "--bootstrap-server",
            "b",
            "--message-create-time",
            "-1",
        ]))
        .unwrap();
        assert_eq!(parsed.create_time, None);

        let parsed = parse_args(&args(&[
            "--topic",
            "t",
            "--bootstrap-server",
            "b",
            "--message-create-time",
            "1234",
        ]))
        .unwrap();
        assert_eq!(parsed.create_time, Some(1234));
    }

    #[test]
    fn parse_args_missing_value_errors() {
        let error = parse_args(&args(&["--topic"])).unwrap_err();
        assert_eq!(error.message(), "argument --topic: expected one argument");
    }

    #[test]
    fn parse_args_unrecognized_argument_errors() {
        let error = parse_args(&args(&["--topic", "t", "--bootstrap-server", "b", "--bogus"])).unwrap_err();
        assert_eq!(error.message(), "unrecognized arguments: '--bogus'");
    }

    // ---- properties parsing -------------------------------------------------

    #[test]
    fn parse_properties_handles_common_forms() {
        let contents = "\
# a comment
! another comment

bootstrap.servers=localhost:9092
linger.ms : 5
compression.type snappy
  padded.key = padded.value
";
        let props = parse_properties(contents);
        assert_eq!(
            props,
            vec![
                ("bootstrap.servers".to_string(), "localhost:9092".to_string()),
                ("linger.ms".to_string(), "5".to_string()),
                ("compression.type".to_string(), "snappy".to_string()),
                ("padded.key".to_string(), "padded.value".to_string()),
            ]
        );
    }
}
