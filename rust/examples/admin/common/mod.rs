// Copyright 2026 Confluent Inc.
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

//! Helpers shared by the admin examples: argument parsing, client creation and
//! the exit-code convention. Each example includes this file with
//! `#[path = "common/mod.rs"] mod common;`.
//!
//! Command line: `[-X key=value]... [--flag | --name=value]... <positional>...`.
//! `--help` prints the usage text.
//! `-X` sets a client configuration property (librdkafka's convention), for
//! example `-X security.protocol=SASL_PLAINTEXT`. The bootstrap servers come
//! from `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) unless `-X
//! bootstrap.servers=...` is given.
//!
//! Exit codes: 0 when everything succeeded, 1 when the client could not be
//! created or any requested operation failed, 2 on a usage error.

use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use confluent_kafka::admin::{Admin, AdminClient, AdminClientConfig};
use confluent_kafka::common::{Error, TopicPartition};

/// Why an example stopped early.
pub enum Failure {
    /// The command line was wrong: the message is printed with the usage text.
    Usage(String),
    /// `--help` was given: the usage text is printed and the example succeeds.
    Help,
    /// The client returned an error that ends the example.
    Kafka(Error),
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        Failure::Kafka(error)
    }
}

/// Returns a usage failure with the given message.
pub fn usage<T>(message: impl Into<String>) -> Result<T, Failure> {
    Err(Failure::Usage(message.into()))
}

/// Set by [`fail`] when one of several operations fails, so the example still
/// reports the others and exits with code 1 at the end.
static ANY_FAILED: AtomicBool = AtomicBool::new(false);

/// Prints a per-key failure to stderr and makes the example exit with code 1.
pub fn fail(message: impl Display, error: &Error) {
    ANY_FAILED.store(true, Ordering::Relaxed);
    eprintln!("{message}: {}", describe(error));
}

/// The error's message followed by the messages of its sources.
pub fn describe(error: &Error) -> String {
    let mut text = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        text.push_str(&format!(" (caused by: {cause})"));
        source = cause.source();
    }
    text
}

/// The parsed command line.
pub struct Args {
    config: HashMap<String, String>,
    options: Vec<(String, Option<String>)>,
    positional: Vec<String>,
}

impl Args {
    fn parse(raw: impl Iterator<Item = String>) -> Result<Self, Failure> {
        let mut config = HashMap::new();
        let mut options = Vec::new();
        let mut positional = Vec::new();
        let mut raw = raw.peekable();
        while let Some(arg) = raw.next() {
            if arg == "--help" {
                return Err(Failure::Help);
            } else if arg == "-X" {
                let Some(property) = raw.next() else {
                    return usage("-X needs a key=value argument");
                };
                // Not repeated in the error: it may be a secret such as `sasl.jaas.config`.
                let Some((key, value)) = property.split_once('=') else {
                    return usage("-X needs a key=value argument");
                };
                config.insert(key.to_string(), value.to_string());
            } else if let Some(option) = arg.strip_prefix("--") {
                match option.split_once('=') {
                    Some((name, value)) => options.push((name.to_string(), Some(value.to_string()))),
                    None => options.push((option.to_string(), None)),
                }
            } else {
                positional.push(arg);
            }
        }
        Ok(Self { config, options, positional })
    }

    /// Takes the boolean option `--name`.
    pub fn flag(&mut self, name: &str) -> Result<bool, Failure> {
        let mut found = false;
        for value in self.take(name) {
            if value.is_some() {
                return usage(format!("--{name} takes no value"));
            }
            found = true;
        }
        Ok(found)
    }

    /// Takes the option `--name=value`, if given; the last one wins.
    pub fn value(&mut self, name: &str) -> Result<Option<String>, Failure> {
        Ok(self.values(name)?.pop())
    }

    /// Takes every `--name=value` option, in command-line order.
    pub fn values(&mut self, name: &str) -> Result<Vec<String>, Failure> {
        self.take(name)
            .into_iter()
            .map(|value| value.ok_or_else(|| Failure::Usage(format!("--{name} needs a value: --{name}=..."))))
            .collect()
    }

    /// Returns the positional arguments, failing on any option not taken before.
    pub fn positional(self) -> Result<Vec<String>, Failure> {
        if let Some((name, _)) = self.options.first() {
            return usage(format!("unknown option --{name}"));
        }
        Ok(self.positional)
    }

    fn take(&mut self, name: &str) -> Vec<Option<String>> {
        let (taken, kept) = std::mem::take(&mut self.options)
            .into_iter()
            .partition::<Vec<_>, _>(|(option, _)| option == name);
        self.options = kept;
        taken.into_iter().map(|(_, value)| value).collect()
    }
}

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn create_admin(config: &HashMap<String, String>) -> Result<Arc<dyn Admin>, Error> {
    let mut props = HashMap::from([(AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap_servers())]);
    props.extend(config.iter().map(|(key, value)| (key.clone(), value.clone())));
    Ok(Arc::from(AdminClient::create(AdminClientConfig::new(&props)?)?))
}

/// Parses the command line, creates the admin client, runs the example and
/// closes the client, returning the exit code described in the module docs.
pub async fn main<F, Fut>(usage_text: &str, run: F) -> ExitCode
where
    F: FnOnce(Arc<dyn Admin>, Args) -> Fut,
    Fut: Future<Output = Result<(), Failure>>,
{
    let outcome = match Args::parse(std::env::args().skip(1)) {
        Ok(mut args) => {
            let config = std::mem::take(&mut args.config);
            match create_admin(&config) {
                Ok(admin) => {
                    let outcome = run(Arc::clone(&admin), args).await;
                    admin.close().await;
                    outcome
                },
                Err(error) => Err(Failure::Kafka(error)),
            }
        },
        Err(failure) => Err(failure),
    };
    match outcome {
        Ok(()) if ANY_FAILED.load(Ordering::Relaxed) => ExitCode::FAILURE,
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::Help) => {
            println!("{usage_text}");
            ExitCode::SUCCESS
        },
        Err(Failure::Usage(message)) => {
            eprintln!("{message}\n\n{usage_text}");
            ExitCode::from(2)
        },
        Err(Failure::Kafka(error)) => {
            eprintln!("Error: {}", describe(&error));
            ExitCode::FAILURE
        },
    }
}

/// Parses `text` as a `T`, naming `what` in the usage error.
pub fn parse<T: FromStr>(what: &str, text: &str) -> Result<T, Failure> {
    text.parse().or_else(|_| usage(format!("invalid {what}: {text:?}")))
}

/// Picks the value whose name matches `text`, ignoring case and treating `-`
/// as `_`.
pub fn choose<T: Clone>(what: &str, text: &str, choices: &[(&str, T)]) -> Result<T, Failure> {
    let wanted = text.to_ascii_lowercase().replace('-', "_");
    match choices.iter().find(|(name, _)| *name == wanted) {
        Some((_, value)) => Ok(value.clone()),
        None => {
            let names: Vec<&str> = choices.iter().map(|(name, _)| *name).collect();
            usage(format!("invalid {what}: {text:?}, expected one of {}", names.join(", ")))
        },
    }
}

/// Splits `args` into groups of `size`, failing when they do not divide evenly
/// or, with `required`, when there are none.
pub fn groups<'a>(
    args: &'a [String],
    size: usize,
    required: bool,
) -> Result<std::slice::ChunksExact<'a, String>, Failure> {
    if !args.len().is_multiple_of(size) || (required && args.is_empty()) {
        return usage(format!("expected arguments in groups of {size}"));
    }
    Ok(args.chunks_exact(size))
}

/// Parses `<topic> <partition>` at the start of `group`.
pub fn topic_partition(group: &[String]) -> Result<TopicPartition, Failure> {
    Ok(TopicPartition::new(group[0].clone(), parse("partition", &group[1])?))
}

/// Parses a sequence of `<topic> <partition>` pairs.
pub fn topic_partitions(args: &[String], required: bool) -> Result<Vec<TopicPartition>, Failure> {
    groups(args, 2, required)?.map(topic_partition).collect()
}

/// Splits a comma-separated list, dropping empty items.
pub fn comma_list(text: &str) -> Vec<String> {
    text.split(',').filter(|item| !item.is_empty()).map(str::to_string).collect()
}

/// Lowercase hexadecimal encoding.
pub fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Decodes the output of [`to_hex`]. The error does not repeat `text`, which
/// may be a secret such as a delegation token's HMAC.
pub fn from_hex(text: &str) -> Result<Vec<u8>, Failure> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return usage("invalid hex string");
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).or_else(|_| usage("invalid hex string")))
        .collect()
}
