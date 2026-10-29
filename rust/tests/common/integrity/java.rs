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

//! The final read-back done by Apache Kafka's Java client instead of the Rust
//! one: `kafka-console-consumer.sh`, run in a broker container, reads every
//! topic from the start (read_committed) and each record it prints goes to the
//! [`Ledger`] exactly as a Rust-read record does, so the Java client is held
//! to every check of a run: digest, exact count, order, aborted transactions.
//!
//! The tool prints a record on one line. With `BytesDeserializer` for the key,
//! the value and the header values it prints `Bytes.toString()`, which keeps
//! printable ASCII but `\` and writes every other byte as `\xHH`. No escaped
//! field can then hold a tab (the field separator), a newline or the header
//! separator (`\x1f`), and each one is decoded back to its exact bytes.

use std::time::Duration;

use confluent_kafka::common::header::{RecordHeader, RecordHeaders};
use confluent_kafka::consumer::{ConsumerRecord, ConsumerRecordOptionsBuilder};

use super::checker::{Event, Ledger};
use super::workload::TopicSpec;

/// Where the Java console consumer runs: a broker container, and the
/// bootstrap servers it reaches the brokers at from inside it.
#[derive(Clone, Debug)]
pub struct JavaReader {
    pub container: String,
    pub bootstrap: String,
}

/// What the tool prints for a null key, value or header value. A record
/// holding these very bytes would be misread as null, and fail its digest.
const NULL_LITERAL: &str = "<null-7f3a91c2>";

/// The tool exits once no record arrives for this long.
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Reads every partition of `topics` with the Java console consumer and
/// reports each record to `ledger`. Returns whether every read ran to its end;
/// a record it did not print is then reported lost.
pub async fn java_final_read_back(reader: &JavaReader, topics: &[TopicSpec], ledger: &Ledger) -> bool {
    let mut complete = true;
    for topic in topics {
        let deserializer = "org.apache.kafka.common.serialization.BytesDeserializer";
        // `stdout.encoding`: header keys are printed as strings (`hé`).
        let script = format!(
            "KAFKA_OPTS=-Dstdout.encoding=UTF-8 timeout 600 /opt/kafka/bin/kafka-console-consumer.sh \
             --bootstrap-server {bootstrap} --topic {name} --from-beginning \
             --isolation-level read_committed --timeout-ms {idle} \
             --property print.timestamp=true --property print.partition=true \
             --property print.offset=true --property print.headers=true --property print.key=true \
             --property key.deserializer={deserializer} --property value.deserializer={deserializer} \
             --property headers.deserializer={deserializer} \
             --property headers.separator=$'\\x1f' --property null.literal='{NULL_LITERAL}'",
            bootstrap = reader.bootstrap,
            name = topic.name,
            idle = IDLE_TIMEOUT.as_millis(),
        );
        let container = reader.container.clone();
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new("docker")
                .args(["exec", &container, "bash", "-c", &script])
                .output()
        })
        .await
        .expect("docker exec task");
        let output = match output {
            Ok(output) if output.status.success() => output,
            Ok(output) => {
                ledger.event(
                    Event::ConsumerError,
                    format!(
                        "Java read-back of {}: {}: {}",
                        topic.name,
                        output.status,
                        String::from_utf8_lossy(&output.stderr).trim()
                    ),
                );
                complete = false;
                continue;
            },
            Err(e) => {
                ledger.event(Event::ConsumerError, format!("Java read-back of {}: {e}", topic.name));
                complete = false;
                continue;
            },
        };
        for line in output.stdout.split(|b| *b == b'\n') {
            // The tool also prints notices (a deprecated option) on stdout.
            if !line.starts_with(b"CreateTime:") {
                continue;
            }
            match parse_line(&topic.name, line) {
                Ok(record) => ledger.final_record(&record),
                Err(e) => {
                    ledger.event(
                        Event::ConsumerError,
                        format!("Java read-back of {}: {e}: {}", topic.name, String::from_utf8_lossy(line)),
                    );
                    complete = false;
                },
            }
        }
    }
    complete
}

/// `CreateTime:<ts> TAB Partition:<p> TAB Offset:<o> TAB <headers> TAB <key> TAB <value>`,
/// the headers `NO_HEADERS` or `<key>:<value>` joined by `\x1f`.
fn parse_line(topic: &str, line: &[u8]) -> Result<ConsumerRecord<Vec<u8>, Vec<u8>>, String> {
    let fields: Vec<&[u8]> = line.split(|b| *b == b'\t').collect();
    let [timestamp, partition, offset, headers, key, value] = fields[..] else {
        return Err(format!("{} fields, expected 6", fields.len()));
    };
    let number = |field: &[u8], prefix: &str| -> Result<i64, String> {
        std::str::from_utf8(field)
            .ok()
            .and_then(|f| f.strip_prefix(prefix))
            .and_then(|n| n.parse().ok())
            .ok_or_else(|| format!("no {prefix}<number>"))
    };
    let headers = if headers == b"NO_HEADERS" {
        Vec::new()
    } else {
        headers
            .split(|b| *b == 0x1f)
            .map(|header| {
                let colon = header.iter().position(|b| *b == b':').ok_or("a header without ':'")?;
                let key = std::str::from_utf8(&header[..colon]).map_err(|e| format!("header key: {e}"))?;
                Ok(RecordHeader::new(key.to_string(), unescape(&header[colon + 1..])?))
            })
            .collect::<Result<Vec<_>, String>>()?
    };
    let options = ConsumerRecordOptionsBuilder::new()
        .set_topic(topic)
        .set_partition(number(partition, "Partition:")? as i32)
        .set_offset(number(offset, "Offset:")?)
        .set_timestamp(number(timestamp, "CreateTime:")?)
        .set_key(unescape(key)?)
        .set_value(unescape(value)?)
        .set_headers(RecordHeaders::with_header_iter(headers))
        .build()
        .map_err(|e| e.to_string())?;
    Ok(ConsumerRecord::with_options(options))
}

/// The bytes `Bytes.toString()` printed as `field`; `None` for the null
/// literal.
fn unescape(field: &[u8]) -> Result<Option<Vec<u8>>, String> {
    if field == NULL_LITERAL.as_bytes() {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(field.len());
    let mut i = 0;
    while i < field.len() {
        if field[i] == b'\\' {
            let hex = field
                .get(i + 1..i + 4)
                .filter(|h| h[0] == b'x')
                .and_then(|h| std::str::from_utf8(&h[1..]).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or_else(|| format!("a bad escape at byte {i}"))?;
            out.push(hex);
            i += 4;
        } else {
            out.push(field[i]);
            i += 1;
        }
    }
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use confluent_kafka::common::header::{Header, Headers};

    use super::*;

    /// Java's `Bytes.toString()`.
    fn java_escape(bytes: &[u8]) -> Vec<u8> {
        bytes
            .iter()
            .flat_map(|&b| {
                if (b' '..=b'~').contains(&b) && b != b'\\' {
                    vec![b]
                } else {
                    format!("\\x{b:02X}").into_bytes()
                }
            })
            .collect()
    }

    #[test]
    fn test_every_byte_decodes_back() {
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(unescape(&java_escape(&all)).unwrap(), Some(all));
        assert_eq!(unescape(b"").unwrap(), Some(Vec::new()));
        assert_eq!(unescape(NULL_LITERAL.as_bytes()).unwrap(), None);
        assert!(unescape(b"\\x4").is_err());
        assert!(unescape(b"\\y41").is_err());
    }

    #[test]
    fn test_a_printed_line_parses_back() {
        let line = b"CreateTime:42\tPartition:3\tOffset:7\th0:\\x00a\x1fh\xc3\xa9:<null-7f3a91c2>\t<null-7f3a91c2>\t\\x5C\\x09";
        let record = parse_line("t", line).unwrap();
        assert_eq!((record.partition(), record.offset(), record.timestamp()), (3, 7, 42));
        assert_eq!(record.key(), None);
        assert_eq!(record.value(), Some(&b"\\\t".to_vec()));
        let headers = record.headers().to_array();
        assert_eq!(headers.len(), 2);
        assert_eq!((headers[0].key(), headers[0].value()), ("h0", Some(&b"\x00a"[..])));
        assert_eq!((headers[1].key(), headers[1].value()), ("hé", None));
        assert!(parse_line("t", b"CreateTime:42\tPartition:3").is_err());
    }
}
