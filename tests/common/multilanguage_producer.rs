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

//! A `Producer<Vec<u8>, Vec<u8>>` implementation that tunnels every call
//! over gRPC to a server in another language.
//!
//! Two deliberate simplifications relative to the trait, both captured in
//! `design/history/MILESTONE-6/DESIGN-multilanguage-tests.md`:
//!
//!   1. `KafkaFuture` is not modeled on the wire. The server awaits the
//!      producer's future internally; the unary RPC response is the
//!      resolved future. We wrap it in a [`KafkaFuture::completed`].
//!
//!   2. `send_with_callback`'s closure stays Rust-side. After awaiting the
//!      RPC we synchronously invoke the user's callback with the decoded
//!      `RecordMetadata` or `KafkaError` reference.
//!
//! Used only when `--features multilanguage-tests` is enabled.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::KafkaFuture;
use confluent_kafka::common::Node;
use confluent_kafka::common::PartitionInfo;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::producer::Callback;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;
use confluent_kafka::producer::RecordMetadata;
use multilanguage_test_server::proto::producer_service_client::ProducerServiceClient;
use multilanguage_test_server::proto::{
    self, CloseRequest, CloseTimeoutRequest, CreateProducerRequest, FlushRequest, PartitionsForRequest, SendRequest,
};
use tonic::transport::Channel;

/// gRPC-backed `Producer` whose calls are executed by an out-of-process
/// server in another language (Python or C++) that ultimately drives the
/// same Rust client through a language binding.
pub struct MultilanguageProducer {
    /// Server-local producer handle returned by `CreateProducer`.
    producer_id: u64,
    /// Cloned per RPC; tonic clients share their underlying connection.
    client: ProducerServiceClient<Channel>,
    /// Backend label (e.g. "python", "c") used in panic messages and logs.
    backend: &'static str,
}

impl MultilanguageProducer {
    /// Connect to the gRPC server at `channel` and create a server-side
    /// producer with the given `config`. The `config` map is forwarded
    /// verbatim to the server, which uses it to construct a `KafkaProducer`
    /// (or, if empty, a `MockProducer` for client-side smoke testing).
    pub async fn new(
        channel: Channel,
        config: HashMap<String, String>,
        backend: &'static str,
    ) -> Result<Self, KafkaError> {
        let mut client = ProducerServiceClient::new(channel);
        let response = client
            .create_producer(CreateProducerRequest { config })
            .await
            .map_err(|status| status_to_kafka_error(&status, backend))?
            .into_inner();
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(Self { producer_id: response.producer_id, client, backend })
    }
}

impl Producer<Vec<u8>, Vec<u8>> for MultilanguageProducer {
    async fn send(&self, record: ProducerRecord<Vec<u8>, Vec<u8>>) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        self.send_with_callback(record, None).await
    }

    async fn send_with_callback(
        &self,
        record: ProducerRecord<Vec<u8>, Vec<u8>>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, KafkaError> {
        let mut client = self.client.clone();
        let request = SendRequest {
            producer_id: self.producer_id,
            record: Some(producer_record_to_proto(record)),
            with_callback: callback.is_some(),
        };
        let response = client
            .send(request)
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        let result = match response.result {
            Some(proto::send_response::Result::Metadata(m)) => Ok(record_metadata_from_proto(m)),
            Some(proto::send_response::Result::Error(e)) => Err(kafka_error_from_proto(e)),
            None => Err(KafkaError::illegal_state(format!(
                "{} backend returned empty SendResponse",
                self.backend
            ))),
        };
        // Local invocation: the server doesn't call back across the wire;
        // we run the user's closure here with the decoded result.
        if let Some(cb) = callback {
            match &result {
                Ok(metadata) => cb(Some(metadata), None),
                Err(err) => cb(None, Some(err)),
            }
        }
        Ok(KafkaFuture::completed(result))
    }

    async fn flush(&self) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let response = client
            .flush(FlushRequest { producer_id: self.producer_id })
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(()),
        }
    }

    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        let mut client = self.client.clone();
        let response = client
            .partitions_for(PartitionsForRequest { producer_id: self.producer_id, topic: topic.to_string() })
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(response.partitions.into_iter().map(partition_info_from_proto).collect())
    }

    async fn close(&self) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let response = client
            .close(CloseRequest { producer_id: self.producer_id })
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(()),
        }
    }

    async fn close_timeout(&self, timeout: Duration) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let timeout_ms = i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX);
        let response = client
            .close_timeout(CloseTimeoutRequest { producer_id: self.producer_id, timeout_ms })
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire <-> domain conversions
// ---------------------------------------------------------------------------

fn producer_record_to_proto(record: ProducerRecord<Vec<u8>, Vec<u8>>) -> proto::ProducerRecord {
    let (topic, partition, timestamp, headers, key, value) = record.into_parts();
    // Iterate inline so we don't have to name the internal RecordHeaders type;
    // the public Header trait gives us key() and value() accessors.
    let headers = (&headers)
        .into_iter()
        .map(|h| proto::Header {
            key: h.key().to_string(),
            value: h.value().map(|v| v.to_vec()).unwrap_or_default(),
        })
        .collect();
    proto::ProducerRecord { topic, partition, timestamp, key, value, headers }
}

fn record_metadata_from_proto(m: proto::RecordMetadata) -> RecordMetadata {
    // The proto carries the resolved offset (base + batch_index). RecordMetadata::new
    // computes offset as base_offset + batch_index, so we pass the resolved value as
    // base_offset with batch_index = 0.
    RecordMetadata::new(
        TopicPartition::new(m.topic, m.partition),
        m.offset,
        0,
        m.timestamp,
        m.serialized_key_size,
        m.serialized_value_size,
    )
}

pub(crate) fn partition_info_from_proto(p: proto::PartitionInfo) -> PartitionInfo {
    PartitionInfo::with_offline_replicas(
        p.topic,
        p.partition,
        p.leader.map(node_from_proto),
        p.replicas.into_iter().map(node_from_proto).collect(),
        p.in_sync_replicas.into_iter().map(node_from_proto).collect(),
        p.offline_replicas.into_iter().map(node_from_proto).collect(),
    )
}

pub(crate) fn node_from_proto(n: proto::Node) -> Node {
    match n.rack {
        Some(rack) => Node::with_rack(n.id, n.host, n.port, Some(rack)),
        None => Node::new(n.id, n.host, n.port),
    }
}

/// Rebuilds a [`KafkaError`] from its wire form.
///
/// # Standing limitation: a guessed variant can shadow the transported code
///
/// The C FFI does not expose the Rust `KafkaError` discriminator, so **both**
/// gRPC servers infer `variant` from the message text (`guess_variant` in
/// `server.cc`, `_guess_variant` in `grpc_translate.py` — deliberately identical,
/// see the comment on the former). Slice G1 made that guess apply to every error
/// rather than to two producer call sites, which fixed a real 1-vs-3 divergence
/// but widened a second one that is still open:
///
/// five of the variants below (`IllegalArgument`, `IllegalState`, `Timeout`,
/// `RecordTooLarge`, `Serialization`) map to `KafkaError` cases that carry **no**
/// `Errors` slot, so reconstructing one of them *discards* `p.code`, and
/// `error()` then reports `UnknownServerError` with `code() == -1`. Some broker
/// errors' own default messages match a guess pattern —
/// `Errors::RequestTimedOut`'s is literally `"The request timed out."`, which
/// contains `"timed out"` — so such an error arrives on a gRPC backend as
/// `Timeout(-1)` where the native backend reports `RequestTimedOut(7)`. The same
/// applies to `TopicAuthorizationFailed`, `InvalidTopicException`,
/// `GroupAuthorizationFailed`, `OffsetMetadataTooLarge`,
/// `DelegationTokenExpired` and `PrincipalDeserializationFailure`; for the three
/// authorization/invalid-topic variants `message` is dropped as well, and neither
/// server populates the `unauthorized_topics` / `invalid_topics` / `group_id`
/// payloads those variants would need.
///
/// **Consequence for scenario authors:** an assertion of the form
/// `err.error() == Errors::X` is only sound on all four backends if `X`'s message
/// does not match a `guess_variant` pattern. Slice G3's three such assertions
/// (`ElectionNotNeeded`, `NoReassignmentInProgress`, `UnsupportedVersion`) were
/// each confirmed green on the C and both Python backends, which is what
/// establishes that their messages do not trip the guess. A new one that fails on
/// the gRPC backends alone should be checked against this list before being
/// treated as a client defect.
///
/// The principled fix is to prefer the transported `code` over a guessed
/// code-less variant when the code is a real broker code, but that changes error
/// reconstruction for all 34 guessing sites across the producer, consumer and
/// admin suites, so it is reported rather than made from inside the admin slice
/// (`PLAN-multilanguage-admin.md` §0).
pub(crate) fn kafka_error_from_proto(p: proto::KafkaError) -> KafkaError {
    use proto::kafka_error::Variant;
    let variant = Variant::try_from(p.variant).unwrap_or(Variant::Generic);
    let errors = errors_from_code(p.code);
    match variant {
        Variant::Generic => {
            if p.is_fatal {
                KafkaError::fatal(errors, p.message)
            } else {
                KafkaError::with_message(errors, p.message)
            }
        },
        Variant::TopicAuthorization => KafkaError::topic_authorization(p.unauthorized_topics.into_iter().collect()),
        Variant::InvalidTopic => KafkaError::invalid_topics(p.invalid_topics.into_iter().collect()),
        Variant::GroupAuthorization => KafkaError::group_authorization(p.group_id.unwrap_or_default()),
        Variant::BufferExhausted => KafkaError::buffer_exhausted(p.message),
        Variant::IllegalArgument => KafkaError::illegal_argument(p.message),
        Variant::IllegalState => KafkaError::illegal_state(p.message),
        Variant::Timeout => KafkaError::timeout(p.message),
        Variant::RecordTooLarge => KafkaError::record_too_large(p.message),
        Variant::Serialization => KafkaError::serialization(p.message),
    }
}

/// Best-effort `i32` → `Errors` mapping. Falls back to `UnknownServerError`
/// when the code is out of `i16` range. The variant discriminator already
/// carries the type-level information; the code is informational, and
/// `Errors::for_code` itself returns `UnknownServerError` for unrecognised
/// codes.
fn errors_from_code(code: i32) -> Errors {
    match i16::try_from(code) {
        Ok(c) => Errors::for_code(c),
        Err(_) => Errors::UnknownServerError,
    }
}

/// Map a tonic transport-level failure to a `KafkaError`. These are
/// gRPC-layer problems (connection refused, server crashed mid-call, etc.)
/// that aren't produced by a real Kafka client; surfacing them as
/// `IllegalState` makes failures visible without conflating with broker
/// errors.
pub(crate) fn status_to_kafka_error(status: &tonic::Status, backend: &'static str) -> KafkaError {
    KafkaError::illegal_state(format!(
        "{} gRPC backend transport error ({:?}): {}",
        backend,
        status.code(),
        status.message()
    ))
}
