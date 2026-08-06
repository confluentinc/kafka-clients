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
//!      `RecordMetadata` or `KafkaError` reference. Passing a callback also
//!      sets the proto `with_callback` flag, which makes the *server* register
//!      a real delivery callback through its own binding and record each
//!      invocation in a log readable via `GetCallbackLog` — that log, not this
//!      local closure, is what
//!      [`crate::common::callback_log::ProducerCallbackLog`] asserts on.
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
use confluent_kafka::consumer::ConsumerGroupMetadata;
use confluent_kafka::consumer::OffsetAndMetadata;
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

    /// The error every transactional method returns until PLAN §9.6 gives the
    /// harness the corresponding RPCs.
    fn transactions_not_in_harness(&self, operation: &str) -> KafkaError {
        KafkaError::unsupported_version(format!(
            "{} is not available through the {} multilanguage backend (PLAN §9.6)",
            operation, self.backend
        ))
    }

    /// The server-local producer id. Needed by
    /// [`crate::common::callback_log::grpc::ProducerLog`], which reads the
    /// server-side delivery-callback log for the same producer.
    pub fn producer_id(&self) -> u64 {
        self.producer_id
    }
}

impl Producer<Vec<u8>, Vec<u8>> for MultilanguageProducer {
    /// The multilanguage harness has no transactional RPCs: Milestone 11 defers the
    /// C / Python / gRPC transaction surface, tracked as
    /// `design/history/Milestone-11/PLAN.md` §9.6. Returns an explicit error rather
    /// than silently succeeding (CLAUDE.md §5).
    async fn init_transactions(&self) -> Result<(), KafkaError> {
        Err(self.transactions_not_in_harness("initTransactions"))
    }

    /// Not in the harness — see [`Self::init_transactions`].
    fn begin_transaction(&self) -> Result<(), KafkaError> {
        Err(self.transactions_not_in_harness("beginTransaction"))
    }

    /// Not in the harness — see [`Self::init_transactions`].
    async fn send_offsets_to_transaction(
        &self,
        _offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        _group_metadata: ConsumerGroupMetadata,
    ) -> Result<(), KafkaError> {
        Err(self.transactions_not_in_harness("sendOffsetsToTransaction"))
    }

    /// Not in the harness — see [`Self::init_transactions`].
    async fn commit_transaction(&self) -> Result<(), KafkaError> {
        Err(self.transactions_not_in_harness("commitTransaction"))
    }

    /// Not in the harness — see [`Self::init_transactions`].
    async fn abort_transaction(&self) -> Result<(), KafkaError> {
        Err(self.transactions_not_in_harness("abortTransaction"))
    }

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
