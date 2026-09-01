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

use std::sync::Arc;

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::KafkaFuture;
use confluent_kafka::common::MetricName;
use confluent_kafka::common::MetricValue;
use confluent_kafka::common::Node;
use confluent_kafka::common::PartitionInfo;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::metrics::{ClosureGauge, KafkaMetric, MetricConfig, MetricValueProvider, SystemTime};
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::consumer::ConsumerGroupMetadata;
use confluent_kafka::consumer::OffsetAndMetadata;
use confluent_kafka::producer::Callback;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerRecord;
use confluent_kafka::producer::RecordMetadata;
use multilanguage_test_server::proto::producer_service_client::ProducerServiceClient;
use multilanguage_test_server::proto::{
    self, CloseRequest, CloseTimeoutRequest, CreateProducerRequest, FlushRequest, MetricsRequest, PartitionsForRequest,
    SendOffsetsToTransactionRequest, SendRequest, TransactionRequest,
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

    /// Map a `StatusResponse` (the reply shared by the flush/close/transaction
    /// control RPCs) to a `Result`: an empty `error` field is success.
    fn status_result(&self, response: proto::StatusResponse) -> Result<(), KafkaError> {
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(()),
        }
    }

    /// Run an async RPC to completion from a sync trait method. Valid on the
    /// multi-thread runtime the multilanguage tests use, mirroring
    /// `MultilanguageConsumer::block`.
    fn block<T>(&self, fut: impl std::future::Future<Output = T>) -> T {
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
    }

    /// The server-local producer id. Needed by
    /// [`crate::common::callback_log::grpc::ProducerLog`], which reads the
    /// server-side delivery-callback log for the same producer.
    pub fn producer_id(&self) -> u64 {
        self.producer_id
    }
}

impl Producer<Vec<u8>, Vec<u8>> for MultilanguageProducer {
    /// Java `initTransactions()` tunneled over gRPC: the server awaits the
    /// producer's `init_transactions` future and returns the resolved
    /// `StatusResponse` (empty on success, a `KafkaError` on failure).
    async fn init_transactions(&self) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let response = client
            .init_transactions(TransactionRequest { producer_id: self.producer_id })
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        self.status_result(response)
    }

    /// Java `beginTransaction()`. This trait method is **sync** (in Java it is a
    /// pure local state transition), but tunneling it still needs a gRPC round
    /// trip, so we drive the async call to completion with [`Self::block`] —
    /// mirroring `MultilanguageConsumer`'s sync state-read methods.
    fn begin_transaction(&self) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let producer_id = self.producer_id;
        let backend = self.backend;
        self.block(async move {
            let response = client
                .begin_transaction(TransactionRequest { producer_id })
                .await
                .map_err(|status| status_to_kafka_error(&status, backend))?
                .into_inner();
            match response.error {
                Some(err) => Err(kafka_error_from_proto(err)),
                None => Ok(()),
            }
        })
    }

    /// Java `sendOffsetsToTransaction(offsets, groupMetadata)` tunneled over
    /// gRPC — the producer half of consume-transform-produce. The offsets and
    /// the consuming group's metadata are marshaled onto the wire; the server
    /// rebuilds a `ConsumerGroupMetadata` handle and drives its own binding's
    /// `send_offsets_to_transaction`. See [`Self::init_transactions`] for the
    /// `StatusResponse` convention.
    async fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: ConsumerGroupMetadata,
    ) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        // The offsets reach the wire, so impose a deterministic order before
        // encoding (producer-transactions.md §10): a `HashMap` iterates
        // nondeterministically, so sort by topic then partition to keep the
        // encoding stable. `leader_epoch` is already normalized to `None` for
        // negatives by `OffsetAndMetadata::leader_epoch`; `metadata` is always
        // present (possibly empty), so it is always sent.
        let mut entries: Vec<proto::OffsetEntry> = offsets
            .into_iter()
            .map(|(tp, oam)| proto::OffsetEntry {
                topic: tp.topic().to_string(),
                partition: tp.partition(),
                offset: oam.offset(),
                leader_epoch: oam.leader_epoch(),
                metadata: Some(oam.metadata().to_string()),
            })
            .collect();
        entries.sort_by(|a, b| a.topic.cmp(&b.topic).then_with(|| a.partition.cmp(&b.partition)));
        let request = SendOffsetsToTransactionRequest {
            producer_id: self.producer_id,
            offsets: entries,
            group_metadata: Some(proto::ConsumerGroupMetadata {
                group_id: group_metadata.group_id().to_string(),
                generation_id: group_metadata.generation_id(),
                member_id: group_metadata.member_id().to_string(),
                group_instance_id: group_metadata.group_instance_id().map(str::to_string),
            }),
        };
        let response = client
            .send_offsets_to_transaction(request)
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        self.status_result(response)
    }

    /// Java `commitTransaction()` tunneled over gRPC — see
    /// [`Self::init_transactions`] for the response convention.
    async fn commit_transaction(&self) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let response = client
            .commit_transaction(TransactionRequest { producer_id: self.producer_id })
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        self.status_result(response)
    }

    /// Java `abortTransaction()` tunneled over gRPC — see
    /// [`Self::init_transactions`] for the response convention.
    async fn abort_transaction(&self) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let response = client
            .abort_transaction(TransactionRequest { producer_id: self.producer_id })
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        self.status_result(response)
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

    /// Wired through to the backend's real registry over the `Metrics` RPC —
    /// this is a live producer in another language, NOT a mock, so reporting an
    /// empty map would misreport its state. Mirrors the consumer backend.
    ///
    /// Each entry is rebuilt locally as a `ClosureGauge` returning the value the
    /// backend measured while serving the RPC. That is snapshot, not live,
    /// semantics — which is exactly what `Producer::metrics` documents.
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        let mut client = self.client.clone();
        let id = self.producer_id;
        let backend = self.backend;
        self.block(async move {
            let resp = client
                .metrics(MetricsRequest { producer_id: id })
                .await
                .map_err(|s| status_to_kafka_error(&s, backend))
                .expect("metrics RPC failed")
                .into_inner();
            match resp.result {
                Some(proto::metrics_response::Result::Metrics(list)) => {
                    list.metrics.into_iter().map(metric_from_proto).collect()
                },
                Some(proto::metrics_response::Result::Error(e)) => {
                    panic!("metrics failed on the {backend} backend: {}", kafka_error_from_proto(e))
                },
                None => HashMap::new(),
            }
        })
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

fn metric_from_proto(m: proto::Metric) -> (MetricName, Arc<KafkaMetric>) {
    let tags: std::collections::BTreeMap<String, String> = m.tags.into_iter().collect();
    let name = MetricName::new(m.name, m.group, m.description, tags);
    // A `None` value means the backend sent no `value` oneof member; report 0.0
    // rather than fabricating a kind.
    let value = match m.value {
        Some(proto::metric::Value::DoubleValue(d)) => MetricValue::Double(d),
        Some(proto::metric::Value::StringValue(s)) => MetricValue::String(s),
        Some(proto::metric::Value::LongValue(l)) => MetricValue::Long(l),
        Some(proto::metric::Value::IntValue(i)) => MetricValue::Int(i),
        None => MetricValue::Double(0.0),
    };
    let gauge = ClosureGauge::new(move |_config, _now| value.clone());
    let metric = KafkaMetric::new(
        name.clone(),
        MetricValueProvider::Gauge(Box::new(gauge)),
        Arc::new(MetricConfig::new()),
        Arc::new(SystemTime),
    );
    (name, Arc::new(metric))
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
