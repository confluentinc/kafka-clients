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
//!      `RecordMetadata` or `Error` reference. Passing a callback also
//!      sets the proto `with_callback` flag, which makes the *server* register
//!      a real delivery callback through its own binding and record each
//!      invocation in a log readable via `GetCallbackLog` — that log, not this
//!      local closure, is what
//!      [`crate::common::callback_log::ProducerCallbackLog`] asserts on.
//!
//! Used only when `--features multilanguage-tests` is enabled.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use confluent_kafka::common::Error;
use confluent_kafka::common::KafkaFuture;
use confluent_kafka::common::MetricName;
use confluent_kafka::common::MetricValue;
use confluent_kafka::common::Node;
use confluent_kafka::common::PartitionInfo;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::errors::ApiError;
use confluent_kafka::common::errors::AuthenticationError;
use confluent_kafka::common::errors::AuthorizationError;
use confluent_kafka::common::errors::AuthorizerNotReadyError;
use confluent_kafka::common::errors::BootstrapResolutionError;
use confluent_kafka::common::errors::DisconnectError;
use confluent_kafka::common::errors::GroupAuthorizationError;
use confluent_kafka::common::errors::InterruptError;
use confluent_kafka::common::errors::InvalidOffsetError;
use confluent_kafka::common::errors::SslAuthenticationError;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::metrics::{KafkaMetric, Metrics, MetricsView, gauge};
use confluent_kafka::consumer::ConsumerCommitFailedError;
use confluent_kafka::consumer::ConsumerGroupMetadata;
use confluent_kafka::consumer::ConsumerNoOffsetForPartitionError;
use confluent_kafka::consumer::ConsumerOffsetOutOfRangeError;
use confluent_kafka::consumer::ConsumerRetriableCommitFailedError;
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

use crate::common::multilanguage_consumer::remote_group_metadata_id;

/// gRPC-backed `Producer` whose calls are executed by an out-of-process
/// server in another language (Python or C++) that ultimately drives the
/// same Rust client through a language binding.
pub struct MultilanguageProducer {
    /// Server-local producer handle returned by `CreateProducer`.
    producer_id: u64,
    /// Cloned per RPC; tonic clients share their underlying connection.
    client: ProducerServiceClient<Channel>,
    /// The same server over a channel without the 30 s per-request timeout,
    /// for the blocking RPCs, each of which sets its own deadline: see
    /// [`blocking_deadline`].
    blocking_client: ProducerServiceClient<Channel>,
    /// [`blocking_deadline`] of this producer's config.
    blocking_deadline: Duration,
    /// Backend label (e.g. "python", "c") used in panic messages and logs.
    backend: &'static str,
}

impl MultilanguageProducer {
    /// Connect to the gRPC server at `channel` and create a server-side
    /// producer with the given `config`. The `config` map is forwarded
    /// verbatim to the server, which uses it to construct a `KafkaProducer`
    /// (or, if empty, a `MockProducer` for client-side smoke testing).
    /// `untimed_channel` reaches the same server with no per-request timeout;
    /// the blocking RPCs go over it.
    pub async fn new(
        channel: Channel,
        untimed_channel: Channel,
        config: HashMap<String, String>,
        backend: &'static str,
    ) -> Result<Self, Error> {
        let blocking_deadline = blocking_deadline(&config);
        let mut client = ProducerServiceClient::new(channel);
        let response = client
            .create_producer(CreateProducerRequest { config })
            .await
            .map_err(|status| status_to_kafka_error(&status, backend))?
            .into_inner();
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(Self {
            producer_id: response.producer_id,
            client,
            blocking_client: ProducerServiceClient::new(untimed_channel),
            blocking_deadline,
            backend,
        })
    }

    /// `message` as a request that may run for `deadline`, for the
    /// blocking client.
    fn blocking_request<T>(message: T, deadline: Duration) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(deadline);
        request
    }

    /// Map a `StatusResponse` (the reply shared by the flush/close/transaction
    /// control RPCs) to a `Result`: an empty `error` field is success.
    fn status_result(&self, response: proto::StatusResponse) -> Result<(), Error> {
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
    /// `StatusResponse` (empty on success, an `Error` on failure).
    async fn init_transactions(&self) -> Result<(), Error> {
        let mut client = self.blocking_client.clone();
        let response = client
            .init_transactions(Self::blocking_request(
                TransactionRequest { producer_id: self.producer_id },
                self.blocking_deadline,
            ))
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        self.status_result(response)
    }

    /// Java `beginTransaction()`. This trait method is **sync** (in Java it is a
    /// pure local state transition), but tunneling it still needs a gRPC round
    /// trip, so we drive the async call to completion with [`Self::block`] —
    /// mirroring `MultilanguageConsumer`'s sync state-read methods.
    fn begin_transaction(&self) -> Result<(), Error> {
        let mut client = self.blocking_client.clone();
        let producer_id = self.producer_id;
        let backend = self.backend;
        let deadline = self.blocking_deadline;
        self.block(async move {
            let response = client
                .begin_transaction(Self::blocking_request(TransactionRequest { producer_id }, deadline))
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
    /// gRPC — the producer half of consume-transform-produce. The offsets are
    /// marshaled onto the wire. The group metadata must come from a consumer on
    /// the same backend: it travels as the id of the server-side handle that
    /// consumer handed out, and the server passes that handle to its own
    /// binding's `send_offsets_to_transaction`. Other metadata returns an
    /// `illegal_state` error without an RPC. See [`Self::init_transactions`] for
    /// the `StatusResponse` convention.
    async fn send_offsets_to_transaction(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        group_metadata: &dyn ConsumerGroupMetadata,
    ) -> Result<(), Error> {
        let group_metadata_id = remote_group_metadata_id(group_metadata, self.backend)?;
        let mut client = self.blocking_client.clone();
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
        let request =
            SendOffsetsToTransactionRequest { producer_id: self.producer_id, offsets: entries, group_metadata_id };
        let response = client
            .send_offsets_to_transaction(Self::blocking_request(request, self.blocking_deadline))
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        self.status_result(response)
    }

    /// Java `commitTransaction()` tunneled over gRPC — see
    /// [`Self::init_transactions`] for the response convention.
    async fn commit_transaction(&self) -> Result<(), Error> {
        let mut client = self.blocking_client.clone();
        let response = client
            .commit_transaction(Self::blocking_request(
                TransactionRequest { producer_id: self.producer_id },
                self.blocking_deadline,
            ))
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        self.status_result(response)
    }

    /// Java `abortTransaction()` tunneled over gRPC — see
    /// [`Self::init_transactions`] for the response convention.
    async fn abort_transaction(&self) -> Result<(), Error> {
        let mut client = self.blocking_client.clone();
        let response = client
            .abort_transaction(Self::blocking_request(
                TransactionRequest { producer_id: self.producer_id },
                self.blocking_deadline,
            ))
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        self.status_result(response)
    }

    async fn send(&self, record: ProducerRecord<Vec<u8>, Vec<u8>>) -> Result<KafkaFuture<RecordMetadata>, Error> {
        self.send_with_callback(record, None).await
    }

    async fn send_with_callback(
        &self,
        record: ProducerRecord<Vec<u8>, Vec<u8>>,
        callback: Option<Box<dyn Callback>>,
    ) -> Result<KafkaFuture<RecordMetadata>, Error> {
        let mut client = self.blocking_client.clone();
        let request = SendRequest {
            producer_id: self.producer_id,
            record: Some(producer_record_to_proto(&record)),
            with_callback: callback.is_some(),
        };
        let response = client
            .send(Self::blocking_request(request, self.blocking_deadline))
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        // The send call itself failed (Java's `send` throwing): the native
        // producer returns `Err` without firing the callback, and so does this.
        let raised_by_send = response.raised_by_send;
        let result = match response.result {
            Some(proto::send_response::Result::Error(e)) if raised_by_send => return Err(kafka_error_from_proto(e)),
            Some(proto::send_response::Result::Metadata(m)) => Ok(record_metadata_from_proto(m)),
            Some(proto::send_response::Result::Error(e)) => Err(kafka_error_from_proto(e)),
            None => Err(Error::local_illegal_state(format!(
                "{} backend returned empty SendResponse",
                self.backend
            ))),
        };
        // Local invocation: the server doesn't call back across the wire;
        // we run the user's closure here with the decoded result.
        if let Some(cb) = callback {
            match &result {
                Ok(metadata) => cb.on_completion(Some(metadata), None),
                Err(err) => cb.on_completion(None, Some(err)),
            }
        }
        Ok(KafkaFuture::completed_future(result))
    }

    async fn flush(&self) -> Result<(), Error> {
        let mut client = self.blocking_client.clone();
        let response = client
            .flush(Self::blocking_request(
                FlushRequest { producer_id: self.producer_id },
                self.blocking_deadline,
            ))
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(()),
        }
    }

    async fn partitions_for(&self, topic: &str) -> Result<Vec<PartitionInfo>, Error> {
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
    /// Each entry is rebuilt locally as a `gauge::new` gauge returning the value the
    /// backend measured while serving the RPC. That is snapshot, not live,
    /// semantics: a live cross-process view would need an RPC per read.
    fn metrics(&self) -> MetricsView {
        let mut client = self.client.clone();
        let id = self.producer_id;
        let backend = self.backend;
        MetricsView::from(self.block(async move {
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
        }))
    }

    async fn close(&self) -> Result<(), Error> {
        let mut client = self.blocking_client.clone();
        let response = client
            .close(Self::blocking_request(
                CloseRequest { producer_id: self.producer_id },
                self.blocking_deadline,
            ))
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(()),
        }
    }

    async fn close_with_timeout(&self, timeout: Duration) -> Result<(), Error> {
        let mut client = self.blocking_client.clone();
        let timeout_ms = i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX);
        // The call's own timeout plus the margin, but no longer than an
        // untimed `close()` may take, and no shorter than the default.
        let deadline = timeout
            .saturating_add(BLOCKING_DEADLINE_MARGIN)
            .clamp(DEFAULT_RPC_DEADLINE, self.blocking_deadline);
        // `close_timeout` here is the tonic-generated client method for
        // `rpc CloseTimeout`, NOT the `Producer` trait method above — the
        // proto RPC name is the wire contract and does not follow §2's
        // `_with_` rule, so the two spellings differ deliberately.
        let response = client
            .close_timeout(Self::blocking_request(
                CloseTimeoutRequest { producer_id: self.producer_id, timeout_ms },
                deadline,
            ))
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

fn producer_record_to_proto(record: &ProducerRecord<Vec<u8>, Vec<u8>>) -> proto::ProducerRecord {
    // Iterate inline so we don't have to name the internal RecordHeaders type;
    // the public Header trait gives us key() and value() accessors.
    let headers = record
        .headers()
        .into_iter()
        .map(|h| proto::Header {
            key: h.key().to_string(),
            value: h.value().map(|v| v.to_vec()).unwrap_or_default(),
        })
        .collect();
    proto::ProducerRecord {
        topic: record.topic().to_string(),
        partition: record.partition(),
        timestamp: record.timestamp(),
        key: record.key().cloned(),
        value: record.value().cloned(),
        headers,
    }
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
    let provider = gauge::new(move |_config, _now| value.clone());
    // A standalone registry mints the `KafkaMetric`, as Java code obtains one
    // (its constructor is public only "for testing"); the metric outlives it.
    let metric = Metrics::new().add_metric_if_absent(name.clone(), None, Box::new(provider));
    (name, metric)
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

/// The per-request timeout of [`BackendHandle::channel`](crate::common::backend_pool::BackendHandle::channel),
/// which every other RPC keeps.
const DEFAULT_RPC_DEADLINE: Duration = Duration::from_secs(30);

/// What a blocking RPC's deadline adds to the longest wait its call may
/// legitimately make, for the server's own work and the round trip.
const BLOCKING_DEADLINE_MARGIN: Duration = Duration::from_secs(30);

/// The deadline of the producer's blocking RPCs (`send`, `flush`, the
/// transaction calls, `close`): the longest wait the native call may
/// legitimately make under `config`, plus [`BLOCKING_DEADLINE_MARGIN`], and
/// never less than [`DEFAULT_RPC_DEADLINE`].
///
/// The longest wait is the largest of `max.block.ms` (the metadata wait of
/// `send` and the transaction calls), `request.timeout.ms` (an in-flight
/// request `close()` waits out, as for a fenced producer) and
/// `delivery.timeout.ms` (a record `flush()` waits on), each at Java's
/// default (60 s, 30 s, 120 s) when the config does not set it.
fn blocking_deadline(config: &HashMap<String, String>) -> Duration {
    let ms = |key: &str, default: u64| config.get(key).and_then(|v| v.trim().parse::<u64>().ok()).unwrap_or(default);
    let longest = ms("max.block.ms", 60_000)
        .max(ms("request.timeout.ms", 30_000))
        .max(ms("delivery.timeout.ms", 120_000));
    Duration::from_millis(longest)
        .saturating_add(BLOCKING_DEADLINE_MARGIN)
        .max(DEFAULT_RPC_DEADLINE)
}

/// Rebuilds an [`Error`] from its wire form, with its cause.
///
/// The cause (the proto's `cause`, one level deep) is rebuilt from its code and
/// message and attached where the error's class takes one: a bare
/// `KafkaException`, as Java's `KafkaException(String, Throwable)`, so
/// [`Error::source`] answers as the native error's does. For any other class
/// the cause is not attached, so a test asserting on `source()` there sees
/// `None` and fails, rather than passing on a rebuilt guess.
pub(crate) fn kafka_error_from_proto(mut p: proto::KafkaError) -> Error {
    use crate::common::error_code::UNKNOWN_SERVER_ERROR;

    let cause = p.cause.take().map(|cause| {
        kafka_error_without_cause(proto::KafkaError {
            code: cause.code,
            message: cause.message,
            bare_kafka_error: cause.bare_kafka_error,
            ..Default::default()
        })
    });
    match cause {
        Some(cause) if p.code == UNKNOWN_SERVER_ERROR && p.bare_kafka_error => {
            Error::kafka_message_source(p.message, cause)
        },
        _ => kafka_error_without_cause(p),
    }
}

/// [`kafka_error_from_proto`] for the error itself, its cause left aside.
fn kafka_error_without_cause(p: proto::KafkaError) -> Error {
    use crate::common::error_code::*;

    // `code` is the discriminator. It is the FFI error code
    // (`kafka_common_Error_e`), which is injective over the client's error
    // classes except for -1, shared by the bare `KafkaException` and
    // `UnknownServerException` and told apart by `bare_kafka_error` — that is
    // why the proto no longer carries a `variant` field and neither server
    // hand-matches message text any more.
    //
    // `is_retriable` / `is_fatal` are gone with it: both sides derive them from
    // the code (`Error::is_retriable_error`,
    // `request_utils::RequestUtils::is_fatal_error`), so carrying them over the wire only
    // created a second, divergeable source of truth.
    match p.code {
        // Classes whose payload the proto transports. Java's constructors take
        // the payload *and* a message, so both survive; the no-message
        // constructors would drop `p.message` on the floor.
        TOPIC_AUTHORIZATION_FAILED => {
            Error::topic_authorization_message(p.unauthorized_topics.into_iter().collect(), p.message)
        },
        INVALID_TOPIC_ERROR => Error::invalid_topics_message(p.invalid_topics.into_iter().collect(), p.message),
        GROUP_AUTHORIZATION_FAILED => match p.group_id {
            Some(group_id) => Error::group_authorization_with_message(group_id, p.message),
            None => Error::GroupAuthorization(GroupAuthorizationError::new(p.message)),
        },

        // Classes only the client raises, so `Errors` has no code for them and
        // they carry an FFI-local negative. Each holds nothing but a message,
        // so the code plus `p.message` reconstructs it exactly.
        LOCAL_CONCURRENT_MODIFICATION => Error::local_concurrent_modification(p.message),
        LOCAL_ILLEGAL_ARGUMENT => Error::local_illegal_argument(p.message),
        LOCAL_ILLEGAL_STATE => Error::local_illegal_state(p.message),
        LOCAL_TIMEOUT => Error::local_timeout(p.message),
        LOCAL_UNSUPPORTED_OPERATION => Error::local_unsupported_operation(p.message),
        API => Error::Api(ApiError::new(p.message)),
        AUTHENTICATION => Error::Authentication(AuthenticationError::new(p.message)),
        AUTHORIZATION => Error::Authorization(AuthorizationError::new(p.message)),
        AUTHORIZER_NOT_READY => Error::AuthorizerNotReady(AuthorizerNotReadyError::new(p.message)),
        CONFIG => Error::config_message(p.message),
        DISCONNECT => Error::Disconnect(DisconnectError::new(p.message)),
        INTERRUPT => Error::Interrupt(InterruptError::new(p.message)),
        INVALID_OFFSET => Error::InvalidOffset(InvalidOffsetError::new(p.message)),
        SCHEMA => Error::schema(p.message),
        SERIALIZATION => Error::serialization(p.message),
        SSL_AUTHENTICATION => Error::SslAuthentication(SslAuthenticationError::new(p.message)),
        TRANSACTION_ABORTED => Error::transaction_aborted_message(p.message),
        WAKEUP => Error::wakeup(p.message),
        CONSUMER_COMMIT_FAILED => Error::ConsumerCommitFailed(ConsumerCommitFailedError::new(p.message)),
        CONSUMER_RETRIABLE_COMMIT_FAILED => {
            Error::ConsumerRetriableCommitFailed(ConsumerRetriableCommitFailedError::new(p.message))
        },
        INVALID_RECEIVE => Error::invalid_receive(p.message),
        PRODUCER_BUFFER_EXHAUSTED => Error::buffer_exhausted(p.message),
        BOOTSTRAP_RESOLUTION => Error::BootstrapResolution(BootstrapResolutionError::new(p.message)),
        // Code -1 is shared by Java's bare `KafkaException` and its
        // `ApiException` subclass `UnknownServerException`; the server says
        // which it is.
        UNKNOWN_SERVER_ERROR if p.bare_kafka_error => Error::kafka_message(p.message),
        CONSUMER_OFFSET_OUT_OF_RANGE => Error::ConsumerOffsetOutOfRange(ConsumerOffsetOutOfRangeError::with_message(
            p.message,
            p.offset_out_of_range_partitions
                .into_iter()
                .map(|e| {
                    let tp = e.partition.unwrap_or_default();
                    (TopicPartition::new(tp.topic, tp.partition), e.offset)
                })
                .collect(),
        )),
        // Its public constructor builds the message from the partitions, so the
        // class is rebuilt only when that reproduces the server's message
        // exactly; otherwise it falls back to UNKNOWN_SERVER_ERROR with the
        // server's text, as for the other payload classes below, so a
        // difference shows instead of being papered over.
        CONSUMER_NO_OFFSET_FOR_PARTITION if !p.partitions.is_empty() => {
            let rebuilt = Error::ConsumerNoOffsetForPartition(ConsumerNoOffsetForPartitionError::with_partitions(
                p.partitions
                    .iter()
                    .map(|tp| TopicPartition::new(tp.topic.clone(), tp.partition)),
            ));
            if rebuilt.message() == p.message {
                rebuilt
            } else {
                error_with_message(UNKNOWN_SERVER_ERROR, p.message).expect("UNKNOWN_SERVER_ERROR owns a class")
            }
        },

        // Everything else is a class that owns its protocol code, which is
        // exactly what the generated `error_with_message` table (a copy of
        // `Errors::error_with_message`, Java's `Errors.exception(String)`)
        // reconstructs — including `REQUEST_TIMED_OUT` -> `Error::Timeout` and
        // `MESSAGE_TOO_LARGE` -> `Error::RecordTooLarge`, the two the C++ server
        // used to guess at from the message text.
        //
        // A code the table has no class for falls back to `UNKNOWN_SERVER_ERROR`,
        // as `Errors.forCode` does in Java. Four negatives land there,
        // deliberately, carrying the message. They are exactly the client-side
        // classes that hold structured payload the proto does not transport —
        // `CorrelationIdMismatch`'s two correlation ids, `QuotaViolation`'s
        // metric and bounds, `RecordDeserialization`'s partition and offset, and
        // `ConsumerLogTruncation`'s partition map — so an arm above could only
        // reconstruct the class by fabricating that payload. Widening the proto
        // is the fix if a test ever needs to assert on one of them, as it was
        // for `ConsumerNoOffsetForPartition` and `ConsumerOffsetOutOfRange`.
        code => match error_with_message(code, p.message.clone()) {
            Some(error) => error,
            None => error_with_message(UNKNOWN_SERVER_ERROR, p.message).expect("UNKNOWN_SERVER_ERROR owns a class"),
        },
    }
}

/// A tonic transport-level failure: a gRPC-layer problem (connection refused,
/// server crashed mid-call, a handler that raised instead of replying) that no
/// real Kafka client produces. It panics, failing the test: returned as an
/// `Error`, it would be one a test could match by class and so pass on a
/// broken server. Every server returns a binding's error as its reply's
/// `error`, so a transport failure is never the binding's answer.
pub(crate) fn status_to_kafka_error(status: &tonic::Status, backend: &'static str) -> Error {
    panic!(
        "{} gRPC backend transport error ({:?}): {}",
        backend,
        status.code(),
        status.message()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::error_code::INVALID_PRODUCER_ID_MAPPING;
    use crate::common::error_code::LOCAL_ILLEGAL_STATE;
    use crate::common::error_code::UNKNOWN_SERVER_ERROR;

    fn cause(code: i32, message: &str) -> Option<proto::KafkaErrorCause> {
        Some(proto::KafkaErrorCause { code, message: message.to_string(), bare_kafka_error: false })
    }

    /// A bare `KafkaException` with a cause rebuilds with that cause, of its class.
    #[test]
    fn bare_kafka_error_keeps_its_cause() {
        let error = kafka_error_from_proto(proto::KafkaError {
            code: UNKNOWN_SERVER_ERROR,
            message: "in an error state".to_string(),
            bare_kafka_error: true,
            cause: cause(INVALID_PRODUCER_ID_MAPPING, "bad mapping"),
            ..Default::default()
        });
        assert!(matches!(error, Error::KafkaError(_)), "got {error:?}");
        assert_eq!(error.message(), "in an error state");
        let source = error.source().expect("a cause");
        assert!(matches!(source, Error::InvalidPidMapping(_)), "got {source:?}");
        assert_eq!(source.message(), "bad mapping");
        assert!(std::error::Error::source(&error).is_some());
    }

    /// Any other class is rebuilt without the cause, so `source()` is `None`.
    #[test]
    fn other_class_drops_the_cause() {
        let error = kafka_error_from_proto(proto::KafkaError {
            code: LOCAL_ILLEGAL_STATE,
            message: "state".to_string(),
            cause: cause(INVALID_PRODUCER_ID_MAPPING, "bad mapping"),
            ..Default::default()
        });
        assert!(error.is_local_illegal_state_error(), "got {error:?}");
        assert!(error.source().is_none());
    }

    /// The blocking deadline is the longest configured wait plus the margin,
    /// at Java's defaults when unset.
    #[test]
    fn blocking_deadline_from_config() {
        assert_eq!(blocking_deadline(&HashMap::new()), Duration::from_secs(150));
        let config = HashMap::from([
            ("max.block.ms".to_string(), "5000".to_string()),
            ("request.timeout.ms".to_string(), "200000".to_string()),
            ("delivery.timeout.ms".to_string(), "1000".to_string()),
        ]);
        assert_eq!(blocking_deadline(&config), Duration::from_secs(230));
        let small = HashMap::from([
            ("max.block.ms".to_string(), "0".to_string()),
            ("request.timeout.ms".to_string(), "0".to_string()),
            ("delivery.timeout.ms".to_string(), "0".to_string()),
        ]);
        assert_eq!(blocking_deadline(&small), Duration::from_secs(30));
    }

    /// No cause: a bare error stays bare, with no source.
    #[test]
    fn no_cause() {
        let error = kafka_error_from_proto(proto::KafkaError {
            code: UNKNOWN_SERVER_ERROR,
            message: "plain".to_string(),
            bare_kafka_error: true,
            ..Default::default()
        });
        assert!(matches!(error, Error::KafkaError(_)), "got {error:?}");
        assert!(error.source().is_none());
    }
}
