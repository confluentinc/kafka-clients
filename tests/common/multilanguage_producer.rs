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
//!      `RecordMetadata` or `Error` reference.
//!
//! Used only when `--features multilanguage-tests` is enabled.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::common::Error;
use confluent_kafka::common::KafkaFuture;
use confluent_kafka::common::Node;
use confluent_kafka::common::PartitionInfo;
use confluent_kafka::common::TopicPartition;
use confluent_kafka::common::errors::ApiError;
use confluent_kafka::common::errors::AuthenticationError;
use confluent_kafka::common::errors::AuthorizationError;
use confluent_kafka::common::errors::AuthorizerNotReadyError;
use confluent_kafka::common::errors::DisconnectError;
use confluent_kafka::common::errors::InterruptError;
use confluent_kafka::common::errors::InvalidOffsetError;
use confluent_kafka::common::errors::SslAuthenticationError;
use confluent_kafka::common::header::Header;
use confluent_kafka::common::network::InvalidReceiveError;
use confluent_kafka::common::protocol::Errors;
use confluent_kafka::consumer::ConsumerCommitFailedError;
use confluent_kafka::consumer::ConsumerGroupMetadata;
use confluent_kafka::consumer::ConsumerRetriableCommitFailedError;
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
    pub async fn new(channel: Channel, config: HashMap<String, String>, backend: &'static str) -> Result<Self, Error> {
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
    fn transactions_not_in_harness(&self, operation: &str) -> Error {
        Error::unsupported_version(format!(
            "{} is not available through the {} multilanguage backend (PLAN §9.6)",
            operation, self.backend
        ))
    }
}

impl Producer<Vec<u8>, Vec<u8>> for MultilanguageProducer {
    /// The multilanguage harness has no transactional RPCs: Milestone 11 defers the
    /// C / Python / gRPC transaction surface, tracked as
    /// `design/history/Milestone-11/PLAN.md` §9.6. Returns an explicit error rather
    /// than silently succeeding (CLAUDE.md §5).
    async fn init_transactions(&self) -> Result<(), Error> {
        Err(self.transactions_not_in_harness("initTransactions"))
    }

    /// Not in the harness — see [`Self::init_transactions`].
    fn begin_transaction(&self) -> Result<(), Error> {
        Err(self.transactions_not_in_harness("beginTransaction"))
    }

    /// Not in the harness — see [`Self::init_transactions`].
    async fn send_offsets_to_transaction(
        &self,
        _offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        _group_metadata: ConsumerGroupMetadata,
    ) -> Result<(), Error> {
        Err(self.transactions_not_in_harness("sendOffsetsToTransaction"))
    }

    /// Not in the harness — see [`Self::init_transactions`].
    async fn commit_transaction(&self) -> Result<(), Error> {
        Err(self.transactions_not_in_harness("commitTransaction"))
    }

    /// Not in the harness — see [`Self::init_transactions`].
    async fn abort_transaction(&self) -> Result<(), Error> {
        Err(self.transactions_not_in_harness("abortTransaction"))
    }

    async fn send(&self, record: ProducerRecord<Vec<u8>, Vec<u8>>) -> Result<KafkaFuture<RecordMetadata>, Error> {
        self.send_with_callback(record, None).await
    }

    async fn send_with_callback(
        &self,
        record: ProducerRecord<Vec<u8>, Vec<u8>>,
        callback: Option<Callback>,
    ) -> Result<KafkaFuture<RecordMetadata>, Error> {
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
            None => Err(Error::local_illegal_state(format!(
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

    async fn flush(&self) -> Result<(), Error> {
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

    async fn close(&self) -> Result<(), Error> {
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

    async fn close_timeout(&self, timeout: Duration) -> Result<(), Error> {
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

pub(crate) fn kafka_error_from_proto(p: proto::KafkaError) -> Error {
    use crate::common::error_code::*;

    // `code` is the sole discriminator. It is the FFI error code
    // (`kafka_common_ErrorCode_t`), which is injective over the client's error
    // classes, so it identifies the class on its own — that is why the proto no
    // longer carries a `variant` field and neither server hand-matches message
    // text any more.
    //
    // `is_retriable` / `is_fatal` are gone with it: both sides derive them from
    // the code (`Error::is_retriable_error`,
    // `request_utils::is_fatal_error`), so carrying them over the wire only
    // created a second, divergeable source of truth.
    match p.code {
        // Classes whose payload the proto transports. Java's constructors take
        // the payload *and* a message, so both survive; the no-message
        // constructors would drop `p.message` on the floor.
        TOPIC_AUTHORIZATION_FAILED => {
            Error::topic_authorization_with_message(p.unauthorized_topics.into_iter().collect(), p.message)
        },
        INVALID_TOPIC_ERROR => Error::invalid_topics_with_message(p.invalid_topics.into_iter().collect(), p.message),
        GROUP_AUTHORIZATION_FAILED => {
            Error::group_authorization_with_message(p.group_id.unwrap_or_default(), p.message)
        },

        // Classes only the client raises, so `Errors` has no code for them and
        // they carry an FFI-local negative. Each holds nothing but a message,
        // so the code plus `p.message` reconstructs it exactly.
        LOCAL_CONCURRENT_MODIFICATION => Error::local_concurrent_modification(p.message),
        LOCAL_ILLEGAL_ARGUMENT => Error::local_illegal_argument(p.message),
        LOCAL_ILLEGAL_STATE => Error::local_illegal_state(p.message),
        LOCAL_TIMEOUT => Error::local_timeout(p.message),
        API => Error::Api(ApiError::new(p.message)),
        AUTHENTICATION => Error::Authentication(AuthenticationError::new(p.message)),
        AUTHORIZATION => Error::Authorization(AuthorizationError::new(p.message)),
        AUTHORIZER_NOT_READY => Error::AuthorizerNotReady(AuthorizerNotReadyError::new(p.message)),
        CONFIG => Error::config(p.message),
        DISCONNECT => Error::Disconnect(DisconnectError::new(p.message)),
        INTERRUPT => Error::Interrupt(InterruptError::new(p.message)),
        INVALID_OFFSET => Error::InvalidOffset(InvalidOffsetError::new(p.message)),
        SCHEMA => Error::schema(p.message),
        SERIALIZATION => Error::serialization(p.message),
        SSL_AUTHENTICATION => Error::SslAuthentication(SslAuthenticationError::new(p.message)),
        TRANSACTION_ABORTED => Error::transaction_aborted_with_message(p.message),
        WAKEUP => Error::wakeup(p.message),
        CONSUMER_COMMIT_FAILED => Error::ConsumerCommitFailed(ConsumerCommitFailedError::new(p.message)),
        CONSUMER_RETRIABLE_COMMIT_FAILED => {
            Error::ConsumerRetriableCommitFailed(ConsumerRetriableCommitFailedError::new(p.message))
        },
        INVALID_RECEIVE => Error::InvalidReceive(InvalidReceiveError::new(p.message)),
        PRODUCER_BUFFER_EXHAUSTED => Error::buffer_exhausted(p.message),

        // Everything else is a class that owns its protocol code, which is
        // exactly what `Errors::error_with_message` reconstructs — including
        // `REQUEST_TIMED_OUT` -> `Error::Timeout` and `MESSAGE_TOO_LARGE` ->
        // `Error::RecordTooLarge`, the two the C++ server used to guess at from
        // the message text.
        //
        // Six negatives also fall here, deliberately, as
        // `Error::KafkaError(UnknownServerError)` carrying the message. They
        // are exactly the client-side classes that hold structured payload the
        // proto does not transport — `CorrelationIdMismatch`'s two correlation
        // ids, `QuotaViolation`'s metric and bounds,
        // `RecordDeserialization`'s partition and offset, and
        // `ConsumerLogTruncation` / `ConsumerNoOffsetForPartition` /
        // `ConsumerOffsetOutOfRange`'s partition maps — so an arm above could
        // only reconstruct the class by fabricating that payload. Widening the
        // proto is the fix if a test ever needs to assert on one of them.
        code => Error::with_message(errors_from_code(code), p.message),
    }
}

/// Best-effort `i32` → `Errors` mapping. Falls back to `UnknownServerError`
/// when the code is out of `i16` range — which is also what
/// [`Errors::for_code`] answers for an unrecognised in-range code, so the
/// negatives that reach this function (the client-side classes with no
/// `Errors` entry) resolve there too.
///
/// The code is *not* informational — it is the only type-level information on
/// the wire — and this is where the coded classes are reconstructed:
/// [`Errors::error_with_message`] turns a code its class owns straight back
/// into that class. The client-side classes, which `Errors` has no code for,
/// are handled by the explicit arms in [`kafka_error_from_proto`] instead and
/// reach this function only for the six the proto cannot carry.
fn errors_from_code(code: i32) -> Errors {
    match i16::try_from(code) {
        Ok(c) => Errors::for_code(c),
        Err(_) => Errors::UnknownServerError,
    }
}

/// Map a tonic transport-level failure to a `Error`. These are
/// gRPC-layer problems (connection refused, server crashed mid-call, etc.)
/// that aren't produced by a real Kafka client; surfacing them as
/// `LocalIllegalState` makes failures visible without conflating with broker
/// errors.
pub(crate) fn status_to_kafka_error(status: &tonic::Status, backend: &'static str) -> Error {
    Error::local_illegal_state(format!(
        "{} gRPC backend transport error ({:?}): {}",
        backend,
        status.code(),
        status.message()
    ))
}
