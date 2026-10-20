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

//! A minimal in-process fake broker for constructor-wiring tests.
//!
//! Rust-only and `#[cfg(test)]` (DoD #7): Java checks that
//! `metadata.cluster.check.enable` reaches the producer / consumer
//! `NetworkClient` only through the broker integration test
//! `ClientRebootstrapTest.testRebootstrapOnMetadataClusterCheckFail`, which
//! needs a multi-broker restart with swapped listener ports that the Docker
//! harness cannot do. This fake lets a unit test build a REAL `KafkaProducer` /
//! `AsyncKafkaConsumer` (DoD #12: no hand-built `NetworkClient`) and observe
//! the ApiVersions request each connection sends on the wire.
//!
//! It speaks just enough protocol for the client to learn the cluster:
//! `ApiVersions` gets the default broker response and `Metadata` gets one
//! broker (node 0, this listener) leading partition 0 of one topic.
//! `ListOffsets` requests are recorded and held until the test sets an answer
//! with [`FakeBroker::answer_list_offsets`], which lets a consumer test step
//! through the moments Java's `MockClient` tests stop at (request sent, no
//! response yet). A `Fetch` is answered with the records the test sets with
//! [`FakeBroker::answer_fetch`], once; without one it is answered empty after a
//! short wait, as a broker does when `fetch.max.wait.ms` elapses with no data.
//! Every other request is read and left unanswered.
//!
//! Requests are recorded when they arrive, as Java's `MockClient` records them
//! when the client sends them, even while an earlier request on the same
//! connection is held: a reader task records each frame and hands it to the
//! connection's responder, which answers in arrival order, as a broker must.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::ApiVersionsRequestData;
use crate::FetchRequestData;
use crate::FetchResponseData;
use crate::ListOffsetsRequestData;
use crate::ListOffsetsResponseData;
use crate::MetadataResponseData;
use crate::api_message_type::ListenerType;
use crate::common::Uuid;
use crate::common::compress::Compression;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use crate::common::record::TimestampType;
use crate::common::record::internal::{MemoryRecords, RecordBatch, SimpleRecord};
use crate::common::requests::{ApiVersionsResponse, FetchResponse, RequestHeader, RequestUtils, ResponseHeader};
use crate::fetch_response_data::{FetchableTopicResponse, PartitionData as FetchPartitionData};
use crate::list_offsets_response_data::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse};
use crate::metadata_response_data::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic};

/// The node id the fake advertises for itself in its Metadata responses.
pub(crate) const FAKE_BROKER_NODE_ID: i32 = 0;

/// One ApiVersions request as the fake received it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReceivedApiVersions {
    /// The request version.
    pub(crate) version: i16,
    /// `ClusterId` (v5+, KIP-1242), `None` when absent / null.
    pub(crate) cluster_id: Option<String>,
    /// `NodeId` (v5+, KIP-1242), -1 when absent.
    pub(crate) node_id: i32,
}

/// How the fake answers every partition of a held `ListOffsets` request.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ListOffsetsAnswer {
    /// The partition error.
    pub(crate) error: Errors,
    /// The offset returned with [`Errors::None`].
    pub(crate) offset: i64,
}

/// The records the fake returns for every partition of one `Fetch`, as Java's
/// `KafkaConsumerTest.FetchInfo`: `count` records from `offset`, with the log
/// spanning `log_first_offset..=log_last_offset`, so the high watermark is
/// `log_last_offset + 1`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct FetchAnswer {
    /// The log start offset.
    pub(crate) log_first_offset: i64,
    /// The last offset in the log.
    pub(crate) log_last_offset: i64,
    /// The offset of the first record returned.
    pub(crate) offset: i64,
    /// How many records are returned.
    pub(crate) count: i32,
}

/// How long a `Fetch` without a [`FetchAnswer`] waits before it is answered
/// empty: the fake's `fetch.max.wait.ms`.
const EMPTY_FETCH_WAIT: Duration = Duration::from_millis(20);

/// What the serving tasks share with the test.
struct Shared {
    api_versions: Mutex<Vec<ReceivedApiVersions>>,
    /// The `Timestamp` of every partition of every `ListOffsets` received.
    list_offsets_timestamps: Mutex<Vec<i64>>,
    /// `None` holds `ListOffsets` requests; `Some` answers them.
    list_offsets_answer: watch::Sender<Option<ListOffsetsAnswer>>,
    /// How many `Fetch` requests arrived.
    fetch_requests: AtomicUsize,
    /// The answer for the next `Fetch`, taken by the one that uses it.
    fetch_answer: Mutex<Option<FetchAnswer>>,
}

/// A request read from a connection, waiting for its turn to be answered.
enum Pending {
    /// The response is already serialized.
    Ready(Vec<u8>),
    /// Answered once the test sets a [`ListOffsetsAnswer`].
    ListOffsets {
        response_header: ResponseHeader,
        version: i16,
        request: ListOffsetsRequestData,
    },
    /// Answered with the [`FetchAnswer`], or empty after [`EMPTY_FETCH_WAIT`].
    Fetch {
        response_header: ResponseHeader,
        version: i16,
        request: FetchRequestData,
    },
}

/// The fake broker: a listener plus the requests it has seen.
pub(crate) struct FakeBroker {
    addr: SocketAddr,
    shared: Arc<Shared>,
    accept_task: JoinHandle<()>,
}

impl FakeBroker {
    /// Binds `127.0.0.1:0` and serves every accepted connection. Metadata
    /// responses carry `cluster_id` and a single-partition `topic`.
    pub(crate) async fn start(cluster_id: &str, topic: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake broker");
        let addr = listener.local_addr().expect("fake broker address");
        let shared = Arc::new(Shared {
            api_versions: Mutex::new(Vec::new()),
            list_offsets_timestamps: Mutex::new(Vec::new()),
            list_offsets_answer: watch::Sender::new(None),
            fetch_requests: AtomicUsize::new(0),
            fetch_answer: Mutex::new(None),
        });
        let seen = Arc::clone(&shared);
        let cluster_id = cluster_id.to_string();
        let topic = topic.to_string();
        let topic_id = Uuid::random_uuid();
        let accept_task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(
                    stream,
                    addr,
                    Arc::clone(&seen),
                    cluster_id.clone(),
                    topic.clone(),
                    topic_id,
                ));
            }
        });
        Self { addr, shared, accept_task }
    }

    /// `host:port` for `bootstrap.servers`.
    pub(crate) fn bootstrap(&self) -> String {
        self.addr.to_string()
    }

    /// The ApiVersions requests received so far, in arrival order.
    pub(crate) fn api_versions_requests(&self) -> Vec<ReceivedApiVersions> {
        self.shared.api_versions.lock().expect("fake broker mutex poisoned").clone()
    }

    /// The `Timestamp` of each partition of the `ListOffsets` requests
    /// received so far, in arrival order: `-1` (`LATEST_TIMESTAMP`) asks for
    /// the log-end offset.
    pub(crate) fn list_offsets_timestamps(&self) -> Vec<i64> {
        self.shared
            .list_offsets_timestamps
            .lock()
            .expect("fake broker mutex poisoned")
            .clone()
    }

    /// Answers the held `ListOffsets` requests, and every later one, with
    /// `answer`; `None` holds them again.
    pub(crate) fn answer_list_offsets(&self, answer: Option<ListOffsetsAnswer>) {
        self.shared.list_offsets_answer.send_replace(answer);
    }

    /// Answers the next `Fetch` with `answer`; the ones after it are answered
    /// empty again.
    pub(crate) fn answer_fetch(&self, answer: FetchAnswer) {
        *self.shared.fetch_answer.lock().expect("fake broker mutex poisoned") = Some(answer);
    }

    /// How many `Fetch` requests have arrived so far.
    pub(crate) fn fetch_requests(&self) -> usize {
        self.shared.fetch_requests.load(Ordering::SeqCst)
    }

    /// Waits (up to `timeout`) until at least `count` ApiVersions requests have
    /// arrived, and returns them.
    pub(crate) async fn await_api_versions_requests(
        &self,
        count: usize,
        timeout: std::time::Duration,
    ) -> Vec<ReceivedApiVersions> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let seen = self.api_versions_requests();
            if seen.len() >= count || tokio::time::Instant::now() >= deadline {
                return seen;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}

impl Drop for FakeBroker {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

/// Serves one connection: a reader task records each request as it arrives
/// and queues it; this task answers the queue in arrival order.
async fn serve(
    stream: TcpStream,
    addr: SocketAddr,
    seen: Arc<Shared>,
    cluster_id: String,
    topic: String,
    topic_id: Uuid,
) {
    let (reader, mut writer) = stream.into_split();
    let (pending_tx, mut pending_rx) = mpsc::unbounded_channel();
    let reader_task = tokio::spawn(read_requests(
        reader,
        addr,
        Arc::clone(&seen),
        cluster_id,
        topic,
        topic_id,
        pending_tx,
    ));
    while let Some(pending) = pending_rx.recv().await {
        let Some(response) = respond(&seen, pending).await else {
            break;
        };
        let mut out = Vec::with_capacity(4 + response.len());
        out.extend_from_slice(&(response.len() as i32).to_be_bytes());
        out.extend_from_slice(&response);
        if writer.write_all(&out).await.is_err() {
            break;
        }
    }
    reader_task.abort();
}

/// Reads, records and queues every request of a connection until it closes.
async fn read_requests(
    mut stream: OwnedReadHalf,
    addr: SocketAddr,
    seen: Arc<Shared>,
    cluster_id: String,
    topic: String,
    topic_id: Uuid,
    pending: mpsc::UnboundedSender<Pending>,
) {
    loop {
        let mut size = [0u8; 4];
        if stream.read_exact(&mut size).await.is_err() {
            return;
        }
        let mut frame = vec![0u8; i32::from_be_bytes(size) as usize];
        if stream.read_exact(&mut frame).await.is_err() {
            return;
        }
        let mut accessor = ByteBufferAccessor::new(frame);
        let header = RequestHeader::parse(&mut accessor).expect("fake broker: request header");
        let version = header.api_version();
        let response_header = header.to_response_header();
        let request = if *header.api_key() == ApiKeys::API_VERSIONS {
            let data = ApiVersionsRequestData::read(&mut accessor, version).expect("fake broker: ApiVersions body");
            seen.api_versions
                .lock()
                .expect("fake broker mutex poisoned")
                .push(ReceivedApiVersions { version, cluster_id: data.cluster_id.clone(), node_id: data.node_id });
            let mut body = ApiVersionsResponse::default_api_versions_response(ListenerType::Broker);
            Pending::Ready(serialize(RequestUtils::serialize(
                response_header.data(),
                response_header.header_version(),
                body.data_mut(),
                version,
            )))
        } else if *header.api_key() == ApiKeys::METADATA {
            let mut body = metadata_response(addr, &cluster_id, &topic, topic_id);
            Pending::Ready(serialize(RequestUtils::serialize(
                response_header.data(),
                response_header.header_version(),
                &mut body,
                version,
            )))
        } else if *header.api_key() == ApiKeys::LIST_OFFSETS {
            let request = ListOffsetsRequestData::read(&mut accessor, version).expect("fake broker: ListOffsets body");
            seen.list_offsets_timestamps.lock().expect("fake broker mutex poisoned").extend(
                request
                    .topics()
                    .iter()
                    .flat_map(|topic| topic.partitions().iter().map(|partition| partition.timestamp())),
            );
            Pending::ListOffsets { response_header, version, request }
        } else if *header.api_key() == ApiKeys::FETCH {
            let request = FetchRequestData::read(&mut accessor, version).expect("fake broker: Fetch body");
            seen.fetch_requests.fetch_add(1, Ordering::SeqCst);
            Pending::Fetch { response_header, version, request }
        } else {
            continue;
        };
        if pending.send(request).is_err() {
            return;
        }
    }
}

/// The serialized response to `pending`, once it can be given; `None` when
/// the fake is shutting down.
async fn respond(seen: &Shared, pending: Pending) -> Option<Vec<u8>> {
    match pending {
        Pending::Ready(response) => Some(response),
        Pending::ListOffsets { response_header, version, request } => {
            let mut answers = seen.list_offsets_answer.subscribe();
            let answer = answers.wait_for(Option::is_some).await.ok()?.expect("checked by wait_for");
            let mut body = list_offsets_response(&request, answer);
            Some(serialize(RequestUtils::serialize(
                response_header.data(),
                response_header.header_version(),
                &mut body,
                version,
            )))
        },
        Pending::Fetch { response_header, version, request } => {
            let answer = seen.fetch_answer.lock().expect("fake broker mutex poisoned").take();
            if answer.is_none() {
                tokio::time::sleep(EMPTY_FETCH_WAIT).await;
            }
            let mut body = fetch_response(&request, answer);
            Some(serialize(RequestUtils::serialize(
                response_header.data(),
                response_header.header_version(),
                &mut body,
                version,
            )))
        },
    }
}

/// The bytes of a serialized response.
fn serialize<E: std::fmt::Debug>(response: Result<ByteBufferAccessor, E>) -> Vec<u8> {
    response.expect("fake broker: serialize response").into_buffer()
}

fn metadata_response(addr: SocketAddr, cluster_id: &str, topic: &str, topic_id: Uuid) -> MetadataResponseData {
    let mut broker = MetadataResponseBroker::new();
    broker.set_node_id(FAKE_BROKER_NODE_ID);
    broker.set_host(addr.ip().to_string());
    broker.set_port(i32::from(addr.port()));

    let mut partition = MetadataResponsePartition::new();
    partition.set_partition_index(0);
    partition.set_leader_id(FAKE_BROKER_NODE_ID);
    partition.set_leader_epoch(0);
    partition.set_replica_nodes(vec![FAKE_BROKER_NODE_ID]);
    partition.set_isr_nodes(vec![FAKE_BROKER_NODE_ID]);
    partition.set_offline_replicas(Vec::new());
    partition.set_error_code(Errors::None.code());

    let mut topic_metadata = MetadataResponseTopic::new();
    topic_metadata.set_name(Some(topic.to_string()));
    topic_metadata.set_topic_id(topic_id);
    topic_metadata.set_error_code(Errors::None.code());
    topic_metadata.set_is_internal(false);
    topic_metadata.set_partitions(vec![partition]);

    let mut data = MetadataResponseData::new();
    data.set_cluster_id(Some(cluster_id.to_string()));
    data.set_controller_id(FAKE_BROKER_NODE_ID);
    data.set_brokers(vec![broker]);
    data.set_topics(vec![topic_metadata]);
    data
}

/// Answers every partition of `request` with `answer`.
fn list_offsets_response(request: &ListOffsetsRequestData, answer: ListOffsetsAnswer) -> ListOffsetsResponseData {
    let topics = request
        .topics()
        .iter()
        .map(|topic| {
            let partitions = topic
                .partitions()
                .iter()
                .map(|partition| {
                    let mut response = ListOffsetsPartitionResponse::new();
                    response.set_partition_index(partition.partition_index());
                    response.set_error_code(answer.error.code());
                    response.set_timestamp(-1);
                    response.set_offset(if answer.error == Errors::None {
                        answer.offset
                    } else {
                        -1
                    });
                    response.set_leader_epoch(0);
                    response
                })
                .collect();
            let mut response = ListOffsetsTopicResponse::new();
            response.set_name(topic.name().clone());
            response.set_partitions(partitions);
            response
        })
        .collect();
    let mut data = ListOffsetsResponseData::new();
    data.set_topics(topics);
    data
}

/// Answers every partition of `request`: with the records of `answer`, as
/// Java's `KafkaConsumerTest.fetchResponse(Map<TopicPartition, FetchInfo>)`
/// builds them, or with no records and unknown offsets (`-1`, which the
/// consumer does not apply) when there is no answer.
fn fetch_response(request: &FetchRequestData, answer: Option<FetchAnswer>) -> FetchResponseData {
    let responses = request
        .topics()
        .iter()
        .map(|topic| {
            let partitions = topic
                .partitions()
                .iter()
                .map(|partition| {
                    let mut response = FetchPartitionData::new();
                    response.set_partition_index(partition.partition());
                    response.set_error_code(Errors::None.code());
                    response.set_last_stable_offset(-1);
                    response.set_preferred_read_replica(FetchResponse::INVALID_PREFERRED_REPLICA_ID);
                    match answer {
                        Some(answer) => {
                            let records: Vec<SimpleRecord> = (0..answer.count)
                                .map(|i| {
                                    SimpleRecord::with_timestamp_key_value(
                                        0,
                                        Some(format!("key-{i}").into_bytes()),
                                        Some(format!("value-{i}").into_bytes()),
                                    )
                                })
                                .collect();
                            let records = MemoryRecords::with_records_with_magic_initial_offset_timestamp_type(
                                RecordBatch::CURRENT_MAGIC_VALUE,
                                answer.offset,
                                Compression::none().build(),
                                TimestampType::CreateTime,
                                &records,
                            );
                            response.set_high_watermark(answer.log_last_offset + 1);
                            response.set_log_start_offset(answer.log_first_offset);
                            response.set_records(Some(records.buffer_bytes().clone()));
                        },
                        None => {
                            response.set_high_watermark(-1);
                            response.set_log_start_offset(-1);
                            response.set_records(Some(bytes::Bytes::new()));
                        },
                    }
                    response
                })
                .collect();
            let mut response = FetchableTopicResponse::new();
            response.set_topic(topic.topic().clone());
            response.set_topic_id(topic.topic_id());
            response.set_partitions(partitions);
            response
        })
        .collect();
    let mut data = FetchResponseData::new();
    data.set_error_code(Errors::None.code());
    data.set_session_id(crate::common::requests::FetchMetadata::INVALID_SESSION_ID);
    data.set_responses(responses);
    data
}
