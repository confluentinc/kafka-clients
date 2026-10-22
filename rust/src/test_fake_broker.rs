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
//! `FindCoordinator` names this broker (node 0) the coordinator of every key,
//! and `OffsetFetch` returns the committed offset the test sets with
//! [`FakeBroker::set_committed_offset`] (`-1`, no committed offset, until then)
//! for every partition asked for. Every other request is read and left
//! unanswered.
//!
//! A test can also kill the broker ([`FakeBroker::kill`]), or have SASL/PLAIN
//! authentication succeed on the first connections and fail
//! (UNSUPPORTED_SASL_MECHANISM) on later ones
//! ([`FakeBroker::accept_sasl_handshakes`]).
//!
//! [`FakeBroker::start_with_group_coordinator`] additionally acts as the group
//! coordinator: a joining or heartbeating `ConsumerGroupHeartbeat` gets
//! member epoch 1 with an empty assignment. A leave heartbeat (member epoch -1) is recorded and left
//! unanswered, so the client's leave can only end by a timeout.
//!
//! Requests are recorded when they arrive, as Java's `MockClient` records them
//! when the client sends them, even while an earlier request on the same
//! connection is held: a reader task records each frame and hands it to the
//! connection's responder, which answers in arrival order, as a broker must.
//! A request left unanswered is recorded but never handed to the responder, so
//! it does not hold back the responses to the requests after it.

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
use crate::ConsumerGroupHeartbeatRequestData;
use crate::ConsumerGroupHeartbeatResponseData;
use crate::FetchRequestData;
use crate::FetchResponseData;
use crate::FindCoordinatorRequestData;
use crate::FindCoordinatorResponseData;
use crate::ListOffsetsRequestData;
use crate::ListOffsetsResponseData;
use crate::MetadataResponseData;
use crate::OffsetFetchRequestData;
use crate::OffsetFetchResponseData;
use crate::SaslAuthenticateResponseData;
use crate::SaslHandshakeResponseData;
use crate::api_message_type::ListenerType;
use crate::common::Uuid;
use crate::common::compress::Compression;
use crate::common::protocol::{ApiKeys, ByteBufferAccessor, Errors};
use crate::common::record::TimestampType;
use crate::common::record::internal::{MemoryRecords, RecordBatch, SimpleRecord};
use crate::common::requests::{ApiVersionsResponse, FetchResponse, RequestHeader, RequestUtils, ResponseHeader};
use crate::consumer_group_heartbeat_response_data::Assignment;
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
    /// Whether the fake acts as the group coordinator.
    group_coordinator: bool,
    /// The member epoch of every `ConsumerGroupHeartbeat` received, in arrival
    /// order (only recorded when acting as the group coordinator).
    heartbeat_epochs: Mutex<Vec<i32>>,
    /// `SaslHandshake`s answered so far. The first `sasl_handshakes_accepted`
    /// succeed (PLAIN, and the `SaslAuthenticate` after them succeeds too);
    /// later ones are answered UNSUPPORTED_SASL_MECHANISM.
    sasl_handshakes: AtomicUsize,
    sasl_handshakes_accepted: AtomicUsize,
    /// The serving task of every accepted connection, so [`FakeBroker::kill`]
    /// can close them.
    connections: Mutex<Vec<JoinHandle<()>>>,
    /// How many `OffsetFetch` requests arrived.
    offset_fetch_requests: AtomicUsize,
    /// The committed offset `OffsetFetch` returns for every partition.
    committed_offset: Mutex<i64>,
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
        Self::start_inner(cluster_id, topic, false).await
    }

    /// Like [`Self::start`], and also acts as the group coordinator (see the
    /// module docs): joins are answered, leave heartbeats are not.
    pub(crate) async fn start_with_group_coordinator(cluster_id: &str, topic: &str) -> Self {
        Self::start_inner(cluster_id, topic, true).await
    }

    async fn start_inner(cluster_id: &str, topic: &str, group_coordinator: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fake broker");
        let addr = listener.local_addr().expect("fake broker address");
        let shared = Arc::new(Shared {
            api_versions: Mutex::new(Vec::new()),
            list_offsets_timestamps: Mutex::new(Vec::new()),
            list_offsets_answer: watch::Sender::new(None),
            fetch_requests: AtomicUsize::new(0),
            fetch_answer: Mutex::new(None),
            group_coordinator,
            heartbeat_epochs: Mutex::new(Vec::new()),
            sasl_handshakes: AtomicUsize::new(0),
            sasl_handshakes_accepted: AtomicUsize::new(usize::MAX),
            connections: Mutex::new(Vec::new()),
            offset_fetch_requests: AtomicUsize::new(0),
            committed_offset: Mutex::new(-1),
        });
        let seen = Arc::clone(&shared);
        let cluster_id = cluster_id.to_string();
        let topic = topic.to_string();
        let topic_id = Uuid::random_uuid();
        let accept_task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let connection = tokio::spawn(serve(
                    stream,
                    addr,
                    Arc::clone(&seen),
                    cluster_id.clone(),
                    topic.clone(),
                    topic_id,
                ));
                seen.connections.lock().expect("fake broker mutex poisoned").push(connection);
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

    /// SASL/PLAIN: the first `accepted` `SaslHandshake`s (connections)
    /// authenticate; every later one is answered UNSUPPORTED_SASL_MECHANISM
    /// (offering only SCRAM-SHA-512), so that connection fails
    /// authentication. With `accepted = 1` the bootstrap connection learns
    /// the metadata and the connection to the partition leader fails.
    pub(crate) fn accept_sasl_handshakes(&self, accepted: usize) {
        self.shared.sasl_handshakes_accepted.store(accepted, Ordering::SeqCst);
    }

    /// Kills the broker: stops listening, so later connection attempts are
    /// refused, and closes every open connection, as `docker kill` does.
    pub(crate) fn kill(&self) {
        self.accept_task.abort();
        for connection in self.shared.connections.lock().expect("fake broker mutex poisoned").drain(..) {
            connection.abort();
        }
    }

    /// How many `Fetch` requests have arrived so far.
    pub(crate) fn fetch_requests(&self) -> usize {
        self.shared.fetch_requests.load(Ordering::SeqCst)
    }

    /// The member epochs of the `ConsumerGroupHeartbeat` requests received so
    /// far, in arrival order (always empty unless started with
    /// [`Self::start_with_group_coordinator`]).
    pub(crate) fn heartbeat_epochs(&self) -> Vec<i32> {
        self.shared.heartbeat_epochs.lock().expect("fake broker mutex poisoned").clone()
    }

    /// Sets the committed offset `OffsetFetch` returns for every partition.
    pub(crate) fn set_committed_offset(&self, offset: i64) {
        *self.shared.committed_offset.lock().expect("fake broker mutex poisoned") = offset;
    }

    /// How many `OffsetFetch` requests have arrived so far.
    pub(crate) fn offset_fetch_requests(&self) -> usize {
        self.shared.offset_fetch_requests.load(Ordering::SeqCst)
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
    // Aborts the reader when this task ends or is aborted by `kill`, so the
    // read half is dropped with the write half and the socket closes.
    struct AbortOnDrop(JoinHandle<()>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let _reader_task = AbortOnDrop(tokio::spawn(read_requests(
        reader,
        addr,
        Arc::clone(&seen),
        cluster_id,
        topic,
        topic_id,
        pending_tx,
    )));
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
        } else if *header.api_key() == ApiKeys::SASL_HANDSHAKE {
            let accept = seen.sasl_handshakes.fetch_add(1, Ordering::SeqCst)
                < seen.sasl_handshakes_accepted.load(Ordering::SeqCst);
            let mut body = SaslHandshakeResponseData::new();
            if accept {
                body.set_error_code(Errors::None.code());
                body.set_mechanisms(vec!["PLAIN".to_string()]);
            } else {
                body.set_error_code(Errors::UnsupportedSaslMechanism.code());
                body.set_mechanisms(vec!["SCRAM-SHA-512".to_string()]);
            }
            Pending::Ready(serialize(RequestUtils::serialize(
                response_header.data(),
                response_header.header_version(),
                &mut body,
                version,
            )))
        } else if *header.api_key() == ApiKeys::SASL_AUTHENTICATE {
            let mut body = SaslAuthenticateResponseData::new();
            body.set_error_code(Errors::None.code());
            body.set_auth_bytes(Vec::new());
            Pending::Ready(serialize(RequestUtils::serialize(
                response_header.data(),
                response_header.header_version(),
                &mut body,
                version,
            )))
        } else if *header.api_key() == ApiKeys::FIND_COORDINATOR {
            let request =
                FindCoordinatorRequestData::read(&mut accessor, version).expect("fake broker: FindCoordinator body");
            let mut body = find_coordinator_response(addr, &request, version);
            Pending::Ready(serialize(RequestUtils::serialize(
                response_header.data(),
                response_header.header_version(),
                &mut body,
                version,
            )))
        } else if *header.api_key() == ApiKeys::OFFSET_FETCH {
            let request = OffsetFetchRequestData::read(&mut accessor, version).expect("fake broker: OffsetFetch body");
            seen.offset_fetch_requests.fetch_add(1, Ordering::SeqCst);
            let committed = *seen.committed_offset.lock().expect("fake broker mutex poisoned");
            let mut body = offset_fetch_response(&request, committed);
            Pending::Ready(serialize(RequestUtils::serialize(
                response_header.data(),
                response_header.header_version(),
                &mut body,
                version,
            )))
        } else if *header.api_key() == ApiKeys::FETCH {
            let request = FetchRequestData::read(&mut accessor, version).expect("fake broker: Fetch body");
            seen.fetch_requests.fetch_add(1, Ordering::SeqCst);
            Pending::Fetch { response_header, version, request }
        } else if *header.api_key() == ApiKeys::CONSUMER_GROUP_HEARTBEAT && seen.group_coordinator {
            let request = ConsumerGroupHeartbeatRequestData::read(&mut accessor, version)
                .expect("fake broker: ConsumerGroupHeartbeat body");
            seen.heartbeat_epochs
                .lock()
                .expect("fake broker mutex poisoned")
                .push(request.member_epoch);
            if request.member_epoch < 0 {
                // A leave heartbeat: recorded above, never answered.
                continue;
            }
            let mut body = ConsumerGroupHeartbeatResponseData::new();
            body.member_id = Some(request.member_id.clone());
            body.member_epoch = 1;
            body.heartbeat_interval_ms = 100;
            body.assignment = Some(Assignment::new());
            Pending::Ready(serialize(RequestUtils::serialize(
                response_header.data(),
                response_header.header_version(),
                &mut body,
                version,
            )))
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

/// Names this broker the coordinator of every key of `request` (v4+), or of
/// its single key (v0-3).
fn find_coordinator_response(
    addr: SocketAddr,
    request: &FindCoordinatorRequestData,
    version: i16,
) -> FindCoordinatorResponseData {
    use crate::find_coordinator_response_data::Coordinator;
    let coordinators = request
        .coordinator_keys()
        .iter()
        .map(|key| {
            let mut coordinator = Coordinator::new();
            coordinator.set_key(key.clone());
            coordinator.set_node_id(FAKE_BROKER_NODE_ID);
            coordinator.set_host(addr.ip().to_string());
            coordinator.set_port(i32::from(addr.port()));
            coordinator.set_error_code(Errors::None.code());
            coordinator.set_error_message(None);
            coordinator
        })
        .collect();
    let mut data = FindCoordinatorResponseData::new();
    if version < 4 {
        data.set_error_code(Errors::None.code());
        data.set_error_message(None);
        data.set_node_id(FAKE_BROKER_NODE_ID);
        data.set_host(addr.ip().to_string());
        data.set_port(i32::from(addr.port()));
    } else {
        data.set_coordinators(coordinators);
    }
    data
}

/// Answers every partition of every group of `request` (v8+) with
/// `committed`.
fn offset_fetch_response(request: &OffsetFetchRequestData, committed: i64) -> OffsetFetchResponseData {
    use crate::offset_fetch_response_data::{
        OffsetFetchResponseGroup, OffsetFetchResponsePartitions, OffsetFetchResponseTopics,
    };
    let groups = request
        .groups()
        .iter()
        .map(|group| {
            let topics = group
                .topics()
                .iter()
                .flatten()
                .map(|topic| {
                    let partitions = topic
                        .partition_indexes()
                        .iter()
                        .map(|partition| {
                            let mut response = OffsetFetchResponsePartitions::new();
                            response.set_partition_index(*partition);
                            response.set_committed_offset(committed);
                            response.set_committed_leader_epoch(-1);
                            response.set_metadata(Some(String::new()));
                            response.set_error_code(Errors::None.code());
                            response
                        })
                        .collect();
                    let mut response = OffsetFetchResponseTopics::new();
                    response.set_name(topic.name().clone());
                    response.set_topic_id(topic.topic_id());
                    response.set_partitions(partitions);
                    response
                })
                .collect();
            let mut response = OffsetFetchResponseGroup::new();
            response.set_group_id(group.group_id().clone());
            response.set_topics(topics);
            response.set_error_code(Errors::None.code());
            response
        })
        .collect();
    let mut data = OffsetFetchResponseData::new();
    data.set_groups(groups);
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
