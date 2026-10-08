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

//! Workloads that run inside a binding's gRPC server.
//!
//! A `python`, `python-async` or `c` workload does not drive its client one
//! call at a time from here. It asks the binding's multilanguage gRPC server to
//! run the whole produce or consume loop itself
//! (`multilanguage-test-server/proto/chaos_service.proto`), and turns the
//! events the server streams back into [`WorkloadEvent`]s for the verifier.
//! The binding therefore batches, pipelines, retries and runs its rebalance
//! listener exactly as an application's would, while fault injection, the
//! topic-id map and verification stay in the harness.
//!
//! The server-side loops mirror [`super::workload`]'s in-process Rust ones, so
//! the verdict means the same thing for every backend.
//!
//! # Topic ids
//!
//! The harness reads a workload's stream long after the binding observed the
//! events on it: the server queues are unbounded, and a fast producer keeps the
//! harness minutes behind. So an event cannot be stamped with the topic id
//! current when it is *read* -- across a topic recreate that scores records of
//! the destroyed generation as the new one's, or the reverse. Instead the
//! harness switches its topic-id map through [`switch_topic_id`], which first
//! queues a `Marker` on every running remote stream (`MarkWorkload`); each
//! stream's reader keeps stamping the old id until it reads that marker. An
//! event's id is therefore the one current when the CLIENT observed it -- its
//! delivery callback ran, or the poll that returned it -- up to the latency of
//! one `MarkWorkload` call, the same boundary the in-process workloads get from
//! reading the live map in their callbacks.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use confluent_kafka::common::Uuid;
use multilanguage_test_server::proto;
use multilanguage_test_server::proto::chaos_workload_service_client::ChaosWorkloadServiceClient;
use multilanguage_test_server::proto::workload_event::Event;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Channel;

use super::common::multilanguage_producer::kafka_error_from_proto;
use super::verifier::{ConsumerOp, RebalanceCallback, Verifier, WorkloadEvent};
use super::workload::{
    COMMIT_CHECK_INTERVAL, CommitMode, POLL_TIMEOUT, Role, TopicIds, Workload, WorkloadContext, WorkloadSpec,
};
use super::workload_config::{consumer_props, producer_props};

/// How often the stop watcher checks the workload's stop flag.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// How often the stop watcher repeats `StopWorkload` until the stream ends. The
/// first request can reach the server before it registered the workload (the
/// watcher runs alongside `RunProducer` / `RunConsumer`), and stopping an
/// unknown workload is a successful no-op; repeating it is harmless.
const STOP_RESEND_INTERVAL: Duration = Duration::from_secs(1);

/// How long [`switch_topic_id`] waits for one workload's `MarkWorkload`. The
/// call only queues an event on the server, so running out means the server is
/// wedged.
const MARK_TIMEOUT: Duration = Duration::from_secs(30);

/// A workload run by a binding's gRPC server (`ChaosWorkloadService`).
pub struct RemoteWorkload {
    spec: WorkloadSpec,
    ctx: WorkloadContext,
    verifier: Arc<dyn Verifier>,
    client: ChaosWorkloadServiceClient<Channel>,
    /// The topic ids at the reader's position in the stream; see the module
    /// docs and [`switch_topic_id`].
    view: Arc<Mutex<GenerationView>>,
}

/// The topic ids one remote stream's reader stamps with.
#[derive(Default)]
struct GenerationView {
    /// The id at the reader's position for every topic [`switch_topic_id`] has
    /// switched since this workload registered. Other topics follow the live
    /// map.
    ids: HashMap<String, Uuid>,
    /// Markers queued on the stream but not read yet, in stream order: marker,
    /// topic, the id that applies from the marker on.
    pending: VecDeque<(u64, String, Uuid)>,
}

/// A request to queue `marker` on a workload's stream; `done` fires once the
/// server queued it (or the workload is gone).
struct MarkRequest {
    marker: u64,
    done: oneshot::Sender<()>,
}

/// A remote workload [`switch_topic_id`] must mark.
struct StreamEntry {
    key: u64,
    label: String,
    topic_ids: TopicIds,
    view: Arc<Mutex<GenerationView>>,
    marks: mpsc::UnboundedSender<MarkRequest>,
}

/// Every remote workload whose stream may still carry events. Process-wide
/// because the harness has no handle on its workloads once they run on their
/// own threads; entries are matched to a harness by its topic-id map.
static STREAMS: Mutex<Vec<StreamEntry>> = Mutex::new(Vec::new());
static NEXT_STREAM_KEY: AtomicU64 = AtomicU64::new(1);
static NEXT_MARKER: AtomicU64 = AtomicU64::new(1);

fn streams() -> MutexGuard<'static, Vec<StreamEntry>> {
    // Entries are plain data; a panic elsewhere while holding the lock leaves
    // them consistent, and the deregistration in `Drop` must not panic again.
    STREAMS.lock().unwrap_or_else(PoisonError::into_inner)
}

fn lock_view(view: &Mutex<GenerationView>) -> MutexGuard<'_, GenerationView> {
    view.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Sets `topic`'s id in the harness's live map `topic_ids` to `id`, so that
/// every remote workload stamps it on the events its client observes from now
/// on -- and only on those. Call this instead of writing the map directly
/// whenever a topic's id changes while workloads run (a recreate).
///
/// For each running remote workload sharing `topic_ids` it freezes the
/// stream's id for `topic` at the current one, queues a marker on the stream
/// and waits until the server has queued it, and only then writes the map.
/// The reader switches to `id` when it reads the marker. In-process workloads
/// read the map directly, in their callbacks, so they switch with the write.
///
/// Panics if a workload's server does not queue the marker within
/// [`MARK_TIMEOUT`]: stamping that stream could no longer be trusted.
pub async fn switch_topic_id(topic_ids: &TopicIds, topic: &str, id: Uuid) {
    let waits: Vec<(String, oneshot::Receiver<()>)> = {
        let streams = streams();
        let current = topic_ids
            .lock()
            .expect("topic_ids poisoned")
            .get(topic)
            .copied()
            .unwrap_or_else(Uuid::zero);
        streams
            .iter()
            .filter(|entry| Arc::ptr_eq(&entry.topic_ids, topic_ids))
            .filter_map(|entry| {
                let marker = NEXT_MARKER.fetch_add(1, Ordering::Relaxed);
                {
                    let mut view = lock_view(&entry.view);
                    // Freeze before the map changes. A topic already switched
                    // keeps its entry: its pending markers carry the chain.
                    view.ids.entry(topic.to_string()).or_insert(current);
                    view.pending.push_back((marker, topic.to_string(), id));
                }
                let (done, wait) = oneshot::channel();
                // A closed channel means the stream already ended: nothing
                // more to stamp.
                entry.marks.send(MarkRequest { marker, done }).ok()?;
                Some((entry.label.clone(), wait))
            })
            .collect()
    };
    for (label, wait) in waits {
        match tokio::time::timeout(MARK_TIMEOUT, wait).await {
            // Queued, or the workload ended (its mark loop dropped the request).
            Ok(_) => {},
            Err(_) => panic!(
                "workload {label}: MarkWorkload for the switch of {topic} to {id} did not complete within \
                 {MARK_TIMEOUT:?}; the gRPC server looks wedged"
            ),
        }
    }
    topic_ids.lock().expect("topic_ids poisoned").insert(topic.to_string(), id);
}

/// Removes a workload from [`STREAMS`] when released or dropped.
struct StreamRegistration(u64);

impl StreamRegistration {
    /// Idempotent. Drops the entry's mark sender, which ends the workload's
    /// mark loop once it has served what was already queued.
    fn release(&self) {
        streams().retain(|entry| entry.key != self.0);
    }
}

impl Drop for StreamRegistration {
    fn drop(&mut self) {
        self.release();
    }
}

impl RemoteWorkload {
    /// `channel` must have no per-request timeout: the workload's stream stays
    /// open for the whole run (see `BackendHandle::streaming_channel`).
    pub fn new(spec: WorkloadSpec, ctx: WorkloadContext, verifier: Arc<dyn Verifier>, channel: Channel) -> Self {
        let client = ChaosWorkloadServiceClient::new(channel)
            // A 1 MiB-record producer's batches can exceed tonic's 4 MiB
            // default when the harness falls behind.
            .max_decoding_message_size(usize::MAX);
        Self { spec, ctx, verifier, client, view: Arc::default() }
    }

    /// Makes this workload visible to [`switch_topic_id`]. Done before the
    /// stream is opened, so that no stream that could carry events is missed by
    /// a switch.
    fn register(&self) -> (StreamRegistration, mpsc::UnboundedReceiver<MarkRequest>) {
        let (marks, requests) = mpsc::unbounded_channel();
        let key = NEXT_STREAM_KEY.fetch_add(1, Ordering::Relaxed);
        streams().push(StreamEntry {
            key,
            label: self.spec.label(),
            topic_ids: self.ctx.topic_ids.clone(),
            view: self.view.clone(),
            marks,
        });
        (StreamRegistration(key), requests)
    }

    async fn start(&self) -> Result<tonic::Streaming<proto::WorkloadEventBatch>, tonic::Status> {
        let label = self.spec.label();
        let bootstrap = self.ctx.bootstrap_for(self.spec.backend).to_string();
        let mut client = self.client.clone();
        let response = match self.spec.role {
            Role::Producer => {
                client
                    .run_producer(proto::RunProducerRequest {
                        workload_id: label.clone(),
                        config: producer_props(&bootstrap, &label, self.ctx.msg_size, &self.ctx.security),
                        topic: self.ctx.topic.clone(),
                        target_rps: self.ctx.target_rps,
                        msg_size: u32::try_from(self.ctx.msg_size).expect("--msg-size fits in u32"),
                    })
                    .await?
            },
            Role::Consumer => {
                client
                    .run_consumer(proto::RunConsumerRequest {
                        workload_id: label.clone(),
                        config: consumer_props(
                            &bootstrap,
                            &self.ctx.group,
                            &label,
                            self.ctx.msg_size,
                            &self.ctx.security,
                        ),
                        topics: self.ctx.topics.clone(),
                        commit_mode: match self.ctx.commit_mode {
                            CommitMode::Sync => proto::CommitMode::Sync,
                            CommitMode::Async => proto::CommitMode::Async,
                        } as i32,
                        poll_timeout_ms: millis(POLL_TIMEOUT),
                        commit_check_interval_ms: millis(COMMIT_CHECK_INTERVAL),
                        msg_size: u32::try_from(self.ctx.msg_size).expect("--msg-size fits in u32"),
                    })
                    .await?
            },
        };
        Ok(response.into_inner())
    }

    /// Reads the workload's stream to its end, feeding every event to the
    /// verifier. Panics (failing the run, as a panicking in-process workload
    /// does) unless the stream ends with `Finished`.
    async fn read_events(&self, mut stream: tonic::Streaming<proto::WorkloadEventBatch>) {
        let label = self.spec.label();
        loop {
            let batch = match stream.message().await {
                Ok(Some(batch)) => batch,
                Ok(None) => panic!("workload {label}: the gRPC server ended the stream without finishing"),
                Err(status) => panic!("workload {label}: the gRPC stream failed: {status}"),
            };
            for event in batch.events {
                match event.event {
                    Some(Event::Finished(_)) => return,
                    Some(Event::Failed(failed)) => {
                        panic!("workload {label} failed on the gRPC server: {}", error_text(failed.error))
                    },
                    Some(Event::Marker(marker)) => self.on_marker(marker.marker),
                    Some(event) => self.record(event),
                    None => panic!("workload {label}: the gRPC server sent an empty event"),
                }
            }
            // `run` joins this reader with `stop_when_asked` in one future, which
            // polls the watcher only when the reader returns `Pending`. While
            // the server streams faster than the harness records (a Python
            // producer at ~90k records/s), `message()` is always ready and never
            // does, so the stop request went out minutes late. One yield per
            // batch gives the watcher its turn.
            tokio::task::yield_now().await;
        }
    }

    /// The reader reached `marker`: every later event was observed after the
    /// switch it was queued for, so apply that switch (and, defensively, any
    /// earlier one whose marker the server did not deliver).
    fn on_marker(&self, marker: u64) {
        let mut view = lock_view(&self.view);
        let Some(position) = view.pending.iter().position(|(m, _, _)| *m == marker) else {
            panic!(
                "workload {}: the gRPC server streamed an unknown marker {marker}",
                self.spec.label()
            );
        };
        let applied: Vec<_> = view.pending.drain(..=position).collect();
        for (_, topic, id) in applied {
            view.ids.insert(topic, id);
        }
    }

    /// The id to stamp on an event for `topic` at the reader's position.
    fn topic_id_for(&self, topic: &str) -> Uuid {
        let switched = lock_view(&self.view).ids.get(topic).copied();
        switched.unwrap_or_else(|| self.ctx.topic_id_for(topic))
    }

    /// One streamed event, as the in-process workload would have recorded it.
    ///
    /// Topic ids come from [`Self::topic_id_for`]: the generation current
    /// when the binding observed the event (its delivery callback or poll), not
    /// when the harness reads it, which can be minutes later -- see the module
    /// docs.
    fn record(&self, event: Event) {
        let label = self.spec.label();
        let topic = &self.ctx.topic;
        let recorded = match event {
            Event::Sent(sent) => {
                WorkloadEvent::Sent { index: sent.index, topic: topic.clone(), producer: label.clone() }
            },
            Event::Delivered(delivered) => WorkloadEvent::Delivered {
                index: delivered.index,
                topic: topic.clone(),
                topic_id: self.topic_id_for(topic),
                partition: delivered.partition,
                offset: delivered.offset,
            },
            Event::SendFailed(failed) => {
                let error = error_text(failed.error);
                eprintln!("chaos: {label} send of {topic}#{} failed: {error}", failed.index);
                WorkloadEvent::SendFailed { index: failed.index, topic: topic.clone(), error }
            },
            Event::ProducerStats(stats) => {
                // Same line as the in-process producer; the matrix runner
                // parses it.
                let secs = stats.elapsed_seconds.max(f64::EPSILON);
                eprintln!(
                    "chaos: {label} sent {} records in {secs:.1}s ({:.0} records/s, {:.1} MiB/s, target {} records/s)",
                    stats.sent,
                    stats.sent as f64 / secs,
                    stats.sent as f64 * self.ctx.msg_size as f64 / secs / (1024.0 * 1024.0),
                    self.ctx.target_rps
                );
                return;
            },
            Event::Consumed(consumed) => WorkloadEvent::Consumed {
                consumer: label,
                index: consumed.index,
                topic_id: self.topic_id_for(&consumed.topic),
                topic: consumed.topic,
                partition: consumed.partition,
                offset: consumed.offset,
            },
            Event::Corrupted(corrupted) => {
                // Same line as the in-process consumer (`record_consumed`).
                eprintln!(
                    "chaos {label}: corrupted record at {}-{} offset {}: {}",
                    corrupted.topic, corrupted.partition, corrupted.offset, corrupted.detail
                );
                WorkloadEvent::Corrupted {
                    consumer: label,
                    topic: corrupted.topic,
                    partition: corrupted.partition,
                    offset: corrupted.offset,
                    detail: corrupted.detail,
                }
            },
            Event::Rebalance(rebalance) => {
                let callback = match proto::RebalanceKind::try_from(rebalance.kind) {
                    Ok(proto::RebalanceKind::Assigned) => RebalanceCallback::Assigned,
                    Ok(proto::RebalanceKind::Revoked) => RebalanceCallback::Revoked,
                    Ok(proto::RebalanceKind::Lost) => RebalanceCallback::Lost,
                    Err(_) => panic!("workload {label}: unknown rebalance kind {}", rebalance.kind),
                };
                let mut partitions: Vec<(String, i32)> =
                    rebalance.partitions.into_iter().map(|tp| (tp.topic, tp.partition)).collect();
                partitions.sort();
                let shown: Vec<String> = partitions.iter().map(|(t, p)| format!("{t}-{p}")).collect();
                // `observed_at_unix_nanos` (the server's wall clock at the
                // callback) is not recorded: `WorkloadEvent::Rebalance` has no
                // field for it yet.
                eprintln!("chaos {label}: {callback} {shown:?}");
                WorkloadEvent::Rebalance { consumer: label, callback, partitions }
            },
            Event::Committed(committed) => WorkloadEvent::Committed {
                consumer: label,
                topic: committed.topic,
                partition: committed.partition,
                offset: committed.offset,
            },
            Event::ConsumerError(failure) => {
                let error = error_text(failure.error);
                let (op, what, error) = match proto::ConsumerOp::try_from(failure.op) {
                    Ok(proto::ConsumerOp::Poll) => (ConsumerOp::Poll, "poll error", error),
                    Ok(proto::ConsumerOp::Commit) => (ConsumerOp::Commit, "commit error", error),
                    Ok(proto::ConsumerOp::RevokeCommit) => {
                        (ConsumerOp::RevokeCommit, "commit inside on_partitions_revoked failed", error)
                    },
                    Ok(proto::ConsumerOp::ReadCommitted) => {
                        (ConsumerOp::ReadCommitted, "committed() read-back failed", error)
                    },
                    // A close error is the consumer's, not the workload's: the
                    // server still drained and goes on to ConsumerClosed and
                    // Finished, so it is recorded rather than failing the run.
                    Ok(proto::ConsumerOp::Close) => (ConsumerOp::Close, "close error", error),
                    Err(_) => panic!("workload {label}: unknown consumer operation {}", failure.op),
                };
                eprintln!("chaos {label}: {what}: {error}");
                WorkloadEvent::ConsumerError { consumer: label, op, error }
            },
            Event::ConsumerClosing(_) => WorkloadEvent::ConsumerClosing { consumer: label },
            Event::ConsumerClosed(_) => WorkloadEvent::ConsumerClosed { consumer: label },
            Event::Finished(_) | Event::Failed(_) | Event::Marker(_) => {
                unreachable!("terminal events and markers are handled in read_events")
            },
        };
        self.verifier.record(recorded);
    }

    /// Waits for the harness to set `stop`, then asks the server to stop and
    /// drain the workload, repeating the request until the stream ends (see
    /// [`STOP_RESEND_INTERVAL`]). Returns early once the stream has ended by
    /// itself.
    ///
    /// Runs alongside opening the stream, not after: the call resolves only
    /// once the server sends its response headers, and a server that holds
    /// them back until a first event would otherwise leave a workload that
    /// never emits (a consumer never assigned anything) unstoppable.
    async fn stop_when_asked(&self, stop: &AtomicBool, ended: &AtomicBool) {
        while !stop.load(Ordering::Relaxed) {
            if ended.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(STOP_POLL_INTERVAL).await;
        }
        let label = self.spec.label();
        let mut next_request = Instant::now();
        while !ended.load(Ordering::Relaxed) {
            if Instant::now() >= next_request {
                let request = proto::StopWorkloadRequest { workload_id: label.clone() };
                match self.client.clone().stop_workload(request).await {
                    Ok(response) => {
                        if let Some(error) = response.into_inner().error {
                            panic!("workload {label}: StopWorkload failed: {}", error_text(Some(error)));
                        }
                    },
                    Err(status) => panic!("workload {label}: StopWorkload failed: {status}"),
                }
                next_request = Instant::now() + STOP_RESEND_INTERVAL;
            }
            tokio::time::sleep(STOP_POLL_INTERVAL).await;
        }
    }

    /// Serves [`switch_topic_id`]'s marker requests once the server has
    /// registered the workload (`started`), one at a time and in order, so the
    /// markers land on the stream in the order their switches were queued in
    /// [`GenerationView::pending`]. Ends when the registration is released.
    async fn mark_when_asked(
        &self,
        mut requests: mpsc::UnboundedReceiver<MarkRequest>,
        started: oneshot::Receiver<()>,
    ) {
        if started.await.is_err() {
            // Opening the stream failed; the reader panics with the reason.
            return;
        }
        let label = self.spec.label();
        while let Some(MarkRequest { marker, done }) = requests.recv().await {
            let request = proto::MarkWorkloadRequest { workload_id: label.clone(), marker };
            match self.client.clone().mark_workload(request).await {
                Ok(response) => {
                    if !response.into_inner().found {
                        // The server already finished the workload: its stream
                        // carries no further events to stamp.
                        eprintln!("chaos: workload {label} had already finished when marker {marker} was queued");
                    }
                },
                Err(status) => panic!("workload {label}: MarkWorkload failed: {status}"),
            }
            let _ = done.send(());
        }
    }
}

#[async_trait(?Send)]
impl Workload for RemoteWorkload {
    fn label(&self) -> String {
        self.spec.label()
    }

    async fn run(self: Box<Self>, stop: Arc<AtomicBool>) {
        let label = self.label();
        eprintln!("chaos: starting workload {label} in its binding's gRPC server");
        let (registration, mark_requests) = self.register();
        let (started_tx, started_rx) = oneshot::channel();
        // The reader, the stop watcher and the mark loop run side by side on
        // this workload's thread; none is cancelled, so the drain events after
        // a stop are all read before the workload returns.
        let ended = AtomicBool::new(false);
        let reader = async {
            let stream = self
                .start()
                .await
                .unwrap_or_else(|status| panic!("workload {label}: starting it on the gRPC server failed: {status}"));
            let _ = started_tx.send(());
            self.read_events(stream).await;
            ended.store(true, Ordering::Relaxed);
            // No more events to stamp: stop taking switches, which also ends
            // the mark loop.
            registration.release();
        };
        tokio::join!(
            reader,
            self.stop_when_asked(&stop, &ended),
            self.mark_when_asked(mark_requests, started_rx)
        );
    }
}

/// The verifier's text for an error the server reported.
fn error_text(error: Option<proto::KafkaError>) -> String {
    match error {
        Some(error) => kafka_error_from_proto(error).to_string(),
        None => "the gRPC server reported a failure without an error".to_string(),
    }
}

fn millis(duration: Duration) -> u32 {
    u32::try_from(duration.as_millis()).expect("interval fits in u32 milliseconds")
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::time::Instant;

    use multilanguage_test_server::proto::chaos_workload_service_server::{
        ChaosWorkloadService, ChaosWorkloadServiceServer,
    };
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::transport::Endpoint;
    use tonic::transport::server::TcpIncoming;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::verifier::{ChaosVerdict, ConservationVerifier, ExpectedLossHint};
    use crate::workload::Backend;

    /// A lazy channel that never dials: for tests that only call `record`.
    fn unused_channel() -> Channel {
        Endpoint::from_static("http://127.0.0.1:9").connect_lazy()
    }

    fn ids_with(topic: &str, id: Uuid) -> TopicIds {
        Arc::new(Mutex::new(HashMap::from([(topic.to_string(), id)])))
    }

    fn workload(role: Role, topic_ids: TopicIds, verifier: Arc<dyn Verifier>, channel: Channel) -> RemoteWorkload {
        let ctx = WorkloadContext {
            bootstrap: String::new(),
            container_bootstrap: String::new(),
            security: HashMap::new(),
            topic: "t".into(),
            topics: vec!["t".into()],
            topic_ids,
            group: String::new(),
            target_rps: 0,
            msg_size: 0,
            commit_mode: CommitMode::Sync,
        };
        let spec = WorkloadSpec { role, backend: Backend::Python, instance: 1 };
        RemoteWorkload::new(spec, ctx, verifier, channel)
    }

    fn kafka_error(message: &str) -> Option<proto::KafkaError> {
        // REQUEST_TIMED_OUT: a broker code, so it round-trips unchanged.
        Some(proto::KafkaError { code: 7, message: message.into(), ..Default::default() })
    }

    fn delivered(index: u64) -> Event {
        Event::Delivered(proto::Delivered { index, partition: 0, offset: index as i64 })
    }

    /// Keeps every event, for tests that check what was recorded rather than
    /// the verdict.
    #[derive(Default)]
    struct RecordingVerifier(Mutex<Vec<WorkloadEvent>>);

    impl RecordingVerifier {
        /// `(index, topic_id)` of every recorded `Delivered`, in order.
        fn delivered_ids(&self) -> Vec<(u64, Uuid)> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter_map(|e| match e {
                    WorkloadEvent::Delivered { index, topic_id, .. } => Some((*index, *topic_id)),
                    _ => None,
                })
                .collect()
        }
    }

    impl Verifier for RecordingVerifier {
        fn record(&self, event: WorkloadEvent) {
            self.0.lock().unwrap().push(event);
        }

        fn verdict(&self, _min_partitions: usize) -> ChaosVerdict {
            unimplemented!("the recording verifier keeps events only")
        }
    }

    /// A topic never switched through `switch_topic_id` follows the live map,
    /// read when the event is recorded: sent under the old generation, the map
    /// switched, then acknowledged -- the record belongs to the new generation,
    /// and unconsumed it is scored as loss, not excused.
    #[tokio::test]
    async fn an_unswitched_topic_follows_the_live_map() {
        let old_id = Uuid::with_bytes([7u8; 16]);
        let new_id = Uuid::with_bytes([8u8; 16]);
        let ids = ids_with("t", old_id);
        let verifier = Arc::new(ConservationVerifier::new());
        let w = workload(Role::Producer, ids.clone(), verifier.clone(), unused_channel());

        w.record(Event::Sent(proto::Sent { index: 0 }));
        ids.lock().unwrap().insert("t".to_string(), new_id);
        verifier.note_expected_loss(ExpectedLossHint::DestroyedGeneration { id: old_id, deleted_at: Instant::now() });
        w.record(Event::Delivered(proto::Delivered { index: 0, partition: 3, offset: 5 }));

        let verdict = verifier.verdict(1);
        assert_eq!(verdict.unsettled_sends, 0, "{verdict}");
        assert_eq!(verdict.lost, vec![("t".to_string(), 0)], "{verdict}");
        assert_eq!(verdict.expected_lost, 0, "{verdict}");
    }

    /// `switch_topic_id` freezes a registered stream at the old id until its
    /// marker is read, even though the live map switches as soon as the marker
    /// is queued: events ahead of the marker were observed before the switch.
    /// Two switches in a row chain through their markers.
    #[tokio::test]
    async fn a_switch_takes_effect_at_its_marker_in_the_stream() {
        let (id0, id1, id2) = (
            Uuid::with_bytes([1u8; 16]),
            Uuid::with_bytes([2u8; 16]),
            Uuid::with_bytes([3u8; 16]),
        );
        let ids = ids_with("t", id0);
        let verifier = Arc::new(RecordingVerifier::default());
        let w = workload(Role::Producer, ids.clone(), verifier.clone(), unused_channel());
        let (registration, mut requests) = w.register();
        // Stands in for the mark loop: the server queued each marker.
        let markers = tokio::spawn(async move {
            let mut markers = Vec::new();
            while let Some(MarkRequest { marker, done }) = requests.recv().await {
                markers.push(marker);
                done.send(()).unwrap();
            }
            markers
        });

        switch_topic_id(&ids, "t", id1).await;
        assert_eq!(ids.lock().unwrap()["t"], id1, "the live map switches once the marker is queued");
        switch_topic_id(&ids, "t", id2).await;
        registration.release();
        let markers = markers.await.unwrap();
        assert_eq!(markers.len(), 2);

        // Read later, in stream order: the first ack was observed before both
        // switches, the second between them, the third after both.
        w.record(delivered(0));
        w.on_marker(markers[0]);
        w.record(delivered(1));
        w.on_marker(markers[1]);
        w.record(delivered(2));
        assert_eq!(verifier.delivered_ids(), vec![(0, id0), (1, id1), (2, id2)]);
    }

    /// A switch on another harness's map (another test, another run) does not
    /// mark or freeze this workload.
    #[tokio::test]
    async fn a_switch_marks_only_workloads_sharing_its_map() {
        let id = Uuid::with_bytes([4u8; 16]);
        let verifier = Arc::new(RecordingVerifier::default());
        let w = workload(Role::Producer, ids_with("t", id), verifier.clone(), unused_channel());
        let (_registration, mut requests) = w.register();

        let other = ids_with("t", id);
        switch_topic_id(&other, "t", Uuid::with_bytes([5u8; 16])).await;

        assert!(requests.try_recv().is_err(), "no marker for a workload on another map");
        w.record(delivered(0));
        assert_eq!(verifier.delivered_ids(), vec![(0, id)]);
    }

    /// A streamed failure settles its send and carries the client's error text
    /// as the Rust client would render it.
    #[tokio::test]
    async fn send_failed_settles_the_send_with_the_error_text() {
        let ids: TopicIds = Arc::new(Mutex::new(HashMap::new()));
        let verifier = Arc::new(ConservationVerifier::new());
        let w = workload(Role::Producer, ids, verifier.clone(), unused_channel());

        w.record(Event::Sent(proto::Sent { index: 4 }));
        w.record(Event::SendFailed(proto::SendFailed {
            index: 4,
            error: kafka_error("Expiring 1 record(s)"),
        }));
        // Stats are printed, not recorded.
        w.record(Event::ProducerStats(proto::ProducerStats { sent: 5, elapsed_seconds: 1.0 }));

        let verdict = verifier.verdict(0);
        assert_eq!(
            (verdict.unsettled_sends, verdict.failed_sends, verdict.delivered),
            (0, 1, 0),
            "{verdict}"
        );
        let reason = verdict
            .reasons
            .iter()
            .find(|r| r.starts_with("failed sends"))
            .expect("failed-sends reason");
        assert!(reason.contains("t#4: ") && reason.contains("Expiring 1 record(s)"), "{reason}");
    }

    /// A remote consumer's `Corrupted` fails the run, as the Rust consumer's
    /// does, and is not counted as a consumption.
    #[tokio::test]
    async fn a_remote_corrupted_record_fails_the_run() {
        let verifier = Arc::new(ConservationVerifier::new());
        let w = workload(
            Role::Consumer,
            ids_with("t", Uuid::with_bytes([1u8; 16])),
            verifier.clone(),
            unused_channel(),
        );
        w.record(Event::Corrupted(proto::Corrupted {
            topic: "t".into(),
            partition: 2,
            offset: 9,
            detail: "key is 3 byte(s), expected the 8-byte index".into(),
        }));

        let verdict = verifier.verdict(0);
        assert_eq!(verdict.corrupted_records, 1, "{verdict}");
        assert!(!verdict.is_pass(), "{verdict}");
        let reason = verdict
            .reasons
            .iter()
            .find(|r| r.starts_with("corrupted records"))
            .expect("corrupted-records reason");
        assert!(
            reason.contains("consumer-python-1: t-2 offset 9: key is 3 byte(s), expected the 8-byte index"),
            "{reason}"
        );
    }

    /// A remote consumer's listener events drive the same checks as the Rust
    /// listener's: the assignment, the consumption since it, and a committed
    /// read-back that must be that consumption + 1. Each consumer error lands
    /// under its own operation.
    #[tokio::test]
    async fn consumer_events_feed_the_listener_and_commit_checks() {
        let id = Uuid::with_bytes([1u8; 16]);
        let verifier = Arc::new(ConservationVerifier::new());
        let w = workload(Role::Consumer, ids_with("t", id), verifier.clone(), unused_channel());
        let tp = |p| proto::TopicPartitionRef { topic: "t".into(), partition: p };

        w.record(Event::Rebalance(proto::Rebalance {
            kind: proto::RebalanceKind::Assigned as i32,
            partitions: vec![tp(1), tp(0)],
            observed_at_unix_nanos: 1,
        }));
        w.record(Event::Consumed(proto::Consumed {
            index: 0,
            topic: "t".into(),
            partition: 0,
            offset: 41,
        }));
        w.record(Event::Consumed(proto::Consumed {
            index: 1,
            topic: "t".into(),
            partition: 1,
            offset: 7,
        }));
        w.record(Event::Committed(proto::Committed {
            topic: "t".into(),
            partition: 0,
            offset: 42,
        }));
        w.record(Event::Committed(proto::Committed {
            topic: "t".into(),
            partition: 1,
            offset: 3,
        }));
        for op in [
            proto::ConsumerOp::Poll,
            proto::ConsumerOp::Commit,
            proto::ConsumerOp::RevokeCommit,
            proto::ConsumerOp::ReadCommitted,
        ] {
            w.record(Event::ConsumerError(proto::ConsumerError {
                op: op as i32,
                error: kafka_error("boom"),
            }));
        }
        w.record(Event::Rebalance(proto::Rebalance {
            kind: proto::RebalanceKind::Revoked as i32,
            partitions: vec![tp(0), tp(1)],
            observed_at_unix_nanos: 2,
        }));
        w.record(Event::ConsumerClosing(proto::ConsumerClosing {}));
        w.record(Event::ConsumerClosed(proto::ConsumerClosed {}));

        let verdict = verifier.verdict(0);
        assert_eq!(
            (verdict.assigned_callbacks, verdict.revoked_callbacks, verdict.lost_callbacks),
            (1, 1, 0)
        );
        assert_eq!(verdict.listener_consumers, 1, "{verdict}");
        assert!(verdict.rebalance_violations.is_empty(), "{verdict}");
        assert_eq!(verdict.commit_checks, 2, "{verdict}");
        assert_eq!(
            verdict.commit_violations,
            vec![
                "consumer-python-1: committed offset 3 on t-1 is behind its own consumption (last consumed 7, \
                 expected 8)"
                    .to_string()
            ],
            "{verdict}"
        );
        assert_eq!(
            (verdict.poll_errors, verdict.commit_errors, verdict.revoke_commit_errors),
            (1, 1, 1),
            "{verdict}"
        );
        assert!(
            verdict
                .error_breakdown
                .iter()
                .any(|(text, n)| *n == 1 && text.starts_with("consumer committed() read-back: ")),
            "{verdict}"
        );
    }

    /// A close() error is recorded as a consumer error naming close() -- the
    /// workload still drained -- rather than failing the run as `Failed` does.
    #[tokio::test]
    async fn a_close_error_is_recorded_not_fatal() {
        let verifier = Arc::new(RecordingVerifier::default());
        let w = workload(Role::Consumer, ids_with("t", Uuid::zero()), verifier.clone(), unused_channel());

        w.record(Event::ConsumerError(proto::ConsumerError {
            op: proto::ConsumerOp::Close as i32,
            error: kafka_error("close timed out"),
        }));
        w.record(Event::ConsumerClosed(proto::ConsumerClosed {}));

        let events = verifier.0.lock().unwrap();
        match &events[..] {
            [
                WorkloadEvent::ConsumerError { op, error, .. },
                WorkloadEvent::ConsumerClosed { .. },
            ] => {
                assert_eq!(*op, ConsumerOp::Close);
                assert!(error.contains("close timed out"), "{error}");
            },
            other => panic!("unexpected events {other:?}"),
        }
    }

    // ── Against a fake ChaosWorkloadService ──

    /// One workload's server-side state: events queued but not streamed yet
    /// (held back until `released`, to stand in for a harness that has fallen
    /// behind), and what the harness asked for.
    #[derive(Default)]
    struct FakeState {
        queued: Vec<proto::WorkloadEvent>,
        released: bool,
        stopped: bool,
        markers: Vec<u64>,
    }

    impl FakeState {
        fn push(&mut self, event: Event) {
            self.queued.push(proto::WorkloadEvent { event: Some(event) });
        }
    }

    /// Serves one workload. With `defer_headers`, a Run* call does not respond
    /// (so the client's call does not resolve) until the workload is stopped --
    /// how a server that sends its headers only with the first event treats a
    /// consumer that never emits.
    #[derive(Clone)]
    struct FakeServer {
        state: Arc<Mutex<FakeState>>,
        defer_headers: bool,
    }

    type EventStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::WorkloadEventBatch, Status>> + Send>>;

    impl FakeServer {
        async fn run(&self) -> Result<Response<EventStream>, Status> {
            while self.defer_headers && !self.state.lock().unwrap().stopped {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let (tx, rx) = mpsc::unbounded_channel();
            let state = self.state.clone();
            tokio::spawn(async move {
                loop {
                    let batch: Vec<_> = {
                        let mut s = state.lock().unwrap();
                        if s.released {
                            s.queued.drain(..).collect()
                        } else {
                            Vec::new()
                        }
                    };
                    let finished = batch.iter().any(|e| matches!(e.event, Some(Event::Finished(_))));
                    if !batch.is_empty() && tx.send(Ok(proto::WorkloadEventBatch { events: batch })).is_err() {
                        return;
                    }
                    if finished {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            });
            Ok(Response::new(Box::pin(UnboundedReceiverStream::new(rx))))
        }
    }

    #[tonic::async_trait]
    impl ChaosWorkloadService for FakeServer {
        type RunProducerStream = EventStream;
        type RunConsumerStream = EventStream;

        async fn run_producer(
            &self,
            _request: Request<proto::RunProducerRequest>,
        ) -> Result<Response<EventStream>, Status> {
            self.run().await
        }

        async fn run_consumer(
            &self,
            _request: Request<proto::RunConsumerRequest>,
        ) -> Result<Response<EventStream>, Status> {
            self.run().await
        }

        async fn stop_workload(
            &self,
            _request: Request<proto::StopWorkloadRequest>,
        ) -> Result<Response<proto::StatusResponse>, Status> {
            let mut s = self.state.lock().unwrap();
            if !s.stopped {
                s.stopped = true;
                s.released = true;
                s.push(Event::Finished(proto::Finished {}));
            }
            Ok(Response::new(proto::StatusResponse { error: None }))
        }

        async fn mark_workload(
            &self,
            request: Request<proto::MarkWorkloadRequest>,
        ) -> Result<Response<proto::MarkWorkloadResponse>, Status> {
            let marker = request.into_inner().marker;
            let mut s = self.state.lock().unwrap();
            s.markers.push(marker);
            s.push(Event::Marker(proto::Marker { marker }));
            Ok(Response::new(proto::MarkWorkloadResponse { found: true }))
        }
    }

    /// Serves `server` on an ephemeral port; returns a channel to it.
    async fn serve(server: FakeServer) -> Channel {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(ChaosWorkloadServiceServer::new(server))
                .serve_with_incoming(incoming),
        );
        Endpoint::from_shared(format!("http://{addr}")).unwrap().connect_lazy()
    }

    /// The end-to-end case the markers exist for: acks the client observed
    /// before a recreate reach the harness only after it switched its map
    /// (here: held back on the server until after the switch). They keep the
    /// old id; an ack observed after the switch gets the new one.
    #[tokio::test]
    async fn run_stamps_by_stream_position_not_by_arrival() {
        let (old_id, new_id) = (Uuid::with_bytes([7u8; 16]), Uuid::with_bytes([8u8; 16]));
        let ids = ids_with("t", old_id);
        let state = Arc::new(Mutex::new(FakeState::default()));
        state.lock().unwrap().push(Event::Sent(proto::Sent { index: 0 }));
        state.lock().unwrap().push(delivered(0));
        let channel = serve(FakeServer { state: state.clone(), defer_headers: false }).await;
        let verifier = Arc::new(RecordingVerifier::default());
        let w = Box::new(workload(Role::Producer, ids.clone(), verifier.clone(), channel));
        let stop = Arc::new(AtomicBool::new(false));

        let harness = async {
            // `run` registers the workload on its first poll; switch only once
            // it has, as the harness's recreate only happens mid-run.
            while !streams().iter().any(|e| Arc::ptr_eq(&e.topic_ids, &ids)) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            switch_topic_id(&ids, "t", new_id).await;
            {
                let mut s = state.lock().unwrap();
                s.push(Event::Sent(proto::Sent { index: 1 }));
                s.push(delivered(1));
                s.released = true;
            }
            stop.store(true, Ordering::Relaxed);
        };
        tokio::time::timeout(Duration::from_secs(20), async { tokio::join!(w.run(stop.clone()), harness) })
            .await
            .expect("the workload finished");

        assert_eq!(state.lock().unwrap().markers.len(), 1);
        assert_eq!(verifier.delivered_ids(), vec![(0, old_id), (1, new_id)]);
        assert!(
            streams().iter().all(|e| !Arc::ptr_eq(&e.topic_ids, &ids)),
            "deregistered at the end"
        );
    }

    /// A workload whose server answers its Run* call only once stopped (a
    /// server sending headers with the first event, for a consumer that never
    /// emits) can still be stopped: the stop watcher does not wait for the call
    /// to resolve.
    #[tokio::test]
    async fn run_stops_a_workload_before_its_stream_opens() {
        let state = Arc::new(Mutex::new(FakeState::default()));
        let channel = serve(FakeServer { state: state.clone(), defer_headers: true }).await;
        let verifier = Arc::new(RecordingVerifier::default());
        let w = Box::new(workload(Role::Consumer, ids_with("t", Uuid::zero()), verifier, channel));
        let stop = Arc::new(AtomicBool::new(false));

        let harness = async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            stop.store(true, Ordering::Relaxed);
        };
        tokio::time::timeout(Duration::from_secs(20), async { tokio::join!(w.run(stop.clone()), harness) })
            .await
            .expect("the workload stopped although its stream never opened before the stop");
        assert!(state.lock().unwrap().stopped);
    }
}
