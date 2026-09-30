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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use multilanguage_test_server::proto;
use multilanguage_test_server::proto::chaos_workload_service_client::ChaosWorkloadServiceClient;
use multilanguage_test_server::proto::workload_event::Event;
use tonic::transport::Channel;

use super::common::multilanguage_producer::kafka_error_from_proto;
use super::verifier::{ConsumerOp, RebalanceCallback, Verifier, WorkloadEvent};
use super::workload::{COMMIT_CHECK_INTERVAL, CommitMode, POLL_TIMEOUT, Role, Workload, WorkloadContext, WorkloadSpec};
use super::workload_config::{consumer_props, producer_props};

/// How often the stop watcher checks the workload's stop flag.
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// A workload run by a binding's gRPC server (`ChaosWorkloadService`).
pub struct RemoteWorkload {
    spec: WorkloadSpec,
    ctx: WorkloadContext,
    verifier: Arc<dyn Verifier>,
    client: ChaosWorkloadServiceClient<Channel>,
}

impl RemoteWorkload {
    /// `channel` must have no per-request timeout: the workload's stream stays
    /// open for the whole run (see `BackendHandle::streaming_channel`).
    pub fn new(spec: WorkloadSpec, ctx: WorkloadContext, verifier: Arc<dyn Verifier>, channel: Channel) -> Self {
        let client = ChaosWorkloadServiceClient::new(channel)
            // A 1 MiB-record producer's batches can exceed tonic's 4 MiB
            // default when the harness falls behind.
            .max_decoding_message_size(usize::MAX);
        Self { spec, ctx, verifier, client }
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

    /// One streamed event, as the in-process workload would have recorded it.
    ///
    /// Topic ids are stamped here, from the harness's live map, when the event
    /// arrives -- the same ack-time rule as `workload::delivery_callback`, whose
    /// comment explains why. The stream adds milliseconds to when the harness
    /// learns of an ack, well inside the margin that rule relies on.
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
                topic_id: self.ctx.topic_id_for(topic),
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
                topic_id: self.ctx.topic_id_for(&consumed.topic),
                topic: consumed.topic,
                partition: consumed.partition,
                offset: consumed.offset,
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
                let (op, what) = match proto::ConsumerOp::try_from(failure.op) {
                    Ok(proto::ConsumerOp::Poll) => (ConsumerOp::Poll, "poll error"),
                    Ok(proto::ConsumerOp::Commit) => (ConsumerOp::Commit, "commit error"),
                    Ok(proto::ConsumerOp::RevokeCommit) => {
                        (ConsumerOp::RevokeCommit, "commit inside on_partitions_revoked failed")
                    },
                    Ok(proto::ConsumerOp::ReadCommitted) => (ConsumerOp::ReadCommitted, "committed() read-back failed"),
                    Err(_) => panic!("workload {label}: unknown consumer operation {}", failure.op),
                };
                let error = error_text(failure.error);
                eprintln!("chaos {label}: {what}: {error}");
                WorkloadEvent::ConsumerError { consumer: label, op, error }
            },
            Event::ConsumerClosing(_) => WorkloadEvent::ConsumerClosing { consumer: label },
            Event::ConsumerClosed(_) => WorkloadEvent::ConsumerClosed { consumer: label },
            Event::Finished(_) | Event::Failed(_) => unreachable!("terminal events end the stream in read_events"),
        };
        self.verifier.record(recorded);
    }

    /// Waits for the harness to set `stop`, then asks the server to stop and
    /// drain the workload. Returns early once the stream has ended by itself.
    async fn stop_when_asked(&self, stop: &AtomicBool, ended: &AtomicBool) {
        while !stop.load(Ordering::Relaxed) {
            if ended.load(Ordering::Relaxed) {
                return;
            }
            tokio::time::sleep(STOP_POLL_INTERVAL).await;
        }
        let label = self.spec.label();
        let request = proto::StopWorkloadRequest { workload_id: label.clone() };
        match self.client.clone().stop_workload(request).await {
            Ok(response) => {
                if let Some(error) = response.into_inner().error {
                    panic!("workload {label}: StopWorkload failed: {}", error_text(Some(error)));
                }
            },
            Err(status) => panic!("workload {label}: StopWorkload failed: {status}"),
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
        let stream = self
            .start()
            .await
            .unwrap_or_else(|status| panic!("workload {label}: starting it on the gRPC server failed: {status}"));
        // The reader and the stop watcher run side by side on this workload's
        // thread; neither is cancelled, so the drain events after a stop are
        // all read before the workload returns.
        let ended = AtomicBool::new(false);
        let reader = async {
            self.read_events(stream).await;
            ended.store(true, Ordering::Relaxed);
        };
        tokio::join!(reader, self.stop_when_asked(&stop, &ended));
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
    use std::collections::HashMap;
    use std::time::Instant;

    use confluent_kafka::common::Uuid;
    use tonic::transport::Endpoint;

    use super::*;
    use crate::verifier::{ConservationVerifier, ExpectedLossHint};
    use crate::workload::{Backend, TopicIds};

    /// A workload whose client is never used: `record` only maps events, and a
    /// lazy channel does not dial until a call is made.
    fn workload(role: Role, topic_ids: TopicIds, verifier: Arc<ConservationVerifier>) -> RemoteWorkload {
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
        let channel = Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
        RemoteWorkload::new(spec, ctx, verifier, channel)
    }

    fn kafka_error(message: &str) -> Option<proto::KafkaError> {
        // REQUEST_TIMED_OUT: a broker code, so it round-trips unchanged.
        Some(proto::KafkaError { code: 7, message: message.into(), ..Default::default() })
    }

    /// A streamed ack is stamped with the topic id current when it ARRIVES,
    /// the same rule as the in-process `delivery_callback`: sent under the old
    /// generation, recreated, then acknowledged — the record belongs to the new
    /// generation, and unconsumed it is scored as loss, not excused.
    #[tokio::test]
    async fn delivered_is_stamped_with_the_generation_current_on_arrival() {
        let old_id = Uuid::with_bytes([7u8; 16]);
        let new_id = Uuid::with_bytes([8u8; 16]);
        let ids: TopicIds = Arc::new(std::sync::Mutex::new(HashMap::from([("t".to_string(), old_id)])));
        let verifier = Arc::new(ConservationVerifier::new());
        let w = workload(Role::Producer, ids.clone(), verifier.clone());

        w.record(Event::Sent(proto::Sent { index: 0 }));
        ids.lock().unwrap().insert("t".to_string(), new_id);
        verifier.note_expected_loss(ExpectedLossHint::DestroyedGeneration { id: old_id, deleted_at: Instant::now() });
        w.record(Event::Delivered(proto::Delivered { index: 0, partition: 3, offset: 5 }));

        let verdict = verifier.verdict(1);
        assert_eq!(verdict.unsettled_sends, 0, "{verdict}");
        assert_eq!(verdict.lost, vec![("t".to_string(), 0)], "{verdict}");
        assert_eq!(verdict.expected_lost, 0, "{verdict}");
    }

    /// A streamed failure settles its send and carries the client's error text
    /// as the Rust client would render it.
    #[tokio::test]
    async fn send_failed_settles_the_send_with_the_error_text() {
        let ids: TopicIds = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let verifier = Arc::new(ConservationVerifier::new());
        let w = workload(Role::Producer, ids, verifier.clone());

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

    /// A remote consumer's listener events drive the same checks as the Rust
    /// listener's: the assignment, the consumption since it, and a committed
    /// read-back that must be that consumption + 1. Each consumer error lands
    /// under its own operation.
    #[tokio::test]
    async fn consumer_events_feed_the_listener_and_commit_checks() {
        let id = Uuid::with_bytes([1u8; 16]);
        let ids: TopicIds = Arc::new(std::sync::Mutex::new(HashMap::from([("t".to_string(), id)])));
        let verifier = Arc::new(ConservationVerifier::new());
        let w = workload(Role::Consumer, ids, verifier.clone());
        let tp = |p| proto::TopicPartitionRef { topic: "t".into(), partition: p };

        w.record(Event::Rebalance(proto::Rebalance {
            kind: proto::RebalanceKind::Assigned as i32,
            partitions: vec![tp(1), tp(0)],
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
}
