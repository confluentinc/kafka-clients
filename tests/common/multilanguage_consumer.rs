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

//! A `Consumer<Vec<u8>, Vec<u8>>` implementation that tunnels every supported
//! call over gRPC to a server in another language (Python or C++), mirroring
//! [`crate::common::multilanguage_producer::MultilanguageProducer`].
//!
//! Scope: only the surface the C FFI / Python binding support is bridged
//! (poll, subscribe(topics), assign, commit_sync, committed, position, seek,
//! pause/resume, *_offsets, offsets_for_times, partitions_for, list_topics,
//! the non-blocking state reads, wakeup, close).
//!
//! The callback-taking trait methods (`subscribe_with_listener`,
//! `commit_async_*_with_callback`) return an `illegal_state` error, and
//! deliberately keep doing so even though the bindings *do* bridge those
//! callbacks now: a `dyn ConsumerRebalanceListener` living in this process
//! cannot be handed to a server in another one. Callback coverage for the gRPC
//! backends goes through [`crate::common::callback_log::ConsumerCallbackLog`]
//! instead, which asks the server to register a listener built by its own
//! binding and reads back what it observed. Regex pattern subscription remains
//! unbridged entirely; tests needing it are native-Rust-only.
//!
//! The trait's blocking-in-Java methods are `async` and forward to a unary RPC
//! that the server awaits. The handful of methods that are *sync* in the trait
//! (`assignment`, `subscription`, `paused`, `wakeup`) still need a server
//! round-trip; they use `block_in_place` + `Handle::block_on`, which is valid
//! because the multilanguage tests run on the multi-thread runtime.
//!
//! Used only when `--features multilanguage-tests` is enabled.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use confluent_kafka::common::header::{RecordHeader, RecordHeaders};
use confluent_kafka::common::record::TimestampType;
use confluent_kafka::common::{KafkaError, PartitionInfo, TopicPartition};
use confluent_kafka::consumer::{
    CloseOptions, Consumer, ConsumerGroupMetadata, ConsumerHandle, ConsumerRebalanceListener, ConsumerRecord,
    ConsumerRecords, KafkaMetric, MetricName, OffsetAndMetadata, OffsetAndTimestamp, OffsetCommitCallback,
    SubscriptionPattern,
};
use indexmap::IndexMap;
use multilanguage_test_server::proto::consumer_service_client::ConsumerServiceClient;
use multilanguage_test_server::proto::{self};
use tonic::transport::Channel;

use crate::common::multilanguage_producer::{kafka_error_from_proto, partition_info_from_proto, status_to_kafka_error};

/// gRPC-backed `Consumer` driven by an out-of-process server in another
/// language that ultimately calls the same Rust client through a binding.
pub struct MultilanguageConsumer {
    consumer_id: u64,
    client: ConsumerServiceClient<Channel>,
    backend: &'static str,
    client_id: String,
}

impl MultilanguageConsumer {
    /// Connect to `channel` and create a server-side consumer from `config`
    /// (empty config selects a MockConsumer server-side).
    pub async fn new(
        channel: Channel,
        config: HashMap<String, String>,
        backend: &'static str,
    ) -> Result<Self, KafkaError> {
        let client_id = config
            .get("client.id")
            .cloned()
            .unwrap_or_else(|| format!("multilanguage-{backend}"));
        let mut client = ConsumerServiceClient::new(channel);
        let response = client
            .create_consumer(proto::CreateConsumerRequest { config })
            .await
            .map_err(|status| status_to_kafka_error(&status, backend))?
            .into_inner();
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(Self { consumer_id: response.consumer_id, client, backend, client_id })
    }

    /// The server-local consumer id. Needed by
    /// [`crate::common::callback_log::grpc::ConsumerLog`], which drives the
    /// callback-registering RPCs and `GetCallbackLog` against the same
    /// server-side consumer.
    pub fn consumer_id(&self) -> u64 {
        self.consumer_id
    }

    fn unsupported(&self, method: &str) -> KafkaError {
        KafkaError::illegal_state(format!(
            "{} is not supported on the {} gRPC multilanguage backend (an in-process callback cannot cross the wire — use ConsumerCallbackLog; pattern subscription is unbridged)",
            method, self.backend
        ))
    }

    // ── async RPC helpers (shared by the trait methods and the sync wrappers) ──

    async fn status_rpc<Fut>(&self, fut: Fut) -> Result<(), KafkaError>
    where
        Fut: std::future::Future<Output = Result<tonic::Response<proto::StatusResponse>, tonic::Status>>,
    {
        let response = fut.await.map_err(|s| status_to_kafka_error(&s, self.backend))?.into_inner();
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(()),
        }
    }

    async fn subscribe_rpc(&self, topics: Vec<String>) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        // with_listener stays false here: this is the plain subscribe(topics).
        // The listener-registering variant lives on ConsumerCallbackLog, since
        // the listener must be created by the server's own binding.
        let req = proto::SubscribeRequest { consumer_id: self.consumer_id, topics, with_listener: false };
        self.status_rpc(client.subscribe(req)).await
    }

    async fn assign_rpc(&self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let req = proto::AssignRequest { consumer_id: self.consumer_id, partitions: tps_to_proto(partitions) };
        self.status_rpc(client.assign(req)).await
    }

    async fn tp_list_rpc(&self, partitions: &[TopicPartition], which: TpListOp) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let req =
            proto::TopicPartitionListRequest { consumer_id: self.consumer_id, partitions: tps_to_proto(partitions) };
        match which {
            TpListOp::SeekToBeginning => self.status_rpc(client.seek_to_beginning(req)).await,
            TpListOp::SeekToEnd => self.status_rpc(client.seek_to_end(req)).await,
            TpListOp::Pause => self.status_rpc(client.pause(req)).await,
            TpListOp::Resume => self.status_rpc(client.resume(req)).await,
        }
    }

    async fn poll_rpc(&self, timeout: Duration) -> Result<ConsumerRecords<Vec<u8>, Vec<u8>>, KafkaError> {
        let mut client = self.client.clone();
        let req = proto::PollRequest { consumer_id: self.consumer_id, timeout_ms: timeout.as_millis() as i64 };
        let response = client
            .poll(req)
            .await
            .map_err(|s| status_to_kafka_error(&s, self.backend))?
            .into_inner();
        match response.result {
            Some(proto::poll_response::Result::Records(list)) => Ok(consumer_records_from_proto(list)),
            Some(proto::poll_response::Result::Error(e)) => Err(kafka_error_from_proto(e)),
            None => Ok(ConsumerRecords::empty()),
        }
    }

    async fn commit_rpc(&self, offsets: Vec<proto::OffsetMapEntry>) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let req = proto::CommitSyncRequest { consumer_id: self.consumer_id, offsets };
        self.status_rpc(client.commit_sync(req)).await
    }

    async fn committed_rpc(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError> {
        let mut client = self.client.clone();
        let req = proto::CommittedRequest { consumer_id: self.consumer_id, partitions: tps_to_proto(partitions) };
        let response = client
            .committed(req)
            .await
            .map_err(|s| status_to_kafka_error(&s, self.backend))?
            .into_inner();
        match response.result {
            Some(proto::committed_response::Result::Offsets(map)) => Ok(offset_map_from_proto(map)),
            Some(proto::committed_response::Result::Error(e)) => Err(kafka_error_from_proto(e)),
            None => Ok(HashMap::new()),
        }
    }

    async fn position_rpc(&self, partition: &TopicPartition) -> Result<i64, KafkaError> {
        let mut client = self.client.clone();
        let req = proto::PositionRequest { consumer_id: self.consumer_id, partition: Some(tp_to_proto(partition)) };
        let response = client
            .position(req)
            .await
            .map_err(|s| status_to_kafka_error(&s, self.backend))?
            .into_inner();
        match response.result {
            Some(proto::position_response::Result::Offset(o)) => Ok(o),
            Some(proto::position_response::Result::Error(e)) => Err(kafka_error_from_proto(e)),
            None => Err(self.empty_response("Position")),
        }
    }

    async fn long_offsets_rpc(
        &self,
        partitions: &[TopicPartition],
        end: bool,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        let mut client = self.client.clone();
        let req =
            proto::TopicPartitionListRequest { consumer_id: self.consumer_id, partitions: tps_to_proto(partitions) };
        let response = if end {
            client.end_offsets(req).await
        } else {
            client.beginning_offsets(req).await
        }
        .map_err(|s| status_to_kafka_error(&s, self.backend))?
        .into_inner();
        match response.result {
            Some(proto::long_offsets_response::Result::Offsets(map)) => Ok(long_offset_map_from_proto(map)),
            Some(proto::long_offsets_response::Result::Error(e)) => Err(kafka_error_from_proto(e)),
            None => Ok(HashMap::new()),
        }
    }

    async fn partitions_for_rpc(&self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        let mut client = self.client.clone();
        let req = proto::ConsumerPartitionsForRequest { consumer_id: self.consumer_id, topic: topic.to_string() };
        let response = client
            .partitions_for(req)
            .await
            .map_err(|s| status_to_kafka_error(&s, self.backend))?
            .into_inner();
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(response.partitions.into_iter().map(partition_info_from_proto).collect())
    }

    fn empty_response(&self, rpc: &str) -> KafkaError {
        KafkaError::illegal_state(format!("{} backend returned empty {} response", self.backend, rpc))
    }

    /// Run an async RPC to completion from a sync trait method. Valid on the
    /// multi-thread runtime the multilanguage tests use.
    fn block<T>(&self, fut: impl std::future::Future<Output = T>) -> T {
        tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(fut))
    }
}

enum TpListOp {
    SeekToBeginning,
    SeekToEnd,
    Pause,
    Resume,
}

#[async_trait]
impl Consumer<Vec<u8>, Vec<u8>> for MultilanguageConsumer {
    // ── non-blocking state reads (sync in the trait) ──
    fn assignment(&self) -> HashSet<TopicPartition> {
        let mut client = self.client.clone();
        let id = self.consumer_id;
        let backend = self.backend;
        self.block(async move {
            let resp = client
                .assignment(proto::ConsumerIdRequest { consumer_id: id })
                .await
                .map_err(|s| status_to_kafka_error(&s, backend))
                .expect("assignment RPC failed")
                .into_inner();
            match resp.result {
                Some(proto::topic_partition_list_response::Result::Partitions(list)) => {
                    list.partitions.into_iter().map(tp_from_proto).collect()
                },
                _ => HashSet::new(),
            }
        })
    }

    fn subscription(&self) -> HashSet<String> {
        let mut client = self.client.clone();
        let id = self.consumer_id;
        let backend = self.backend;
        self.block(async move {
            let resp = client
                .subscription(proto::ConsumerIdRequest { consumer_id: id })
                .await
                .map_err(|s| status_to_kafka_error(&s, backend))
                .expect("subscription RPC failed")
                .into_inner();
            match resp.result {
                Some(proto::subscription_response::Result::Topics(list)) => list.values.into_iter().collect(),
                _ => HashSet::new(),
            }
        })
    }

    fn paused(&self) -> HashSet<TopicPartition> {
        let mut client = self.client.clone();
        let id = self.consumer_id;
        let backend = self.backend;
        self.block(async move {
            let resp = client
                .paused(proto::ConsumerIdRequest { consumer_id: id })
                .await
                .map_err(|s| status_to_kafka_error(&s, backend))
                .expect("paused RPC failed")
                .into_inner();
            match resp.result {
                Some(proto::topic_partition_list_response::Result::Partitions(list)) => {
                    list.partitions.into_iter().map(tp_from_proto).collect()
                },
                _ => HashSet::new(),
            }
        })
    }

    fn group_metadata(&self) -> ConsumerGroupMetadata {
        unimplemented!("group_metadata is not supported on the gRPC multilanguage backend")
    }

    fn client_id(&self) -> &str {
        &self.client_id
    }

    fn current_lag(&self, _topic_partition: &TopicPartition) -> Option<i64> {
        unimplemented!("current_lag is not supported on the gRPC multilanguage backend")
    }

    fn wakeup(&self) {
        let mut client = self.client.clone();
        let id = self.consumer_id;
        // Best-effort: ignore transport/None result. wakeup never errors in Java.
        self.block(async move {
            let _ = client.wakeup(proto::ConsumerIdRequest { consumer_id: id }).await;
        });
    }

    /// The server-side consumer owns the real handle; there is no RPC that
    /// could hand a `Clone + Send + Sync` reentrancy handle across the wire.
    /// `Consumer::wakeup` above is bridged (as a Wakeup RPC), which is the only
    /// part of the handle the multilanguage tests need.
    ///
    /// (Replaces the pre-Phase-41 `wakeup_handle` method, which no longer
    /// exists on the trait.)
    fn handle(&self) -> ConsumerHandle {
        unimplemented!("handle is not supported on the gRPC multilanguage backend")
    }

    /// The consumer metrics live in the server process and are not modeled on
    /// the wire; no multilanguage test asserts on them.
    fn metrics(&self) -> HashMap<MetricName, Arc<KafkaMetric>> {
        unimplemented!("metrics is not supported on the gRPC multilanguage backend")
    }

    // ── subscription / assignment ──
    async fn subscribe(&mut self, topics: Vec<String>) -> Result<(), KafkaError> {
        self.subscribe_rpc(topics).await
    }

    async fn subscribe_with_listener(
        &mut self,
        _topics: Vec<String>,
        _listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError> {
        Err(self.unsupported("subscribe_with_listener"))
    }

    async fn subscribe_pattern(&mut self, _pattern: SubscriptionPattern) -> Result<(), KafkaError> {
        Err(self.unsupported("subscribe_pattern"))
    }

    async fn subscribe_pattern_with_listener(
        &mut self,
        _pattern: SubscriptionPattern,
        _listener: Arc<dyn ConsumerRebalanceListener>,
    ) -> Result<(), KafkaError> {
        Err(self.unsupported("subscribe_pattern_with_listener"))
    }

    async fn assign(&mut self, partitions: Vec<TopicPartition>) -> Result<(), KafkaError> {
        self.assign_rpc(&partitions).await
    }

    async fn unsubscribe(&mut self) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        self.status_rpc(client.unsubscribe(proto::ConsumerIdRequest { consumer_id: self.consumer_id }))
            .await
    }

    // ── poll ──
    async fn poll(&mut self, timeout: Duration) -> Result<ConsumerRecords<Vec<u8>, Vec<u8>>, KafkaError> {
        self.poll_rpc(timeout).await
    }

    // ── commit ──
    async fn commit_sync(&mut self) -> Result<(), KafkaError> {
        self.commit_rpc(Vec::new()).await
    }

    async fn commit_sync_timeout(&mut self, _timeout: Duration) -> Result<(), KafkaError> {
        self.commit_rpc(Vec::new()).await
    }

    async fn commit_sync_offsets(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<(), KafkaError> {
        self.commit_rpc(offset_map_to_proto(&offsets)).await
    }

    async fn commit_sync_offsets_timeout(
        &mut self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        _timeout: Duration,
    ) -> Result<(), KafkaError> {
        self.commit_rpc(offset_map_to_proto(&offsets)).await
    }

    async fn commit_async(&mut self) -> Result<(), KafkaError> {
        // Mapped to a synchronous commit server-side; the offset ends up
        // committed. Tests that depend on async-not-yet-committed timing are
        // native-Rust-only and never reach this backend.
        self.commit_rpc(Vec::new()).await
    }

    async fn commit_async_with_callback(&mut self, _callback: Arc<dyn OffsetCommitCallback>) -> Result<(), KafkaError> {
        Err(self.unsupported("commit_async_with_callback"))
    }

    async fn commit_async_offsets_with_callback(
        &mut self,
        _offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        _callback: Arc<dyn OffsetCommitCallback>,
    ) -> Result<(), KafkaError> {
        Err(self.unsupported("commit_async_offsets_with_callback"))
    }

    // ── seek ──
    async fn seek(&mut self, partition: TopicPartition, offset: i64) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let req = proto::SeekRequest {
            consumer_id: self.consumer_id,
            partition: Some(tp_to_proto(&partition)),
            offset,
            leader_epoch: None,
            metadata: None,
        };
        self.status_rpc(client.seek(req)).await
    }

    async fn seek_with_metadata(
        &mut self,
        partition: TopicPartition,
        offset_and_metadata: OffsetAndMetadata,
    ) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let req = proto::SeekRequest {
            consumer_id: self.consumer_id,
            partition: Some(tp_to_proto(&partition)),
            offset: offset_and_metadata.offset(),
            leader_epoch: offset_and_metadata.leader_epoch(),
            metadata: Some(offset_and_metadata.metadata().to_string()),
        };
        self.status_rpc(client.seek(req)).await
    }

    async fn seek_to_beginning(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.tp_list_rpc(partitions, TpListOp::SeekToBeginning).await
    }

    async fn seek_to_end(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.tp_list_rpc(partitions, TpListOp::SeekToEnd).await
    }

    // ── position / committed ──
    async fn position(&mut self, partition: &TopicPartition) -> Result<i64, KafkaError> {
        self.position_rpc(partition).await
    }

    async fn position_timeout(&mut self, partition: &TopicPartition, _timeout: Duration) -> Result<i64, KafkaError> {
        self.position_rpc(partition).await
    }

    async fn committed(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError> {
        self.committed_rpc(partitions).await
    }

    async fn committed_timeout(
        &mut self,
        partitions: &[TopicPartition],
        _timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>, KafkaError> {
        self.committed_rpc(partitions).await
    }

    // ── metadata ──
    async fn partitions_for(&mut self, topic: &str) -> Result<Vec<PartitionInfo>, KafkaError> {
        self.partitions_for_rpc(topic).await
    }

    async fn partitions_for_timeout(
        &mut self,
        topic: &str,
        _timeout: Duration,
    ) -> Result<Vec<PartitionInfo>, KafkaError> {
        self.partitions_for_rpc(topic).await
    }

    async fn list_topics(&mut self) -> Result<HashMap<String, Vec<PartitionInfo>>, KafkaError> {
        let mut client = self.client.clone();
        let response = client
            .list_topics(proto::ConsumerIdRequest { consumer_id: self.consumer_id })
            .await
            .map_err(|s| status_to_kafka_error(&s, self.backend))?
            .into_inner();
        match response.result {
            Some(proto::list_topics_response::Result::Topics(listing)) => Ok(topic_listing_from_proto(listing)),
            Some(proto::list_topics_response::Result::Error(e)) => Err(kafka_error_from_proto(e)),
            None => Ok(HashMap::new()),
        }
    }

    async fn list_topics_timeout(
        &mut self,
        _timeout: Duration,
    ) -> Result<HashMap<String, Vec<PartitionInfo>>, KafkaError> {
        self.list_topics().await
    }

    // ── offsets lookup ──
    async fn offsets_for_times(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError> {
        let mut client = self.client.clone();
        let timestamps = timestamps_to_search
            .iter()
            .map(|(tp, ts)| proto::TimestampSpecEntry { partition: Some(tp_to_proto(tp)), timestamp: *ts })
            .collect();
        let req = proto::OffsetsForTimesRequest { consumer_id: self.consumer_id, timestamps };
        let response = client
            .offsets_for_times(req)
            .await
            .map_err(|s| status_to_kafka_error(&s, self.backend))?
            .into_inner();
        match response.result {
            Some(proto::offset_and_timestamp_response::Result::Offsets(map)) => {
                Ok(offset_and_timestamp_map_from_proto(map))
            },
            Some(proto::offset_and_timestamp_response::Result::Error(e)) => Err(kafka_error_from_proto(e)),
            None => Ok(HashMap::new()),
        }
    }

    async fn offsets_for_times_timeout(
        &mut self,
        timestamps_to_search: HashMap<TopicPartition, i64>,
        _timeout: Duration,
    ) -> Result<HashMap<TopicPartition, OffsetAndTimestamp>, KafkaError> {
        self.offsets_for_times(timestamps_to_search).await
    }

    async fn beginning_offsets(
        &mut self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        self.long_offsets_rpc(partitions, false).await
    }

    async fn beginning_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        _timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        self.long_offsets_rpc(partitions, false).await
    }

    async fn end_offsets(&mut self, partitions: &[TopicPartition]) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        self.long_offsets_rpc(partitions, true).await
    }

    async fn end_offsets_timeout(
        &mut self,
        partitions: &[TopicPartition],
        _timeout: Duration,
    ) -> Result<HashMap<TopicPartition, i64>, KafkaError> {
        self.long_offsets_rpc(partitions, true).await
    }

    // ── flow control ──
    async fn pause(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.tp_list_rpc(partitions, TpListOp::Pause).await
    }

    async fn resume(&mut self, partitions: &[TopicPartition]) -> Result<(), KafkaError> {
        self.tp_list_rpc(partitions, TpListOp::Resume).await
    }

    async fn enforce_rebalance(&mut self, _reason: Option<&str>) -> Result<(), KafkaError> {
        Err(self.unsupported("enforce_rebalance"))
    }

    // ── lifecycle ──
    async fn close(&mut self) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        self.status_rpc(client.close(proto::ConsumerCloseRequest { consumer_id: self.consumer_id, timeout_ms: None }))
            .await
    }

    async fn close_with_options(&mut self, _options: CloseOptions) -> Result<(), KafkaError> {
        // CloseOptions has no public timeout getter, and the server's close
        // ignores per-call timeouts anyway, so map to a plain close.
        let mut client = self.client.clone();
        self.status_rpc(client.close(proto::ConsumerCloseRequest { consumer_id: self.consumer_id, timeout_ms: None }))
            .await
    }
}

// ---------------------------------------------------------------------------
// Wire <-> domain conversions
// ---------------------------------------------------------------------------

fn tp_to_proto(tp: &TopicPartition) -> proto::TopicPartition {
    proto::TopicPartition { topic: tp.topic().to_string(), partition: tp.partition() }
}

fn tps_to_proto(tps: &[TopicPartition]) -> Vec<proto::TopicPartition> {
    tps.iter().map(tp_to_proto).collect()
}

fn tp_from_proto(tp: proto::TopicPartition) -> TopicPartition {
    TopicPartition::new(tp.topic, tp.partition)
}

fn offset_and_metadata_from_proto(o: proto::OffsetAndMetadata) -> OffsetAndMetadata {
    OffsetAndMetadata::with_leader_epoch(o.offset, o.leader_epoch, o.metadata)
        .expect("invalid OffsetAndMetadata from backend")
}

fn offset_map_to_proto(offsets: &HashMap<TopicPartition, OffsetAndMetadata>) -> Vec<proto::OffsetMapEntry> {
    offsets
        .iter()
        .map(|(tp, oam)| proto::OffsetMapEntry {
            partition: Some(tp_to_proto(tp)),
            offset: Some(proto::OffsetAndMetadata {
                offset: oam.offset(),
                metadata: oam.metadata().to_string(),
                leader_epoch: oam.leader_epoch(),
            }),
        })
        .collect()
}

fn offset_map_from_proto(map: proto::OffsetMap) -> HashMap<TopicPartition, OffsetAndMetadata> {
    map.entries
        .into_iter()
        .filter_map(|e| {
            let tp = tp_from_proto(e.partition?);
            Some((tp, offset_and_metadata_from_proto(e.offset?)))
        })
        .collect()
}

fn long_offset_map_from_proto(map: proto::LongOffsetMap) -> HashMap<TopicPartition, i64> {
    map.entries
        .into_iter()
        .filter_map(|e| Some((tp_from_proto(e.partition?), e.offset)))
        .collect()
}

fn offset_and_timestamp_map_from_proto(
    map: proto::OffsetAndTimestampMap,
) -> HashMap<TopicPartition, OffsetAndTimestamp> {
    map.entries
        .into_iter()
        .filter_map(|e| {
            let tp = tp_from_proto(e.partition?);
            let o = e.offset?;
            let oat = OffsetAndTimestamp::with_leader_epoch(o.offset, o.timestamp, o.leader_epoch).ok()?;
            Some((tp, oat))
        })
        .collect()
}

fn topic_listing_from_proto(listing: proto::TopicListing) -> HashMap<String, Vec<PartitionInfo>> {
    listing
        .topics
        .into_iter()
        .map(|entry| {
            (
                entry.topic,
                entry.partitions.into_iter().map(partition_info_from_proto).collect(),
            )
        })
        .collect()
}

fn timestamp_type_from_id(id: i32) -> TimestampType {
    match id {
        -1 => TimestampType::NoTimestampType,
        1 => TimestampType::LogAppendTime,
        _ => TimestampType::CreateTime,
    }
}

fn consumer_record_from_proto(r: proto::ConsumerRecord) -> ConsumerRecord<Vec<u8>, Vec<u8>> {
    let headers = RecordHeaders::from_headers(r.headers.into_iter().map(|h| RecordHeader::new(h.key, Some(h.value))));
    let serialized_key_size = r.key.as_ref().map(|k| k.len() as i32).unwrap_or(-1);
    let serialized_value_size = r.value.as_ref().map(|v| v.len() as i32).unwrap_or(-1);
    ConsumerRecord::with_all(
        r.topic,
        r.partition,
        r.offset,
        r.timestamp,
        timestamp_type_from_id(r.timestamp_type),
        serialized_key_size,
        serialized_value_size,
        r.key,
        r.value,
        headers,
        r.leader_epoch,
        None,
    )
}

/// Records bucketed by topic-partition, in the shape `ConsumerRecords::new` takes.
type RecordsByPartition = IndexMap<TopicPartition, Vec<ConsumerRecord<Vec<u8>, Vec<u8>>>>;

fn consumer_records_from_proto(list: proto::ConsumerRecordList) -> ConsumerRecords<Vec<u8>, Vec<u8>> {
    let mut by_partition: RecordsByPartition = IndexMap::new();
    for proto_rec in list.records {
        let tp = TopicPartition::new(proto_rec.topic.clone(), proto_rec.partition);
        by_partition.entry(tp).or_default().push(consumer_record_from_proto(proto_rec));
    }
    // next_offsets: last record offset + 1 per partition (best-effort; the
    // multilanguage tests assert on the records, not next_offsets()).
    let mut next_offsets: HashMap<TopicPartition, OffsetAndMetadata> = HashMap::new();
    for (tp, recs) in &by_partition {
        if let Some(last) = recs.last()
            && let Ok(oam) = OffsetAndMetadata::new(last.offset() + 1)
        {
            next_offsets.insert(tp.clone(), oam);
        }
    }
    ConsumerRecords::new(by_partition, next_offsets)
}
