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

//! Producer internal types (org.apache.kafka.clients.producer.internals)

pub(crate) mod buffer_pool;
pub(crate) mod built_in_partitioner;
pub(crate) mod future_record_metadata;
pub(crate) mod incomplete_batches;
pub(crate) mod kafka_producer_metrics;
pub(crate) mod produce_request_result;
pub(crate) mod producer_batch;
pub(crate) mod producer_metadata;
pub(crate) mod producer_metrics;
/// `ProducerTestUtils` is a test-only Java class, so its translation is compiled only
/// for `cargo test`.
#[cfg(test)]
pub(crate) mod producer_test_utils;
pub(crate) mod record_accumulator;
pub(crate) mod sender;
pub(crate) mod sender_metrics_registry;
pub(crate) mod transaction_manager;
pub(crate) mod transactional_request_result;
pub(crate) mod txn_partition_entry;
pub(crate) mod txn_partition_map;

pub(crate) use buffer_pool::BufferPool;
pub(crate) use built_in_partitioner::BuiltInPartitioner;
pub(crate) use future_record_metadata::FutureRecordMetadata;
pub(crate) use incomplete_batches::IncompleteBatches;
pub(crate) use kafka_producer_metrics::KafkaProducerMetrics;
pub(crate) use produce_request_result::ProduceRequestResult;
pub(crate) use producer_batch::ProducerBatch;
pub(crate) use producer_metadata::ProducerMetadata;
pub(crate) use producer_metrics::ProducerMetrics;
pub(crate) use record_accumulator::{AppendError, PartitionerConfig, RecordAccumulator};
pub(crate) use sender::Sender;
pub(crate) use sender_metrics_registry::SenderMetricsRegistry;
// Re-exported per CLAUDE.md §2 so the send path (Phase 4) and the public
// producer transaction API (Phase 6) import these from the parent module rather
// than the file module path. Unused until then.
#[allow(unused_imports)]
pub(crate) use transaction_manager::{
    Caller, CoordinatorNodes, InFlightBatchPool, PendingRequests, Priority, State, TransactionManager,
    TxnRequestHandler, TxnRequestHandlerKind,
};
pub(crate) use transactional_request_result::TransactionalRequestResult;
pub(crate) use txn_partition_entry::{InFlightBatchKey, TxnPartitionEntry};
pub(crate) use txn_partition_map::TxnPartitionMap;
