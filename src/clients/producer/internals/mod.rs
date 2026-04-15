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

pub mod buffer_pool;
pub mod built_in_partitioner;
pub mod future_record_metadata;
pub mod incomplete_batches;
pub mod produce_request_result;
pub mod producer_batch;
pub mod producer_metadata;
pub mod record_accumulator;

pub use buffer_pool::BufferPool;
pub use future_record_metadata::FutureRecordMetadata;
pub use incomplete_batches::IncompleteBatches;
pub use produce_request_result::ProduceRequestResult;
pub use producer_batch::ProducerBatch;
