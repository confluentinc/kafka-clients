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

//! Translation of `org.apache.kafka.clients.producer.internals`.

pub(crate) mod buffer_pool;
pub(crate) mod error_logging_callback;
pub(crate) mod future_record_metadata;
pub(crate) mod incomplete_batches;
pub(crate) mod produce_request_result;
pub(crate) mod producer_batch;
pub(crate) mod producer_metadata;
pub(crate) mod transaction_manager;

#[allow(unused_imports)]
pub(crate) use buffer_pool::BufferPool;
#[allow(unused_imports)]
pub(crate) use error_logging_callback::ErrorLoggingCallback;
#[allow(unused_imports)]
pub(crate) use future_record_metadata::FutureRecordMetadata;
#[allow(unused_imports)]
pub(crate) use incomplete_batches::IncompleteBatches;
#[allow(unused_imports)]
pub(crate) use produce_request_result::ProduceRequestResult;
#[allow(unused_imports)]
pub(crate) use producer_batch::ProducerBatch;
#[allow(unused_imports)]
pub(crate) use producer_metadata::ProducerMetadata;
#[allow(unused_imports)]
pub(crate) use transaction_manager::TransactionManager;
