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

//! Translation of `org.apache.kafka.clients.producer.RecordSendTest`.
//!
//! Java's `RecordSendTest` lives in the `producer` package directly and
//! exercises the public-facing `Future` returned by `KafkaProducer.send`
//! end-to-end through `ProduceRequestResult` + `FutureRecordMetadata`.
//! In Rust both of those types are `pub(crate)` (module-internal), so
//! the test module mirrors the Java file by calling them directly.

#![cfg(test)]

use std::sync::Arc;
use std::time::Duration;

use crate::common::errors::KafkaError;
use crate::common::record::record_batch::NO_TIMESTAMP;
use crate::common::topic_partition::TopicPartition;
use crate::common::utils::MockTime;
use crate::producer::internals::future_record_metadata::FutureRecordMetadata;
use crate::producer::internals::produce_request_result::{ErrorsByIndex, ProduceRequestResult};

const BASE_OFFSET: i64 = 45;
const REL_OFFSET: i32 = 5;

fn topic_partition() -> TopicPartition {
    TopicPartition::new("test", 0)
}

fn future_for(result: Arc<ProduceRequestResult>) -> FutureRecordMetadata {
    FutureRecordMetadata::new(result, REL_OFFSET, NO_TIMESTAMP, 0, 0, MockTime::arc())
}

/// Java: `RecordSendTest#testTimeout`. Translated as a `tokio::time::timeout`
/// wrapping `future.get()` — the Rust equivalent of `Future.get(5, MS)`.
#[tokio::test]
async fn test_timeout() {
    let request = Arc::new(ProduceRequestResult::new(topic_partition()));
    let future = future_for(Arc::clone(&request));
    assert!(!future.is_done(), "Request is not completed");

    let elapsed = tokio::time::timeout(Duration::from_millis(5), future.get()).await;
    assert!(elapsed.is_err(), "Should have timed out");

    request.set(BASE_OFFSET, NO_TIMESTAMP, None);
    request.done();
    assert!(future.is_done());
    assert_eq!(BASE_OFFSET + REL_OFFSET as i64, future.get().await.unwrap().offset());
}

/// Java: `RecordSendTest#testError`. Asserts that an asynchronous request
/// completed with an error eventually surfaces that error from `future.get()`.
#[tokio::test]
async fn test_error() {
    let request = async_request(BASE_OFFSET, Some(KafkaError::CorruptRecord("boom".to_string())), 50).await;
    let future = future_for(request);
    let err = future.get().await.unwrap_err();
    assert!(
        matches!(err, KafkaError::CorruptRecord(_)),
        "expected CorruptRecord, got {}",
        err.java_class_name()
    );
}

/// Java: `RecordSendTest#testBlocking`. Asserts that an asynchronous
/// request that completes successfully eventually returns the right
/// offset from `future.get()`.
#[tokio::test]
async fn test_blocking() {
    let request = async_request(BASE_OFFSET, None, 50).await;
    let future = future_for(request);
    assert_eq!(BASE_OFFSET + REL_OFFSET as i64, future.get().await.unwrap().offset());
}

/// Create a new request result that will be completed after the given
/// timeout. Mirrors the Java helper of the same name.
async fn async_request(base_offset: i64, error: Option<KafkaError>, timeout_ms: u64) -> Arc<ProduceRequestResult> {
    let request = Arc::new(ProduceRequestResult::new(topic_partition()));
    let request_clone = Arc::clone(&request);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
        match error {
            None => {
                request_clone.set(base_offset, NO_TIMESTAMP, None);
            },
            Some(e) => {
                let f: ErrorsByIndex = Arc::new(move |_idx| Some(e.clone()));
                request_clone.set(-1, NO_TIMESTAMP, Some(f));
            },
        }
        request_clone.done();
    });
    request
}
