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

//! Admin `*Result` types whose Java constructor user code can reach must be
//! constructible from outside the crate.
//!
//! Java declares the `DeleteRecordsResult`, `DescribeConsumerGroupsResult`,
//! `DescribeClassicGroupsResult` and `ListOffsetsResult` constructors `public`,
//! so a hand-written `Admin` fake can build them. This file is a separate crate,
//! so it compiles only if each `new` is `pub`.
//!
//! The `CreateTopicsResult` and `DescribeConfigsResult` constructors are
//! `protected` in Java, and a method that is not public in Java is not public
//! in Rust (CLAUDE.md §3), so they stay `pub(crate)` and are not tested here.

use std::collections::HashMap;

use confluent_kafka::admin::{
    ClassicGroupDescription, ConsumerGroupDescription, DeleteRecordsResult, DeletedRecords,
    DescribeClassicGroupsResult, DescribeConsumerGroupsResult, ListOffsetsResult, ListOffsetsResultInfo,
};
use confluent_kafka::common::{ClassicGroupState, GroupState, GroupType, KafkaFuture, TopicPartition};

#[tokio::test]
async fn delete_records_result_is_constructible_outside_the_crate() {
    let tp = TopicPartition::new("t".to_string(), 0);
    let result = DeleteRecordsResult::new(HashMap::from([(
        tp.clone(),
        KafkaFuture::completed(Ok(DeletedRecords::new(42))),
    )]));
    let deleted = result.low_watermarks()[&tp].get().await.expect("completed future");
    assert_eq!(deleted.low_watermark(), 42);
}

#[tokio::test]
async fn describe_consumer_groups_result_is_constructible_outside_the_crate() {
    let description = ConsumerGroupDescription::new(
        "g",
        false,
        Vec::new(),
        "range",
        GroupType::Consumer,
        GroupState::Stable,
        None,
        None,
        None,
        None,
    );
    let result =
        DescribeConsumerGroupsResult::new(HashMap::from([("g".to_string(), KafkaFuture::completed(Ok(description)))]));
    let described = result.described_groups()["g"].get().await.expect("completed future");
    assert_eq!(described.group_id(), "g");
}

#[tokio::test]
async fn describe_classic_groups_result_is_constructible_outside_the_crate() {
    let description =
        ClassicGroupDescription::new("g", "consumer", "range", Vec::new(), ClassicGroupState::Stable, None, None);
    let result =
        DescribeClassicGroupsResult::new(HashMap::from([("g".to_string(), KafkaFuture::completed(Ok(description)))]));
    let described = result.described_groups()["g"].get().await.expect("completed future");
    assert_eq!(described.group_id(), "g");
}

#[tokio::test]
async fn list_offsets_result_is_constructible_outside_the_crate() {
    let tp = TopicPartition::new("t".to_string(), 0);
    let result = ListOffsetsResult::new(HashMap::from([(
        tp.clone(),
        KafkaFuture::completed(Ok(ListOffsetsResultInfo::new(7, -1, Some(3)))),
    )]));
    let info = result
        .partition_result(&tp)
        .expect("the partition was requested")
        .get()
        .await
        .expect("completed future");
    assert_eq!(info.offset(), 7);
}
