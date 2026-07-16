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

//! Kafka admin client.
//!
//! Corresponds to the `org.apache.kafka.clients.admin` package (the `clients`
//! segment is dropped per the project naming conventions). See
//! `.claude/rules/admin-client.md` for the design decisions that govern this
//! module.

pub mod admin_client_config;
pub mod config;
pub mod config_entry;
pub mod create_topics_result;
pub mod delete_topics_result;
pub mod describe_topics_result;
pub mod list_topics_result;
pub mod mock_admin_client;
pub mod new_topic;
pub mod options;
pub mod topic_description;
pub mod topic_listing;

pub use admin_client_config::AdminClientConfig;
pub use config::Config;
pub use config_entry::{ConfigEntry, ConfigSource, ConfigSynonym, ConfigType};
pub use create_topics_result::{CreateTopicsResult, TopicMetadataAndConfig};
pub use delete_topics_result::DeleteTopicsResult;
pub use describe_topics_result::DescribeTopicsResult;
pub use list_topics_result::ListTopicsResult;
pub use mock_admin_client::MockAdminClient;
pub use new_topic::NewTopic;
pub use options::{CreateTopicsOptions, DeleteTopicsOptions, DescribeTopicsOptions, ListTopicsOptions};
pub use topic_description::TopicDescription;
pub use topic_listing::TopicListing;

use std::time::Duration;

use async_trait::async_trait;

use crate::common::TopicCollection;

/// The administrative client for Kafka, which supports managing and inspecting
/// topics, brokers, configurations and records.
///
/// Corresponds to `org.apache.kafka.clients.admin.Admin`.
///
/// Per `.claude/rules/admin-client.md` §1, every RPC method is a **plain sync
/// `fn`** that returns immediately with a `*Result` holding one
/// [`KafkaFuture`](crate::common::KafkaFuture) per key — the network I/O happens
/// later on the background task, and the caller opts into blocking by awaiting
/// the returned future(s). The only `async fn` is [`close`](Admin::close),
/// which (like Java's `close(Duration)`) joins the background task.
#[async_trait]
pub trait Admin: Send + Sync {
    /// Create a batch of new topics.
    ///
    /// Corresponds to `Admin.createTopics(Collection<NewTopic>, CreateTopicsOptions)`.
    fn create_topics(&self, new_topics: &[NewTopic], options: CreateTopicsOptions) -> CreateTopicsResult;

    /// Delete a batch of topics (by name or by id, per the `TopicCollection`).
    ///
    /// Corresponds to `Admin.deleteTopics(TopicCollection, DeleteTopicsOptions)`.
    fn delete_topics(&self, topics: TopicCollection, options: DeleteTopicsOptions) -> DeleteTopicsResult;

    /// List the topics available in the cluster.
    ///
    /// Corresponds to `Admin.listTopics(ListTopicsOptions)`.
    fn list_topics(&self, options: ListTopicsOptions) -> ListTopicsResult;

    /// Describe some topics in the cluster (by name or by id, per the
    /// `TopicCollection`).
    ///
    /// Corresponds to `Admin.describeTopics(TopicCollection, DescribeTopicsOptions)`.
    fn describe_topics(&self, topics: TopicCollection, options: DescribeTopicsOptions) -> DescribeTopicsResult;

    /// Close the admin client, awaiting the background task to finish
    /// in-flight work up to `timeout`.
    ///
    /// Corresponds to `Admin.close(Duration)`; blocking in Java, so `async` in
    /// Rust (CLAUDE.md §9.4).
    async fn close(&self, timeout: Duration);
}
