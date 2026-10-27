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

//! A new topic to be created via `Admin::create_topics`.

use crate::common::requests::CreateTopicsRequest;
use std::collections::BTreeMap;

use crate::create_topics_request_data::{CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig};

/// A new topic to be created via `Admin::create_topics`.
// `replicas_assignments` and `configs` use `BTreeMap` (rather than `HashMap`)
// so that conversion to the wire request is deterministically ordered — Java
// iterates its `Map` in unspecified order, but a stable order simplifies
// wire-byte comparison and testing.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.admin.NewTopic")]
pub struct NewTopic {
    name: String,
    num_partitions: Option<i32>,
    replication_factor: Option<i16>,
    replicas_assignments: Option<BTreeMap<i32, Vec<i32>>>,
    configs: Option<BTreeMap<String, String>>,
}

impl NewTopic {
    /// A new topic that optionally defaults `num_partitions` and `replication_factor` to the
    /// broker configurations for `num.partitions` and `default.replication.factor` respectively.
    ///
    /// # Arguments
    ///
    /// - `name` - the topic name.
    /// - `num_partitions` - the number of partitions, or `None` for the broker default.
    /// - `replication_factor` - the replication factor, or `None` for the broker default.
    ///
    /// # Translation notes
    ///
    /// Translates both `NewTopic(String, int, short)` and
    /// `NewTopic(String, Optional<Integer>, Optional<Short>)`: callers of the former pass `Some`.
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopic#NewTopic")]
    pub fn with_num_partitions_replication_factor(
        name: impl Into<String>,
        num_partitions: Option<i32>,
        replication_factor: Option<i16>,
    ) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            replicas_assignments: None,
            configs: None,
        }
    }

    /// A new topic with the specified replica assignment configuration.
    ///
    /// # Arguments
    ///
    /// - `name` - the topic name.
    /// - `replicas_assignments` - a map from partition id to replica ids (i.e. broker ids).
    ///   Although not enforced, it is generally a good idea for all partitions to have the same
    ///   number of replicas. The first replica will be treated as the preferred leader.
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopic#NewTopic")]
    pub fn with_replicas_assignments(name: impl Into<String>, replicas_assignments: BTreeMap<i32, Vec<i32>>) -> Self {
        Self {
            name: name.into(),
            num_partitions: None,
            replication_factor: None,
            replicas_assignments: Some(replicas_assignments),
            configs: None,
        }
    }

    /// Returns the name of the topic to be created.
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopic#name")]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the number of partitions for the new topic or -1 if a replica assignment
    /// has been specified.
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopic#numPartitions")]
    pub fn num_partitions(&self) -> i32 {
        self.num_partitions.unwrap_or(CreateTopicsRequest::NO_NUM_PARTITIONS)
    }

    /// Returns the replication factor for the new topic or -1 if a replica assignment
    /// has been specified.
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopic#replicationFactor")]
    pub fn replication_factor(&self) -> i16 {
        self.replication_factor.unwrap_or(CreateTopicsRequest::NO_REPLICATION_FACTOR)
    }

    /// Returns a map from partition id to replica ids (i.e. broker ids) or `None` if the
    /// number of partitions and replication factor have been specified instead.
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopic#replicasAssignments")]
    pub fn replicas_assignments(&self) -> Option<&BTreeMap<i32, Vec<i32>>> {
        self.replicas_assignments.as_ref()
    }

    /// Set the configuration to use on the new topic.
    ///
    /// # Arguments
    ///
    /// - `configs` - The configuration map, or `None` for none.
    ///
    /// # Returns
    ///
    /// This NewTopic object.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopic#configs")]
    pub fn set_configs(mut self, configs: Option<BTreeMap<String, String>>) -> Self {
        self.configs = configs;
        self
    }

    /// Returns the configuration for the new topic or `None` if no configs were ever
    /// specified.
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopic#configs")]
    pub fn configs(&self) -> Option<&BTreeMap<String, String>> {
        self.configs.as_ref()
    }

    /// Converts this new topic to a wire `CreatableTopic`.
    ///
    /// Used by
    /// `KafkaAdminClient::create_topics`.
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopic#convertToCreatableTopic")]
    pub(crate) fn convert_to_creatable_topic(&self) -> CreatableTopic {
        let mut creatable = CreatableTopic::new();
        creatable.set_name(self.name.clone());
        creatable.set_num_partitions(self.num_partitions.unwrap_or(CreateTopicsRequest::NO_NUM_PARTITIONS));
        creatable.set_replication_factor(self.replication_factor.unwrap_or(CreateTopicsRequest::NO_REPLICATION_FACTOR));
        if let Some(assignments) = &self.replicas_assignments {
            for (partition_index, broker_ids) in assignments {
                let mut assignment = CreatableReplicaAssignment::new();
                assignment.set_partition_index(*partition_index);
                assignment.set_broker_ids(broker_ids.clone());
                creatable.assignments.push(assignment);
            }
        }
        if let Some(configs) = &self.configs {
            for (name, value) in configs {
                let mut config = CreatableTopicConfig::new();
                config.set_name(name.clone());
                config.set_value(Some(value.clone()));
                creatable.configs.push(config);
            }
        }
        creatable
    }
}

/// Java's `toString()`: an unset partition
/// count or replication factor renders as `default`, and the two maps as Java's
/// `AbstractMap.toString()` renders them (`{0=[1, 2]}`, `{k=v}`), with `null`
/// for a missing map.
impl std::fmt::Display for NewTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(name={}, numPartitions=", self.name)?;
        match self.num_partitions {
            Some(num_partitions) => write!(f, "{num_partitions}")?,
            None => f.write_str("default")?,
        }
        f.write_str(", replicationFactor=")?;
        match self.replication_factor {
            Some(replication_factor) => write!(f, "{replication_factor}")?,
            None => f.write_str("default")?,
        }
        f.write_str(", replicasAssignments=")?;
        match &self.replicas_assignments {
            Some(assignments) => {
                f.write_str("{")?;
                for (i, (partition, broker_ids)) in assignments.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{partition}=[")?;
                    for (j, broker_id) in broker_ids.iter().enumerate() {
                        if j > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{broker_id}")?;
                    }
                    f.write_str("]")?;
                }
                f.write_str("}")?;
            },
            None => f.write_str("null")?,
        }
        f.write_str(", configs=")?;
        match &self.configs {
            Some(configs) => {
                f.write_str("{")?;
                for (i, (name, value)) in configs.iter().enumerate() {
                    if i > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{name}={value}")?;
                }
                f.write_str("}")?;
            },
            None => f.write_str("null")?,
        }
        f.write_str(")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_constructor() {
        let topic = NewTopic::with_num_partitions_replication_factor("t", Some(3), Some(2));
        assert_eq!(topic.name(), "t");
        assert_eq!(topic.num_partitions(), 3);
        assert_eq!(topic.replication_factor(), 2);
        assert_eq!(topic.replicas_assignments(), None);
    }

    #[test]
    fn optional_defaults_report_minus_one() {
        let topic = NewTopic::with_num_partitions_replication_factor("t", None, None);
        assert_eq!(topic.num_partitions(), CreateTopicsRequest::NO_NUM_PARTITIONS);
        assert_eq!(topic.replication_factor(), CreateTopicsRequest::NO_REPLICATION_FACTOR);
    }

    #[test]
    fn replica_assignments_constructor() {
        let mut assignments = BTreeMap::new();
        assignments.insert(0, vec![1, 2]);
        assignments.insert(1, vec![2, 3]);
        let topic = NewTopic::with_replicas_assignments("t", assignments.clone());
        assert_eq!(topic.num_partitions(), CreateTopicsRequest::NO_NUM_PARTITIONS);
        assert_eq!(topic.replication_factor(), CreateTopicsRequest::NO_REPLICATION_FACTOR);
        assert_eq!(topic.replicas_assignments(), Some(&assignments));
    }

    #[test]
    fn configs_builder_is_fluent() {
        let mut configs = BTreeMap::new();
        configs.insert("retention.ms".to_string(), "1000".to_string());
        let topic =
            NewTopic::with_num_partitions_replication_factor("t", Some(1), Some(1)).set_configs(Some(configs.clone()));
        assert_eq!(topic.configs(), Some(&configs));
    }

    #[test]
    fn convert_to_creatable_topic_with_counts() {
        let creatable =
            NewTopic::with_num_partitions_replication_factor("t", Some(3), Some(2)).convert_to_creatable_topic();
        assert_eq!(creatable.name, "t");
        assert_eq!(creatable.num_partitions, 3);
        assert_eq!(creatable.replication_factor, 2);
        assert!(creatable.assignments.is_empty());
        assert!(creatable.configs.is_empty());
    }

    #[test]
    fn convert_to_creatable_topic_with_assignments_and_configs() {
        let mut assignments = BTreeMap::new();
        assignments.insert(0, vec![1, 2]);
        let mut configs = BTreeMap::new();
        configs.insert("cleanup.policy".to_string(), "compact".to_string());
        let creatable = NewTopic::with_replicas_assignments("t", assignments)
            .set_configs(Some(configs))
            .convert_to_creatable_topic();
        assert_eq!(creatable.num_partitions, CreateTopicsRequest::NO_NUM_PARTITIONS);
        assert_eq!(creatable.assignments.len(), 1);
        assert_eq!(creatable.assignments[0].partition_index, 0);
        assert_eq!(creatable.assignments[0].broker_ids, vec![1, 2]);
        assert_eq!(creatable.configs.len(), 1);
        assert_eq!(creatable.configs[0].name, "cleanup.policy");
        assert_eq!(creatable.configs[0].value.as_deref(), Some("compact"));
    }

    // Translations of Java's `NewTopicTest`. `testConstructorWithPartitionsAndReplicationFactor`,
    // `testConstructorWithOptionalValues`, `testConstructorWithReplicasAssignments`,
    // `testConfigsNotNull` and the two `testConvertToCreatableTopic*` tests are
    // `counts_constructor`, `optional_defaults_report_minus_one`,
    // `replica_assignments_constructor`, `configs_builder_is_fluent` and the two
    // `convert_to_creatable_topic_*` tests above. `testUnmodifiableReplicasAssignments`
    // has no Rust counterpart: Java checks that the returned map rejects `put`,
    // while `replicas_assignments()` returns a shared borrow, which cannot be
    // mutated at all.

    const TEST_TOPIC: &str = "testtopic";
    const NUM_PARTITIONS: i32 = 3;
    const REPLICATION_FACTOR: i16 = 1;
    const CLEANUP_POLICY_CONFIG_KEY: &str = "cleanup.policy";
    const CLEANUP_POLICY_CONFIG_VALUE: &str = "compact";

    /// Translated from `NewTopicTest.testConfigsNull`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopicTest#testConfigsNull")]
    fn test_configs_null() {
        let new_topic = NewTopic::with_num_partitions_replication_factor(
            TEST_TOPIC,
            Some(NUM_PARTITIONS),
            Some(REPLICATION_FACTOR),
        );
        assert_eq!(new_topic.configs(), None);
    }

    /// Translated from `NewTopicTest.testToString`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopicTest#testToString")]
    fn test_to_string() {
        let topic1 = NewTopic::with_num_partitions_replication_factor(
            TEST_TOPIC,
            Some(NUM_PARTITIONS),
            Some(REPLICATION_FACTOR),
        );
        let expected1 = format!(
            "(name={TEST_TOPIC}, numPartitions={NUM_PARTITIONS}, replicationFactor={REPLICATION_FACTOR}, \
             replicasAssignments=null, configs=null)"
        );
        assert_eq!(expected1, topic1.to_string());

        let topic1 = topic1.set_configs(Some(BTreeMap::from([(
            CLEANUP_POLICY_CONFIG_KEY.to_string(),
            CLEANUP_POLICY_CONFIG_VALUE.to_string(),
        )])));
        let expected2 = format!(
            "(name={TEST_TOPIC}, numPartitions={NUM_PARTITIONS}, replicationFactor={REPLICATION_FACTOR}, \
             replicasAssignments=null, configs={{{CLEANUP_POLICY_CONFIG_KEY}={CLEANUP_POLICY_CONFIG_VALUE}}})"
        );
        assert_eq!(expected2, topic1.to_string());

        let partition_index = 0;
        let topic2 = NewTopic::with_replicas_assignments(TEST_TOPIC, BTreeMap::from([(partition_index, vec![1, 2])]));
        let expected3 = format!(
            "(name={TEST_TOPIC}, numPartitions=default, replicationFactor=default, \
             replicasAssignments={{{partition_index}=[1, 2]}}, configs=null)"
        );
        assert_eq!(expected3, topic2.to_string());
    }

    /// Translated from `NewTopicTest.testEqualsAndHashCode`.
    #[test]
    #[doc(alias = "org.apache.kafka.clients.admin.NewTopicTest#testEqualsAndHashCode")]
    fn test_equals_and_hash_code() {
        use std::hash::BuildHasher;
        // One hasher for every value, as Java's `hashCode` is one function.
        let hasher = std::collections::hash_map::RandomState::new();
        let hash_code = |topic: &NewTopic| hasher.hash_one(topic);
        let topic1 = NewTopic::with_num_partitions_replication_factor(
            TEST_TOPIC,
            Some(NUM_PARTITIONS),
            Some(REPLICATION_FACTOR),
        );
        let topic2 = NewTopic::with_num_partitions_replication_factor(
            TEST_TOPIC,
            Some(NUM_PARTITIONS),
            Some(REPLICATION_FACTOR),
        );
        let topic3 = NewTopic::with_num_partitions_replication_factor(
            "another-topic",
            Some(NUM_PARTITIONS),
            Some(REPLICATION_FACTOR),
        );

        assert_eq!(topic1, topic2);
        assert_ne!(topic1, topic3);
        assert_eq!(hash_code(&topic1), hash_code(&topic2));
        assert_ne!(hash_code(&topic1), hash_code(&topic3));
    }

    /// New test, no Java original: Java's `configs(null)` clears configs set
    /// earlier (`NewTopic#configs(Map)`), so none are sent.
    #[test]
    fn set_configs_none_clears_the_configs() {
        let topic = NewTopic::with_num_partitions_replication_factor(
            TEST_TOPIC,
            Some(NUM_PARTITIONS),
            Some(REPLICATION_FACTOR),
        )
        .set_configs(Some(BTreeMap::from([(
            CLEANUP_POLICY_CONFIG_KEY.to_string(),
            CLEANUP_POLICY_CONFIG_VALUE.to_string(),
        )])))
        .set_configs(None);
        assert_eq!(topic.configs(), None);
        assert!(topic.convert_to_creatable_topic().configs.is_empty());
    }
}
