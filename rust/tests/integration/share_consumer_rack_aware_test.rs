// Copyright 2026 Confluent Inc.
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

//! `org.apache.kafka.clients.consumer.ShareConsumerRackAwareTest` (Apache Kafka
//! 4.4.0, `clients-integration-tests`): nothing is translated.
//!
//! Its only method, `testShareConsumerWithRackAwareAssignor`, runs as two
//! `@ClusterTests` arms (`group.share.assignment.interval.ms` `0` and `1000`)
//! on a 3-broker `Type.KRAFT` cluster whose brokers have `broker.rack`
//! `rack0` / `rack1` / `rack2`. Three share consumers of `group0`, with
//! `client.id` `client0`..`client2` and `client.rack` `rack0`..`rack2`,
//! subscribe to a topic whose partitions the test creates on chosen brokers
//! (`createTopics`, then `createPartitions` with explicit assignments) and then
//! moves (`alterPartitionReassignments`). After each step it waits until
//! `describeShareGroups` shows every partition assigned to the member on the
//! rack of the partition's broker.
//!
//! It is NOT translated: the cluster is configured with
//! `group.share.assignors=org.apache.kafka.clients.consumer.RackAwareTestAssignor`,
//! a Java test class of the `clients-integration-tests` test sources on the
//! Java test cluster's classpath, which the `apache/kafka` Docker broker image
//! the harness runs cannot load. The assignment the test asserts is computed
//! by that assignor, so without it the test has nothing to check. The same
//! reason skips `ConsumerIntegrationTest.testRackAwareAssignment` (see
//! `consumer_test.rs`).
