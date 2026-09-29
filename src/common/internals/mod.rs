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

//! Internal types (org.apache.kafka.common.internals)

mod cluster_resource_listeners;
mod kafka_future_impl;
mod partition_states;
mod topic;

pub(crate) use cluster_resource_listeners::ClusterResourceListeners;
pub(crate) use kafka_future_impl::KafkaFutureImpl;
// Re-exported per CLAUDE.md §2: internal imports of the struct must reach it
// via the parent module re-export, not the file module path. Phase 4's
// `SubscriptionState` is the first user; until later phases land, an
// `#[allow(unused_imports)]` keeps `cargo build` clean.
#[allow(unused_imports)]
pub(crate) use partition_states::PartitionStates;
pub(crate) use topic::Topic;
