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

//! Internal consumer types (org.apache.kafka.clients.consumer.internals).
//!
//! Per CLAUDE.md §2, types in this module use `pub(crate)` visibility.
//! Stable user-facing re-exports live in `crate::consumer`.

pub(crate) mod auto_offset_reset_strategy;
pub(crate) mod consumer_interceptors;
pub(crate) mod consumer_metadata;
pub(crate) mod deserializers;
pub(crate) mod events;
pub(crate) mod request_state;
pub(crate) mod subscription_state;
pub(crate) mod timed_request_state;
pub(crate) mod wakeup_trigger;
