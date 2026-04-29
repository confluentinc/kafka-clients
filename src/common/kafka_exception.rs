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

//! Re-export shim for `org.apache.kafka.common.KafkaException`.
//!
//! In Java, `KafkaException` is the parent of every `*Exception` thrown by the
//! Kafka client. We collapse the entire hierarchy into [`crate::common::errors::KafkaError`];
//! this module exposes [`KafkaException`] as a type alias so existing reference
//! sites in translated code continue to read naturally.

pub use crate::common::errors::KafkaError as KafkaException;
