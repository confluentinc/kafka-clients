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

//! Producer callback type.
//!
//! Translated from `org.apache.kafka.clients.producer.Callback`.

use crate::common::Error;
use crate::producer::RecordMetadata;

/// Type alias for the producer send callback.
///
/// In Java, `Callback` is an interface with a single
/// `onCompletion(RecordMetadata, Exception)` method. We use `FnOnce` because
/// each callback is invoked exactly once when the batch completes, fails, or
/// is aborted.
pub type Callback = Box<dyn FnOnce(Option<&RecordMetadata>, Option<&Error>) + Send + Sync>;
