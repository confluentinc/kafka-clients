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

//! Translation of `org.apache.kafka.common.utils`.

pub mod buffer_supplier;
pub mod byte_buffer_input_stream;
pub mod byte_buffer_output_stream;
pub mod byte_utils;
pub mod chunked_bytes_stream;
pub mod crc32c;
pub mod exit;
pub mod exponential_backoff;
pub mod exponential_backoff_manager;
pub mod log_context;
pub mod mock_time;
pub mod producer_id_and_epoch;
pub mod system_time;
pub mod time;
pub mod timer;
// `utils.rs` mirrors Java's `Utils.java` (the static-helpers grab-bag).
// Clippy's `module_inception` lint flags the parent/child name match — we
// keep it per CLAUDE.md rule 2 (each Java class lives in its own file).
#[allow(clippy::module_inception)]
pub mod utils;

pub use buffer_supplier::BufferSupplier;
pub use byte_buffer_input_stream::ByteBufferInputStream;
pub use byte_buffer_output_stream::ByteBufferOutputStream;
pub use chunked_bytes_stream::ChunkedBytesStream;
pub use crc32c::Crc32C;
pub use exponential_backoff::ExponentialBackoff;
pub use exponential_backoff_manager::ExponentialBackoffManager;
pub use log_context::LogContext;
pub use mock_time::MockTime;
pub use producer_id_and_epoch::ProducerIdAndEpoch;
pub use system_time::SystemTime;
pub use time::Time;
pub use timer::Timer;
