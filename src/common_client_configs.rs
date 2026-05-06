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

//! Common client configuration constants.
//!
//! Corresponds to `org.apache.kafka.clients.CommonClientConfigs`.
//! Only the constants needed by the Metadata class are included here;
//! full configuration support will be added as needed.

/// Config key: `bootstrap.servers`
pub const BOOTSTRAP_SERVERS_CONFIG: &str = "bootstrap.servers";

/// Config key: `security.protocol`
pub const SECURITY_PROTOCOL_CONFIG: &str = "security.protocol";

/// The base for exponential retry backoff.
pub const RETRY_BACKOFF_EXP_BASE: i32 = 2;

/// The jitter factor for retry backoff.
pub const RETRY_BACKOFF_JITTER: f64 = 0.2;
