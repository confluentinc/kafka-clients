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

//! Configuration types for Kafka clients (org.apache.kafka.common.config).

pub(crate) mod config_def;
mod config_error;
pub mod config_resource;
mod sasl_configs;
mod ssl_client_auth;
mod ssl_configs;

pub(crate) use config_def::ConfigDef;
pub use config_error::ConfigError;
pub use config_resource::ConfigResource;
pub use sasl_configs::SaslConfigs;
pub use ssl_client_auth::SslClientAuth;
pub use ssl_configs::SslConfigs;
