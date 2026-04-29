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

//! Translation of `org.apache.kafka.common.config`.

pub mod abstract_config;
pub mod config_def;
pub mod config_exception;
pub mod sasl_configs;
pub mod ssl_configs;
pub mod topic_config;

pub use abstract_config::AbstractConfig;
pub use config_def::{
    ConfigDef, ConfigKey, ConfigValue, Importance, NonNullValidator, Password, Range, Type, ValidString, Validator,
};
