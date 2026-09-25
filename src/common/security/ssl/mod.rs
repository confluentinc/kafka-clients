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

//! SSL/TLS security utilities (org.apache.kafka.common.security.ssl).
//!
//! # Dead-code lint
//!
//! Java marks this package "not a supported API", so it is crate-private. It
//! is translated in full (DoD #2), but the client uses only part of it; the
//! rest has no caller yet, or only the translated tests. Nothing outside the
//! crate can reach it, so the module allows dead code rather than dropping
//! Java methods.

#![allow(dead_code, unused_imports)]

mod ssl_factory;

pub use ssl_factory::SslFactory;
