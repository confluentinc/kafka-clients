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

//! Translation of `org.apache.kafka.common.security.authenticator`.
//!
//! Phase 9a ships the PLAIN-only client-side authenticator. SCRAM,
//! OAUTHBEARER, Kerberos/GSSAPI are out of scope and rejected at config
//! validation time (Phase 9b). Server-side classes
//! (`SaslServerAuthenticator`, `KafkaPrincipalBuilder`, `LoginManager`,
//! JAAS server contexts) are out of Milestone 1 entirely.

pub mod sasl_client_authenticator;

pub use sasl_client_authenticator::{PlainCredentials, SaslClientAuthenticator, SaslState};
