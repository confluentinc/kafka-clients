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

//! Host resolution trait.
//!
//! Translated from `org.apache.kafka.clients.HostResolver`.

use std::io;
use std::net::IpAddr;

/// A trait for resolving hostnames to IP addresses.
///
/// This is the async equivalent of Java's `HostResolver` interface.
pub trait HostResolver: Send + Sync {
    /// Resolves the given hostname to a list of IP addresses.
    ///
    /// # Errors
    /// Returns an `io::Error` if the hostname cannot be resolved.
    fn resolve(&self, host: &str) -> impl std::future::Future<Output = io::Result<Vec<IpAddr>>> + Send;
}
