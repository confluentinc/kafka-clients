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

//! An [`AdminBackend`] implementation that tunnels every call over gRPC to a
//! server in another language (Python sync, Python asyncio, or C++), mirroring
//! [`crate::common::multilanguage_consumer::MultilanguageConsumer`].
//!
//! Unlike the consumer client, this does not implement a production trait — see
//! [`AdminBackend`] for why the real `Admin` trait is not implementable from
//! `tests/`. Every method is one unary RPC that the server awaits, so the
//! response carries already-resolved results.
//!
//! Used only when `--features multilanguage-tests` is enabled.

use std::collections::HashMap;
use std::time::Duration;

use confluent_kafka::common::KafkaError;
use multilanguage_test_server::proto::admin_service_client::AdminServiceClient;
use multilanguage_test_server::proto::{self};
use tonic::transport::Channel;

use crate::common::admin_backend::AdminBackend;
use crate::common::multilanguage_producer::{kafka_error_from_proto, status_to_kafka_error};

/// gRPC-backed admin client driven by an out-of-process server in another
/// language that ultimately calls the same Rust client through a binding.
pub struct MultilanguageAdmin {
    admin_id: u64,
    client: AdminServiceClient<Channel>,
    backend: &'static str,
}

impl MultilanguageAdmin {
    /// Connect to `channel` and create a server-side admin client from
    /// `config`.
    pub async fn new(
        channel: Channel,
        config: HashMap<String, String>,
        backend: &'static str,
    ) -> Result<Self, KafkaError> {
        Self::create(channel, proto::CreateAdminRequest { config, num_brokers: None }, backend).await
    }

    /// Connect to `channel` and create a server-side *mock* admin client with
    /// `num_brokers` brokers. An empty config is what selects the mock, matching
    /// the producer / consumer backends.
    pub async fn new_mock(channel: Channel, num_brokers: i32, backend: &'static str) -> Result<Self, KafkaError> {
        let request = proto::CreateAdminRequest { config: HashMap::new(), num_brokers: Some(num_brokers) };
        Self::create(channel, request, backend).await
    }

    async fn create(
        channel: Channel,
        request: proto::CreateAdminRequest,
        backend: &'static str,
    ) -> Result<Self, KafkaError> {
        let mut client = AdminServiceClient::new(channel);
        let response = client
            .create_admin(request)
            .await
            .map_err(|status| status_to_kafka_error(&status, backend))?
            .into_inner();
        if let Some(err) = response.error {
            return Err(kafka_error_from_proto(err));
        }
        Ok(Self { admin_id: response.admin_id, client, backend })
    }

    /// The opaque server-local handle id, for log messages.
    pub fn admin_id(&self) -> u64 {
        self.admin_id
    }
}

impl AdminBackend for MultilanguageAdmin {
    async fn close(&self, timeout: Option<Duration>) -> Result<(), KafkaError> {
        let mut client = self.client.clone();
        let request =
            proto::AdminCloseRequest { admin_id: self.admin_id, timeout_ms: timeout.map(|t| t.as_millis() as i64) };
        let response = client
            .close(request)
            .await
            .map_err(|status| status_to_kafka_error(&status, self.backend))?
            .into_inner();
        match response.error {
            Some(err) => Err(kafka_error_from_proto(err)),
            None => Ok(()),
        }
    }

    fn name(&self) -> &'static str {
        self.backend
    }
}
