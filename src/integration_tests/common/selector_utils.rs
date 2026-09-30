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

//! Helpers for the in-crate tests that drive a [`Selector`] directly. They
//! live here rather than in `tests/common/test_utils.rs` because `Selector`,
//! the channel builders and `SslFactory` are not public API.

use std::collections::HashMap;
use std::net::SocketAddr;

use crate::admin::AdminClientConfig;
use crate::common::network::selectable::USE_DEFAULT_BUFFER_SIZE;
use crate::common::network::{PlaintextChannelBuilder, SaslChannelBuilder, Selectable, Selector, SslChannelBuilder};
use crate::common::security::auth::SecurityProtocol;
use crate::common::security::ssl::SslFactory;
use crate::common::utils::LogContext;

use super::test_context::{TestContext, TestProtocol};

/// Poll timeout used by [`connect_until_ready`], in milliseconds.
const SELECTOR_POLL_TIMEOUT_MS: i64 = 5000;

/// Maximum number of polls [`connect_until_ready`] performs before giving up.
const SELECTOR_MAX_POLL_ITERATIONS: usize = 100;

/// Creates a [`Selector`] whose channel builder matches the protocol selected
/// for this run (`INTEGRATION_TEST_PROTOCOL`), for tests that drive the network
/// layer directly rather than through a client config.
///
/// PLAINTEXT uses a plain channel. For SSL and SASL_SSL the broker certificate
/// is trusted through the cluster CA and hostname verification stays at its
/// default (enabled), since the certificate's SANs cover `127.0.0.1`,
/// `localhost` and the broker container names. SASL_SSL authenticates with
/// SASL/PLAIN as `admin` / `admin-secret`.
///
/// The `ssl.*` / `sasl.*` settings are the ones [`TestContext::apply_security`]
/// gives every client this run, parsed the way a client parses them: the
/// `SslConfigs` / `SaslConfigs` fields are crate-private, and as in Java they
/// travel as config properties.
pub(crate) fn protocol_selector(ctx: &TestContext) -> Selector {
    let mut props = HashMap::from([(
        AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        ctx.protocol_bootstrap_servers().to_string(),
    )]);
    ctx.apply_security(&mut props);
    let config = AdminClientConfig::new(&props).expect("valid test security config");
    let ssl_factory = || SslFactory::new(config.ssl_config()).expect("valid test SSL config");
    match ctx.protocol() {
        TestProtocol::Plaintext => {
            Selector::with_defaults(Selector::NO_IDLE_TIMEOUT_MS, Box::new(PlaintextChannelBuilder::new(None)))
        },
        TestProtocol::Ssl => Selector::with_defaults(
            Selector::NO_IDLE_TIMEOUT_MS,
            Box::new(SslChannelBuilder::new(ssl_factory(), None)),
        ),
        TestProtocol::SaslSsl => {
            let channel_builder = SaslChannelBuilder::new(
                SecurityProtocol::SaslSsl,
                config.sasl_config().clone(),
                Some(ssl_factory()),
                None,
                "integration-test",
                LogContext::empty(),
            )
            .expect("valid test SASL config");
            Selector::with_defaults(Selector::NO_IDLE_TIMEOUT_MS, Box::new(channel_builder))
        },
    }
}

/// Connects `selector` to `addr` as `node_id` and polls until the channel is
/// ready to carry requests, panicking on disconnection or timeout.
///
/// Under PLAINTEXT the channel is ready once the TCP connection is established.
/// Under SSL and SASL_SSL the TLS handshake (and, for SASL_SSL, authentication)
/// completes over subsequent polls, so readiness is `is_channel_ready`, the same
/// signal the dedicated `ssl_sasl_test` waits on.
pub(crate) async fn connect_until_ready(selector: &mut Selector, ctx: &TestContext, node_id: &str, addr: SocketAddr) {
    selector
        .connect(node_id, addr, "localhost", USE_DEFAULT_BUFFER_SIZE, USE_DEFAULT_BUFFER_SIZE)
        .await
        .expect("Failed to connect");

    let plaintext = ctx.protocol() == TestProtocol::Plaintext;
    for _ in 0..SELECTOR_MAX_POLL_ITERATIONS {
        selector.poll(SELECTOR_POLL_TIMEOUT_MS).await.expect("poll failed");
        let ready = if plaintext {
            !selector.connected().is_empty()
        } else {
            selector.is_channel_ready(node_id)
        };
        if ready {
            return;
        }
        if !selector.disconnected().is_empty() {
            panic!("Broker disconnected during connect: {:?}", selector.disconnected());
        }
    }
    panic!("Timed out waiting for connection to the broker");
}
