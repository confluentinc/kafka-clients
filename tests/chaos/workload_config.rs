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

//! Config-property maps shared by every workload backend.
//!
//! The backend factories (`tests/common/backend_factory.rs`) all accept a flat
//! `HashMap<String, String>` — the same shape `ProducerConfig::from_properties`
//! parses and Python's `KafkaProducer(dict)` accepts — so one property builder
//! serves rust / python / c alike.

use std::collections::HashMap;

use super::common::kafka_cluster::{SASL_PASSWORD, SASL_USERNAME};
use super::config::SecurityProtocol;

/// The client-side security keys for `protocol`, to be merged into every
/// client config of the run (admin, producers, consumers).
///
/// - `PLAINTEXT`: empty (the client defaults to `security.protocol=PLAINTEXT`).
/// - `SSL` / `SASL_SSL`: `security.protocol`, the cluster CA as a PEM
///   `ssl.truststore.certificates`, and an empty
///   `ssl.endpoint.identification.algorithm` (hostname verification off, as in
///   the integration suite's `TestContext::apply_security`; the broker
///   certificate's SANs do cover `127.0.0.1`, this just keeps the two fixtures
///   identical).
/// - `SASL_PLAINTEXT` / `SASL_SSL`: `sasl.mechanism=PLAIN` and a
///   `sasl.jaas.config` for the cluster's `admin` / `admin-secret` user.
///
/// Does NOT set `bootstrap.servers`: the caller pairs these with the
/// protocol-matched listener address (`ChaosHarness::protocol_bootstrap`).
pub fn security_props(protocol: SecurityProtocol, ca_cert_pem: &str) -> HashMap<String, String> {
    let mut props = HashMap::new();
    if protocol == SecurityProtocol::Plaintext {
        return props;
    }
    props.insert("security.protocol".to_string(), protocol.config_value().to_string());
    if protocol.uses_tls() {
        props.insert("ssl.truststore.certificates".to_string(), ca_cert_pem.to_string());
        props.insert("ssl.endpoint.identification.algorithm".to_string(), String::new());
    }
    if protocol.uses_sasl() {
        props.insert("sasl.mechanism".to_string(), "PLAIN".to_string());
        props.insert("sasl.jaas.config".to_string(), plain_jaas_config(SASL_USERNAME, SASL_PASSWORD));
    }
    props
}

/// Java-style `sasl.jaas.config` line for `PlainLoginModule`.
fn plain_jaas_config(username: &str, password: &str) -> String {
    format!(
        "org.apache.kafka.common.security.plain.PlainLoginModule required \
         username=\"{username}\" password=\"{password}\";"
    )
}

/// Producer properties tuned for chaos: `acks=all`, idempotent, and a
/// `delivery.timeout.ms` well above `linger.ms + request.timeout.ms` so a
/// record in flight during a broker roll is retried to its true outcome
/// rather than timing out mid-fault. `security` (from [`security_props`]) is
/// merged in last.
pub fn producer_props(bootstrap: &str, client_id: &str, security: &HashMap<String, String>) -> HashMap<String, String> {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), client_id.to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "60000".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("delivery.timeout.ms".to_string(), "120000".to_string()),
        ("linger.ms".to_string(), "5".to_string()),
        ("enable.idempotence".to_string(), "true".to_string()),
    ]);
    props.extend(security.iter().map(|(k, v)| (k.clone(), v.clone())));
    props
}

/// Consumer properties: KIP-848 group protocol, `earliest` reset, manual
/// commit (the workload commits explicitly so the ledger reflects committed
/// offsets). `security` (from [`security_props`]) is merged in last.
pub fn consumer_props(
    bootstrap: &str,
    group_id: &str,
    client_id: &str,
    security: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), client_id.to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        // Do not let the consumer recreate a topic the topic-recreate action
        // just deleted — the harness owns topic lifecycle.
        ("allow.auto.create.topics".to_string(), "false".to_string()),
    ]);
    props.extend(security.iter().map(|(k, v)| (k.clone(), v.clone())));
    props
}

#[cfg(test)]
mod tests {
    use super::*;

    const CA: &str = "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n";
    const JAAS: &str = "org.apache.kafka.common.security.plain.PlainLoginModule required \
                        username=\"admin\" password=\"admin-secret\";";

    #[test]
    fn plaintext_adds_no_security_keys() {
        assert!(security_props(SecurityProtocol::Plaintext, CA).is_empty());
        let props = producer_props("b:1", "p", &security_props(SecurityProtocol::Plaintext, CA));
        assert!(!props.contains_key("security.protocol"));
        assert_eq!(props["bootstrap.servers"], "b:1");
    }

    #[test]
    fn ssl_trusts_the_cluster_ca_without_sasl() {
        let props = security_props(SecurityProtocol::Ssl, CA);
        assert_eq!(props["security.protocol"], "SSL");
        assert_eq!(props["ssl.truststore.certificates"], CA);
        assert_eq!(props["ssl.endpoint.identification.algorithm"], "");
        assert!(!props.contains_key("sasl.mechanism"));
        assert!(!props.contains_key("sasl.jaas.config"));
        assert_eq!(props.len(), 3);
    }

    #[test]
    fn sasl_plaintext_authenticates_without_tls() {
        let props = security_props(SecurityProtocol::SaslPlaintext, CA);
        assert_eq!(props["security.protocol"], "SASL_PLAINTEXT");
        assert_eq!(props["sasl.mechanism"], "PLAIN");
        assert_eq!(props["sasl.jaas.config"], JAAS);
        assert!(!props.contains_key("ssl.truststore.certificates"));
        assert_eq!(props.len(), 3);
    }

    #[test]
    fn sasl_ssl_has_both_tls_and_sasl_keys() {
        let props = security_props(SecurityProtocol::SaslSsl, CA);
        assert_eq!(props["security.protocol"], "SASL_SSL");
        assert_eq!(props["ssl.truststore.certificates"], CA);
        assert_eq!(props["ssl.endpoint.identification.algorithm"], "");
        assert_eq!(props["sasl.mechanism"], "PLAIN");
        assert_eq!(props["sasl.jaas.config"], JAAS);
        assert_eq!(props.len(), 5);
    }

    #[test]
    fn producer_and_consumer_props_merge_security_and_keep_chaos_tuning() {
        let sec = security_props(SecurityProtocol::SaslSsl, CA);
        let p = producer_props("b:1", "producer-rust-1", &sec);
        assert_eq!(p["security.protocol"], "SASL_SSL");
        assert_eq!(p["acks"], "all");
        assert_eq!(p["enable.idempotence"], "true");
        assert_eq!(p["client.id"], "producer-rust-1");

        let c = consumer_props("b:1", "g", "consumer-rust-1", &sec);
        assert_eq!(c["security.protocol"], "SASL_SSL");
        assert_eq!(c["sasl.jaas.config"], JAAS);
        assert_eq!(c["group.protocol"], "consumer");
        assert_eq!(c["enable.auto.commit"], "false");
        assert_eq!(c["group.id"], "g");
    }
}
