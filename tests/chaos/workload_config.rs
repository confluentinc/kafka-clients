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

/// Producer properties tuned for chaos: `acks=all`, idempotent, and a
/// `delivery.timeout.ms` well above `linger.ms + request.timeout.ms` so a
/// record in flight during a broker roll is retried to its true outcome
/// rather than timing out mid-fault.
pub fn producer_props(bootstrap: &str, client_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("client.id".to_string(), client_id.to_string()),
        ("acks".to_string(), "all".to_string()),
        ("max.block.ms".to_string(), "60000".to_string()),
        ("request.timeout.ms".to_string(), "30000".to_string()),
        ("delivery.timeout.ms".to_string(), "120000".to_string()),
        ("linger.ms".to_string(), "5".to_string()),
        ("enable.idempotence".to_string(), "true".to_string()),
    ])
}

/// Consumer properties: KIP-848 group protocol, `earliest` reset, manual
/// commit (the workload commits explicitly so the ledger reflects committed
/// offsets).
pub fn consumer_props(bootstrap: &str, group_id: &str, client_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.to_string()),
        ("group.id".to_string(), group_id.to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("client.id".to_string(), client_id.to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
        // Do not let the consumer recreate a topic the topic-recreate action
        // just deleted — the harness owns topic lifecycle.
        ("allow.auto.create.topics".to_string(), "false".to_string()),
    ])
}
