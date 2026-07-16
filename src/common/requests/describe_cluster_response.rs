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

//! DescribeCluster response handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.DescribeClusterResponse`.

use std::collections::HashMap;
use std::io;

use crate::common::Node;
use crate::common::protocol::{ApiKeys, Errors, Readable};
use crate::describe_cluster_response_data::DescribeClusterResponseData;

use super::abstract_response::single_error_count;

/// A DescribeCluster response.
///
/// Corresponds to `org.apache.kafka.common.requests.DescribeClusterResponse`.
#[derive(Debug, Clone)]
pub struct DescribeClusterResponse {
    data: DescribeClusterResponseData,
}

impl DescribeClusterResponse {
    /// Creates a new `DescribeClusterResponse` from the underlying data.
    pub fn new(data: DescribeClusterResponseData) -> Self {
        Self { data }
    }

    /// Returns the API key for this response.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::DESCRIBE_CLUSTER
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &DescribeClusterResponseData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut DescribeClusterResponseData {
        &mut self.data
    }

    /// Returns the throttle time in milliseconds.
    pub fn throttle_time_ms(&self) -> i32 {
        self.data.throttle_time_ms
    }

    /// Sets the throttle time in the response.
    pub fn maybe_set_throttle_time_ms(&mut self, throttle_time_ms: i32) {
        self.data.set_throttle_time_ms(throttle_time_ms);
    }

    /// Returns a map from broker id to [`Node`].
    ///
    /// Corresponds to `DescribeClusterResponse.nodes`.
    pub fn nodes(&self) -> HashMap<i32, Node> {
        self.data
            .brokers
            .iter()
            .map(|b| {
                (
                    b.broker_id,
                    Node::with_rack_and_fenced(b.broker_id, b.host.clone(), b.port, b.rack.clone(), b.is_fenced),
                )
            })
            .collect()
    }

    /// Returns the error counts for this response.
    pub fn error_counts(&self) -> HashMap<Errors, i32> {
        single_error_count(Errors::for_code(self.data.error_code))
    }

    /// Parses a `DescribeClusterResponse` from a readable buffer at the given
    /// version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = DescribeClusterResponseData::read(readable, version)?;
        Ok(Self::new(data))
    }

    /// Whether the client should throttle on this response (always, v0+).
    pub fn should_client_throttle(&self, _version: i16) -> bool {
        true
    }
}

impl std::fmt::Display for DescribeClusterResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DescribeClusterResponse(data={:?})", self.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::describe_cluster_response_data::DescribeClusterBroker;

    #[test]
    fn nodes_maps_brokers_by_id() {
        let mut data = DescribeClusterResponseData::new();
        data.set_error_code(Errors::None.code());
        let mut b0 = DescribeClusterBroker::new();
        b0.set_broker_id(0);
        b0.set_host("host0".to_string());
        b0.set_port(9092);
        let mut b1 = DescribeClusterBroker::new();
        b1.set_broker_id(1);
        b1.set_host("host1".to_string());
        b1.set_port(9093);
        data.set_brokers(vec![b0, b1]);
        let response = DescribeClusterResponse::new(data);
        let nodes = response.nodes();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes.get(&0).unwrap().host(), "host0");
        assert_eq!(nodes.get(&1).unwrap().port(), 9093);
    }

    #[test]
    fn error_counts_reads_error_code() {
        let mut data = DescribeClusterResponseData::new();
        data.set_error_code(Errors::InvalidRequest.code());
        let response = DescribeClusterResponse::new(data);
        assert_eq!(response.error_counts().get(&Errors::InvalidRequest), Some(&1));
    }
}
