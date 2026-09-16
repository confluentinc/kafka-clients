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

//! `ListGroups` request handling.
//!
//! Corresponds to `org.apache.kafka.common.requests.ListGroupsRequest`.
//!
//! Possible error codes:
//!  - `CoordinatorLoadInProgress` (14)
//!  - `CoordinatorNotAvailable` (15)
//!  - `AuthorizationFailed` (29)

use std::io;

use crate::ListGroupsRequestData;
use crate::ListGroupsResponseData;
use crate::common::GroupType;
use crate::common::protocol::{ApiKeys, Errors, Readable};

use super::ConcreteRequest;
use super::ConcreteResponse;
use super::ListGroupsResponse;
use super::RequestBuilder;

/// A `ListGroups` request.
///
/// Corresponds to `org.apache.kafka.common.requests.ListGroupsRequest`.
#[derive(Debug, Clone)]
pub struct ListGroupsRequest {
    data: ListGroupsRequestData,
    version: i16,
}

impl ListGroupsRequest {
    /// Creates a new `ListGroupsRequest` from data and version.
    pub fn new(data: ListGroupsRequestData, version: i16) -> Self {
        Self { data, version }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListGroupsRequestData {
        &self.data
    }

    /// Returns a mutable reference to the underlying data.
    pub(crate) fn data_mut(&mut self) -> &mut ListGroupsRequestData {
        &mut self.data
    }

    /// Returns the API version of this request.
    pub fn version(&self) -> i16 {
        self.version
    }

    /// Returns the API key for this request.
    pub fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_GROUPS
    }

    /// Builds the canonical error response for this request, matching Java's
    /// `ListGroupsRequest.getErrorResponse(throttleTimeMs, Throwable)`.
    pub fn get_error_response(&self, throttle_time_ms: i32, error: &Errors) -> ConcreteResponse {
        let mut data = ListGroupsResponseData::new();
        data.set_groups(Vec::new()).set_error_code(error.code());
        if self.version >= 1 {
            data.set_throttle_time_ms(throttle_time_ms);
        }
        ConcreteResponse::ListGroups(ListGroupsResponse::new(data))
    }

    /// Parses a `ListGroupsRequest` from a readable buffer at the given version.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails.
    pub fn parse(readable: &mut dyn Readable, version: i16) -> io::Result<Self> {
        let data = ListGroupsRequestData::read(readable, version)?;
        Ok(Self::new(data, version))
    }
}

impl std::fmt::Display for ListGroupsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ListGroupsRequest(version={}, data={})", self.version, self.data)
    }
}

/// Builder for [`ListGroupsRequest`].
///
/// Corresponds to `ListGroupsRequest.Builder` in Java.
#[derive(Debug, Clone)]
pub struct ListGroupsRequestBuilder {
    data: ListGroupsRequestData,
    oldest_allowed_version: i16,
    latest_allowed_version: i16,
}

impl ListGroupsRequestBuilder {
    /// Creates a builder wrapping the given data with the full supported
    /// version range.
    pub fn new(data: ListGroupsRequestData) -> Self {
        Self {
            data,
            oldest_allowed_version: ApiKeys::LIST_GROUPS.oldest_version(),
            latest_allowed_version: ApiKeys::LIST_GROUPS.latest_version(),
        }
    }

    /// Returns a reference to the underlying data.
    pub fn data(&self) -> &ListGroupsRequestData {
        &self.data
    }
}

impl RequestBuilder for ListGroupsRequestBuilder {
    fn api_key(&self) -> &'static ApiKeys {
        &ApiKeys::LIST_GROUPS
    }

    fn oldest_allowed_version(&self) -> i16 {
        self.oldest_allowed_version
    }

    fn latest_allowed_version(&self) -> i16 {
        self.latest_allowed_version
    }

    fn build_version(&mut self, version: i16) -> io::Result<ConcreteRequest> {
        // Mirrors `ListGroupsRequest.Builder.build(short version)`.
        if !self.data.states_filter.is_empty() && version < 4 {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "The broker only supports ListGroups v{version}, but we need v4 or newer to \
                     request groups by states."
                ),
            ));
        }

        if !self.data.types_filter.is_empty() && version < 5 {
            // Types filter is supported by brokers with version 3.8.0 or later. Older brokers only
            // support classic groups, so listing consumer groups on an older broker does not need
            // to use a types filter. If the types filter is only for consumer and classic, or just
            // classic groups, it can be safely omitted. This allows a modern admin client to list
            // consumer groups on older brokers in a straightforward way.
            let mut types_copy: std::collections::HashSet<String> = self.data.types_filter.iter().cloned().collect();
            let contained_classic = types_copy.remove(&GroupType::Classic.to_string());
            let contained_consumer = types_copy.remove(&GroupType::Consumer.to_string());
            if !types_copy.is_empty() || (!contained_classic && contained_consumer) {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "The broker only supports ListGroups v{version}, but we need v5 or newer to \
                         request groups by type. Requested group types: [{}].",
                        self.data.types_filter.join(", ")
                    ),
                ));
            }
            let mut data = self.data.clone();
            data.set_types_filter(Vec::new());
            return Ok(ConcreteRequest::ListGroups(ListGroupsRequest::new(data, version)));
        }
        Ok(ConcreteRequest::ListGroups(ListGroupsRequest::new(self.data.clone(), version)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Byte-level wire-encoding check for a v4 (flexible) request with a single
    /// states filter. Field-by-field:
    ///   states_filter: compact array len 1 -> 0x02 (N+1)
    ///     "Stable": compact string len 6 -> 0x07 (N+1), then bytes 53 74 61 62 6c 65
    ///   (types_filter is v5+, absent at v4)
    ///   _tagged_fields: 0x00
    #[test]
    fn test_serialize_known_byte_vector_v4() {
        let mut data = ListGroupsRequestData::new();
        data.set_states_filter(vec!["Stable".to_string()]);
        let mut builder = ListGroupsRequestBuilder::new(data);
        let mut req = builder.build_version(4).unwrap();
        let expected: &[u8] = &[0x02, 0x07, 0x53, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x00];
        assert_eq!(req.serialize().unwrap().into_buffer().as_slice(), expected);
    }

    #[test]
    fn test_api_key() {
        let builder = ListGroupsRequestBuilder::new(ListGroupsRequestData::new());
        assert_eq!(builder.api_key(), &ApiKeys::LIST_GROUPS);
    }

    /// A states filter requires v4+; older versions are rejected.
    #[test]
    fn test_states_filter_requires_v4() {
        let mut data = ListGroupsRequestData::new();
        data.set_states_filter(vec!["Stable".to_string()]);
        let mut builder = ListGroupsRequestBuilder::new(data);
        let err = builder.build_version(3).expect_err("states filter must require v4");
        assert!(
            err.to_string().contains("v4 or newer to request groups by states"),
            "got: {err}"
        );
    }

    /// A `[classic]`-only types filter is omitted (not rejected) on pre-v5.
    #[test]
    fn test_classic_only_types_filter_omitted_pre_v5() {
        let mut data = ListGroupsRequestData::new();
        data.set_types_filter(vec![GroupType::Classic.to_string()]);
        let mut builder = ListGroupsRequestBuilder::new(data);
        let built = builder.build_version(4).expect("classic-only filter omitted");
        match built {
            ConcreteRequest::ListGroups(req) => assert!(req.data().types_filter.is_empty()),
            other => panic!("expected ListGroups variant, got {other:?}"),
        }
    }

    /// A `[classic, consumer]` types filter is omitted on pre-v5.
    #[test]
    fn test_classic_and_consumer_types_filter_omitted_pre_v5() {
        let mut data = ListGroupsRequestData::new();
        data.set_types_filter(vec![GroupType::Classic.to_string(), GroupType::Consumer.to_string()]);
        let mut builder = ListGroupsRequestBuilder::new(data);
        let built = builder.build_version(4).expect("classic+consumer filter omitted");
        match built {
            ConcreteRequest::ListGroups(req) => assert!(req.data().types_filter.is_empty()),
            other => panic!("expected ListGroups variant, got {other:?}"),
        }
    }

    /// A `[consumer]`-only types filter is rejected on pre-v5 (no classic).
    #[test]
    fn test_consumer_only_types_filter_rejected_pre_v5() {
        let mut data = ListGroupsRequestData::new();
        data.set_types_filter(vec![GroupType::Consumer.to_string()]);
        let mut builder = ListGroupsRequestBuilder::new(data);
        let err = builder
            .build_version(4)
            .expect_err("consumer-only filter must be rejected pre-v5");
        assert!(err.to_string().contains("v5 or newer to request groups by type"), "got: {err}");
    }

    /// A non-consumer/classic types filter is rejected on pre-v5.
    #[test]
    fn test_share_types_filter_rejected_pre_v5() {
        let mut data = ListGroupsRequestData::new();
        data.set_types_filter(vec![GroupType::Share.to_string()]);
        let mut builder = ListGroupsRequestBuilder::new(data);
        let err = builder.build_version(4).expect_err("share filter must be rejected pre-v5");
        assert!(err.to_string().contains("v5 or newer"), "got: {err}");
    }

    /// The error response carries the exception's error code and no groups.
    #[test]
    fn test_get_error_response() {
        let request = ListGroupsRequest::new(ListGroupsRequestData::new(), 4);
        let resp = request.get_error_response(100, &Errors::CoordinatorNotAvailable);
        match resp {
            ConcreteResponse::ListGroups(r) => {
                assert_eq!(r.data().error_code, Errors::CoordinatorNotAvailable.code());
                assert!(r.data().groups.is_empty());
                assert_eq!(r.data().throttle_time_ms, 100);
            },
            other => panic!("expected ListGroups response, got {other:?}"),
        }
    }
}
