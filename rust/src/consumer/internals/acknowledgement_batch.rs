// Copyright 2026 Confluent Inc.
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

//! A contiguous range of acknowledged offsets, in the form the share RPCs
//! send.
//!
//! Translated from `org.apache.kafka.clients.consumer.internals.AcknowledgementBatch`.

use std::fmt;

use crate::{share_acknowledge_request_data, share_fetch_request_data};

/// The acknowledgements of the offsets `first_offset..=last_offset`: one
/// acknowledge type per offset, or a single type for the whole range.
///
/// The acknowledge types are Java's `List<Byte>`, which Java lets be `null`;
/// nothing in the client sets it to `null`, so it is a plain `Vec` here.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch")]
pub(crate) struct AcknowledgementBatch {
    first_offset: i64,
    last_offset: i64,
    acknowledge_types: Vec<i8>,
}

impl AcknowledgementBatch {
    /// An empty batch at offset 0.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch#AcknowledgementBatch")]
    pub(crate) fn new() -> Self {
        Self { first_offset: 0, last_offset: 0, acknowledge_types: Vec::new() }
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch#firstOffset")]
    pub(crate) fn first_offset(&self) -> i64 {
        self.first_offset
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch#lastOffset")]
    pub(crate) fn last_offset(&self) -> i64 {
        self.last_offset
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch#acknowledgeTypes")]
    pub(crate) fn acknowledge_types(&self) -> &[i8] {
        &self.acknowledge_types
    }

    /// The acknowledge types, to modify: Java's `acknowledgeTypes()` returns
    /// the mutable list, which `Acknowledgements` appends to and truncates.
    /// [`Self::acknowledge_types`] is the same Java method's read-only view.
    pub(crate) fn acknowledge_types_mut(&mut self) -> &mut Vec<i8> {
        &mut self.acknowledge_types
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch#setFirstOffset")]
    pub(crate) fn set_first_offset(&mut self, v: i64) -> &mut Self {
        self.first_offset = v;
        self
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch#setLastOffset")]
    pub(crate) fn set_last_offset(&mut self, v: i64) -> &mut Self {
        self.last_offset = v;
        self
    }

    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch#setAcknowledgeTypes")]
    pub(crate) fn set_acknowledge_types(&mut self, v: Vec<i8>) -> &mut Self {
        self.acknowledge_types = v;
        self
    }

    /// The batch as a `ShareAcknowledge` request batch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch#toShareAcknowledgeRequest")]
    pub(crate) fn to_share_acknowledge_request(&self) -> share_acknowledge_request_data::AcknowledgementBatch {
        let mut batch = share_acknowledge_request_data::AcknowledgementBatch::new();
        batch
            .set_first_offset(self.first_offset)
            .set_last_offset(self.last_offset)
            .set_acknowledge_types(self.acknowledge_types.clone());
        batch
    }

    /// The batch as a `ShareFetch` request batch.
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.AcknowledgementBatch#toShareFetchRequest")]
    pub(crate) fn to_share_fetch_request(&self) -> share_fetch_request_data::AcknowledgementBatch {
        let mut batch = share_fetch_request_data::AcknowledgementBatch::new();
        batch
            .set_first_offset(self.first_offset)
            .set_last_offset(self.last_offset)
            .set_acknowledge_types(self.acknowledge_types.clone());
        batch
    }
}

impl fmt::Display for AcknowledgementBatch {
    // Java's `toString()`, the types through `MessageUtil.deepToString`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AcknowledgementBatch(firstOffset={}, lastOffset={}, acknowledgeTypes=[",
            self.first_offset, self.last_offset
        )?;
        for (index, acknowledge_type) in self.acknowledge_types.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{acknowledge_type}")?;
        }
        f.write_str("])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Java has no `AcknowledgementBatchTest`; these pin the conversions to the
    // two request batches, `equals` and `toString`.

    fn batch(first_offset: i64, last_offset: i64, acknowledge_types: Vec<i8>) -> AcknowledgementBatch {
        let mut batch = AcknowledgementBatch::new();
        batch
            .set_first_offset(first_offset)
            .set_last_offset(last_offset)
            .set_acknowledge_types(acknowledge_types);
        batch
    }

    #[test]
    fn test_new_is_empty_at_zero() {
        let batch = AcknowledgementBatch::new();
        assert_eq!(0, batch.first_offset());
        assert_eq!(0, batch.last_offset());
        assert!(batch.acknowledge_types().is_empty());
    }

    #[test]
    fn test_to_requests_copy_every_field() {
        let batch = batch(5, 7, vec![1, 0, 3]);

        let acknowledge = batch.to_share_acknowledge_request();
        assert_eq!(5, acknowledge.first_offset());
        assert_eq!(7, acknowledge.last_offset());
        assert_eq!(&[1, 0, 3], acknowledge.acknowledge_types().as_slice());

        let fetch = batch.to_share_fetch_request();
        assert_eq!(5, fetch.first_offset());
        assert_eq!(7, fetch.last_offset());
        assert_eq!(&[1, 0, 3], fetch.acknowledge_types().as_slice());
    }

    #[test]
    fn test_equals_compares_every_field() {
        assert_eq!(batch(1, 2, vec![1, 2]), batch(1, 2, vec![1, 2]));
        assert_ne!(batch(1, 2, vec![1, 2]), batch(0, 2, vec![1, 2]));
        assert_ne!(batch(1, 2, vec![1, 2]), batch(1, 3, vec![1, 2]));
        assert_ne!(batch(1, 2, vec![1, 2]), batch(1, 2, vec![2, 1]));
    }

    #[test]
    fn test_to_string() {
        assert_eq!(
            "AcknowledgementBatch(firstOffset=3, lastOffset=5, acknowledgeTypes=[1, 0, 2])",
            batch(3, 5, vec![1, 0, 2]).to_string()
        );
        assert_eq!(
            "AcknowledgementBatch(firstOffset=0, lastOffset=0, acknowledgeTypes=[])",
            AcknowledgementBatch::new().to_string()
        );
    }
}
