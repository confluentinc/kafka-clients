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

//! A new partition reassignment target.
//!
//! Corresponds to `org.apache.kafka.clients.admin.NewPartitionReassignment`.

use crate::common::Error;

/// A new partition reassignment, which can be applied via
/// `Admin::alter_partition_reassignments`.
///
/// Corresponds to `org.apache.kafka.clients.admin.NewPartitionReassignment`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewPartitionReassignment {
    target_replicas: Vec<i32>,
}

impl NewPartitionReassignment {
    /// Creates a new partition reassignment for the given target replicas.
    ///
    /// Mirrors the `NewPartitionReassignment(List<Integer>)` constructor.
    ///
    /// # Errors
    ///
    /// Returns an error (invalid argument) if no replicas are supplied,
    /// mirroring Java's `IllegalArgumentException`.
    pub fn new(target_replicas: Vec<i32>) -> Result<Self, Error> {
        if target_replicas.is_empty() {
            return Err(Error::local_illegal_argument(
                "Cannot create a new partition reassignment without any replicas",
            ));
        }
        Ok(Self { target_replicas })
    }

    /// Returns the target replicas of this reassignment.
    ///
    /// Mirrors `targetReplicas()`.
    pub fn target_replicas(&self) -> &[i32] {
        &self.target_replicas
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_stores_target_replicas() {
        let r = NewPartitionReassignment::new(vec![1, 2, 3]).unwrap();
        assert_eq!(r.target_replicas(), &[1, 2, 3]);
    }

    #[test]
    fn new_rejects_empty_replicas() {
        let err = NewPartitionReassignment::new(Vec::new()).unwrap_err();
        assert!(
            err.message()
                .contains("Cannot create a new partition reassignment without any replicas")
        );
    }
}
