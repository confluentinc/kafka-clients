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

//! An ongoing partition reassignment listed via
//! `Admin::list_partition_reassignments`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.PartitionReassignment`.

/// A partition reassignment, which has been listed via
/// `Admin::list_partition_reassignments`.
///
/// Corresponds to `org.apache.kafka.clients.admin.PartitionReassignment`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionReassignment {
    replicas: Vec<i32>,
    adding_replicas: Vec<i32>,
    removing_replicas: Vec<i32>,
}

impl PartitionReassignment {
    /// Creates a partition reassignment from its replica sets.
    pub fn new(replicas: Vec<i32>, adding_replicas: Vec<i32>, removing_replicas: Vec<i32>) -> Self {
        Self { replicas, adding_replicas, removing_replicas }
    }

    /// The brokers which this partition currently resides on.
    ///
    /// Mirrors `replicas()`.
    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// The brokers that we are adding this partition to as part of a
    /// reassignment. A subset of replicas.
    ///
    /// Mirrors `addingReplicas()`.
    pub fn adding_replicas(&self) -> &[i32] {
        &self.adding_replicas
    }

    /// The brokers that we are removing this partition from as part of a
    /// reassignment. A subset of replicas.
    ///
    /// Mirrors `removingReplicas()`.
    pub fn removing_replicas(&self) -> &[i32] {
        &self.removing_replicas
    }
}

impl std::fmt::Display for PartitionReassignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PartitionReassignment(replicas={:?}, addingReplicas={:?}, removingReplicas={:?})",
            self.replicas, self.adding_replicas, self.removing_replicas
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_return_stored_values() {
        let r = PartitionReassignment::new(vec![1, 2, 3], vec![4], vec![1]);
        assert_eq!(r.replicas(), &[1, 2, 3]);
        assert_eq!(r.adding_replicas(), &[4]);
        assert_eq!(r.removing_replicas(), &[1]);
    }
}
