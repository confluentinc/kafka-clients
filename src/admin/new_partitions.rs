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

//! Describes new partitions for a topic in a call to `Admin::create_partitions`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.NewPartitions`.

/// Describes new partitions for a particular topic in a call to
/// `Admin::create_partitions`.
///
/// Corresponds to `org.apache.kafka.clients.admin.NewPartitions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewPartitions {
    total_count: i32,
    new_assignments: Option<Vec<Vec<i32>>>,
}

impl NewPartitions {
    /// Increase the partition count for a topic to the given `total_count`.
    /// The assignment of new replicas to brokers will be decided by the broker.
    ///
    /// `total_count` is the total number of partitions after the operation
    /// succeeds.
    pub fn increase_to(total_count: i32) -> Self {
        Self { total_count, new_assignments: None }
    }

    /// Increase the partition count for a topic to the given `total_count`
    /// assigning the new partitions according to the given `new_assignments`.
    ///
    /// The length of the given `new_assignments` should equal
    /// `total_count - old_count`, since the assignment of existing partitions
    /// is not changed. Each inner list of `new_assignments` should have a
    /// length equal to the topic's replication factor. The first broker id in
    /// each inner list is the "preferred replica".
    ///
    /// `total_count` is the total number of partitions after the operation
    /// succeeds; `new_assignments` are the replica assignments for the new
    /// partitions.
    pub fn increase_to_with_assignments(total_count: i32, new_assignments: Vec<Vec<i32>>) -> Self {
        Self { total_count, new_assignments: Some(new_assignments) }
    }

    /// The total number of partitions after the operation succeeds.
    pub fn total_count(&self) -> i32 {
        self.total_count
    }

    /// The replica assignments for the new partitions, or `None` if the
    /// assignment will be done by the controller.
    pub fn assignments(&self) -> Option<&Vec<Vec<i32>>> {
        self.new_assignments.as_ref()
    }
}

impl std::fmt::Display for NewPartitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(totalCount={}, newAssignments={:?})",
            self.total_count, self.new_assignments
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increase_to_has_no_assignments() {
        let np = NewPartitions::increase_to(3);
        assert_eq!(np.total_count(), 3);
        assert!(np.assignments().is_none());
    }

    #[test]
    fn increase_to_with_assignments() {
        let np = NewPartitions::increase_to_with_assignments(6, vec![vec![1, 2], vec![2, 3], vec![3, 1]]);
        assert_eq!(np.total_count(), 6);
        assert_eq!(np.assignments(), Some(&vec![vec![1, 2], vec![2, 3], vec![3, 1]]));
    }
}
