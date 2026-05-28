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

//! `MemberStateListener` — listener trait notified on member-epoch / group
//! assignment updates.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.MemberStateListener`.

#![allow(dead_code)]

use std::collections::HashSet;

use crate::common::TopicPartition;

/// Listener for getting notified of membership state changes.
///
/// Implementations are registered with the membership manager and are
/// invoked synchronously on the background task when the member epoch is
/// updated, or when the group assignment changes.
///
/// Java is an `interface MemberStateListener`. The Rust translation is a
/// trait — implementations live on [`super::commit_request_manager`]
/// (Phase 9) and on the consumer's app-thread state notifier (Phase 11).
///
/// Phase 8 ships only the trait definition; the Phase 9 worktree wires
/// up `CommitRequestManager` to implement it (merge-time reconciliation).
pub(crate) trait MemberStateListener: Send + Sync + 'static {
    /// Called whenever the epoch changes with new values received from the
    /// broker or cleared if the member is not part of the group anymore
    /// (when it gets fenced, leaves the group or fails).
    ///
    /// # Parameters
    ///
    /// * `member_epoch` — New member epoch received from the broker. `None`
    ///   if the member is not part of the group anymore.
    /// * `member_id` — Current member ID. It won't change until the process
    ///   is terminated.
    ///
    /// Java: `void onMemberEpochUpdated(Optional<Integer>, String)`.
    fn on_member_epoch_updated(&self, member_epoch: Option<i32>, member_id: &str);

    /// Invoked when a group member's assigned set of partitions changes.
    /// Assignments can change via group coordinator partition assignment
    /// changes, unsubscribing, and when leaving the group.
    ///
    /// # Parameters
    ///
    /// * `partitions` — New assignment, can be empty but not "null".
    ///
    /// Java: `default void onGroupAssignmentUpdated(Set<TopicPartition>) {}`.
    /// Default implementation is a no-op.
    fn on_group_assignment_updated(&self, _partitions: &HashSet<TopicPartition>) {}
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    /// Trait-object smoke test — confirms that:
    /// 1. The trait can be used through `Arc<dyn MemberStateListener>`.
    /// 2. The default `on_group_assignment_updated` doesn't panic.
    /// 3. `on_member_epoch_updated` plumbs through both `Some` and `None`
    ///    epoch values.
    #[derive(Default)]
    struct RecordingListener {
        epoch_calls: Mutex<Vec<(Option<i32>, String)>>,
        assignment_calls: Mutex<Vec<HashSet<TopicPartition>>>,
    }

    impl MemberStateListener for RecordingListener {
        fn on_member_epoch_updated(&self, member_epoch: Option<i32>, member_id: &str) {
            self.epoch_calls.lock().unwrap().push((member_epoch, member_id.to_string()));
        }

        fn on_group_assignment_updated(&self, partitions: &HashSet<TopicPartition>) {
            self.assignment_calls.lock().unwrap().push(partitions.clone());
        }
    }

    #[test]
    fn epoch_updates_recorded() {
        let listener = RecordingListener::default();
        listener.on_member_epoch_updated(Some(42), "m1");
        listener.on_member_epoch_updated(None, "m1");
        let calls = listener.epoch_calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], (Some(42), "m1".to_string()));
        assert_eq!(calls[1], (None, "m1".to_string()));
    }

    #[test]
    fn default_on_group_assignment_updated_is_noop() {
        // A listener that only implements the required method must inherit
        // the no-op default for `on_group_assignment_updated`.
        struct EpochOnly;
        impl MemberStateListener for EpochOnly {
            fn on_member_epoch_updated(&self, _: Option<i32>, _: &str) {}
        }
        let l = EpochOnly;
        let mut tps = HashSet::new();
        tps.insert(TopicPartition::new("t".to_string(), 0));
        l.on_group_assignment_updated(&tps); // must not panic
    }
}
