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

//! Translated from `org.apache.kafka.clients.consumer.CloseOptionsTest`.
//!
//! Skipped tests:
//! - `operationShouldNotBeNull` — Java asserts `NullPointerException` from
//!   `Objects.requireNonNull` when a `null` operation is passed. In Rust,
//!   `with_group_membership_operation` takes a non-null
//!   `GroupMembershipOperation` enum by value; there is nothing to test.

use std::time::Duration;

use confluent_kafka::consumer::{CloseOptions, GroupMembershipOperation};

/// Translated from `CloseOptionsTest.operationShouldHaveDefaultValue`.
#[test]
fn operation_should_have_default_value() {
    let opts = CloseOptions::timeout(Duration::ZERO);
    assert_eq!(opts.group_membership_operation_value(), GroupMembershipOperation::Default);
}

/// Translated from `CloseOptionsTest.timeoutCouldBeNull`.
///
/// Java passes `null` for the `Duration` to verify the field stays
/// `Optional.empty()`. Rust's `Duration` is non-null; the equivalent
/// "no timeout set" path is the no-arg [`CloseOptions::default`]
/// constructor, which leaves the internal `Option<Duration>` field as
/// `None`.
#[test]
fn timeout_could_be_null() {
    let close_options = CloseOptions::default();
    assert_eq!(close_options.timeout_value(), None);
}

/// Translated from `CloseOptionsTest.timeoutShouldBeDefaultEmpty`.
#[test]
fn timeout_should_be_default_empty() {
    let opts = CloseOptions::group_membership_operation(GroupMembershipOperation::Default);
    assert_eq!(opts.timeout_value(), None);
}
