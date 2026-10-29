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

//! Describes consumer groups with `Admin::describe_consumer_groups_with_options`:
//! the group's type, state, partition assignor and coordinator, and per member
//! its ids, host and assigned partitions. Both consumer (KIP-848) and classic
//! groups are described. With `--authorized-operations` the broker also returns
//! the operations the caller may perform on each group.
//!
//! ```text
//! cargo run --example admin_describe_consumer_groups -- --authorized-operations orders-app
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, DescribeConsumerGroupsOptions, MemberAssignment};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, usage};

const USAGE: &str = "usage: admin_describe_consumer_groups [-X key=value]... [--authorized-operations] <group>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn partitions(assignment: &MemberAssignment) -> String {
    let mut partitions: Vec<_> = assignment.topic_partitions().iter().collect();
    partitions.sort_by(|a, b| (a.topic(), a.partition()).cmp(&(b.topic(), b.partition())));
    let names: Vec<String> = partitions.iter().map(ToString::to_string).collect();
    format!("[{}]", names.join(","))
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let include_authorized_operations = args.flag("authorized-operations")?;
    let groups = args.positional()?;
    if groups.is_empty() {
        return usage("no group given");
    }

    let result = admin.describe_consumer_groups_with_options(
        &groups,
        DescribeConsumerGroupsOptions::new().set_include_authorized_operations(include_authorized_operations),
    );
    let futures = result.described_groups();
    for group in &groups {
        match futures[group].get().await {
            Ok(description) => {
                println!(
                    "Group {}: type {}, state {}, assignor {:?}, coordinator {}, simple {}",
                    description.group_id(),
                    description.r#type(),
                    description.group_state(),
                    description.partition_assignor(),
                    description
                        .coordinator()
                        .map_or("none".to_string(), |node| node.id().to_string()),
                    description.is_simple_consumer_group()
                );
                // Only consumer (KIP-848) groups have epochs.
                if let (Some(group_epoch), Some(target_assignment_epoch)) =
                    (description.group_epoch(), description.target_assignment_epoch())
                {
                    println!("  group epoch {group_epoch}, target assignment epoch {target_assignment_epoch}");
                }
                for member in description.members() {
                    println!(
                        "  member {}: group instance id {}, client id {:?}, host {}, assignment {}",
                        member.consumer_id(),
                        member.group_instance_id().unwrap_or("none"),
                        member.client_id(),
                        member.host(),
                        partitions(member.assignment())
                    );
                }
                if let Some(operations) = description.authorized_operations() {
                    let names: Vec<String> = operations.iter().map(ToString::to_string).collect();
                    println!("  authorized operations: {}", names.join(", "));
                }
            },
            Err(error) => fail(format!("Failed to describe group {group}"), &error),
        }
    }
    Ok(())
}
