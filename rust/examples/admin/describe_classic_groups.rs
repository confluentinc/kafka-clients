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

//! Describes classic groups with `Admin::describe_classic_groups_with_options`:
//! the group's state, protocol type, protocol and coordinator, and per member
//! its ids, host and assigned partitions. With `--authorized-operations` the
//! broker also returns the operations the caller may perform on each group.
//!
//! ```text
//! cargo run --example admin_describe_classic_groups -- --authorized-operations legacy-app
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, DescribeClassicGroupsOptions, MemberAssignment};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, usage};

const USAGE: &str = "usage: admin_describe_classic_groups [-X key=value]... [--authorized-operations] <group>...";

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

    let result = admin.describe_classic_groups_with_options(
        &groups,
        DescribeClassicGroupsOptions::new().set_include_authorized_operations(include_authorized_operations),
    );
    let futures = result.described_groups();
    for group in &groups {
        match futures[group].get().await {
            Ok(description) => {
                // `protocol` is the protocol type (`consumer` for consumer
                // groups), `protocol_data` the chosen protocol (the assignor).
                println!(
                    "Group {}: state {}, protocol {:?}, protocol data {:?}, coordinator {}, simple {}",
                    description.group_id(),
                    description.state(),
                    description.protocol(),
                    description.protocol_data(),
                    description
                        .coordinator()
                        .map_or("none".to_string(), |node| node.id().to_string()),
                    description.is_simple_consumer_group()
                );
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
