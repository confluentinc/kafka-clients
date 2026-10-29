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

//! Starts or cancels partition reassignments with
//! `Admin::alter_partition_reassignments`. Each partition is given as topic and
//! partition followed by its new replicas, a comma-separated list of broker ids
//! whose first entry becomes the preferred leader, or by `cancel` to stop an
//! ongoing reassignment. The controller moves the replicas in the background;
//! `admin_list_partition_reassignments` shows the ones still in progress.
//!
//! ```text
//! cargo run --example admin_alter_partition_reassignments -- orders 0 1,2,3 orders 1 cancel
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, NewPartitionReassignment};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, comma_list, fail, groups, parse, topic_partition, usage};

const USAGE: &str =
    "usage: admin_alter_partition_reassignments [-X key=value]... <topic> <partition> <broker_id,...|cancel>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn ids(reassignment: &NewPartitionReassignment) -> String {
    let ids: Vec<String> = reassignment.target_replicas().iter().map(ToString::to_string).collect();
    format!("[{}]", ids.join(","))
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let mut partitions = Vec::new();
    let mut reassignments = HashMap::new();
    for group in groups(&positional, 3, true)? {
        let partition = topic_partition(group)?;
        // No target replicas cancels the partition's reassignment.
        let reassignment = if group[2] == "cancel" {
            None
        } else {
            let replicas = comma_list(&group[2])
                .iter()
                .map(|text| parse("broker id", text))
                .collect::<Result<Vec<i32>, _>>()?;
            if replicas.is_empty() {
                return usage(format!("no replicas given for partition {partition}"));
            }
            Some(NewPartitionReassignment::new(replicas)?)
        };
        if reassignments.insert(partition.clone(), reassignment).is_none() {
            partitions.push(partition);
        }
    }

    let result = admin.alter_partition_reassignments(&reassignments);
    for partition in &partitions {
        let outcome = result.values()[partition].get().await;
        match (&reassignments[partition], outcome) {
            (Some(reassignment), Ok(())) => {
                println!("Reassignment of partition {partition} to {} started", ids(reassignment))
            },
            (Some(reassignment), Err(error)) => fail(
                format!("Failed to reassign partition {partition} to {}", ids(reassignment)),
                &error,
            ),
            (None, Ok(())) => println!("Reassignment of partition {partition} cancelled"),
            (None, Err(error)) => fail(format!("Failed to cancel the reassignment of partition {partition}"), &error),
        }
    }
    Ok(())
}
