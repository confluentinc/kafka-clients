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

//! Deletes a group's committed offsets for the given partitions with
//! `Admin::delete_consumer_group_offsets`. The broker refuses to delete the
//! offsets of a topic the group is still subscribed to.
//!
//! ```text
//! cargo run --example admin_delete_consumer_group_offsets -- orders-app orders 0 orders 1
//! ```

use std::collections::HashSet;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, topic_partitions, usage};

const USAGE: &str = "usage: admin_delete_consumer_group_offsets [-X key=value]... <group> (<topic> <partition>)...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let Some((group, rest)) = positional.split_first() else {
        return usage("no group given");
    };
    let mut partitions = topic_partitions(rest, true)?;
    let mut seen = HashSet::new();
    partitions.retain(|partition| seen.insert(partition.clone()));

    let result = admin.delete_consumer_group_offsets(group, &seen);
    for partition in &partitions {
        let deleted = async { result.partition_result(partition)?.get().await };
        match deleted.await {
            Ok(()) => println!("Group {group}: {partition} offset deleted"),
            Err(error) => fail(format!("Failed to delete group {group} offset for {partition}"), &error),
        }
    }
    Ok(())
}
