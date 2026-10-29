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

//! Sets a group's committed offsets with `Admin::alter_consumer_group_offsets`.
//! The group must have no active members: the broker rejects the change while
//! consumers are in the group. A group that does not exist yet is created with
//! the given offsets.
//!
//! ```text
//! cargo run --example admin_alter_consumer_group_offsets -- orders-app orders 0 42 orders 1 17
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;
use confluent_kafka::consumer::OffsetAndMetadata;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, groups, parse, topic_partition, usage};

const USAGE: &str =
    "usage: admin_alter_consumer_group_offsets [-X key=value]... <group> (<topic> <partition> <offset>)...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let Some((group, rest)) = positional.split_first() else {
        return usage("no group given");
    };

    let mut partitions = Vec::new();
    let mut offsets = HashMap::new();
    for spec in groups(rest, 3, true)? {
        let partition = topic_partition(spec)?;
        let offset = OffsetAndMetadata::new(parse("offset", &spec[2])?)?;
        if offsets.insert(partition.clone(), offset).is_none() {
            partitions.push(partition);
        }
    }

    let result = admin.alter_consumer_group_offsets(group, &offsets);
    for partition in &partitions {
        match result.partition_result(partition).get().await {
            Ok(()) => println!("Group {group}: {partition} offset set to {}", offsets[partition].offset()),
            Err(error) => fail(format!("Failed to alter group {group} offset for {partition}"), &error),
        }
    }
    Ok(())
}
