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

//! Lists the ongoing partition reassignments with
//! `Admin::list_partition_reassignments`: per partition its current replicas
//! and the replicas being added and removed. Without partitions it lists every
//! reassignment in the cluster.
//!
//! ```text
//! cargo run --example admin_list_partition_reassignments -- orders 0 orders 1
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, topic_partitions};

const USAGE: &str = "usage: admin_list_partition_reassignments [-X key=value]... [<topic> <partition>]...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn ids(replicas: &[i32]) -> String {
    let ids: Vec<String> = replicas.iter().map(ToString::to_string).collect();
    format!("[{}]", ids.join(","))
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let partitions = topic_partitions(&positional, false)?;

    let result = if partitions.is_empty() {
        admin.list_partition_reassignments()
    } else {
        admin.list_partition_reassignments_with_partitions(partitions.into_iter().collect())
    };
    let mut reassignments: Vec<_> = result.reassignments().get().await?.into_iter().collect();
    if reassignments.is_empty() {
        println!("No ongoing reassignments");
        return Ok(());
    }
    reassignments
        .sort_by(|(left, _), (right, _)| (left.topic(), left.partition()).cmp(&(right.topic(), right.partition())));
    for (partition, reassignment) in &reassignments {
        println!(
            "Partition {partition}: replicas {}, adding {}, removing {}",
            ids(reassignment.replicas()),
            ids(reassignment.adding_replicas()),
            ids(reassignment.removing_replicas())
        );
    }
    Ok(())
}
