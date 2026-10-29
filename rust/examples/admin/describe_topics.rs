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

//! Describes topics with `Admin::describe_topics_with_topic_names`: the topic
//! id and, per partition, the leader, the replicas and the in-sync replicas.
//! With `--authorized-operations` the broker also returns the operations the
//! caller may perform on each topic.
//!
//! ```text
//! cargo run --example admin_describe_topics -- --authorized-operations orders
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, DescribeTopicsOptions};
use confluent_kafka::common::Node;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, usage};

const USAGE: &str = "usage: admin_describe_topics [-X key=value]... [--authorized-operations] <topic>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn ids(nodes: &[Node]) -> String {
    let ids: Vec<String> = nodes.iter().map(|node| node.id().to_string()).collect();
    format!("[{}]", ids.join(","))
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let include_authorized_operations = args.flag("authorized-operations")?;
    let topics = args.positional()?;
    if topics.is_empty() {
        return usage("no topic given");
    }

    let result = admin.describe_topics_with_topic_names_options(
        &topics,
        DescribeTopicsOptions::new().set_include_authorized_operations(include_authorized_operations),
    );
    // A by-name call keys its futures by topic name.
    let Some(futures) = result.topic_name_values() else {
        return Ok(());
    };
    for topic in &topics {
        match futures[topic].get().await {
            Ok(description) => {
                println!(
                    "Topic {} (id {}){}",
                    description.name(),
                    description.topic_id(),
                    if description.is_internal() { " internal" } else { "" }
                );
                for partition in description.partitions() {
                    println!(
                        "  partition {}: leader {}, replicas {}, isr {}",
                        partition.partition(),
                        partition.leader().map_or("none".to_string(), |leader| leader.id().to_string()),
                        ids(partition.replicas()),
                        ids(partition.isr())
                    );
                }
                if let Some(operations) = description.authorized_operations() {
                    let names: Vec<String> = operations.iter().map(ToString::to_string).collect();
                    println!("  authorized operations: {}", names.join(", "));
                }
            },
            Err(error) => fail(format!("Failed to describe topic {topic}"), &error),
        }
    }
    Ok(())
}
