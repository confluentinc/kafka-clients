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

//! Describes the cluster with `Admin::describe_cluster_with_options`: its id,
//! the controller and the brokers. With `--authorized-operations` the broker
//! also returns the operations the caller may perform on the cluster, and with
//! `--fenced` the brokers list includes the fenced ones.
//!
//! ```text
//! cargo run --example admin_describe_cluster -- --authorized-operations
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, DescribeClusterOptions};
use confluent_kafka::common::Node;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, usage};

const USAGE: &str = "usage: admin_describe_cluster [-X key=value]... [--authorized-operations] [--fenced]";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn node(node: &Node) -> String {
    format!(
        "{} at {}:{}{}{}",
        node.id(),
        node.host(),
        node.port(),
        node.rack().map_or(String::new(), |rack| format!(", rack {rack}")),
        if node.is_fenced() { ", fenced" } else { "" }
    )
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let include_authorized_operations = args.flag("authorized-operations")?;
    let include_fenced_brokers = args.flag("fenced")?;
    if !args.positional()?.is_empty() {
        return usage("unexpected argument");
    }

    let result = admin.describe_cluster_with_options(
        DescribeClusterOptions::new()
            .set_include_authorized_operations(include_authorized_operations)
            .set_include_fenced_brokers(include_fenced_brokers),
    );
    println!("Cluster id: {}", result.cluster_id().get().await?);
    match result.controller().get().await? {
        Some(controller) => println!("Controller: {}", node(&controller)),
        None => println!("Controller: none"),
    }
    let mut nodes = result.nodes().get().await?;
    nodes.sort_by_key(|node| node.id());
    for broker in &nodes {
        println!("Broker {}", node(broker));
    }
    // Absent unless the operations were asked for.
    if let Some(operations) = result.authorized_operations().get().await? {
        let names: Vec<String> = operations.iter().map(ToString::to_string).collect();
        println!("Authorized operations: {}", names.join(", "));
    }
    Ok(())
}
