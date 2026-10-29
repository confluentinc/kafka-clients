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

//! Describes the log directories of brokers with `Admin::describe_log_dirs`:
//! per directory its total and usable bytes and, per replica it holds, the
//! replica's size and offset lag. A future replica is one the broker is moving
//! into that directory.
//!
//! ```text
//! cargo run --example admin_describe_log_dirs -- 0 1 2
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, parse, usage};

const USAGE: &str = "usage: admin_describe_log_dirs [-X key=value]... <broker_id>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn bytes(value: Option<i64>) -> String {
    value.map_or("unknown".to_string(), |bytes| bytes.to_string())
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    if positional.is_empty() {
        return usage("no broker given");
    }
    let mut brokers = Vec::new();
    for text in &positional {
        let broker: i32 = parse("broker id", text)?;
        if !brokers.contains(&broker) {
            brokers.push(broker);
        }
    }

    let result = admin.describe_log_dirs(&brokers);
    for broker in &brokers {
        let descriptions = match result.descriptions()[broker].get().await {
            Ok(descriptions) => descriptions,
            Err(error) => {
                fail(format!("Failed to describe log dirs of broker {broker}"), &error);
                continue;
            },
        };
        println!("Broker {broker}: {} log dirs", descriptions.len());
        let mut dirs: Vec<_> = descriptions.iter().collect();
        dirs.sort_by_key(|(dir, _)| *dir);
        for (dir, description) in dirs {
            if let Some(error) = description.error() {
                fail(format!("Failed to describe log dir {dir} of broker {broker}"), error);
                continue;
            }
            println!(
                "  log dir {dir}: total {} bytes, usable {} bytes{}",
                bytes(description.total_bytes()),
                bytes(description.usable_bytes()),
                if description.is_cordoned() { ", cordoned" } else { "" }
            );
            let mut replicas: Vec<_> = description.replica_infos().iter().collect();
            replicas.sort_by(|(left, _), (right, _)| {
                (left.topic(), left.partition()).cmp(&(right.topic(), right.partition()))
            });
            for (partition, info) in replicas {
                println!(
                    "    replica {partition}: size {}, offset lag {}{}",
                    info.size(),
                    info.offset_lag(),
                    if info.is_future() { ", future" } else { "" }
                );
            }
        }
    }
    Ok(())
}
