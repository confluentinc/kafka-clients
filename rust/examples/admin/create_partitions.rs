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

//! Adds partitions to existing topics with `Admin::create_partitions`. Each
//! topic is followed by the total number of partitions it should have, which
//! must be larger than its current count: Kafka never removes partitions. The
//! broker places the new partitions' replicas.
//!
//! ```text
//! cargo run --example admin_create_partitions -- orders 6 payments 4
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, NewPartitions};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, groups, parse};

const USAGE: &str = "usage: admin_create_partitions [-X key=value]... <topic> <total_partitions>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let args = args.positional()?;
    let mut topics = Vec::new();
    let mut new_partitions = HashMap::new();
    for group in groups(&args, 2, true)? {
        let total: i32 = parse("partition count", &group[1])?;
        topics.push((group[0].clone(), total));
        new_partitions.insert(group[0].clone(), NewPartitions::increase_to(total));
    }

    let result = admin.create_partitions(&new_partitions);
    for (topic, total) in &topics {
        match result.values()[topic].get().await {
            Ok(()) => println!("Topic {topic} now has {total} partitions"),
            Err(error) => fail(format!("Failed to add partitions to topic {topic}"), &error),
        }
    }
    Ok(())
}
