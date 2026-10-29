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

//! Describes where replicas live with `Admin::describe_replica_log_dirs`: for
//! each replica, given as topic, partition and broker id, the log directory
//! that holds it and, while the broker moves it, the directory it moves to.
//!
//! ```text
//! cargo run --example admin_describe_replica_log_dirs -- orders 0 1 orders 1 2
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;
use confluent_kafka::common::TopicPartitionReplica;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, groups, parse};

const USAGE: &str = "usage: admin_describe_replica_log_dirs [-X key=value]... <topic> <partition> <broker_id>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let mut replicas = Vec::new();
    for group in groups(&positional, 3, true)? {
        let replica = TopicPartitionReplica::new(
            group[0].clone(),
            parse("partition", &group[1])?,
            parse("broker id", &group[2])?,
        );
        if !replicas.contains(&replica) {
            replicas.push(replica);
        }
    }

    let result = admin.describe_replica_log_dirs(&replicas);
    for replica in &replicas {
        match result.values()[replica].get().await {
            Ok(info) => {
                let mut line = format!(
                    "Replica {replica}: log dir {}, offset lag {}",
                    info.current_replica_log_dir().unwrap_or("none"),
                    info.current_replica_offset_lag()
                );
                if let Some(future_dir) = info.future_replica_log_dir() {
                    line.push_str(&format!(
                        ", moving to {future_dir}, future offset lag {}",
                        info.future_replica_offset_lag()
                    ));
                }
                println!("{line}");
            },
            Err(error) => fail(format!("Failed to describe replica {replica}"), &error),
        }
    }
    Ok(())
}
