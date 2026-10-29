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

//! Moves replicas between the log directories of their broker with
//! `Admin::alter_replica_log_dirs`. Each replica is given as topic, partition
//! and broker id, followed by the absolute path of the target directory, which
//! must be one of the broker's `log.dirs`. The broker copies the replica in the
//! background; `admin_describe_replica_log_dirs` shows the progress.
//!
//! Needs a broker configured with several `log.dirs`.
//!
//! ```text
//! cargo run --example admin_alter_replica_log_dirs -- orders 0 1 /var/lib/kafka/data-2
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;
use confluent_kafka::common::TopicPartitionReplica;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, groups, parse};

const USAGE: &str =
    "usage: admin_alter_replica_log_dirs [-X key=value]... <topic> <partition> <broker_id> <log_dir>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let mut replicas = Vec::new();
    let mut assignment = HashMap::new();
    for group in groups(&positional, 4, true)? {
        let replica = TopicPartitionReplica::new(
            group[0].clone(),
            parse("partition", &group[1])?,
            parse("broker id", &group[2])?,
        );
        if assignment.insert(replica.clone(), group[3].clone()).is_none() {
            replicas.push(replica);
        }
    }

    let result = admin.alter_replica_log_dirs(&assignment);
    for replica in &replicas {
        let log_dir = &assignment[replica];
        match result.values()[replica].get().await {
            Ok(()) => println!("Replica {replica} moving to {log_dir}"),
            Err(error) => fail(format!("Failed to move replica {replica} to {log_dir}"), &error),
        }
    }
    Ok(())
}
