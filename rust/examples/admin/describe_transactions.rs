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

//! Describes transactional ids with `Admin::describe_transactions`: each id's
//! coordinator, state, producer id and epoch, timeout, and the partitions of
//! its current transaction. Each id is asked of its own transaction
//! coordinator; an id the coordinator does not know fails with
//! `TransactionalIdNotFoundError`.
//!
//! ```text
//! cargo run --example admin_describe_transactions -- my-transactional-id
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, usage};

const USAGE: &str = "usage: admin_describe_transactions [-X key=value]... <transactional_id>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let transactional_ids = args.positional()?;
    if transactional_ids.is_empty() {
        return usage("no transactional id given");
    }

    let result = admin.describe_transactions(&transactional_ids);
    for transactional_id in &transactional_ids {
        let description = async { result.description(transactional_id)?.get().await };
        match description.await {
            Ok(description) => {
                println!(
                    "Transaction {transactional_id}: state {}, coordinator {}, producer id {}, producer epoch {}, \
                     timeout {} ms, start time {}",
                    description.state(),
                    description.coordinator_id(),
                    description.producer_id(),
                    description.producer_epoch(),
                    description.transaction_timeout_ms(),
                    description
                        .transaction_start_time_ms()
                        .map_or("none".to_string(), |time| time.to_string())
                );
                let mut partitions: Vec<_> = description.topic_partitions().iter().collect();
                partitions.sort_by(|a, b| (a.topic(), a.partition()).cmp(&(b.topic(), b.partition())));
                for partition in partitions {
                    println!("  partition {partition}");
                }
            },
            Err(error) => fail(format!("Failed to describe transaction {transactional_id}"), &error),
        }
    }
    Ok(())
}
