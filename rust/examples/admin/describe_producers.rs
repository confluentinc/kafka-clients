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

//! Describes the active idempotent and transactional producers of partitions
//! with `Admin::describe_producers_with_options`. The partition leader answers
//! unless `--broker` names another replica.
//!
//! A producer with a current transaction start offset has a transaction open on
//! the partition. Its producer id, producer epoch and coordinator epoch are
//! what `admin_abort_transaction` takes to abort a hanging transaction; an
//! absent coordinator epoch or start offset prints as -1.
//!
//! ```text
//! cargo run --example admin_describe_producers -- orders 0 orders 1
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, DescribeProducersOptions};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, parse, topic_partitions};

const USAGE: &str = "usage: admin_describe_producers [-X key=value]... [--broker=<id>] (<topic> <partition>)...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let broker_id = args.value("broker")?.map(|text| parse("broker id", &text)).transpose()?;
    let partitions = topic_partitions(&args.positional()?, true)?;

    let mut options = DescribeProducersOptions::new();
    if let Some(broker_id) = broker_id {
        options = options.set_broker_id(broker_id);
    }
    let result = admin.describe_producers_with_options(&partitions, options);
    for partition in &partitions {
        let state = async { result.partition_result(partition)?.get().await };
        match state.await {
            Ok(state) => {
                let producers = state.active_producers();
                println!("Partition {partition}: {} active producers", producers.len());
                for producer in producers {
                    println!(
                        "  producer id {}, producer epoch {}, coordinator epoch {}, last sequence {}, last timestamp \
                         {}, current transaction start offset {}",
                        producer.producer_id(),
                        producer.producer_epoch(),
                        producer.coordinator_epoch().unwrap_or(-1),
                        producer.last_sequence(),
                        producer.last_timestamp(),
                        producer.current_transaction_start_offset().unwrap_or(-1)
                    );
                }
            },
            Err(error) => fail(format!("Failed to describe producers of {partition}"), &error),
        }
    }
    Ok(())
}
