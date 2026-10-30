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

//! Aborts a hanging transaction on one partition with `Admin::abort_transaction`.
//! The partition leader writes an abort marker for the producer, so consumers
//! reading committed records can move past the transaction and the last stable
//! offset advances. It is a recovery tool for a transaction its coordinator no
//! longer tracks, and needs `CLUSTER_ACTION` permission.
//!
//! Take the producer id, producer epoch and coordinator epoch from the
//! partition's producer with a current transaction start offset in
//! `admin_describe_producers`; a negative coordinator epoch is sent as 0, as
//! Kafka's `kafka-transactions.sh` does.
//!
//! ```text
//! cargo run --example admin_abort_transaction -- orders 0 4001 0 5
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{AbortTransactionSpec, Admin};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, parse, topic_partition, usage};

const USAGE: &str = "usage: admin_abort_transaction [-X key=value]... <topic> <partition> <producer_id> \
                     <producer_epoch> <coordinator_epoch>";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    if positional.len() != 5 {
        return usage("expected a topic, a partition, a producer id, a producer epoch and a coordinator epoch");
    }
    let partition = topic_partition(&positional)?;
    let producer_id: i64 = parse("producer id", &positional[2])?;
    let producer_epoch: i16 = parse("producer epoch", &positional[3])?;
    let coordinator_epoch: i32 = parse("coordinator epoch", &positional[4])?;

    let spec = AbortTransactionSpec::new(partition.clone(), producer_id, producer_epoch, coordinator_epoch.max(0));
    admin.abort_transaction(spec).all().get().await?;
    println!("Aborted transaction of producer id {producer_id}, producer epoch {producer_epoch} on {partition}");
    Ok(())
}
