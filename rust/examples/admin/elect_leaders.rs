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

//! Elects partition leaders with `Admin::elect_leaders`. A preferred election
//! moves leadership back to the first replica of each partition; an unclean
//! election picks a leader among out-of-sync replicas when no in-sync replica
//! is alive, which may lose records. Without partitions it runs on every
//! partition of the cluster.
//!
//! A partition whose preferred replica already leads fails with
//! `ElectionNotNeededError`, which only means there was nothing to do; when no
//! partition is given, the broker leaves such partitions out of the result.
//!
//! ```text
//! cargo run --example admin_elect_leaders -- preferred orders 0 orders 1
//! ```

use std::collections::HashSet;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;
use confluent_kafka::common::ElectionType;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, choose, fail, topic_partitions, usage};

const USAGE: &str = "usage: admin_elect_leaders [-X key=value]... <preferred|unclean> [<topic> <partition>]...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let Some((election_type, partitions)) = positional.split_first() else {
        return usage("no election type given");
    };
    let election_type = choose(
        "election type",
        election_type,
        &[
            ("preferred", ElectionType::Preferred),
            ("unclean", ElectionType::Unclean),
        ],
    )?;
    let partitions = topic_partitions(partitions, false)?;
    let partitions = (!partitions.is_empty()).then(|| partitions.into_iter().collect::<HashSet<_>>());

    let result = admin.elect_leaders(election_type, partitions);
    let mut outcomes: Vec<_> = result.partitions().get().await?.into_iter().collect();
    // Asked for every partition, the broker leaves out those already led by their preferred replica.
    if outcomes.is_empty() {
        println!("No leader election needed");
        return Ok(());
    }
    outcomes.sort_by(|(left, _), (right, _)| (left.topic(), left.partition()).cmp(&(right.topic(), right.partition())));
    for (partition, error) in &outcomes {
        match error {
            None => println!("Leader elected for {partition}"),
            Some(error) => fail(format!("Failed to elect leader for {partition}"), error),
        }
    }
    Ok(())
}
