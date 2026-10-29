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

//! Creates topics with `Admin::create_topics`.
//!
//! Each topic takes an optional partition count and replication factor; a
//! missing one uses the broker's default (`num.partitions`,
//! `default.replication.factor`). With `--validate-only` the broker checks the
//! request without creating anything.
//!
//! ```text
//! cargo run --example admin_create_topics -- orders:3:1 payments
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, CreateTopicsOptions, NewTopic};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, describe, fail, parse, usage};

const USAGE: &str =
    "usage: admin_create_topics [-X key=value]... [--validate-only] <topic>[:partitions[:replication_factor]]...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let validate_only = args.flag("validate-only")?;
    let specs = args.positional()?;
    if specs.is_empty() {
        return usage("no topic given");
    }

    let mut new_topics = Vec::new();
    for spec in &specs {
        let mut parts = spec.split(':');
        let name = parts.next().unwrap_or_default();
        let num_partitions = parts.next().map(|text| parse("partition count", text)).transpose()?;
        let replication_factor = parts.next().map(|text| parse("replication factor", text)).transpose()?;
        if name.is_empty() || parts.next().is_some() {
            return usage(format!("invalid topic spec {spec:?}"));
        }
        new_topics.push(NewTopic::with_num_partitions_replication_factor(
            name,
            num_partitions,
            replication_factor,
        ));
    }

    // The call returns at once; each topic's outcome arrives on its own future.
    let result =
        admin.create_topics_with_options(&new_topics, CreateTopicsOptions::new().set_validate_only(validate_only));
    let verb = if validate_only { "validated" } else { "created" };
    for new_topic in &new_topics {
        let name = new_topic.name();
        if let Err(error) = result.values()[name].get().await {
            fail(format!("Failed to create topic {name}"), &error);
            continue;
        }
        // The broker also returns what it created: these refinements of the
        // topic's future fail on their own when it withholds them, for example
        // from a caller without `DESCRIBE_CONFIGS` on the topic, which is
        // still created.
        let details = async {
            let topic_id = result.topic_id(name).get().await?;
            let num_partitions = result.num_partitions(name).get().await?;
            let replication_factor = result.replication_factor(name).get().await?;
            Ok::<_, confluent_kafka::common::Error>((topic_id, num_partitions, replication_factor))
        };
        match details.await {
            Ok((topic_id, num_partitions, replication_factor)) => println!(
                "Topic {name} {verb}: id {topic_id}, {num_partitions} partitions, replication factor {replication_factor}"
            ),
            Err(error) => println!("Topic {name} {verb}; its details were not returned: {}", describe(&error)),
        }
    }
    Ok(())
}
