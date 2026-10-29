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

//! Lists a group's committed offsets with
//! `Admin::list_consumer_group_offsets_with_group_specs_options`. Without
//! partitions it lists every partition the group has committed an offset for;
//! with partitions it lists those, reporting the ones without a committed
//! offset. With `--require-stable` the broker waits for pending transactional
//! offsets to be committed or aborted instead of leaving them out.
//!
//! ```text
//! cargo run --example admin_list_consumer_group_offsets -- orders-app orders 0 orders 1
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsSpec};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, topic_partitions, usage};

const USAGE: &str =
    "usage: admin_list_consumer_group_offsets [-X key=value]... [--require-stable] <group> [<topic> <partition>]...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let require_stable = args.flag("require-stable")?;
    let positional = args.positional()?;
    let Some((group, rest)) = positional.split_first() else {
        return usage("no group given");
    };
    let partitions = topic_partitions(rest, false)?;

    // No partitions means every partition with a committed offset.
    let spec = ListConsumerGroupOffsetsSpec::new().set_topic_partitions(if partitions.is_empty() {
        None
    } else {
        Some(partitions)
    });
    let result = admin.list_consumer_group_offsets_with_group_specs_options(
        &HashMap::from([(group.clone(), spec)]),
        ListConsumerGroupOffsetsOptions::new().set_require_stable(require_stable),
    );
    let offsets = result.partitions_to_offset_and_metadata_with_group_id(group)?.get().await?;
    let mut offsets: Vec<_> = offsets.into_iter().collect();
    offsets.sort_by(|(a, _), (b, _)| (a.topic(), a.partition()).cmp(&(b.topic(), b.partition())));
    println!("Group {group}: {} partitions", offsets.len());
    for (partition, offset) in &offsets {
        match offset {
            Some(offset) => println!(
                "  {partition}: offset {}, leader epoch {}, metadata {:?}",
                offset.offset(),
                offset.leader_epoch().map_or("none".to_string(), |epoch| epoch.to_string()),
                offset.metadata()
            ),
            None => println!("  {partition}: no committed offset"),
        }
    }
    Ok(())
}
