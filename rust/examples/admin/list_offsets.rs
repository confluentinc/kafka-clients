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

//! Looks up partition offsets with `Admin::list_offsets_with_options`. Each
//! partition is given as topic and partition followed by what to look up:
//! `earliest`, `latest`, `max_timestamp` (the record with the highest
//! timestamp), `earliest_local` and `latest_tiered` (tiered storage),
//! `earliest_pending_upload`, or `timestamp:<ms>` (the first record at or
//! after that time). With `read_committed` the latest offset stops at the last
//! stable offset, before any open transaction.
//!
//! ```text
//! cargo run --example admin_list_offsets -- read_uncommitted orders 0 earliest orders 0 timestamp:1700000000000
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, ListOffsetsOptions, OffsetSpec};
use confluent_kafka::common::IsolationLevel;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, choose, fail, groups, parse, topic_partition, usage};

const USAGE: &str = "usage: admin_list_offsets [-X key=value]... <read_committed|read_uncommitted> \
     <topic> <partition> <earliest|latest|max_timestamp|earliest_local|latest_tiered|earliest_pending_upload|timestamp:ms>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn offset_spec(text: &str) -> Result<OffsetSpec, Failure> {
    if let Some(timestamp) = text.strip_prefix("timestamp:") {
        return Ok(OffsetSpec::for_timestamp(parse("timestamp", timestamp)?));
    }
    choose(
        "offset spec",
        text,
        &[
            ("earliest", OffsetSpec::earliest()),
            ("latest", OffsetSpec::latest()),
            ("max_timestamp", OffsetSpec::max_timestamp()),
            ("earliest_local", OffsetSpec::earliest_local()),
            ("latest_tiered", OffsetSpec::latest_tiered()),
            ("earliest_pending_upload", OffsetSpec::earliest_pending_upload()),
        ],
    )
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let Some((isolation_level, specs)) = positional.split_first() else {
        return usage("no isolation level given");
    };
    let isolation_level = choose(
        "isolation level",
        isolation_level,
        &[
            ("read_committed", IsolationLevel::ReadCommitted),
            ("read_uncommitted", IsolationLevel::ReadUncommitted),
        ],
    )?;
    let mut partitions = Vec::new();
    let mut offset_specs = HashMap::new();
    for group in groups(specs, 3, true)? {
        let partition = topic_partition(group)?;
        // One lookup per partition: a later spec for the same partition wins.
        if offset_specs.insert(partition.clone(), offset_spec(&group[2])?).is_none() {
            partitions.push(partition);
        }
    }

    let result =
        admin.list_offsets_with_options(&offset_specs, ListOffsetsOptions::with_isolation_level(isolation_level));
    for partition in &partitions {
        match result.partition_result(partition)?.get().await {
            Ok(info) => println!(
                "{partition}: offset {}, timestamp {}, leader epoch {}",
                info.offset(),
                info.timestamp(),
                info.leader_epoch().map_or("none".to_string(), |epoch| epoch.to_string())
            ),
            Err(error) => fail(format!("Failed to list offsets of {partition}"), &error),
        }
    }
    Ok(())
}
