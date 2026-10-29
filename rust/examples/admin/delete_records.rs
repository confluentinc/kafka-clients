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

//! Deletes the records of partitions up to an offset with
//! `Admin::delete_records`. Every record before the given offset is removed
//! and the partition's low watermark moves to it; offset `-1` deletes up to
//! the high watermark, that is every record currently in the partition.
//!
//! ```text
//! cargo run --example admin_delete_records -- orders 0 100 orders 1 -1
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, RecordsToDelete};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, groups, parse, topic_partition};

const USAGE: &str = "usage: admin_delete_records [-X key=value]... <topic> <partition> <offset>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let args = args.positional()?;
    let mut partitions = Vec::new();
    let mut records_to_delete = HashMap::new();
    for group in groups(&args, 3, true)? {
        let partition = topic_partition(group)?;
        let offset = parse("offset", &group[2])?;
        partitions.push(partition.clone());
        records_to_delete.insert(partition, RecordsToDelete::with_offset(offset));
    }

    let result = admin.delete_records(&records_to_delete);
    for partition in &partitions {
        match result.low_watermarks()[partition].get().await {
            Ok(deleted) => println!(
                "Deleted records of partition {partition}: low watermark {}",
                deleted.low_watermark()
            ),
            Err(error) => fail(format!("Failed to delete records of partition {partition}"), &error),
        }
    }
    Ok(())
}
