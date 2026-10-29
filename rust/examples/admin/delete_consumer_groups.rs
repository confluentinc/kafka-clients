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

//! Deletes consumer groups, with their committed offsets, with
//! `Admin::delete_consumer_groups`. Only a group without active members can be
//! deleted.
//!
//! ```text
//! cargo run --example admin_delete_consumer_groups -- orders-app payments-app
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, usage};

const USAGE: &str = "usage: admin_delete_consumer_groups [-X key=value]... <group>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let groups = args.positional()?;
    if groups.is_empty() {
        return usage("no group given");
    }

    let futures = admin.delete_consumer_groups(&groups).deleted_groups();
    for group in &groups {
        match futures[group].get().await {
            Ok(()) => println!("Group {group} deleted"),
            Err(error) => fail(format!("Failed to delete group {group}"), &error),
        }
    }
    Ok(())
}
