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

//! Lists the topics in the cluster with `Admin::list_topics_with_options`:
//! each topic's name and id. Internal topics such as `__consumer_offsets` are
//! left out unless `--internal` is given.
//!
//! ```text
//! cargo run --example admin_list_topics -- --internal
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, ListTopicsOptions};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, usage};

const USAGE: &str = "usage: admin_list_topics [-X key=value]... [--internal]";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let list_internal = args.flag("internal")?;
    if !args.positional()?.is_empty() {
        return usage("unexpected argument");
    }

    let result = admin.list_topics_with_options(ListTopicsOptions::new().set_list_internal(list_internal));
    let mut listings = result.listings().get().await?;
    listings.sort_by(|a, b| a.name().cmp(b.name()));
    for listing in &listings {
        println!(
            "Topic {} (id {}){}",
            listing.name(),
            listing.topic_id(),
            if listing.is_internal() { " internal" } else { "" }
        );
    }
    Ok(())
}
