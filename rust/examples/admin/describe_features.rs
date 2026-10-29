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

//! Describes the cluster's features with `Admin::describe_features_with_options`:
//! the finalized version levels, which the cluster currently runs, and the
//! version ranges the answering node supports. With `--node-id` that node
//! answers, otherwise any broker does.
//!
//! ```text
//! cargo run --example admin_describe_features
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, DescribeFeaturesOptions};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, parse, usage};

const USAGE: &str = "usage: admin_describe_features [-X key=value]... [--node-id=<id>]";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let node_id = args.value("node-id")?.map(|text| parse("node id", &text)).transpose()?;
    if !args.positional()?.is_empty() {
        return usage("unexpected positional argument");
    }

    let mut options = DescribeFeaturesOptions::new();
    if let Some(node_id) = node_id {
        options = options.set_node_id(node_id);
    }
    let metadata = admin.describe_features_with_options(options).feature_metadata().get().await?;

    match metadata.finalized_features_epoch() {
        Some(epoch) => println!("Finalized features epoch: {epoch}"),
        None => println!("Finalized features epoch: none"),
    }
    let mut finalized: Vec<_> = metadata.finalized_features().iter().collect();
    finalized.sort_by_key(|(name, _)| *name);
    for (name, range) in finalized {
        println!(
            "Finalized feature {name}: min level {}, max level {}",
            range.min_version_level(),
            range.max_version_level()
        );
    }
    let mut supported: Vec<_> = metadata.supported_features().iter().collect();
    supported.sort_by_key(|(name, _)| *name);
    for (name, range) in supported {
        println!(
            "Supported feature {name}: min version {}, max version {}",
            range.min_version(),
            range.max_version()
        );
    }
    Ok(())
}
