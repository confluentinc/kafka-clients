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

//! Deletes topics by name with `Admin::delete_topics_with_string_slice`. A
//! topic's future completes once the controller has deleted it; brokers may
//! still report it for a short while after.
//!
//! ```text
//! cargo run --example admin_delete_topics -- orders payments
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, usage};

const USAGE: &str = "usage: admin_delete_topics [-X key=value]... <topic>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let topics = args.positional()?;
    if topics.is_empty() {
        return usage("no topic given");
    }

    let result = admin.delete_topics_with_string_slice(&topics);
    // A by-name call keys its futures by topic name.
    let Some(futures) = result.topic_name_values() else {
        return Ok(());
    };
    for topic in &topics {
        match futures[topic].get().await {
            Ok(()) => println!("Topic {topic} deleted"),
            Err(error) => fail(format!("Failed to delete topic {topic}"), &error),
        }
    }
    Ok(())
}
