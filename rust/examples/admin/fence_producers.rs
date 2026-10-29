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

//! Fences the producers of transactional ids with `Admin::fence_producers`.
//! The coordinator bumps each id's producer epoch and aborts its open
//! transaction, so a producer still using the id fails with
//! `ProducerFencedError` on its next transactional call. The example prints the
//! producer id and the new epoch.
//!
//! ```text
//! cargo run --example admin_fence_producers -- my-transactional-id
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, usage};

const USAGE: &str = "usage: admin_fence_producers [-X key=value]... <transactional_id>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let transactional_ids = args.positional()?;
    if transactional_ids.is_empty() {
        return usage("no transactional id given");
    }

    let result = admin.fence_producers(&transactional_ids);
    for transactional_id in &transactional_ids {
        let fenced = async {
            let producer_id = result.producer_id(transactional_id)?.get().await?;
            let epoch = result.epoch_id(transactional_id)?.get().await?;
            Ok::<_, confluent_kafka::common::Error>((producer_id, epoch))
        };
        match fenced.await {
            Ok((producer_id, epoch)) => println!("Fenced {transactional_id}: producer id {producer_id}, epoch {epoch}"),
            Err(error) => fail(format!("Failed to fence {transactional_id}"), &error),
        }
    }
    Ok(())
}
