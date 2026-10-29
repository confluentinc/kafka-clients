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

//! Force-terminates the transaction of a transactional id with
//! `Admin::force_terminate_transaction` (KIP-939). Like `admin_fence_producers`
//! it bumps the producer epoch and aborts any ongoing transaction, but it
//! targets one id and also aborts a prepared two-phase-commit transaction,
//! which a producer with `transaction.two.phase.commit.enable` leaves open on
//! purpose until its external coordinator decides.
//!
//! ```text
//! cargo run --example admin_force_terminate_transaction -- my-transactional-id
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, usage};

const USAGE: &str = "usage: admin_force_terminate_transaction [-X key=value]... <transactional_id>";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let [transactional_id] = positional.as_slice() else {
        return usage("expected exactly one transactional id");
    };

    admin.force_terminate_transaction(transactional_id).result().get().await?;
    println!("Transaction {transactional_id} terminated");
    Ok(())
}
