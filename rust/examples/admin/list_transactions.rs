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

//! Lists the transactions the transaction coordinators know about with
//! `Admin::list_transactions_with_options`. Every broker is asked, so on a
//! large cluster filter the result: by state (`--states`, Java's state names
//! such as `Ongoing` or `PrepareCommit`, in any case), by producer id, by
//! transactional id regular expression, or with `--duration-ms` to keep only
//! transactions running for longer than that.
//!
//! ```text
//! cargo run --example admin_list_transactions -- --states=Ongoing --duration-ms=60000
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, ListTransactionsOptions, TransactionState};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, choose, comma_list, parse, usage};

const USAGE: &str = "usage: admin_list_transactions [-X key=value]... [--states=<state>,...] \
                     [--producer-ids=<id>,...] [--duration-ms=<ms>] [--pattern=<regex>]";

const STATES: &[(&str, TransactionState)] = &[
    ("ongoing", TransactionState::Ongoing),
    ("prepareabort", TransactionState::PrepareAbort),
    ("preparecommit", TransactionState::PrepareCommit),
    ("completeabort", TransactionState::CompleteAbort),
    ("completecommit", TransactionState::CompleteCommit),
    ("empty", TransactionState::Empty),
    ("prepareepochfence", TransactionState::PrepareEpochFence),
];

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let states = args.value("states")?;
    let producer_ids = args.value("producer-ids")?;
    let duration_ms = args.value("duration-ms")?.map(|text| parse("duration", &text)).transpose()?;
    let pattern = args.value("pattern")?;
    if !args.positional()?.is_empty() {
        return usage("unexpected positional argument");
    }

    let mut options = ListTransactionsOptions::new();
    if let Some(states) = states {
        let states = comma_list(&states)
            .iter()
            .map(|state| choose("state", state, STATES))
            .collect::<Result<Vec<_>, _>>()?;
        options = options.filter_states(states);
    }
    if let Some(producer_ids) = producer_ids {
        let producer_ids = comma_list(&producer_ids)
            .iter()
            .map(|id| parse("producer id", id))
            .collect::<Result<Vec<i64>, _>>()?;
        options = options.filter_producer_ids(producer_ids);
    }
    if let Some(duration_ms) = duration_ms {
        options = options.filter_on_duration(duration_ms);
    }
    if pattern.is_some() {
        options = options.filter_on_transactional_id_pattern(pattern);
    }

    let mut listings = admin.list_transactions_with_options(options).all().get().await?;
    if listings.is_empty() {
        println!("No transactions");
    }
    listings.sort_by(|a, b| a.transactional_id().cmp(b.transactional_id()));
    for listing in &listings {
        println!(
            "Transaction {}: producer id {}, state {}",
            listing.transactional_id(),
            listing.producer_id(),
            listing.state()
        );
    }
    Ok(())
}
