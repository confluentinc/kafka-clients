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

//! Lists the groups known to the cluster with `Admin::list_groups_with_options`.
//! `--states` keeps only the groups in the given states and `--types` only the
//! groups of the given types. The call asks every broker; a broker that fails
//! is reported and the listings of the others are still printed.
//!
//! ```text
//! cargo run --example admin_list_groups -- --states=stable,empty --types=consumer,classic
//! ```

use std::collections::HashSet;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, ListGroupsOptions};
use confluent_kafka::common::{GroupState, GroupType};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, choose, comma_list, fail, usage};

const USAGE: &str = "usage: admin_list_groups [-X key=value]... [--states=state,...] [--types=type,...]";

const STATES: &[(&str, GroupState)] = &[
    ("preparing_rebalance", GroupState::PreparingRebalance),
    ("completing_rebalance", GroupState::CompletingRebalance),
    ("stable", GroupState::Stable),
    ("dead", GroupState::Dead),
    ("empty", GroupState::Empty),
    ("assigning", GroupState::Assigning),
    ("reconciling", GroupState::Reconciling),
    ("not_ready", GroupState::NotReady),
    ("unknown", GroupState::Unknown),
];

const TYPES: &[(&str, GroupType)] = &[
    ("consumer", GroupType::Consumer),
    ("classic", GroupType::Classic),
    ("share", GroupType::Share),
    ("streams", GroupType::Streams),
];

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let states = args.value("states")?.unwrap_or_default();
    let types = args.value("types")?.unwrap_or_default();
    if !args.positional()?.is_empty() {
        return usage("unexpected positional argument");
    }
    let states = comma_list(&states)
        .iter()
        .map(|state| choose("group state", state, STATES))
        .collect::<Result<HashSet<_>, _>>()?;
    let types = comma_list(&types)
        .iter()
        .map(|group_type| choose("group type", group_type, TYPES))
        .collect::<Result<HashSet<_>, _>>()?;

    let result = admin.list_groups_with_options(ListGroupsOptions::new().in_group_states(states).with_types(types));
    // `valid` never fails: a broker's failure is in `errors` instead.
    let mut listings = result.valid().get().await?;
    listings.sort_by(|a, b| a.group_id().cmp(b.group_id()));
    for listing in &listings {
        println!(
            "Group {}: type {}, state {}, protocol {:?}, simple {}",
            listing.group_id(),
            listing.r#type().map_or("none".to_string(), |group_type| group_type.to_string()),
            listing.group_state().map_or("none".to_string(), |state| state.to_string()),
            listing.protocol(),
            listing.is_simple_consumer_group()
        );
    }
    for error in result.errors().get().await? {
        fail("Failed to list groups", &error);
    }
    Ok(())
}
