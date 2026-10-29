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

//! Removes members from a classic consumer group with
//! `Admin::remove_members_from_consumer_group_with_options`. Given group
//! instance ids, it removes those static members; given none, it removes every
//! member of the group, failing when the group has none. `--reason` is sent to
//! the broker with the request.
//!
//! ```text
//! cargo run --example admin_remove_members_from_consumer_group -- --reason=maintenance orders-app instance-1
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, MemberToRemove, RemoveMembersFromConsumerGroupOptions};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, usage};

const USAGE: &str =
    "usage: admin_remove_members_from_consumer_group [-X key=value]... [--reason=text] <group> [<instance_id>]...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let reason = args.value("reason")?;
    let positional = args.positional()?;
    let Some((group, instance_ids)) = positional.split_first() else {
        return usage("no group given");
    };

    let members: Vec<MemberToRemove> = instance_ids.iter().map(MemberToRemove::new).collect();
    let mut options = if members.is_empty() {
        RemoveMembersFromConsumerGroupOptions::new()
    } else {
        RemoveMembersFromConsumerGroupOptions::with_members(members.clone())?
    };
    options.set_reason(reason);

    let result = admin.remove_members_from_consumer_group_with_options(group, options);
    if members.is_empty() {
        match result.all().get().await {
            Ok(()) => println!("Group {group}: all members removed"),
            Err(error) => fail(format!("Failed to remove all members from group {group}"), &error),
        }
        return Ok(());
    }
    for member in &members {
        let instance_id = member.group_instance_id();
        let removed = async { result.member_result(member)?.get().await };
        match removed.await {
            Ok(()) => println!("Group {group}: member {instance_id} removed"),
            Err(error) => fail(format!("Failed to remove member {instance_id} from group {group}"), &error),
        }
    }
    Ok(())
}
