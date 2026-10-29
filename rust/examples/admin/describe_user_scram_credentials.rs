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

//! Describes SASL/SCRAM credentials with
//! `Admin::describe_user_scram_credentials`: the mechanisms and iteration
//! counts of each user's credentials, never the salted passwords.
//!
//! Without users every user with a credential is described. A given user
//! without one fails with a resource-not-found error.
//!
//! ```text
//! cargo run --example admin_describe_user_scram_credentials -- alice bob
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, UserScramCredentialsDescription};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail};

const USAGE: &str = "usage: admin_describe_user_scram_credentials [-X key=value]... [<user>...]";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn print_description(description: &UserScramCredentialsDescription) {
    let mut credentials: Vec<String> = description
        .credential_infos()
        .iter()
        .map(|info| format!("{} iterations={}", info.mechanism().mechanism_name(), info.iterations()))
        .collect();
    credentials.sort();
    println!("User {}: {}", description.name(), credentials.join(", "));
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let users = args.positional()?;

    if users.is_empty() {
        let descriptions = admin.describe_user_scram_credentials().all().get().await?;
        let mut names: Vec<&String> = descriptions.keys().collect();
        names.sort();
        for name in names {
            print_description(&descriptions[name]);
        }
        return Ok(());
    }
    // One request for all users; each user's description is its own future.
    let result = admin.describe_user_scram_credentials_with_users(&users);
    for user in &users {
        match result.description(user).get().await {
            Ok(description) => print_description(&description),
            Err(error) => fail(format!("Failed to describe SCRAM credentials of user {user}"), &error),
        }
    }
    Ok(())
}
