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

//! Creates, updates and deletes SASL/SCRAM credentials with
//! `Admin::alter_user_scram_credentials`.
//!
//! The arguments are a sequence of alterations:
//! `UPSERT <user> <mechanism> <iterations> <password> [<salt>]` sets a
//! credential, salting the password with a random salt unless one is given,
//! and `DELETE <user> <mechanism>` removes one. The mechanism is
//! `SCRAM-SHA-256` or `SCRAM-SHA-512`. A KRaft controller accepts only one
//! alteration per user in a request: a user with several fails with a
//! duplicate-resource error. The broker needs no SCRAM listener to store
//! credentials, only a KRaft metadata version that supports them.
//!
//! ```text
//! cargo run --example admin_alter_user_scram_credentials -- \
//!     UPSERT alice SCRAM-SHA-256 8192 secret DELETE bob SCRAM-SHA-512
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{
    Admin, ScramCredentialInfo, ScramMechanism, UserScramCredentialAlteration, UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, usage};

const USAGE: &str = "usage: admin_alter_user_scram_credentials [-X key=value]... \
                     (UPSERT <user> <mechanism> <iterations> <password> [<salt>] | DELETE <user> <mechanism>)...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

// The errors below never repeat an argument: with an argument missing, the one
// in its place may be a password.

fn mechanism(text: &str) -> Result<ScramMechanism, Failure> {
    match ScramMechanism::from_mechanism_name(&text.to_ascii_uppercase()) {
        ScramMechanism::Unknown => usage("invalid mechanism, expected SCRAM-SHA-256 or SCRAM-SHA-512"),
        mechanism => Ok(mechanism),
    }
}

fn iterations(text: &str) -> Result<i32, Failure> {
    text.parse().or_else(|_| usage("invalid iterations, expected a number"))
}

/// Takes the next `count` arguments of the alteration `keyword`.
fn take<'a>(args: &mut &'a [String], keyword: &str, count: usize) -> Result<&'a [String], Failure> {
    if args.len() < count {
        return usage(format!("{keyword} needs {count} arguments"));
    }
    let (taken, rest) = args.split_at(count);
    *args = rest;
    Ok(taken)
}

fn alterations(mut args: &[String]) -> Result<Vec<UserScramCredentialAlteration>, Failure> {
    let mut alterations = Vec::new();
    while let Some((keyword, rest)) = args.split_first() {
        args = rest;
        match keyword.as_str() {
            "UPSERT" => {
                let fields = take(&mut args, "UPSERT", 4)?;
                let info = ScramCredentialInfo::new(mechanism(&fields[1])?, iterations(&fields[2])?);
                // The salt is optional: the next argument is one unless it starts
                // another alteration.
                let salt = match args.split_first() {
                    Some((salt, rest)) if salt != "UPSERT" && salt != "DELETE" => {
                        args = rest;
                        Some(salt)
                    },
                    _ => None,
                };
                let upsertion = match salt {
                    Some(salt) => UserScramCredentialUpsertion::with_salt(
                        fields[0].as_str(),
                        info,
                        fields[3].as_bytes().to_vec(),
                        salt.as_bytes().to_vec(),
                    ),
                    None => UserScramCredentialUpsertion::with_str(fields[0].as_str(), info, &fields[3]),
                };
                alterations.push(UserScramCredentialAlteration::Upsertion(upsertion));
            },
            "DELETE" => {
                let fields = take(&mut args, "DELETE", 2)?;
                alterations.push(UserScramCredentialAlteration::Deletion(UserScramCredentialDeletion::new(
                    fields[0].as_str(),
                    mechanism(&fields[1])?,
                )));
            },
            _ => return usage("expected UPSERT or DELETE"),
        }
    }
    if alterations.is_empty() {
        return usage("no alteration given");
    }
    Ok(alterations)
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let args = args.positional()?;
    let alterations = alterations(&args)?;

    // The result has one future per user, covering all of that user's alterations.
    let mut users: Vec<&str> = Vec::new();
    for alteration in &alterations {
        if !users.contains(&alteration.user()) {
            users.push(alteration.user());
        }
    }
    let result = admin.alter_user_scram_credentials(&alterations);
    for user in users {
        match result.values()[user].get().await {
            Ok(()) => println!("Altered SCRAM credentials of user {user}"),
            Err(error) => fail(format!("Failed to alter SCRAM credentials of user {user}"), &error),
        }
    }
    Ok(())
}
