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

//! Describes delegation tokens with
//! `Admin::describe_delegation_token_with_options`. With owner principals
//! (`<type>:<name>`, for example `User:alice`) only their tokens are returned,
//! otherwise every token the caller may describe: its own, those it renews and
//! those it has `DESCRIBE` permission on.
//!
//! A token's HMAC is its password: with the token id it authenticates as the
//! owner. It is printed only with `--show-hmac`.
//!
//! The broker must have delegation tokens enabled and the connection must be
//! authenticated with SASL (not with a delegation token) or two-way TLS.
//!
//! ```text
//! cargo run --example admin_describe_delegation_token -- -X security.protocol=SASL_PLAINTEXT ... User:alice
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, DescribeDelegationTokenOptions};
use confluent_kafka::common::security::auth::KafkaPrincipal;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, to_hex, usage};

const USAGE: &str = "usage: admin_describe_delegation_token [-X key=value]... [--show-hmac] [<type>:<name>]...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn principal(text: &str) -> Result<KafkaPrincipal, Failure> {
    match text.split_once(':') {
        Some((principal_type, name)) if !principal_type.is_empty() && !name.is_empty() => {
            Ok(KafkaPrincipal::new(principal_type, name))
        },
        _ => usage(format!("invalid principal {text:?}, expected <type>:<name>")),
    }
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let show_hmac = args.flag("show-hmac")?;
    let owners = args
        .positional()?
        .iter()
        .map(|text| principal(text))
        .collect::<Result<Vec<_>, _>>()?;

    // No owner means every token.
    let owners = if owners.is_empty() { None } else { Some(owners) };
    let result = admin.describe_delegation_token_with_options(DescribeDelegationTokenOptions::new().set_owners(owners));
    let mut tokens = result.delegation_tokens().get().await?;
    if tokens.is_empty() {
        println!("No delegation tokens");
    }
    tokens.sort_by(|a, b| a.token_info().token_id().cmp(b.token_info().token_id()));
    for token in &tokens {
        let info = token.token_info();
        println!(
            "Token {}: owner {}, requester {}, renewers [{}], issued {}, expires {}, max {}",
            info.token_id(),
            info.owner_as_string(),
            info.token_requester_as_string(),
            info.renewers_as_string().join(","),
            info.issue_timestamp(),
            info.expiry_timestamp(),
            info.max_timestamp()
        );
        if show_hmac {
            println!("  HMAC (hex): {}", to_hex(token.hmac()));
        }
    }
    Ok(())
}
