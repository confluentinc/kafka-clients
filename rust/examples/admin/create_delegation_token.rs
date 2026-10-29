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

//! Creates a delegation token with `Admin::create_delegation_token_with_options`
//! and prints its information and HMAC. The HMAC, in hexadecimal, is what
//! `admin_renew_delegation_token` and `admin_expire_delegation_token` take.
//! It is the token's password, so the output is a credential.
//!
//! Principals are written `<type>:<name>`, for example `User:alice`. Without
//! `--owner` the token belongs to the authenticated caller; without
//! `--max-lifetime-ms` the broker's `delegation.token.max.lifetime.ms` applies.
//!
//! The broker must have delegation tokens enabled
//! (`delegation.token.secret.key`) and the connection must be authenticated
//! with SASL (not with a delegation token) or two-way TLS.
//!
//! ```text
//! cargo run --example admin_create_delegation_token -- \
//!     -X security.protocol=SASL_PLAINTEXT -X sasl.mechanism=SCRAM-SHA-256 \
//!     -X 'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="secret";' \
//!     --renewer=User:bob
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, CreateDelegationTokenOptions};
use confluent_kafka::common::security::auth::KafkaPrincipal;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, parse, to_hex, usage};

const USAGE: &str = "usage: admin_create_delegation_token [-X key=value]... [--renewer=<type>:<name>]... \
                     [--owner=<type>:<name>] [--max-lifetime-ms=<ms>]";

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
    let renewers = args
        .values("renewer")?
        .iter()
        .map(|text| principal(text))
        .collect::<Result<Vec<_>, _>>()?;
    let owner = args.value("owner")?.map(|text| principal(&text)).transpose()?;
    let max_lifetime_ms = args
        .value("max-lifetime-ms")?
        .map(|text| parse("max lifetime", &text))
        .transpose()?;
    if !args.positional()?.is_empty() {
        return usage("unexpected positional argument");
    }

    let mut options = CreateDelegationTokenOptions::new().set_renewers(renewers).set_owner(owner);
    if let Some(max_lifetime_ms) = max_lifetime_ms {
        options = options.set_max_lifetime_ms(max_lifetime_ms);
    }
    let result = admin.create_delegation_token_with_options(options);
    let token = result.delegation_token().get().await?;
    let info = token.token_info();
    println!("Created delegation token {}", info.token_id());
    println!("Owner: {}", info.owner_as_string());
    println!("Requester: {}", info.token_requester_as_string());
    println!("Renewers: {}", info.renewers_as_string().join(","));
    println!("Issue timestamp: {}", info.issue_timestamp());
    println!("Expiry timestamp: {}", info.expiry_timestamp());
    println!("Max timestamp: {}", info.max_timestamp());
    // The HMAC is the token's password: with the token id it authenticates as
    // the owner. This is the only time it is shown without asking for it, so
    // keep it as you would a password.
    println!("HMAC (base64): {}", token.hmac_as_base64_string());
    println!("HMAC (hex): {}", to_hex(token.hmac()));
    Ok(())
}
