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

//! Renews a delegation token with `Admin::renew_delegation_token_with_options`
//! and prints its new expiry timestamp. The token is named by its HMAC in
//! hexadecimal, as `admin_create_delegation_token` prints it. Without
//! `--period-ms` the broker's `delegation.token.expiry.time.ms` applies; the
//! expiry never passes the token's max timestamp.
//!
//! Only the owner or a renewer may renew a token, over a connection
//! authenticated with SASL (not with a delegation token) or two-way TLS.
//!
//! ```text
//! cargo run --example admin_renew_delegation_token -- -X security.protocol=SASL_PLAINTEXT ... \
//!     --period-ms=3600000 <hmac_hex>
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, RenewDelegationTokenOptions};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, from_hex, parse, usage};

const USAGE: &str = "usage: admin_renew_delegation_token [-X key=value]... [--period-ms=<ms>] <hmac_hex>";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let period_ms = args.value("period-ms")?.map(|text| parse("renew period", &text)).transpose()?;
    let positional = args.positional()?;
    let [hmac] = positional.as_slice() else {
        return usage("expected exactly one HMAC");
    };
    let hmac = from_hex(hmac)?;

    let mut options = RenewDelegationTokenOptions::new();
    if let Some(period_ms) = period_ms {
        options = options.set_renew_time_period_ms(period_ms);
    }
    let result = admin.renew_delegation_token_with_options(&hmac, options);
    let expiry_timestamp = result.expiry_timestamp().get().await?;
    println!("Delegation token renewed, expiry timestamp: {expiry_timestamp}");
    Ok(())
}
