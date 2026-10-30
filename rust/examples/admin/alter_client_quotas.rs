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

//! Alters the client quotas of one entity with `Admin::alter_client_quotas`.
//!
//! The entity is `type=name[,type=name]`, the entity type being `user`,
//! `client-id` or `ip` and the name `DEFAULT` naming the default entity of that
//! type. Each following `key=value` sets a quota (for example
//! `producer_byte_rate=1048576`) and `key=REMOVE` removes it. With
//! `--validate-only` the broker checks the alteration without applying it.
//!
//! ```text
//! cargo run --example admin_alter_client_quotas -- user=alice,client-id=app \
//!     producer_byte_rate=1048576 request_percentage=REMOVE
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, AlterClientQuotasOptions};
use confluent_kafka::common::quota::{ClientQuotaAlteration, ClientQuotaEntity, Op};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, comma_list, fail, parse, usage};

const USAGE: &str = "usage: admin_alter_client_quotas [-X key=value]... [--validate-only] \
                     <entity_type>=<name|DEFAULT>[,<entity_type>=<name|DEFAULT>]... <key>=<value|REMOVE>...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

/// The entity as `type=name[,type=name]`, sorted by type; `<default>` names
/// the default entity of a type.
fn entity_text(entity: &ClientQuotaEntity) -> String {
    let mut entries: Vec<String> = entity
        .entries()
        .iter()
        .map(|(entity_type, name)| format!("{entity_type}={}", name.as_deref().unwrap_or("<default>")))
        .collect();
    entries.sort();
    entries.join(",")
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let validate_only = args.flag("validate-only")?;
    let args = args.positional()?;
    let Some((entity_spec, op_specs)) = args.split_first() else {
        return usage("no entity given");
    };
    if op_specs.is_empty() {
        return usage("no quota given");
    }

    let mut entries = HashMap::new();
    for entry in comma_list(entity_spec) {
        let Some((entity_type, name)) = entry.split_once('=') else {
            return usage(format!("invalid entity {entry:?}: expected type=name"));
        };
        entries.insert(entity_type.to_string(), (name != "DEFAULT").then(|| name.to_string()));
    }
    let entity = ClientQuotaEntity::new(entries);
    let mut ops = Vec::new();
    for spec in op_specs {
        let Some((key, value)) = spec.split_once('=') else {
            return usage(format!("invalid quota {spec:?}: expected key=value"));
        };
        // A missing value removes the quota.
        let value = if value == "REMOVE" {
            None
        } else {
            Some(parse("quota value", value)?)
        };
        ops.push(Op::new(key, value));
    }

    let alterations = [ClientQuotaAlteration::new(entity.clone(), ops)];
    let result = admin.alter_client_quotas_with_options(
        &alterations,
        AlterClientQuotasOptions::new().set_validate_only(validate_only),
    );
    match result.values()[&entity].get().await {
        Ok(()) => println!(
            "Client quotas of {} {}",
            entity_text(&entity),
            if validate_only { "validated" } else { "altered" }
        ),
        Err(error) => fail(format!("Failed to alter client quotas of {}", entity_text(&entity)), &error),
    }
    Ok(())
}
