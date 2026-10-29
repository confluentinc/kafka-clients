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

//! Describes client quotas with `Admin::describe_client_quotas`.
//!
//! The filter is a sequence of `<entity_type> <name>` pairs, the entity type
//! being `user`, `client-id` or `ip`; the name `DEFAULT` matches the default
//! entity of that type and `ANY` any named entity of it. Without pairs every
//! configured entity is described. With `--strict` only entities made of
//! exactly the given types match; otherwise entities with further types do too.
//!
//! ```text
//! cargo run --example admin_describe_client_quotas -- user alice client-id ANY
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;
use confluent_kafka::common::quota::{ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, groups};

const USAGE: &str =
    "usage: admin_describe_client_quotas [-X key=value]... [--strict] [<entity_type> <name|DEFAULT|ANY>]...";

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
    let strict = args.flag("strict")?;
    let args = args.positional()?;
    let components: Vec<ClientQuotaFilterComponent> = groups(&args, 2, false)?
        .map(|pair| match pair[1].as_str() {
            "DEFAULT" => ClientQuotaFilterComponent::of_default_entity(pair[0].as_str()),
            "ANY" => ClientQuotaFilterComponent::of_entity_type(pair[0].as_str()),
            name => ClientQuotaFilterComponent::of_entity(pair[0].as_str(), name),
        })
        .collect();
    let filter = match (components.is_empty(), strict) {
        (true, false) => ClientQuotaFilter::all(),
        (_, false) => ClientQuotaFilter::contains(components),
        (_, true) => ClientQuotaFilter::contains_only(components),
    };

    let entities = admin.describe_client_quotas(&filter).entities().get().await?;
    let mut lines: Vec<String> = entities
        .iter()
        .map(|(entity, quotas)| {
            let mut quotas: Vec<String> = quotas.iter().map(|(key, value)| format!("{key}={value}")).collect();
            quotas.sort();
            format!("{}: {}", entity_text(entity), quotas.join(", "))
        })
        .collect();
    lines.sort();
    for line in &lines {
        println!("{line}");
    }
    println!("{} entities match", lines.len());
    Ok(())
}
