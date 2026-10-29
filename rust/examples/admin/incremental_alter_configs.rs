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

//! Changes the configuration of resources with
//! `Admin::incremental_alter_configs`. Each resource is a type (`topic`,
//! `broker`, `broker_logger`, `group` or `client_metrics`), a name and a
//! `;`-separated list of operations: `key=SET:value` sets a value,
//! `key=APPEND:value` and `key=SUBTRACT:value` add items to or remove items
//! from a list value, and `key=DELETE` reverts the key to its default. Keys not
//! named keep their current value.
//!
//! ```text
//! cargo run --example admin_incremental_alter_configs -- topic orders 'retention.ms=SET:86400000;cleanup.policy=APPEND:compact'
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, AlterConfigOp, ConfigEntry, OpType};
use confluent_kafka::common::config::ConfigResource;
use confluent_kafka::common::config::config_resource::Type;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, choose, fail, groups, usage};

const USAGE: &str = "usage: admin_incremental_alter_configs [-X key=value]... <type> <name> <key=OP:value;key=DELETE;...>...\n\
                     types: topic, broker, broker_logger, group, client_metrics; OP: SET, APPEND, SUBTRACT";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

fn resource_type(text: &str) -> Result<Type, Failure> {
    choose(
        "resource type",
        text,
        &[
            ("topic", Type::Topic),
            ("broker", Type::Broker),
            ("broker_logger", Type::BrokerLogger),
            ("group", Type::Group),
            ("client_metrics", Type::ClientMetrics),
        ],
    )
}

fn label(resource: &ConfigResource) -> String {
    format!("{} {}", resource.r#type().to_string().to_lowercase(), resource.name())
}

fn operations(text: &str) -> Result<Vec<AlterConfigOp>, Failure> {
    let mut operations = Vec::new();
    for spec in text.split(';').filter(|spec| !spec.is_empty()) {
        let Some((key, operation)) = spec.split_once('=') else {
            return usage(format!("invalid operation {spec:?}: expected key=OP:value or key=DELETE"));
        };
        let (op_type, value) = if operation.eq_ignore_ascii_case("delete") {
            (OpType::Delete, None)
        } else {
            let Some((op_type, value)) = operation.split_once(':') else {
                return usage(format!("invalid operation {spec:?}: expected key=OP:value or key=DELETE"));
            };
            let op_type = choose(
                "operation",
                op_type,
                &[
                    ("set", OpType::Set),
                    ("append", OpType::Append),
                    ("subtract", OpType::Subtract),
                ],
            )?;
            (op_type, Some(value.to_string()))
        };
        operations.push(AlterConfigOp::new(ConfigEntry::new(key.to_string(), value), op_type));
    }
    if operations.is_empty() {
        return usage(format!("no operation in {text:?}"));
    }
    Ok(operations)
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let args = args.positional()?;
    let mut resources = Vec::new();
    let mut configs: HashMap<ConfigResource, Vec<AlterConfigOp>> = HashMap::new();
    for group in groups(&args, 3, true)? {
        let resource = ConfigResource::new(resource_type(&group[0])?, group[1].clone());
        let operations = operations(&group[2])?;
        if !configs.contains_key(&resource) {
            resources.push(resource.clone());
        }
        configs.entry(resource).or_default().extend(operations);
    }

    let result = admin.incremental_alter_configs(&configs);
    for resource in &resources {
        match result.values()[resource].get().await {
            Ok(()) => println!("Altered configs of {}", label(resource)),
            Err(error) => fail(format!("Failed to alter configs of {}", label(resource)), &error),
        }
    }
    Ok(())
}
