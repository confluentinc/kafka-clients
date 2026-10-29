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

//! Describes the configuration of resources with
//! `Admin::describe_configs_with_options`. Each resource is a type (`topic`,
//! `broker`, `broker_logger`, `group` or `client_metrics`) followed by its name;
//! a broker is named by its id. Every entry shows its value, where the value
//! comes from and whether it is the default, sensitive or read-only. With
//! `--synonyms` the broker also returns the other configurations the value
//! could come from, in order of precedence, and with `--documentation` the
//! description of each entry.
//!
//! ```text
//! cargo run --example admin_describe_configs -- --synonyms topic orders broker 0
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, ConfigEntry, DescribeConfigsOptions};
use confluent_kafka::common::config::ConfigResource;
use confluent_kafka::common::config::config_resource::Type;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, choose, fail, groups};

const USAGE: &str = "usage: admin_describe_configs [-X key=value]... [--synonyms] [--documentation] <type> <name>...\n\
                     types: topic, broker, broker_logger, group, client_metrics";

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

fn entry_line(entry: &ConfigEntry) -> String {
    // A sensitive entry's value is never returned.
    let value = if entry.is_sensitive() {
        "******"
    } else {
        entry.value().unwrap_or("null")
    };
    let flags: Vec<&str> = [
        (entry.is_default(), "default"),
        (entry.is_sensitive(), "sensitive"),
        (entry.is_read_only(), "read-only"),
    ]
    .into_iter()
    .filter_map(|(set, flag)| set.then_some(flag))
    .collect();
    let flags = if flags.is_empty() {
        String::new()
    } else {
        format!(" ({})", flags.join(", "))
    };
    format!("{} = {value} [{}]{flags}", entry.name(), entry.source())
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let include_synonyms = args.flag("synonyms")?;
    let include_documentation = args.flag("documentation")?;
    let args = args.positional()?;
    let mut resources = Vec::new();
    for group in groups(&args, 2, true)? {
        let resource = ConfigResource::new(resource_type(&group[0])?, group[1].clone());
        if !resources.contains(&resource) {
            resources.push(resource);
        }
    }

    let result = admin.describe_configs_with_options(
        &resources,
        DescribeConfigsOptions::new()
            .set_include_synonyms(include_synonyms)
            .set_include_documentation(include_documentation),
    );
    for resource in &resources {
        match result.values()[resource].get().await {
            Ok(config) => {
                println!("Configs of {}:", label(resource));
                let mut entries: Vec<&ConfigEntry> = config.entries().collect();
                entries.sort_by(|a, b| a.name().cmp(b.name()));
                for entry in entries {
                    println!("  {}", entry_line(entry));
                    for synonym in entry.synonyms() {
                        let value = if entry.is_sensitive() {
                            "******"
                        } else {
                            synonym.value().unwrap_or("null")
                        };
                        println!("    synonym {} = {value} [{}]", synonym.name(), synonym.source());
                    }
                    // Some brokers send an empty description even when none was asked for.
                    if let Some(documentation) = entry.documentation().filter(|text| !text.is_empty()) {
                        println!("    documentation: {documentation}");
                    }
                }
            },
            Err(error) => fail(format!("Failed to describe configs of {}", label(resource)), &error),
        }
    }
    Ok(())
}
