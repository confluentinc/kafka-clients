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

//! Lists the resources that have a configuration with
//! `Admin::list_config_resources_with_options`. Without arguments every
//! resource type is listed; otherwise only the given types (`topic`, `broker`,
//! `broker_logger`, `group` or `client_metrics`).
//!
//! ```text
//! cargo run --example admin_list_config_resources -- topic group
//! ```

use std::collections::HashSet;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, ListConfigResourcesOptions};
use confluent_kafka::common::config::config_resource::Type;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, choose};

const USAGE: &str = "usage: admin_list_config_resources [-X key=value]... [<type>...]\n\
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

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let types = args
        .positional()?
        .iter()
        .map(|text| resource_type(text))
        .collect::<Result<HashSet<_>, _>>()?;

    let result = if types.is_empty() {
        admin.list_config_resources()
    } else {
        admin.list_config_resources_with_options(&types, ListConfigResourcesOptions::new())
    };
    let mut resources: Vec<(String, String)> = result
        .all()
        .get()
        .await?
        .iter()
        .map(|resource| (resource.r#type().to_string().to_lowercase(), resource.name().to_string()))
        .collect();
    resources.sort();
    for (resource_type, name) in &resources {
        println!("{resource_type} {name}");
    }
    Ok(())
}
