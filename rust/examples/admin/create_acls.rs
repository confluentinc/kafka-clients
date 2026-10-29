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

//! Creates ACL bindings with `Admin::create_acls`.
//!
//! Each binding is seven arguments: the resource type, resource name and
//! pattern type of the resource pattern, then the principal, host, operation
//! and permission type of the access control entry. Enum values are matched
//! by name, ignoring case. The broker must have an authorizer configured
//! (`authorizer.class.name`), otherwise every binding fails with a
//! security-disabled error.
//!
//! ```text
//! cargo run --example admin_create_acls -- topic orders literal User:alice '*' read allow
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;
use confluent_kafka::common::acl::{AccessControlEntry, AclBinding, AclOperation, AclPermissionType};
use confluent_kafka::common::resource::{PatternType, ResourcePattern, ResourceType};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, groups, usage};

const USAGE: &str = "usage: admin_create_acls [-X key=value]... \
                     (<resource_type> <resource_name> <pattern_type> \
                     <principal> <host> <operation> <permission_type>)...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

/// Fails with a usage error when `text` parsed to the enum's `unknown` value.
fn known<T: PartialEq>(what: &str, text: &str, parsed: T, unknown: T) -> Result<T, Failure> {
    if parsed == unknown {
        return usage(format!("invalid {what}: {text:?}"));
    }
    Ok(parsed)
}

fn acl_binding(group: &[String]) -> Result<AclBinding, Failure> {
    let resource_type = known(
        "resource type",
        &group[0],
        ResourceType::from_string(&group[0]),
        ResourceType::Unknown,
    )?;
    let pattern_type = known(
        "pattern type",
        &group[2],
        PatternType::from_string(&group[2].to_ascii_uppercase()),
        PatternType::Unknown,
    )?;
    let operation = known(
        "operation",
        &group[5],
        AclOperation::from_string(&group[5]),
        AclOperation::Unknown,
    )?;
    let permission_type = known(
        "permission type",
        &group[6],
        AclPermissionType::from_string(&group[6]),
        AclPermissionType::Unknown,
    )?;
    // A binding names one specific resource and entry: `any` and `match` are
    // only valid in filters.
    let pattern = ResourcePattern::new(resource_type, group[1].as_str(), pattern_type)
        .map_err(|error| Failure::Usage(format!("invalid ACL binding: {error}")))?;
    let entry = AccessControlEntry::new(group[3].as_str(), group[4].as_str(), operation, permission_type)
        .map_err(|error| Failure::Usage(format!("invalid ACL binding: {error}")))?;
    Ok(AclBinding::new(pattern, entry))
}

/// The binding in the command-line format.
fn binding_text(binding: &AclBinding) -> String {
    let (pattern, entry) = (binding.pattern(), binding.entry());
    format!(
        "{} {} {} {} {} {} {}",
        pattern.resource_type(),
        pattern.name(),
        pattern.pattern_type(),
        entry.principal(),
        entry.host(),
        entry.operation(),
        entry.permission_type()
    )
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let args = args.positional()?;
    let bindings = groups(&args, 7, true)?.map(acl_binding).collect::<Result<Vec<_>, _>>()?;

    let result = admin.create_acls(&bindings);
    for binding in &bindings {
        match result.values()[binding].get().await {
            Ok(()) => println!("Created ACL {}", binding_text(binding)),
            Err(error) => fail(format!("Failed to create ACL {}", binding_text(binding)), &error),
        }
    }
    Ok(())
}
