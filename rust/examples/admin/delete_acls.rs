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

//! Deletes the ACL bindings that match the given filters with
//! `Admin::delete_acls`.
//!
//! Each filter is seven arguments: resource type, resource name, pattern type,
//! principal, host, operation and permission type. Enum values are matched by
//! name, ignoring case, and accept `any`; the pattern type also accepts
//! `match`. `ANY` or `None` as the name, principal or host matches any value.
//! Every binding a filter deletes is printed. The broker must have an
//! authorizer configured (`authorizer.class.name`), otherwise every filter
//! fails with a security-disabled error.
//!
//! ```text
//! cargo run --example admin_delete_acls -- topic orders literal User:alice ANY any any
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;
use confluent_kafka::common::acl::{
    AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType,
};
use confluent_kafka::common::resource::{PatternType, ResourcePatternFilter, ResourceType};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, fail, groups, usage};

const USAGE: &str = "usage: admin_delete_acls [-X key=value]... \
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

/// `ANY` or `None` matches any name, principal or host.
fn any_or(text: &str) -> Option<String> {
    (text != "ANY" && text != "None").then(|| text.to_string())
}

fn acl_binding_filter(group: &[String]) -> Result<AclBindingFilter, Failure> {
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
    Ok(AclBindingFilter::new(
        ResourcePatternFilter::new(resource_type, any_or(&group[1]), pattern_type),
        AccessControlEntryFilter::new(any_or(&group[3]), any_or(&group[4]), operation, permission_type),
    ))
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

/// The filter in the command-line format, `ANY` standing for a missing field.
fn filter_text(filter: &AclBindingFilter) -> String {
    let (pattern, entry) = (filter.pattern_filter(), filter.entry_filter());
    format!(
        "{} {} {} {} {} {} {}",
        pattern.resource_type(),
        pattern.name().unwrap_or("ANY"),
        pattern.pattern_type(),
        entry.principal().unwrap_or("ANY"),
        entry.host().unwrap_or("ANY"),
        entry.operation(),
        entry.permission_type()
    )
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let args = args.positional()?;
    let filters = groups(&args, 7, true)?.map(acl_binding_filter).collect::<Result<Vec<_>, _>>()?;

    let result = admin.delete_acls(&filters);
    for filter in &filters {
        match result.values()[filter].get().await {
            Ok(results) => {
                println!("Filter {}: {} matching ACLs", filter_text(filter), results.values().len());
                // Each match is deleted or fails on its own.
                for filter_result in results.values() {
                    match (filter_result.binding(), filter_result.error()) {
                        (Some(binding), None) => println!("  deleted ACL {}", binding_text(binding)),
                        (Some(binding), Some(error)) => {
                            fail(format!("Failed to delete ACL {}", binding_text(binding)), error)
                        },
                        (None, Some(error)) => {
                            fail(format!("Failed to delete an ACL matching {}", filter_text(filter)), error)
                        },
                        (None, None) => {},
                    }
                }
            },
            Err(error) => fail(format!("Failed to delete ACLs matching {}", filter_text(filter)), &error),
        }
    }
    Ok(())
}
