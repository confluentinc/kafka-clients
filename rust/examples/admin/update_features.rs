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

//! Updates finalized feature version levels with
//! `Admin::update_features_with_options`. Each update names a feature, its new
//! max version level and how it may change: `upgrade`, `safe_downgrade`
//! (refused when it could lose metadata) or `unsafe_downgrade`. A max level of
//! 0 with a downgrade disables the feature. With `--validate-only` the
//! controller checks the updates without applying them.
//!
//! The controller applies the updates in one request; each feature still
//! reports its own outcome. `admin_describe_features` prints the current
//! levels and the supported ranges.
//!
//! ```text
//! cargo run --example admin_update_features -- --validate-only metadata.version 27 upgrade
//! ```

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::{Admin, FeatureUpdate, UpdateFeaturesOptions, UpgradeType};

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, choose, describe, fail, groups, parse, usage};

const USAGE: &str = "usage: admin_update_features [-X key=value]... [--validate-only] \
                     (<feature> <max_level> <upgrade|safe_downgrade|unsafe_downgrade>)...";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, mut args: Args) -> Result<(), Failure> {
    let validate_only = args.flag("validate-only")?;
    let positional = args.positional()?;

    let mut names = Vec::new();
    let mut updates = HashMap::new();
    for group in groups(&positional, 3, true)? {
        let name = group[0].clone();
        let max_level = parse("max version level", &group[1])?;
        let upgrade_type = choose(
            "upgrade type",
            &group[2],
            &[
                ("upgrade", UpgradeType::Upgrade),
                ("safe_downgrade", UpgradeType::SafeDowngrade),
                ("unsafe_downgrade", UpgradeType::UnsafeDowngrade),
            ],
        )?;
        let update = FeatureUpdate::new(max_level, upgrade_type).map_err(|error| Failure::Usage(describe(&error)))?;
        if updates.insert(name.clone(), update).is_some() {
            return usage(format!("feature {name} given twice"));
        }
        names.push(name);
    }

    // Unlike most RPCs this one checks its arguments before sending.
    let result =
        admin.update_features_with_options(&updates, UpdateFeaturesOptions::new().set_validate_only(validate_only))?;
    for name in &names {
        let update = &updates[name];
        match result.values()[name].get().await {
            Ok(()) => println!(
                "Feature {name} {}: max level {}",
                if validate_only { "validated" } else { "updated" },
                update.max_version_level()
            ),
            Err(error) => fail(format!("Failed to update feature {name}"), &error),
        }
    }
    Ok(())
}
