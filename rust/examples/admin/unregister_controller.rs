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

//! Unregisters a KRaft controller with `Admin::unregister_controller`
//! (KAFKA-20395): the active controller removes the controller's registration,
//! for example after the node has been removed from the controller quorum
//! (KIP-853) and shut down for good. The controller refuses an id that is not
//! registered (`ControllerIdNotRegisteredError`) and the id of the active
//! controller itself (`InvalidRequestError`).
//!
//! A broker forwards the request to the active controller; with
//! `-X bootstrap.controllers=...` the client sends it to the controllers
//! directly.
//!
//! ```text
//! cargo run --example admin_unregister_controller -- 3001
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use confluent_kafka::admin::Admin;

#[path = "common/mod.rs"]
#[expect(dead_code, reason = "every example uses only some of the shared helpers")]
mod common;
use common::{Args, Failure, parse, usage};

const USAGE: &str = "usage: admin_unregister_controller [-X key=value]... <controller_id>";

#[tokio::main]
async fn main() -> ExitCode {
    common::main(USAGE, run).await
}

async fn run(admin: Arc<dyn Admin>, args: Args) -> Result<(), Failure> {
    let positional = args.positional()?;
    let [controller_id] = positional.as_slice() else {
        return usage("expected exactly one controller id");
    };
    let controller_id: i32 = parse("controller id", controller_id)?;

    admin.unregister_controller(controller_id).all().get().await?;
    println!("Controller {controller_id} unregistered");
    Ok(())
}
