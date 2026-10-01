// Copyright 2025 Confluent Inc.
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

//! The testcontainers harness of `tests/common`, compiled into the crate's own
//! test binary. The files are included by path, not copied, so the in-crate
//! tests and the `tests/integration` suites share one harness.

// `dead_code`: the harness serves every integration suite; this binary uses
// only part of it.
#[expect(dead_code)]
#[path = "../../../tests/common/cluster_config.rs"]
pub(crate) mod cluster_config;
#[path = "../../../tests/common/cluster_pool.rs"]
pub(crate) mod cluster_pool;
#[expect(dead_code)]
#[path = "../../../tests/common/kafka_cluster.rs"]
pub(crate) mod kafka_cluster;
#[path = "../../../tests/common/test_certs.rs"]
pub(crate) mod test_certs;
#[expect(dead_code)]
#[path = "../../../tests/common/test_context.rs"]
pub(crate) mod test_context;

pub(crate) mod selector_utils;

/// `cluster_pool` reaps the gRPC backend containers pooled on a cluster's
/// network before evicting it. This binary never starts a backend, so under
/// `multilanguage-tests` the answer is always "none" — the real
/// `tests/common/backend_pool.rs` is not compiled into the crate.
#[cfg(feature = "multilanguage-tests")]
pub(crate) mod backend_pool {
    pub(crate) fn take_and_remove_handles_on_network(_network: &str) -> usize {
        0
    }

    pub(crate) fn has_live_handles_on_network(_network: &str) -> bool {
        false
    }

    pub(crate) fn force_stop_all_backends() {}
}
