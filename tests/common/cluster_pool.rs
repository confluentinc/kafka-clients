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

//! Shared cluster instance registry for integration tests.
//!
//! A process-global pool mapping [`ClusterConfig`] to a running
//! [`KafkaCluster`]. All tests requesting the same configuration share
//! one container, amortizing the Docker startup cost.
//!
//! Thread safety is achieved through `Mutex` + `tokio::sync::OnceCell`:
//! - The `Mutex` protects the pool map (held briefly, only to look up or
//!   insert a `OnceCell`).
//! - The `OnceCell` ensures that exactly one task starts the container;
//!   all other callers await its completion.
//!
//! # Bounded residency
//!
//! The pool aims to keep [`TARGET_LIVE_CLUSTERS`] clusters running, evicting
//! the least-recently-used *idle* cluster to make room. It is a target rather
//! than a hard ceiling — see the constant for why. Without any bound the
//! suite's 13 distinct configs produce 24 broker JVMs that stay resident for
//! the whole run — measured at 7.1 GiB, which starves brokers on a 15 GB host
//! until they fail Raft quorum registration and self-terminate.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Once};

use super::cluster_config::ClusterConfig;
use super::kafka_cluster::KafkaCluster;

use tokio::sync::OnceCell;

/// Type alias for the cluster pool's inner value: a once-cell holding a shared cluster.
type ClusterCell = Arc<OnceCell<Arc<KafkaCluster>>>;

/// Target number of clusters kept running simultaneously.
///
/// Starting a new cluster first evicts least-recently-used clusters that no
/// test is holding. This is a **target, not a ceiling**: eviction can only
/// reclaim idle clusters, so when every live cluster is in use the new one is
/// started anyway rather than waiting. Nothing serializes that check either,
/// so concurrent misses can each overshoot — the hard upper bound remains the
/// number of distinct [`ClusterConfig`]s in the suite.
///
/// Blocking instead would *not* deadlock (no test holds two clusters at once
/// — each creates exactly one `TestContext`, which releases its cluster when
/// the test ends), but it would serialize the suite behind a resource limit
/// for no measured benefit. With this target plus the per-broker heap cap
/// (`KAFKA_HEAP_OPTS` in [`super::kafka_cluster`]), peak residency measured
/// 16 brokers / 4.8 GiB against 24 brokers / 7.1 GiB unbounded — enough
/// headroom that the starvation failures disappeared.
const TARGET_LIVE_CLUSTERS: usize = 5;

/// One pool slot: the cell plus the LRU stamp of its last request.
struct PoolEntry {
    cell: ClusterCell,
    /// Value of [`LRU_CLOCK`] when this config was last requested.
    last_used: u64,
}

/// Monotonic counter stamped onto [`PoolEntry::last_used`] on every request.
/// A counter rather than a clock so ordering is exact and test-independent.
static LRU_CLOCK: AtomicU64 = AtomicU64::new(0);

/// Process-global pool of shared Kafka cluster instances.
///
/// Each unique `ClusterConfig` gets at most one running container.
static CLUSTER_POOL: std::sync::LazyLock<Mutex<HashMap<ClusterConfig, PoolEntry>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Ensures the atexit cleanup hook is registered exactly once.
static CLEANUP_REGISTERED: Once = Once::new();

/// Registers an `atexit` handler that forcibly removes all Docker containers
/// in the pool when the process exits.
///
/// Rust does not run destructors for `LazyLock` statics at process exit,
/// so `ContainerAsync`'s `Drop` never fires. This hook uses `docker rm -f`
/// as a synchronous fallback that works even after the tokio runtime is gone.
fn register_cleanup_hook() {
    CLEANUP_REGISTERED.call_once(|| {
        unsafe extern "C" {
            safe fn atexit(callback: extern "C" fn()) -> std::os::raw::c_int;
        }

        extern "C" fn cleanup_containers() {
            let pool = CLUSTER_POOL.lock().expect("cluster pool lock poisoned");
            let clusters: Vec<_> = pool.values().filter_map(|entry| entry.cell.get().cloned()).collect();
            drop(pool);

            for cluster in &clusters {
                teardown(cluster);
            }
        }

        atexit(cleanup_containers);
    });
}

/// Forcibly removes a cluster's containers and network.
///
/// [`KafkaCluster`] has no `Drop` impl, so dropping the `Arc` alone frees the
/// Rust value but leaves the containers running — eviction must tear them
/// down explicitly. Uses the same `docker rm -f` calls as the atexit hook so
/// there is exactly one teardown path.
fn teardown(cluster: &KafkaCluster) {
    for id in cluster.container_ids() {
        let _ = std::process::Command::new("docker").args(["rm", "-f", id]).output();
    }
    let _ = std::process::Command::new("docker")
        .args(["network", "rm", cluster.network_name()])
        .output();
}

/// Evicts least-recently-used idle clusters until at most `keep` remain live.
///
/// A cluster is *idle* — and therefore safe to evict — only when both:
///   - the pool holds the sole reference to its [`KafkaCluster`]
///     (`Arc::strong_count == 1`), i.e. no `TestContext` is using it, and
///   - the pool holds the sole reference to its [`ClusterCell`], i.e. no task
///     has taken the cell out of the map and is about to `get_or_init` it.
///
/// The second check closes a race: a concurrent [`get_or_create`] clones the
/// cell, releases the lock, and only *then* reads the cluster out of it.
/// Without it we could tear down containers that caller is about to use.
///
/// Never blocks waiting for a cluster to become idle (see
/// [`TARGET_LIVE_CLUSTERS`]).
async fn evict_lru_until(keep: usize) {
    let evicted: Vec<Arc<KafkaCluster>> = {
        let mut pool = CLUSTER_POOL.lock().expect("cluster pool lock poisoned");

        let live = pool.values().filter(|entry| entry.cell.get().is_some()).count();
        if live <= keep {
            return;
        }

        // Idle candidates, least-recently-used first.
        let mut candidates: Vec<(u64, ClusterConfig)> = pool
            .iter()
            .filter(|(_, entry)| {
                Arc::strong_count(&entry.cell) == 1
                    && entry.cell.get().is_some_and(|cluster| Arc::strong_count(cluster) == 1)
            })
            .map(|(config, entry)| (entry.last_used, config.clone()))
            .collect();
        candidates.sort_by_key(|(last_used, _)| *last_used);

        let mut evicted = Vec::new();
        for (_, config) in candidates.into_iter().take(live.saturating_sub(keep)) {
            if let Some(entry) = pool.remove(&config)
                && let Some(cluster) = entry.cell.get()
            {
                evicted.push(Arc::clone(cluster));
            }
        }
        evicted
    };

    if evicted.is_empty() {
        return;
    }
    let count = evicted.len();

    // Tear down outside the lock (the pool mutex is on the hot path of every
    // `TestContext::new`) AND off the async runtime. Each `docker rm -f`
    // blocks for a few hundred ms and an eviction issues one per container
    // plus one for the network, so running it inline would stall a tokio
    // worker for a second or more with other tests' tasks queued behind it.
    //
    // Dropping the last `Arc<KafkaCluster>` here is also safer than dropping
    // it in async context: `ContainerAsync`'s own `Drop` wants a runtime
    // handle, which a `spawn_blocking` thread has and an async context
    // cannot block on.
    tokio::task::spawn_blocking(move || {
        for cluster in &evicted {
            teardown(cluster);
        }
    })
    .await
    .expect("cluster teardown task panicked");

    eprintln!("INFO: evicted {count} idle Kafka cluster(s) to stay within TARGET_LIVE_CLUSTERS={TARGET_LIVE_CLUSTERS}");
}

/// Get or create a shared [`KafkaCluster`] for the given config.
///
/// The first caller with a given config triggers container startup;
/// subsequent callers await the same `OnceCell` and receive a reference
/// to the already-running cluster.
///
/// Before starting a *new* cluster the pool is trimmed to
/// [`TARGET_LIVE_CLUSTERS`] by evicting idle clusters, least-recently-used
/// first.
pub async fn get_or_create(config: &ClusterConfig) -> Arc<KafkaCluster> {
    register_cleanup_hook();

    let (cell, already_live) = {
        let mut pool = CLUSTER_POOL.lock().expect("cluster pool lock poisoned");
        let stamp = LRU_CLOCK.fetch_add(1, Ordering::Relaxed);
        let entry = pool
            .entry(config.clone())
            .or_insert_with(|| PoolEntry { cell: Arc::new(OnceCell::new()), last_used: stamp });
        entry.last_used = stamp;
        (Arc::clone(&entry.cell), entry.cell.get().is_some())
    };

    // Only a cluster we are about to start adds to residency; an existing one
    // is already counted. Trim to `TARGET_LIVE_CLUSTERS - 1` so this one fits.
    if !already_live {
        evict_lru_until(TARGET_LIVE_CLUSTERS.saturating_sub(1)).await;
    }

    cell.get_or_init(|| async { Arc::new(KafkaCluster::start_with_config(config).await) })
        .await
        .clone()
}
