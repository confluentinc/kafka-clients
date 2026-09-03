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
            // Backend containers first: they are attached to the clusters'
            // networks, and `docker network rm` fails while a network has
            // active endpoints. `backend_pool`'s own hook also does this, but
            // `atexit` runs handlers in reverse registration order and which
            // hook registers first depends on which test ran first — so do not
            // depend on that ordering.
            force_remove_backend_containers();

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
///
/// Every container attached to `cluster.network_name()` must already be gone
/// when this runs, or the `network rm` fails on active endpoints and the
/// network leaks. The `WARN` makes such a leak visible instead of silent: a
/// leaked network consumes a slice of Docker's address pool for the rest of the
/// process, and once the pool is exhausted *every* subsequent cluster start
/// fails.
fn teardown(cluster: &KafkaCluster) {
    for id in cluster.container_ids() {
        let _ = std::process::Command::new("docker").args(["rm", "-f", id]).output();
    }
    match std::process::Command::new("docker")
        .args(["network", "rm", cluster.network_name()])
        .output()
    {
        Ok(output) if !output.status.success() => eprintln!(
            "WARN: failed to remove Docker network {} (it will leak for the rest of this process): {}",
            cluster.network_name(),
            String::from_utf8_lossy(&output.stderr).trim()
        ),
        Ok(_) => {},
        Err(e) => eprintln!("WARN: could not run `docker network rm {}`: {e}", cluster.network_name()),
    }
}

/// De-pools and removes the gRPC backend containers attached to `network`,
/// returning how many were removed.
///
/// A backend container is a **child of the cluster that owns the network**: it
/// is started with `--network <cluster network>` so it can reach the brokers by
/// container hostname, but it is pooled independently and outlives the test
/// that asked for it. Evicting the cluster therefore has to reap them too —
/// otherwise the eviction's `docker network rm` fails on active endpoints, the
/// network leaks, and a container that can no longer reach any broker stays
/// resident competing for the Docker VM's memory.
///
/// Must run on a blocking thread; see the callee's docs.
#[cfg(feature = "multilanguage-tests")]
fn teardown_backends_on(network: &str) -> usize {
    super::backend_pool::take_and_remove_handles_on_network(network)
}

/// No gRPC backends exist without the `multilanguage-tests` feature.
#[cfg(not(feature = "multilanguage-tests"))]
fn teardown_backends_on(_network: &str) -> usize {
    0
}

/// Whether a test still holds, or is starting, a backend container on
/// `network` — such a cluster must not be evicted.
#[cfg(feature = "multilanguage-tests")]
fn backends_in_use_on(network: &str) -> bool {
    super::backend_pool::has_live_handles_on_network(network)
}

#[cfg(not(feature = "multilanguage-tests"))]
fn backends_in_use_on(_network: &str) -> bool {
    false
}

/// `docker rm -f` every pooled backend container without de-pooling it, so no
/// `ContainerAsync::drop` runs. Safe from the `atexit` handler.
#[cfg(feature = "multilanguage-tests")]
fn force_remove_backend_containers() {
    super::backend_pool::force_remove_all_containers();
}

#[cfg(not(feature = "multilanguage-tests"))]
fn force_remove_backend_containers() {}

/// Evicts least-recently-used idle clusters until at most `keep` remain live.
///
/// A cluster is *idle* — and therefore safe to evict — only when all three of:
///   - the pool holds the sole reference to its [`KafkaCluster`]
///     (`Arc::strong_count == 1`), i.e. no `TestContext` is using it,
///   - the pool holds the sole reference to its [`ClusterCell`], i.e. no task
///     has taken the cell out of the map and is about to `get_or_init` it, and
///   - no gRPC backend container on its network is checked out or starting
///     (see [`backends_in_use_on`]).
///
/// The second check closes a race: a concurrent [`get_or_create`] clones the
/// cell, releases the lock, and only *then* reads the cluster out of it.
/// Without it we could tear down containers that caller is about to use.
///
/// Never blocks waiting for a cluster to become idle (see
/// [`TARGET_LIVE_CLUSTERS`]).
///
/// # Lock order
///
/// `CLUSTER_POOL` then `BACKEND_POOL` (via [`backends_in_use_on`]). Nothing
/// takes them in the other order — `backend_pool::get_or_start` takes only its
/// own lock — so this cannot deadlock.
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
                    && entry.cell.get().is_some_and(|cluster| {
                        Arc::strong_count(cluster) == 1 && !backends_in_use_on(cluster.network_name())
                    })
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
    let backends_removed = tokio::task::spawn_blocking(move || {
        let mut backends_removed = 0;
        for cluster in &evicted {
            // The cluster owns the network; the gRPC backend containers on it are
            // children. Reap them first or the `network rm` below fails on active
            // endpoints. Safe here and only here: this cluster is idle, so by the
            // `backends_in_use_on` check above no test holds a handle on it.
            backends_removed += teardown_backends_on(cluster.network_name());
            teardown(cluster);
        }
        backends_removed
    })
    .await
    .expect("cluster teardown task panicked");

    eprintln!(
        "INFO: evicted {count} idle Kafka cluster(s) (and {backends_removed} attached gRPC backend container(s)) \
         to stay within TARGET_LIVE_CLUSTERS={TARGET_LIVE_CLUSTERS}"
    );
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
