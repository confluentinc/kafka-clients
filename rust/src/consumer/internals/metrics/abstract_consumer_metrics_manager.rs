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

//! Shared base of the consumer metrics managers
//! (`org.apache.kafka.clients.consumer.internals.metrics.AbstractConsumerMetricsManager`,
//! KAFKA-19542).

use crate::consumer::internals::metrics::MetricsLedger;

/// Utility class that serves as a common abstraction point for consumers to
/// create and register their metrics, and to ensure they're removed on
/// [`close`](Self::close) via the [`MetricsLedger`] instance.
///
/// Java's `abstract class ... implements AutoCloseable` has no abstract
/// methods: it holds the `protected final MetricsLedger metrics` and closes it.
/// Rust has no inheritance, so each manager embeds this struct as its `inner`
/// field (the `ConsumerHeartbeatRequestManager { inner: AbstractHeartbeatRequestManager }`
/// precedent), reaches the ledger through [`metrics`](Self::metrics) and
/// forwards its own `close()` here. A trait would add nothing: no caller is
/// generic over "a consumer metrics manager", and each manager is closed
/// through its concrete type (DoD #7).
#[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.AbstractConsumerMetricsManager")]
pub(crate) struct AbstractConsumerMetricsManager {
    metrics: MetricsLedger,
}

impl AbstractConsumerMetricsManager {
    /// Java's `protected AbstractConsumerMetricsManager(MetricsLedger metrics)`.
    /// Each manager's public constructor takes the `Metrics` registry and passes
    /// a fresh ledger over it (Java's `this(new MetricsLedger(metrics), ..)`).
    #[doc(
        alias = "org.apache.kafka.clients.consumer.internals.metrics.AbstractConsumerMetricsManager#AbstractConsumerMetricsManager"
    )]
    pub(crate) fn new(metrics: MetricsLedger) -> Self {
        Self { metrics }
    }

    /// Java's `protected final MetricsLedger metrics` field.
    pub(crate) fn metrics(&self) -> &MetricsLedger {
        &self.metrics
    }

    /// Removes every metric and sensor the manager registered through its
    /// ledger. Java `close()` (`AutoCloseable`).
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.metrics.AbstractConsumerMetricsManager#close")]
    pub(crate) fn close(&self) {
        self.metrics.close();
    }
}

#[cfg(test)]
pub(crate) mod tests {
    //! `AbstractConsumerMetricsManagerTest` is an abstract JUnit class whose one
    //! test, `testCleanup`, each manager's test class inherits by overriding
    //! `metricsManager(Metrics, String)`. Rust has no test inheritance: the body
    //! is [`test_cleanup`], and each manager's test module calls it with its own
    //! factory.

    use std::sync::Arc;

    use crate::common::metrics::Metrics;

    /// `AbstractConsumerMetricsManagerTest.testCleanup`: building the manager
    /// adds metrics, and closing it removes exactly those.
    ///
    /// `metrics_manager` is Java's `metricsManager(Metrics, String
    /// groupDescription)` override; it returns the manager's `close` so one
    /// helper serves every manager type.
    pub(crate) fn test_cleanup<F>(metrics_manager: F)
    where
        F: FnOnce(&Arc<Metrics>, &str) -> Box<dyn FnOnce()>,
    {
        let metrics = Arc::new(Metrics::new());
        let metric_count = metrics.metrics().len();

        let close = metrics_manager(&metrics, "test");
        assert!(metrics.metrics().len() > metric_count);
        close();

        assert_eq!(metric_count, metrics.metrics().len());
        metrics.close();
    }
}
