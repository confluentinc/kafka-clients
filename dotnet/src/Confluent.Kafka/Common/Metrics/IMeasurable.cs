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

using System;

namespace Confluent.Kafka.Common.Metrics;

/// <summary>
/// A measurable quantity that can be registered as a metric, Java's <c>org.apache.kafka.common.metrics.Measurable</c>.
/// </summary>
/// <remarks>
/// Java's <c>value(MetricConfig, long)</c> is a <c>default</c> method that returns <see cref="Measure"/>. An interface
/// on netstandard2.0 cannot hold its body, so an implementer writes <see cref="IMetricValueProvider{T}.Value"/> too,
/// and can return <see cref="MeasurableExtensions.Value(IMeasurable, MetricConfig, long)"/>, Java's default, from it.
/// </remarks>
public interface IMeasurable : IMetricValueProvider<double>
{
    /// <summary>Measure this quantity and return the result as a double.</summary>
    /// <param name="config">The configuration for this metric.</param>
    /// <param name="now">The POSIX time in milliseconds the measurement is being taken.</param>
    /// <returns>The measured value.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="config"/> is <see langword="null"/>, on the measurable <see cref="KafkaMetric.Measurable"/>
    /// returns: the native core cannot take a <see langword="null"/> configuration.
    /// </exception>
    /// <exception cref="ArgumentException">
    /// A tag of <paramref name="config"/> holds a NUL character or an unpaired surrogate, on the measurable
    /// <see cref="KafkaMetric.Measurable"/> returns, as <see cref="KafkaMetric.Config(MetricConfig)"/> throws.
    /// </exception>
    /// <exception cref="IllegalStateException">
    /// On the measurable <see cref="KafkaMetric.Measurable"/> returns, for a rate whose <paramref name="config"/> has a
    /// zero time window: the native core's caught panic, where Java's <c>Rate.windowSize</c> throws
    /// <c>ArithmeticException</c> (a recorded difference).
    /// </exception>
    double Measure(MetricConfig config, long now);
}

/// <summary>
/// Java's <c>default</c> method of <see cref="IMeasurable"/>, which a .NET interface on netstandard2.0 cannot hold.
/// </summary>
public static class MeasurableExtensions
{
    /// <summary>
    /// Measure this quantity and return the result as a double. This default implementation delegates to
    /// <see cref="IMeasurable.Measure(MetricConfig, long)"/>.
    /// </summary>
    /// <param name="measurable">The measurable quantity.</param>
    /// <param name="config">The configuration for this metric.</param>
    /// <param name="now">The POSIX time in milliseconds the measurement is being taken.</param>
    /// <returns>The measured value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="measurable"/> is <see langword="null"/>.</exception>
    /// <remarks>
    /// A call written <c>measurable.Value(config, now)</c> calls the object's own
    /// <see cref="IMetricValueProvider{T}.Value"/>, since every implementer has one; this method is Java's default
    /// body, for an implementer to return from it.
    /// </remarks>
    public static double Value(this IMeasurable measurable, MetricConfig config, long now) =>
        (measurable ?? throw new ArgumentNullException(nameof(measurable))).Measure(config, now);
}
