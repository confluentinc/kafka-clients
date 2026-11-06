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

// GENERATED, DO NOT EDIT. Produced from the Java source by `cargo xtask generate-error-codes`
// and checked for staleness by `cargo xtask check-generated`.

using System;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Metrics;

/// <summary>
/// Thrown when a sensor records a value that causes a metric to go outside the bounds configured as its quota. Java's
/// <c>org.apache.kafka.common.metrics.QuotaViolationException</c>.
/// </summary>
public class QuotaViolationException : global::Confluent.Kafka.Common.KafkaException
{
    /// <summary>
    /// Initializes the exception, as Java's <c>QuotaViolationException(KafkaMetric metric, double value, double
    /// bound)</c>.
    /// </summary>
    public QuotaViolationException(KafkaMetric metric, double value, double bound)
        : base("'" + JavaText.ValueOf((metric ?? throw new ArgumentNullException(nameof(metric))).MetricName()) + "' violated quota. Actual: " + JavaText.ValueOf(value) + ", Threshold: " + JavaText.ValueOf(bound))
    {
        Metric = metric;
        Value = value;
        Bound = bound;
    }

    internal QuotaViolationException(CoreError _, string? message, Exception? innerException, KafkaMetric metric, double value, double bound)
        : base(_, message, innerException)
    {
        Metric = metric;
        Value = value;
        Bound = bound;
    }

    /// <summary>Java's <c>metric()</c>.</summary>
    public KafkaMetric Metric { get; }

    /// <summary>Java's <c>value()</c>.</summary>
    public double Value { get; }

    /// <summary>Java's <c>bound()</c>.</summary>
    public double Bound { get; }

    internal override int FfiId => -26;

    internal override string TypeAndMessage() => JavaText.TypeAndMessage(this, Message);
}
