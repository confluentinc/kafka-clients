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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The value provider a client's <see cref="KafkaMetric.Measurable"/> returns: a view over the
/// <c>kafka_common_metrics_Measurable_t</c> borrowed from the metric's own handle, valid as long as that handle. It
/// keeps its <see cref="KafkaMetric"/>, and so the handle, alive, and each <see cref="Measure"/> holds a reference on
/// the handle for the call.
/// </summary>
internal sealed class NativeMeasurable : IMeasurable
{
    private readonly KafkaMetric _metric;
    private readonly IntPtr _measurable;

    internal NativeMeasurable(KafkaMetric metric, IntPtr measurable)
    {
        _metric = metric;
        _measurable = measurable;
    }

    public double Measure(MetricConfig config, long now)
    {
        MetricConfigMarshal.Validate(config, nameof(config));
        IntPtr native = MetricConfigMarshal.ToNative(config, nameof(config));
        SafeKafkaMetricHandle handle = _metric.Handle!;
        bool added = false;
        try
        {
            handle.DangerousAddRef(ref added);
            IntPtr outErr = IntPtr.Zero;
            double value = NativeMethods.MeasurableMeasure(_measurable, native, now, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return value;
        }
        finally
        {
            if (added)
            {
                handle.DangerousRelease();
            }

            MetricConfigMarshal.Destroy(native);
        }
    }

    // Java's default value(), which returns measure().
    public double Value(MetricConfig config, long now) => MeasurableExtensions.Value(this, config, now);
}
