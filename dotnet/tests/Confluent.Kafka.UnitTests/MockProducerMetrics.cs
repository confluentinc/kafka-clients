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
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>MockProducer.setMockMetrics(MetricName, Metric)</c>, which the binding does not expose, made through the
/// C ABI for the tests: a mock producer registers a metric another client's <c>Metrics()</c> returned.
/// </summary>
internal static class MockProducerMetrics
{
    /// <summary>Registers <paramref name="metric"/> on <paramref name="mockProducer"/> under <paramref name="name"/>.</summary>
    internal static void Set(object mockProducer, MetricName name, KafkaMetric metric)
    {
        SafeProducerHandle mock = NativeOf(mockProducer).Handle;
        IntPtr nativeName = MetricsViewMarshal.ToNativeName(name);
        if (nativeName == IntPtr.Zero)
        {
            throw new ArgumentException("the name cannot cross the C ABI: " + name, nameof(name));
        }

        try
        {
            IntPtr outErr = IntPtr.Zero;
            bool added = false;
            try
            {
                Native.MockProducerSetMockMetrics(mock.DangerousAddRefClass(ref added), nativeName, HandleOf(metric), ref outErr);
            }
            finally
            {
                if (added)
                {
                    mock.DangerousRelease();
                }
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            MetricsViewMarshal.DestroyName(nativeName);
        }
    }

    /// <summary>The metric named <paramref name="name"/> in <paramref name="metrics"/>, whatever its group.</summary>
    internal static KafkaMetric Find(IReadOnlyDictionary<MetricName, IMetric> metrics, string name)
    {
        foreach (KeyValuePair<MetricName, IMetric> entry in metrics)
        {
            if (entry.Key.Name() == name)
            {
                return (KafkaMetric)entry.Value;
            }
        }

        throw new InvalidOperationException("no metric " + name);
    }

    private static SafeKafkaMetricHandle HandleOf(KafkaMetric metric) =>
        (SafeKafkaMetricHandle)typeof(KafkaMetric)
            .GetField("_handle", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(metric)!;

    private static NativeProducer NativeOf(object producer) =>
        (NativeProducer)producer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;

    /// <summary>The C ABI call only these tests make.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_producer_MockProducer_set_mock_metrics_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void MockProducerSetMockMetrics(
            IntPtr self, IntPtr name, SafeKafkaMetricHandle metric, ref IntPtr outErr);
    }
}
