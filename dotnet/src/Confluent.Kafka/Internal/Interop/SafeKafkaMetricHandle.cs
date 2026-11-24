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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The owned <c>kafka_common_metrics_KafkaMetric_t</c> a <see cref="Common.Metrics.KafkaMetric"/> keeps, so that each
/// read of its value measures the metric again. It is independent of the metrics view and of the client it came from,
/// takes no reference on either, and stays valid after both are destroyed. It is destroyed once, when the handle is
/// released (by the finalizer, as the public class is not disposable); the metric stays registered.
/// </summary>
internal sealed class SafeKafkaMetricHandle : SafeHandleZeroIsInvalid
{
    internal SafeKafkaMetricHandle(IntPtr metric)
    {
        SetHandle(metric);
    }

    protected override bool ReleaseHandle()
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.KafkaMetricDestroy(handle, ref outErr);
        if (outErr == IntPtr.Zero)
        {
            return true;
        }

        NativeMethods.ErrorDestroy(outErr);
        return false;
    }
}
