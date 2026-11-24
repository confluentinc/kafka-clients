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
/// The owned <c>kafka_common_metrics_MetricsView_t</c> a client takes at construction and every map its
/// <c>Metrics()</c> returns reads: a live view that reads the client's metric registry at each call. It takes no
/// reference on the client and stays valid after the client is destroyed. It is destroyed once, when the handle is
/// released by the finalizer, after the client and every map over it are unreachable.
/// </summary>
internal sealed class SafeMetricsViewHandle : SafeHandleZeroIsInvalid
{
    internal SafeMetricsViewHandle(IntPtr view)
    {
        SetHandle(view);
    }

    protected override bool ReleaseHandle()
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MetricsViewDestroy(handle, ref outErr);
        if (outErr == IntPtr.Zero)
        {
            return true;
        }

        NativeMethods.ErrorDestroy(outErr);
        return false;
    }
}
