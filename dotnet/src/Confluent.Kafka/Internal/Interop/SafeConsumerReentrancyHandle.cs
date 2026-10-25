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

using System;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Owns a <c>kafka_consumer_ConsumerHandle_t</c>, which holds its consumer weakly; the release destroys it.
/// </summary>
internal sealed class SafeConsumerReentrancyHandle : SafeHandleZeroIsInvalid
{
    private SafeConsumerReentrancyHandle()
    {
    }

    /// <inheritdoc/>
    protected override bool ReleaseHandle()
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerHandleDestroy(handle, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }

        return true;
    }
}
