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
/// The handle of a <c>kafka_Map_t</c> that owns the futures it holds: each of those futures holds a reference on it
/// until its own delivery, so the map is destroyed, with its keys and futures, only after the last of them.
/// </summary>
internal sealed class SafeKafkaMapHandle : SafeHandleZeroIsInvalid
{
    internal SafeKafkaMapHandle(IntPtr map)
    {
        SetHandle(map);
    }

    protected override bool ReleaseHandle()
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MapDestroy(handle, ref outErr);
        if (outErr == IntPtr.Zero)
        {
            return true;
        }

        NativeMethods.ErrorDestroy(outErr);
        return false;
    }
}
