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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The handle of a <c>kafka_common_KafkaFuture_t</c>: an owned future is destroyed on release; a borrowed one holds a
/// reference on its owner instead, released in its place.
/// </summary>
internal sealed class SafeKafkaFutureHandle : SafeHandleZeroIsInvalid
{
    private readonly SafeHandle? _owner;

    private SafeKafkaFutureHandle(IntPtr future, SafeHandle? owner)
    {
        _owner = owner;
        SetHandle(future);
    }

    internal static SafeKafkaFutureHandle Owned(IntPtr future) => new SafeKafkaFutureHandle(future, null);

    internal static SafeKafkaFutureHandle Borrowed(IntPtr future, SafeHandle owner)
    {
        bool added = false;
        owner.DangerousAddRef(ref added);
        return new SafeKafkaFutureHandle(future, owner);
    }

    protected override bool ReleaseHandle()
    {
        if (_owner is not null)
        {
            _owner.DangerousRelease();
            return true;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.KafkaFutureDestroy(handle, ref outErr);
        if (outErr == IntPtr.Zero)
        {
            return true;
        }

        NativeMethods.ErrorDestroy(outErr);
        return false;
    }
}
