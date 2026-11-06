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
/// The owned <c>kafka_consumer_ConsumerGroupMetadata_t</c> a <see cref="Consumer.ConsumerGroupMetadata"/> keeps, so
/// that <c>SendOffsetsToTransaction</c> can pass it to the core. It holds no reference to the consumer it came from, and
/// is destroyed once, when the handle is released (by the finalizer, as the public class is not disposable).
/// </summary>
internal sealed class SafeConsumerGroupMetadataHandle : SafeHandleZeroIsInvalid
{
    internal SafeConsumerGroupMetadataHandle(IntPtr metadata)
    {
        SetHandle(metadata);
    }

    protected override bool ReleaseHandle()
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerGroupMetadataDestroy(handle, ref outErr);
        if (outErr == IntPtr.Zero)
        {
            return true;
        }

        NativeMethods.ErrorDestroy(outErr);
        return false;
    }
}
