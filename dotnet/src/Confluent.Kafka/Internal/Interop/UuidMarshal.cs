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
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a <c>kafka_common_Uuid_t</c> into a <see cref="Uuid"/>, destroying an owned one and leaving one a future owns.
/// </summary>
internal static class UuidMarshal
{
    internal static Uuid CopyOut(IntPtr uuid)
    {
        if (uuid == IntPtr.Zero)
        {
            return Uuid.Zero;
        }

        IntPtr outErr = IntPtr.Zero;
        long mostSignificantBits = NativeMethods.UuidMostSignificantBits(uuid, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long leastSignificantBits = NativeMethods.UuidLeastSignificantBits(uuid, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new Uuid(mostSignificantBits, leastSignificantBits);
    }

    internal static Uuid CopyOutAndDestroy(IntPtr uuid)
    {
        if (uuid == IntPtr.Zero)
        {
            return Uuid.Zero;
        }

        try
        {
            return CopyOut(uuid);
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.UuidDestroy(uuid, ref destroyErr);
            if (destroyErr != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(destroyErr);
            }
        }
    }
}
