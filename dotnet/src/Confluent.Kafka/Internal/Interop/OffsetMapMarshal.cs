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
using System.Collections.Generic;
using Confluent.Kafka.Common;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>Copies a <c>kafka_Map_t</c> of <c>TopicPartition_t</c> to <c>OffsetAndMetadata_t</c> into a managed dictionary.</summary>
internal static class OffsetMapMarshal
{
    internal static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> CopyOutAndDestroy(
        IntPtr map, (string Topic, int Partition)[]? requested)
    {
        try
        {
            return OffsetMapMarshalShared.EveryRequested(CopyOut(map), requested);
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.MapDestroy(map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    internal static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> CopyOut(IntPtr map)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(map, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (count <= 0)
        {
            return EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance;
        }

        Dictionary<TopicPartition, OffsetAndMetadata> result = new Dictionary<TopicPartition, OffsetAndMetadata>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr key = NativeMethods.MapKey(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.MapValue(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (key == IntPtr.Zero || value == IntPtr.Zero)
            {
                continue;
            }

            result[OffsetMapMarshalShared.CopyKey(key)] = CopyOutValue(value);
        }

        return result;
    }

    internal static IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> CopyOutNullable(IntPtr map)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(map, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        if (count <= 0)
        {
            return EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>.Instance;
        }

        Dictionary<TopicPartition, OffsetAndMetadata?> result = new Dictionary<TopicPartition, OffsetAndMetadata?>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr key = NativeMethods.MapKey(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.MapValue(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (key == IntPtr.Zero)
            {
                continue;
            }

            result[OffsetMapMarshalShared.CopyKey(key)] = value == IntPtr.Zero ? null : CopyOutValue(value);
        }

        return result;
    }

    internal static OffsetAndMetadata CopyOutValue(IntPtr offsetAndMetadata)
    {
        IntPtr outErr = IntPtr.Zero;
        long offset = NativeMethods.OffsetAndMetadataOffset(offsetAndMetadata, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        string metadata = Utf8Marshal.PtrToString(NativeMethods.OffsetAndMetadataMetadata(offsetAndMetadata, ref outErr)) ?? string.Empty;
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int epoch = NativeMethods.OffsetAndMetadataLeaderEpoch(offsetAndMetadata, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new OffsetAndMetadata(offset, metadata, OffsetMapMarshalShared.ReadLeaderEpoch(epoch >= 0, epoch));
    }
}
