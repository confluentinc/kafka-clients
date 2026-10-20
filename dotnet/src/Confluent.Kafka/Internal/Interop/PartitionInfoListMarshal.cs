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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a <c>kafka_List_t</c> of <c>kafka_common_PartitionInfo_t</c>, or a
/// <c>kafka_common_PartitionInfoList_t</c>, into an owned list of <see cref="PartitionInfo"/>, each element through
/// <see cref="PartitionInfoMarshal"/>; only <see cref="CopyOutAndDestroy"/> frees the list.
/// </summary>
internal static class PartitionInfoListMarshal
{
    internal static IReadOnlyList<PartitionInfo> CopyOut(IntPtr list)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.ListSize(list, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<PartitionInfo> result = new List<PartitionInfo>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr info = NativeMethods.ListGet(list, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (info != IntPtr.Zero)
            {
                result.Add(PartitionInfoMarshal.CopyOut(info));
            }
        }

        return result;
    }

    internal static IReadOnlyList<PartitionInfo> CopyOutAndDestroy(IntPtr list)
    {
        try
        {
            return CopyOut(list);
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ListDestroy(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    internal static IReadOnlyList<PartitionInfo> CopyOutPartitionInfoList(IntPtr list)
    {
        int count = NativeMethods.PartitionInfoListCount(list);
        if (count <= 0)
        {
            return Array.Empty<PartitionInfo>();
        }

        List<PartitionInfo> result = new List<PartitionInfo>(count);
        for (int i = 0; i < count; i++)
        {
            IntPtr info = NativeMethods.PartitionInfoListGet(list, i);
            if (info == IntPtr.Zero)
            {
                continue;
            }

            result.Add(PartitionInfoMarshal.CopyOut(info));
        }

        return result;
    }
}
