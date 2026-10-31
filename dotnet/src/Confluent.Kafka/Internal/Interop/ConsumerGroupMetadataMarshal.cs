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
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies an owned <c>ConsumerGroupMetadata_t</c> into a <see cref="ConsumerGroupMetadata"/> and destroys it.
/// </summary>
internal static class ConsumerGroupMetadataMarshal
{
    internal static ConsumerGroupMetadata CopyOutAndDestroy(IntPtr metadata)
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
            string groupId = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupId(metadata, ref outErr)) ?? string.Empty;
            KafkaException.ThrowIfFfiFailed(ref outErr);
            int generationId = NativeMethods.ConsumerGroupMetadataGenerationId(metadata, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            string memberId = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataMemberId(metadata, ref outErr)) ?? string.Empty;
            KafkaException.ThrowIfFfiFailed(ref outErr);
            string? groupInstanceId = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupInstanceId(metadata, ref outErr));
            KafkaException.ThrowIfFfiFailed(ref outErr);

            return new ConsumerGroupMetadata(groupId, generationId, memberId, groupInstanceId);
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ConsumerGroupMetadataDestroy(metadata, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }
}
