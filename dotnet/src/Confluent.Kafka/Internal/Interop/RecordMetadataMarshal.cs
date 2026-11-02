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
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a borrowed <c>kafka_producer_RecordMetadata_t</c> into an owned <see cref="RecordMetadata"/> through its
/// per-field readers; it never frees the handle.
/// </summary>
internal static class RecordMetadataMarshal
{
    internal static RecordMetadata CopyOut(IntPtr metadata)
    {
        IntPtr outErr = IntPtr.Zero;
        long offset = NativeMethods.RecordMetadataOffset(metadata, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int partition = NativeMethods.RecordMetadataPartition(metadata, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr topic = NativeMethods.RecordMetadataTopic(metadata, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long timestamp = NativeMethods.RecordMetadataTimestamp(metadata, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);

        return new RecordMetadata(Utf8Marshal.PtrToString(topic) ?? string.Empty, partition, offset, timestamp);
    }
}
