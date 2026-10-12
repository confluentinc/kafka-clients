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
/// Copies a flat transient <c>kafka_producer_RecordMetadata_t</c> handle (ffi §A2 Category 2)
/// into an owned managed <see cref="RecordMetadata"/> via the per-field accessors — the default
/// over the <c>RecordMetadata_copy</c> callback (PLAN §6.1). The topic is a NUL-terminated
/// <c>const char*</c> owned by the handle, copied out with <see cref="Utf8Marshal.PtrToString(IntPtr)"/>
/// (NUL-scan form, §A3) <b>before</b> the caller destroys the handle; the result holds only
/// copied values and never references native memory.
/// </summary>
internal static class RecordMetadataMarshal
{
    /// <summary>
    /// Reads offset / partition / topic / timestamp out of <paramref name="metadata"/> (a
    /// non-null <c>RecordMetadata_t</c> handle) into an owned <see cref="RecordMetadata"/>. Does
    /// <b>not</b> destroy the handle — the caller frees it with
    /// <see cref="NativeMethods.RecordMetadataDestroy(IntPtr)"/> after this returns.
    /// </summary>
    internal static RecordMetadata CopyOut(IntPtr metadata)
    {
        long offset = NativeMethods.RecordMetadataOffset(metadata);
        int partition = NativeMethods.RecordMetadataPartition(metadata);

        // Copy the topic out BEFORE the handle is destroyed (the borrowed const char* dies with
        // the handle, ffi §A3). A defensive null → empty string keeps RecordMetadata.Topic
        // non-null (the ABI returns a valid pointer for a non-null handle).
        string topic = Utf8Marshal.PtrToString(NativeMethods.RecordMetadataTopic(metadata)) ?? string.Empty;

        long timestamp = NativeMethods.RecordMetadataTimestamp(metadata);

        return new RecordMetadata(topic, partition, offset, timestamp);
    }
}
