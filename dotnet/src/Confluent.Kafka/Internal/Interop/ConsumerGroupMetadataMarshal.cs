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
/// Marshals an owned (Category-3) <c>ConsumerGroupMetadata_t</c> handle into a public
/// <see cref="ConsumerGroupMetadata"/> and then destroys it (ffi-marshalling.md §B2/§B3).
/// The symmetric sibling of <see cref="ConsumerRecordsMarshal"/> for the group-metadata
/// getter — it keeps the owned-handle read-then-destroy discipline out of
/// <c>NativeConsumer</c> and needs no <c>unsafe</c> (the NUL-terminated
/// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> reads are safe managed API).
/// </summary>
internal static class ConsumerGroupMetadataMarshal
{
    /// <summary>
    /// Reads all four fields off the owned <paramref name="metadata"/> handle and frees
    /// it exactly once in a <c>finally</c> (even if a read throws). The borrowed
    /// NUL-terminated string pointers are copied out <b>before</b> the destroy — they
    /// die with the handle (§B3). A null <c>group_instance_id</c> pointer maps to
    /// <see langword="null"/> (a non-static member).
    /// </summary>
    /// <param name="metadata">The owned, non-null metadata handle.</param>
    internal static ConsumerGroupMetadata CopyOutAndDestroy(IntPtr metadata)
    {
        try
        {
            // Copy every field BEFORE _destroy — the borrowed const char* pointers die
            // with the handle (§B3). The three string accessors are NUL-terminated,
            // handle-owned form; generation_id is a scalar. group_instance_id may be
            // IntPtr.Zero (a non-static member) → PtrToString returns null.
            string groupId = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupId(metadata)) ?? string.Empty;
            int generationId = NativeMethods.ConsumerGroupMetadataGenerationId(metadata);
            string memberId = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataMemberId(metadata)) ?? string.Empty;
            string? groupInstanceId = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupInstanceId(metadata));

            return new ConsumerGroupMetadata(groupId, generationId, memberId, groupInstanceId);
        }
        finally
        {
            // Owned Category-3 handle — free it exactly once after reading (ffi §B2).
            NativeMethods.ConsumerGroupMetadataDestroy(metadata);
        }
    }
}
