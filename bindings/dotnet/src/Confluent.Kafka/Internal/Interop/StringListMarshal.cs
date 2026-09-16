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
/// Marshals an owned (Category-3) <c>StringList_t</c> borrow-root into an owned
/// <see cref="string"/> snapshot and then destroys it (ffi-marshalling.md §B2/§B3). The
/// list is the borrow-root; each string from <c>StringList_get</c> is a Category-4
/// <b>borrowed view</b> (owned by the list, never freed on its own), so every string is
/// copied out <b>before</b> the root is destroyed. The sibling of
/// <see cref="TopicPartitionListMarshal"/>; it needs no <c>unsafe</c> (the NUL-terminated
/// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> reads are safe managed API).
/// </summary>
internal static class StringListMarshal
{
    /// <summary>
    /// Reads every string off the owned <paramref name="list"/> borrow-root — each a
    /// NUL-terminated <c>const char*</c> (§B3, NOT the length-delimited receive-path form)
    /// — into an owned <see cref="string"/> array, then frees the root exactly once in a
    /// <c>finally</c> (even if a read throws). The borrowed string pointers are copied out
    /// before the destroy — they die with the root (§B2 Category 4). The result is an
    /// immutable owned snapshot, matching Java's "returns a copy" contract.
    /// </summary>
    /// <param name="list">The owned, non-null string-list handle.</param>
    internal static IReadOnlyCollection<string> CopyOutAndDestroy(IntPtr list)
    {
        try
        {
            int count = NativeMethods.StringListCount(list);
            if (count <= 0)
            {
                // Shared empty instance — matches the sibling marshallers
                // (TopicPartitionListMarshal, OffsetMapMarshal, ...) and avoids a negative
                // count reaching `new string[count]`, which throws OverflowException.
                return Array.Empty<string>();
            }

            string[] result = new string[count];
            for (int i = 0; i < count; i++)
            {
                // Borrowed string (Category 4) — owned by the list, never freed on its
                // own; copy it out BEFORE _destroy (§B2/§B3).
                result[i] = Utf8Marshal.PtrToString(NativeMethods.StringListGet(list, i)) ?? string.Empty;
            }

            return result;
        }
        finally
        {
            // Owned Category-3 borrow-root — free it exactly once after copy-out (§B2).
            NativeMethods.StringListDestroy(list);
        }
    }
}
