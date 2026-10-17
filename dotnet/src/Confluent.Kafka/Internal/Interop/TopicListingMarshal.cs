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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a borrowed <c>kafka_admin_TopicListing_t</c> into a fully owned managed
/// <see cref="TopicListing"/>, so nothing survives the
/// <c>ListTopicsResult_destroy</c> that follows the walk (ffi §B2 Category 4 / §B4).
/// </summary>
internal static class TopicListingMarshal
{
    /// <summary>Copies one borrowed listing out.</summary>
    /// <param name="listing">
    /// The borrowed <c>get_value(i)</c> pointer. Valid only until the result root is
    /// destroyed.
    /// </param>
    /// <returns>The owned listing.</returns>
    /// <exception cref="KafkaException">
    /// The result reported a listing the ABI could not produce — impossible within the
    /// result's own <c>count</c>, but surfaced rather than dereferenced.
    /// </exception>
    internal static TopicListing CopyOut(IntPtr listing)
    {
        if (listing == IntPtr.Zero)
        {
            // Guarded by `count` at the call site, so unreachable; the aggregate shape has
            // no per-key channel, so this faults the whole listTopics task rather than
            // silently dropping a topic from the map.
            throw new KafkaException("The listTopics result produced no listing for an index within its own count.");
        }

        // NUL-terminated, borrowed (ffi §B3 row 2) — copied out here.
        string name = Utf8Marshal.PtrToString(NativeMethods.TopicListingName(listing)) ?? string.Empty;

        string topicIdText =
            Utf8Marshal.PtrToString(NativeMethods.TopicListingTopicId(listing)) ?? string.Empty;

        // Same reading as TopicDescriptionMarshal: an empty id is the metadata-unavailable
        // spelling and becomes Uuid.Zero (Java's ZERO_UUID default), while a genuinely
        // malformed one throws out of Parse.
        Uuid topicId = topicIdText.Length == 0 ? Uuid.Zero : Uuid.Parse(topicIdText);

        bool isInternal = NativeMethods.TopicListingIsInternal(listing);

        return new TopicListing(name, topicId, isInternal);
    }
}
