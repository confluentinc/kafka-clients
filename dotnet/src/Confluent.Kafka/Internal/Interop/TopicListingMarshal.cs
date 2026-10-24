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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies the borrowed <c>kafka_Map_t</c> of topic names to <c>kafka_admin_TopicListing_t</c> that a
/// <c>namesToListings</c> future delivers into an owned managed map, in the core's (name) order.
/// </summary>
internal static class TopicListingMarshal
{
    internal static IReadOnlyDictionary<string, TopicListing> CopyOutMap(IntPtr map)
    {
        Dictionary<string, TopicListing> listings = new Dictionary<string, TopicListing>(StringComparer.Ordinal);
        if (map == IntPtr.Zero)
        {
            return listings;
        }

        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(map, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        for (int i = 0; i < count; i++)
        {
            IntPtr key = NativeMethods.MapKey(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.MapValue(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            listings[Utf8Marshal.PtrToString(key) ?? string.Empty] = CopyOut(value);
        }

        return listings;
    }

    internal static TopicListing CopyOut(IntPtr listing)
    {
        IntPtr outErr = IntPtr.Zero;
        string name = Utf8Marshal.PtrToString(NativeMethods.TopicListingName(listing, ref outErr)) ?? string.Empty;
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr topicId = NativeMethods.TopicListingTopicId(listing, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Uuid id = UuidMarshal.CopyOutAndDestroy(topicId);
        sbyte isInternal = NativeMethods.TopicListingIsInternal(listing, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new TopicListing(name, id, isInternal != 0);
    }
}
