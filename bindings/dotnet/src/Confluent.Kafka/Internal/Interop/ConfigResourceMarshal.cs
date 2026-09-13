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
/// Turns a <c>ConfigResource.Type.id()</c> code coming out of the ABI into a
/// <see cref="ConfigResourceType"/>. <b>Route an ABI resource-type id through here rather
/// than casting it at the reader</b> — see the remarks for what a cast gets wrong.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Two different unknowns arrive on the same accessor, and merging them is the
/// defect.</b> A <c>get_type(i)</c> of <c>-1</c> is the ABI's <em>index out of range</em>
/// return — a value no Java <c>ConfigResource.Type</c> has. It must not become
/// <see cref="ConfigResourceType.Unknown"/>, which is <c>0</c> and is a real Java member
/// (<c>ConfigResource.java:41</c>): a genuine <c>UNKNOWN</c> resource and a read past the
/// end would then be indistinguishable. A <b>non-negative</b> id with no member is the
/// other case entirely — a type a newer broker knows and this client does not — and Java
/// degrades it to <c>UNKNOWN</c> rather than failing
/// (<c>ConfigResource.java:57-59</c>: <c>TYPES.getOrDefault(id, UNKNOWN)</c>).
/// </para>
/// <para>
/// <b>Extracted rather than inlined at each reader (M15/P3 round 1, finding 69.2).</b>
/// The mapping is reached from <c>listConfigResources</c>' element reader today and from
/// the config RPCs' composite <em>key</em> readers next; open-coding it per reader is how
/// one of them ends up a raw cast with no <c>forId</c> fallback, handing a caller an
/// unnamed enum value where Java yields <c>UNKNOWN</c>. Same argument as
/// <see cref="AuthorizedOperationsMarshal"/>: state the rule once so the sibling readers
/// are consistent by construction rather than by imitation.
/// </para>
/// </remarks>
internal static class ConfigResourceMarshal
{
    /// <summary>
    /// Java's <c>ConfigResource.Type.forId</c>, plus a guard for the ABI's own
    /// out-of-range return.
    /// </summary>
    /// <param name="id">
    /// The <c>ConfigResource.Type.id()</c> code the ABI produced for one index.
    /// </param>
    /// <returns>
    /// The matching member, or <see cref="ConfigResourceType.Unknown"/> for a
    /// non-negative id this client has no member for.
    /// </returns>
    /// <exception cref="KafkaException">
    /// <paramref name="id"/> is negative — the ABI's "index out of range" return, for an
    /// index the caller took from the result's own <c>count</c>. Unreachable in practice;
    /// throwing rather than inventing a type is the same defensive treatment
    /// <see cref="KeyedResultMarshal.ReadStringKey"/> gives a missing key. The throw
    /// propagates to the trampoline's no-throw boundary, which faults the awaiter.
    /// </exception>
    internal static ConfigResourceType TypeFromId(int id)
    {
        if (id < 0)
        {
            throw new KafkaException(
                "The admin result produced no resource type for an index within its own count.");
        }

        return Enum.IsDefined(typeof(ConfigResourceType), id)
            ? (ConfigResourceType)id
            : ConfigResourceType.Unknown;
    }
}
