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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The supported consumer group protocols — Java's <c>org.apache.kafka.clients.consumer.GroupProtocol</c>.
/// <c>ToString()</c> is Java's <c>name</c>.
/// </summary>
public enum GroupProtocol
{
    /// <summary>The Classic consumer group protocol (pre KIP-848) — Java's <c>CLASSIC</c>.</summary>
    CLASSIC,

    /// <summary>The Consumer rebalance protocol (KIP-848) — Java's <c>CONSUMER</c>.</summary>
    CONSUMER,
}

/// <summary>
/// The static method Java declares on the <see cref="GroupProtocol"/> enum itself, which a C# enum cannot carry.
/// From C# 14 it is called on the enum (<c>GroupProtocol.Of(…)</c>); older C# calls it on this class.
/// </summary>
public static class GroupProtocolMethods
{
    private const string JavaClass = "org.apache.kafka.clients.consumer.GroupProtocol";

    extension(GroupProtocol)
    {
        /// <summary>Case-insensitive group protocol lookup by name — Java's <c>of(String)</c>.</summary>
        /// <param name="name">The name of the group protocol.</param>
        /// <returns>The corresponding group protocol.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
        /// <exception cref="IllegalArgumentException">
        /// The name does not match any protocol, with Java's message
        /// "No enum constant org.apache.kafka.clients.consumer.GroupProtocol.&lt;NAME&gt;".
        /// </exception>
        public static GroupProtocol Of(string name)
        {
            if (name is null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            string upper = JavaText.ToUpperRoot(name);
            return upper switch
            {
                nameof(GroupProtocol.CLASSIC) => GroupProtocol.CLASSIC,
                nameof(GroupProtocol.CONSUMER) => GroupProtocol.CONSUMER,
                _ => throw new IllegalArgumentException("No enum constant " + JavaClass + "." + upper),
            };
        }
    }
}
