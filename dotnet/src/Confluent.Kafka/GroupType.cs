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

namespace Confluent.Kafka;

/// <summary>
/// The type of a group — Java's <c>org.apache.kafka.common.GroupType</c>.
/// </summary>
/// <remarks>
/// <para>
/// Java's five constants, in Java's order. <see cref="Unknown"/> is a real member, not an
/// error: a broker newer than this client can name a type this client does not know.
/// </para>
/// <para>
/// The value crossing the C ABI is Java's <c>toString()</c> spelling (<c>"Consumer"</c>,
/// <c>"Classic"</c>, <c>"Share"</c>, <c>"Streams"</c>, <c>"Unknown"</c>), not the underlying
/// <c>int</c>, which means nothing outside this assembly; do not persist it.
/// </para>
/// <para>
/// Java's <c>parse(String)</c> is not published (a recorded deviation): it decodes a wire
/// spelling the caller never sees.
/// </para>
/// </remarks>
public enum GroupType
{
    /// <summary>
    /// The type is not known to this client — Java's <c>UNKNOWN</c>, the type for a name this
    /// client does not recognise.
    /// </summary>
    Unknown,

    /// <summary>A consumer group using the KIP-848 protocol — Java's <c>CONSUMER</c>.</summary>
    Consumer,

    /// <summary>A consumer group using the classic protocol — Java's <c>CLASSIC</c>.</summary>
    Classic,

    /// <summary>A share group — Java's <c>SHARE</c>.</summary>
    Share,

    /// <summary>A Kafka Streams group — Java's <c>STREAMS</c>.</summary>
    Streams,
}
