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
/// The type of a group — the .NET realization of Java's
/// <c>org.apache.kafka.common.GroupType</c> (<c>GroupType.java:25-30</c>).
/// </summary>
/// <remarks>
/// <para>
/// All five Java constants ship, in Java's declaration order. <c>UNKNOWN</c> is a real
/// member of the set, not an error sentinel: a broker newer than this client can name a
/// type this client does not know, and Java's <c>parse</c> maps that to <c>UNKNOWN</c>
/// rather than failing (<c>:44-50</c>).
/// </para>
/// <para>
/// ⚠ <b>The name is the contract, not an ordinal.</b> Java's <c>GroupType</c> carries no
/// numeric id — the ABI header says so in as many words — so the value crossing the
/// boundary is Java's <c>toString()</c> spelling (<c>"Consumer"</c>, <c>"Classic"</c>,
/// <c>"Share"</c>, <c>"Streams"</c>, <c>"Unknown"</c>), read and written by
/// <c>Confluent.Kafka.Internal.Interop.GroupMarshal</c>. The underlying <c>int</c> of
/// these members is therefore meaningless outside this assembly; do not persist it.
/// </para>
/// <para>
/// <b>Java's <c>parse(String)</c> is deliberately not published.</b> It exists so Java can
/// decode the wire name, which is this binding's marshalling concern and not the caller's:
/// every type reaching a caller has already been decoded. Publishing it would add public
/// surface whose only input is a wire spelling the caller never sees
/// (<c>definition-of-done.md</c> §7).
/// </para>
/// </remarks>
public enum GroupType
{
    /// <summary>
    /// The type is not known to this client — Java's <c>UNKNOWN</c> (<c>:26</c>), the
    /// value <c>parse</c> yields for a name it does not recognise.
    /// </summary>
    Unknown,

    /// <summary>A consumer group using the KIP-848 protocol — Java's <c>CONSUMER</c> (<c>:27</c>).</summary>
    Consumer,

    /// <summary>A consumer group using the classic protocol — Java's <c>CLASSIC</c> (<c>:28</c>).</summary>
    Classic,

    /// <summary>A share group — Java's <c>SHARE</c> (<c>:29</c>).</summary>
    Share,

    /// <summary>A Kafka Streams group — Java's <c>STREAMS</c> (<c>:30</c>).</summary>
    Streams,
}
