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

namespace Confluent.Kafka.Common;

/// <summary>
/// The kind of leader election to conduct — Java's <c>org.apache.kafka.common.ElectionType</c>,
/// passed to <see cref="Admin.IAdmin.ElectLeaders"/>.
/// </summary>
/// <remarks>
/// <para>
/// Each member's value is Java's <c>ElectionType.value</c> byte, not an ordinal. The value does
/// not cross the C ABI: the call passes the matching <c>kafka_common_ElectionType_t</c>
/// singleton.
/// </para>
/// <para>
/// Java's <c>value</c> field and <c>valueOf(byte)</c> factory are not ported: a cast gives the
/// value. Where Java's <c>valueOf</c> throws <c>IllegalArgumentException</c> for another byte,
/// <see cref="Admin.IAdmin.ElectLeaders"/> throws <see cref="System.ArgumentOutOfRangeException"/>
/// for a value cast in from outside the two members.
/// </para>
/// </remarks>
public enum ElectionType
{
    /// <summary>
    /// Elect the preferred replica — the first replica in the partition's assignment —
    /// as leader. Java's <c>PREFERRED</c> (value 0).
    /// </summary>
    Preferred = 0,

    /// <summary>
    /// Elect any in-sync-or-not replica as leader, accepting possible data loss. Java's
    /// <c>UNCLEAN</c> (value 1).
    /// </summary>
    Unclean = 1,
}
