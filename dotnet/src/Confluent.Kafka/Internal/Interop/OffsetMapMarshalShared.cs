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
/// The bits shared by the three offset-map copy-out marshallers
/// (<see cref="OffsetMapMarshal"/> / <see cref="OffsetAndTimestampMapMarshal"/> /
/// <see cref="LongOffsetMapMarshal"/>): copying a borrowed <c>TopicPartition_t</c> map
/// key into an owned <see cref="TopicPartition"/>, and mapping the ABI's leader-epoch
/// presence flag to a nullable <see cref="int"/>. Every map has the same key shape, so
/// this avoids repeating the key copy-out and the presence-flag decode three times.
/// </summary>
internal static class OffsetMapMarshalShared
{
    /// <summary>
    /// Copies a <b>borrowed</b> (Category-4) <c>TopicPartition_t</c> map key into an
    /// owned <see cref="TopicPartition"/>. The topic is a NUL-terminated, handle-owned
    /// <c>const char*</c> (§B3 NUL-scan form), copied out before the owning map root is
    /// destroyed. Never frees the borrowed key.
    /// </summary>
    internal static TopicPartition CopyKey(IntPtr keyPtr)
    {
        string topic = Utf8Marshal.PtrToString(NativeMethods.TopicPartitionTopic(keyPtr)) ?? string.Empty;
        int partition = NativeMethods.TopicPartitionPartition(keyPtr);
        return new TopicPartition(topic, partition);
    }

    /// <summary>
    /// Maps the ABI leader-epoch presence flag to a nullable epoch: <paramref name="present"/>
    /// <c>false</c> ⇒ <see langword="null"/> (absent), <c>true</c> ⇒
    /// <paramref name="epoch"/>. Honors the presence flag rather than hardcoding the
    /// out-value (PLAN §1 Critic check).
    /// </summary>
    internal static int? ReadLeaderEpoch(bool present, int epoch) => present ? epoch : (int?)null;
}
