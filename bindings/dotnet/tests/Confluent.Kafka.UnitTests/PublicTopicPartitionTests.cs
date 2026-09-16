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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public <see cref="TopicPartition"/> value-type constructor guard (M7/P2a) — the single
/// authoritative test for the negative-partition rejection. It is a pure value-type check with
/// <b>no consumer interaction</b>: the ctor validates its argument before any TopicPartition can
/// reach a consumer op, so a negative partition can never be smuggled into <c>Position</c> /
/// <c>Seek</c> / <c>Commit</c> / <c>Assign</c> / the query family through a constructed
/// <see cref="TopicPartition"/>. This consolidates the eight per-op copies that each re-asserted
/// this same ctor guard (each had zero consumer interaction).
/// </summary>
/// <remarks>
/// Asserts the <b>superset</b> of every consolidated copy (verified against
/// <c>TopicPartition.cs:55-56</c>: <c>new ArgumentOutOfRangeException(nameof(partition), partition,
/// "Partition must not be negative.")</c>): both the <see cref="ArgumentException.ParamName"/> and
/// the exact message content (DoD §3 — error-message content is part of the behavioral contract).
/// The distinct interop-layer guard (<c>NativeConsumer.Seek(partition:-1)</c>) is a different code
/// path and remains covered by
/// <c>ConsumerUnsubscribeSeekGroupMetadataTests.Seek_NegativePartition_ThrowsArgumentOutOfRange</c>.
/// </remarks>
public sealed class PublicTopicPartitionTests
{
    [Fact]
    public void NegativePartition_Throws()
    {
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => new TopicPartition("t", -1));

        // Superset of every consolidated per-op copy: ParamName AND message content.
        Assert.Equal("partition", ex.ParamName);
        Assert.Contains("Partition must not be negative.", ex.Message, StringComparison.Ordinal);
    }
}
