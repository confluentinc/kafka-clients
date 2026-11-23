// Copyright 2026 Confluent Inc.
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
using System.Linq;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Consumer;

using Xunit;

using ConsumerOffsetOutOfRangeException = Confluent.Kafka.Consumer.OffsetOutOfRangeException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The exceptions that take a <see cref="TopicPartition"/> keep a <see langword="null"/> one, alone or as a map key,
/// as Java's do: the getters return what was given, and the message Java builds from it prints <c>null</c>.
/// <c>NoOffsetForPartitionException</c>'s collection form refuses a null element as Java's <c>Set.copyOf</c> does
/// (<c>GeneratedErrorClassesTests</c>).
/// </summary>
public sealed class PublicExceptionNullPartitionTests
{
    private static readonly TopicPartition s_tp0 = new TopicPartition("np", 0);

    [Fact]
    public void RecordDeserializationException_ANullPartition_IsKept()
    {
        RecordDeserializationException e = new RecordDeserializationException(
            RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
            null!,
            5,
            7,
            TimestampType.CREATE_TIME,
            null,
            new byte[] { 1 },
            null,
            "bad value",
            null);

        Assert.Null(e.TopicPartition());
        Assert.Equal(5, e.Offset());
        Assert.Equal("bad value", e.Message);
        Assert.Contains("bad value", e.ToString(), StringComparison.Ordinal);
    }

    // Java's Collections.singleton(null) keeps the null, and the message prints it.
    [Fact]
    public void NoOffsetForPartitionException_ANullPartition_IsKept_AndPrintsNull()
    {
        NoOffsetForPartitionException e = new NoOffsetForPartitionException((TopicPartition)null!);

        Assert.Equal("Undefined offset with no reset policy for partition: null", e.Message);
        Assert.Equal(new TopicPartition[] { null! }, e.Partitions());
        Assert.Contains(e.Message, e.ToString(), StringComparison.Ordinal);
    }

    // Java keeps the caller's map, and keySet() holds its null key.
    [Fact]
    public void OffsetOutOfRangeException_ANullKey_IsKept_AndPrintsNull()
    {
        EntryMap<TopicPartition, long> offsets = new EntryMap<TopicPartition, long>(
            new KeyValuePair<TopicPartition, long>(s_tp0, 5),
            new KeyValuePair<TopicPartition, long>(null!, 6));

        ConsumerOffsetOutOfRangeException built = new ConsumerOffsetOutOfRangeException(offsets);
        ConsumerOffsetOutOfRangeException given = new ConsumerOffsetOutOfRangeException("out of range", offsets);

        Assert.Equal(
            "Offsets out of range with no configured reset policy for partitions: {np-0=5, null=6}", built.Message);
        Assert.Equal("out of range", given.Message);
        foreach (ConsumerOffsetOutOfRangeException e in new[] { built, given })
        {
            Assert.Same(offsets, e.OffsetOutOfRangePartitions());
            Assert.Equal(new TopicPartition[] { s_tp0, null! }, e.Partitions().ToArray());
            Assert.Contains(e.Message, e.ToString(), StringComparison.Ordinal);
        }
    }

    [Fact]
    public void LogTruncationException_ANullKey_IsKept_AndPrintsNull()
    {
        EntryMap<TopicPartition, long> fetchOffsets =
            new EntryMap<TopicPartition, long>(new KeyValuePair<TopicPartition, long>(null!, 5));
        EntryMap<TopicPartition, OffsetAndMetadata> divergentOffsets = new EntryMap<TopicPartition, OffsetAndMetadata>(
            new KeyValuePair<TopicPartition, OffsetAndMetadata>(null!, new OffsetAndMetadata(6)));

        LogTruncationException built = new LogTruncationException(fetchOffsets, divergentOffsets);
        LogTruncationException given = new LogTruncationException("truncated", fetchOffsets, divergentOffsets);

        Assert.Equal(
            "Truncated partitions detected with divergent offsets "
            + "{null=OffsetAndMetadata{offset=6, leaderEpoch=null, metadata=''}}",
            built.Message);
        Assert.Equal("truncated", given.Message);
        foreach (LogTruncationException e in new[] { built, given })
        {
            Assert.Same(divergentOffsets, e.DivergentOffsets());
            Assert.Same(fetchOffsets, e.OffsetOutOfRangePartitions());
            Assert.Equal(new TopicPartition[] { null! }, e.Partitions().ToArray());
            Assert.Contains(e.Message, e.ToString(), StringComparison.Ordinal);
        }
    }

    [Fact]
    public void RecordTooLargeException_ANullKey_IsKept()
    {
        EntryMap<TopicPartition, long> partitions =
            new EntryMap<TopicPartition, long>(new KeyValuePair<TopicPartition, long>(null!, 5));

        RecordTooLargeException e = new RecordTooLargeException("too large", partitions);

        Assert.Same(partitions, e.RecordTooLargePartitions());
        Assert.Equal("too large", e.Message);
        Assert.Contains("too large", e.ToString(), StringComparison.Ordinal);
    }
}
