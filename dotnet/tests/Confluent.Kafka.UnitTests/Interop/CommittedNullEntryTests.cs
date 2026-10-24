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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <c>Committed</c> gives every requested partition an entry, <see langword="null"/> where it has no committed offset,
/// as Java's <c>KafkaConsumer.committed</c> does, while the mocks keep Java's <c>MockConsumer.committed</c>, which
/// leaves such a partition out.
/// </summary>
public sealed class CommittedNullEntryTests
{
    private const string Topic = "committed-null-entry-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition s_tp1 = new TopicPartition(Topic, 1);

    private static readonly (string Topic, int Partition)[] s_requested = { (Topic, 0), (Topic, 1) };

    [Fact]
    public void NoCommittedOffsets_TheRealConsumer_GivesThePartitionWithoutOneANullEntry()
    {
        NativeConsumer mock = MockWithTp0Committed(7);
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);
        NativeConsumer real = NativeConsumer.Create(RealConfig(), withPump: false);
        using IDisposable realDisposal = TestTimeout.Disposal(real);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
            real.CopyCommitted(CommittedRoot(mock), s_requested);

        Assert.Equal(2, committed.Count);
        Assert.Equal(new OffsetAndMetadata(7), committed[s_tp0]);
        Assert.True(committed.ContainsKey(s_tp1));
        Assert.Null(committed[s_tp1]);
    }

    [Fact]
    public void NoCommittedOffsets_TheMock_LeavesThePartitionWithoutOneOut()
    {
        NativeConsumer mock = MockWithTp0Committed(7);
        using IDisposable mockDisposal = TestTimeout.Disposal(mock);

        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
            mock.CopyCommitted(CommittedRoot(mock), s_requested);

        KeyValuePair<TopicPartition, OffsetAndMetadata?> entry = Assert.Single(committed);
        Assert.Equal(s_tp0, entry.Key);
        Assert.Equal(new OffsetAndMetadata(7), entry.Value);
    }

    [Fact]
    public void MockConsumer_Committed_ReportsOnlyCommittedPartitions_AndZeroWhenUnassigned()
    {
        MockConsumer<byte[], byte[]> consumer =
            new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0, s_tp1 });
        consumer.Commit(new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(4) });

        AssertOnlyTp0(consumer.Committed(new[] { s_tp0, s_tp1 }), 4);
        AssertOnlyTp0(consumer.Committed(new[] { s_tp0, s_tp1 }, TimeSpan.FromSeconds(5)), 4);

        consumer.Assign(new[] { s_tp0 });
        Assert.Empty(consumer.Committed(new[] { s_tp0 }));
    }

    [Fact]
    public async Task AsyncMockConsumer_Committed_ReportsOnlyCommittedPartitions_AndZeroWhenUnassigned()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0, s_tp1 }), s_deadline);
            await TestTimeout.Run(
                () => consumer.Commit(
                    new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(4) }),
                s_deadline);

            AssertOnlyTp0(await TestTimeout.Run(() => consumer.Committed(new[] { s_tp0, s_tp1 }), s_deadline), 4);
            AssertOnlyTp0(
                await TestTimeout.Run(
                    () => consumer.Committed(new[] { s_tp0, s_tp1 }, TimeSpan.FromSeconds(5)), s_deadline),
                4);

            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            Assert.Empty(await TestTimeout.Run(() => consumer.Committed(new[] { s_tp0 }), s_deadline));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    private static void AssertOnlyTp0(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed, long offset)
    {
        KeyValuePair<TopicPartition, OffsetAndMetadata?> entry = Assert.Single(committed);
        Assert.Equal(s_tp0, entry.Key);
        Assert.Equal(new OffsetAndMetadata(offset), entry.Value);
    }

    private static NativeConsumer MockWithTp0Committed(long offset)
    {
        NativeConsumer mock = NativeConsumer.CreateMock(withPump: false);
        try
        {
            mock.Assign(new[] { s_tp0, s_tp1 });
            mock.CommitSyncOffsets(
                new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) });
            return mock;
        }
        catch
        {
            TestTimeout.Dispose(mock);
            throw;
        }
    }

    private static IntPtr CommittedRoot(NativeConsumer mock)
    {
        using NativeCallArguments arguments = new NativeCallArguments();
        IntPtr outErr = IntPtr.Zero;
        IntPtr map = NativeMethods.ConsumerCommitted(mock.Handle, arguments.TopicPartitionList(s_requested), ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return map;
    }

    private static Dictionary<string, string> RealConfig() => new()
    {
        ["bootstrap.servers"] = "127.0.0.1:1",
        ["group.protocol"] = "consumer",
        ["group.id"] = "committed-null-entry-group",
    };
}
