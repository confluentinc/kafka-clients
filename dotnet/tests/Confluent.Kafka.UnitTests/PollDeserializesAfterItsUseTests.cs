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
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A non-parallel xUnit collection for <see cref="PollDeserializesAfterItsUseTests"/>, whose listener blocks inside
/// the process-wide <see cref="Trace"/> lock, so that no other class's trace waits for it.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class BlockingTraceCollection
{
    internal const string Name = "blocking-trace";
}

/// <summary>
/// <c>Poll</c> deserializes its records after its use of the consumer has ended, on the sync and the async consumer:
/// a deserializer that disposes the consumer does not deadlock, and after a failing record every partition is sought
/// back in one use of the poll's thread, which another thread's call on a mock waits for.
/// </summary>
[Collection(BlockingTraceCollection.Name)]
public sealed class PollDeserializesAfterItsUseTests
{
    private const string Topic = "poll-after-use-topic";

    private const int Partitions = 3;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Fact]
    public void SyncPoll_ADeserializerThatDisposesTheConsumer_ReturnsTheRecords_AndTheConsumerIsClosed()
    {
        DisposingDeserializer values = new DisposingDeserializer();
        MockConsumer<string?, string?> consumer = new MockConsumer<string?, string?>(new DisposingDeserializer(), values);
        values.Target = consumer;
        try
        {
            consumer.Assign(new[] { s_tp0 });
            consumer.Seek(s_tp0, 0);
            consumer.AddRecord(Topic, 0, 0, key: null, value: Bytes("a"));
            consumer.AddRecord(Topic, 0, 1, key: null, value: Bytes("b"));
        }
        catch
        {
            TestTimeout.Dispose(consumer);
            throw;
        }

        object? outcome = BlockingCall.RunGet(() => consumer.Poll(s_pollTimeout));

        ConsumerRecords<string?, string?> records = Assert.IsType<ConsumerRecords<string?, string?>>(outcome);
        Assert.Equal(new[] { "a", "b" }, records.OrderBy(r => r.Offset).Select(r => r.Value));
        Assert.Equal(2, values.Calls);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public async Task AsyncPoll_ADeserializerThatDisposesTheConsumer_ReturnsTheRecords_AndTheConsumerIsClosed()
    {
        DisposingDeserializer values = new DisposingDeserializer();
        AsyncMockConsumer<string?, string?> consumer =
            new AsyncMockConsumer<string?, string?>(new DisposingDeserializer(), values);
        values.Target = consumer;
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(s_tp0, 0), s_deadline);
            consumer.AddRecord(Topic, 0, 0, key: null, value: Bytes("a"));
            consumer.AddRecord(Topic, 0, 1, key: null, value: Bytes("b"));
        }
        catch
        {
            await TestTimeout.DisposeAsync(consumer);
            throw;
        }

        ConsumerRecords<string?, string?> records =
            await TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline);

        Assert.Equal(new[] { "a", "b" }, records.OrderBy(r => r.Offset).Select(r => r.Value));
        Assert.Equal(2, values.Calls);
        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
    }

    [Fact]
    public void SyncPoll_ADeserializerThatThrows_LeavesThePositionUnchanged()
    {
        MockConsumer<string?, string?> consumer =
            new MockConsumer<string?, string?>(new FailingDeserializer(), new FailingDeserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 3);
        consumer.AddRecord(Topic, 0, 3, key: null, value: Bytes("bad"));
        consumer.AddRecord(Topic, 0, 4, key: null, value: Bytes("good"));

        RecordDeserializationException e = Assert.Throws<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

        Assert.Equal(3L, e.Offset);
        Assert.Equal(3L, consumer.Position(s_tp0));
    }

    [Fact]
    public async Task AsyncPoll_ADeserializerThatThrows_LeavesThePositionUnchanged()
    {
        AsyncMockConsumer<string?, string?> consumer =
            new AsyncMockConsumer<string?, string?>(new FailingDeserializer(), new FailingDeserializer());
        try
        {
            await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
            await TestTimeout.Run(() => consumer.Seek(s_tp0, 3), s_deadline);
            consumer.AddRecord(Topic, 0, 3, key: null, value: Bytes("bad"));
            consumer.AddRecord(Topic, 0, 4, key: null, value: Bytes("good"));

            RecordDeserializationException e = await Assert.ThrowsAsync<RecordDeserializationException>(
                () => TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline));

            Assert.Equal(3L, e.Offset);
            Assert.Equal(3L, await TestTimeout.Run(() => consumer.Position(s_tp0), s_deadline));
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }
    }

    [Fact]
    public void SyncPoll_AnotherThreadsPosition_DuringTheSeekBacks_WaitsForThem_AndEveryPartitionIsRewound()
    {
        string topic = UniqueTopic();
        PartitionDeserializer values = new PartitionDeserializer();
        MockConsumer<byte[], int> consumer = new MockConsumer<byte[], int>(Serdes.ByteArray, values);
        values.Reassign = consumer.Assign;
        try
        {
            SetUp(consumer.Assign, consumer.Seek, consumer.AddRecord, topic);
            long position = -1;

            SeekBackWindow window = RunTheSeekBackWindow(
                topic,
                values,
                () => consumer.Poll(s_pollTimeout),
                probe =>
                {
                    position = consumer.Position(probe);
                    return Task.CompletedTask;
                });

            window.AssertTheOtherCallWaited();
            Assert.Null(window.Failure);
            Assert.Equal(0L, position);
            Assert.All(Kept(topic, values.Failed), kept => Assert.Equal(0L, consumer.Position(kept)));
            AddRecords(consumer.AddRecord, Kept(topic, values.Failed));
            AssertRedelivered(consumer.Poll(s_pollTimeout), topic, values.Failed);
            Assert.Equal(Kept(topic, values.Failed), consumer.Assignment().OrderBy(tp => tp.Partition));
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public void SyncPoll_AnotherThreadsClose_DuringTheSeekBacks_WaitsForThem_ThenCloses()
    {
        string topic = UniqueTopic();
        PartitionDeserializer values = new PartitionDeserializer();
        MockConsumer<byte[], int> consumer = new MockConsumer<byte[], int>(Serdes.ByteArray, values);
        values.Reassign = consumer.Assign;
        try
        {
            SetUp(consumer.Assign, consumer.Seek, consumer.AddRecord, topic);

            SeekBackWindow window = RunTheSeekBackWindow(
                topic,
                values,
                () => consumer.Poll(s_pollTimeout),
                _ =>
                {
                    consumer.Close();
                    return Task.CompletedTask;
                });

            window.AssertTheOtherCallWaited();
            Assert.Null(window.Failure);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task AsyncPoll_AnotherThreadsPosition_DuringTheSeekBacks_WaitsForThem_AndEveryPartitionIsRewound()
    {
        string topic = UniqueTopic();
        PartitionDeserializer values = new PartitionDeserializer();
        AsyncMockConsumer<byte[], int> consumer = new AsyncMockConsumer<byte[], int>(Serdes.ByteArray, values);
        values.Reassign = partitions => Block(() => consumer.Assign(partitions));
        try
        {
            SetUp(
                partitions => Block(() => consumer.Assign(partitions)),
                (partition, offset) => Block(() => consumer.Seek(partition, offset)),
                consumer.AddRecord,
                topic);
            Task<long>? position = null;

            SeekBackWindow window = RunTheSeekBackWindow(
                topic,
                values,
                () => Block(() => consumer.Poll(s_pollTimeout)),
                probe => position = consumer.Position(probe));

            window.AssertTheOtherCallWaited();
            Assert.Null(window.Failure);
            Assert.Equal(0L, await TestTimeout.Run(() => position!, s_deadline));
            foreach (TopicPartition kept in Kept(topic, values.Failed))
            {
                Assert.Equal(0L, await TestTimeout.Run(() => consumer.Position(kept), s_deadline));
            }

            AddRecords(consumer.AddRecord, Kept(topic, values.Failed));
            AssertRedelivered(
                await TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline), topic, values.Failed);
            Assert.Equal(Kept(topic, values.Failed), consumer.Assignment().OrderBy(tp => tp.Partition));
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncPoll_AnotherThreadsClose_DuringTheSeekBacks_WaitsForThem_ThenCloses()
    {
        string topic = UniqueTopic();
        PartitionDeserializer values = new PartitionDeserializer();
        AsyncMockConsumer<byte[], int> consumer = new AsyncMockConsumer<byte[], int>(Serdes.ByteArray, values);
        values.Reassign = partitions => Block(() => consumer.Assign(partitions));
        try
        {
            SetUp(
                partitions => Block(() => consumer.Assign(partitions)),
                (partition, offset) => Block(() => consumer.Seek(partition, offset)),
                consumer.AddRecord,
                topic);

            SeekBackWindow window = RunTheSeekBackWindow(
                topic,
                values,
                () => Block(() => consumer.Poll(s_pollTimeout)),
                _ => consumer.Close());

            window.AssertTheOtherCallWaited();
            Assert.Null(window.Failure);
            Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public async Task AsyncPoll_AnotherThreadsCall_WhileThePollsThreadClosesDuringTheSeekBacks_WaitsForTheClose_ThenIsRefused()
    {
        string topic = UniqueTopic();
        PartitionDeserializer values = new PartitionDeserializer();
        AsyncMockConsumer<byte[], int> consumer = new AsyncMockConsumer<byte[], int>(Serdes.ByteArray, values);
        values.Reassign = partitions => Block(() => consumer.Assign(partitions));
        using ManualResetEventSlim closing = new ManualResetEventSlim();
        using ManualResetEventSlim finish = new ManualResetEventSlim();
        Thread? pollsThread = null;
        try
        {
            SetUp(
                partitions => Block(() => consumer.Assign(partitions)),
                (partition, offset) => Block(() => consumer.Seek(partition, offset)),
                consumer.AddRecord,
                topic);
            _ = consumer.Assignment();
            using BlockingSeekBackTrace trace = BlockingSeekBackTrace.Start(topic);
            Task<ConsumerRecords<byte[], int>>? poll = null;
            Task? close = null;
            Exception? polling = null;
            pollsThread = StartThread(() =>
            {
                polling = Record.Exception(() =>
                {
                    poll = consumer.Poll(s_pollTimeout);
                    if (trace.WaitEntered())
                    {
                        close = consumer.Close();
                    }
                });
                closing.Set();
                _ = finish.Wait(s_deadline);
            });
            Assert.True(closing.Wait(s_deadline), "the poll's thread never closed during the seek-backs");
            Assert.Null(polling);
            Assert.NotNull(close);

            int[] state = new int[2];
            Exception? refused = null;
            Thread caller = StartThread(() =>
            {
                Volatile.Write(ref state[0], 1);
                refused = Record.Exception(() => consumer.Assignment());
                Volatile.Write(ref state[1], 1);
            });
            SpinUntilWaitingOrEnded(caller, () => Volatile.Read(ref state[0]) == 1, () => Volatile.Read(ref state[1]) == 1);
            bool waited = Volatile.Read(ref state[1]) == 0;
            trace.Release();
            Join(caller);

            Assert.True(waited, "the other thread's call returned while the close waited for the seek-backs");
            Assert.IsType<ObjectDisposedException>(refused);
            await TestTimeout.Run(() => close!, s_deadline);
            RecordDeserializationException failed = await Assert.ThrowsAsync<RecordDeserializationException>(
                () => TestTimeout.Run(() => poll!, s_deadline));
            Assert.Equal(new TopicPartition(topic, values.Failed), failed.TopicPartition);
            Assert.Equal(0L, failed.Offset);
        }
        finally
        {
            finish.Set();
            _ = pollsThread?.Join(s_deadline);
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    private static string UniqueTopic() => "seek-back-window-" + Guid.NewGuid().ToString("N");

    private static void SetUp(
        Action<IReadOnlyCollection<TopicPartition>> assign,
        Action<TopicPartition, long> seek,
        Action<string, int, long, byte[]?, byte[]?> addRecord,
        string topic)
    {
        TopicPartition[] all = Enumerable.Range(0, Partitions).Select(p => new TopicPartition(topic, p)).ToArray();
        assign(all);
        foreach (TopicPartition partition in all)
        {
            seek(partition, 0);
        }

        AddRecords(addRecord, all);
    }

    private static void AddRecords(
        Action<string, int, long, byte[]?, byte[]?> addRecord,
        IEnumerable<TopicPartition> partitions)
    {
        foreach (TopicPartition partition in partitions)
        {
            addRecord(partition.Topic, partition.Partition, 0, null, new[] { (byte)partition.Partition });
            addRecord(partition.Topic, partition.Partition, 1, null, new[] { (byte)partition.Partition });
        }
    }

    private static TopicPartition[] Kept(string topic, int failed) =>
        Enumerable.Range(0, Partitions).Where(p => p != failed).Select(p => new TopicPartition(topic, p)).ToArray();

    private static void AssertRedelivered(ConsumerRecords<byte[], int> records, string topic, int failed)
    {
        IEnumerable<(int Partition, long Offset, int Value)> expected = Kept(topic, failed)
            .SelectMany(tp => new[] { (tp.Partition, 0L, tp.Partition), (tp.Partition, 1L, tp.Partition) });
        Assert.Equal(
            expected,
            records.Select(r => (r.Partition, r.Offset, r.Value)).OrderBy(r => r.Partition).ThenBy(r => r.Offset));
        Assert.All(records, r => Assert.Equal(topic, r.Topic));
    }

    private static SeekBackWindow RunTheSeekBackWindow(
        string topic,
        PartitionDeserializer values,
        Action poll,
        Func<TopicPartition, Task> other)
    {
        BlockingSeekBackTrace trace = BlockingSeekBackTrace.Start(topic);
        try
        {
            Exception? polled = null;
            Thread poller = StartThread(() => polled = Record.Exception(poll));
            if (!trace.WaitEntered())
            {
                throw new TimeoutException("the seek-backs never traced the unassigned partition");
            }

            TopicPartition probe = new TopicPartition(topic, values.Failed == 0 ? 1 : 0);
            int[] state = new int[2];
            Task? call = null;
            Exception? failure = null;
            Thread caller = StartThread(() =>
            {
                Volatile.Write(ref state[0], 1);
                failure = Record.Exception(() => { call = other(probe); });
                Volatile.Write(ref state[1], 1);
                if (failure is null)
                {
                    failure = Record.Exception(() => Block(() => call!));
                }
            });
            SpinUntilWaitingOrEnded(caller, () => Volatile.Read(ref state[0]) == 1, () => Volatile.Read(ref state[1]) == 1);
            bool waited = Volatile.Read(ref state[1]) == 0;
            trace.Release();
            Join(poller);
            Join(caller);
            return new SeekBackWindow(topic, values.Failed, waited, polled, failure, trace.Lines());
        }
        finally
        {
            trace.Dispose();
        }
    }

    private static Thread StartThread(Action body)
    {
        Thread thread = new Thread(() => body());
        thread.IsBackground = true;
        thread.Start();
        return thread;
    }

    private static void Join(Thread thread)
    {
        if (!thread.Join(s_deadline))
        {
            throw new TimeoutException("a thread of the seek-back window never ended");
        }
    }

    private static void SpinUntilWaitingOrEnded(Thread thread, Func<bool> calling, Func<bool> ended)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!ended() && !(calling() && (thread.ThreadState & System.Threading.ThreadState.WaitSleepJoin) != 0))
        {
            if (elapsed.Elapsed > s_deadline)
            {
                throw new TimeoutException("the other thread's call neither waited nor ended");
            }

            Thread.Yield();
        }
    }

    private static void Block(Func<Task> operation) => TestTimeout.Run(operation, s_deadline).GetAwaiter().GetResult();

    private static T Block<T>(Func<Task<T>> operation) =>
        TestTimeout.Run(operation, s_deadline).GetAwaiter().GetResult();

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    /// <summary>What one seek-back window saw: the poll's outcome, the other thread's, and the seek-back traces.</summary>
    private sealed class SeekBackWindow
    {
        private readonly string _topic;

        private readonly int _failed;

        private readonly bool _waited;

        private readonly Exception? _polled;

        private readonly List<string> _lines;

        internal SeekBackWindow(
            string topic, int failed, bool waited, Exception? polled, Exception? failure, List<string> lines)
        {
            _topic = topic;
            _failed = failed;
            _waited = waited;
            _polled = polled;
            Failure = failure;
            _lines = lines;
        }

        internal Exception? Failure { get; }

        internal void AssertTheOtherCallWaited()
        {
            Assert.True(_waited, "the other thread's call returned while the seek-backs ran");
            RecordDeserializationException failed = Assert.IsType<RecordDeserializationException>(_polled);
            Assert.Equal(new TopicPartition(_topic, _failed), failed.TopicPartition);
            Assert.Equal(0L, failed.Offset);
            string line = Assert.Single(_lines);
            Assert.Contains($"Could not seek {_topic}-{_failed} back to offset 0", line, StringComparison.Ordinal);
        }
    }

    /// <summary>Holds the first seek-back trace of its topic until released, and keeps every such line.</summary>
    private sealed class BlockingSeekBackTrace : TraceListener
    {
        private readonly string _marker;

        private readonly StringBuilder _pending = new StringBuilder();

        private readonly List<string> _lines = new List<string>();

        private readonly ManualResetEventSlim _entered = new ManualResetEventSlim();

        private readonly ManualResetEventSlim _released = new ManualResetEventSlim();

        private BlockingSeekBackTrace(string topic) => _marker = "Could not seek " + topic + "-";

        internal static BlockingSeekBackTrace Start(string topic)
        {
            BlockingSeekBackTrace trace = new BlockingSeekBackTrace(topic);
            Trace.Listeners.Add(trace);
            return trace;
        }

        internal bool WaitEntered() => _entered.Wait(s_deadline);

        internal void Release() => _released.Set();

        internal List<string> Lines()
        {
            lock (_lines)
            {
                return _lines.ToList();
            }
        }

        public override void Write(string? message)
        {
            lock (_lines)
            {
                _pending.Append(message);
            }
        }

        public override void WriteLine(string? message)
        {
            bool first;
            lock (_lines)
            {
                _pending.Append(message);
                string line = _pending.ToString();
                _pending.Clear();
                if (line.IndexOf(_marker, StringComparison.Ordinal) < 0)
                {
                    return;
                }

                _lines.Add(line);
                first = _lines.Count == 1;
            }

            if (first)
            {
                _entered.Set();
                _ = _released.Wait(s_deadline);
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _released.Set();
                Trace.Listeners.Remove(this);
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// Reads a value's single byte as its partition. Its first call unassigns that partition, so that the partition's
    /// seek back fails and traces, the one point inside the seek-backs a test can hold, and then fails.
    /// </summary>
    private sealed class PartitionDeserializer : IDeserializer<int>
    {
        private int _calls;

        private int _failed = -1;

        internal Action<IReadOnlyCollection<TopicPartition>>? Reassign { get; set; }

        internal int Failed => Volatile.Read(ref _failed);

        public int Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            int partition = data[0];
            if (Interlocked.Increment(ref _calls) != 1)
            {
                return partition;
            }

            Volatile.Write(ref _failed, partition);
            Reassign!(Kept(topic, partition));
            throw new FormatException("the first record fails");
        }
    }

    /// <summary>A UTF-8 deserializer that counts its calls and disposes its target on each one.</summary>
    private sealed class DisposingDeserializer : IDeserializer<string?>
    {
        internal IDisposable? Target { get; set; }

        internal int Calls { get; private set; }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            Calls++;
            Target?.Dispose();
            return Encoding.UTF8.GetString(data.ToArray());
        }
    }

    /// <summary>A UTF-8 deserializer that throws for the value "bad".</summary>
    private sealed class FailingDeserializer : IDeserializer<string?>
    {
        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            string text = Encoding.UTF8.GetString(data.ToArray());
            if (text == "bad")
            {
                throw new FormatException("bad record");
            }

            return text;
        }
    }
}
