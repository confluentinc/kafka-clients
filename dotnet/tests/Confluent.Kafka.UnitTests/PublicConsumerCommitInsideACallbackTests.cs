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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A commit made inside a commit callback, on each consumer: the real consumers run the callbacks queued behind the
/// running one inside it, the mocks run its own callback inline, and an async callback's calls back into its consumer.
/// </summary>
public sealed class PublicConsumerCommitInsideACallbackTests
{
    private const string Topic = "t";

    private const string MockHandleUnsupported =
        "ConsumerHandle async operations are not supported on a MockConsumer handle; drive the MockConsumer directly.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Theory]
    [InlineData("CommitAsync")]
    [InlineData("Commit")]
    public void KafkaConsumer_ACommitInsideACallback_RunsTheQueuedCallbacksFirst(string nested)
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer();
        try
        {
            List<string> events = new List<string>();
            consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
            {
                Add(events, "first");
                consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => Add(events, "queued")));
                Add(events, "queued it");
                if (nested == "CommitAsync")
                {
                    consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => Add(events, "nested")));
                }
                else
                {
                    consumer.Commit(Empty());
                }

                Add(events, "first end");
            }));

            TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
            TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);

            List<string> expected = new List<string> { "first", "queued it", "queued", "first end" };
            if (nested == "CommitAsync")
            {
                expected.Add("nested");
            }

            Assert.Equal(expected, Snapshot(events));
        }
        finally
        {
            Close(consumer);
        }
    }

    [Fact]
    public void KafkaConsumer_ACommitAsyncWithAnEmptyMapInsideACallback_GivesItsCallbackNullOffsets()
    {
        KafkaConsumer<byte[], byte[]> consumer = NewKafkaConsumer();
        try
        {
            List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? Offsets, Exception? Exception)> seen =
                new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?)>();
            consumer.CommitAsync(Empty(), new DelegateCallback((_, _) =>
                consumer.CommitAsync(Empty(), new DelegateCallback((o, e) => Add(seen, (o, e))))));

            TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
            TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);

            (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =
                Assert.Single(Snapshot(seen));
            Assert.Null(offsets);
            Assert.Null(exception);
        }
        finally
        {
            Close(consumer);
        }
    }

    [Fact]
    public void MockConsumer_ACommitAsyncInsideACallback_RunsItsCallbackInline()
    {
        MockConsumer<byte[], byte[]> consumer = NewMockConsumer();
        try
        {
            List<string> events = new List<string>();
            consumer.CommitAsync(Offsets(1), new DelegateCallback((_, _) =>
            {
                Add(events, "first");
                consumer.CommitAsync(Offsets(2), new DelegateCallback((o, _) => Add(events, "nested " + Format(o))));
                Add(events, "first end");
            }));

            Assert.Equal(new[] { "first", "nested " + Topic + "-0@2", "first end" }, Snapshot(events));
            Assert.Equal(2, consumer.Committed(new[] { s_tp0 })[s_tp0]!.Offset);
        }
        finally
        {
            Close(consumer);
        }
    }

    [Theory]
    [InlineData("CommitAsync")]
    [InlineData("Commit")]
    public async Task AsyncKafkaConsumer_ACommitInsideAnAsyncCallback_RunsTheQueuedCallbackInsideIt(string nested)
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer = await NewAsyncKafkaConsumer();
        try
        {
            List<string> events = new List<string>();
            await TestTimeout.Run(
                () => consumer.CommitAsync(Empty(), new AsyncDelegateCallback(async (_, _) =>
                {
                    await Task.Yield();
                    Add(events, "first");
                    await consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => Add(events, "queued")));
                    Add(events, "queued it");
                    if (nested == "CommitAsync")
                    {
                        await consumer.CommitAsync(Empty(), new DelegateCallback((_, _) => Add(events, "nested")));
                    }
                    else
                    {
                        await consumer.Commit(Empty());
                    }

                    Add(events, "first end");
                })),
                s_deadline);

            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);

            List<string> expected = new List<string> { "first", "queued it", "queued", "first end" };
            if (nested == "CommitAsync")
            {
                expected.Add("nested");
            }

            Assert.Equal(expected, Snapshot(events));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Fact]
    public async Task AsyncMockConsumer_ACommitAsyncInsideAnAsyncCallback_RunsItsCallbackInline()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = await NewAsyncMockConsumer();
        try
        {
            List<string> events = new List<string>();
            await TestTimeout.Run(
                () => consumer.CommitAsync(Offsets(1), new AsyncDelegateCallback(async (_, _) =>
                {
                    await Task.Yield();
                    Add(events, "first");
                    await consumer.CommitAsync(Offsets(2), new DelegateCallback((_, _) => Add(events, "nested")));
                    Add(events, "first end");
                })),
                s_deadline);

            Assert.Equal(new[] { "first", "nested", "first end" }, Snapshot(events));
            IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> committed =
                await TestTimeout.Run(() => consumer.Committed(new[] { s_tp0 }), s_deadline);
            Assert.Equal(2, committed[s_tp0]!.Offset);
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    [Theory]
    [InlineData("kafka")]
    [InlineData("mock")]
    public async Task AnAsyncCommitCallback_CallsBackIntoItsConsumer(string kind)
    {
        bool mock = kind == "mock";
        IAsyncConsumer<byte[], byte[]> consumer = mock ? await NewAsyncMockConsumer() : await NewAsyncKafkaConsumer();
        try
        {
            List<string> seen = new List<string>();
            AsyncDelegateCallback callback = new AsyncDelegateCallback(async (_, _) =>
            {
                await Task.Yield();
                Add(seen, "Assignment: " + Outcome(() => Format(consumer.Assignment())));
                Add(seen, "CommitAsync: " + await OutcomeAsync(() => consumer.CommitAsync(Empty())));
                Add(seen, "Pause: " + await OutcomeAsync(() => consumer.Pause(new[] { s_tp0 })));
                Add(seen, "Paused: " + Outcome(() => Format(consumer.Paused())));
            });

            await TestTimeout.Run(() => consumer.CommitAsync(mock ? Offsets(1) : Empty(), callback), s_deadline);
            await TestTimeout.Run(() => consumer.Commit(Empty()), s_deadline);

            Assert.Equal(
                new[]
                {
                    "Assignment: ok [" + Topic + "-0]",
                    "CommitAsync: ok",
                    "Pause: " + (mock ? "KafkaException 35: " + MockHandleUnsupported : "ok"),
                    "Paused: ok [" + (mock ? string.Empty : Topic + "-0") + "]",
                },
                Snapshot(seen));
        }
        finally
        {
            await CloseAndDispose(consumer);
        }
    }

    private static string Outcome(Func<string> call)
    {
        try
        {
            return "ok " + call();
        }
        catch (KafkaException e)
        {
            return "KafkaException " + e.Code + ": " + e.Message;
        }
        catch (Exception e)
        {
            return e.GetType().Name + ": " + e.Message;
        }
    }

    private static async Task<string> OutcomeAsync(Func<Task> call)
    {
        try
        {
            await call();
            return "ok";
        }
        catch (KafkaException e)
        {
            return "KafkaException " + e.Code + ": " + e.Message;
        }
        catch (Exception e)
        {
            return e.GetType().Name + ": " + e.Message;
        }
    }

    private static string Format(IReadOnlyCollection<TopicPartition> partitions) =>
        "[" + string.Join(", ", partitions.Select(tp => tp.Topic + "-" + tp.Partition)) + "]";

    private static string Format(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets) =>
        offsets is null
            ? "null"
            : string.Join(", ", offsets.Select(entry => entry.Key.Topic + "-" + entry.Key.Partition + "@" + entry.Value.Offset));

    private static void Add<T>(List<T> list, T item)
    {
        lock (list)
        {
            list.Add(item);
        }
    }

    private static T[] Snapshot<T>(List<T> list)
    {
        lock (list)
        {
            return list.ToArray();
        }
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset) =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset) };

    private static Dictionary<string, string> KafkaConfig() =>
        new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "g",
        };

    private static KafkaConsumer<byte[], byte[]> NewKafkaConsumer()
    {
        KafkaConsumer<byte[], byte[]> consumer =
            new KafkaConsumer<byte[], byte[]>(KafkaConfig(), Serdes.ByteArray, Serdes.ByteArray);
        consumer.Assign(new[] { s_tp0 });
        return consumer;
    }

    private static MockConsumer<byte[], byte[]> NewMockConsumer()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        consumer.Assign(new[] { s_tp0 });
        consumer.UpdateBeginningOffset(Topic, 0, 0);
        return consumer;
    }

    private static async Task<AsyncKafkaConsumer<byte[], byte[]>> NewAsyncKafkaConsumer()
    {
        AsyncKafkaConsumer<byte[], byte[]> consumer =
            new AsyncKafkaConsumer<byte[], byte[]>(KafkaConfig(), Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
        return consumer;
    }

    private static async Task<AsyncMockConsumer<byte[], byte[]>> NewAsyncMockConsumer()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, "earliest");
        await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
        consumer.UpdateBeginningOffset(Topic, 0, 0);
        return consumer;
    }

    private static void Close(IConsumer<byte[], byte[]> consumer)
    {
        try
        {
            TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    private static async Task CloseAndDispose(IAsyncConsumer<byte[], byte[]> consumer)
    {
        try
        {
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    /// <summary>A commit callback that runs the action it is given.</summary>
    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?> _onComplete;

        internal DelegateCallback(Action<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?> onComplete) =>
            _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete(offsets, exception);
    }

    /// <summary>An async commit callback that runs the function it is given.</summary>
    private sealed class AsyncDelegateCallback : IAsyncOffsetCommitCallback
    {
        private readonly Func<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?, Task> _onComplete;

        internal AsyncDelegateCallback(
            Func<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?, Task> onComplete) =>
            _onComplete = onComplete;

        public Task OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete(offsets, exception);
    }
}
