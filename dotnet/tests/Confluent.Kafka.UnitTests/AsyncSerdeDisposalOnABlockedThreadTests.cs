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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A synchronous <c>Dispose()</c> of an asynchronous client waits on the calling thread for a serde's
/// <c>DisposeAsync()</c>. That thread is here the only one of its synchronization context or of its task scheduler,
/// so the wait ends only if no <c>await</c> in the serde's <c>DisposeAsync()</c> posts its continuation back to it.
/// A <c>Create</c> whose client the core fails to build blocks no thread: it awaits that disposal.
/// </summary>
public sealed class AsyncSerdeDisposalOnABlockedThreadTests
{
    private const string ContextHow = "synchronization-context";
    private const string SchedulerHow = "task-scheduler";

    private static readonly TimeSpan s_bound = TimeSpan.FromSeconds(30);

    [Theory]
    [InlineData(ContextHow)]
    [InlineData(SchedulerHow)]
    public async Task AProducersDispose_Returns_AndDisposesEachSerializerOnce(string how)
    {
        YieldingSerde key = new YieldingSerde();
        YieldingSerde value = new YieldingSerde();
        AsyncKafkaProducer<string, string> producer =
            await AsyncKafkaProducer<string, string>.Create(UnreachableProducer(), key, value);

        int posted = OnItsOnlyThread(how, producer.Dispose);

        Assert.Equal(0, posted);
        Assert.Equal(1, key.AsyncDisposals);
        Assert.Equal(1, value.AsyncDisposals);
    }

    [Theory]
    [InlineData(ContextHow)]
    [InlineData(SchedulerHow)]
    public async Task AConsumersDispose_Returns_AndDisposesEachDeserializerOnce(string how)
    {
        YieldingSerde key = new YieldingSerde();
        YieldingSerde value = new YieldingSerde();
        AsyncKafkaConsumer<string, string> consumer =
            await AsyncKafkaConsumer<string, string>.Create(UnreachableConsumer(), key, value);

        int posted = OnItsOnlyThread(how, consumer.Dispose);

        Assert.Equal(0, posted);
        Assert.Equal(1, key.AsyncDisposals);
        Assert.Equal(1, value.AsyncDisposals);
    }

    [Fact]
    public async Task AProducersFailedCreate_FailsItsTask_AndDisposesEachSerializerOnce()
    {
        YieldingSerde key = new YieldingSerde();
        YieldingSerde value = new YieldingSerde();
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "unresolvable.invalid:9092",
        };

        Task<AsyncKafkaProducer<string, string>> created = AsyncKafkaProducer<string, string>.Create(configs, key, value);

        KafkaException failure = await Assert.ThrowsAnyAsync<KafkaException>(() => TestTimeout.Run(() => created, s_bound));

        Assert.Equal("Failed to construct kafka producer", failure.Message);
        Assert.Equal(1, key.AsyncDisposals);
        Assert.Equal(1, value.AsyncDisposals);
    }

    [Fact]
    public async Task AConsumersFailedCreate_FailsItsTask_AndDisposesEachDeserializerOnce()
    {
        YieldingSerde key = new YieldingSerde();
        YieldingSerde value = new YieldingSerde();
        Dictionary<string, object?> configs = UnreachableConsumer();
        configs["group.id"] = string.Empty;

        Task<AsyncKafkaConsumer<string, string>> created = AsyncKafkaConsumer<string, string>.Create(configs, key, value);

        KafkaException failure = await Assert.ThrowsAnyAsync<KafkaException>(() => TestTimeout.Run(() => created, s_bound));

        Assert.Equal("Failed to construct kafka consumer", failure.Message);
        Assert.Equal(1, key.AsyncDisposals);
        Assert.Equal(1, value.AsyncDisposals);
    }

    [Fact]
    public async Task ADisposalAnAwaitedCloseStarted_IsWaitedForByADisposeOnTheContextsThread()
    {
        YieldingSerde key = new YieldingSerde();
        YieldingSerde value = new YieldingSerde();
        AsyncKafkaProducer<string, string> producer =
            await AsyncKafkaProducer<string, string>.Create(UnreachableProducer(), key, value);
        Task closed = Task.CompletedTask;

        int posted = OnItsOnlyThread(
            ContextHow,
            () =>
            {
                closed = producer.Close(TimeSpan.Zero);
                producer.Dispose();
            });

        await TestTimeout.Run(() => closed, s_bound);
        Assert.Equal(0, posted);
        Assert.Equal(1, key.AsyncDisposals);
        Assert.Equal(1, value.AsyncDisposals);
    }

    private static int OnItsOnlyThread(string how, Action action)
    {
        Exception? thrown = null;
        int posted = 0;
        bool contextKept = true;
        Thread thread = new Thread(
            () =>
            {
                Action guarded = () =>
                {
                    try
                    {
                        action();
                    }
                    catch (Exception e)
                    {
                        thrown = e;
                    }
                };
                if (how == ContextHow)
                {
                    UnpumpedContext context = new UnpumpedContext();
                    SynchronizationContext.SetSynchronizationContext(context);
                    guarded();
                    contextKept = ReferenceEquals(SynchronizationContext.Current, context);
                    posted = context.Posted;
                }
                else
                {
                    OneThreadScheduler scheduler = new OneThreadScheduler();
                    new Task(guarded).Start(scheduler);
                    scheduler.RunTheFirst();
                    posted = scheduler.Queued - 1;
                }
            })
        {
            IsBackground = true,
        };
        thread.Start();
        if (!thread.Join(s_bound))
        {
            throw new System.TimeoutException(
                "The call did not return within " + s_bound + ": it waits for a continuation posted to its own thread.");
        }

        if (thrown is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(thrown).Throw();
        }

        Assert.True(contextKept, "The call left its thread without the synchronization context it had.");
        return posted;
    }

    private static Dictionary<string, object?> UnreachableProducer() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
        };

    private static Dictionary<string, object?> UnreachableConsumer() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "async-serde-disposal-blocked-thread-group",
            ["enable.auto.commit"] = "false",
        };

    /// <summary>A serializer and deserializer of strings whose <c>DisposeAsync()</c> awaits before it completes.</summary>
    private sealed class YieldingSerde : IAsyncSerializer<string>, IAsyncDeserializer<string>, IAsyncDisposable
    {
        private int _asyncDisposals;

        internal int AsyncDisposals => Volatile.Read(ref _asyncDisposals);

        public ValueTask<ReadOnlyMemory<byte>?> Serialize(
            string topic, Headers headers, string? data, CancellationToken cancellationToken) => default;

        public ValueTask<string?> Deserialize(
            string topic, Headers headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) => default;

        public async ValueTask DisposeAsync()
        {
            await Task.Yield();
            Interlocked.Increment(ref _asyncDisposals);
        }
    }

    /// <summary>A synchronization context that counts what is posted to it and runs none of it.</summary>
    private sealed class UnpumpedContext : SynchronizationContext
    {
        private int _posted;

        internal int Posted => Volatile.Read(ref _posted);

        public override void Post(SendOrPostCallback d, object? state) => Interlocked.Increment(ref _posted);

        public override void Send(SendOrPostCallback d, object? state) => Interlocked.Increment(ref _posted);

        public override SynchronizationContext CreateCopy() => this;
    }

    /// <summary>A task scheduler that counts the tasks queued to it and runs only the first, on the thread that asks.</summary>
    private sealed class OneThreadScheduler : TaskScheduler
    {
        private readonly BlockingCollection<Task> _tasks = new BlockingCollection<Task>();
        private int _queued;

        internal int Queued => Volatile.Read(ref _queued);

        internal void RunTheFirst() => TryExecuteTask(_tasks.Take());

        protected override void QueueTask(Task task)
        {
            Interlocked.Increment(ref _queued);
            _tasks.Add(task);
        }

        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;

        protected override IEnumerable<Task> GetScheduledTasks() => _tasks.ToArray();
    }
}
