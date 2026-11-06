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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The deserializers a consumer was given: <see cref="KafkaConsumer{TKey, TValue}"/> and
/// <see cref="AsyncKafkaConsumer{TKey, TValue}"/> dispose each one that is <see cref="IDisposable"/> once, key then
/// value, after the destroy and on the thread that destroyed, and when the core cannot build the consumer; a close that
/// is refused disposes nothing, and the mocks never dispose them. Translates Python's deserializer-close tests of
/// <c>test_kafka_consumer.py</c>.
/// </summary>
public sealed class PublicDeserializerDisposalTests
{
    private const string CloseHow = "Close";

    private const string CloseWithOptionsHow = "Close(CloseOptions)";

    private const string DisposeHow = "Dispose";

    private const string DisposeAsyncHow = "DisposeAsync";

    private const string ConcurrentMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private const int WakeupCode = -18;

    private const string ClosedMessage = "This consumer has already been closed.";

    private static readonly TimeSpan s_closeTimeout = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromSeconds(60);

    private static readonly TimeSpan s_otherThreadWait = TimeSpan.FromSeconds(10);

    private static readonly TopicPartition[] s_partitions = { new TopicPartition("deserializer-disposal-topic", 0) };

    [Theory]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(SyncFlavor, CloseWithOptionsHow)]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, CloseWithOptionsHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task TestKafkaConsumer_test_given_deserializers_are_closed_at_close_but_not_configured_EachCloseDisposesOnce_AfterTheDestroy(
        string flavor,
        string how)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        DestroyLog destroys = WatchDestroy(consumer);
        key.OnDispose = () => key.DestroysSeen = destroys.Destroys().Count;
        try
        {
            Assert.Equal(0, key.Disposals);
            Assert.Equal(0, value.Disposals);

            int closingThread = await CloseBy(consumer, how);

            Assert.Equal(1, key.Disposals);
            Assert.Equal(1, value.Disposals);
            Assert.Equal(1, key.DestroysSeen);
            (int destroyThread, string? _) = Assert.Single(destroys.Destroys());
            Assert.Equal(destroyThread, key.ThreadId);
            Assert.Equal(destroyThread, value.ThreadId);
            if (closingThread != -1)
            {
                Assert.Equal(closingThread, key.ThreadId);
            }
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task TestKafkaConsumer_test_given_deserializers_are_closed_at_close_but_not_configured_ASecondCloseDisposesNothingMore(
        string flavor)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        try
        {
            _ = await CloseBy(consumer, CloseWithOptionsHow);
            _ = await CloseBy(consumer, CloseHow);
            _ = await CloseBy(consumer, DisposeHow);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public void TestKafkaConsumer_test_a_failed_construction_closes_the_deserializers_built_AnEmptyGroupId_DisposesBoth(
        string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        DisposableDeserializer key = new DisposableDeserializer("key", log);
        DisposableDeserializer value = new DisposableDeserializer("value", log);
        Dictionary<string, object?> configs = Unreachable();
        configs["group.id"] = string.Empty;

        KafkaException failure = Assert.ThrowsAny<KafkaException>(() => Consumer(flavor, configs, key, value));

        Assert.Equal("Failed to construct kafka consumer", failure.Message);
        Assert.Equal(
            "The configured group.id should not be an empty string or whitespace.",
            Assert.IsAssignableFrom<KafkaException>(failure.InnerException).Message);
        Assert.Equal(new[] { "key", "value" }, log.ToArray());
        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor, "max.poll.records")]
    [InlineData(SyncFlavor, "group.protocol")]
    [InlineData(AsyncFlavor, "max.poll.records")]
    [InlineData(AsyncFlavor, "group.protocol")]
    public void AConfigurationTheCoreRefuses_DisposesBothDeserializers_KeyThenValue(string flavor, string refused)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        DisposableDeserializer key = new DisposableDeserializer("key", log);
        DisposableDeserializer value = new DisposableDeserializer("value", log);
        Dictionary<string, object?> configs = Unreachable();
        configs[refused] = refused == "group.protocol" ? "classic" : "many";

        Exception failure = Assert.ThrowsAny<Exception>(() => Consumer(flavor, configs, key, value));

        if (refused == "group.protocol")
        {
            Assert.IsType<UnsupportedOperationException>(failure);
            Assert.Equal(
                "Classic group protocol is not yet supported in this client; set group.protocol=consumer (KIP-848).",
                failure.Message);
        }
        else
        {
            Assert.IsType<ConfigException>(failure);
            Assert.Equal("Invalid value many for configuration max.poll.records: Not a number of type INT", failure.Message);
        }

        Assert.Equal(new[] { "key", "value" }, log.ToArray());
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task TestKafkaConsumer_test_a_deserializer_close_that_raises_is_logged_not_raised_TheKeyThrows(string flavor)
    {
        string id = Guid.NewGuid().ToString("N");
        DisposableDeserializer key = new DisposableDeserializer("key-" + id, throws: true);
        DisposableDeserializer value = new DisposableDeserializer("value-" + id);

        (IReadOnlyList<string> Lines, IReadOnlyList<(TraceEventType Type, string Message)> Events) trace =
            await CloseCapturingTheTrace(flavor, id, key, value);

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
        string message = $"Exception while closing serde {key}: {key.Thrown}";
        Assert.Equal(message, Assert.Single(trace.Lines));
        Assert.Equal((TraceEventType.Warning, message), Assert.Single(trace.Events));
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task TestKafkaConsumer_test_an_async_consumer_deserializer_close_that_raises_is_logged_not_raised_TheValueThrows(
        string flavor)
    {
        string id = Guid.NewGuid().ToString("N");
        DisposableDeserializer key = new DisposableDeserializer("key-" + id);
        DisposableDeserializer value = new DisposableDeserializer("value-" + id, throws: true);

        (IReadOnlyList<string> Lines, IReadOnlyList<(TraceEventType Type, string Message)> Events) trace =
            await CloseCapturingTheTrace(flavor, id, key, value);

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
        string message = $"Exception while closing serde {value}: {value.Thrown}";
        Assert.Equal(message, Assert.Single(trace.Lines));
        Assert.Equal((TraceEventType.Warning, message), Assert.Single(trace.Events));
    }

    [Fact]
    public async Task TestKafkaConsumer_test_async_consumer_closes_its_deserializers()
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(Unreachable(), key, value);
        try
        {
            await TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), Deadline);
        }
        finally
        {
            await TestTimeout.DisposeAsync(consumer);
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task ACloseThatFails_StillDisposesBoth_KeyThenValue(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        IConsumerCommon consumer = Consumer(
            flavor, Unreachable(), new DisposableDeserializer("key", log), new DisposableDeserializer("value", log));
        InvalidOperationException raised = new InvalidOperationException("commit callback failed");
        try
        {
            await Assign(consumer);
            await QueueCommitCallback(consumer, new DelegateCallback(() => throw raised));

            KafkaException wrapped = await Assert.ThrowsAsync<KafkaException>(
                () => CloseBy(consumer, CloseWithOptionsHow));

            Assert.Equal("Failed to close kafka consumer", wrapped.Message);
            Assert.Same(raised, wrapped.InnerException);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Equal(new[] { "key", "value" }, log.ToArray());
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task ACloseFromAnotherThreadDuringAnOp_IsRefused_AndDisposesNothing_ThenDisposeDisposesOnce(string flavor)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        RunningPoll? poll = null;
        try
        {
            poll = await StartAPollHeldInsideItsCommitCallback(consumer, release);

            Exception refused = await Xunit.Record.ExceptionAsync(() => CloseBy(consumer, CloseHow));

            Assert.Equal(ConcurrentMessage, Assert.IsType<ConcurrentModificationException>(refused).Message);
            release.Set();
            consumer.Wakeup();
            Assert.Equal(WakeupCode, Assert.IsType<WakeupException>(await poll.Outcome()).Code);
            Assert.Equal(0, key.Disposals);
            Assert.Equal(0, value.Disposals);
            Assert.Equal(s_partitions, consumer.Assignment());

            _ = await CloseBy(consumer, DisposeHow);

            Assert.Equal(1, key.Disposals);
            Assert.Equal(1, value.Disposals);
        }
        finally
        {
            release.Set();
            poll?.End();
            await DisposeBounded(consumer);
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task ADisposeFromAnotherThreadDuringAPoll_DisposesAfterTheTeardown(string flavor, string how)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        DestroyLog destroys = WatchDestroy(consumer);
        key.OnDispose = () => key.DestroysSeen = destroys.Destroys().Count;
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        RunningPoll? poll = null;
        try
        {
            poll = await StartAPollHeldInsideItsCommitCallback(consumer, release);
            release.Set();
            Assert.Equal(0, key.Disposals);

            int disposingThread = await CloseBy(consumer, how);

            Assert.Equal(WakeupCode, Assert.IsType<WakeupException>(await poll.Outcome()).Code);
            Assert.Equal(1, key.Disposals);
            Assert.Equal(1, value.Disposals);
            Assert.Equal(1, key.DestroysSeen);
            (int destroyThread, string? _) = Assert.Single(destroys.Destroys());
            Assert.Equal(destroyThread, key.ThreadId);
            Assert.Equal(destroyThread, value.ThreadId);
            if (disposingThread != -1)
            {
                Assert.Equal(disposingThread, key.ThreadId);
            }
        }
        finally
        {
            release.Set();
            poll?.End();
            await DisposeBounded(consumer);
        }
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task ACloseInsideACommitCallback_IsRefused_AndDisposesNothing_ThenDisposeDisposesOnce(string flavor)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        Exception? closeInside = null;
        Exception? disposeInside = null;
        Task? closing = null;
        int disposalsInside = -1;
        try
        {
            await QueueCommitCallback(consumer, new DelegateCallback(() =>
            {
                if (consumer is KafkaConsumer<byte[], byte[]> sync)
                {
                    closeInside = Xunit.Record.Exception(sync.Close);
                    disposeInside = Xunit.Record.Exception(sync.Dispose);
                }
                else
                {
                    closing = ((AsyncKafkaConsumer<byte[], byte[]>)consumer).Close();
                }

                disposalsInside = key.Disposals + value.Disposals;
            }));
            Assert.Null(await DeliverByACommit(consumer));

            if (closing is null)
            {
                Assert.Equal(ConcurrentMessage, Assert.IsType<ConcurrentModificationException>(closeInside).Message);
                Assert.Equal(ConcurrentMessage, Assert.IsType<ConcurrentModificationException>(disposeInside).Message);
            }
            else
            {
                ConcurrentModificationException refused =
                    await Assert.ThrowsAsync<ConcurrentModificationException>(() => TestTimeout.Run(() => closing, Deadline));
                Assert.Equal(ConcurrentMessage, refused.Message);
            }

            Assert.Equal(0, disposalsInside);
            Assert.Equal(0, key.Disposals);
            Assert.Equal(0, value.Disposals);
            Assert.Empty(consumer.Assignment());

            _ = await CloseBy(consumer, DisposeHow);

            Assert.Equal(1, key.Disposals);
            Assert.Equal(1, value.Disposals);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(DisposeHow)]
    [InlineData(DisposeAsyncHow)]
    public async Task TheAsyncConsumersDisposeInsideACallback_DisposesOffTheDrainThread_AfterTheDeferredTeardown(string how)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        DisposableDeserializer key = new DisposableDeserializer("key", log);
        DisposableDeserializer value = new DisposableDeserializer("value", log);
        AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(Unreachable(), key, value);
        DestroyLog destroys = WatchDestroy(consumer);
        Thread? drain = null;
        bool drainAliveAtDisposal = true;
        key.OnDispose = () =>
        {
            key.DestroysSeen = destroys.Destroys().Count;
            drainAliveAtDisposal = drain!.IsAlive;
        };
        Task? disposing = null;
        int disposalsWhenDisposeReturned = -1;
        try
        {
            await QueueCommitCallback(consumer, new DelegateCallback(() =>
            {
                drain = Thread.CurrentThread;
                if (how == DisposeHow)
                {
                    consumer.Dispose();
                    disposing = Task.CompletedTask;
                }
                else
                {
                    disposing = consumer.DisposeAsync().AsTask();
                }

                disposalsWhenDisposeReturned = key.Disposals + value.Disposals;
            }));
            Assert.Null(await DeliverByACommit(consumer));

            Assert.True(value.WaitDisposed(Deadline), "the deserializers were never disposed after the callback");
            Assert.True(disposing!.IsCompleted, "Dispose waited inside the callback");
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Equal(0, disposalsWhenDisposeReturned);
        Assert.Equal(NativeConsumer.PumpThreadName, drain!.Name);
        Assert.Equal(new[] { "key", "value" }, log.ToArray());
        Assert.NotEqual(NativeConsumer.PumpThreadName, key.ThreadName);
        Assert.NotEqual(drain.ManagedThreadId, key.ThreadId);
        Assert.False(drainAliveAtDisposal, "a deserializer was disposed before the drain thread was joined");
        Assert.Equal(1, key.DestroysSeen);
        (int destroyThread, string? _) = Assert.Single(destroys.Destroys());
        Assert.Equal(destroyThread, key.ThreadId);
        Assert.Equal(destroyThread, value.ThreadId);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(SyncFlavor, CloseWithOptionsHow)]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, CloseWithOptionsHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task ADeserializerWhoseDisposeClosesItsConsumer_ReturnsAtOnce_AndTheTeardownEnds(string flavor, string how)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        Exception? nested = null;
        bool nestedCompleted = false;
        key.OnDispose = () => nested = Xunit.Record.Exception(() => nestedCompleted = CloseAndDisposeInside(consumer));
        try
        {
            _ = await CloseBy(consumer, how);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Null(nested);
        Assert.True(nestedCompleted, "a Close or Dispose from inside a deserializer's Dispose did not end at once");
        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
        Assert.Equal(ClosedMessage, Assert.Throws<IllegalStateException>(() => consumer.Assignment()).Message);
    }

    [Theory]
    [InlineData(SyncFlavor, CloseHow, DisposeHow)]
    [InlineData(SyncFlavor, CloseHow, CloseHow)]
    [InlineData(SyncFlavor, DisposeHow, DisposeHow)]
    [InlineData(SyncFlavor, DisposeHow, CloseHow)]
    [InlineData(AsyncFlavor, CloseHow, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow, CloseHow)]
    [InlineData(AsyncFlavor, DisposeHow, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeHow, CloseHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow, CloseHow)]
    [InlineData(AsyncFlavor, CloseHow, DisposeAsyncHow)]
    [InlineData(AsyncFlavor, DisposeHow, DisposeAsyncHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow, DisposeAsyncHow)]
    public async Task ADeserializerWhoseDisposeWaitsForAnotherThreadThatClosesItsConsumer_BothReturn(
        string flavor, string how, string otherHow)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        OtherThreadClose other = new OtherThreadClose(consumer, otherHow);
        key.OnDispose = other.RunAndWait;
        try
        {
            _ = await CloseBy(consumer, how);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.True(other.Returned, "a " + otherHow + " on another thread, waited for by a deserializer's Dispose, did not return");
        Assert.Null(other.Failure);
        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
        Assert.Equal(ClosedMessage, Assert.Throws<IllegalStateException>(() => consumer.Assignment()).Message);
    }

    [Theory]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task ADeserializerWhoseDisposeWaitsForACloseParkedBeforeTheDestroy_BothReturn(string flavor, string otherHow)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        SemaphoreSlim gate = CloseGateOf(consumer);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        RunningPoll? poll = null;
        GatedClose? other = null;
        bool otherReturned = false;
        try
        {
            poll = await StartAPollHeldInsideItsCommitCallback(consumer, release);
            Task disposing = CloseBy(consumer, DisposeHow);
            Assert.True(SpinWait.SpinUntil(() => gate.CurrentCount == 0, Deadline), "the Dispose never took the close gate");
            other = GatedClose.Start(consumer, otherHow);
            Assert.True(other.WaitParked(Deadline), "the other " + otherHow + " never reached the close gate");
            key.OnDispose = () => otherReturned = other.WaitReturned(s_otherThreadWait);
            Assert.Equal(0, key.Disposals);

            release.Set();
            await disposing;

            Assert.Equal(WakeupCode, Assert.IsType<WakeupException>(await poll.Outcome()).Code);
        }
        finally
        {
            release.Set();
            poll?.End();
            await DisposeBounded(consumer);
        }

        Assert.True(otherReturned, "a " + otherHow + " parked on the close gate, waited for by a deserializer's Dispose, did not return");
        Assert.NotNull(other);
        Assert.Null(other.Failure);
        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task AfterTheDestroy_ACloseOrDisposeFromAnotherThread_DoesNotWaitForTheCloseGate(string flavor, string how)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        _ = await CloseBy(consumer, DisposeHow);
        SemaphoreSlim gate = CloseGateOf(consumer);
        Assert.True(gate.Wait(0), "the close gate is still held after the Dispose");
        try
        {
            GatedClose other = GatedClose.Start(consumer, how);

            Assert.True(other.WaitReturned(s_otherThreadWait), "a " + how + " after the destroy waited for the close gate");
            Assert.Null(other.Failure);
        }
        finally
        {
            gate.Release();
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public void AConfigurationValueTheBindingRefuses_DisposesNothing(string flavor)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        Dictionary<string, object?> configs = Unreachable();
        configs["max.poll.records"] = true;

        ConfigException failure = Assert.Throws<ConfigException>(() => Consumer(flavor, configs, key, value));

        Assert.Equal(
            "Invalid value true for configuration max.poll.records: " +
            "Expected value to be a 32-bit integer, but it was a System.Boolean",
            failure.Message);
        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public void ANullArgument_DisposesNothing(string flavor)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");

        Assert.Equal("configs", Assert.Throws<ArgumentNullException>(() => Consumer(flavor, null!, key, value)).ParamName);
        Assert.Equal("configs", Assert.Throws<ArgumentNullException>(() => Consumer(flavor, null!, null!, null!)).ParamName);
        Assert.Equal(
            NullDeserializer("key.deserializer"),
            Assert.Throws<ConfigException>(() => Consumer(flavor, Unreachable(), null!, null!)).Message);
        Assert.Equal(
            NullDeserializer("key.deserializer"),
            Assert.Throws<ConfigException>(() => Consumer(flavor, Unreachable(), null!, value)).Message);
        Assert.Equal(
            NullDeserializer("value.deserializer"),
            Assert.Throws<ConfigException>(() => Consumer(flavor, Unreachable(), key, null!)).Message);
        Assert.Equal(
            NullDeserializer("key.deserializer"),
            Assert.Throws<ConfigException>(() => Mock(flavor, null!, value)).Message);
        Assert.Equal(
            NullDeserializer("value.deserializer"),
            Assert.Throws<ConfigException>(() => Mock(flavor, key, null!)).Message);

        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task OneInstanceGivenAsKeyAndValue_IsDisposedTwice(string flavor)
    {
        DisposableDeserializer both = new DisposableDeserializer("both");
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), both, both);
        try
        {
            _ = await CloseBy(consumer, CloseHow);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Equal(2, both.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task TheKeyIsDisposedBeforeTheValue(string flavor, string how)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        IConsumerCommon consumer = Consumer(
            flavor, Unreachable(), new DisposableDeserializer("key", log), new DisposableDeserializer("value", log));
        try
        {
            _ = await CloseBy(consumer, how);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Equal(new[] { "key", "value" }, log.ToArray());
    }

    [Theory]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task ADeserializerThatIsOnlyIAsyncDisposable_IsNotDisposed(string flavor, string how)
    {
        AsyncOnlyDeserializer key = new AsyncOnlyDeserializer();
        AsyncOnlyDeserializer value = new AsyncOnlyDeserializer();
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        try
        {
            _ = await CloseBy(consumer, how);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(SyncFlavor, CloseWithOptionsHow)]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, CloseWithOptionsHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task TheMockConsumers_NeverDisposeTheirDeserializers(string flavor, string how)
    {
        DisposableDeserializer key = new DisposableDeserializer("key");
        DisposableDeserializer value = new DisposableDeserializer("value");
        IConsumerCommon consumer = Mock(flavor, key, value);
        try
        {
            _ = await CloseBy(consumer, how);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "deserializer-disposal-group",
            ["enable.auto.commit"] = "false",
        };

    private static IConsumerCommon Consumer(
        string flavor,
        IReadOnlyDictionary<string, object?> configs,
        IDeserializer<byte[]> key,
        IDeserializer<byte[]> value) =>
        flavor == SyncFlavor
            ? new KafkaConsumer<byte[], byte[]>(configs, key, value)
            : new AsyncKafkaConsumer<byte[], byte[]>(configs, key, value);

    private static IConsumerCommon Mock(string flavor, IDeserializer<byte[]> key, IDeserializer<byte[]> value) =>
        flavor == SyncFlavor
            ? new MockConsumer<byte[], byte[]>(key, value)
            : new AsyncMockConsumer<byte[], byte[]>(key, value);

    private static string NullDeserializer(string key) =>
        "Invalid value null for configuration " + key + ": must be non-null.";

    private static DestroyLog WatchDestroy(IConsumerCommon consumer)
    {
        NativeConsumer native = (NativeConsumer)consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!;
        FieldInfo afterDestroy = typeof(SafeConsumerHandle)
            .GetField("_afterDestroy", BindingFlags.NonPublic | BindingFlags.Instance)!;
        Action release = (Action)afterDestroy.GetValue(native.Handle)!;
        DestroyLog log = new DestroyLog();
        afterDestroy.SetValue(native.Handle, new Action(() =>
        {
            log.Add(Thread.CurrentThread);
            release();
        }));
        return log;
    }

    private static async Task<int> CloseBy(IConsumerCommon consumer, string how)
    {
        int closingThread = -1;
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            await TestTimeout.RunOffThisThread(
                () =>
                {
                    closingThread = Environment.CurrentManagedThreadId;
                    switch (how)
                    {
                        case CloseHow:
                            sync.Close();
                            break;
                        case CloseWithOptionsHow:
                            sync.Close(CloseOptions.Timeout(s_closeTimeout));
                            break;
                        default:
                            sync.Dispose();
                            break;
                    }

                    return Task.CompletedTask;
                },
                Deadline);
            return closingThread;
        }

        IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)consumer;
        switch (how)
        {
            case CloseHow:
                await TestTimeout.Run(() => async.Close(), Deadline);
                break;
            case CloseWithOptionsHow:
                await TestTimeout.Run(() => async.Close(CloseOptions.Timeout(s_closeTimeout)), Deadline);
                break;
            case DisposeHow:
                await TestTimeout.RunOffThisThread(
                    () =>
                    {
                        closingThread = Environment.CurrentManagedThreadId;
                        async.Dispose();
                        return Task.CompletedTask;
                    },
                    Deadline);
                break;
            default:
                await TestTimeout.Run(() => async.DisposeAsync().AsTask(), Deadline);
                break;
        }

        return closingThread;
    }

    private static bool CloseAndDisposeInside(IConsumerCommon consumer)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            sync.Close();
            sync.Close(CloseOptions.Timeout(TimeSpan.Zero));
            sync.Dispose();
            return true;
        }

        IAsyncConsumer<byte[], byte[]> async = (IAsyncConsumer<byte[], byte[]>)consumer;
        Task closing = async.Close();
        Task closingWithOptions = async.Close(CloseOptions.Timeout(TimeSpan.Zero));
        ValueTask disposing = async.DisposeAsync();
        async.Dispose();
        return closing.Status == TaskStatus.RanToCompletion &&
            closingWithOptions.Status == TaskStatus.RanToCompletion &&
            disposing.AsTask().Status == TaskStatus.RanToCompletion;
    }

    private static Task CloseOrDispose(IConsumerCommon consumer, string how)
    {
        if (how == DisposeHow)
        {
            ((IDisposable)consumer).Dispose();
            return Task.CompletedTask;
        }

        if (how == DisposeAsyncHow)
        {
            return ((IAsyncDisposable)consumer).DisposeAsync().AsTask();
        }

        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            sync.Close();
            return Task.CompletedTask;
        }

        return ((IAsyncConsumer<byte[], byte[]>)consumer).Close();
    }

    private static SemaphoreSlim CloseGateOf(IConsumerCommon consumer)
    {
        NativeConsumer native = (NativeConsumer)consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!;
        return (SemaphoreSlim)typeof(NativeConsumer)
            .GetField("_closeGate", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(native)!;
    }

    private static Task DisposeBounded(IConsumerCommon consumer)
    {
        if (consumer is IAsyncDisposable async)
        {
            return TestTimeout.DisposeAsync(async, Deadline);
        }

        TestTimeout.Dispose((IDisposable)consumer, Deadline);
        return Task.CompletedTask;
    }

    private static async Task Assign(IConsumerCommon consumer)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            TestTimeout.Run(() => sync.Assign(s_partitions), Deadline);
            return;
        }

        await TestTimeout.Run(() => ((IAsyncConsumer<byte[], byte[]>)consumer).Assign(s_partitions), Deadline);
    }

    private static async Task QueueCommitCallback(IConsumerCommon consumer, IOffsetCommitCallback callback)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            TestTimeout.Run(() => sync.CommitAsync(Empty(), callback), Deadline);
            return;
        }

        await TestTimeout.Run(() => ((IAsyncConsumer<byte[], byte[]>)consumer).CommitAsync(Empty(), callback), Deadline);
    }

    private static async Task<Exception?> DeliverByACommit(IConsumerCommon consumer)
    {
        if (consumer is IConsumer<byte[], byte[]> sync)
        {
            return await Xunit.Record.ExceptionAsync(
                () => TestTimeout.RunOffThisThread(
                    () =>
                    {
                        sync.Commit(Empty());
                        return Task.CompletedTask;
                    },
                    Deadline));
        }

        return await Xunit.Record.ExceptionAsync(
            () => TestTimeout.Run(() => ((IAsyncConsumer<byte[], byte[]>)consumer).Commit(Empty()), Deadline));
    }

    private static async Task<RunningPoll> StartAPollHeldInsideItsCommitCallback(
        IConsumerCommon consumer,
        ManualResetEventSlim release)
    {
        await Assign(consumer);
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        await QueueCommitCallback(consumer, new DelegateCallback(() =>
        {
            entered.Set();
            _ = release.Wait(Deadline);
        }));
        RunningPoll poll = RunningPoll.Start(consumer);
        Assert.True(entered.Wait(Deadline), "the commit callback never ran inside the Poll");
        return poll;
    }

    private static async Task<(IReadOnlyList<string> Lines, IReadOnlyList<(TraceEventType Type, string Message)> Events)>
        CloseCapturingTheTrace(string flavor, string id, DisposableDeserializer key, DisposableDeserializer value)
    {
        using TraceCapture trace = new TraceCapture(id);
        IConsumerCommon consumer = Consumer(flavor, Unreachable(), key, value);
        try
        {
            _ = await CloseBy(consumer, CloseWithOptionsHow);
        }
        finally
        {
            await DisposeBounded(consumer);
        }

        return (trace.Lines(), trace.Events());
    }

    private static Dictionary<TopicPartition, OffsetAndMetadata> Empty() =>
        new Dictionary<TopicPartition, OffsetAndMetadata>();

    /// <summary>
    /// A <c>Close</c>, <c>Dispose</c> or <c>DisposeAsync</c> made on a thread of its own, which reports when the call
    /// has passed the consumer's closed check and waits for the close gate, and when it returns.
    /// </summary>
    private sealed class GatedClose
    {
        private readonly ManualResetEventSlim _called = new ManualResetEventSlim(false);
        private Thread? _thread;
        private Task? _closing;

        internal Exception? Failure { get; private set; }

        internal static GatedClose Start(IConsumerCommon consumer, string how)
        {
            GatedClose close = new GatedClose();
            close._thread = new Thread(() => close.Run(consumer, how)) { IsBackground = true };
            close._thread.Start();
            return close;
        }

        internal bool WaitParked(TimeSpan timeout)
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            while (elapsed.Elapsed < timeout)
            {
                if (_called.IsSet)
                {
                    return _closing is { IsCompleted: false };
                }

                if ((_thread!.ThreadState & System.Threading.ThreadState.WaitSleepJoin) != 0)
                {
                    return true;
                }

                Thread.Sleep(1);
            }

            return false;
        }

        internal bool WaitReturned(TimeSpan timeout)
        {
            if (!_called.Wait(timeout))
            {
                return false;
            }

            try
            {
                return _closing is null || _closing.Wait(timeout);
            }
            catch (AggregateException failure)
            {
                Failure = failure.InnerException;
                return true;
            }
        }

        private void Run(IConsumerCommon consumer, string how)
        {
            try
            {
                _closing = how == DisposeHow || consumer is IConsumer<byte[], byte[]>
                    ? null
                    : how == DisposeAsyncHow
                        ? ((IAsyncDisposable)consumer).DisposeAsync().AsTask()
                        : ((IAsyncConsumer<byte[], byte[]>)consumer).Close();
                if (_closing is null)
                {
                    CloseOrDispose(consumer, how).GetAwaiter().GetResult();
                }
            }
            catch (Exception failure)
            {
                Failure = failure;
            }

            _called.Set();
        }
    }

    /// <summary>A deserializer's Dispose that closes or disposes its consumer on another thread and waits for it.</summary>
    private sealed class OtherThreadClose
    {
        private readonly IConsumerCommon _consumer;
        private readonly string _how;

        internal OtherThreadClose(IConsumerCommon consumer, string how)
        {
            _consumer = consumer;
            _how = how;
        }

        internal bool Returned { get; private set; }

        internal Exception? Failure { get; private set; }

        internal void RunAndWait()
        {
            Task closing = Task.Run(() => CloseOrDispose(_consumer, _how));
            try
            {
                Returned = closing.Wait(s_otherThreadWait);
            }
            catch (AggregateException failure)
            {
                Returned = true;
                Failure = failure.InnerException;
            }
        }
    }

    /// <summary>A <c>Poll</c> made on a thread of its own, which stays alive until <see cref="End"/>.</summary>
    private sealed class RunningPoll
    {
        private readonly ManualResetEventSlim _end = new ManualResetEventSlim(false);
        private readonly TaskCompletionSource<Exception?> _outcome =
            new TaskCompletionSource<Exception?>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal static RunningPoll Start(IConsumerCommon consumer)
        {
            RunningPoll poll = new RunningPoll();
            Thread thread = new Thread(() => poll.Run(consumer)) { IsBackground = true };
            thread.Start();
            return poll;
        }

        internal Task<Exception?> Outcome() => TestTimeout.Run(() => _outcome.Task, Deadline);

        internal void End() => _end.Set();

        private void Run(IConsumerCommon consumer)
        {
            if (consumer is IConsumer<byte[], byte[]> sync)
            {
                _outcome.SetResult(Xunit.Record.Exception(() => sync.Poll(s_pollTimeout)));
            }
            else
            {
                Task<ConsumerRecords<byte[], byte[]>> polling =
                    ((IAsyncConsumer<byte[], byte[]>)consumer).Poll(s_pollTimeout);
                _ = polling.ContinueWith(
                    ended => _outcome.SetResult(ended.Exception?.InnerException),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }

            _ = _end.Wait(Deadline);
        }
    }

    /// <summary>A commit callback that runs the action it is given.</summary>
    private sealed class DelegateCallback : IOffsetCommitCallback
    {
        private readonly Action _onComplete;

        internal DelegateCallback(Action onComplete) => _onComplete = onComplete;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            _onComplete();
    }

    /// <summary>
    /// A deserializer that records each <c>Dispose</c>: how many, the thread, its name in a shared log, and an optional
    /// probe run inside it; it can throw from <c>Dispose</c>.
    /// </summary>
    private sealed class DisposableDeserializer : IDeserializer<byte[]>, IDisposable
    {
        private readonly string _name;
        private readonly ConcurrentQueue<string>? _log;
        private readonly bool _throws;
        private readonly ManualResetEventSlim _disposed = new ManualResetEventSlim();
        private int _disposals;

        internal DisposableDeserializer(string name, ConcurrentQueue<string>? log = null, bool throws = false)
        {
            _name = name;
            _log = log;
            _throws = throws;
        }

        internal int Disposals => Volatile.Read(ref _disposals);

        internal int ThreadId { get; private set; } = -1;

        internal string? ThreadName { get; private set; }

        internal Exception? Thrown { get; private set; }

        internal Action? OnDispose { get; set; }

        internal int DestroysSeen { get; set; } = -1;

        public byte[] Deserialize(string topic, ReadOnlySpan<byte> data) => data.ToArray();

        public void Dispose()
        {
            ThreadId = Environment.CurrentManagedThreadId;
            ThreadName = Thread.CurrentThread.Name;
            _log?.Enqueue(_name);
            OnDispose?.Invoke();
            _ = Interlocked.Increment(ref _disposals);
            _disposed.Set();
            if (_throws)
            {
                InvalidOperationException thrown = new InvalidOperationException("dispose failed: " + _name);
                Thrown = thrown;
                throw thrown;
            }
        }

        public override string ToString() => "DisposableDeserializer(" + _name + ")";

        internal bool WaitDisposed(TimeSpan timeout) => _disposed.Wait(timeout);
    }

    /// <summary>A deserializer that is only <see cref="IAsyncDisposable"/>, counting its <c>DisposeAsync</c> calls.</summary>
    private sealed class AsyncOnlyDeserializer : IDeserializer<byte[]>, IAsyncDisposable
    {
        private int _disposals;

        internal int Disposals => Volatile.Read(ref _disposals);

        public byte[] Deserialize(string topic, ReadOnlySpan<byte> data) => data.ToArray();

        public ValueTask DisposeAsync()
        {
            _ = Interlocked.Increment(ref _disposals);
            return default;
        }
    }
}
