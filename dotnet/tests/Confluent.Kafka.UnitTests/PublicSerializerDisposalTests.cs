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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Producer;

using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The serializers a producer was given: <see cref="KafkaProducer{TKey, TValue}"/> and
/// <see cref="AsyncKafkaProducer{TKey, TValue}"/> dispose each one that is <see cref="IDisposable"/> once, key then
/// value, after the destroy and on the thread that destroyed, and when the core cannot build the producer; the mocks
/// never dispose them. Translates Java's <c>KafkaProducerTest.testSerializerClose</c> and Python's serializer-close
/// tests of <c>test_kafka_producer.py</c> and <c>test_producer_family.py</c>.
/// </summary>
public sealed class PublicSerializerDisposalTests
{
    private const int MaxBlockMs = 500;

    private const string CloseHow = "Close";

    private const string CloseWithTimeoutHow = "Close(TimeSpan)";

    private const string DisposeHow = "Dispose";

    private const string DisposeAsyncHow = "DisposeAsync";

    private const string UnresolvableBootstrap = "unresolvable.invalid:9092";

    private static readonly TimeSpan s_closeTimeout = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(SyncFlavor, CloseWithTimeoutHow)]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, CloseWithTimeoutHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task KafkaProducerTest_testSerializerClose_DisposesEachSerializerOnce_AfterTheDestroy(
        string flavor,
        string how)
    {
        DisposableSerializer key = new DisposableSerializer("key");
        DisposableSerializer value = new DisposableSerializer("value");
        object producer = Producer(flavor, Unreachable(), key, value);
        DestroyLog destroys = WatchDestroy(producer);
        key.OnDispose = () => key.DestroysSeen = destroys.Destroys().Count;
        try
        {
            Assert.Equal(0, key.Disposals);
            Assert.Equal(0, value.Disposals);

            int closingThread = await CloseBy(producer, how);

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
            await DisposeBounded(producer);
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task TestKafkaProducer_test_given_serializers_are_closed_at_close_but_not_configured_ASecondCloseDisposesNothingMore(
        string flavor)
    {
        DisposableSerializer key = new DisposableSerializer("key");
        DisposableSerializer value = new DisposableSerializer("value");
        object producer = Producer(flavor, Unreachable(), key, value);
        try
        {
            _ = await CloseBy(producer, CloseHow);
            _ = await CloseBy(producer, CloseHow);
            _ = await CloseBy(producer, DisposeHow);
        }
        finally
        {
            await DisposeBounded(producer);
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public void TestKafkaProducer_test_a_failed_construction_closes_the_serializers_built_AnUnresolvableBootstrap_DisposesBoth(
        string flavor)
    {
        DisposableSerializer key = new DisposableSerializer("key");
        DisposableSerializer value = new DisposableSerializer("value");
        Dictionary<string, object?> configs = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = UnresolvableBootstrap,
        };

        KafkaException failure = Assert.ThrowsAny<KafkaException>(() => Producer(flavor, configs, key, value));

        Assert.Equal("Failed to construct kafka producer", failure.Message);
        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public void AConfigurationTheCoreRefuses_DisposesBothSerializers_KeyThenValue(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        DisposableSerializer key = new DisposableSerializer("key", log);
        DisposableSerializer value = new DisposableSerializer("value", log);
        Dictionary<string, object?> configs = Unreachable();
        configs["batch.size"] = "hello";

        ConfigException failure = Assert.Throws<ConfigException>(() => Producer(flavor, configs, key, value));

        Assert.Equal("Invalid value hello for configuration batch.size: Not a number of type INT", failure.Message);
        Assert.Equal(new[] { "key", "value" }, log.ToArray());
    }

    [Fact]
    public async Task TestKafkaProducer_test_async_producer_closes_its_serializers()
    {
        DisposableSerializer key = new DisposableSerializer("key");
        DisposableSerializer value = new DisposableSerializer("value");
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(Unreachable(), key, value);
        try
        {
            await TestTimeout.Run(() => producer.Close(), Deadline);
        }
        finally
        {
            await TestTimeout.DisposeAsync(producer);
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task TestKafkaProducer_test_a_serializer_close_that_raises_is_logged_not_raised_TheKeyThrows(string flavor)
    {
        string id = Guid.NewGuid().ToString("N");
        DisposableSerializer key = new DisposableSerializer("key-" + id, throws: true);
        DisposableSerializer value = new DisposableSerializer("value-" + id);

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
    public async Task TestKafkaProducer_test_an_async_producer_serializer_close_that_raises_is_logged_not_raised_TheValueThrows(
        string flavor)
    {
        string id = Guid.NewGuid().ToString("N");
        DisposableSerializer key = new DisposableSerializer("key-" + id);
        DisposableSerializer value = new DisposableSerializer("value-" + id, throws: true);

        (IReadOnlyList<string> Lines, IReadOnlyList<(TraceEventType Type, string Message)> Events) trace =
            await CloseCapturingTheTrace(flavor, id, key, value);

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
        string message = $"Exception while closing serde {value}: {value.Thrown}";
        Assert.Equal(message, Assert.Single(trace.Lines));
        Assert.Equal((TraceEventType.Warning, message), Assert.Single(trace.Events));
    }

    [Fact]
    public async Task TestProducerFamily_test_cancelling_close_waits_for_its_teardown_before_closing_the_serializers_ATokenThatFiresDuringClose()
    {
        DisposableSerializer key = new DisposableSerializer("key");
        DisposableSerializer value = new DisposableSerializer("value");
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(Unreachable(), key, value);
        DestroyLog destroys = WatchDestroy(producer);
        using ManualResetEventSlim release = new ManualResetEventSlim();
        using ManualResetEventSlim entered = new ManualResetEventSlim();
        Thread? pump = null;
        bool pumpAliveAtDisposal = true;
        key.OnDispose = () =>
        {
            key.DestroysSeen = destroys.Destroys().Count;
            pumpAliveAtDisposal = pump!.IsAlive;
        };
        ProbeCallback callback = new ProbeCallback((_, _) =>
        {
            pump = Thread.CurrentThread;
            entered.Set();
            _ = release.Wait(Deadline);
        });
        try
        {
            Task<AsyncKafkaFuture<RecordMetadata>> send =
                producer.Send(Record(Topic("serde-close-token")), callback).AsTask();
            Assert.True(entered.Wait(Deadline), "the delivery callback never ran");
            using CancellationTokenSource token = new CancellationTokenSource();

            Task close = producer.Close(token.Token);
            token.Cancel();
            _ = await Task.WhenAny(close, Task.Delay(Settle));

            Assert.False(close.IsCompleted, "Close returned while the delivery callback it waits for still ran");
            Assert.Equal(0, key.Disposals);
            Assert.Equal(0, value.Disposals);
            release.Set();
            await TestTimeout.Run(() => close, Deadline);
            Assert.Equal(1, key.Disposals);
            Assert.Equal(1, value.Disposals);
            Assert.Equal(1, key.DestroysSeen);
            Assert.False(pumpAliveAtDisposal, "a serializer was disposed before the pump thread was joined");
            Assert.True(send.IsCompleted, "the teardown did not settle the send's first stage");
        }
        finally
        {
            release.Set();
            await TestTimeout.DisposeAsync(producer);
        }
    }

    [Theory]
    [InlineData(CloseHow)]
    [InlineData(CloseWithTimeoutHow)]
    public async Task TestProducerFamily_test_cancelling_close_waits_for_its_teardown_before_closing_the_serializers_ATokenCancelledBeforeTheCall(
        string how)
    {
        DisposableSerializer key = new DisposableSerializer("key");
        DisposableSerializer value = new DisposableSerializer("value");
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(Unreachable(), key, value);
        try
        {
            using CancellationTokenSource token = new CancellationTokenSource();
            token.Cancel();

            Assert.Throws<OperationCanceledException>(() =>
            {
                _ = how == CloseHow ? producer.Close(token.Token) : producer.Close(s_closeTimeout, token.Token);
            });

            Assert.Equal(0, key.Disposals);
            Assert.Equal(0, value.Disposals);
            Assert.NotEmpty(producer.Metrics());
        }
        finally
        {
            await TestTimeout.DisposeAsync(producer);
        }

        Assert.Equal(1, key.Disposals);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public void AConfigurationValueTheBindingRefuses_DisposesNothing(string flavor)
    {
        DisposableSerializer key = new DisposableSerializer("key");
        DisposableSerializer value = new DisposableSerializer("value");
        Dictionary<string, object?> configs = Unreachable();
        configs["batch.size"] = true;

        ConfigException failure = Assert.Throws<ConfigException>(() => Producer(flavor, configs, key, value));

        Assert.Equal(
            "Invalid value true for configuration batch.size: " +
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
        DisposableSerializer key = new DisposableSerializer("key");
        DisposableSerializer value = new DisposableSerializer("value");

        Assert.Equal("configs", Assert.Throws<ArgumentNullException>(() => Producer(flavor, null!, key, value)).ParamName);
        Assert.Equal(
            "keySerializer",
            Assert.Throws<ArgumentNullException>(() => Producer(flavor, Unreachable(), null!, value)).ParamName);
        Assert.Equal(
            "valueSerializer",
            Assert.Throws<ArgumentNullException>(() => Producer(flavor, Unreachable(), key, null!)).ParamName);

        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task OneInstanceGivenAsKeyAndValue_IsDisposedTwice(string flavor)
    {
        DisposableSerializer both = new DisposableSerializer("both");
        object producer = Producer(flavor, Unreachable(), both, both);
        try
        {
            _ = await CloseBy(producer, CloseHow);
        }
        finally
        {
            await DisposeBounded(producer);
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
        object producer = Producer(
            flavor, Unreachable(), new DisposableSerializer("key", log), new DisposableSerializer("value", log));
        try
        {
            _ = await CloseBy(producer, how);
        }
        finally
        {
            await DisposeBounded(producer);
        }

        Assert.Equal(new[] { "key", "value" }, log.ToArray());
    }

    [Theory]
    [InlineData(SyncFlavor)]
    [InlineData(AsyncFlavor)]
    public async Task ACloseFromADeliveryCallback_DisposesOnTheDrainThread_AfterTheDeferredTeardown(string flavor)
    {
        ConcurrentQueue<string> log = new ConcurrentQueue<string>();
        DisposableSerializer key = new DisposableSerializer("key", log);
        DisposableSerializer value = new DisposableSerializer("value", log);
        object producer = Producer(flavor, Unreachable(), key, value);
        DestroyLog destroys = WatchDestroy(producer);
        key.OnDispose = () => key.DestroysSeen = destroys.Destroys().Count;
        int disposalsWhenCloseReturned = -1;
        ProbeCallback callback = new ProbeCallback((_, _) =>
        {
            if (producer is KafkaProducer<byte[], byte[]> sync)
            {
                sync.Close();
            }
            else
            {
                _ = ((AsyncKafkaProducer<byte[], byte[]>)producer).Close();
            }

            disposalsWhenCloseReturned = key.Disposals + value.Disposals;
        });
        try
        {
            await Send(producer, callback);
            Assert.True(callback.WaitDone(Deadline), "the delivery callback never ran");
            Assert.Null(callback.BodyFailure);
            Assert.True(value.WaitDisposed(Deadline), "the serializers were never disposed after the callback");
        }
        finally
        {
            await DisposeBounded(producer);
        }

        string drainThread = flavor == SyncFlavor ? NativeProducer.CompletionThreadName : NativeProducer.PumpThreadName;
        Assert.Equal(0, disposalsWhenCloseReturned);
        Assert.Equal(new[] { "key", "value" }, log.ToArray());
        Assert.Equal(drainThread, key.ThreadName);
        Assert.Equal(drainThread, value.ThreadName);
        Assert.Equal(1, key.DestroysSeen);
        Assert.Equal(1, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task ASerializerThatIsOnlyIAsyncDisposable_IsNotDisposed(string flavor, string how)
    {
        AsyncOnlySerializer key = new AsyncOnlySerializer();
        AsyncOnlySerializer value = new AsyncOnlySerializer();
        object producer = Producer(flavor, Unreachable(), key, value);
        try
        {
            _ = await CloseBy(producer, how);
        }
        finally
        {
            await DisposeBounded(producer);
        }

        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    [Theory]
    [InlineData(SyncFlavor, CloseHow)]
    [InlineData(SyncFlavor, CloseWithTimeoutHow)]
    [InlineData(SyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, CloseHow)]
    [InlineData(AsyncFlavor, CloseWithTimeoutHow)]
    [InlineData(AsyncFlavor, DisposeHow)]
    [InlineData(AsyncFlavor, DisposeAsyncHow)]
    public async Task TheMockProducers_NeverDisposeTheirSerializers(string flavor, string how)
    {
        DisposableSerializer key = new DisposableSerializer("key");
        DisposableSerializer value = new DisposableSerializer("value");
        object producer = flavor == SyncFlavor
            ? new MockProducer<byte[], byte[]>(key, value)
            : new AsyncMockProducer<byte[], byte[]>(key, value);
        try
        {
            _ = await CloseBy(producer, how);
        }
        finally
        {
            await DisposeBounded(producer);
        }

        Assert.Equal(0, key.Disposals);
        Assert.Equal(0, value.Disposals);
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = MaxBlockMs,
        };

    private static object Producer(
        string flavor,
        IReadOnlyDictionary<string, object?> configs,
        ISerializer<byte[]> key,
        ISerializer<byte[]> value) =>
        flavor == SyncFlavor
            ? new KafkaProducer<byte[], byte[]>(configs, key, value)
            : new AsyncKafkaProducer<byte[], byte[]>(configs, key, value);

    private static async Task<int> CloseBy(object producer, string how)
    {
        int closingThread = -1;
        if (producer is IProducer<byte[], byte[]> sync)
        {
            TestTimeout.Run(
                () =>
                {
                    closingThread = Environment.CurrentManagedThreadId;
                    switch (how)
                    {
                        case CloseHow:
                            sync.Close();
                            break;
                        case CloseWithTimeoutHow:
                            sync.Close(s_closeTimeout);
                            break;
                        default:
                            sync.Dispose();
                            break;
                    }
                },
                Deadline);
            return closingThread;
        }

        IAsyncProducer<byte[], byte[]> async = (IAsyncProducer<byte[], byte[]>)producer;
        switch (how)
        {
            case CloseHow:
                await TestTimeout.Run(() => async.Close(), Deadline);
                break;
            case CloseWithTimeoutHow:
                await TestTimeout.Run(() => async.Close(s_closeTimeout), Deadline);
                break;
            case DisposeHow:
                TestTimeout.Run(
                    () =>
                    {
                        closingThread = Environment.CurrentManagedThreadId;
                        async.Dispose();
                    },
                    Deadline);
                break;
            default:
                await TestTimeout.Run(() => async.DisposeAsync().AsTask(), Deadline);
                break;
        }

        return closingThread;
    }

    private static Task DisposeBounded(object producer)
    {
        if (producer is IAsyncDisposable async)
        {
            return TestTimeout.DisposeAsync(async, Deadline);
        }

        TestTimeout.Dispose((IDisposable)producer, Deadline);
        return Task.CompletedTask;
    }

    private static async Task Send(object producer, IDeliveryCallback callback)
    {
        ProducerRecord<byte[], byte[]> record = Record(Topic("serde-close-in-callback"));
        if (producer is KafkaProducer<byte[], byte[]> sync)
        {
            _ = sync.Send(record, callback);
            return;
        }

        _ = await TestTimeout.Run(
            () => ((AsyncKafkaProducer<byte[], byte[]>)producer).Send(record, callback).AsTask(),
            Deadline);
    }

    private static async Task<(IReadOnlyList<string> Lines, IReadOnlyList<(TraceEventType Type, string Message)> Events)>
        CloseCapturingTheTrace(string flavor, string id, DisposableSerializer key, DisposableSerializer value)
    {
        using TraceCapture trace = new TraceCapture(id);
        object producer = Producer(flavor, Unreachable(), key, value);
        try
        {
            _ = await CloseBy(producer, CloseHow);
        }
        finally
        {
            await DisposeBounded(producer);
        }

        return (trace.Lines(), trace.Events());
    }

    /// <summary>
    /// A serializer that records each <c>Dispose</c>: how many, the thread, its name in a shared log, and an optional
    /// probe run inside it; it can throw from <c>Dispose</c>.
    /// </summary>
    private sealed class DisposableSerializer : ISerializer<byte[]>, IDisposable
    {
        private readonly string _name;
        private readonly ConcurrentQueue<string>? _log;
        private readonly bool _throws;
        private readonly ManualResetEventSlim _disposed = new ManualResetEventSlim();
        private int _disposals;

        internal DisposableSerializer(string name, ConcurrentQueue<string>? log = null, bool throws = false)
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

        public byte[]? Serialize(string topic, byte[] data) => data;

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

        public override string ToString() => "DisposableSerializer(" + _name + ")";

        internal bool WaitDisposed(TimeSpan timeout) => _disposed.Wait(timeout);
    }

    /// <summary>A serializer that is only <see cref="IAsyncDisposable"/>, counting its <c>DisposeAsync</c> calls.</summary>
    private sealed class AsyncOnlySerializer : ISerializer<byte[]>, IAsyncDisposable
    {
        private int _disposals;

        internal int Disposals => Volatile.Read(ref _disposals);

        public byte[]? Serialize(string topic, byte[] data) => data;

        public ValueTask DisposeAsync()
        {
            _ = Interlocked.Increment(ref _disposals);
            return default;
        }
    }
}
