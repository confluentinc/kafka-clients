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
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The delivery callback, <see cref="IDeliveryCallback"/> and <c>Send(record, callback)</c>, on both producer mocks;
/// every behavioural test runs against the sync and the async flavor.
/// </summary>
public sealed class PublicProducerDeliveryCallbackTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "delivery-topic";

    private const string Sync = "sync";

    private const string Async = "async";

    private const string KafkaError = "a KafkaException";

    private const string OtherError = "not a KafkaException";

    private const string CompletionThreadName = "confluent-kafka-producer-completion";

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Success_DeliversTheRealMetadata(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        RecordMetadata returned = await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(
                Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 2),
            callback));

        (RecordMetadata Metadata, Exception? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));

        Assert.Null(completion.Exception);
        Assert.Equal(Topic, completion.Metadata.Topic);
        Assert.Equal(2, completion.Metadata.Partition);
        Assert.Equal(0L, completion.Metadata.Offset);

        // The callback and the caller get the same instance: one completion drives both.
        Assert.Same(returned, completion.Metadata);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Success_NonAsciiTopic_RoundTripsThroughTheDeliveredMetadata(string flavor)
    {
        // A non-ASCII topic guards the UTF-8 copy-out of the native metadata.
        const string NonAscii = "livraison-主题-ünîcödé-🚚";
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(NonAscii, Encoding.UTF8.GetBytes("v"), partition: 0),
            callback));

        (RecordMetadata Metadata, Exception? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));
        Assert.Equal(NonAscii, completion.Metadata.Topic);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ExactlyOnce_OneRecord_InvokesTheCallbackExactlyOnce(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0),
            callback));

        Assert.Single(await SettledCompletions(callback, expected: 1));
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ExactlyOnce_ManyRecords_InvokesTheCallbackOncePerRecord(string flavor)
    {
        // Records sent without awaiting each fire their own callback once.
        const int RecordCount = 16;
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback shared = new RecordingDeliveryCallback();
        RecordingDeliveryCallback[] perRecord = Enumerable.Range(0, RecordCount)
            .Select(_ => new RecordingDeliveryCallback())
            .ToArray();

        List<Task<RecordMetadata>> sends = new List<Task<RecordMetadata>>(RecordCount * 2);
        for (int i = 0; i < RecordCount; i++)
        {
            sends.Add(producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"shared-{i}"), partition: 0),
                shared));
            sends.Add(producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"each-{i}"), partition: 1),
                perRecord[i]));
        }

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        Assert.Equal(RecordCount, (await SettledCompletions(shared, expected: RecordCount)).Count);

        foreach (RecordingDeliveryCallback callback in perRecord)
        {
            Assert.Single(await SettledCompletions(callback, expected: 1));
        }
    }

    [Fact]
    public async Task Ordering_Async_CallbackRunsBeforeTheTaskIsCompleted()
    {
        // Java completes the future after the callbacks run. The probe is the send task's state seen from inside the
        // callback; comparing continuation order would race RunContinuationsAsynchronously.
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        TaskStateProbeCallback probe = new TaskStateProbeCallback();
        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), probe).Delivery();
        probe.Task = sendTask;

        DriveUntilResolved(producer.CompleteNext);
        await WithTimeout(sendTask);

        Assert.True(probe.WasInvoked);
        // Guards against a probe that never ran.
        Assert.False(probe.TaskWasNull);
        Assert.False(probe.TaskWasCompleted);
    }

    [Fact]
    public async Task Ordering_Sync_CallbackRunsBeforeSendReturns()
    {
        // The sync probe: the callback has run, on the completion thread, by the time the blocking Get returns.
        using MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        bool invokedAtReturn = false;
        int getThreadId = 0;
        List<(int Id, string? Name)> threadsAtReturn = new List<(int, string?)>();
        Task<RecordMetadata> sendTask = Task.Run(() =>
        {
            getThreadId = Environment.CurrentManagedThreadId;
            RecordMetadata metadata = producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback).Get();
            invokedAtReturn = callback.Completions.Count == 1;
            threadsAtReturn = callback.Threads;
            return metadata;
        });

        DriveUntilResolved(producer.CompleteNext);
        await WithTimeout(sendTask);

        Assert.True(invokedAtReturn, "the delivery callback had not run when the blocking Get returned");
        (int Id, string? Name) ran = Assert.Single(threadsAtReturn);
        Assert.Equal(CompletionThreadName, ran.Name);
        Assert.NotEqual(getThreadId, ran.Id);
    }

    [Fact]
    public async Task Ordering_Sync_CallbackRunsBeforeSendThrows()
    {
        // On failure too, the callback runs, on the completion thread, before Get throws.
        using MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        bool invokedAtThrow = false;
        int getThreadId = 0;
        List<(int Id, string? Name)> threadsAtThrow = new List<(int, string?)>();
        Task sendTask = Task.Run(() =>
        {
            getThreadId = Environment.CurrentManagedThreadId;
            try
            {
                producer.Send(
                    new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback).Get();
            }
            catch (KafkaException)
            {
                invokedAtThrow = callback.Completions.Count == 1;
                threadsAtThrow = callback.Threads;
            }
        });

        DriveUntilResolved(() => producer.ErrorNext(new KafkaException(2, "ordering-throw", isRetriable: true)));
        await TestTimeout.Run(() => sendTask, s_deadline);

        Assert.True(invokedAtThrow, "the delivery callback had not run when the blocking Get threw");
        (int Id, string? Name) ran = Assert.Single(threadsAtThrow);
        Assert.Equal(CompletionThreadName, ran.Name);
        Assert.NotEqual(getThreadId, ran.Id);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Failure_DeliversPlaceholderMetadataAndTheException(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor, autoComplete: false);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 3), callback);

        // Code 2 = CorruptMessage, which the core classifies retriable / non-fatal.
        DriveUntilResolved(() => producer.ErrorNext(new KafkaException(2, "boom-delivery", isRetriable: true)));

        KafkaException thrown = await Assert.ThrowsAsync<KafkaException>(() => WithTimeout(sendTask));

        (RecordMetadata Metadata, Exception? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));

        // Java's callback never sees null metadata: the -1 placeholder keeps the topic and the record's partition.
        Assert.NotNull(completion.Metadata);
        Assert.Equal(Topic, completion.Metadata.Topic);
        Assert.Equal(3, completion.Metadata.Partition);
        Assert.Equal(-1L, completion.Metadata.Offset);
        Assert.Equal(-1L, completion.Metadata.Timestamp);

        // The callback gets the send's own exception: the same code, message and flags.
        KafkaException delivered = Assert.IsType<KafkaException>(completion.Exception);
        Assert.Equal(2, delivered.Code);
        Assert.Contains("boom-delivery", delivered.Message, StringComparison.Ordinal);
        Assert.True(delivered.IsRetriable);
        Assert.Equal(thrown.Code, delivered.Code);
        Assert.Equal(thrown.Message, delivered.Message);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Failure_WithoutAnExplicitPartition_DeliversThePartitionTheMockChose(string flavor)
    {
        // Java's MockProducer picks partition 0 for a topic its cluster does not know, and its failure
        // placeholder keeps that partition.
        using Flavor producer = Flavor.Create(flavor, autoComplete: false);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v")), callback);

        DriveUntilResolved(() => producer.ErrorNext(new KafkaException(2, "no-partition", isRetriable: true)));
        await Assert.ThrowsAsync<KafkaException>(() => WithTimeout(sendTask));

        (RecordMetadata Metadata, Exception? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));
        Assert.Equal(Topic, completion.Metadata.Topic);
        Assert.Equal(0, completion.Metadata.Partition);
    }

    [Theory]
    [InlineData(Sync, KafkaError)]
    [InlineData(Sync, OtherError)]
    [InlineData(Async, KafkaError)]
    [InlineData(Async, OtherError)]
    public async Task ErrorNext_TheSameInstanceReachesTheCallbackAndTheFuture(string flavor, string kind)
    {
        using Flavor producer = Flavor.Create(flavor, autoComplete: false);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        Exception injected = kind == KafkaError
            ? new KafkaException(2, "the injected instance", isRetriable: true)
            : new InjectedException("the injected instance");

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 1), callback);
        DriveUntilResolved(() => producer.ErrorNext(injected));

        Exception failure = await Assert.ThrowsAnyAsync<Exception>(() => WithTimeout(sendTask));
        Assert.Same(injected, failure);
        (RecordMetadata metadata, Exception? delivered) = Assert.Single(await SettledCompletions(callback, expected: 1));
        Assert.Same(injected, delivered);
        Assert.Equal(1, metadata.Partition);
        Assert.Equal(-1L, metadata.Offset);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NoCallback_OnSynchronousThrow_AfterDispose(string flavor)
    {
        // Java's doSend rethrows a closed-producer error without running the callback.
        Flavor producer = Flavor.Create(flavor);
        producer.Dispose();
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        // Post-dispose stays type-only (the consumer norm asserts no ObjectName).
        Assert.Throws<ObjectDisposedException>(() => producer.SendInline(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback));

        Assert.Empty(callback.Completions);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NoCallback_OnSynchronousThrow_SerializerThrows(string flavor)
    {
        // Java's doSend rethrows a SerializationException without running the callback.
        ThrowingSerializer<byte[]> throwing = new ThrowingSerializer<byte[]>();
        using Flavor producer = Flavor.Create(flavor, valueSerializer: throwing);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        SerializationException error = Assert.Throws<SerializationException>(() => producer.SendInline(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback));

        Assert.Contains("value", error.Message, StringComparison.Ordinal);
        Assert.Contains(Topic, error.Message, StringComparison.Ordinal);
        Assert.IsType<ThrowingSerializer<byte[]>.BoomException>(error.InnerException);
        Assert.Equal(1, throwing.InvocationCount);
        Assert.Empty(callback.Completions);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NoCallback_OnSynchronousThrow_NullRecord(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        // ArgumentNullException(nameof(record)) sets no custom message → ParamName is the contract.
        ArgumentNullException error =
            Assert.Throws<ArgumentNullException>(() => producer.SendInline(null!, callback));

        Assert.Equal("record", error.ParamName);
        Assert.Empty(callback.Completions);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ThrowingCallback_IsSwallowed_AndTheSendStillSucceeds(string flavor)
    {
        // Java logs and swallows a throwing callback: the send is unaffected, and a second send still completes.
        using Flavor producer = Flavor.Create(flavor);
        ThrowingDeliveryCallback throwing = new ThrowingDeliveryCallback("boom-callback");

        RecordMetadata first = await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("first"), partition: 0), throwing));

        Assert.True(throwing.WasInvoked);
        Assert.Equal(Topic, first.Topic);
        Assert.Equal(0L, first.Offset);

        RecordingDeliveryCallback recording = new RecordingDeliveryCallback();
        RecordMetadata second = await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("second"), partition: 0), recording));

        Assert.Equal(1L, second.Offset);
        Assert.Single(await SettledCompletions(recording, expected: 1));
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ThrowingCallback_DoesNotStrandTheOtherRecordsInTheBatch(string flavor)
    {
        // Every record carries a throwing callback, and every send still resolves: one bug must not fail N sends.
        const int RecordCount = 8;
        using Flavor producer = Flavor.Create(flavor);
        ThrowingDeliveryCallback throwing = new ThrowingDeliveryCallback("boom-batch");

        List<Task<RecordMetadata>> sends = new List<Task<RecordMetadata>>(RecordCount);
        for (int i = 0; i < RecordCount; i++)
        {
            sends.Add(producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0), throwing));
        }

        RecordMetadata[] results = null!;
        await TestTimeout.Run(async () => results = await Task.WhenAll(sends), s_deadline);

        Assert.Equal(RecordCount, results.Length);
        Assert.All(results, metadata => Assert.Equal(Topic, metadata.Topic));
        Assert.Equal(RecordCount, throwing.InvocationCount);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ThrowingCallback_IsTraced_NotSilentlyDiscarded(string flavor)
    {
        // The throw is also written to Trace; a per-test marker keeps parallel tests apart.
        string marker = "delivery-trace-" + Guid.NewGuid().ToString("N");
        CapturingTraceListener listener = new CapturingTraceListener();
        Trace.Listeners.Add(listener);
        try
        {
            using Flavor producer = Flavor.Create(flavor);
            await WithTimeout(producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0),
                new ThrowingDeliveryCallback(marker)));
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }

        string line = await SettledTraceLine(listener, marker);

        // The trace names the delivery callback, not the trampoline or the offset-commit callback.
        Assert.Contains("IDeliveryCallback", line, StringComparison.Ordinal);
        Assert.DoesNotContain("IOffsetCommitCallback", line, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Callback_MaySendAgainFromInsideItself(string flavor)
    {
        // Retrying from inside the callback, the canonical use, must not deadlock on either flavor.
        using Flavor producer = Flavor.Create(flavor);
        ReentrantDeliveryCallback reentrant = new ReentrantDeliveryCallback(
            () => producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("retry"), partition: 1),
                new RecordingDeliveryCallback()));

        RecordMetadata first = await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("first"), partition: 0),
            reentrant));

        Assert.Equal(0, first.Partition);

        Task<RecordMetadata>? retry = null;
        Stopwatch elapsed = Stopwatch.StartNew();
        while (retry is null && elapsed.Elapsed < s_deadline)
        {
            retry = reentrant.Reentrant;
            await Task.Delay(2);
        }

        Assert.NotNull(retry);
        RecordMetadata second = await WithTimeout(retry!);
        Assert.Equal(1, second.Partition);
    }

    [Fact]
    public async Task Cancellation_Async_CallbackStillFiresAfterAGetWaitWasCanceled()
    {
        // A token that ends the caller's Get wait leaves the record and its callback alone.
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using CancellationTokenSource cts = new CancellationTokenSource();
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(
            () => producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0),
                callback).AsTask(),
            s_deadline);
        Task<RecordMetadata> wait = future.Get(cts.Token);

        cts.Cancel();
        OperationCanceledException canceled =
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => WithTimeout(wait));
        Assert.Equal(cts.Token, canceled.CancellationToken);
        Assert.Empty(callback.Completions);
        Assert.False(future.IsDone);

        Assert.True(producer.CompleteNext());

        (RecordMetadata Metadata, Exception? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));
        Assert.Null(completion.Exception);
        Assert.Equal(Topic, completion.Metadata.Topic);
        Assert.Same(completion.Metadata, await WithTimeout(future.Get()));
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NullCallback_ThrowsArgumentNullException(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor);

        // ArgumentNullException(nameof(callback)) sets no custom message, so ParamName is the contract.
        ArgumentNullException error = Assert.Throws<ArgumentNullException>(() => producer.SendInline(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback: null));

        Assert.Equal("callback", error.ParamName);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NullRecord_IsReportedBeforeANullCallback(string flavor)
    {
        // Java's precondition order: the record, then the callback, then the closed check.
        using Flavor producer = Flavor.Create(flavor);

        ArgumentNullException error =
            Assert.Throws<ArgumentNullException>(() => producer.SendInline(null!, callback: null));
        Assert.Equal("record", error.ParamName);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NullCallback_IsReportedBeforeTheDisposedCheck(string flavor)
    {
        // The argument guards precede the closed check.
        Flavor producer = Flavor.Create(flavor);
        producer.Dispose();

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(() => producer.SendInline(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback: null));
        Assert.Equal("callback", error.ParamName);
    }

    [Theory]
    [InlineData(typeof(IProducer<byte[], byte[]>))]
    [InlineData(typeof(KafkaProducer<byte[], byte[]>))]
    [InlineData(typeof(MockProducer<byte[], byte[]>))]
    public void SyncSurfaces_DeclareTheCallbackOverload(Type surface)
    {
        // The real clients need a broker, so this pins the shape on every class; the mocks cover the behaviour.
        MethodInfo? method = FindSend(
            surface, typeof(ProducerRecord<byte[], byte[]>), typeof(IDeliveryCallback));

        Assert.NotNull(method);

        // Pinned by reflection: `producer.Send(r, cb);` compiles against any return type.
        Assert.Equal(typeof(KafkaFuture<RecordMetadata>), method!.ReturnType);
    }

    [Theory]
    [InlineData(typeof(IProducer<byte[], byte[]>))]
    [InlineData(typeof(KafkaProducer<byte[], byte[]>))]
    [InlineData(typeof(MockProducer<byte[], byte[]>))]
    public void SyncSurfaces_PlainSend_ReturnsKafkaFuture(Type surface)
    {
        // The callback-less overload has the same return type: both are one Java send.
        MethodInfo? method = FindSend(surface, typeof(ProducerRecord<byte[], byte[]>));

        Assert.NotNull(method);
        Assert.Equal(typeof(KafkaFuture<RecordMetadata>), method!.ReturnType);
    }

    [Theory]
    [InlineData(typeof(IAsyncProducer<byte[], byte[]>))]
    [InlineData(typeof(AsyncKafkaProducer<byte[], byte[]>))]
    [InlineData(typeof(AsyncMockProducer<byte[], byte[]>))]
    public void AsyncSurfaces_DeclareTheCallbackOverload(Type surface)
    {
        MethodInfo? method = FindSend(
            surface,
            typeof(ProducerRecord<byte[], byte[]>),
            typeof(IDeliveryCallback),
            typeof(CancellationToken));

        Assert.NotNull(method);

        // The two-stage shape, pinned by reflection: `await producer.Send(r);` compiles against either shape.
        Assert.Equal(typeof(ValueTask<AsyncKafkaFuture<RecordMetadata>>), method!.ReturnType);

        // The CancellationToken keeps its default, so `Send(record, callback)` compiles on the
        // async surface too.
        Assert.True(method.GetParameters()[2].HasDefaultValue);
    }

    [Theory]
    [InlineData(typeof(IAsyncProducer<byte[], byte[]>))]
    [InlineData(typeof(AsyncKafkaProducer<byte[], byte[]>))]
    [InlineData(typeof(AsyncMockProducer<byte[], byte[]>))]
    public void AsyncSurfaces_PlainSend_ReturnsTheTwoStageShape(Type surface)
    {
        // The callback-less overload has the same two-stage shape.
        MethodInfo? method = FindSend(
            surface,
            typeof(ProducerRecord<byte[], byte[]>),
            typeof(CancellationToken));

        Assert.NotNull(method);
        Assert.Equal(typeof(ValueTask<AsyncKafkaFuture<RecordMetadata>>), method!.ReturnType);
        Assert.True(method.GetParameters()[1].HasDefaultValue);
    }

    [Fact]
    public void OnCompletion_ReturnsVoid_NotTask()
    {
        // The callback stays a sync void, as Java's, and its overload is kept beside the future.
        Assert.Equal(
            typeof(void),
            typeof(IDeliveryCallback).GetMethod(nameof(IDeliveryCallback.OnCompletion))!.ReturnType);

        ParameterInfo[] parameters =
            typeof(IDeliveryCallback).GetMethod(nameof(IDeliveryCallback.OnCompletion))!.GetParameters();
        Assert.Equal(typeof(RecordMetadata), parameters[0].ParameterType);
        Assert.Equal(typeof(Exception), parameters[1].ParameterType);
    }

    // Type.GetMethod on an interface does not search its base interfaces.
    private static MethodInfo? FindSend(Type surface, params Type[] parameters) =>
        surface.GetMethod("Send", parameters)
        ?? surface.GetInterfaces()
            .Select(i => i.GetMethod("Send", parameters))
            .FirstOrDefault(m => m is not null);

    // Waits for the expected count, then a settle window, so a second invocation fails instead of passing.
    private static async Task<List<(RecordMetadata Metadata, Exception? Exception)>> SettledCompletions(
        RecordingDeliveryCallback callback, int expected)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (callback.Completions.Count < expected && elapsed.Elapsed < s_deadline)
        {
            await Task.Delay(2).ConfigureAwait(false);
        }

        Assert.Equal(expected, callback.Completions.Count);

        await Task.Delay(100).ConfigureAwait(false);
        Assert.Equal(expected, callback.Completions.Count);

        return callback.Completions;
    }

    // The trace is written on the completion thread, so it can lag the awaited send.
    private static async Task<string> SettledTraceLine(CapturingTraceListener listener, string marker)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < s_deadline)
        {
            string? found = listener.Snapshot()
                .FirstOrDefault(line => line.Contains(marker, StringComparison.Ordinal));
            if (found is not null)
            {
                return found;
            }

            await Task.Delay(2).ConfigureAwait(false);
        }

        throw new TimeoutException("the swallowed delivery-callback failure was never traced");
    }

    // The sync flavor sends on a worker, so an early CompleteNext or ErrorNext can find nothing pending.
    private static void DriveUntilResolved(Func<bool> drive)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!drive())
        {
            if (elapsed.Elapsed > s_deadline)
            {
                throw new TimeoutException(
                    "No pending send registered to drive within the deadline — treated as a hang.");
            }

            Thread.Sleep(2);
        }
    }

    private static async Task<RecordMetadata> WithTimeout(Task<RecordMetadata> task)
    {
        Task winner = await Task.WhenAny(task, Task.Delay(s_deadline)).ConfigureAwait(false);
        if (winner != task)
        {
            throw new TimeoutException("Send did not complete within the deadline — treated as a hang.");
        }

        return await task.ConfigureAwait(false);
    }

    /// <summary>The exception a test injects.</summary>
    private sealed class InjectedException : Exception
    {
        internal InjectedException(string message)
            : base(message)
        {
        }
    }

    /// <summary>Records each completion and the thread it ran on.</summary>
    private sealed class RecordingDeliveryCallback : IDeliveryCallback
    {
        private readonly List<(RecordMetadata Metadata, Exception? Exception)> _completions =
            new List<(RecordMetadata, Exception?)>();

        private readonly List<(int Id, string? Name)> _threads = new List<(int, string?)>();

        // Locked: the callback runs on the completion thread.
        internal List<(RecordMetadata Metadata, Exception? Exception)> Completions
        {
            get
            {
                lock (_completions)
                {
                    return new List<(RecordMetadata, Exception?)>(_completions);
                }
            }
        }

        // Index-aligned with Completions.
        internal List<(int Id, string? Name)> Threads
        {
            get
            {
                lock (_completions)
                {
                    return new List<(int, string?)>(_threads);
                }
            }
        }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            // Record only: an assertion thrown here would be swallowed.
            lock (_completions)
            {
                _completions.Add((metadata, exception));
                _threads.Add((Environment.CurrentManagedThreadId, Thread.CurrentThread.Name));
            }
        }
    }

    /// <summary>A callback that throws.</summary>
    private sealed class ThrowingDeliveryCallback : IDeliveryCallback
    {
        private readonly string _message;

        private int _invocationCount;

        internal ThrowingDeliveryCallback(string message)
        {
            _message = message;
        }

        internal bool WasInvoked => Volatile.Read(ref _invocationCount) > 0;

        internal int InvocationCount => Volatile.Read(ref _invocationCount);

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Interlocked.Increment(ref _invocationCount);
            throw new InvalidOperationException(_message);
        }
    }

    /// <summary>Sends again from inside the callback and captures any throw.</summary>
    private sealed class ReentrantDeliveryCallback : IDeliveryCallback
    {
        private readonly Func<Task<RecordMetadata>> _send;

        internal ReentrantDeliveryCallback(Func<Task<RecordMetadata>> send)
        {
            _send = send;
        }

        internal Task<RecordMetadata>? Reentrant { get; private set; }

        public void OnCompletion(RecordMetadata metadata, Exception? exception) =>
            Reentrant = _send();
    }

    /// <summary>Records the send task's state as seen from inside the callback.</summary>
    private sealed class TaskStateProbeCallback : IDeliveryCallback
    {
        internal Task<RecordMetadata>? Task { get; set; }

        internal bool WasInvoked { get; private set; }

        internal bool TaskWasNull { get; private set; }

        internal bool TaskWasCompleted { get; private set; }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Task<RecordMetadata>? sendTask = Task;
            TaskWasNull = sendTask is null;
            TaskWasCompleted = sendTask is not null && sendTask.IsCompleted;
            WasInvoked = true;
        }
    }

    /// <summary>Captures what is written to <see cref="Trace"/>; tests match it on a per-test marker.</summary>
    private sealed class CapturingTraceListener : TraceListener
    {
        private readonly StringBuilder _pending = new StringBuilder();

        private readonly List<string> _lines = new List<string>();

        internal List<string> Snapshot()
        {
            lock (_lines)
            {
                return new List<string>(_lines);
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
            lock (_lines)
            {
                _pending.Append(message);
                _lines.Add(_pending.ToString());
                _pending.Clear();
            }
        }
    }

    /// <summary>
    /// One shape over the sync and the async mock, so each behavioural test runs against both. The sync flavor runs
    /// the blocking <c>Send(...).Get()</c> on a worker; <c>SendInline</c> sends on the calling thread.
    /// </summary>
    private abstract class Flavor : IDisposable
    {
        internal static Flavor Create(
            string flavor,
            bool autoComplete = true,
            ISerializer<byte[]>? valueSerializer = null)
        {
            ISerializer<byte[]> value = valueSerializer ?? Serdes.ByteArray;
            return flavor == Sync
                ? new SyncFlavor(autoComplete, value)
                : new AsyncFlavor(autoComplete, value);
        }

        internal abstract Task<RecordMetadata> Send(
            ProducerRecord<byte[], byte[]> record, IDeliveryCallback callback);

        internal abstract void SendInline(
            ProducerRecord<byte[], byte[]> record, IDeliveryCallback? callback);

        internal abstract bool CompleteNext();

        internal abstract bool ErrorNext(Exception exception);

        public abstract void Dispose();

        /// <summary>The flavor over <see cref="MockProducer{TKey, TValue}"/>.</summary>
        private sealed class SyncFlavor : Flavor
        {
            private readonly MockProducer<byte[], byte[]> _producer;

            internal SyncFlavor(bool autoComplete, ISerializer<byte[]> valueSerializer)
            {
                _producer = new MockProducer<byte[], byte[]>(
                    Serdes.ByteArray, valueSerializer, autoComplete);
            }

            internal override Task<RecordMetadata> Send(
                ProducerRecord<byte[], byte[]> record, IDeliveryCallback callback) =>
                Task.Run(() => _producer.Send(record, callback).Get());

            // The future is discarded: every caller asserts a synchronous throw out of Send itself.
            internal override void SendInline(
                ProducerRecord<byte[], byte[]> record, IDeliveryCallback? callback) =>
                _ = _producer.Send(record, callback!);

            internal override bool CompleteNext() => _producer.CompleteNext();

            internal override bool ErrorNext(Exception exception) =>
                _producer.ErrorNext(exception);

            public override void Dispose() => _producer.Dispose();
        }

        /// <summary>The flavor over <see cref="AsyncMockProducer{TKey, TValue}"/>.</summary>
        private sealed class AsyncFlavor : Flavor
        {
            private readonly AsyncMockProducer<byte[], byte[]> _producer;

            internal AsyncFlavor(bool autoComplete, ISerializer<byte[]> valueSerializer)
            {
                _producer = new AsyncMockProducer<byte[], byte[]>(
                    Serdes.ByteArray, valueSerializer, autoComplete);
            }

            internal override Task<RecordMetadata> Send(
                ProducerRecord<byte[], byte[]> record, IDeliveryCallback callback) =>
                _producer.Send(record, callback).Delivery();

            internal override void SendInline(
                ProducerRecord<byte[], byte[]> record, IDeliveryCallback? callback) =>
                _ = _producer.Send(record, callback!);

            internal override bool CompleteNext() => _producer.CompleteNext();

            internal override bool ErrorNext(Exception exception) =>
                _producer.ErrorNext(exception);

            public override void Dispose() => _producer.Dispose();
        }
    }
}
