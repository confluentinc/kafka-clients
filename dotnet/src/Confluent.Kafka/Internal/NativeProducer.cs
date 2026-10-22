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
using System.Globalization;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The native producer behind every producer class: it owns the producer's handle and its completion thread, counts
/// the calls in flight, and tears down in order: refuse new calls, close, join the completion thread, wait for the
/// calls in flight, then destroy on the closing thread.
/// </summary>
internal sealed class NativeProducer : IDisposable, IAsyncDisposable
{
    internal const string CompletionThreadName = "confluent-kafka-producer-completion";

    internal const string PumpThreadName = "confluent-kafka-producer-pump";

    private const string RecordNotBuilt = "The producer record could not be built.";

    private const string MaxBlockMsConfig = "max.block.ms";

    private const long DefaultMaxBlockMs = 60000;

    private const string FlushInCallbackMessage =
        "KafkaProducer.flush() invocation inside a callback is not permitted because it may lead to deadlock.";

    private const string CallbackWaitTimeoutMessage =
        "Timeout expired after {0}ms while awaiting {1} from a send() callback, which blocks the producer's " +
        "completion thread";

    private const string AsyncCallbackWaitTimeoutMessage =
        "Timeout expired after {0}ms while awaiting {1} from an async send() callback, whose own report it waits for";

    private const string CloseNegativeTimeoutMessage = "The timeout cannot be negative.";

    private const string CloseInCallbackMessage =
        "Overriding close timeout {0} ms to 0 ms in order to prevent useless blocking due to self-join. This means " +
        "you have incorrectly invoked close with a non-zero timeout from the producer call-back.";

    private delegate T MockCall<T>(IntPtr mock, ref IntPtr outErr);

    private static readonly NativeMethods.ProducerVoidCallback s_voidCompletion = OnVoidCompletion;

    private static readonly NativeMethods.ProducerListCallback s_listCompletion = OnListCompletion;

    private readonly SafeProducerHandle _handle;
    private readonly CompletionThread _completion;
    private readonly DeliveryReporter _reporter;
    private readonly long _maxBlockMs;
    private readonly HandleUses _uses = new HandleUses();
    private int _closed;

    private NativeProducer(SafeProducerHandle handle, string completionThreadName, long maxBlockMs)
    {
        _handle = handle;
        _maxBlockMs = maxBlockMs;
        _completion = new CompletionThread(completionThreadName, DrainOf(handle, _uses));
        _reporter = new DeliveryReporter(handle, _completion);
        handle.ReleaseAfterDestroy(AfterDestroy);
    }

    internal SafeProducerHandle Handle
    {
        get
        {
            ThrowIfClosed();
            return _handle;
        }
    }

    internal static NativeProducer Create(
        IReadOnlyDictionary<string, string> config,
        string completionThreadName = CompletionThreadName)
    {
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        foreach (KeyValuePair<string, string> entry in config)
        {
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    $"Configuration value for key '{entry.Key}' must not be null.",
                    nameof(config));
            }
        }

        IntPtr classPointer = NewKafkaProducer(config);
        IntPtr outErr = IntPtr.Zero;
        IntPtr view = NativeMethods.KafkaProducerAsProducer(classPointer, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.KafkaProducerDestroy(classPointer, ref destroyErr);
            FreeError(ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return Start(new SafeKafkaProducerHandle(view, classPointer), completionThreadName, MaxBlockMsOf(config));
    }

    internal static NativeProducer CreateMock(
        bool autoComplete = true,
        string completionThreadName = CompletionThreadName)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr classPointer = NativeMethods.MockProducerWithAutoComplete(autoComplete ? (sbyte)1 : (sbyte)0, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr view = NativeMethods.MockProducerAsProducer(classPointer, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.MockProducerDestroy(classPointer, ref destroyErr);
            FreeError(ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        return Start(new SafeMockProducerHandle(view, classPointer), completionThreadName, DefaultMaxBlockMs);
    }

    internal Task FlushWithCallback(CancellationToken cancellationToken = default)
    {
        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();
        if (IsMock)
        {
            if (_completion.IsDraining)
            {
                return FlushBlockingAsTask();
            }

            if (InCallback)
            {
                return CompletePendingAsTask();
            }
        }
        else if (InCallback)
        {
            return Task.FromException(FlushInCallback());
        }

        TimeSpan timeout = ReportWaitTimeout;
        string format = CallbackWaitFormat;
        AsyncCompletion<IntPtr> done = new AsyncCompletion<IntPtr>(FreeError);
        Submit(
            new Action<IntPtr>(done.Complete),
            opaque => NativeMethods.ProducerFlushCb(_handle, s_voidCompletion, opaque));
        return AwaitVoid(done, timeout, format, "flush", cancellationToken);
    }

    internal Task<IReadOnlyList<PartitionInfo>> PartitionsForWithCallback(
        string topic,
        CancellationToken cancellationToken = default)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        ThrowIfClosed();
        cancellationToken.ThrowIfCancellationRequested();
        if (IsMock && _completion.IsCurrentThread)
        {
            return PartitionsForBlockingAsTask(topic);
        }

        TimeSpan timeout = WaitTimeout;
        string format = CallbackWaitFormat;
        AsyncCompletion<(IntPtr Value, IntPtr Error)> done =
            new AsyncCompletion<(IntPtr Value, IntPtr Error)>(FreeListPayload);
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic))
        {
            IntPtr topicPointer = topicPin.Pointer;
            Submit(
                new Action<IntPtr, IntPtr>((value, error) => done.Complete((value, error))),
                opaque => NativeMethods.ProducerPartitionsForCb(_handle, topicPointer, s_listCompletion, opaque));
        }

        return AwaitPartitions(done, timeout, format, topic, cancellationToken);
    }

    internal ValueTask<AsyncKafkaFuture<RecordMetadata>> SendViaPump(
        SerializedProducerRecord record,
        IDeliveryCallback? callback,
        CancellationToken cancellationToken = default,
        IAsyncDeliveryCallback? asyncCallback = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnterUse();
        try
        {
            AsyncDeliveryState state = new AsyncDeliveryState(
                _reporter, callback, asyncCallback, record.Topic, record.Partition ?? -1, cancellationToken);
            KeepIfMock(state);
            IntPtr outErr = IntPtr.Zero;
            if (!state.Send(_handle, in record, ref outErr))
            {
                KafkaException.ThrowIfFfiFailed(ref outErr);
                throw new InvalidOperationException(RecordNotBuilt);
            }

            return state.FirstStage();
        }
        finally
        {
            ExitUse();
        }
    }

    internal KafkaFuture<RecordMetadata> Send(SerializedProducerRecord record, IDeliveryCallback? callback)
    {
        EnterUse();
        try
        {
            DeliveryState state = new DeliveryState(_reporter, callback, record.Topic, record.Partition ?? -1);
            KeepIfMock(state);
            IntPtr outErr = IntPtr.Zero;
            IntPtr future;
            bool sent;
            try
            {
                future = ProducerSendMarshal.Send(_handle, in record, state, ref outErr, out sent);
            }
            catch
            {
                state.Release();
                throw;
            }

            DeliveryState.DestroyFuture(future);
            if (!sent)
            {
                state.Release();
                KafkaException.ThrowIfFfiFailed(ref outErr);
                throw new InvalidOperationException(RecordNotBuilt);
            }

            if (outErr != IntPtr.Zero)
            {
                state.KeepUntilDestroy();
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            return new KafkaFuture<RecordMetadata>(state.Future);
        }
        finally
        {
            ExitUse();
        }
    }

    // The core's mock drops a send it never completes (Clear, destroy) without its callback, so the state is kept
    // until the destroy has returned.
    private void KeepIfMock(DeliveryState state)
    {
        if (IsMock)
        {
            state.KeepUntilDestroy();
        }
    }

    internal void Flush()
    {
        ThrowIfClosed();
        if (IsMock)
        {
            if (_completion.IsDraining)
            {
                FlushBlocking();
                return;
            }

            if (InCallback)
            {
                CompletePending();
                return;
            }
        }
        else if (InCallback)
        {
            throw FlushInCallback();
        }

        Completion<IntPtr> done = new Completion<IntPtr>(FreeError);
        IntPtr error = WaitQueued(
            done,
            new Action<IntPtr>(done.Complete),
            opaque => NativeMethods.ProducerFlushCb(_handle, s_voidCompletion, opaque),
            "flush",
            waitsForReports: true);
        KafkaException.ThrowIfFfiFailed(ref error);
    }

    internal IReadOnlyList<PartitionInfo> PartitionsFor(string topic)
    {
        if (topic is null)
        {
            throw new ArgumentNullException(nameof(topic));
        }

        ThrowIfClosed();
        if (IsMock && _completion.IsCurrentThread)
        {
            return PartitionsForBlocking(topic);
        }

        Completion<(IntPtr Value, IntPtr Error)> done = new Completion<(IntPtr Value, IntPtr Error)>(FreeListPayload);
        (IntPtr Value, IntPtr Error) payload;
        using (Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic))
        {
            IntPtr topicPointer = topicPin.Pointer;
            payload = WaitQueued(
                done,
                new Action<IntPtr, IntPtr>((value, error) => done.Complete((value, error))),
                opaque => NativeMethods.ProducerPartitionsForCb(_handle, topicPointer, s_listCompletion, opaque),
                MetadataOf(topic));
        }

        return PartitionsOf(payload);
    }

    internal IReadOnlyDictionary<MetricName, IMetric> Metrics()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr map;
        EnterUse();
        try
        {
            map = NativeMethods.ProducerMetrics(_handle, ref outErr);
        }
        finally
        {
            ExitUse();
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return map == IntPtr.Zero
            ? new Dictionary<MetricName, IMetric>()
            : KafkaMetricMapMarshal.CopyOutAndDestroy(map);
    }

    internal bool MockCompleteNext() =>
        CallMock((IntPtr mock, ref IntPtr outErr) => NativeMethods.MockProducerCompleteNext(mock, ref outErr)) != 0;

    // A null exception is refused, although Java's errorNext(null) completes the send: CompleteNext spells that.
    internal bool MockErrorNext(Exception exception)
    {
        if (exception is null)
        {
            throw new ArgumentNullException(nameof(exception));
        }

        return CallMock((IntPtr mock, ref IntPtr outErr) =>
            NativeMethods.MockProducerErrorNext(mock, ManagedExceptionCarrier.ToError(exception), ref outErr)) != 0;
    }

    internal int MockHistoryCount()
    {
        IntPtr history = CallMock((IntPtr mock, ref IntPtr outErr) => NativeMethods.MockProducerHistory(mock, ref outErr));
        IntPtr sizeErr = IntPtr.Zero;
        int count;
        try
        {
            count = history == IntPtr.Zero ? 0 : NativeMethods.ListSize(history, ref sizeErr);
        }
        finally
        {
            FreeList(history);
        }

        KafkaException.ThrowIfFfiFailed(ref sizeErr);
        return count;
    }

    internal void MockClear() =>
        CallMock((IntPtr mock, ref IntPtr outErr) =>
        {
            NativeMethods.MockProducerClear(mock, ref outErr);
            return 0;
        });

    internal Task CloseWithCallback(CancellationToken cancellationToken = default) =>
        CloseWithCallbackWithin(null, cancellationToken);

    internal Task CloseWithCallback(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        CloseWithCallbackWithin(CloseTimeoutMs(timeout), cancellationToken);

    internal void Close() => CloseWithin(null);

    internal void Close(TimeSpan timeout) => CloseWithin(CloseTimeoutMs(timeout));

    public void Dispose()
    {
        FlushOpenMock();
        if (!TryBeginClose())
        {
            return;
        }

        if (InCallback)
        {
            CloseFromCallback(null);
            return;
        }

        try
        {
            CloseAndTearDown(null);
        }
        catch (Exception)
        {
            // Dispose does not throw; Close reports the same error.
        }
    }

    public async ValueTask DisposeAsync()
    {
        await FlushOpenMockAsync().ConfigureAwait(false);
        if (!TryBeginClose())
        {
            return;
        }

        if (InCallback)
        {
            CloseFromCallback(null);
            return;
        }

        try
        {
            await CloseAndTearDownAsync(null).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // DisposeAsync does not throw; Close reports the same error.
        }
    }

    // A mock completes its pending sends before it closes, as Python's mock does on exit.
    private void FlushOpenMock()
    {
        if (!IsMock || Volatile.Read(ref _closed) != 0)
        {
            return;
        }

        try
        {
            Flush();
        }
        catch (Exception)
        {
            // Dispose does not throw; Flush reports the same error.
        }
    }

    private async Task FlushOpenMockAsync()
    {
        if (!IsMock || Volatile.Read(ref _closed) != 0)
        {
            return;
        }

        try
        {
            await FlushWithCallback().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // DisposeAsync does not throw; Flush reports the same error.
        }
    }

    internal void ThrowIfClosed()
    {
        if (Volatile.Read(ref _closed) != 0)
        {
            throw new ObjectDisposedException(nameof(NativeProducer));
        }
    }

    private static IntPtr NewKafkaProducer(IReadOnlyDictionary<string, string> config)
    {
        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>(config.Count * 2);
        IntPtr map = IntPtr.Zero;
        IntPtr producerConfig = IntPtr.Zero;
        IntPtr outErr = IntPtr.Zero;
        try
        {
            map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (KeyValuePair<string, string> entry in config)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                pins.Add(key);
                Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                pins.Add(value);
                NativeMethods.MapPut(map, key.Pointer, value.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            producerConfig = NativeMethods.ProducerConfigNew(map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr producer = NativeMethods.KafkaProducerNew(producerConfig, IntPtr.Zero, IntPtr.Zero, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return producer;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            if (producerConfig != IntPtr.Zero)
            {
                NativeMethods.ProducerConfigDestroy(producerConfig, ref destroyErr);
                FreeError(ref destroyErr);
            }

            foreach (Utf8Marshal.PinnedUtf8String pin in pins)
            {
                pin.Dispose();
            }

            if (map != IntPtr.Zero)
            {
                NativeMethods.MapDestroy(map, ref destroyErr);
                FreeError(ref destroyErr);
            }
        }
    }

    private static long MaxBlockMsOf(IReadOnlyDictionary<string, string> config) =>
        config.TryGetValue(MaxBlockMsConfig, out string? value)
        && long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out long maxBlockMs)
            ? maxBlockMs
            : DefaultMaxBlockMs;

    private static NativeProducer Start(SafeProducerHandle handle, string completionThreadName, long maxBlockMs)
    {
        NativeProducer producer = new NativeProducer(handle, completionThreadName, maxBlockMs);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ProducerSetCallbacksNotify(
                handle, CompletionThread.NotifyHook, producer._completion.NotifyOpaque, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        catch
        {
            Interlocked.Exchange(ref producer._closed, 1);
            producer.TearDown();
            throw;
        }

        return producer;
    }

    // The drain holds the handle weakly so that a producer never closed can still reach its finalizer and be leaked
    // with a warning. It is a use of the handle that a close does not refuse, since the close's own completion needs
    // it; only the destroy does.
    private static Func<int> DrainOf(SafeProducerHandle handle, HandleUses uses)
    {
        WeakReference<SafeProducerHandle> weak = new WeakReference<SafeProducerHandle>(handle);
        return () =>
        {
            if (!weak.TryGetTarget(out SafeProducerHandle? target) || !uses.TryEnterUnlessDestroyed())
            {
                return 0;
            }

            try
            {
                return ExecuteCallbacks(target);
            }
            finally
            {
                if (uses.Exit())
                {
                    uses.Wake();
                }
            }
        };
    }

    private static int ExecuteCallbacks(SafeProducerHandle handle)
    {
        IntPtr outErr = IntPtr.Zero;
        int ran = NativeMethods.ProducerExecuteCallbacks(handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return ran;
    }

    private static void FreeError(IntPtr error)
    {
        if (error != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    private static void FreeError(ref IntPtr error)
    {
        FreeError(error);
        error = IntPtr.Zero;
    }

    private static void FreeList(IntPtr list)
    {
        if (list != IntPtr.Zero)
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.ListDestroy(list, ref outErr);
            FreeError(outErr);
        }
    }

    private static void FreeListPayload((IntPtr Value, IntPtr Error) payload)
    {
        FreeList(payload.Value);
        FreeError(payload.Error);
    }

    private static IReadOnlyList<PartitionInfo> PartitionsOf((IntPtr Value, IntPtr Error) payload)
    {
        IntPtr error = payload.Error;
        if (error != IntPtr.Zero)
        {
            FreeList(payload.Value);
            KafkaException.ThrowIfFfiFailed(ref error);
        }

        return payload.Value == IntPtr.Zero
            ? Array.Empty<PartitionInfo>()
            : PartitionInfoListMarshal.CopyOutAndDestroy(payload.Value);
    }

    private static string MetadataOf(string topic) => "the metadata of topic " + topic;

    private static Exception FlushInCallback()
    {
        using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin(FlushInCallbackMessage);
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorKafkaMessageSource(message.Pointer, IntPtr.Zero, ref outErr);
        return CoreError(error, ref outErr);
    }

    private static Exception CoreError(IntPtr error, ref IntPtr outErr)
    {
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return KafkaException.FromFfiError(error, owned: true)!;
    }

    private static void OnVoidCompletion(IntPtr error, IntPtr opaque)
    {
        try
        {
            GCHandle gcHandle = GCHandle.FromIntPtr(opaque);
            Action<IntPtr> completion = (Action<IntPtr>)gcHandle.Target!;
            gcHandle.Free();
            completion(error);
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    private static void OnListCompletion(IntPtr value, IntPtr error, IntPtr opaque)
    {
        try
        {
            GCHandle gcHandle = GCHandle.FromIntPtr(opaque);
            Action<IntPtr, IntPtr> completion = (Action<IntPtr, IntPtr>)gcHandle.Target!;
            gcHandle.Free();
            completion(value, error);
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    private static void SubmitOwned(Delegate completion, Action<IntPtr> submit)
    {
        GCHandle gcHandle = GCHandle.Alloc(completion);
        try
        {
            submit(GCHandle.ToIntPtr(gcHandle));
        }
        catch
        {
            gcHandle.Free();
            throw;
        }
    }

    private static void TraceCloseOverride(long timeoutMs)
    {
        if (timeoutMs <= 0)
        {
            return;
        }

        try
        {
            Trace.TraceWarning(string.Format(CultureInfo.InvariantCulture, CloseInCallbackMessage, timeoutMs));
        }
        catch (Exception)
        {
            // A throwing TraceListener must not stop the close.
        }
    }

    private long CloseTimeoutMs(TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero && !IsMock)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout), CloseNegativeTimeoutMessage);
        }

        return (long)timeout.TotalMilliseconds;
    }

    private bool IsMock => _handle is SafeMockProducerHandle;

    private bool InCallback => _completion.IsCurrentThread || _reporter.InAsyncCallback;

    private TimeSpan WaitTimeout => _completion.IsCurrentThread ? MaxBlock : Timeout.InfiniteTimeSpan;

    // A wait that also waits for the reports cannot end inside an async callback, which reports only once it ends.
    private TimeSpan ReportWaitTimeout =>
        _completion.IsCurrentThread || _reporter.InAsyncCallback ? MaxBlock : Timeout.InfiniteTimeSpan;

    private TimeSpan MaxBlock => TimeSpan.FromMilliseconds(Math.Min(_maxBlockMs, int.MaxValue));

    private string CallbackWaitFormat =>
        _completion.IsCurrentThread ? CallbackWaitTimeoutMessage : AsyncCallbackWaitTimeoutMessage;

    private Exception CallbackWaitTimeout(string format, string operation)
    {
        using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin(
            string.Format(CultureInfo.InvariantCulture, format, _maxBlockMs, operation));
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorTimeout(message.Pointer, ref outErr);
        return CoreError(error, ref outErr);
    }

    // One use spans the submit only: the destroy fires a completion still pending, so it never waits for one.
    private void Submit(Delegate completion, Action<IntPtr> submit)
    {
        EnterUse();
        try
        {
            SubmitOwned(completion, submit);
        }
        finally
        {
            ExitUse();
        }
    }

    private T WaitQueued<T>(
        Completion<T> done,
        Delegate completion,
        Action<IntPtr> submit,
        string operation,
        bool waitsForReports = false)
    {
        TimeSpan timeout = waitsForReports ? ReportWaitTimeout : WaitTimeout;
        string format = CallbackWaitFormat;
        Submit(completion, submit);
        if (!done.Wait(timeout, out T payload))
        {
            throw CallbackWaitTimeout(format, operation);
        }

        return payload;
    }

    private async Task<T> AwaitQueued<T>(
        AsyncCompletion<T> done,
        TimeSpan timeout,
        string format,
        string operation,
        CancellationToken cancellationToken)
    {
        if (!await done.Wait(timeout, cancellationToken).ConfigureAwait(false))
        {
            throw CallbackWaitTimeout(format, operation);
        }

        return done.Payload;
    }

    private async Task AwaitVoid(
        AsyncCompletion<IntPtr> done,
        TimeSpan timeout,
        string format,
        string operation,
        CancellationToken cancellationToken)
    {
        IntPtr error = await AwaitQueued(done, timeout, format, operation, cancellationToken).ConfigureAwait(false);
        KafkaException.ThrowIfFfiFailed(ref error);
    }

    private async Task<IReadOnlyList<PartitionInfo>> AwaitPartitions(
        AsyncCompletion<(IntPtr Value, IntPtr Error)> done,
        TimeSpan timeout,
        string format,
        string topic,
        CancellationToken cancellationToken) =>
        PartitionsOf(
            await AwaitQueued(done, timeout, format, MetadataOf(topic), cancellationToken).ConfigureAwait(false));

    private void FlushBlocking()
    {
        IntPtr outErr = IntPtr.Zero;
        EnterUse();
        try
        {
            NativeMethods.ProducerFlush(_handle, ref outErr);
        }
        finally
        {
            ExitUse();
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private Task FlushBlockingAsTask()
    {
        try
        {
            FlushBlocking();
            return Task.CompletedTask;
        }
        catch (Exception exception)
        {
            return Task.FromException(exception);
        }
    }

    // From a mock callback the core cannot see, Java's mock flush body: complete every pending send and return without
    // waiting for the reports, which the core queues to run after the calling callback returns.
    private void CompletePending()
    {
        while (MockCompleteNext())
        {
        }
    }

    private Task CompletePendingAsTask()
    {
        try
        {
            CompletePending();
            return Task.CompletedTask;
        }
        catch (Exception exception)
        {
            return Task.FromException(exception);
        }
    }

    private IReadOnlyList<PartitionInfo> PartitionsForBlocking(string topic)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr list;
        EnterUse();
        try
        {
            using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
            list = NativeMethods.ProducerPartitionsFor(_handle, topicPin.Pointer, ref outErr);
        }
        finally
        {
            ExitUse();
        }

        return PartitionsOf((list, outErr));
    }

    private Task<IReadOnlyList<PartitionInfo>> PartitionsForBlockingAsTask(string topic)
    {
        try
        {
            return Task.FromResult(PartitionsForBlocking(topic));
        }
        catch (Exception exception)
        {
            return Task.FromException<IReadOnlyList<PartitionInfo>>(exception);
        }
    }

    private T CallMock<T>(MockCall<T> call)
    {
        IntPtr outErr = IntPtr.Zero;
        T result;
        bool added = false;
        EnterUse();
        try
        {
            result = call(_handle.DangerousAddRefClass(ref added), ref outErr);
        }
        finally
        {
            if (added)
            {
                _handle.DangerousRelease();
            }

            ExitUse();
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);
        return result;
    }

    private Task CloseWithCallbackWithin(long? timeoutMs, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!TryBeginClose())
        {
            return Task.CompletedTask;
        }

        if (InCallback)
        {
            CloseFromCallback(timeoutMs);
            return Task.CompletedTask;
        }

        return CloseAndTearDownAsync(timeoutMs);
    }

    private void CloseWithin(long? timeoutMs)
    {
        if (!TryBeginClose())
        {
            return;
        }

        if (InCallback)
        {
            CloseFromCallback(timeoutMs);
            return;
        }

        CloseAndTearDown(timeoutMs);
    }

    private void SubmitClose(long? timeoutMs, Action<IntPtr> onClosed) =>
        SubmitOwned(
            onClosed,
            opaque =>
            {
                if (timeoutMs is long ms)
                {
                    NativeMethods.ProducerCloseWithTimeoutCb(_handle, ms, s_voidCompletion, opaque);
                }
                else
                {
                    NativeMethods.ProducerCloseCb(_handle, s_voidCompletion, opaque);
                }
            });

    private void CloseFromCallback(long? timeoutMs)
    {
        if (!IsMock)
        {
            TraceCloseOverride(timeoutMs ?? long.MaxValue);
        }

        // An async callback's synchronous part runs inside the drain, where the close would not wait for its report.
        if (_reporter.InAsyncCallback && _completion.IsCurrentThread)
        {
            _completion.Post(SubmitCloseFromCallback);
            return;
        }

        SubmitCloseFromCallback();
    }

    private void SubmitCloseFromCallback() =>
        SubmitOwned(
            new Action<IntPtr>(error =>
            {
                FreeError(error);
                _completion.RequestTeardown(TearDown);
            }),
            opaque => NativeMethods.ProducerCloseWithTimeoutCb(_handle, 0, s_voidCompletion, opaque));

    private void CloseAndTearDown(long? timeoutMs)
    {
        Completion<IntPtr> closed = new Completion<IntPtr>(FreeError);
        IntPtr error = IntPtr.Zero;
        try
        {
            SubmitClose(timeoutMs, closed.Complete);
            _ = closed.Wait(Timeout.InfiniteTimeSpan, out error);
        }
        finally
        {
            TearDownKeeping(ref error);
        }

        KafkaException.ThrowIfFfiFailed(ref error);
    }

    private async Task CloseAndTearDownAsync(long? timeoutMs)
    {
        TaskCompletionSource<IntPtr> closed =
            new TaskCompletionSource<IntPtr>(TaskCreationOptions.RunContinuationsAsynchronously);
        IntPtr error = IntPtr.Zero;
        try
        {
            SubmitClose(timeoutMs, e => closed.SetResult(e));
            error = await closed.Task.ConfigureAwait(false);
        }
        finally
        {
            TearDownKeeping(ref error);
        }

        KafkaException.ThrowIfFfiFailed(ref error);
    }

    private void TearDownKeeping(ref IntPtr closeError)
    {
        try
        {
            TearDown();
        }
        catch
        {
            FreeError(ref closeError);
            throw;
        }
    }

    private void TearDown()
    {
        _completion.Dispose();
        _uses.WaitForNoneThenMarkDestroyed();
        _reporter.BeginDestroy();
        _handle.CloseExplicitly();
    }

    private void AfterDestroy()
    {
        _completion.ReleaseNotifyOpaque();
        _reporter.ReleaseKept();
    }

    private void EnterUse()
    {
        _uses.Enter();
        if (Volatile.Read(ref _closed) != 0)
        {
            ExitUse();
            throw new ObjectDisposedException(nameof(NativeProducer));
        }
    }

    private void ExitUse()
    {
        if (_uses.Exit() && Volatile.Read(ref _closed) != 0)
        {
            _uses.Wake();
        }
    }

    private bool TryBeginClose() => Interlocked.CompareExchange(ref _closed, 1, 0) == 0;
}
