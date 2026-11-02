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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The offset-commit callback registration: <c>onComplete</c> driven through the core's invoker, its report, and
/// when the registration's <c>self_</c> is released.
/// </summary>
public sealed class ConsumerCommitCallbackBridgeTests
{
    private const string Topic = "commit-callback-bridge-topic";

    private const string ConcurrentMessage = "KafkaConsumer is not safe for multi-threaded access.";

    private const int InvalidGroupIdCode = 24;

    private const string NoGroupId =
        "To use the group management or offset commit APIs, you must provide a valid group.id in the consumer "
        + "configuration.";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_notYetWindow = TimeSpan.FromMilliseconds(500);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Fact]
    public void OnComplete_ATimeoutError_DeliversSystemTimeoutException_AndNullOffsets()
    {
        RecordingCallback callback = new RecordingCallback();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(callback, false, Reporter());

        Exception? reported = Invoke(registration, Offsets(7), TimeoutError("commit timed out"));

        Assert.Null(reported);
        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) = Assert.Single(callback.Calls);
        Assert.Null(offsets);
        TimeoutException timeout = Assert.IsType<TimeoutException>(exception);
        Assert.Equal("commit timed out", timeout.Message);
        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void OnComplete_Success_DeliversACopyOfTheOffsets_AndANullException()
    {
        RecordingCallback callback = new RecordingCallback();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(callback, false, Reporter());

        Exception? reported = Invoke(registration, Offsets(42, "meta"), IntPtr.Zero);

        Assert.Null(reported);
        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) = Assert.Single(callback.Calls);
        Assert.Null(exception);
        Assert.NotNull(offsets);
        KeyValuePair<TopicPartition, OffsetAndMetadata> entry = Assert.Single(offsets!);
        Assert.Equal(s_tp0, entry.Key);
        Assert.Equal(42, entry.Value.Offset);
        Assert.Equal("meta", entry.Value.Metadata);
        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void OnComplete_AnExplicitEmptyOffsetsCommit_DeliversNullOffsets()
    {
        RecordingCallback callback = new RecordingCallback();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(callback, true, Reporter());

        Exception? reported = Invoke(registration, new Dictionary<TopicPartition, OffsetAndMetadata>(), IntPtr.Zero);

        Assert.Null(reported);
        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) = Assert.Single(callback.Calls);
        Assert.Null(offsets);
        Assert.Null(exception);
    }

    [Fact]
    public void OnComplete_WithNoUserCallback_ReportsSuccessOnce_AndReleases()
    {
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root((IOffsetCommitCallback?)null, false, Reporter());

        Exception? reported = Invoke(registration, Offsets(3), TimeoutError("ignored"));

        Assert.Null(reported);
        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void OnComplete_AThrowingCallback_ReportsTheSameInstance()
    {
        InvalidCastException thrown = new InvalidCastException("callback boom");
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(
            new ThrowingCallback(thrown), false, Reporter());

        Exception? reported = Invoke(registration, Offsets(1), IntPtr.Zero);

        Assert.Same(thrown, reported);
        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void OnComplete_RunsInsideTheReportersCallbackFlag()
    {
        ConsumerCallbackReporter reporter = Reporter();
        bool? inside = null;
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(
            new DelegateCallback((_, _) => inside = reporter.InCallback), false, reporter);

        Assert.Null(Invoke(registration, Offsets(1), IntPtr.Zero));

        Assert.True(inside);
        Assert.False(reporter.InCallback);
    }

    [Fact]
    public void Release_Twice_IsHarmless()
    {
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(new RecordingCallback(), false, Reporter());

        registration.Release();
        registration.Release();

        Assert.True(registration.IsReleased);
    }

    [Fact]
    public async Task AsyncOnComplete_IsReportedWhenItsTaskEnds_NotWhenItReturns()
    {
        TaskCompletionSource<bool> release = NewSignal();
        ManualResetEventSlim entered = new ManualResetEventSlim();
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? delivered = null;
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(
            new AsyncDelegateCallback(async (offsets, _) =>
            {
                delivered = offsets;
                entered.Set();
                await release.Task;
            }),
            false,
            Reporter());

        Task<Exception?> invoked = Task.Run(() => Invoke(registration, Offsets(9, "async"), IntPtr.Zero));

        Assert.True(entered.Wait(s_deadline), "the callback was not called");
        await Task.WhenAny(invoked, Task.Delay(s_notYetWindow));
        Assert.False(invoked.IsCompleted, "the invoker returned before the callback's Task ended");
        Assert.False(registration.IsReleased, "the registration was released before the callback's Task ended");

        release.SetResult(true);
        Assert.Null(await TestTimeout.Run(() => invoked, s_deadline));
        AssertReleasedAfterTheReport(registration);
        KeyValuePair<TopicPartition, OffsetAndMetadata> entry = Assert.Single(delivered!);
        Assert.Equal(s_tp0, entry.Key);
        Assert.Equal(9, entry.Value.Offset);
        Assert.Equal("async", entry.Value.Metadata);
    }

    [Fact]
    public async Task AsyncOnComplete_AFaultedTask_ReportsTheSameInstance()
    {
        InvalidCastException thrown = new InvalidCastException("async callback boom");
        TaskCompletionSource<bool> outcome = NewSignal();
        (CommitCallbackRegistration registration, Task<Exception?> invoked) = InvokeOnAPendingTask(outcome);

        outcome.SetException(thrown);

        Assert.Same(thrown, await TestTimeout.Run(() => invoked, s_deadline));
        AssertReleasedAfterTheReport(registration);
    }

    [Fact]
    public async Task AsyncOnComplete_ACanceledTask_ReportsTheCancellation()
    {
        TaskCompletionSource<bool> outcome = NewSignal();
        (CommitCallbackRegistration registration, Task<Exception?> invoked) = InvokeOnAPendingTask(outcome);

        outcome.SetCanceled();

        Assert.IsAssignableFrom<OperationCanceledException>(await TestTimeout.Run(() => invoked, s_deadline));
        AssertReleasedAfterTheReport(registration);
    }

    [Fact]
    public void AsyncOnComplete_ThrowingBeforeItReturnsATask_ReportsTheSameInstance()
    {
        InvalidCastException thrown = new InvalidCastException("thrown before the Task");
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(
            new AsyncDelegateCallback((_, _) => throw thrown), false, Reporter());

        Exception? reported = Invoke(registration, Offsets(1), IntPtr.Zero);

        Assert.Same(thrown, reported);
        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void AsyncOnComplete_ReturningANullTask_IsReportedAsSuccess()
    {
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(
            new AsyncDelegateCallback((_, _) => null!), false, Reporter());

        Exception? reported = Invoke(registration, Offsets(1), IntPtr.Zero);

        Assert.Null(reported);
        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void AsyncOnComplete_ATimeoutError_DeliversSystemTimeoutException_AndNullOffsets()
    {
        List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? Offsets, Exception? Exception)> calls =
            new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?)>();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(
            new AsyncDelegateCallback((offsets, exception) =>
            {
                calls.Add((offsets, exception));
                return Task.CompletedTask;
            }),
            false,
            Reporter());

        Exception? reported = Invoke(registration, Offsets(7), TimeoutError("async commit timed out"));

        Assert.Null(reported);
        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) = Assert.Single(calls);
        Assert.Null(offsets);
        Assert.Equal("async commit timed out", Assert.IsType<TimeoutException>(exception).Message);
        Assert.True(registration.IsReleased);
    }

    [Fact]
    public void AsyncOnComplete_AnExplicitEmptyOffsetsCommit_DeliversNullOffsets()
    {
        List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? Offsets, Exception? Exception)> calls =
            new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?)>();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(
            new AsyncDelegateCallback((offsets, exception) =>
            {
                calls.Add((offsets, exception));
                return Task.CompletedTask;
            }),
            true,
            Reporter());

        Exception? reported = Invoke(registration, new Dictionary<TopicPartition, OffsetAndMetadata>(), IntPtr.Zero);

        Assert.Null(reported);
        (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) = Assert.Single(calls);
        Assert.Null(offsets);
        Assert.Null(exception);
    }

    [Fact]
    public void AsyncOnComplete_RunsItsSynchronousPartInsideTheReportersCallbackFlag()
    {
        ConsumerCallbackReporter reporter = Reporter();
        bool? inside = null;
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(
            new AsyncDelegateCallback((_, _) =>
            {
                inside = reporter.InCallback;
                return Task.CompletedTask;
            }),
            false,
            reporter);

        Assert.Null(Invoke(registration, Offsets(1), IntPtr.Zero));

        Assert.True(inside);
        Assert.False(reporter.InCallback);
    }

    [Fact]
    public void Reporter_ALateReportAfterDestroy_IsSkipped_AndFreesItsError()
    {
        ConsumerCallbackReporter reporter = Reporter();
        reporter.MarkDestroyed();
        using ManualResetEventSlim reported = new ManualResetEventSlim();

        WeakReference failure = ReportAFailureLate(reporter, reported);

        Assert.True(reported.IsSet, "the late report did not run");
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.False(failure.IsAlive, "the skipped report kept the error that carries the exception");
    }

    [Fact]
    public void SyncMock_ASucceededCommit_ReleasesItsRegistrationOnceItReported()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        try
        {
            RecordingCallback callback = new RecordingCallback();
            consumer.CommitAsync(Offsets(5), callback);

            Assert.Single(callback.Calls);
            CommitCallbackRegistration registration = Assert.IsType<CommitCallbackRegistration>(Registration(consumer));
            Assert.True(registration.IsReleased);
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public void SyncMock_CommitAsyncWithOffsetsAndNoCallback_PassesARegistrationThatReportsOnce()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        try
        {
            consumer.Assign(new[] { s_tp0 });
            consumer.CommitAsync(Offsets(5));

            CommitCallbackRegistration registration = Assert.IsType<CommitCallbackRegistration>(Registration(consumer));
            Assert.True(registration.IsReleased);
            Assert.Equal(5, consumer.Committed(new[] { s_tp0 })[s_tp0]!.Offset);
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    [Fact]
    public void KafkaConsumer_ACommitTheCoreRefused_KeepsItsRegistrationUntilTheConsumerIsDestroyed()
    {
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:1", ["group.protocol"] = "consumer" },
            Serdes.ByteArray,
            Serdes.ByteArray);
        RecordingCallback callback = new RecordingCallback();

        KafkaException refused = Assert.Throws<KafkaException>(() => consumer.CommitAsync(Offsets(1), callback));

        Assert.Equal(InvalidGroupIdCode, refused.Code);
        Assert.Equal(NoGroupId, refused.Message);
        CommitCallbackRegistration registration = Assert.IsType<CommitCallbackRegistration>(Registration(consumer));
        Assert.False(registration.IsReleased, "the registration of a refused commit was released before destroy");

        TestTimeout.Run(() => consumer.Close(CloseOptions.Timeout(TimeSpan.Zero)), s_deadline);

        Assert.True(registration.IsReleased, "destroy did not release the refused commit's registration");
        Assert.Empty(callback.Calls);
    }

    [Fact]
    public async Task AsyncMock_ACommitTheCoreRefused_KeepsItsRegistrationUntilTheConsumerIsDestroyed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        RecordingCallback callback = new RecordingCallback();
        using ManualResetEventSlim entered = new ManualResetEventSlim(false);
        using ManualResetEventSlim release = new ManualResetEventSlim(false);
        await consumer.Subscribe(
            new[] { Topic },
            new SyncListener(() =>
            {
                entered.Set();
                _ = release.Wait(s_deadline);
            }));

        Task rebalance = consumer.Rebalance(new[] { s_tp0 });
        Task refusedCommit;
        try
        {
            Assert.True(entered.Wait(s_deadline), "the listener never ran");
            refusedCommit = consumer.CommitAsync(callback);
        }
        finally
        {
            release.Set();
        }

        await TestTimeout.Run(() => rebalance, s_deadline);
        InvalidOperationException concurrent = await Assert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.Run(() => refusedCommit, s_deadline));
        Assert.Equal(ConcurrentMessage, concurrent.Message);
        CommitCallbackRegistration registration = Assert.IsType<CommitCallbackRegistration>(Registration(consumer));
        Assert.False(registration.IsReleased, "the registration of a refused commit was released before destroy");

        await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);

        Assert.True(registration.IsReleased, "destroy did not release the refused commit's registration");
        Assert.Empty(callback.Calls);
    }

    [Fact]
    public async Task AsyncMock_AnAsyncCallbackCommit_KeepsItsRegistrationUntilItsTaskEnded()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = NewAsyncMock();
        try
        {
            TaskCompletionSource<bool> release = NewSignal();
            ManualResetEventSlim entered = new ManualResetEventSlim();
            Task commit = consumer.CommitAsync(
                Offsets(4),
                new AsyncDelegateCallback(async (_, _) =>
                {
                    entered.Set();
                    await release.Task;
                }));

            Assert.True(entered.Wait(s_deadline), "the callback was not called");
            CommitCallbackRegistration registration = Assert.IsType<CommitCallbackRegistration>(Registration(consumer));
            await Task.WhenAny(commit, Task.Delay(s_notYetWindow));
            Assert.False(commit.IsCompleted, "CommitAsync returned before the callback's Task ended");
            Assert.False(registration.IsReleased, "the registration was released before the callback's Task ended");

            release.SetResult(true);
            await TestTimeout.Run(() => commit, s_deadline);
            AssertReleasedAfterTheReport(registration);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public void SyncMock_CommitCallbacksChurnedUnderAggressiveGc_EachFireOnce()
    {
        MockConsumer<byte[], byte[]> consumer = NewMock();
        try
        {
            RecordingCallback callback = new RecordingCallback();
            for (int i = 0; i < 200; i++)
            {
                consumer.CommitAsync(Offsets(i), callback);
                if (i % 20 == 0)
                {
                    GC.Collect();
                    GC.WaitForPendingFinalizers();
                }
            }

            Assert.Equal(200, callback.Calls.Count);
            for (int i = 0; i < 200; i++)
            {
                Assert.Equal(i, callback.Calls[i].Offsets![s_tp0].Offset);
            }
        }
        finally
        {
            TestTimeout.Run(consumer.Dispose, s_deadline);
        }
    }

    private static Exception? Invoke(
        CommitCallbackRegistration registration,
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IntPtr error)
    {
        try
        {
            using NativeCallArguments arguments = new NativeCallArguments();
            IntPtr map = arguments.OffsetMap(NativeConsumer.SnapshotCommitOffsets(offsets));
            IntPtr handle = registration.NewHandle();
            IntPtr outErr = IntPtr.Zero;
            try
            {
                NativeMethods.OffsetCommitCallbackInvokeOnComplete(handle, map, error, ref outErr);
            }
            finally
            {
                CommitCallbackRegistration.DestroyHandle(handle);
            }

            return KafkaException.FromFfiError(outErr, owned: true);
        }
        finally
        {
            if (error != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(error);
            }
        }
    }

    private static IntPtr TimeoutError(string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr outErr = IntPtr.Zero;
        IntPtr error = NativeMethods.ErrorLocalTimeout(pinned.Pointer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return error;
    }

    private static ConsumerCallbackReporter Reporter() => new ConsumerCallbackReporter(IntPtr.Zero);

    // An async callback's registration is released just after its report, which is what ends the invoke or the commit.
    private static void AssertReleasedAfterTheReport(CommitCallbackRegistration registration) =>
        Assert.True(
            SpinWait.SpinUntil(() => registration.IsReleased, s_deadline),
            "the registration was not released after the callback's report");

    private static (CommitCallbackRegistration Registration, Task<Exception?> Invoked) InvokeOnAPendingTask(
        TaskCompletionSource<bool> outcome)
    {
        ManualResetEventSlim entered = new ManualResetEventSlim();
        CommitCallbackRegistration registration = CommitCallbackRegistration.Root(
            new AsyncDelegateCallback((_, _) =>
            {
                entered.Set();
                return outcome.Task;
            }),
            false,
            Reporter());

        Task<Exception?> invoked = Task.Run(() => Invoke(registration, Offsets(1), IntPtr.Zero));
        Assert.True(entered.Wait(s_deadline), "the callback was not called");
        Assert.False(invoked.Wait(s_notYetWindow), "the invoker returned before the callback's Task ended");
        Assert.False(registration.IsReleased, "the registration was released before the callback's Task ended");
        return (registration, invoked);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference ReportAFailureLate(ConsumerCallbackReporter reporter, ManualResetEventSlim reported)
    {
        TaskCompletionSource<bool> outcome = new TaskCompletionSource<bool>();
        reporter.Exit(reporter.Enter(asyncCallback: true));
        reporter.ReportWhenEnded(1, outcome.Task, null, reported.Set);
        InvalidCastException failure = new InvalidCastException("reported after destroy");
        outcome.SetException(failure);
        return new WeakReference(failure);
    }

    private static TaskCompletionSource<bool> NewSignal() =>
        new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

    private static Dictionary<TopicPartition, OffsetAndMetadata> Offsets(long offset, string metadata = "") =>
        new Dictionary<TopicPartition, OffsetAndMetadata> { [s_tp0] = new OffsetAndMetadata(offset, metadata) };

    private static MockConsumer<byte[], byte[]> NewMock() =>
        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static AsyncMockConsumer<byte[], byte[]> NewAsyncMock() =>
        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

    private static CommitCallbackRegistration? Registration(object consumer) =>
        ((NativeConsumer)consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!).CurrentCommitCallbackRegistration;

    /// <summary>Records each completion's offsets and exception.</summary>
    private sealed class RecordingCallback : IOffsetCommitCallback
    {
        internal List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? Offsets, Exception? Exception)> Calls { get; } =
            new List<(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>?, Exception?)>();

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            Calls.Add((offsets, exception));
    }

    /// <summary>A commit callback that throws the exception it is given.</summary>
    private sealed class ThrowingCallback : IOffsetCommitCallback
    {
        private readonly Exception _exception;

        internal ThrowingCallback(Exception exception) => _exception = exception;

        public void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception) =>
            throw _exception;
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

    /// <summary>A listener that runs the action it is given when partitions are assigned.</summary>
    private sealed class SyncListener : ConsumerRebalanceListenerBase
    {
        private readonly Action _onAssigned;

        internal SyncListener(Action onAssigned) => _onAssigned = onAssigned;

        public override void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions)
        {
        }

        public override void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions) => _onAssigned();
    }
}
