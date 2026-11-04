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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the teardown helpers of <see cref="TestTimeout"/>: a teardown that never ends fails with a
/// <see cref="TimeoutException"/> naming the disposed type, also when it blocks before its first await, and a
/// teardown that ends is passed through unchanged.
/// </summary>
public sealed class TestTimeoutTeardownTests
{
    private static readonly TimeSpan s_short = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan s_outer = TimeSpan.FromSeconds(20);

    [Fact]
    public void TheSharedBound_IsAtLeastThirtySeconds()
    {
        Assert.True(TestTimeout.TeardownBound >= TimeSpan.FromSeconds(30), TestTimeout.TeardownBound.ToString());
    }

    [Fact]
    public async Task DisposeAsync_ThatNeverEnds_ThrowsATimeoutExceptionNamingTheType()
    {
        NeverEndingAsyncDisposable target = new NeverEndingAsyncDisposable();

        TimeoutException hung = await Assert.ThrowsAsync<TimeoutException>(() => TestTimeout.DisposeAsync(target, s_short));

        Assert.StartsWith(Hung("NeverEndingAsyncDisposable"), hung.Message, StringComparison.Ordinal);
        Assert.Equal(1, target.Calls);
    }

    [Fact]
    public async Task DisposeAsync_ThatBlocksBeforeItsFirstAwait_IsBounded()
    {
        ManualResetEventSlim gate = new ManualResetEventSlim();
        BlockingAsyncDisposable target = new BlockingAsyncDisposable(gate);
        try
        {
            await TestTimeout.RunOffThisThread(
                async () =>
                {
                    TimeoutException hung = await Assert.ThrowsAsync<TimeoutException>(() => TestTimeout.DisposeAsync(target, s_short));

                    Assert.StartsWith(Hung("BlockingAsyncDisposable"), hung.Message, StringComparison.Ordinal);
                    Assert.NotEqual(Environment.CurrentManagedThreadId, target.ThreadId);
                },
                s_outer);
        }
        finally
        {
            gate.Set();
        }
    }

    [Fact]
    public void Dispose_ThatNeverEnds_ThrowsATimeoutExceptionNamingTheType()
    {
        ManualResetEventSlim gate = new ManualResetEventSlim();
        BlockingDisposable target = new BlockingDisposable(gate);
        try
        {
            TestTimeout.Run(
                () =>
                {
                    TimeoutException hung = Assert.Throws<TimeoutException>(() => TestTimeout.Dispose(target, s_short));

                    Assert.StartsWith(Hung("BlockingDisposable"), hung.Message, StringComparison.Ordinal);
                    Assert.NotEqual(Environment.CurrentManagedThreadId, target.ThreadId);
                },
                s_outer);
        }
        finally
        {
            gate.Set();
        }
    }

    [Fact]
    public void Teardown_ThatNeverEnds_ThrowsATimeoutExceptionNamingTheTargetsType()
    {
        ManualResetEventSlim gate = new ManualResetEventSlim();
        GenericTarget<byte[], string> target = new GenericTarget<byte[], string>();
        try
        {
            TestTimeout.Run(
                () =>
                {
                    TimeoutException hung = Assert.Throws<TimeoutException>(() => TestTimeout.Teardown(target, () => gate.Wait(), s_short));

                    Assert.StartsWith(Hung("GenericTarget<Byte[], String>"), hung.Message, StringComparison.Ordinal);
                },
                s_outer);
        }
        finally
        {
            gate.Set();
        }
    }

    [Fact]
    public async Task TeardownAsync_ThatNeverEnds_ThrowsATimeoutExceptionNamingTheTargetsType()
    {
        GenericTarget<int, long> target = new GenericTarget<int, long>();

        TimeoutException hung = await Assert.ThrowsAsync<TimeoutException>(
            () => TestTimeout.TeardownAsync(target, () => new TaskCompletionSource<bool>().Task, s_short));

        Assert.StartsWith(Hung("GenericTarget<Int32, Int64>"), hung.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ATeardownThatThrows_PassesItsOwnExceptionThrough()
    {
        InvalidOperationException boom = new InvalidOperationException("boom");

        Assert.Same(boom, Assert.Throws<InvalidOperationException>(() => TestTimeout.Dispose(new ThrowingDisposable(boom))));
        Assert.Same(boom, await Assert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.DisposeAsync(new ThrowingAsyncDisposable(boom, beforeItsFirstAwait: true))));
        Assert.Same(boom, await Assert.ThrowsAsync<InvalidOperationException>(
            () => TestTimeout.DisposeAsync(new ThrowingAsyncDisposable(boom, beforeItsFirstAwait: false))));
    }

    [Fact]
    public async Task TheWrappers_DisposeTheirTargetOnce_AndANullTargetIsANoOp()
    {
        CountingDisposable syncTarget = new CountingDisposable();
        using (TestTimeout.Disposal(syncTarget))
        {
            Assert.Equal(0, syncTarget.Calls);
        }

        CountingDisposable asyncTarget = new CountingDisposable();
        await using (TestTimeout.AsyncDisposal(asyncTarget))
        {
            Assert.Equal(0, asyncTarget.Calls);
        }

        using (TestTimeout.Disposal(null))
        {
        }

        await using (TestTimeout.AsyncDisposal(null))
        {
        }

        Assert.Equal(1, syncTarget.Calls);
        Assert.Equal(1, asyncTarget.Calls);
    }

    private static string Hung(string type) =>
        $"Teardown of {type} did not complete within {s_short} — treated as a hang (fail fast). Diagnostics: the action ";

    /// <summary>A target whose <c>DisposeAsync</c> returns a task that never completes.</summary>
    private sealed class NeverEndingAsyncDisposable : IAsyncDisposable
    {
        internal int Calls { get; private set; }

        public ValueTask DisposeAsync()
        {
            Calls++;
            return new ValueTask(new TaskCompletionSource<bool>().Task);
        }
    }

    /// <summary>A target whose <c>DisposeAsync</c> blocks on a gate before it returns its task.</summary>
    private sealed class BlockingAsyncDisposable : IAsyncDisposable
    {
        private readonly ManualResetEventSlim _gate;

        internal BlockingAsyncDisposable(ManualResetEventSlim gate) => _gate = gate;

        internal int ThreadId { get; private set; }

        public ValueTask DisposeAsync()
        {
            ThreadId = Environment.CurrentManagedThreadId;
            _gate.Wait();
            return default;
        }
    }

    /// <summary>A target whose <c>Dispose</c> blocks on a gate.</summary>
    private sealed class BlockingDisposable : IDisposable
    {
        private readonly ManualResetEventSlim _gate;

        internal BlockingDisposable(ManualResetEventSlim gate) => _gate = gate;

        internal int ThreadId { get; private set; }

        public void Dispose()
        {
            ThreadId = Environment.CurrentManagedThreadId;
            _gate.Wait();
        }
    }

    /// <summary>A generic target, to pin how a generic type is named.</summary>
    private sealed class GenericTarget<TKey, TValue>
    {
    }

    /// <summary>A target whose <c>Dispose</c> throws a given exception.</summary>
    private sealed class ThrowingDisposable : IDisposable
    {
        private readonly Exception _exception;

        internal ThrowingDisposable(Exception exception) => _exception = exception;

        public void Dispose() => throw _exception;
    }

    /// <summary>A target whose <c>DisposeAsync</c> throws a given exception, before or after its first await.</summary>
    private sealed class ThrowingAsyncDisposable : IAsyncDisposable
    {
        private readonly Exception _exception;
        private readonly bool _beforeItsFirstAwait;

        internal ThrowingAsyncDisposable(Exception exception, bool beforeItsFirstAwait)
        {
            _exception = exception;
            _beforeItsFirstAwait = beforeItsFirstAwait;
        }

        public ValueTask DisposeAsync()
        {
            if (_beforeItsFirstAwait)
            {
                throw _exception;
            }

            return new ValueTask(ThrowAfterAYield());
        }

        private async Task ThrowAfterAYield()
        {
            await Task.Yield();
            throw _exception;
        }
    }

    /// <summary>A target that counts its disposals, sync and async.</summary>
    private sealed class CountingDisposable : IDisposable, IAsyncDisposable
    {
        private int _calls;

        internal int Calls => Volatile.Read(ref _calls);

        public void Dispose() => Interlocked.Increment(ref _calls);

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _calls);
            return default;
        }
    }
}
