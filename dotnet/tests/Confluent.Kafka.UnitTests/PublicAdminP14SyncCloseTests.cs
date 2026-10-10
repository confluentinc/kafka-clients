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

using Confluent.Kafka.Admin;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M15/P14 S5g — the sync admin flavour's <c>void Close(TimeSpan)</c> and blocking <see cref="IDisposable.Dispose"/>
/// on both sync clients: the negative-timeout precondition, a close waking a thread blocked in a future's
/// <c>Get()</c>, the one-shot teardown latch shared by <c>Close</c> and <c>Dispose</c>, and an RPC after close on both
/// flavours. Every blocking call runs on a dedicated thread with a bounded join (<see cref="BlockingCall"/>).
/// </summary>
public sealed class PublicAdminP14SyncCloseTests
{
    private const string Mock = "mock";
    private const string Real = "real";

    /// <summary>A bootstrap nothing listens on: the real client starts, and its RPCs wait for a broker.</summary>
    private static readonly IReadOnlyDictionary<string, string> s_unreachable =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["bootstrap.servers"] = "127.0.0.1:1" };

    /// <summary>
    /// A negative timeout throws <see cref="ArgumentOutOfRangeException"/> with the async flavour's exact message and
    /// parameter name, before anything reaches the native client — so the client stays open and a later close works —
    /// and it still throws after the client is closed, because validation comes before the teardown latch.
    /// </summary>
    [Theory]
    [InlineData(Mock)]
    [InlineData(Real)]
    public void Close_WithANegativeTimeout_ThrowsBeforeAnyNativeCall_AndTheClientStaysOpen(string kind)
    {
        IAdmin admin = Create(kind);
        TimeSpan negative = TimeSpan.FromMilliseconds(-1);
        string expected = new ArgumentOutOfRangeException("timeout", negative, "Timeout must not be negative.").Message;

        ArgumentOutOfRangeException thrown =
            Assert.IsType<ArgumentOutOfRangeException>(BlockingCall.RunWait(() => admin.Close(negative)));
        Assert.Equal("timeout", thrown.ParamName);
        Assert.Equal(negative, thrown.ActualValue);
        Assert.Equal(expected, thrown.Message);

        // Still open: an RPC is accepted rather than refused as disposed.
        Assert.NotNull(admin.ListTopics());

        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));

        ArgumentOutOfRangeException afterClose =
            Assert.IsType<ArgumentOutOfRangeException>(BlockingCall.RunWait(() => admin.Close(negative)));
        Assert.Equal("timeout", afterClose.ParamName);
        Assert.Equal(expected, afterClose.Message);
    }

    /// <summary>
    /// PLAN §3.6: closing while another thread is blocked in a future's <c>Get()</c> wakes that thread with the core's
    /// error. The real client against an unreachable bootstrap holds <c>CreateTopics</c> pending (the mock cannot hold a
    /// call pending); <c>Close(TimeSpan.Zero)</c> on a second thread fails it, and the blocked <c>Get()</c> throws.
    /// </summary>
    [Fact]
    public void Close_WakesAThreadBlockedInGet_WithTheCoresError()
    {
        KafkaAdminClient admin = new KafkaAdminClient(s_unreachable);
        try
        {
            CreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic("close-wakes-get", 1, 1) });
            BlockingCall blocked = BlockingCall.StartWait(() => result.All().Get());
            blocked.AssertBlocked();

            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));

            // Java's TimeoutException("The AdminClient thread has exited. Call: " + callName), code 7 (REQUEST_TIMED_OUT).
            KafkaException woken = AdminP14FlavourSeamTests.AssertThrew(
                blocked.Join(), 7, "The AdminClient thread has exited. Call: createTopics");
            Assert.True(woken.IsRetriable, "the close fails pending calls with Java's (retriable) TimeoutException");
            Assert.True(result.All().IsDone, "the aggregate is done once its key failed");
        }
        finally
        {
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(admin.Dispose));
        }
    }

    /// <summary>
    /// <c>Close</c> and <c>Dispose</c> share one teardown latch, so whichever runs first closes the client and every
    /// later one — a second <c>Close</c>, a <c>Dispose</c> after a <c>Close</c>, a <c>Close</c> after a
    /// <c>Dispose</c>, a second <c>Dispose</c> — returns at once, without error.
    /// </summary>
    [Theory]
    [InlineData(Mock)]
    [InlineData(Real)]
    public void CloseAndDispose_ShareOneLatch_EveryLaterCallIsANoOp(string kind)
    {
        IAdmin closedFirst = Create(kind);
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => closedFirst.Close(TimeSpan.FromSeconds(5))));
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => closedFirst.Close(TimeSpan.FromSeconds(5))));
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(closedFirst.Dispose));
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(closedFirst.Dispose));

        IAdmin disposedFirst = Create(kind);
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(disposedFirst.Dispose));
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(disposedFirst.Dispose));
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => disposedFirst.Close(TimeSpan.Zero)));
        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => disposedFirst.Close(TimeSpan.FromSeconds(5))));
    }

    /// <summary>
    /// An RPC after <c>Close</c> behaves as it did before S5g, on both flavours: it throws
    /// <see cref="ObjectDisposedException"/> synchronously, before anything is sent. Its <c>ObjectName</c> is today's —
    /// the sync type's name on both flavours, because the shared core names the type by real-or-mock only.
    /// </summary>
    [Theory]
    [InlineData(Mock, false)]
    [InlineData(Real, false)]
    [InlineData(Mock, true)]
    [InlineData(Real, true)]
    public async Task AnRpcAfterClose_ThrowsObjectDisposed_OnBothFlavours(string kind, bool async)
    {
        string objectName = kind == Mock ? nameof(MockAdminClient) : nameof(KafkaAdminClient);
        ObjectDisposedException thrown;
        if (async)
        {
            IAsyncAdmin admin = kind == Mock ? new AsyncMockAdminClient(1) : new AsyncKafkaAdminClient(s_unreachable);
            await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(5)), BlockingCall.Deadline);
            thrown = Assert.Throws<ObjectDisposedException>(() => admin.ListTopics());
            await TestTimeout.Run(() => admin.DisposeAsync().AsTask(), BlockingCall.Deadline);
        }
        else
        {
            IAdmin admin = Create(kind);
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.FromSeconds(5))));
            thrown = Assert.Throws<ObjectDisposedException>(() => admin.ListTopics());
            admin.Dispose();
        }

        Assert.Equal(objectName, thrown.ObjectName);
        Assert.Equal(new ObjectDisposedException(objectName).Message, thrown.Message);
    }

    private static IAdmin Create(string kind) =>
        kind == Mock ? new MockAdminClient(1) : new KafkaAdminClient(s_unreachable);
}
