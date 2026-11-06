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
using Confluent.Kafka.Common;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The admin clients' <c>Close(TimeSpan)</c> and <c>Dispose</c>: the negative-timeout precondition on the real clients,
/// a close waking a thread blocked in a future's <c>Get()</c>, one teardown shared by <c>Close</c> and <c>Dispose</c>,
/// and an RPC after a close, which the real clients refuse and the mocks, whose <c>Close</c> does nothing, accept.
/// </summary>
public sealed class PublicAdminP14SyncCloseTests
{
    private const string Mock = "mock";
    private const string Real = "real";

    private static readonly IReadOnlyDictionary<string, object?> s_unreachable =
        new Dictionary<string, object?>(StringComparer.Ordinal) { ["bootstrap.servers"] = "127.0.0.1:1" };

    [Fact]
    public void Close_WithANegativeTimeout_ThrowsJavasMessageBeforeAnyNativeCall_AndTheClientStaysOpen()
    {
        IAdmin admin = new KafkaAdminClient(s_unreachable);
        TimeSpan negative = TimeSpan.FromMilliseconds(-1);
        IllegalArgumentException thrown =
            Assert.IsType<IllegalArgumentException>(BlockingCall.RunWait(() => admin.Close(negative)));
        Assert.Equal("The timeout cannot be negative.", thrown.Message);

        Assert.NotNull(admin.ListTopics());

        Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));

        IllegalArgumentException afterClose =
            Assert.IsType<IllegalArgumentException>(BlockingCall.RunWait(() => admin.Close(negative)));
        Assert.Equal("The timeout cannot be negative.", afterClose.Message);
        TestTimeout.Dispose(admin);
    }

    [Fact]
    public void Close_OnAMock_ReadsNoTimeout_AndTheMockStaysUsable()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        try
        {
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.FromMilliseconds(-1))));
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));

            object? topics = BlockingCall.RunGet(() => admin.ListTopics().NamesToListings().Get());
            Assert.Empty(Assert.IsAssignableFrom<IReadOnlyDictionary<string, TopicListing>>(topics));
        }
        finally
        {
            TestTimeout.Dispose(admin);
        }
    }

    [Fact]
    public void Close_WakesAThreadBlockedInGet_WithTheCoresError()
    {
        KafkaAdminClient admin = new KafkaAdminClient(s_unreachable);
        try
        {
            ListTopicsResult result = admin.ListTopics();
            BlockingCall blocked = BlockingCall.StartGet(() => result.NamesToListings().Get());
            blocked.AssertBlocked();

            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.Zero)));

            KafkaException woken = BlockingCall.AssertThrew(
                blocked.Join(), 7, "The AdminClient thread has exited. Call: listTopics");
            Assert.IsAssignableFrom<Common.Errors.RetriableException>(woken);
            Assert.True(result.NamesToListings().IsDone);
            BlockingCall.AssertThrew(
                BlockingCall.RunGet(() => result.Names().Get()), 7, "The AdminClient thread has exited. Call: listTopics");
        }
        finally
        {
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(admin.Dispose));
        }
    }

    [Theory]
    [InlineData(Mock)]
    [InlineData(Real)]
    public void CloseAndDispose_ShareOneTeardown_EveryLaterCallIsANoOp(string kind)
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnRpcAfterClose_ThrowsIllegalState_OnTheRealClients(bool async)
    {
        IllegalStateException thrown;
        if (async)
        {
            IAsyncAdmin admin = new AsyncKafkaAdminClient(s_unreachable);
            await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(5)), BlockingCall.Deadline);
            thrown = Assert.Throws<IllegalStateException>(() => admin.ListTopics());
            await TestTimeout.DisposeAsync(admin);
        }
        else
        {
            IAdmin admin = new KafkaAdminClient(s_unreachable);
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.FromSeconds(5))));
            thrown = Assert.Throws<IllegalStateException>(() => admin.ListTopics());
            TestTimeout.Dispose(admin);
        }

        Assert.Equal("Cannot accept new calls when AdminClient is closing.", thrown.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnRpcAfterAMockClose_Works_AndAfterDispose_ThrowsIllegalState(bool async)
    {
        IllegalStateException thrown;
        if (async)
        {
            IAsyncAdmin admin = AsyncMockAdminClient.Create().Build();
            await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(5)), BlockingCall.Deadline);
            IReadOnlyDictionary<string, TopicListing> topics =
                await TestTimeout.Run(() => admin.ListTopics().NamesToListings().Get(), BlockingCall.Deadline);
            Assert.Empty(topics);
            await TestTimeout.DisposeAsync(admin);
            thrown = Assert.Throws<IllegalStateException>(() => admin.ListTopics());
        }
        else
        {
            IAdmin admin = MockAdminClient.Create().Build();
            Assert.Same(BlockingCall.Returned, BlockingCall.RunWait(() => admin.Close(TimeSpan.FromSeconds(5))));
            object? topics = BlockingCall.RunGet(() => admin.ListTopics().NamesToListings().Get());
            Assert.Empty(Assert.IsAssignableFrom<IReadOnlyDictionary<string, TopicListing>>(topics));
            TestTimeout.Dispose(admin);
            thrown = Assert.Throws<IllegalStateException>(() => admin.ListTopics());
        }

        Assert.Equal("Cannot accept new calls when AdminClient is closing.", thrown.Message);
    }

    private static IAdmin Create(string kind) =>
        kind == Mock ? MockAdminClient.Create().Build() : new KafkaAdminClient(s_unreachable);
}
