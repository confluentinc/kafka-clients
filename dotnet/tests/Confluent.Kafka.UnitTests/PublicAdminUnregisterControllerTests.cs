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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>UnregisterController</c> on both flavours: the mocks' answer with and without a raft controller, the real clients'
/// timeout, <c>All()</c>'s identity, and a call after <c>Dispose</c>.
/// </summary>
public sealed class PublicAdminUnregisterControllerTests
{
    private const int UnsupportedVersionCode = 35;

    private const int RequestTimedOutCode = 7;

    private const string NodeAssignmentTimeoutMessage =
        "Timed out waiting for a node assignment. Call: unregisterController";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Theory]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(-1, false)]
    [InlineData(int.MaxValue, true)]
    public void MockAdminClient_WithoutARaftController_FailsAllWithUnsupportedVersionAndNoMessage(
        int controllerId, bool withOptions)
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        UnregisterControllerResult result = admin.UnregisterController(
            controllerId, withOptions ? new UnregisterControllerOptions { TimeoutMs = 1000 } : null);

        KafkaException failure = Assert.Throws<KafkaException>(() => result.All().Get(s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(string.Empty, failure.Message);
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(-1, false)]
    [InlineData(int.MaxValue, true)]
    public async Task AsyncMockAdminClient_WithoutARaftController_FailsAllWithUnsupportedVersionAndNoMessage(
        int controllerId, bool withOptions)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncUnregisterControllerResult result = admin.UnregisterController(
            controllerId, withOptions ? new UnregisterControllerOptions { TimeoutMs = 1000 } : null);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.All().Get()), s_deadline);
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(string.Empty, failure.Message);
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(-1, true)]
    public void MockAdminClient_WithARaftController_CompletesAll(int controllerId, bool withOptions)
    {
        MockAdminClient admin = MockAdminClient.Create().UsingRaftController(true).Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        UnregisterControllerResult result = admin.UnregisterController(
            controllerId, withOptions ? new UnregisterControllerOptions { TimeoutMs = 1000 } : null);

        result.All().Get(s_deadline);
        Assert.True(result.All().IsDone);
        Assert.False(result.All().IsCompletedExceptionally);
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(-1, true)]
    public async Task AsyncMockAdminClient_WithARaftController_CompletesAll(int controllerId, bool withOptions)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().UsingRaftController(true).Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncUnregisterControllerResult result = admin.UnregisterController(
            controllerId, withOptions ? new UnregisterControllerOptions { TimeoutMs = 1000 } : null);

        await TestTimeout.Run(() => result.All().Get(), s_deadline);
        Assert.True(result.All().IsDone);
        Assert.False(result.All().IsCompletedExceptionally);
    }

    [Fact]
    public async Task UsingRaftControllerFalse_FailsAllAsTheDefault_OnBothMocks()
    {
        MockAdminClient sync = MockAdminClient.Create().UsingRaftController(true).UsingRaftController(false).Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = AsyncMockAdminClient.Create().UsingRaftController(false).Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        KafkaException syncFailure = Assert.Throws<KafkaException>(() => sync.UnregisterController(1).All().Get(s_deadline));
        KafkaException asyncFailure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => async.UnregisterController(1).All().Get()), s_deadline);

        Assert.Equal(UnsupportedVersionCode, syncFailure.Code);
        Assert.Equal(string.Empty, syncFailure.Message);
        Assert.Equal(UnsupportedVersionCode, asyncFailure.Code);
        Assert.Equal(string.Empty, asyncFailure.Message);
    }

    [Fact]
    public async Task All_IsTheSameFutureOnEveryCall_OnBothFlavours()
    {
        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncMockAdminClient async = AsyncMockAdminClient.Create().Build();
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        UnregisterControllerResult syncResult = sync.UnregisterController(1);
        AsyncUnregisterControllerResult asyncResult = async.UnregisterController(1);
        Assert.True(syncResult.All() == syncResult.All());
        Assert.True(asyncResult.All() == asyncResult.All());

        Assert.Throws<KafkaException>(() => syncResult.All().Get(s_deadline));
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(() => asyncResult.All().Get()), s_deadline);

        Assert.True(syncResult.All() == syncResult.All());
        Assert.True(asyncResult.All() == asyncResult.All());
        Assert.True(syncResult.All().IsDone);
        Assert.True(asyncResult.All().IsDone);
    }

    [Fact]
    public async Task RealClient_TimeoutMaxWait_FailsAllWithTheTimeoutCode_OnBothFlavours()
    {
        KafkaAdminClient sync = new KafkaAdminClient(Unreachable());
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);
        AsyncKafkaAdminClient async = new AsyncKafkaAdminClient(Unreachable());
        using IDisposable asyncDisposal = TestTimeout.Disposal(async);

        UnregisterControllerResult syncResult =
            sync.UnregisterController(1, new UnregisterControllerOptions { TimeoutMs = 10 });
        AsyncUnregisterControllerResult asyncResult =
            async.UnregisterController(1, new UnregisterControllerOptions { TimeoutMs = 10 });

        KafkaException syncFailure = Assert.Throws<KafkaException>(() => syncResult.All().Get(s_deadline));
        KafkaException asyncFailure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => asyncResult.All().Get()), s_deadline);

        Assert.Equal(RequestTimedOutCode, syncFailure.Code);
        Assert.True(syncFailure.IsRetriable);
        Assert.Equal(NodeAssignmentTimeoutMessage, syncFailure.Message);
        Assert.Equal(RequestTimedOutCode, asyncFailure.Code);
        Assert.Equal(NodeAssignmentTimeoutMessage, asyncFailure.Message);
    }

    [Fact]
    public void AfterDispose_UnregisterControllerThrowsObjectDisposed()
    {
        MockAdminClient mock = MockAdminClient.Create().Build();
        AsyncMockAdminClient asyncMock = AsyncMockAdminClient.Create().Build();
        KafkaAdminClient real = new KafkaAdminClient(Unreachable());
        AsyncKafkaAdminClient asyncReal = new AsyncKafkaAdminClient(Unreachable());
        TestTimeout.Run(mock.Dispose, s_deadline);
        TestTimeout.Run(asyncMock.Dispose, s_deadline);
        TestTimeout.Run(real.Dispose, s_deadline);
        TestTimeout.Run(asyncReal.Dispose, s_deadline);

        Assert.Throws<ObjectDisposedException>(() => mock.UnregisterController(1));
        Assert.Throws<ObjectDisposedException>(() => asyncMock.UnregisterController(1));
        Assert.Throws<ObjectDisposedException>(() => real.UnregisterController(1, new UnregisterControllerOptions()));
        Assert.Throws<ObjectDisposedException>(() => asyncReal.UnregisterController(1));
    }

    private static Dictionary<string, string> Unreachable() =>
        new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" };
}
