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
using System.Threading.Tasks;

using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The producer servicers, sync and async, over test producers built on the mock producer
/// (<c>dotnet/CLAUDE.md</c> §7.6).
/// </summary>
public sealed class ProducerServiceTests
{
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task CloseTimeout_ClosesWithTheRequestTimeout(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        ProducerProbe probe = new ProducerProbe();
        ulong producerId = server.RegisterTestProducer(probe);

        Proto.StatusResponse response = await server.Service.CloseTimeout(
            new Proto.CloseTimeoutRequest { ProducerId = producerId, TimeoutMs = 1500 }, server.Context);

        Assert.Null(response.Error);
        Assert.Equal(1, probe.CloseCalls);
        Assert.Equal(TimeSpan.FromMilliseconds(1500), probe.CloseTimeout);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Close_ClosesWithoutATimeout(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        ProducerProbe probe = new ProducerProbe();
        ulong producerId = server.RegisterTestProducer(probe);

        Proto.StatusResponse response = await server.Service.Close(
            new Proto.CloseRequest { ProducerId = producerId }, server.Context);

        Assert.Null(response.Error);
        Assert.Equal(1, probe.CloseCalls);
        Assert.Null(probe.CloseTimeout);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Send_PendingPastTheServerBound_IsTheServerTimeout(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        ulong producerId = server.RegisterPendingMock();
        server.SetSendTimeout(TimeSpan.FromMilliseconds(50));

        Proto.SendResponse response = await SendOnAnotherThread(server, producerId).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(7, response.Error.Code);
        Assert.Equal("dotnet server: producer future timed out after 120s", response.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Send_FailedWithATimeoutException_IsThatErrorNotTheServerTimeout(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        ulong producerId = server.RegisterPendingMock();

        Task<Proto.SendResponse> send = SendOnAnotherThread(server, producerId);
        await WaitUntil(() => server.PendingHistoryCount() == 1);
        Assert.True(server.ErrorPendingNext(new TimeoutException("the core timed out")));
        Proto.SendResponse response = await send.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(-5, response.Error.Code);
        Assert.Equal("the core timed out", response.Error.Message);
    }

    private static Task<Proto.SendResponse> SendOnAnotherThread(ProducerServer server, ulong producerId) =>
        Task.Run(() => server.Service.Send(
            new Proto.SendRequest { ProducerId = producerId, Record = new Proto.ProducerRecord { Topic = "t" } },
            server.Context));

    private static async Task WaitUntil(Func<bool> condition)
    {
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "condition not reached within 10 s");
            await Task.Delay(5);
        }
    }
}
