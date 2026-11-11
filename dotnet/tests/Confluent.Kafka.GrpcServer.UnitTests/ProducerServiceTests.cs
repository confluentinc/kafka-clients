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
using System.Collections.Generic;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using FutureTimeoutException = Confluent.Kafka.TimeoutException;
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
        Assert.True(server.ErrorPendingNext(new FutureTimeoutException("the core timed out")));
        Proto.SendResponse response = await send.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(-5, response.Error.Code);
        Assert.Equal("the core timed out", response.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Transaction_InitBeginCommit_ReachesTheProducer(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        ulong producerId = server.RegisterTransactionalMock();
        Proto.TransactionRequest request = new Proto.TransactionRequest { ProducerId = producerId };

        Assert.Null((await server.Service.InitTransactions(request, server.Context)).Error);
        Assert.Null((await server.Service.BeginTransaction(request, server.Context)).Error);
        Assert.Equal((true, false, false), server.MockTransactionState());
        Assert.Null((await server.Service.CommitTransaction(request, server.Context)).Error);

        Assert.Equal((false, true, false), server.MockTransactionState());
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Transaction_Abort_ReachesTheProducer(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        ulong producerId = server.RegisterTransactionalMock();
        Proto.TransactionRequest request = new Proto.TransactionRequest { ProducerId = producerId };

        Assert.Null((await server.Service.InitTransactions(request, server.Context)).Error);
        Assert.Null((await server.Service.BeginTransaction(request, server.Context)).Error);
        Assert.Null((await server.Service.AbortTransaction(request, server.Context)).Error);

        Assert.Equal((false, false, true), server.MockTransactionState());
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Transaction_ProducerError_IsMappedToItsCode(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        ulong producerId = server.RegisterTransactionalMock();

        Proto.StatusResponse response = await server.Service.CommitTransaction(
            new Proto.TransactionRequest { ProducerId = producerId }, server.Context);

        Assert.Equal(-4, response.Error.Code);
        Assert.Equal("MockProducer hasn't been initialized for transactions.", response.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Transaction_UnknownProducer_IsIllegalState(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        Proto.TransactionRequest request = new Proto.TransactionRequest { ProducerId = 42 };

        Proto.StatusResponse[] responses =
        {
            await server.Service.InitTransactions(request, server.Context),
            await server.Service.BeginTransaction(request, server.Context),
            await server.Service.CommitTransaction(request, server.Context),
            await server.Service.AbortTransaction(request, server.Context),
            await server.Service.SendOffsetsToTransaction(
                new Proto.SendOffsetsToTransactionRequest { ProducerId = 42, GroupMetadataId = 1 }, server.Context),
        };

        foreach (Proto.StatusResponse response in responses)
        {
            Assert.Equal(-4, response.Error.Code);
            Assert.Equal("unknown producer_id 42", response.Error.Message);
        }
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task SendOffsetsToTransaction_UnknownGroupMetadata_IsIllegalState(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        ulong producerId = server.RegisterTransactionalMock();

        Proto.StatusResponse response = await server.Service.SendOffsetsToTransaction(
            new Proto.SendOffsetsToTransactionRequest { ProducerId = producerId, GroupMetadataId = 7 }, server.Context);

        Assert.Equal(-4, response.Error.Code);
        Assert.Equal("unknown group_metadata_id 7", response.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task SendOffsetsToTransaction_SendsTheOffsetsUnderTheStoredGroup(string flavour)
    {
        using ProducerServer server = ProducerServer.Create(flavour);
        ulong producerId = server.RegisterTransactionalMock();
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        ulong groupMetadataId = server.GroupMetadata.Add(consumer.GroupMetadata());
        Proto.TransactionRequest request = new Proto.TransactionRequest { ProducerId = producerId };
        Assert.Null((await server.Service.InitTransactions(request, server.Context)).Error);
        Assert.Null((await server.Service.BeginTransaction(request, server.Context)).Error);

        Proto.SendOffsetsToTransactionRequest send = new Proto.SendOffsetsToTransactionRequest
        {
            ProducerId = producerId,
            GroupMetadataId = groupMetadataId,
        };
        send.Offsets.Add(new Proto.OffsetEntry { Topic = "t", Partition = 0, Offset = 5 });
        send.Offsets.Add(new Proto.OffsetEntry { Topic = "t", Partition = 1, Offset = 9, LeaderEpoch = 3, Metadata = "m" });
        Proto.StatusResponse response = await server.Service.SendOffsetsToTransaction(send, server.Context);
        Assert.Null(response.Error);
        Assert.Null((await server.Service.CommitTransaction(request, server.Context)).Error);

        IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> sent =
            Assert.Single(server.MockConsumerGroupOffsetsHistory());
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets = sent["dummy.group.id"];
        Assert.Equal(new OffsetAndMetadata(5, string.Empty, null), offsets[new TopicPartition("t", 0)]);
        Assert.Equal(new OffsetAndMetadata(9, "m", 3), offsets[new TopicPartition("t", 1)]);
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
