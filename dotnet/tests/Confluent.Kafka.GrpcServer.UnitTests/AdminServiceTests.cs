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

using Confluent.Kafka.Admin;
using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The admin servicers, sync and async, over the mock admin client (<c>dotnet/CLAUDE.md</c> §7.6).
/// </summary>
public sealed class AdminServiceTests
{
    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task DescribeFeatures_WithoutANodeId_AnswersTheMockMetadata(string flavour)
    {
        using AdminServer server = AdminServer.Create(flavour);
        ulong adminId = await server.CreateMockAdmin();

        Proto.DescribeFeaturesResponse response = await server.Service.DescribeFeatures(
            new Proto.DescribeFeaturesRequest { AdminId = adminId }, server.Context);

        Assert.Null(response.Error);
        Assert.Equal(123, response.Metadata.FinalizedFeaturesEpoch);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task DescribeFeatures_WithANodeId_AnswersTheMockMetadata(string flavour)
    {
        using AdminServer server = AdminServer.Create(flavour);
        ulong adminId = await server.CreateMockAdmin();

        Proto.DescribeFeaturesResponse response = await server.Service.DescribeFeatures(
            new Proto.DescribeFeaturesRequest { AdminId = adminId, NodeId = 0 }, server.Context);

        Assert.Null(response.Error);
        Assert.Equal(123, response.Metadata.FinalizedFeaturesEpoch);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task UnregisterController_OnTheDefaultMock_IsUnsupportedVersionWithAnEmptyMessage(string flavour)
    {
        using AdminServer server = AdminServer.Create(flavour);
        ulong adminId = await server.CreateMockAdmin();

        Proto.StatusResponse response = await server.Service.UnregisterController(
            new Proto.UnregisterControllerRequest { AdminId = adminId, ControllerId = 1, TimeoutMs = 5000 },
            server.Context);

        Assert.Equal(35, response.Error.Code);
        Assert.Equal("", response.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task UnregisterController_OnAnUnknownAdmin_IsTheUnknownAdminError(string flavour)
    {
        using AdminServer server = AdminServer.Create(flavour);

        Proto.StatusResponse response = await server.Service.UnregisterController(
            new Proto.UnregisterControllerRequest { AdminId = 99, ControllerId = 1 }, server.Context);

        Assert.Equal(-4, response.Error.Code);
        Assert.Equal("unknown admin_id 99", response.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task DescribeUserScramCredentials_WhenTheResponseItselfFailed_IsTheWholeCallErrorWithNoEntries(string flavour)
    {
        using AdminServer server = AdminServer.Create(flavour);
        ulong adminId = await server.CreateMockAdmin();
        Proto.DescribeUserScramCredentialsRequest request =
            new Proto.DescribeUserScramCredentialsRequest { AdminId = adminId };
        request.Users.Add("alice");

        Proto.DescribeUserScramCredentialsResponse response =
            await server.Service.DescribeUserScramCredentials(request, server.Context);

        Assert.Empty(response.Entries);
        Assert.Equal(-31, response.Error.Code);
        Assert.Equal("Not implemented yet", response.Error.Message);
    }

    [Theory]
    [InlineData("sync", false)]
    [InlineData("sync", true)]
    [InlineData("async", false)]
    [InlineData("async", true)]
    public async Task Close_OnAMock_DisposesTheRemovedClient(string flavour, bool timed)
    {
        using AdminServer server = AdminServer.Create(flavour);
        ulong adminId = await server.CreateMockAdmin();
        object client = server.Client(adminId);
        Proto.AdminCloseRequest request = new Proto.AdminCloseRequest { AdminId = adminId };
        if (timed)
        {
            request.TimeoutMs = 5000;
        }

        Proto.StatusResponse response = await server.Service.Close(request, server.Context);

        Assert.Null(response.Error);
        Assert.Throws<ObjectDisposedException>(() => ListTopics(client));
        Proto.DescribeFeaturesResponse after = await server.Service.DescribeFeatures(
            new Proto.DescribeFeaturesRequest { AdminId = adminId }, server.Context);
        Assert.Equal(-4, after.Error.Code);
        Assert.Equal($"unknown admin_id {adminId}", after.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Close_OnAnUnknownAdmin_Succeeds(string flavour)
    {
        using AdminServer server = AdminServer.Create(flavour);

        Proto.StatusResponse response = await server.Service.Close(
            new Proto.AdminCloseRequest { AdminId = 99 }, server.Context);

        Assert.Null(response.Error);
    }

    private static void ListTopics(object client)
    {
        if (client is IAdmin admin)
        {
            admin.ListTopics();
        }
        else
        {
            ((IAsyncAdmin)client).ListTopics();
        }
    }
}
