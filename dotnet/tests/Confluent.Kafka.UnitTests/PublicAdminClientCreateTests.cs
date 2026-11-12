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
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// An admin client is created by <c>Create</c>, Java's <c>Admin.create</c>, and has no public constructor. Creating
/// one checks its configuration across keys, as Java's <c>KafkaAdminClient</c> does: an explicit
/// <c>default.api.timeout.ms</c> below <c>request.timeout.ms</c>, or neither or both of <c>bootstrap.servers</c> and
/// <c>bootstrap.controllers</c>, fails creation, and <c>bootstrap.controllers</c> alone builds a client that refuses
/// the calls the controllers do not serve.
/// </summary>
public sealed class PublicAdminClientCreateTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int UnknownServerErrorCode = -1;

    private const int ConfigErrorCode = -10;

    private const int UnsupportedEndpointTypeCode = 115;

    private const string CreateFailed = "Failed to create new KafkaAdminClient";

    private const string UnsupportedEndpointType =
        "This Admin API is not yet supported when communicating directly with the controller quorum.";

    [Fact]
    public void Create_ReturnsAKafkaAdminClient()
    {
        IAdmin admin = IAdmin.Create(Unreachable());
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.IsType<KafkaAdminClient>(admin);
    }

    [Fact]
    public async Task AsyncCreate_ReturnsAnAsyncKafkaAdminClient()
    {
        IAsyncAdmin admin = IAsyncAdmin.Create(Unreachable());
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        Assert.IsType<AsyncKafkaAdminClient>(admin);
    }

    [Fact]
    public async Task Create_ByTheStaticClassesFullName_ReturnsTheClient()
    {
        IAdmin admin = Confluent.Kafka.Admin.Admin.Create(Unreachable());
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        IAsyncAdmin asyncAdmin = Confluent.Kafka.Admin.AsyncAdmin.Create(Unreachable());
        await using IAsyncDisposable asyncAdminDisposal = TestTimeout.AsyncDisposal(asyncAdmin);

        Assert.IsType<KafkaAdminClient>(admin);
        Assert.IsType<AsyncKafkaAdminClient>(asyncAdmin);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Create_ANullConf_ThrowsArgumentNullException(bool async)
    {
        Assert.Equal("conf", Assert.Throws<ArgumentNullException>(() => Create(async, null!)).ParamName);
    }

    [Fact]
    public void Clients_HaveNoPublicConstructor()
    {
        Assert.Empty(typeof(KafkaAdminClient).GetConstructors(BindingFlags.Public | BindingFlags.Instance));
        Assert.Empty(typeof(AsyncKafkaAdminClient).GetConstructors(BindingFlags.Public | BindingFlags.Instance));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Create_DefaultApiTimeoutBelowRequestTimeout_Fails(bool async)
    {
        AssertCreateFails(
            async,
            new Dictionary<string, object?> { ["bootstrap.servers"] = "localhost:1", ["default.api.timeout.ms"] = "500" },
            "The specified value of default.api.timeout.ms must be no smaller than the value of request.timeout.ms.");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Create_WithoutBootstrap_Fails(bool async)
    {
        AssertCreateFails(
            async,
            new Dictionary<string, object?>(),
            "You must set either bootstrap.servers or bootstrap.controllers");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Create_WithBothBootstraps_Fails(bool async)
    {
        AssertCreateFails(
            async,
            new Dictionary<string, object?> { ["bootstrap.servers"] = "localhost:1", ["bootstrap.controllers"] = "localhost:2" },
            "You cannot set both bootstrap.servers and bootstrap.controllers");
    }

    [Fact]
    public void BootstrapControllersAlone_ListTopics_FailsAtOnce()
    {
        IAdmin admin = IAdmin.Create(ControllersOnly());
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AssertUnsupportedEndpointType(FutureAssert.Throws<UnsupportedEndpointTypeException>(() => admin.ListTopics().Names().Get(s_deadline)));
    }

    [Fact]
    public async Task AsyncBootstrapControllersAlone_ListTopics_FailsAtOnce()
    {
        IAsyncAdmin admin = IAsyncAdmin.Create(ControllersOnly());
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AssertUnsupportedEndpointType(await FutureAssert.ThrowsAsync<UnsupportedEndpointTypeException>(
            () => TestTimeout.Run(() => admin.ListTopics().Names().Get(), s_deadline)));
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?> { ["bootstrap.servers"] = "127.0.0.1:1" };

    private static Dictionary<string, object?> ControllersOnly() =>
        new Dictionary<string, object?> { ["bootstrap.controllers"] = "localhost:1" };

    private static void AssertCreateFails(bool async, IReadOnlyDictionary<string, object?> config, string causeMessage)
    {
        KafkaException outer = Assert.Throws<KafkaException>(() => Create(async, config));

        Assert.Equal(UnknownServerErrorCode, outer.Code);
        Assert.Equal(CreateFailed, outer.Message);

        ConfigException cause = Assert.IsType<ConfigException>(outer.InnerException);
        Assert.Equal(ConfigErrorCode, cause.Code);
        Assert.Equal(causeMessage, cause.Message);
    }

    private static object Create(bool async, IReadOnlyDictionary<string, object?> config) =>
        async ? IAsyncAdmin.Create(config) : IAdmin.Create(config);

    private static void AssertUnsupportedEndpointType(KafkaException failure)
    {
        Assert.Equal(UnsupportedEndpointTypeCode, failure.Code);
        Assert.Equal(UnsupportedEndpointType, failure.Message);
        Assert.False(failure is RetriableException);
    }
}
