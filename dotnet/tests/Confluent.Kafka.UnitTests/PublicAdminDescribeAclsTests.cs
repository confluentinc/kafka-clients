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
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Resource;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>DescribeAcls</c> end to end against the mocks, whose one future fails with the mock's own answer, as Java's
/// mock does: the result has <c>Values</c> and no <c>All</c>, the preconditions and the options.
/// </summary>
public sealed class PublicAdminDescribeAclsTests
{
    private const string NotImplemented = "Not implemented yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Surface_IsValuesOnly_WithNoAll()
    {
        MethodInfo[] declared = typeof(DescribeAclsResult)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .ToArray();

        // A control: the reflection sees this type's own methods, so the absence below is not vacuous.
        Assert.Contains(declared, method => method.Name == nameof(DescribeAclsResult.Values));
        Assert.DoesNotContain(declared, method => method.Name == "All");

        Assert.Equal(
            typeof(KafkaFuture<IReadOnlyCollection<AclBinding>>),
            typeof(DescribeAclsResult)
                .GetMethod(nameof(DescribeAclsResult.Values))!
                .ReturnType);
        Assert.Equal(
            typeof(AsyncKafkaFuture<IReadOnlyCollection<AclBinding>>),
            typeof(AsyncDescribeAclsResult)
                .GetMethod(nameof(AsyncDescribeAclsResult.Values))!
                .ReturnType);
    }

    [Fact]
    public async Task WholeCallFailure_FaultsTheSingleTask()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeAclsResult result = admin.DescribeAcls(Filter("unimplemented"));

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values().Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task Values_IsTheSameTaskEveryCall()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeAclsResult result = admin.DescribeAcls(Filter("same-task"));
        Assert.Same(result.Values().Get(), result.Values().Get());

        await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values().Get(), s_deadline));
    }

    [Fact]
    public async Task WildcardFilter_IsAccepted_NotRejected()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeAclsResult result = admin.DescribeAcls(
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Any, null, PatternType.Any),
                new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any)));

        NotSupportedException failure = await Assert.ThrowsAsync<NotSupportedException>(
            () => TestTimeout.Run(() => result.Values().Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(
            "filter",
            Assert.Throws<ArgumentNullException>(() => admin.DescribeAcls(null!)).ParamName);

        // A negative timeout is not a precondition: it is sent as 0.
        Assert.Null(Record.Exception(
            () => admin.DescribeAcls(
                Filter("t"), new DescribeAclsOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.DescribeAcls(Filter("gone")));
    }

    [Fact]
    public void Options_DefaultToJavasDefaults() => Assert.Null(new DescribeAclsOptions().TimeoutMs);

    private static AclBindingFilter Filter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntryFilter(
                "User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
}
