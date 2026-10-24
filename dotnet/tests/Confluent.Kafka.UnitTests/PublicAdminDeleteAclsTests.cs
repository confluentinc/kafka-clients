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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>DeleteAcls</c> end to end against the mocks, which fail every filter with the mock's own answer, as Java's mock
/// does: per-filter results keyed by filter value (a null and an empty name stay distinct keys), the preconditions
/// and the options.
/// </summary>
public sealed class PublicAdminDeleteAclsTests
{
    private const string NotImplemented = "Not implemented yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Values_AreKeyedByValue_NotByReference()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDeleteAclsResult result = admin.DeleteAcls(new[] { Filter("keyed-by-value") });

        AclBindingFilter lookup = Filter("keyed-by-value");
        Assert.NotSame(lookup, Assert.Single(result.Values).Key);
        Assert.True(result.Values.ContainsKey(lookup));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[lookup].Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task NullAndEmpty_RoundTripAsDistinctKeys()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AclBindingFilter wildcard = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Any),
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any));
        AclBindingFilter literalEmpty = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Any),
            new AccessControlEntryFilter(
                string.Empty, string.Empty, AclOperation.Any, AclPermissionType.Any));

        AsyncDeleteAclsResult result = admin.DeleteAcls(new[] { wildcard, literalEmpty });

        Assert.Equal(2, result.Values.Count);
        Assert.True(result.Values.ContainsKey(wildcard));
        Assert.True(result.Values.ContainsKey(literalEmpty));

        foreach (AclBindingFilter key in result.Values.Keys)
        {
            Assert.Equal(key.PatternFilter.Name, key.EntryFilter.Principal);
            Assert.Equal(key.PatternFilter.Name, key.EntryFilter.Host);
        }

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public async Task EachFilter_GetsItsOwnTask_AndItsKeyRoundTrips()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AclBindingFilter alpha = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, "rt-alpha", PatternType.Literal),
            new AccessControlEntryFilter(
                "User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
        AclBindingFilter beta = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Group, "rt-beta", PatternType.Match),
            new AccessControlEntryFilter(
                "User:bob", "10.0.0.7", AclOperation.DescribeConfigs, AclPermissionType.Deny));

        AsyncDeleteAclsResult result = admin.DeleteAcls(new[] { alpha, beta });

        Assert.Equal(2, result.Values.Count);
        Assert.NotSame(result.Values[alpha].Get(), result.Values[beta].Get());
        Assert.Contains(alpha, result.Values.Keys);
        Assert.Contains(beta, result.Values.Keys);

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[alpha].Get(), s_deadline));
        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[beta].Get(), s_deadline));
    }

    [Fact]
    public async Task OneKey_IsAwaitableWithoutTheOthers()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AclBindingFilter first = Filter("solo-first");
        AsyncDeleteAclsResult result = admin.DeleteAcls(new[] { first, Filter("solo-second") });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[first].Get(), s_deadline));
        Assert.Equal(NotImplemented, failure.Message);

        // Observe the other one too, so no task is left with an unobserved fault.
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public async Task All_OverAnEmptyRequest_IsAnEmptyCollection()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDeleteAclsResult result = admin.DeleteAcls(Array.Empty<AclBindingFilter>());

        Assert.Empty(result.Values);
        Assert.Empty(await TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public async Task DuplicateFilters_CollapseToOneEntry()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDeleteAclsResult result = admin.DeleteAcls(new[] { Filter("dup"), Filter("dup") });

        Assert.Single(result.Values);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public async Task NonAsciiName_RoundTripsThroughTheResultKey()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AclBindingFilter filter = Filter("tópico-café-日本");
        AsyncDeleteAclsResult result = admin.DeleteAcls(new[] { filter });

        Assert.Equal("tópico-café-日本", Assert.Single(result.Values).Key.PatternFilter.Name);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(
            "filters",
            Assert.Throws<ArgumentNullException>(() => admin.DeleteAcls(null!)).ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => admin.DeleteAcls(new AclBindingFilter[] { null! }));
        Assert.StartsWith(
            "The ACL filters must not contain a null element.", nullElement.Message, StringComparison.Ordinal);
        Assert.Equal("filters", nullElement.ParamName);

        // A negative timeout is not a precondition: it is sent as 0.
        Assert.Null(Record.Exception(
            () => admin.DeleteAcls(
                new[] { Filter("t") }, new DeleteAclsOptions { TimeoutMs = -1 })));
    }

    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.DeleteAcls(new[] { Filter("gone") }));
    }

    [Fact]
    public void Options_DefaultToJavasDefaults() => Assert.Null(new DeleteAclsOptions().TimeoutMs);

    [Fact]
    public void G4_5_FilterResult_ExposesException_NotError()
    {
        const BindingFlags Public = BindingFlags.Public | BindingFlags.Instance;
        PropertyInfo? exception = typeof(DeleteAclsResult.FilterResult).GetProperty("Exception", Public);
        Assert.NotNull(exception);
        Assert.Equal(typeof(KafkaException), exception!.PropertyType);
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(exception));

        Assert.Null(typeof(DeleteAclsResult.FilterResult).GetProperty("Error", Public | BindingFlags.NonPublic));
    }

    private static AclBindingFilter Filter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntryFilter(
                "User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
}
