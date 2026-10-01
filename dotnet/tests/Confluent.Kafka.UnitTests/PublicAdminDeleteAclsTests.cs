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
/// <c>deleteAcls</c> end to end against <see cref="MockAdminClient"/>, no broker (M15/P6).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Every filter fails here, and that is Java's behaviour rather than a gap.</b> Java's
/// <c>MockAdminClient.deleteAcls</c> throws
/// <c>UnsupportedOperationException("Not implemented yet")</c>
/// (<c>MockAdminClient.java:816-818</c>) and the Rust mock mirrors it as one exceptional
/// future <em>per filter</em> (<c>admin-client.md</c> §9). The ABI still materialises a real
/// result table, so the <b>key</b> reader runs over real borrowed
/// <c>kafka_common_AclBindingFilter_t</c> handles and each filter-level error is read through
/// <see cref="KafkaException.FromBorrowedHandle"/> — real native memory, not a stand-in. What
/// the mock cannot reach is a filter-level <em>success</em>, and with it the whole inner
/// axis; <c>AdminP6ResultMarshalTests</c> covers that by injection.
/// </para>
/// <para>
/// ⚠⚠ <b>The round trip carries the null-versus-empty rule all the way through native.</b> A
/// filter with a <see langword="null"/> name and one with <c>""</c> are submitted, decoded by
/// the core, handed back, and must still be two distinct keys.
/// </para>
/// </remarks>
public sealed class PublicAdminDeleteAclsTests
{
    private const string NotImplemented = "Not implemented yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// A filter built fresh, equal by value to the one submitted, finds its own awaitable
    /// (PLAN D39).
    /// </summary>
    [Fact]
    public async Task Values_AreKeyedByValue_NotByReference()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        DeleteAclsResult result = admin.DeleteAcls(new[] { Filter("keyed-by-value") });

        AclBindingFilter lookup = Filter("keyed-by-value");
        Assert.NotSame(lookup, Assert.Single(result.Values).Key);
        Assert.True(result.Values.ContainsKey(lookup));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[lookup], s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// ⚠⚠ A <see langword="null"/> name and an <c>""</c> name survive the full round trip as
    /// <b>two</b> distinct keys. Collapsing either direction — on the way out or on the way
    /// back — loses one entry here.
    /// </summary>
    [Fact]
    public async Task NullAndEmpty_RoundTripAsDistinctKeys()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AclBindingFilter wildcard = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, null, PatternType.Any),
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any));
        AclBindingFilter literalEmpty = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, string.Empty, PatternType.Any),
            new AccessControlEntryFilter(
                string.Empty, string.Empty, AclOperation.Any, AclPermissionType.Any));

        DeleteAclsResult result = admin.DeleteAcls(new[] { wildcard, literalEmpty });

        Assert.Equal(2, result.Values.Count);
        Assert.True(result.Values.ContainsKey(wildcard));
        Assert.True(result.Values.ContainsKey(literalEmpty));

        // The decoded keys really are null-vs-empty, not two copies of one of them.
        foreach (AclBindingFilter key in result.Values.Keys)
        {
            Assert.Equal(key.PatternFilter.Name, key.EntryFilter.Principal);
            Assert.Equal(key.PatternFilter.Name, key.EntryFilter.Host);
        }

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// Each filter gets <b>its own</b> awaitable, and its key round trips through the ABI
    /// unchanged — the seven flat accessors reassembled into the nested Java shape (PLAN D36).
    /// </summary>
    [Fact]
    public async Task EachFilter_GetsItsOwnTask_AndItsKeyRoundTrips()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AclBindingFilter alpha = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, "rt-alpha", PatternType.Literal),
            new AccessControlEntryFilter(
                "User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
        AclBindingFilter beta = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Group, "rt-beta", PatternType.Match),
            new AccessControlEntryFilter(
                "User:bob", "10.0.0.7", AclOperation.DescribeConfigs, AclPermissionType.Deny));

        DeleteAclsResult result = admin.DeleteAcls(new[] { alpha, beta });

        Assert.Equal(2, result.Values.Count);
        Assert.NotSame(result.Values[alpha], result.Values[beta]);
        Assert.Contains(alpha, result.Values.Keys);
        Assert.Contains(beta, result.Values.Keys);

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[alpha], s_deadline));
        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[beta], s_deadline));
    }

    /// <summary>
    /// One filter can be awaited without awaiting the others — the per-key granularity Java
    /// publishes.
    /// </summary>
    [Fact]
    public async Task OneKey_IsAwaitableWithoutTheOthers()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AclBindingFilter first = Filter("solo-first");
        DeleteAclsResult result = admin.DeleteAcls(new[] { first, Filter("solo-second") });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[first], s_deadline));
        Assert.Equal(NotImplemented, failure.Message);

        // Observe the other one too, so no task is left with an unobserved fault.
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// <c>All()</c> over an empty request completes with an empty collection.
    /// </summary>
    [Fact]
    public async Task All_OverAnEmptyRequest_IsAnEmptyCollection()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        DeleteAclsResult result = admin.DeleteAcls(Array.Empty<AclBindingFilter>());

        Assert.Empty(result.Values);
        Assert.Empty(await TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// A repeated filter collapses to one entry — Java keys its result on a map, so two
    /// value-equal filters are one key.
    /// </summary>
    [Fact]
    public async Task DuplicateFilters_CollapseToOneEntry()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        DeleteAclsResult result = admin.DeleteAcls(new[] { Filter("dup"), Filter("dup") });

        Assert.Single(result.Values);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// A non-ASCII resource name survives the hand-rolled UTF-8 round trip in both directions
    /// (ffi §A3/§B3).
    /// </summary>
    [Fact]
    public async Task NonAsciiName_RoundTripsThroughTheResultKey()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AclBindingFilter filter = Filter("tópico-café-日本");
        DeleteAclsResult result = admin.DeleteAcls(new[] { filter });

        Assert.Equal("tópico-café-日本", Assert.Single(result.Values).Key.PatternFilter.Name);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// Preconditions are .NET exceptions raised before any native call (ffi §B5).
    /// </summary>
    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        Assert.Equal(
            "filters",
            Assert.Throws<ArgumentNullException>(() => admin.DeleteAcls(null!)).ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => admin.DeleteAcls(new AclBindingFilter[] { null! }));
        Assert.StartsWith(
            "The ACL filters must not contain a null element.", nullElement.Message, StringComparison.Ordinal);
        Assert.Equal("filters", nullElement.ParamName);

        ArgumentOutOfRangeException negativeTimeout = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DeleteAcls(
                new[] { Filter("t") }, new DeleteAclsOptions { TimeoutMs = -1 }));
        Assert.StartsWith(
            "DeleteAclsOptions.TimeoutMs must not be negative; leave it null to use the client default.",
            negativeTimeout.Message,
            StringComparison.Ordinal);
    }

    /// <summary>A call after <c>Dispose</c> throws <see cref="ObjectDisposedException"/>.</summary>
    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = new MockAdminClient(1);
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.DeleteAcls(new[] { Filter("gone") }));
    }

    /// <summary>The options default to Java's defaults — an unset timeout.</summary>
    [Fact]
    public void Options_DefaultToJavasDefaults() => Assert.Null(new DeleteAclsOptions().TimeoutMs);

    private static AclBindingFilter Filter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntryFilter(
                "User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
}
