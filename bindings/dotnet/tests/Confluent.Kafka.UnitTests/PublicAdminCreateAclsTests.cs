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
/// <c>createAcls</c> end to end against <see cref="MockAdminClient"/>, no broker (M15/P6).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>There is no happy path here, and that is Java's behaviour rather than a gap.</b>
/// Java's <c>MockAdminClient.createAcls</c> throws
/// <c>UnsupportedOperationException("Not implemented yet")</c>
/// (<c>MockAdminClient.java:806-808</c>) and the Rust mock mirrors it as one exceptional
/// future <em>per binding</em> (<c>admin-client.md</c> §9). That still exercises the whole
/// receive path: the walk reads a real <c>kafka_admin_CreateAclsResult_t</c>, reassembles
/// each key from a real borrowed <c>kafka_common_AclBinding_t</c>, and reads each per-key
/// error through <see cref="KafkaException.FromBorrowedHandle"/> — so the key reader and
/// the borrowed-error rule are covered by real native memory, not by a stand-in.
/// </para>
/// <para>
/// ⚠⚠ <b>Every lookup below goes through <see cref="AclBinding"/>'s value equality</b>, and
/// several deliberately use a <em>freshly constructed</em> binding rather than the instance
/// that was submitted. Without value equality every such lookup misses silently while an
/// enumeration-only test stays green — the reason PLAN D39 made it a requirement.
/// </para>
/// </remarks>
public sealed class PublicAdminCreateAclsTests
{
    private const string NotImplemented = "Not implemented yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// ⚠ <b>The value-equality test (T-N6).</b> A binding built fresh, equal by value to the
    /// one submitted, finds its own awaitable in <see cref="CreateAclsResult.Values"/>.
    /// </summary>
    [Fact]
    public async Task Values_AreKeyedByValue_NotByReference()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateAclsResult result = admin.CreateAcls(new[] { Binding("keyed-by-value") });

        AclBinding lookup = Binding("keyed-by-value");
        Assert.NotSame(lookup, Assert.Single(result.Values).Key);
        Assert.True(result.Values.ContainsKey(lookup));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[lookup], s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// Each binding gets <b>its own</b> awaitable, and the key the result hands back round
    /// trips through the ABI unchanged — the seven flat accessors reassembled into the nested
    /// Java shape (PLAN D36).
    /// </summary>
    [Fact]
    public async Task EachBinding_GetsItsOwnTask_AndItsKeyRoundTrips()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AclBinding alpha = new AclBinding(
            new ResourcePattern(ResourceType.Topic, "rt-alpha", PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
        AclBinding beta = new AclBinding(
            new ResourcePattern(ResourceType.Group, "rt-beta", PatternType.Prefixed),
            new AccessControlEntry(
                "User:bob", "10.0.0.7", AclOperation.DescribeConfigs, AclPermissionType.Deny));

        CreateAclsResult result = admin.CreateAcls(new[] { alpha, beta });

        Assert.Equal(2, result.Values.Count);
        Assert.NotSame(result.Values[alpha], result.Values[beta]);

        // Every column survived the round trip: a swapped pair in the projector or the reader
        // would produce a key that no longer equals the one submitted.
        Assert.Contains(alpha, result.Values.Keys);
        Assert.Contains(beta, result.Values.Keys);

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[alpha], s_deadline));
        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[beta], s_deadline));
    }

    /// <summary>
    /// One binding can be awaited without awaiting the others — the per-key granularity Java
    /// publishes, and the discriminator against a single shared awaitable.
    /// </summary>
    [Fact]
    public async Task OneKey_IsAwaitableWithoutTheOthers()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AclBinding first = Binding("solo-first");
        CreateAclsResult result = admin.CreateAcls(new[] { first, Binding("solo-second") });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[first], s_deadline));
        Assert.Equal(NotImplemented, failure.Message);

        // Observe the other one too, so no task is left with an unobserved fault.
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// <c>All()</c> faults when any binding failed — Java's <c>KafkaFuture.allOf</c>.
    /// </summary>
    [Fact]
    public async Task All_FaultsWhenAnyBindingFails()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateAclsResult result = admin.CreateAcls(new[] { Binding("all-a"), Binding("all-b") });

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// <c>All()</c> over an empty request completes — there is nothing to wait for, and Java's
    /// <c>allOf</c> of nothing is a completed future.
    /// </summary>
    [Fact]
    public async Task All_OverAnEmptyRequest_Completes()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateAclsResult result = admin.CreateAcls(Array.Empty<AclBinding>());

        Assert.Empty(result.Values);
        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// A repeated binding collapses to one entry — Java keys its result on a <c>Map</c>.
    /// </summary>
    [Fact]
    public async Task DuplicateBindings_CollapseToOneEntry()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        CreateAclsResult result = admin.CreateAcls(new[] { Binding("dup"), Binding("dup") });

        Assert.Single(result.Values);
        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// A non-ASCII resource name survives out and back — the UTF-8 round trip through both
    /// the row projector and the binding reader (ffi §A3/§B3).
    /// </summary>
    [Fact]
    public async Task NonAsciiName_RoundTripsThroughTheResultKey()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        AclBinding binding = Binding("tópico-café-日本");
        CreateAclsResult result = admin.CreateAcls(new[] { binding });

        AclBinding key = Assert.Single(result.Values).Key;
        Assert.Equal("tópico-café-日本", key.Pattern.Name);
        Assert.Equal(binding, key);

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// Preconditions are rejected before any native call (ffi §B5): a null collection, a null
    /// element, and a negative timeout the ABI would silently read as "unset".
    /// </summary>
    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        Assert.Equal(
            "acls",
            Assert.Throws<ArgumentNullException>(() => admin.CreateAcls(null!)).ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => admin.CreateAcls(new AclBinding?[] { Binding("ok"), null }!));
        Assert.Equal("acls", nullElement.ParamName);
        Assert.StartsWith(
            "The ACL bindings must not contain a null element.", nullElement.Message, StringComparison.Ordinal);

        ArgumentOutOfRangeException negativeTimeout = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.CreateAcls(
                new[] { Binding("ok") }, new CreateAclsOptions { TimeoutMs = -1 }));
        Assert.StartsWith(
            "CreateAclsOptions.TimeoutMs must not be negative; leave it null to use the client default.",
            negativeTimeout.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// Using the client after it is closed throws <see cref="ObjectDisposedException"/>
    /// rather than reaching a destroyed handle.
    /// </summary>
    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = new MockAdminClient(1);
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.CreateAcls(new[] { Binding("gone") }));
    }

    /// <summary>
    /// <see cref="CreateAclsOptions"/> ships Java's defaults: an unset timeout, and nothing
    /// else — Java's type adds no field to <c>AbstractOptions</c>.
    /// </summary>
    [Fact]
    public void Options_DefaultToJavasDefaults() => Assert.Null(new CreateAclsOptions().TimeoutMs);

    private static AclBinding Binding(string name) =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
}
