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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>describeAcls</c> end to end against <see cref="MockAdminClient"/>, no broker (M15/P6).
/// </summary>
/// <remarks>
/// Java's <c>MockAdminClient.describeAcls</c> throws
/// <c>UnsupportedOperationException("Not implemented yet")</c>
/// (<c>MockAdminClient.java:811-813</c>), mirrored by the Rust mock as one exceptional future
/// for the whole call. Because the result carries no per-key error channel, that failure
/// arrives as the callback's own <b>owned</b> error and faults the single task — which is
/// exactly the contract being asserted here.
/// </remarks>
public sealed class PublicAdminDescribeAclsTests
{
    private const string NotImplemented = "Not implemented yet";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// ⚠⚠ <b>T-N7.</b> The type publishes <c>Values()</c> and <b>no</b> <c>All()</c> — Java
    /// declares none (<c>DescribeAclsResult.java:39</c>), so adding one would be public
    /// surface Java does not have.
    /// </summary>
    [Fact]
    public void Surface_IsValuesOnly_WithNoAll()
    {
        MethodInfo[] declared = typeof(DescribeAclsResult)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .ToArray();

        // Control-positive: the reflection really sees this type's own methods, so a lookup
        // returning nothing could not make the absence assertion pass vacuously.
        Assert.Contains(declared, method => method.Name == nameof(DescribeAclsResult.Values));
        Assert.DoesNotContain(declared, method => method.Name == "All");

        Assert.Equal(
            typeof(Task<IReadOnlyCollection<AclBinding>>),
            typeof(DescribeAclsResult)
                .GetMethod(nameof(DescribeAclsResult.Values))!
                .ReturnType);
    }

    /// <summary>
    /// A whole-call failure faults the <b>single</b> task, with the message and code the core
    /// reported — there is no per-key channel to absorb it.
    /// </summary>
    [Fact]
    public async Task WholeCallFailure_FaultsTheSingleTask()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        DescribeAclsResult result = admin.DescribeAcls(Filter("unimplemented"));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.Values, s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
        Assert.NotEqual(0, failure.Code);
    }

    /// <summary>
    /// <c>Values()</c> hands back the <b>same</b> task instance every call, so a fault can
    /// never go unobserved on a second projection.
    /// </summary>
    [Fact]
    public async Task Values_IsTheSameTaskEveryCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        DescribeAclsResult result = admin.DescribeAcls(Filter("same-task"));
        Assert.Same(result.Values(), result.Values());

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.Values, s_deadline));
    }

    /// <summary>
    /// A wildcard filter — nulls and ANY everywhere — is accepted rather than screened out:
    /// the ABI rejects no combination (<c>confluent_kafka.h:8052-8063</c>), as Java's filter
    /// constructors do not.
    /// </summary>
    [Fact]
    public async Task WildcardFilter_IsAccepted_NotRejected()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        DescribeAclsResult result = admin.DescribeAcls(
            new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.Any, null, PatternType.Any),
                new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any)));

        // It reached the core and came back as a call-level failure, not an argument error.
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.Values, s_deadline));
        Assert.Equal(NotImplemented, failure.Message);
    }

    /// <summary>
    /// Preconditions are .NET exceptions raised before any native call (ffi §B5).
    /// </summary>
    [Fact]
    public void Preconditions_AreRejectedBeforeTheNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        Assert.Equal(
            "filter",
            Assert.Throws<ArgumentNullException>(() => admin.DescribeAcls(null!)).ParamName);

        ArgumentOutOfRangeException negativeTimeout = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeAcls(
                Filter("t"), new DescribeAclsOptions { TimeoutMs = -1 }));
        Assert.StartsWith(
            "DescribeAclsOptions.TimeoutMs must not be negative; leave it null to use the client default.",
            negativeTimeout.Message,
            StringComparison.Ordinal);
    }

    /// <summary>A call after <c>Dispose</c> throws <see cref="ObjectDisposedException"/>.</summary>
    [Fact]
    public void AfterDispose_Throws()
    {
        MockAdminClient admin = new MockAdminClient(1);
        admin.Dispose();

        Assert.Throws<ObjectDisposedException>(() => admin.DescribeAcls(Filter("gone")));
    }

    /// <summary>The options default to Java's defaults — an unset timeout.</summary>
    [Fact]
    public void Options_DefaultToJavasDefaults() => Assert.Null(new DescribeAclsOptions().TimeoutMs);

    private static AclBindingFilter Filter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntryFilter(
                "User:alice", "*", AclOperation.Read, AclPermissionType.Allow));
}
