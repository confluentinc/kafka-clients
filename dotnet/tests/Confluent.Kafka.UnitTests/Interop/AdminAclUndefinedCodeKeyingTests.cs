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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P13.3 F8 — <c>createAcls</c> and <c>deleteAcls</c> with an ACL enum value that is not a
/// defined member, through production's own submit and the real ABI.
/// </summary>
/// <remarks>
/// <para>
/// The core reads each code through Java's <c>fromCode</c>, so an undefined one becomes
/// <c>UNKNOWN</c>, and it keys its per-key callback by that normalized value: the validated
/// binding for <c>createAcls</c> (since PR #201 round 70) and the filter for
/// <c>deleteAcls</c>. Before F8 the binding kept the caller's raw value, which broke two ways:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <b>One undefined code:</b> the callback's key (<c>Unknown</c>) matched no awaitable, so the
/// awaitable was faulted at the countdown's end with the bridge's "result contained no entry"
/// text instead of the core's answer. The <see cref="KafkaException.Message"/> assertion is
/// what tells the two apart, and the recorded row code proves what reached the core.
/// </description></item>
/// <item><description>
/// <b>Two inputs differing only in undefined codes (98 / 99):</b> two awaitables and a
/// countdown armed for two, where the core answers one — both awaitables hung. Now the two
/// inputs are one key, one row is sent, and the one awaitable completes.
/// </description></item>
/// </list>
/// <para>
/// Each theory runs once per ACL enum column, since each column is normalized by a different
/// constructor. The mock answers every binding and every filter with Java's own
/// <c>UnsupportedOperationException("Not implemented yet")</c>
/// (<c>MockAdminClient.java:806-808</c>, <c>:816-818</c>). A released client proves the
/// countdown was armed with the number of callbacks the core actually made.
/// </para>
/// </remarks>
public sealed class AdminAclUndefinedCodeKeyingTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_releaseBound = TimeSpan.FromSeconds(5);

    /// <summary>The code the mock reports for an RPC Java's mock does not implement.</summary>
    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// <c>createAcls</c>: a binding with one undefined code is sent with <c>UNKNOWN</c> (0) in
    /// that column, and the caller's own binding finds the core's answer.
    /// </summary>
    /// <param name="column">The ACL enum column carrying the undefined code.</param>
    [Theory]
    [InlineData(Column.ResourceType)]
    [InlineData(Column.PatternType)]
    [InlineData(Column.Operation)]
    [InlineData(Column.PermissionType)]
    public async Task CreateAcls_AnUndefinedCode_IsSentAsUnknown_AndResolvesWithTheMocksAnswer(Column column)
    {
        AclBinding binding = Binding(column, 99);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Rows sent = Rows.None;
        CreateAclsResult result = admin.CreateAcls(
            new[] { binding },
            options: null,
            (nativeHandle, resourceTypes, resourceNames, patternTypes, principals, hosts, operations,
                permissionTypes, count, timeoutMs, callback, userData) =>
            {
                sent = new Rows(count, resourceTypes, patternTypes, operations, permissionTypes);
                NativeMethods.AdminClientCreateAclsAsync(
                    nativeHandle, resourceTypes, resourceNames, patternTypes, principals, hosts, operations,
                    permissionTypes, count, timeoutMs, callback, userData);
            });

        Assert.Equal(1, sent.Count);
        Assert.Equal(new[] { 0 }, sent.Codes(column));
        Assert.Equal(binding, Assert.Single(result.Values).Key);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[binding], s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must be armed with the callbacks the core makes");
    }

    /// <summary>
    /// <c>createAcls</c>: two bindings differing only by undefined codes 98 and 99 are one
    /// binding — sent once, one awaitable, and it completes rather than hanging.
    /// </summary>
    /// <param name="column">The ACL enum column carrying the undefined codes.</param>
    [Theory]
    [InlineData(Column.ResourceType)]
    [InlineData(Column.PatternType)]
    [InlineData(Column.Operation)]
    [InlineData(Column.PermissionType)]
    public async Task CreateAcls_TwoBindingsDifferingOnlyInUndefinedCodes_AreOneKey_AndComplete(Column column)
    {
        AclBinding first = Binding(column, 98);
        AclBinding second = Binding(column, 99);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Rows sent = Rows.None;
        CreateAclsResult result = admin.CreateAcls(
            new[] { first, second },
            options: null,
            (nativeHandle, resourceTypes, resourceNames, patternTypes, principals, hosts, operations,
                permissionTypes, count, timeoutMs, callback, userData) =>
            {
                sent = new Rows(count, resourceTypes, patternTypes, operations, permissionTypes);
                NativeMethods.AdminClientCreateAclsAsync(
                    nativeHandle, resourceTypes, resourceNames, patternTypes, principals, hosts, operations,
                    permissionTypes, count, timeoutMs, callback, userData);
            });

        Assert.Equal(1, sent.Count);
        Assert.Equal(new[] { 0 }, sent.Codes(column));
        Assert.Single(result.Values);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[second], s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "two bindings the core answers once must be one key, armed once");
    }

    /// <summary>
    /// <c>deleteAcls</c>: a filter with one undefined code is sent with <c>UNKNOWN</c> (0) in
    /// that column, and the caller's own filter finds the core's answer.
    /// </summary>
    /// <param name="column">The ACL enum column carrying the undefined code.</param>
    [Theory]
    [InlineData(Column.ResourceType)]
    [InlineData(Column.PatternType)]
    [InlineData(Column.Operation)]
    [InlineData(Column.PermissionType)]
    public async Task DeleteAcls_AnUndefinedCode_IsSentAsUnknown_AndResolvesWithTheMocksAnswer(Column column)
    {
        AclBindingFilter filter = Filter(column, 99);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Rows sent = Rows.None;
        DeleteAclsResult result = admin.DeleteAcls(
            new[] { filter },
            options: null,
            (nativeHandle, resourceTypes, resourceNames, patternTypes, principals, hosts, operations,
                permissionTypes, count, timeoutMs, callback, userData) =>
            {
                sent = new Rows(count, resourceTypes, patternTypes, operations, permissionTypes);
                NativeMethods.AdminClientDeleteAclsAsync(
                    nativeHandle, resourceTypes, resourceNames, patternTypes, principals, hosts, operations,
                    permissionTypes, count, timeoutMs, callback, userData);
            });

        Assert.Equal(1, sent.Count);
        Assert.Equal(new[] { 0 }, sent.Codes(column));
        Assert.Equal(filter, Assert.Single(result.Values).Key);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[filter], s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "the countdown must be armed with the callbacks the core makes");
    }

    /// <summary>
    /// <c>deleteAcls</c>: two filters differing only by undefined codes 98 and 99 are one
    /// filter — sent once, one awaitable, and it completes rather than hanging.
    /// </summary>
    /// <param name="column">The ACL enum column carrying the undefined codes.</param>
    [Theory]
    [InlineData(Column.ResourceType)]
    [InlineData(Column.PatternType)]
    [InlineData(Column.Operation)]
    [InlineData(Column.PermissionType)]
    public async Task DeleteAcls_TwoFiltersDifferingOnlyInUndefinedCodes_AreOneKey_AndComplete(Column column)
    {
        AclBindingFilter first = Filter(column, 98);
        AclBindingFilter second = Filter(column, 99);

        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Rows sent = Rows.None;
        DeleteAclsResult result = admin.DeleteAcls(
            new[] { first, second },
            options: null,
            (nativeHandle, resourceTypes, resourceNames, patternTypes, principals, hosts, operations,
                permissionTypes, count, timeoutMs, callback, userData) =>
            {
                sent = new Rows(count, resourceTypes, patternTypes, operations, permissionTypes);
                NativeMethods.AdminClientDeleteAclsAsync(
                    nativeHandle, resourceTypes, resourceNames, patternTypes, principals, hosts, operations,
                    permissionTypes, count, timeoutMs, callback, userData);
            });

        Assert.Equal(1, sent.Count);
        Assert.Equal(new[] { 0 }, sent.Codes(column));
        Assert.Single(result.Values);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[second], s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        Assert.True(
            DisposeAndAwaitRelease(admin, handle),
            "two filters the core answers once must be one key, armed once");
    }

    /// <summary>The four ACL enum columns of a binding or filter row.</summary>
    public enum Column
    {
        /// <summary>The resource pattern's resource type.</summary>
        ResourceType,

        /// <summary>The resource pattern's pattern type.</summary>
        PatternType,

        /// <summary>The entry's operation.</summary>
        Operation,

        /// <summary>The entry's permission type.</summary>
        PermissionType,
    }

    /// <summary>
    /// A binding whose <paramref name="column"/> holds <paramref name="code"/> (undefined) and
    /// whose other columns hold defined members a concrete binding accepts.
    /// </summary>
    private static AclBinding Binding(Column column, int code) =>
        new AclBinding(
            new ResourcePattern(
                column == Column.ResourceType ? (ResourceType)code : ResourceType.Topic,
                "f8-acl",
                column == Column.PatternType ? (PatternType)code : PatternType.Literal),
            new AccessControlEntry(
                "User:alice",
                "*",
                column == Column.Operation ? (AclOperation)code : AclOperation.Read,
                column == Column.PermissionType ? (AclPermissionType)code : AclPermissionType.Allow));

    /// <summary>The filter twin of <see cref="Binding"/>.</summary>
    private static AclBindingFilter Filter(Column column, int code) =>
        new AclBindingFilter(
            new ResourcePatternFilter(
                column == Column.ResourceType ? (ResourceType)code : ResourceType.Topic,
                "f8-acl",
                column == Column.PatternType ? (PatternType)code : PatternType.Literal),
            new AccessControlEntryFilter(
                "User:alice",
                "*",
                column == Column.Operation ? (AclOperation)code : AclOperation.Read,
                column == Column.PermissionType ? (AclPermissionType)code : AclPermissionType.Allow));

    /// <summary>
    /// Disposes the client, then waits — bounded — for the native release: a per-key
    /// trampoline resolves its key before it releases the operation, so an awaiter can reach
    /// <c>Dispose</c> a moment early. A leak never releases, so the bound only decides how
    /// long a red takes to report.
    /// </summary>
    private static bool DisposeAndAwaitRelease(NativeAdminClient admin, SafeAdminHandle handle)
    {
        TestTimeout.Run(admin.Dispose, s_deadline);
        return SpinWait.SpinUntil(() => handle.IsClosed, s_releaseBound);
    }

    /// <summary>The enum columns the submit handed native, copied out during the call.</summary>
    private sealed class Rows
    {
        public static readonly Rows None = new Rows(
            -1, Array.Empty<int>(), Array.Empty<int>(), Array.Empty<int>(), Array.Empty<int>());

        private readonly int[] _resourceTypes;
        private readonly int[] _patternTypes;
        private readonly int[] _operations;
        private readonly int[] _permissionTypes;

        public Rows(int count, int[] resourceTypes, int[] patternTypes, int[] operations, int[] permissionTypes)
        {
            Count = count;
            _resourceTypes = resourceTypes.Take(Math.Max(count, 0)).ToArray();
            _patternTypes = patternTypes.Take(Math.Max(count, 0)).ToArray();
            _operations = operations.Take(Math.Max(count, 0)).ToArray();
            _permissionTypes = permissionTypes.Take(Math.Max(count, 0)).ToArray();
        }

        public int Count { get; }

        public int[] Codes(Column column) =>
            column switch
            {
                Column.ResourceType => _resourceTypes,
                Column.PatternType => _patternTypes,
                Column.Operation => _operations,
                _ => _permissionTypes,
            };
    }
}
