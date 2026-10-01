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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P9 CP5 — <c>deleteAcls</c>, the first and only RPC whose per-key <b>key</b> is an
/// owned handle (shape 4a): the callback receives a
/// <c>kafka_common_AclBindingFilter_t*</c> it must destroy, alongside the owned value and
/// owned error every other per-key RPC already had.
/// </summary>
/// <remarks>
/// <para>
/// Nothing managed can observe the key destroy, so what these tests pin is the copy-out that
/// has to precede it: a task resolves only when
/// <see cref="AdminCallbacks.DeleteAclsPerKeyKey"/> reconstructs a filter <em>equal</em> to
/// the requested one, so a key read after the destroy, or read wrongly, shows up as a key
/// that never settles rather than as a passing loop.
/// </para>
/// <para>
/// ⚠ The core minting that key handle is also what makes
/// <see cref="AclRowMarshal.ReadFilter(IntPtr)"/>'s null-means-match-any rule reachable for
/// the first time — the ABI exposes no filter constructor, which is why that reader's own
/// remarks call it otherwise unassertable.
/// </para>
/// </remarks>
public sealed class AdminP9PerKeyStage4Tests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code the mock reports for every unimplemented RPC.</summary>
    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// Three filters differing only in resource name each fault with Java's own message, which
    /// is what proves the owned key was copied out and matched before it was destroyed.
    /// </summary>
    [Fact]
    public async Task DeleteAcls_EveryFilterFaultsIndependently_WithJavasOwnMessage()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        AclBindingFilter[] filters =
        {
            Filter("cp5-alpha"),
            Filter("cp5-beta"),
            Filter("cp5-gamma"),
        };

        DeleteAclsResult result = admin.DeleteAcls(filters, options: null);

        Assert.Equal(filters.Length, result.Values.Count);
        foreach (AclBindingFilter filter in filters)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[filter], s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>
    /// ⚠ <b>The null-means-match-any rule, over a real core-minted key.</b> An all-ANY filter
    /// carries a null resource name, principal and host; a key reader that turned those into
    /// <c>""</c> would build an unequal <see cref="AclBindingFilter"/>, the lookup would miss,
    /// and this key would never settle.
    /// </summary>
    [Fact]
    public async Task DeleteAcls_AnAllAnyFilter_RoundTripsItsNullsAsNulls()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        AclBindingFilter any = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Any, null, PatternType.Any),
            new AccessControlEntryFilter(null, null, AclOperation.Any, AclPermissionType.Any));

        // Paired with a fully-specified filter, so "every key settled" cannot be satisfied by
        // a reader that collapses both onto one entry.
        AclBindingFilter named = Filter("cp5-null-vs-empty");

        DeleteAclsResult result = admin.DeleteAcls(new[] { any, named }, options: null);

        // ⚠ The mock's OWN code/message, asserted per key. A key the reader reconstructed
        // wrongly is not left pending — the operation faults it with a synthesized
        // "not present in the response" error instead — so "it threw" alone proves nothing.
        foreach (AclBindingFilter filter in new[] { any, named })
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[filter], s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>
    /// The <c>n == 0</c> rule: an empty filter list fires <b>no</b> callback, so only the
    /// submit's own countdown slot can release the operation.
    /// </summary>
    [Fact]
    public void DeleteAcls_AnEmptyFilterList_ResolvesAtTheSubmitBoundary_AndReleasesTheClient()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Assert.Empty(admin.DeleteAcls(Array.Empty<AclBindingFilter>(), options: null).Values);

        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.True(
            handle.IsClosed,
            "zero callbacks must still release: the submit token is the only thing that can");
    }

    /// <summary>
    /// Many owned keys per operation, over many operations, leave the reference count
    /// <b>balanced</b> — an over-release would throw out of the
    /// <see cref="System.Runtime.InteropServices.SafeHandle"/>, an under-release leaves
    /// <c>IsClosed</c> false forever.
    /// </summary>
    [Fact]
    public async Task ManyFiltersOverManyOperations_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        AclBindingFilter[] filters = new AclBindingFilter[10];
        for (int i = 0; i < filters.Length; i++)
        {
            filters[i] = Filter($"cp5-balance-{i}");
        }

        for (int round = 0; round < 3; round++)
        {
            DeleteAclsResult result = admin.DeleteAcls(filters, options: null);
            Assert.Equal(filters.Length, result.Values.Count);

            foreach (KeyValuePair<AclBindingFilter, Task<DeleteAclsResult.FilterResults>> entry
                in result.Values)
            {
                await TestTimeout.Run(() => Settle(entry.Value), s_deadline);
            }

            Assert.False(handle.IsClosed, "the client is still alive between operations");
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "30 owned-key callbacks must leave the count balanced");
    }

    /// <summary>Awaits one key's outcome, treating a fault as a settlement.</summary>
    private static async Task Settle(Task awaitable)
    {
        try
        {
            await awaitable.ConfigureAwait(false);
        }
        catch (KafkaException)
        {
        }
    }

    /// <summary>A filter differing only in resource name, with ANY everywhere else.</summary>
    private static AclBindingFilter Filter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Any),
            new AccessControlEntryFilter(
                "User:alice", "*", AclOperation.Any, AclPermissionType.Any));
}
