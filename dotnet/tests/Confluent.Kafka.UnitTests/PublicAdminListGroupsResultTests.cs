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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins <see cref="ListGroupsResult"/> and <see cref="AsyncListGroupsResult"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListGroupsResult</c>: three accessors, each over its own core future.
/// </summary>
public sealed class PublicAdminListGroupsResultTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void EachAccessor_YieldsItsOwnFuture_Sync()
    {
        (IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> all,
            IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> valid,
            IKafkaFutureImpl<IReadOnlyCollection<Exception>> errors,
            IReadOnlyCollection<GroupListing> listings,
            IReadOnlyCollection<Exception> failures) = PartialFailure(AdminFlavour.Sync);

        ListGroupsResult result = ListGroupsResult.OfClient((all, valid, errors, NoResult()));

        Assert.Same(failures.First(), FutureAssert.Throws<KafkaException>(() => result.All().Get(s_deadline)));
        Assert.Same(listings, result.Valid().Get(s_deadline));
        Assert.Same(failures, result.Errors().Get(s_deadline));
    }

    [Fact]
    public async Task EachAccessor_YieldsItsOwnFuture_Async()
    {
        (IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> all,
            IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> valid,
            IKafkaFutureImpl<IReadOnlyCollection<Exception>> errors,
            IReadOnlyCollection<GroupListing> listings,
            IReadOnlyCollection<Exception> failures) = PartialFailure(AdminFlavour.Async);

        AsyncListGroupsResult result = AsyncListGroupsResult.OfClient((all, valid, errors, NoResult()));

        Assert.Same(failures.First(), await FutureAssert.ThrowsAsync<KafkaException>(() => result.All().Get()));
        Assert.Same(listings, await result.Valid().Get());
        Assert.Same(failures, await result.Errors().Get());
    }

    [Fact]
    public async Task Empty_SucceedsOnAllThreeAccessors()
    {
        using (MockAdminClient admin = MockAdminClient.Create().Build())
        {
            ListGroupsResult result = admin.ListGroups();
            Assert.Empty(result.All().Get(s_deadline));
            Assert.Empty(result.Valid().Get(s_deadline));
            Assert.Empty(result.Errors().Get(s_deadline));
        }

        await using AsyncMockAdminClient asyncAdmin = AsyncMockAdminClient.Create().Build();
        AsyncListGroupsResult asyncResult = asyncAdmin.ListGroups();
        Assert.Empty(await asyncResult.All().Get());
        Assert.Empty(await asyncResult.Valid().Get());
        Assert.Empty(await asyncResult.Errors().Get());
    }

    [Fact]
    public void PublicShape_IsJavasThreeAccessors_AsMethods()
    {
        MethodInfo all = typeof(ListGroupsResult).GetMethod(
            nameof(ListGroupsResult.All), Type.EmptyTypes)!;
        MethodInfo valid = typeof(ListGroupsResult).GetMethod(
            nameof(ListGroupsResult.Valid), Type.EmptyTypes)!;
        MethodInfo errors = typeof(ListGroupsResult).GetMethod(
            nameof(ListGroupsResult.Errors), Type.EmptyTypes)!;

        Assert.Equal(typeof(KafkaFuture<IReadOnlyCollection<GroupListing>>), all.ReturnType);
        Assert.Equal(typeof(KafkaFuture<IReadOnlyCollection<GroupListing>>), valid.ReturnType);

        // Java's Collection<Throwable>.
        Assert.Equal(typeof(KafkaFuture<IReadOnlyCollection<Exception>>), errors.ReturnType);

        // Nullability positions: 0 = the KafkaFuture, 1 = the collection, 2 = its element.
        foreach (MethodInfo accessor in new[] { all, valid, errors })
        {
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableFlag(accessor.ReturnParameter, 0));
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableFlag(accessor.ReturnParameter, 1));
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableFlag(accessor.ReturnParameter, 2));
        }

        Assert.Empty(typeof(ListGroupsResult).GetProperties());
        Assert.Equal(
            new[] { "All", "Errors", "Valid" },
            typeof(ListGroupsResult)
                .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        Assert.Empty(typeof(ListGroupsResult).GetConstructors());
    }

    private static GroupListing Listing(string groupId) =>
        new GroupListing(groupId, GroupType.CONSUMER, "consumer", GroupState.STABLE);

    private static KafkaException Error(string message) => new KafkaException(message);

    private static SafeAdminResultHandle NoResult() => new SafeAdminResultHandle(IntPtr.Zero, NoDestroy);

    private static void NoDestroy(IntPtr result, ref IntPtr outErr)
    {
    }

    private static (
        IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> All,
        IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> Valid,
        IKafkaFutureImpl<IReadOnlyCollection<Exception>> Errors,
        IReadOnlyCollection<GroupListing> Listings,
        IReadOnlyCollection<Exception> Failures) PartialFailure(AdminFlavour flavour)
    {
        IReadOnlyCollection<GroupListing> listings = new[] { Listing("g1") };
        IReadOnlyCollection<Exception> failures = new[] { Error("first"), Error("second"), Error("third") };
        IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> all = NativeAdminClient.NewFuture<IReadOnlyCollection<GroupListing>>(flavour);
        IKafkaFutureImpl<IReadOnlyCollection<GroupListing>> valid = NativeAdminClient.NewFuture<IReadOnlyCollection<GroupListing>>(flavour);
        IKafkaFutureImpl<IReadOnlyCollection<Exception>> errors = NativeAdminClient.NewFuture<IReadOnlyCollection<Exception>>(flavour);
        all.CompleteWithError(failures.First());
        valid.Complete(listings);
        errors.Complete(failures);
        return (all, valid, errors, listings, failures);
    }

    private static byte NullableFlag(ParameterInfo parameter, int index) => NullableAnnotation.Flag(parameter, index);
}
