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
/// Pins <see cref="ListGroupsResult"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListGroupsResult</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The three accessors differ only under partial failure, so every test below that
/// matters supplies listings <em>and</em> errors together.</b> With an all-success result
/// <c>All</c> and <c>Valid</c> are indistinguishable and <c>Errors</c> is empty — a suite built
/// only on that shape would pass against an implementation that had aliased all three to the
/// same projection, which is precisely the defect this type exists to avoid
/// (<c>ListGroupsResult.java:52-58</c>).
/// </para>
/// <para>
/// ⚠ <b>The two lists have independent lengths.</b> Java's result is a partition of one mixed
/// collection (<c>:43-49</c>), so the listing count and the error count are unrelated — the
/// native result likewise carries two lists with separate counts. At least one case below uses
/// a shape (one listing, three errors) that a parallel-array reading would misread in both
/// directions.
/// </para>
/// </remarks>
public sealed class PublicAdminListGroupsResultTests
{
    /// <summary>
    /// With no errors, <c>all()</c> yields every listing (<c>:55</c>) — Java's
    /// <c>all.complete(validResult)</c> branch.
    /// </summary>
    [Fact]
    public async Task All_YieldsEveryListing_WhenNoErrorOccurred()
    {
        GroupListing first = Listing("g1");
        GroupListing second = Listing("g2");

        ListGroupsResult result = ResultOf(new[] { first, second }, Array.Empty<KafkaException>());

        Assert.Equal(new[] { first, second }, await result.All());
    }

    /// <summary>
    /// One error and <c>all()</c> fails, yielding nothing — not even the listings that <b>were</b>
    /// fetched (<c>:52-53</c>, javadoc <c>:66-67</c>). The exception is the <b>first</b> one, and
    /// the object itself: Java passes <c>errorsResult.get(0)</c> straight to
    /// <c>completeExceptionally</c>.
    /// </summary>
    [Fact]
    public async Task All_FaultsWithTheFirstError_WhenAnyErrorOccurred()
    {
        KafkaException first = Error("first");
        KafkaException second = Error("second");

        ListGroupsResult result = ResultOf(new[] { Listing("g1") }, new[] { first, second });

        KafkaException thrown = await Assert.ThrowsAsync<KafkaException>(() => result.All());

        Assert.Same(first, thrown);

        // ⚠ The successfully fetched listing is NOT yielded as a consolation: `all()` is
        // all-or-nothing, which is the whole reason `valid()` exists beside it.
        Assert.Single(await result.Valid());
    }

    /// <summary>
    /// <c>valid()</c> ignores errors completely and yields the partial results (javadoc
    /// <c>:76-80</c>) — it does not fault, and it does not shrink to empty because an error is
    /// present.
    /// </summary>
    [Fact]
    public async Task Valid_YieldsThePartialResults_AndIgnoresErrors()
    {
        GroupListing listing = Listing("g1");

        ListGroupsResult result = ResultOf(new[] { listing }, new[] { Error("boom") });

        IReadOnlyCollection<GroupListing> valid = await result.Valid();

        Assert.Equal(new[] { listing }, valid);
    }

    /// <summary>
    /// <c>errors()</c> yields every error and never faults (javadoc <c>:92-93</c>) — an error is
    /// an element here, not a failure. All of them, not just the one <c>all()</c> throws.
    /// </summary>
    [Fact]
    public async Task Errors_YieldsEveryError_AndNeverFaults()
    {
        KafkaException first = Error("first");
        KafkaException second = Error("second");

        ListGroupsResult result = ResultOf(Array.Empty<GroupListing>(), new[] { first, second });

        Assert.Equal(new[] { first, second }, await result.Errors());

        // Java's "if nothing can be fetched, an empty collection is yielded" (:77).
        Assert.Empty(await result.Valid());
    }

    /// <summary>
    /// The listing count and the error count are independent: a result may carry one listing and
    /// three errors. Nothing bounds either list by the other's length.
    /// </summary>
    [Fact]
    public async Task TheTwoLists_HaveIndependentLengths()
    {
        GroupListing listing = Listing("g1");
        KafkaException[] errors = { Error("a"), Error("b"), Error("c") };

        ListGroupsResult result = ResultOf(new[] { listing }, errors);

        Assert.Equal(new[] { listing }, await result.Valid());
        Assert.Equal(errors, await result.Errors());

        // The reverse shape: more listings than errors, and the single error still governs all().
        GroupListing[] listings = { Listing("g1"), Listing("g2"), Listing("g3") };
        ListGroupsResult reversed = ResultOf(listings, new[] { Error("only") });

        Assert.Equal(listings, await reversed.Valid());
        Assert.Single(await reversed.Errors());
        await Assert.ThrowsAsync<KafkaException>(() => reversed.All());
    }

    /// <summary>
    /// The empty result — no groups and no errors — succeeds on all three accessors rather than
    /// faulting: with no error present, Java takes the <c>all.complete(validResult)</c> branch
    /// (<c>:55</c>) whatever the listing count.
    /// </summary>
    [Fact]
    public async Task Empty_SucceedsOnAllThreeAccessors()
    {
        ListGroupsResult result = ResultOf(Array.Empty<GroupListing>(), Array.Empty<KafkaException>());

        Assert.Empty(await result.All());
        Assert.Empty(await result.Valid());
        Assert.Empty(await result.Errors());
    }

    /// <summary>
    /// Each accessor re-reads the one shared source, so the three stay consistent however often
    /// they are called — Java's three futures are all completed from a single <c>thenApply</c>
    /// (<c>:40-60</c>).
    /// </summary>
    [Fact]
    public async Task EveryAccessor_ReadsTheOneSharedSource()
    {
        GroupListing listing = Listing("g1");
        KafkaException error = Error("boom");

        ListGroupsResult result = ResultOf(new[] { listing }, new[] { error });

        Assert.Same(await result.Valid(), await result.Valid());
        Assert.Same(await result.Errors(), await result.Errors());
        Assert.Same(error, await Assert.ThrowsAsync<KafkaException>(() => result.All()));
        Assert.Same(error, await Assert.ThrowsAsync<KafkaException>(() => result.All()));
    }

    /// <summary>
    /// The public shape is Java's three <b>methods</b>, each yielding a task — not properties,
    /// because each starts an <c>await</c> of the shared source rather than reading a field. The
    /// shipped <see cref="ListTopicsResult.NamesToListings"/> and
    /// <see cref="ListConfigResourcesResult.All"/> make the same call.
    /// </summary>
    [Fact]
    public void PublicShape_IsJavasThreeAccessors_AsMethods()
    {
        MethodInfo all = typeof(ListGroupsResult).GetMethod(
            nameof(ListGroupsResult.All), Type.EmptyTypes)!;
        MethodInfo valid = typeof(ListGroupsResult).GetMethod(
            nameof(ListGroupsResult.Valid), Type.EmptyTypes)!;
        MethodInfo errors = typeof(ListGroupsResult).GetMethod(
            nameof(ListGroupsResult.Errors), Type.EmptyTypes)!;

        Assert.Equal(typeof(Task<IReadOnlyCollection<GroupListing>>), all.ReturnType);
        Assert.Equal(typeof(Task<IReadOnlyCollection<GroupListing>>), valid.ReturnType);

        // ⚠ Java's Collection<Throwable> narrows to this binding's one flat KafkaException —
        // every per-group error crossing the ABI is a native error handle.
        Assert.Equal(typeof(Task<IReadOnlyCollection<KafkaException>>), errors.ReturnType);

        // Non-null: each is completed from the source's value (:55-58), never with null.
        Assert.Equal(1, NullableFlag(all.ReturnParameter));
        Assert.Equal(1, NullableFlag(valid.ReturnParameter));
        Assert.Equal(1, NullableFlag(errors.ReturnParameter));

        // No accessor is a property, and none was invented beyond Java's three.
        Assert.Empty(typeof(ListGroupsResult).GetProperties());
        Assert.Equal(
            new[] { "All", "Errors", "Valid" },
            typeof(ListGroupsResult)
                .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        // Java's constructor is package-private; nothing public may build one.
        Assert.Empty(typeof(ListGroupsResult).GetConstructors());
    }

    private static GroupListing Listing(string groupId) =>
        new GroupListing(groupId, GroupType.Consumer, "consumer", GroupState.Stable);

    private static KafkaException Error(string message) => new KafkaException(message);

    private static ListGroupsResult ResultOf(
        IReadOnlyCollection<GroupListing> valid,
        IReadOnlyCollection<KafkaException> errors) =>
        new ListGroupsResult(Task.FromResult((valid, errors)));

    private static byte NullableFlag(ParameterInfo parameter) => NullableAnnotation.Flag(parameter);
}
