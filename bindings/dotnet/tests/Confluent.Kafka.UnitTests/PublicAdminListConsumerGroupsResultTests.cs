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

#pragma warning disable CS0618 // Java deprecates this result type and its listing type; exercising them is the point.

/// <summary>
/// Pins <see cref="ListConsumerGroupsResult"/> against Java's
/// <c>org.apache.kafka.clients.admin.ListConsumerGroupsResult</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The three accessors differ only under partial failure, so every test below that
/// matters supplies listings <em>and</em> errors together.</b> With an all-success result
/// <c>All</c> and <c>Valid</c> are indistinguishable and <c>Errors</c> is empty — a suite built
/// only on that shape would pass against an implementation that had aliased all three to the
/// same projection, which is precisely the defect this type exists to avoid
/// (<c>ListConsumerGroupsResult.java:51-57</c>).
/// </para>
/// <para>
/// ⚠ <b>The two lists have independent lengths.</b> Java's result is a partition of one mixed
/// collection (<c>:44-49</c>), so the listing count and the error count are unrelated — the
/// native result likewise carries two lists with separate counts. At least one case below uses
/// a shape (one listing, three errors) that a parallel-array reading would misread in both
/// directions.
/// </para>
/// <para>
/// ⚠ <b>The whole type is deprecated in Java</b> (<c>:30</c>), as is the
/// <see cref="ConsumerGroupListing"/> it yields — hence the file-wide
/// <c>CS0618</c> suppression above, and the test that pins the deprecation is mirrored rather
/// than quietly dropped.
/// </para>
/// <para>
/// <b>This is a pure value type</b> — the ABI wiring and the client method arrive in later
/// slices — so everything here is managed and broker-free.
/// </para>
/// </remarks>
public sealed class PublicAdminListConsumerGroupsResultTests
{
    /// <summary>
    /// With no errors, <c>all()</c> yields every listing (<c>:54</c>) — Java's
    /// <c>all.complete(curValid)</c> branch.
    /// </summary>
    [Fact]
    public async Task All_YieldsEveryListing_WhenNoErrorOccurred()
    {
        ConsumerGroupListing first = Listing("g1");
        ConsumerGroupListing second = Listing("g2");

        ListConsumerGroupsResult result =
            ResultOf(new[] { first, second }, Array.Empty<KafkaException>());

        Assert.Equal(new[] { first, second }, await result.All());
    }

    /// <summary>
    /// One error and <c>all()</c> fails, yielding nothing — not even the listings that <b>were</b>
    /// fetched (<c>:51-52</c>, javadoc <c>:66-67</c>). The exception is the <b>first</b> one, and
    /// the object itself: Java passes <c>curErrors.get(0)</c> straight to
    /// <c>completeExceptionally</c>.
    /// </summary>
    [Fact]
    public async Task All_FaultsWithTheFirstError_WhenAnyErrorOccurred()
    {
        KafkaException first = Error("first");
        KafkaException second = Error("second");

        ListConsumerGroupsResult result = ResultOf(new[] { Listing("g1") }, new[] { first, second });

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
        ConsumerGroupListing listing = Listing("g1");

        ListConsumerGroupsResult result = ResultOf(new[] { listing }, new[] { Error("boom") });

        IReadOnlyCollection<ConsumerGroupListing> valid = await result.Valid();

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

        ListConsumerGroupsResult result =
            ResultOf(Array.Empty<ConsumerGroupListing>(), new[] { first, second });

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
        ConsumerGroupListing listing = Listing("g1");
        KafkaException[] errors = { Error("a"), Error("b"), Error("c") };

        ListConsumerGroupsResult result = ResultOf(new[] { listing }, errors);

        Assert.Equal(new[] { listing }, await result.Valid());
        Assert.Equal(errors, await result.Errors());

        // The reverse shape: more listings than errors, and the single error still governs all().
        ConsumerGroupListing[] listings = { Listing("g1"), Listing("g2"), Listing("g3") };
        ListConsumerGroupsResult reversed = ResultOf(listings, new[] { Error("only") });

        Assert.Equal(listings, await reversed.Valid());
        Assert.Single(await reversed.Errors());
        await Assert.ThrowsAsync<KafkaException>(() => reversed.All());
    }

    /// <summary>
    /// The empty result — no groups and no errors — succeeds on all three accessors rather than
    /// faulting: with no error present, Java takes the <c>all.complete(curValid)</c> branch
    /// (<c>:54</c>) whatever the listing count.
    /// </summary>
    [Fact]
    public async Task Empty_SucceedsOnAllThreeAccessors()
    {
        ListConsumerGroupsResult result =
            ResultOf(Array.Empty<ConsumerGroupListing>(), Array.Empty<KafkaException>());

        Assert.Empty(await result.All());
        Assert.Empty(await result.Valid());
        Assert.Empty(await result.Errors());
    }

    /// <summary>
    /// Each accessor re-reads the one shared source, so the three stay consistent however often
    /// they are called — Java's three futures are all completed from a single <c>thenApply</c>
    /// (<c>:41-59</c>).
    /// </summary>
    [Fact]
    public async Task EveryAccessor_ReadsTheOneSharedSource()
    {
        ConsumerGroupListing listing = Listing("g1");
        KafkaException error = Error("boom");

        ListConsumerGroupsResult result = ResultOf(new[] { listing }, new[] { error });

        Assert.Same(await result.Valid(), await result.Valid());
        Assert.Same(await result.Errors(), await result.Errors());
        Assert.Same(error, await Assert.ThrowsAsync<KafkaException>(() => result.All()));
        Assert.Same(error, await Assert.ThrowsAsync<KafkaException>(() => result.All()));
    }

    /// <summary>
    /// Java's <c>@Deprecated(since = "4.1")</c> on the class (<c>:30</c>) is mirrored as
    /// <see cref="ObsoleteAttribute"/> — at <b>warning</b> severity, so the surface stays
    /// callable as it is in Java.
    /// </summary>
    /// <remarks>
    /// Java deprecates only the class: the three accessors carry no annotation of their own
    /// (<c>:69</c>, <c>:82</c>, <c>:95</c>), and neither does the successor
    /// <see cref="ListGroupsResult"/> — both asserted, so a later slice cannot spread the
    /// deprecation past where Java puts it.
    /// </remarks>
    [Fact]
    public void TheDeprecation_IsMirroredAsAWarning()
    {
        ObsoleteAttribute onClass = typeof(ListConsumerGroupsResult)
            .GetCustomAttribute<ObsoleteAttribute>()!;
        Assert.NotNull(onClass);
        Assert.False(onClass.IsError);

        foreach (MethodInfo accessor in Accessors())
        {
            Assert.Null(accessor.GetCustomAttribute<ObsoleteAttribute>());
        }

        Assert.Null(typeof(ListGroupsResult).GetCustomAttribute<ObsoleteAttribute>());
    }

    /// <summary>
    /// The public shape is Java's three <b>methods</b>, each yielding a task — not properties,
    /// because each starts an <c>await</c> of the shared source rather than reading a field. The
    /// shipped <see cref="ListGroupsResult.All"/> and
    /// <see cref="ListTopicsResult.NamesToListings"/> make the same call.
    /// </summary>
    [Fact]
    public void PublicShape_IsJavasThreeAccessors_AsMethods()
    {
        MethodInfo all = typeof(ListConsumerGroupsResult).GetMethod(
            nameof(ListConsumerGroupsResult.All), Type.EmptyTypes)!;
        MethodInfo valid = typeof(ListConsumerGroupsResult).GetMethod(
            nameof(ListConsumerGroupsResult.Valid), Type.EmptyTypes)!;
        MethodInfo errors = typeof(ListConsumerGroupsResult).GetMethod(
            nameof(ListConsumerGroupsResult.Errors), Type.EmptyTypes)!;

        Assert.Equal(typeof(Task<IReadOnlyCollection<ConsumerGroupListing>>), all.ReturnType);
        Assert.Equal(typeof(Task<IReadOnlyCollection<ConsumerGroupListing>>), valid.ReturnType);

        // ⚠ Java's Collection<Throwable> narrows to this binding's one flat KafkaException —
        // every per-group error crossing the ABI is a native error handle.
        Assert.Equal(typeof(Task<IReadOnlyCollection<KafkaException>>), errors.ReturnType);

        // Non-null: each is completed from the source's value (:54-57), never with null.
        Assert.Equal(1, NullableFlag(all.ReturnParameter));
        Assert.Equal(1, NullableFlag(valid.ReturnParameter));
        Assert.Equal(1, NullableFlag(errors.ReturnParameter));

        // No accessor is a property, and none was invented beyond Java's three.
        Assert.Empty(typeof(ListConsumerGroupsResult).GetProperties());
        Assert.Equal(
            new[] { "All", "Errors", "Valid" },
            Accessors()
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));

        // Java's constructor is package-private; nothing public may build one.
        Assert.Empty(typeof(ListConsumerGroupsResult).GetConstructors());
    }

    private static MethodInfo[] Accessors() =>
        typeof(ListConsumerGroupsResult)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);

    private static ConsumerGroupListing Listing(string groupId) =>
        new ConsumerGroupListing(groupId, GroupState.Stable, GroupType.Consumer, false);

    private static KafkaException Error(string message) => new KafkaException(message);

    private static ListConsumerGroupsResult ResultOf(
        IReadOnlyCollection<ConsumerGroupListing> valid,
        IReadOnlyCollection<KafkaException> errors) =>
        new ListConsumerGroupsResult(Task.FromResult((valid, errors)));

    private static byte NullableFlag(ParameterInfo parameter) => NullableAnnotation.Flag(parameter);
}

#pragma warning restore CS0618
