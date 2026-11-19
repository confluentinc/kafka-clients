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
using System.Runtime.CompilerServices;

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The twelve producer and transaction value and options types against their Java counterparts.
/// </summary>
/// <remarks>
/// Every value assertion uses a distinct value per field, so a transposition between two same-typed fields fails.
/// </remarks>
public sealed class PublicAdminP8TypesTests
{
    [Fact]
    public void TransactionState_HasJavasEightConstantsInDeclarationOrder()
    {
        Assert.Equal(
            new[]
            {
                "ONGOING",
                "PREPARE_ABORT",
                "PREPARE_COMMIT",
                "COMPLETE_ABORT",
                "COMPLETE_COMMIT",
                "EMPTY",
                "PREPARE_EPOCH_FENCE",
                "UNKNOWN",
            },
            Enum.GetNames(typeof(TransactionState)));
    }

    [Fact]
    public void TransactionState_HasNoDeadMember()
    {
        // The broker-side TransactionState has a ninth DEAD constant with no client counterpart; the client enum is the
        // contract.
        Assert.DoesNotContain("DEAD", Enum.GetNames(typeof(TransactionState)));
    }

    [Fact]
    public void ProducerState_ExposesEveryFieldWithJavasWidths()
    {
        ProducerState state = new ProducerState(11L, 22, 33, 44L, 55, 66L);

        Assert.Equal(11L, state.ProducerId());
        Assert.Equal(22, state.ProducerEpoch());
        Assert.Equal(33, state.LastSequence());
        Assert.Equal(44L, state.LastTimestamp());
        Assert.Equal(55, state.CoordinatorEpoch());
        Assert.Equal(66L, state.CurrentTransactionStartOffset());

        Assert.Equal(typeof(long), ReturnType(typeof(ProducerState), nameof(ProducerState.ProducerId)));
        Assert.Equal(typeof(int), ReturnType(typeof(ProducerState), nameof(ProducerState.ProducerEpoch)));
        Assert.Equal(typeof(int), ReturnType(typeof(ProducerState), nameof(ProducerState.LastSequence)));
        Assert.Equal(typeof(long), ReturnType(typeof(ProducerState), nameof(ProducerState.LastTimestamp)));
        Assert.Equal(typeof(int?), ReturnType(typeof(ProducerState), nameof(ProducerState.CoordinatorEpoch)));
        Assert.Equal(
            typeof(long?),
            ReturnType(typeof(ProducerState), nameof(ProducerState.CurrentTransactionStartOffset)));
    }

    [Fact]
    public void ProducerState_AbsentOptionalsAreNullNotZeroOrMinusOne()
    {
        ProducerState state = new ProducerState(1L, 2, 3, 4L, null, null);

        Assert.Null(state.CoordinatorEpoch());
        Assert.Null(state.CurrentTransactionStartOffset());
        Assert.False(state.CoordinatorEpoch().HasValue);
        Assert.False(state.CurrentTransactionStartOffset().HasValue);
    }

    [Fact]
    public void ProducerState_MinusOneIsAValueNotAnAbsence()
    {
        // The scalar siblings use -1 as their out-of-range sentinel; the optionals do not.
        ProducerState state = new ProducerState(-1L, -1, -1, -1L, -1, -1L);

        Assert.Equal(-1, state.CoordinatorEpoch());
        Assert.Equal(-1L, state.CurrentTransactionStartOffset());
        Assert.NotEqual(new ProducerState(-1L, -1, -1, -1L, null, null), state);
    }

    [Fact]
    public void ProducerState_EqualityCoversAllSixFields()
    {
        ProducerState baseline = new ProducerState(11L, 22, 33, 44L, 55, 66L);

        Assert.Equal(baseline, new ProducerState(11L, 22, 33, 44L, 55, 66L));
        Assert.Equal(baseline.GetHashCode(), new ProducerState(11L, 22, 33, 44L, 55, 66L).GetHashCode());

        Assert.NotEqual(baseline, new ProducerState(99L, 22, 33, 44L, 55, 66L));
        Assert.NotEqual(baseline, new ProducerState(11L, 99, 33, 44L, 55, 66L));
        Assert.NotEqual(baseline, new ProducerState(11L, 22, 99, 44L, 55, 66L));
        Assert.NotEqual(baseline, new ProducerState(11L, 22, 33, 99L, 55, 66L));
        Assert.NotEqual(baseline, new ProducerState(11L, 22, 33, 44L, 99, 66L));
        Assert.NotEqual(baseline, new ProducerState(11L, 22, 33, 44L, 55, 99L));
        Assert.NotEqual(baseline, new ProducerState(11L, 22, 33, 44L, null, 66L));
        Assert.NotEqual(baseline, new ProducerState(11L, 22, 33, 44L, 55, null));
        Assert.False(baseline.Equals(null));
    }

    [Fact]
    public void G7_8_ProducerState_ToStringNamesEveryFieldAndRendersOptionalsAsJavaDoes()
    {
        Assert.Equal(
            "ProducerState(producerId=11, producerEpoch=22, lastSequence=33, lastTimestamp=44"
                + ", coordinatorEpoch=OptionalInt[55], currentTransactionStartOffset=OptionalLong[66])",
            new ProducerState(11L, 22, 33, 44L, 55, 66L).ToString());

        Assert.Equal(
            "ProducerState(producerId=11, producerEpoch=22, lastSequence=33, lastTimestamp=44"
                + ", coordinatorEpoch=OptionalInt.empty, currentTransactionStartOffset=OptionalLong.empty)",
            new ProducerState(11L, 22, 33, 44L, null, null).ToString());
    }

    [Fact]
    public void TransactionListing_ExposesJavasThreeAccessors()
    {
        TransactionListing listing = new TransactionListing("txn-1", 77L, TransactionState.PREPARE_COMMIT);

        Assert.Equal("txn-1", listing.TransactionalId());
        Assert.Equal(77L, listing.ProducerId());
        Assert.Equal(TransactionState.PREPARE_COMMIT, listing.State());

        // Java names the accessor state() although the field is transactionState.
        Assert.Contains("State", DeclaredPublicMethodNames(typeof(TransactionListing)));
        Assert.DoesNotContain("TransactionState", DeclaredPublicMethodNames(typeof(TransactionListing)));
    }

    [Fact]
    public void TransactionListing_RejectsANullTransactionalId()
    {
        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => new TransactionListing(null!, 1L, TransactionState.EMPTY));

        Assert.Equal("transactionalId", error.ParamName);
    }

    [Fact]
    public void TransactionListing_EqualityCoversAllThreeFields()
    {
        TransactionListing baseline = new TransactionListing("txn-1", 77L, TransactionState.ONGOING);

        Assert.Equal(baseline, new TransactionListing("txn-1", 77L, TransactionState.ONGOING));
        Assert.Equal(
            baseline.GetHashCode(),
            new TransactionListing("txn-1", 77L, TransactionState.ONGOING).GetHashCode());

        Assert.NotEqual(baseline, new TransactionListing("txn-2", 77L, TransactionState.ONGOING));
        Assert.NotEqual(baseline, new TransactionListing("txn-1", 78L, TransactionState.ONGOING));
        Assert.NotEqual(baseline, new TransactionListing("txn-1", 77L, TransactionState.EMPTY));
    }

    [Fact]
    public void TransactionListing_ToStringMatchesJavasShape() =>
        Assert.Equal(
            "TransactionListing(transactionalId='txn-1', producerId=77, transactionState=Ongoing)",
            new TransactionListing("txn-1", 77L, TransactionState.ONGOING).ToString());

    [Fact]
    public void TransactionDescription_ExposesEveryFieldWithJavasWidths()
    {
        TransactionDescription description = new TransactionDescription(
            coordinatorId: 3,
            state: TransactionState.COMPLETE_ABORT,
            producerId: 44L,
            producerEpoch: 5,
            transactionTimeoutMs: 60_000L,
            transactionStartTimeMs: 1_700_000_000_000L,
            topicPartitions: new[] { new TopicPartition("orders", 7) });

        Assert.Equal(3, description.CoordinatorId());
        Assert.Equal(TransactionState.COMPLETE_ABORT, description.State());
        Assert.Equal(44L, description.ProducerId());
        Assert.Equal(5, description.ProducerEpoch());
        Assert.Equal(60_000L, description.TransactionTimeoutMs());
        Assert.Equal(1_700_000_000_000L, description.TransactionStartTimeMs());
        Assert.Equal(new[] { new TopicPartition("orders", 7) }, description.TopicPartitions());

        // transactionTimeoutMs is a long in Java although the wire field is an int, and producerEpoch is an int here but a
        // short on AbortTransactionSpec.
        Assert.Equal(
            typeof(long),
            ReturnType(typeof(TransactionDescription), nameof(TransactionDescription.TransactionTimeoutMs)));
        Assert.Equal(
            typeof(int),
            ReturnType(typeof(TransactionDescription), nameof(TransactionDescription.ProducerEpoch)));
        Assert.Equal(
            typeof(long?),
            ReturnType(typeof(TransactionDescription), nameof(TransactionDescription.TransactionStartTimeMs)));
    }

    [Fact]
    public void TransactionDescription_AbsentStartTimeIsNull() =>
        Assert.Null(
            new TransactionDescription(1, TransactionState.EMPTY, 2L, 3, 4L, null, null)
                .TransactionStartTimeMs());

    [Fact]
    public void TransactionDescription_NullPartitionsBecomeAnEmptyCollection() =>
        Assert.Empty(
            new TransactionDescription(1, TransactionState.EMPTY, 2L, 3, 4L, null, null).TopicPartitions());

    [Fact]
    public void TransactionDescription_PartitionsAreDeduplicatedAndSetCompared()
    {
        TransactionDescription duplicated = new TransactionDescription(
            1,
            TransactionState.ONGOING,
            2L,
            3,
            4L,
            5L,
            new[] { new TopicPartition("t", 0), new TopicPartition("t", 0), new TopicPartition("t", 1) });

        Assert.Equal(2, duplicated.TopicPartitions().Count);

        TransactionDescription reordered = new TransactionDescription(
            1,
            TransactionState.ONGOING,
            2L,
            3,
            4L,
            5L,
            new[] { new TopicPartition("t", 1), new TopicPartition("t", 0) });

        Assert.Equal(duplicated, reordered);
        Assert.Equal(duplicated.GetHashCode(), reordered.GetHashCode());
    }

    [Fact]
    public void TransactionDescription_EqualityCoversAllSevenFields()
    {
        TopicPartition[] partitions = new[] { new TopicPartition("t", 0) };
        TransactionDescription baseline = new TransactionDescription(
            1, TransactionState.ONGOING, 2L, 3, 4L, 5L, partitions);

        Assert.Equal(
            baseline,
            new TransactionDescription(1, TransactionState.ONGOING, 2L, 3, 4L, 5L, partitions));

        Assert.NotEqual(
            baseline,
            new TransactionDescription(9, TransactionState.ONGOING, 2L, 3, 4L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.EMPTY, 2L, 3, 4L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.ONGOING, 9L, 3, 4L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.ONGOING, 2L, 9, 4L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.ONGOING, 2L, 3, 9L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.ONGOING, 2L, 3, 4L, 9L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.ONGOING, 2L, 3, 4L, null, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(
                1, TransactionState.ONGOING, 2L, 3, 4L, 5L, new[] { new TopicPartition("t", 9) }));
    }

    [Fact]
    public void G7_8_TransactionDescription_ToStringNamesEveryFieldAndRendersOptionalsAsJavaDoes()
    {
        Assert.Equal(
            "TransactionDescription(coordinatorId=1, state=Ongoing, producerId=2, producerEpoch=3"
                + ", transactionTimeoutMs=4, transactionStartTimeMs=OptionalLong[5], topicPartitions=[t-0])",
            new TransactionDescription(
                1, TransactionState.ONGOING, 2L, 3, 4L, 5L, new[] { new TopicPartition("t", 0) })
                .ToString());

        Assert.Contains(
            "transactionStartTimeMs=OptionalLong.empty",
            new TransactionDescription(1, TransactionState.ONGOING, 2L, 3, 4L, null, null).ToString());
    }

    [Fact]
    public void AbortTransactionSpec_ProducerEpochIsAShortUnlikeItsSiblings()
    {
        Assert.Equal(
            typeof(short),
            ReturnType(typeof(AbortTransactionSpec), nameof(AbortTransactionSpec.ProducerEpoch)));
        Assert.Equal(typeof(int), ReturnType(typeof(ProducerState), nameof(ProducerState.ProducerEpoch)));
        Assert.Equal(
            typeof(int),
            ReturnType(typeof(TransactionDescription), nameof(TransactionDescription.ProducerEpoch)));
    }

    [Fact]
    public void AbortTransactionSpec_ExposesJavasFourAccessors()
    {
        AbortTransactionSpec spec = new AbortTransactionSpec(new TopicPartition("orders", 4), 11L, 22, 33);

        Assert.Equal(new TopicPartition("orders", 4), spec.TopicPartition());
        Assert.Equal(11L, spec.ProducerId());
        Assert.Equal((short)22, spec.ProducerEpoch());
        Assert.Equal(33, spec.CoordinatorEpoch());
    }

    [Fact]
    public void AbortTransactionSpec_EqualityCoversAllFourFields()
    {
        TopicPartition partition = new TopicPartition("orders", 4);
        AbortTransactionSpec baseline = new AbortTransactionSpec(partition, 11L, 22, 33);

        Assert.Equal(baseline, new AbortTransactionSpec(partition, 11L, 22, 33));
        Assert.Equal(baseline.GetHashCode(), new AbortTransactionSpec(partition, 11L, 22, 33).GetHashCode());

        Assert.NotEqual(baseline, new AbortTransactionSpec(new TopicPartition("orders", 9), 11L, 22, 33));
        Assert.NotEqual(baseline, new AbortTransactionSpec(partition, 99L, 22, 33));
        Assert.NotEqual(baseline, new AbortTransactionSpec(partition, 11L, 99, 33));
        Assert.NotEqual(baseline, new AbortTransactionSpec(partition, 11L, 22, 99));
    }

    [Fact]
    public void AbortTransactionSpec_ToStringMatchesJavasShape() =>
        Assert.Equal(
            "AbortTransactionSpec(topicPartition=orders-4, producerId=11, producerEpoch=22, coordinatorEpoch=33)",
            new AbortTransactionSpec(new TopicPartition("orders", 4), 11L, 22, 33).ToString());

    [Fact]
    public void EveryP8OptionsTypeDefaultsItsTimeoutToNull()
    {
        Assert.Null(new DescribeProducersOptions().TimeoutMs());
        Assert.Null(new DescribeTransactionsOptions().TimeoutMs());
        Assert.Null(new AbortTransactionOptions().TimeoutMs());
        Assert.Null(new TerminateTransactionOptions().TimeoutMs());
        Assert.Null(new FenceProducersOptions().TimeoutMs());
        Assert.Null(new ListTransactionsOptions().TimeoutMs());
    }

    [Fact]
    public void DescribeProducersOptions_BrokerIdDefaultsToNullMeaningQueryTheLeader() =>
        Assert.Null(new DescribeProducersOptions().BrokerId());

    [Fact]
    public void DescribeProducersOptions_EqualityIncludesTheTimeout()
    {
        DescribeProducersOptions baseline = new DescribeProducersOptions().BrokerId(3).TimeoutMs(500);

        Assert.Equal(baseline, new DescribeProducersOptions().BrokerId(3).TimeoutMs(500));
        Assert.Equal(
            baseline.GetHashCode(),
            new DescribeProducersOptions().BrokerId(3).TimeoutMs(500).GetHashCode());

        Assert.NotEqual(baseline, new DescribeProducersOptions().BrokerId(4).TimeoutMs(500));

        // Java's equals includes timeoutMs, the opposite of ListTransactionsOptions.
        Assert.NotEqual(baseline, new DescribeProducersOptions().BrokerId(3).TimeoutMs(501));
        Assert.NotEqual(baseline, new DescribeProducersOptions().BrokerId(3));
    }

    [Fact]
    public void DescribeProducersOptions_ToStringMatchesJavasShape()
    {
        Assert.Equal(
            "DescribeProducersOptions(brokerId=OptionalInt[3], timeoutMs=500)",
            new DescribeProducersOptions().BrokerId(3).TimeoutMs(500).ToString());
        Assert.Equal(
            "DescribeProducersOptions(brokerId=OptionalInt.empty, timeoutMs=null)",
            new DescribeProducersOptions().ToString());
    }

    [Fact]
    public void TimeoutOnlyOptions_ToStringMatchesJavasPerClassBracketStyle()
    {
        // Java is not uniform: two of the four use parentheses and two use braces.
        Assert.Equal(
            "DescribeTransactionsOptions(timeoutMs=7)",
            new DescribeTransactionsOptions().TimeoutMs(7).ToString());
        Assert.Equal(
            "AbortTransactionOptions(timeoutMs=7)",
            new AbortTransactionOptions().TimeoutMs(7).ToString());
        Assert.Equal(
            "TerminateTransactionOptions{timeoutMs=7}",
            new TerminateTransactionOptions().TimeoutMs(7).ToString());
        Assert.Equal(
            "FenceProducersOptions{timeoutMs=7}",
            new FenceProducersOptions().TimeoutMs(7).ToString());
    }

    [Fact]
    public void ListTransactionsOptions_DurationDefaultsToMinusOneNotZero()
    {
        // A 0 would be a real "longer than 0 ms" filter; -1 is the neutral value.
        Assert.Equal(-1L, new ListTransactionsOptions().FilteredDuration());
    }

    [Fact]
    public void ListTransactionsOptions_PatternDefaultsToNullAndTheEmptyStringIsDistinct()
    {
        Assert.Null(new ListTransactionsOptions().FilteredTransactionalIdPattern());

        ListTransactionsOptions empty = new ListTransactionsOptions()
            .FilterOnTransactionalIdPattern(string.Empty);

        Assert.Equal(string.Empty, empty.FilteredTransactionalIdPattern());
        Assert.NotEqual(new ListTransactionsOptions(), empty);
    }

    [Fact]
    public void ListTransactionsOptions_CollectionFiltersDefaultToEmptyAndMeanAll()
    {
        ListTransactionsOptions options = new ListTransactionsOptions();

        Assert.Empty(options.FilteredStates());
        Assert.Empty(options.FilteredProducerIds());

        // Unlike ListConsumerGroupOffsetsSpec, empty and unset are genuinely identical here.
        options.FilterStates(Array.Empty<TransactionState>());
        options.FilterProducerIds(Array.Empty<long>());
        Assert.Equal(new ListTransactionsOptions(), options);
    }

    [Fact]
    public void ListTransactionsOptions_CollectionFiltersRejectNull()
    {
        ListTransactionsOptions options = new ListTransactionsOptions();

        Assert.Throws<ArgumentNullException>(() => options.FilterStates(null!));
        Assert.Throws<ArgumentNullException>(() => options.FilterProducerIds(null!));
    }

    [Fact]
    public void ListTransactionsOptions_CollectionFiltersAreDeduplicatedReadOnlyCopies()
    {
        List<TransactionState> states = new List<TransactionState>
        {
            TransactionState.ONGOING,
            TransactionState.ONGOING,
            TransactionState.PREPARE_ABORT,
        };

        ListTransactionsOptions options = new ListTransactionsOptions().FilterStates(states);

        Assert.Equal(2, options.FilteredStates().Count);

        states.Add(TransactionState.EMPTY);
        Assert.Equal(2, options.FilteredStates().Count);
        Assert.NotSame(states, options.FilteredStates());
    }

    [Fact]
    public void ListTransactionsOptions_EqualityExcludesTheTimeout()
    {
        ListTransactionsOptions baseline = new ListTransactionsOptions()
            .FilterStates(new[] { TransactionState.ONGOING })
            .FilterProducerIds(new[] { 7L })
            .FilterOnDuration(1_000L)
            .FilterOnTransactionalIdPattern("txn-.*")
            .TimeoutMs(500);

        ListTransactionsOptions differentTimeout = new ListTransactionsOptions()
            .FilterStates(new[] { TransactionState.ONGOING })
            .FilterProducerIds(new[] { 7L })
            .FilterOnDuration(1_000L)
            .FilterOnTransactionalIdPattern("txn-.*")
            .TimeoutMs(999);

        // Java's equals deliberately omits timeoutMs.
        Assert.Equal(baseline, differentTimeout);
        Assert.Equal(baseline.GetHashCode(), differentTimeout.GetHashCode());

        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilterOnDuration(1_001L)));
        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilterOnTransactionalIdPattern("other")));
        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilterOnTransactionalIdPattern(null)));
        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilterStates(new[] { TransactionState.EMPTY })));
        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilterProducerIds(new[] { 8L })));
    }

    [Fact]
    public void ListTransactionsOptions_ToStringIncludesTheTimeoutEqualsOmits() =>
        Assert.Equal(
            "ListTransactionsOptions(filteredStates=[Ongoing], filteredProducerIds=[7]"
                + ", filteredDuration=1000, filteredTransactionalIdPattern=txn-.*, timeoutMs=500)",
            new ListTransactionsOptions()
                .FilterStates(new[] { TransactionState.ONGOING })
                .FilterProducerIds(new[] { 7L })
                .FilterOnDuration(1_000L)
                .FilterOnTransactionalIdPattern("txn-.*")
                .TimeoutMs(500).ToString());

    [Fact]
    public void P8TypesPublishExactlyTheirJavaAccessors()
    {
        Assert.Equal(
            new[]
            {
                "CoordinatorEpoch",
                "CurrentTransactionStartOffset",
                "LastSequence",
                "LastTimestamp",
                "ProducerEpoch",
                "ProducerId",
            },
            DeclaredPublicMethodNames(typeof(ProducerState)));

        Assert.Equal(
            new[] { "ProducerId", "State", "TransactionalId" },
            DeclaredPublicMethodNames(typeof(TransactionListing)));

        Assert.Equal(
            new[]
            {
                "CoordinatorId",
                "ProducerEpoch",
                "ProducerId",
                "State",
                "TopicPartitions",
                "TransactionStartTimeMs",
                "TransactionTimeoutMs",
            },
            DeclaredPublicMethodNames(typeof(TransactionDescription)));

        Assert.Equal(
            new[] { "CoordinatorEpoch", "ProducerEpoch", "ProducerId", "TopicPartition" },
            DeclaredPublicMethodNames(typeof(AbortTransactionSpec)));

        SetterMethods.AssertGettersAndSetters(typeof(DescribeProducersOptions), "BrokerId", "TimeoutMs");

        Type listTransactions = typeof(ListTransactionsOptions);
        Assert.Empty(listTransactions.GetProperties());
        Assert.Equal(
            new[]
            {
                "FilteredDuration",
                "FilteredProducerIds",
                "FilteredStates",
                "FilteredTransactionalIdPattern",
                "TimeoutMs",
            },
            Getters.NamesOf(listTransactions));
        Assert.Equal(
            new[]
            {
                "FilterOnDuration",
                "FilterOnTransactionalIdPattern",
                "FilterProducerIds",
                "FilterStates",
                "TimeoutMs",
            },
            SetterMethods.NamesOf(listTransactions));

        foreach (Type timeoutOnly in new[]
        {
            typeof(DescribeTransactionsOptions),
            typeof(AbortTransactionOptions),
            typeof(TerminateTransactionOptions),
            typeof(FenceProducersOptions),
        })
        {
            SetterMethods.AssertGettersAndSetters(timeoutOnly, "TimeoutMs");
        }
    }

    [Fact]
    public void P8ValueTypesAndOptionsDeclareNoProperties()
    {
        // Java's value classes and options declare only accessors, setters and equals/hashCode/toString; all of them are
        // methods here, so the property set is empty.
        Assert.Empty(DeclaredPublicPropertyNames(typeof(ProducerState)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(TransactionListing)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(TransactionDescription)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(AbortTransactionSpec)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(ListTransactionsOptions)));
        Assert.Empty(DeclaredPublicPropertyNames(typeof(DescribeProducersOptions)));
    }

    private static ListTransactionsOptions Mutate(
        ListTransactionsOptions source,
        Action<ListTransactionsOptions> change)
    {
        ListTransactionsOptions copy = new ListTransactionsOptions()
            .FilterStates(source.FilteredStates())
            .FilterProducerIds(source.FilteredProducerIds())
            .FilterOnDuration(source.FilteredDuration())
            .FilterOnTransactionalIdPattern(source.FilteredTransactionalIdPattern())
            .TimeoutMs(source.TimeoutMs());

        change(copy);
        return copy;
    }

    private static Type ReturnType(Type type, string name) =>
        type.GetMethod(name, BindingFlags.Public | BindingFlags.Instance, null, Type.EmptyTypes, null)!.ReturnType;

    private static string[] DeclaredPublicMethodNames(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method =>
                !method.IsSpecialName
                && !method.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false)
                && method.GetBaseDefinition().DeclaringType != typeof(object))
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

    private static string[] DeclaredPublicPropertyNames(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(property => !property.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
            .Select(property => property.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();
}
