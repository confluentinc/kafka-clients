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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the twelve producer/transaction value and options types of M15/P8 against their Java
/// counterparts.
/// </summary>
/// <remarks>
/// ⚠ Every value assertion uses a <b>distinct</b> per-field value, so a transposition between
/// two same-typed fields fails rather than cancelling out.
/// </remarks>
public sealed class PublicAdminP8TypesTests
{
    // ---- TransactionState ------------------------------------------------------------

    [Fact]
    public void TransactionState_HasJavasEightConstantsInDeclarationOrder()
    {
        Assert.Equal(
            new[]
            {
                "Ongoing",
                "PrepareAbort",
                "PrepareCommit",
                "CompleteAbort",
                "CompleteCommit",
                "Empty",
                "PrepareEpochFence",
                "Unknown",
            },
            Enum.GetNames(typeof(TransactionState)));
    }

    [Fact]
    public void TransactionState_HasNoDeadMember()
    {
        // The broker-side coordinator.transaction.TransactionState has a ninth DEAD constant
        // with no client counterpart; the client enum is this binding's contract.
        Assert.DoesNotContain("Dead", Enum.GetNames(typeof(TransactionState)));
    }

    [Fact]
    public void TransactionState_LivesInTheAdminNamespace()
    {
        // Java's package is org.apache.kafka.clients.admin (not common), so the .NET home is
        // Confluent.Kafka.Admin — the mapping every other admin type follows.
        Assert.Equal("Confluent.Kafka.Admin", typeof(TransactionState).Namespace);
    }

    // ---- ProducerState ---------------------------------------------------------------

    [Fact]
    public void ProducerState_ExposesEveryFieldWithJavasWidths()
    {
        ProducerState state = new ProducerState(11L, 22, 33, 44L, 55, 66L);

        Assert.Equal(11L, state.ProducerId);
        Assert.Equal(22, state.ProducerEpoch);
        Assert.Equal(33, state.LastSequence);
        Assert.Equal(44L, state.LastTimestamp);
        Assert.Equal(55, state.CoordinatorEpoch);
        Assert.Equal(66L, state.CurrentTransactionStartOffset);

        Assert.Equal(typeof(long), Property(typeof(ProducerState), nameof(ProducerState.ProducerId)));
        Assert.Equal(typeof(int), Property(typeof(ProducerState), nameof(ProducerState.ProducerEpoch)));
        Assert.Equal(typeof(int), Property(typeof(ProducerState), nameof(ProducerState.LastSequence)));
        Assert.Equal(typeof(long), Property(typeof(ProducerState), nameof(ProducerState.LastTimestamp)));
        Assert.Equal(typeof(int?), Property(typeof(ProducerState), nameof(ProducerState.CoordinatorEpoch)));
        Assert.Equal(
            typeof(long?),
            Property(typeof(ProducerState), nameof(ProducerState.CurrentTransactionStartOffset)));
    }

    [Fact]
    public void ProducerState_AbsentOptionalsAreNullNotZeroOrMinusOne()
    {
        ProducerState state = new ProducerState(1L, 2, 3, 4L, null, null);

        Assert.Null(state.CoordinatorEpoch);
        Assert.Null(state.CurrentTransactionStartOffset);
        Assert.False(state.CoordinatorEpoch.HasValue);
        Assert.False(state.CurrentTransactionStartOffset.HasValue);
    }

    [Fact]
    public void ProducerState_MinusOneIsAValueNotAnAbsence()
    {
        // The scalar siblings use -1 as their out-of-range sentinel; the optionals do not.
        ProducerState state = new ProducerState(-1L, -1, -1, -1L, -1, -1L);

        Assert.Equal(-1, state.CoordinatorEpoch);
        Assert.Equal(-1L, state.CurrentTransactionStartOffset);
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
    public void ProducerState_ToStringNamesEveryFieldAndRendersAbsenceAsNull()
    {
        Assert.Equal(
            "ProducerState(producerId=11, producerEpoch=22, lastSequence=33, lastTimestamp=44"
                + ", coordinatorEpoch=55, currentTransactionStartOffset=66)",
            new ProducerState(11L, 22, 33, 44L, 55, 66L).ToString());

        Assert.Equal(
            "ProducerState(producerId=11, producerEpoch=22, lastSequence=33, lastTimestamp=44"
                + ", coordinatorEpoch=null, currentTransactionStartOffset=null)",
            new ProducerState(11L, 22, 33, 44L, null, null).ToString());
    }

    // ---- TransactionListing ----------------------------------------------------------

    [Fact]
    public void TransactionListing_ExposesJavasThreeAccessors()
    {
        TransactionListing listing = new TransactionListing("txn-1", 77L, TransactionState.PrepareCommit);

        Assert.Equal("txn-1", listing.TransactionalId);
        Assert.Equal(77L, listing.ProducerId);
        Assert.Equal(TransactionState.PrepareCommit, listing.State);

        // Java names the accessor state() although the field is transactionState (:44).
        Assert.Contains("State", DeclaredPublicPropertyNames(typeof(TransactionListing)));
        Assert.DoesNotContain("TransactionState", DeclaredPublicPropertyNames(typeof(TransactionListing)));
    }

    [Fact]
    public void TransactionListing_RejectsANullTransactionalId()
    {
        ArgumentNullException error = Assert.Throws<ArgumentNullException>(
            () => new TransactionListing(null!, 1L, TransactionState.Empty));

        Assert.Equal("transactionalId", error.ParamName);
    }

    [Fact]
    public void TransactionListing_EqualityCoversAllThreeFields()
    {
        TransactionListing baseline = new TransactionListing("txn-1", 77L, TransactionState.Ongoing);

        Assert.Equal(baseline, new TransactionListing("txn-1", 77L, TransactionState.Ongoing));
        Assert.Equal(
            baseline.GetHashCode(),
            new TransactionListing("txn-1", 77L, TransactionState.Ongoing).GetHashCode());

        Assert.NotEqual(baseline, new TransactionListing("txn-2", 77L, TransactionState.Ongoing));
        Assert.NotEqual(baseline, new TransactionListing("txn-1", 78L, TransactionState.Ongoing));
        Assert.NotEqual(baseline, new TransactionListing("txn-1", 77L, TransactionState.Empty));
    }

    [Fact]
    public void TransactionListing_ToStringMatchesJavasShape() =>
        Assert.Equal(
            "TransactionListing(transactionalId='txn-1', producerId=77, transactionState=Ongoing)",
            new TransactionListing("txn-1", 77L, TransactionState.Ongoing).ToString());

    // ---- TransactionDescription ------------------------------------------------------

    [Fact]
    public void TransactionDescription_ExposesEveryFieldWithJavasWidths()
    {
        TransactionDescription description = new TransactionDescription(
            coordinatorId: 3,
            state: TransactionState.CompleteAbort,
            producerId: 44L,
            producerEpoch: 5,
            transactionTimeoutMs: 60_000L,
            transactionStartTimeMs: 1_700_000_000_000L,
            topicPartitions: new[] { new TopicPartition("orders", 7) });

        Assert.Equal(3, description.CoordinatorId);
        Assert.Equal(TransactionState.CompleteAbort, description.State);
        Assert.Equal(44L, description.ProducerId);
        Assert.Equal(5, description.ProducerEpoch);
        Assert.Equal(60_000L, description.TransactionTimeoutMs);
        Assert.Equal(1_700_000_000_000L, description.TransactionStartTimeMs);
        Assert.Equal(new[] { new TopicPartition("orders", 7) }, description.TopicPartitions);

        // transactionTimeoutMs is a long in Java (:30) although the wire field is an int, and
        // producerEpoch is an int here but a short on AbortTransactionSpec.
        Assert.Equal(
            typeof(long),
            Property(typeof(TransactionDescription), nameof(TransactionDescription.TransactionTimeoutMs)));
        Assert.Equal(
            typeof(int),
            Property(typeof(TransactionDescription), nameof(TransactionDescription.ProducerEpoch)));
        Assert.Equal(
            typeof(long?),
            Property(typeof(TransactionDescription), nameof(TransactionDescription.TransactionStartTimeMs)));
    }

    [Fact]
    public void TransactionDescription_AbsentStartTimeIsNull() =>
        Assert.Null(
            new TransactionDescription(1, TransactionState.Empty, 2L, 3, 4L, null, null)
                .TransactionStartTimeMs);

    [Fact]
    public void TransactionDescription_NullPartitionsBecomeAnEmptyCollection() =>
        Assert.Empty(
            new TransactionDescription(1, TransactionState.Empty, 2L, 3, 4L, null, null).TopicPartitions);

    [Fact]
    public void TransactionDescription_PartitionsAreDeduplicatedAndSetCompared()
    {
        TransactionDescription duplicated = new TransactionDescription(
            1,
            TransactionState.Ongoing,
            2L,
            3,
            4L,
            5L,
            new[] { new TopicPartition("t", 0), new TopicPartition("t", 0), new TopicPartition("t", 1) });

        Assert.Equal(2, duplicated.TopicPartitions.Count);

        TransactionDescription reordered = new TransactionDescription(
            1,
            TransactionState.Ongoing,
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
            1, TransactionState.Ongoing, 2L, 3, 4L, 5L, partitions);

        Assert.Equal(
            baseline,
            new TransactionDescription(1, TransactionState.Ongoing, 2L, 3, 4L, 5L, partitions));

        Assert.NotEqual(
            baseline,
            new TransactionDescription(9, TransactionState.Ongoing, 2L, 3, 4L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.Empty, 2L, 3, 4L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.Ongoing, 9L, 3, 4L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.Ongoing, 2L, 9, 4L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.Ongoing, 2L, 3, 9L, 5L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.Ongoing, 2L, 3, 4L, 9L, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(1, TransactionState.Ongoing, 2L, 3, 4L, null, partitions));
        Assert.NotEqual(
            baseline,
            new TransactionDescription(
                1, TransactionState.Ongoing, 2L, 3, 4L, 5L, new[] { new TopicPartition("t", 9) }));
    }

    [Fact]
    public void TransactionDescription_ToStringNamesEveryFieldAndRendersAbsenceAsNull()
    {
        Assert.Equal(
            "TransactionDescription(coordinatorId=1, state=Ongoing, producerId=2, producerEpoch=3"
                + ", transactionTimeoutMs=4, transactionStartTimeMs=5, topicPartitions=[t-0])",
            new TransactionDescription(
                1, TransactionState.Ongoing, 2L, 3, 4L, 5L, new[] { new TopicPartition("t", 0) })
                .ToString());

        Assert.Contains(
            "transactionStartTimeMs=null",
            new TransactionDescription(1, TransactionState.Ongoing, 2L, 3, 4L, null, null).ToString());
    }

    // ---- AbortTransactionSpec --------------------------------------------------------

    [Fact]
    public void AbortTransactionSpec_ProducerEpochIsAShortUnlikeItsSiblings()
    {
        Assert.Equal(
            typeof(short),
            Property(typeof(AbortTransactionSpec), nameof(AbortTransactionSpec.ProducerEpoch)));
        Assert.Equal(typeof(int), Property(typeof(ProducerState), nameof(ProducerState.ProducerEpoch)));
        Assert.Equal(
            typeof(int),
            Property(typeof(TransactionDescription), nameof(TransactionDescription.ProducerEpoch)));
    }

    [Fact]
    public void AbortTransactionSpec_ExposesJavasFourAccessors()
    {
        AbortTransactionSpec spec = new AbortTransactionSpec(new TopicPartition("orders", 4), 11L, 22, 33);

        Assert.Equal(new TopicPartition("orders", 4), spec.TopicPartition);
        Assert.Equal(11L, spec.ProducerId);
        Assert.Equal((short)22, spec.ProducerEpoch);
        Assert.Equal(33, spec.CoordinatorEpoch);
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

    // ---- Options: defaults -----------------------------------------------------------

    [Fact]
    public void EveryP8OptionsTypeDefaultsItsTimeoutToNull()
    {
        Assert.Null(new DescribeProducersOptions().TimeoutMs);
        Assert.Null(new DescribeTransactionsOptions().TimeoutMs);
        Assert.Null(new AbortTransactionOptions().TimeoutMs);
        Assert.Null(new TerminateTransactionOptions().TimeoutMs);
        Assert.Null(new FenceProducersOptions().TimeoutMs);
        Assert.Null(new ListTransactionsOptions().TimeoutMs);
    }

    [Fact]
    public void DescribeProducersOptions_BrokerIdDefaultsToNullMeaningQueryTheLeader() =>
        Assert.Null(new DescribeProducersOptions().BrokerId);

    [Fact]
    public void DescribeProducersOptions_EqualityIncludesTheTimeout()
    {
        DescribeProducersOptions baseline = new DescribeProducersOptions { BrokerId = 3, TimeoutMs = 500 };

        Assert.Equal(baseline, new DescribeProducersOptions { BrokerId = 3, TimeoutMs = 500 });
        Assert.Equal(
            baseline.GetHashCode(),
            new DescribeProducersOptions { BrokerId = 3, TimeoutMs = 500 }.GetHashCode());

        Assert.NotEqual(baseline, new DescribeProducersOptions { BrokerId = 4, TimeoutMs = 500 });

        // Java's equals (:43-44) includes timeoutMs — the opposite of ListTransactionsOptions.
        Assert.NotEqual(baseline, new DescribeProducersOptions { BrokerId = 3, TimeoutMs = 501 });
        Assert.NotEqual(baseline, new DescribeProducersOptions { BrokerId = 3 });
    }

    [Fact]
    public void DescribeProducersOptions_ToStringMatchesJavasShape()
    {
        Assert.Equal(
            "DescribeProducersOptions(brokerId=3, timeoutMs=500)",
            new DescribeProducersOptions { BrokerId = 3, TimeoutMs = 500 }.ToString());
        Assert.Equal(
            "DescribeProducersOptions(brokerId=null, timeoutMs=null)",
            new DescribeProducersOptions().ToString());
    }

    [Fact]
    public void TimeoutOnlyOptions_ToStringMatchesJavasPerClassBracketStyle()
    {
        // Java is not uniform: two of the four use parentheses and two use braces.
        Assert.Equal(
            "DescribeTransactionsOptions(timeoutMs=7)",
            new DescribeTransactionsOptions { TimeoutMs = 7 }.ToString());
        Assert.Equal(
            "AbortTransactionOptions(timeoutMs=7)",
            new AbortTransactionOptions { TimeoutMs = 7 }.ToString());
        Assert.Equal(
            "TerminateTransactionOptions{timeoutMs=7}",
            new TerminateTransactionOptions { TimeoutMs = 7 }.ToString());
        Assert.Equal(
            "FenceProducersOptions{timeoutMs=7}",
            new FenceProducersOptions { TimeoutMs = 7 }.ToString());
    }

    // ---- ListTransactionsOptions: the three neutral encodings -------------------------

    [Fact]
    public void ListTransactionsOptions_DurationDefaultsToMinusOneNotZero()
    {
        // A 0 would be a real "longer than 0 ms" filter; -1 (:33) is the neutral value.
        Assert.Equal(-1L, new ListTransactionsOptions().FilteredDuration);
    }

    [Fact]
    public void ListTransactionsOptions_PatternDefaultsToNullAndTheEmptyStringIsDistinct()
    {
        Assert.Null(new ListTransactionsOptions().FilteredTransactionalIdPattern);

        ListTransactionsOptions empty = new ListTransactionsOptions
        {
            FilteredTransactionalIdPattern = string.Empty,
        };

        Assert.Equal(string.Empty, empty.FilteredTransactionalIdPattern);
        Assert.NotEqual(new ListTransactionsOptions(), empty);
    }

    [Fact]
    public void ListTransactionsOptions_CollectionFiltersDefaultToEmptyAndMeanAll()
    {
        ListTransactionsOptions options = new ListTransactionsOptions();

        Assert.Empty(options.FilteredStates);
        Assert.Empty(options.FilteredProducerIds);

        // Unlike ListConsumerGroupOffsetsSpec, empty and unset are genuinely identical here.
        options.FilteredStates = Array.Empty<TransactionState>();
        options.FilteredProducerIds = Array.Empty<long>();
        Assert.Equal(new ListTransactionsOptions(), options);
    }

    [Fact]
    public void ListTransactionsOptions_CollectionFiltersRejectNull()
    {
        ListTransactionsOptions options = new ListTransactionsOptions();

        Assert.Throws<ArgumentNullException>(() => options.FilteredStates = null!);
        Assert.Throws<ArgumentNullException>(() => options.FilteredProducerIds = null!);
    }

    [Fact]
    public void ListTransactionsOptions_CollectionFiltersAreDeduplicatedReadOnlyCopies()
    {
        List<TransactionState> states = new List<TransactionState>
        {
            TransactionState.Ongoing,
            TransactionState.Ongoing,
            TransactionState.PrepareAbort,
        };

        ListTransactionsOptions options = new ListTransactionsOptions { FilteredStates = states };

        Assert.Equal(2, options.FilteredStates.Count);

        // The stored copy does not track a later mutation of the caller's collection.
        states.Add(TransactionState.Empty);
        Assert.Equal(2, options.FilteredStates.Count);
        Assert.NotSame(states, options.FilteredStates);
    }

    [Fact]
    public void ListTransactionsOptions_EqualityExcludesTheTimeout()
    {
        ListTransactionsOptions baseline = new ListTransactionsOptions
        {
            FilteredStates = new[] { TransactionState.Ongoing },
            FilteredProducerIds = new[] { 7L },
            FilteredDuration = 1_000L,
            FilteredTransactionalIdPattern = "txn-.*",
            TimeoutMs = 500,
        };

        ListTransactionsOptions differentTimeout = new ListTransactionsOptions
        {
            FilteredStates = new[] { TransactionState.Ongoing },
            FilteredProducerIds = new[] { 7L },
            FilteredDuration = 1_000L,
            FilteredTransactionalIdPattern = "txn-.*",
            TimeoutMs = 999,
        };

        // Java's equals (:142-145) deliberately omits timeoutMs.
        Assert.Equal(baseline, differentTimeout);
        Assert.Equal(baseline.GetHashCode(), differentTimeout.GetHashCode());

        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilteredDuration = 1_001L));
        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilteredTransactionalIdPattern = "other"));
        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilteredTransactionalIdPattern = null));
        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilteredStates = new[] { TransactionState.Empty }));
        Assert.NotEqual(baseline, Mutate(baseline, o => o.FilteredProducerIds = new[] { 8L }));
    }

    [Fact]
    public void ListTransactionsOptions_ToStringIncludesTheTimeoutEqualsOmits() =>
        Assert.Equal(
            "ListTransactionsOptions(filteredStates=[Ongoing], filteredProducerIds=[7]"
                + ", filteredDuration=1000, filteredTransactionalIdPattern=txn-.*, timeoutMs=500)",
            new ListTransactionsOptions
            {
                FilteredStates = new[] { TransactionState.Ongoing },
                FilteredProducerIds = new[] { 7L },
                FilteredDuration = 1_000L,
                FilteredTransactionalIdPattern = "txn-.*",
                TimeoutMs = 500,
            }.ToString());

    // ---- Surface pins ----------------------------------------------------------------

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
            DeclaredPublicPropertyNames(typeof(ProducerState)));

        Assert.Equal(
            new[] { "ProducerId", "State", "TransactionalId" },
            DeclaredPublicPropertyNames(typeof(TransactionListing)));

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
            DeclaredPublicPropertyNames(typeof(TransactionDescription)));

        Assert.Equal(
            new[] { "CoordinatorEpoch", "ProducerEpoch", "ProducerId", "TopicPartition" },
            DeclaredPublicPropertyNames(typeof(AbortTransactionSpec)));

        Assert.Equal(
            new[] { "BrokerId", "TimeoutMs" },
            DeclaredPublicPropertyNames(typeof(DescribeProducersOptions)));

        Assert.Equal(
            new[]
            {
                "FilteredDuration",
                "FilteredProducerIds",
                "FilteredStates",
                "FilteredTransactionalIdPattern",
                "TimeoutMs",
            },
            DeclaredPublicPropertyNames(typeof(ListTransactionsOptions)));

        foreach (Type timeoutOnly in new[]
        {
            typeof(DescribeTransactionsOptions),
            typeof(AbortTransactionOptions),
            typeof(TerminateTransactionOptions),
            typeof(FenceProducersOptions),
        })
        {
            Assert.Equal(new[] { "TimeoutMs" }, DeclaredPublicPropertyNames(timeoutOnly));
        }
    }

    [Fact]
    public void P8ValueTypesDeclareNoMethodsBeyondTheObjectOverrides()
    {
        // Java's value classes declare only accessors plus equals/hashCode/toString; the
        // accessors are properties here, so the method set must be empty.
        Assert.Empty(DeclaredPublicMethodNames(typeof(ProducerState)));
        Assert.Empty(DeclaredPublicMethodNames(typeof(TransactionListing)));
        Assert.Empty(DeclaredPublicMethodNames(typeof(TransactionDescription)));
        Assert.Empty(DeclaredPublicMethodNames(typeof(AbortTransactionSpec)));
        Assert.Empty(DeclaredPublicMethodNames(typeof(ListTransactionsOptions)));
        Assert.Empty(DeclaredPublicMethodNames(typeof(DescribeProducersOptions)));
    }

    private static ListTransactionsOptions Mutate(
        ListTransactionsOptions source,
        Action<ListTransactionsOptions> change)
    {
        ListTransactionsOptions copy = new ListTransactionsOptions
        {
            FilteredStates = source.FilteredStates,
            FilteredProducerIds = source.FilteredProducerIds,
            FilteredDuration = source.FilteredDuration,
            FilteredTransactionalIdPattern = source.FilteredTransactionalIdPattern,
            TimeoutMs = source.TimeoutMs,
        };

        change(copy);
        return copy;
    }

    private static Type Property(Type type, string name) =>
        type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance)!.PropertyType;

    /// <summary>
    /// The type's own public instance methods, by name — selected by
    /// <see cref="BindingFlags"/> plus <see cref="CompilerGeneratedAttribute"/> and
    /// <see cref="MethodBase.IsSpecialName"/>, never by a name predicate.
    /// </summary>
    /// <param name="type">The type to inspect.</param>
    /// <returns>The sorted method names.</returns>
    private static string[] DeclaredPublicMethodNames(Type type) =>
        type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method =>
                !method.IsSpecialName
                && !method.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false)
                && method.GetBaseDefinition().DeclaringType != typeof(object))
            .Select(method => method.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

    /// <inheritdoc cref="DeclaredPublicMethodNames"/>
    private static string[] DeclaredPublicPropertyNames(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(property => !property.IsDefined(typeof(CompilerGeneratedAttribute), inherit: false))
            .Select(property => property.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();
}
