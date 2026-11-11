// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The text the binding prints for the Java enums whose <c>toString()</c> is a display name, not the
/// constant name a C# enum's <c>ToString()</c> prints: Java's <c>toString()</c>, for every member.
/// </summary>
public sealed class PublicJavaToStringEnumTextTests
{
    private static readonly (GroupState Member, string Java)[] s_groupStates =
    {
        (GroupState.UNKNOWN, "Unknown"),
        (GroupState.PREPARING_REBALANCE, "PreparingRebalance"),
        (GroupState.COMPLETING_REBALANCE, "CompletingRebalance"),
        (GroupState.STABLE, "Stable"),
        (GroupState.DEAD, "Dead"),
        (GroupState.EMPTY, "Empty"),
        (GroupState.ASSIGNING, "Assigning"),
        (GroupState.RECONCILING, "Reconciling"),
        (GroupState.NOT_READY, "NotReady"),
    };

    private static readonly (GroupType Member, string Java)[] s_groupTypes =
    {
        (GroupType.UNKNOWN, "Unknown"),
        (GroupType.CONSUMER, "Consumer"),
        (GroupType.CLASSIC, "Classic"),
        (GroupType.SHARE, "Share"),
        (GroupType.STREAMS, "Streams"),
    };

    private static readonly (TransactionState Member, string Java)[] s_transactionStates =
    {
        (TransactionState.ONGOING, "Ongoing"),
        (TransactionState.PREPARE_ABORT, "PrepareAbort"),
        (TransactionState.PREPARE_COMMIT, "PrepareCommit"),
        (TransactionState.COMPLETE_ABORT, "CompleteAbort"),
        (TransactionState.COMPLETE_COMMIT, "CompleteCommit"),
        (TransactionState.EMPTY, "Empty"),
        (TransactionState.PREPARE_EPOCH_FENCE, "PrepareEpochFence"),
        (TransactionState.UNKNOWN, "Unknown"),
    };

    private static readonly (ClassicGroupState Member, string Java)[] s_classicGroupStates =
    {
        (ClassicGroupState.UNKNOWN, "Unknown"),
        (ClassicGroupState.PREPARING_REBALANCE, "PreparingRebalance"),
        (ClassicGroupState.COMPLETING_REBALANCE, "CompletingRebalance"),
        (ClassicGroupState.STABLE, "Stable"),
        (ClassicGroupState.DEAD, "Dead"),
        (ClassicGroupState.EMPTY, "Empty"),
    };

    [Fact]
    public void GroupState_PrintsJavasToString_ForEveryMember()
    {
        Assert.Equal(Enum.GetValues(typeof(GroupState)).Length, s_groupStates.Length);
        foreach ((GroupState member, string java) in s_groupStates)
        {
            Assert.Equal(java, GroupMarshal.NameFromState(member));
            Assert.Equal(
                "(groupId='g', type=none, protocol='consumer', groupState=" + java + ")",
                new GroupListing("g", null, "consumer", member).ToString());
            Assert.Contains(
                ", type=Consumer, groupState=" + java + ", ",
                Description(GroupType.CONSUMER, member).ToString(),
                StringComparison.Ordinal);
        }

        Assert.Equal(
            "(groupId='g', type=none, protocol='consumer', groupState=99)",
            new GroupListing("g", null, "consumer", (GroupState)99).ToString());
    }

    [Fact]
    public void GroupType_PrintsJavasToString_ForEveryMember()
    {
        Assert.Equal(Enum.GetValues(typeof(GroupType)).Length, s_groupTypes.Length);
        foreach ((GroupType member, string java) in s_groupTypes)
        {
            Assert.Equal(java, GroupMarshal.NameFromType(member));
            Assert.Equal(
                "(groupId='g', type=" + java + ", protocol='consumer', groupState=none)",
                new GroupListing("g", member, "consumer", null).ToString());
            Assert.Contains(
                ", type=" + java + ", groupState=Stable, ",
                Description(member, GroupState.STABLE).ToString(),
                StringComparison.Ordinal);
        }

        Assert.Equal(
            "(groupId='g', type=99, protocol='consumer', groupState=none)",
            new GroupListing("g", (GroupType)99, "consumer", null).ToString());
    }

    [Fact]
    public void TransactionState_PrintsJavasToString_ForEveryMember()
    {
        Assert.Equal(Enum.GetValues(typeof(TransactionState)).Length, s_transactionStates.Length);
        foreach ((TransactionState member, string java) in s_transactionStates)
        {
            Assert.Equal(java, TransactionMarshal.NameFromState(member));
            Assert.Equal(
                "TransactionListing(transactionalId='t', producerId=1, transactionState=" + java + ")",
                new TransactionListing("t", 1L, member).ToString());
            Assert.StartsWith(
                "TransactionDescription(coordinatorId=1, state=" + java + ", ",
                new TransactionDescription(1, member, 2L, 3, 4L, null, null).ToString(),
                StringComparison.Ordinal);
            Assert.StartsWith(
                "ListTransactionsOptions(filteredStates=[" + java + "], ",
                new ListTransactionsOptions().FilterStates(new[] { member }).ToString(),
                StringComparison.Ordinal);
        }

        Assert.Null(TransactionMarshal.NameFromState((TransactionState)99));
        Assert.Equal(
            "state",
            Assert.Throws<ArgumentOutOfRangeException>(() => TransactionMarshal.WireName((TransactionState)99)).ParamName);
        Assert.Equal(
            "TransactionListing(transactionalId='t', producerId=1, transactionState=99)",
            new TransactionListing("t", 1L, (TransactionState)99).ToString());
    }

    [Fact]
    public void ClassicGroupState_PrintsJavasToString_ForEveryMember()
    {
        Assert.Equal(Enum.GetValues(typeof(ClassicGroupState)).Length, s_classicGroupStates.Length);
        foreach ((ClassicGroupState member, string java) in s_classicGroupStates)
        {
            Assert.Equal(java, GroupMarshal.NameFromClassicState(member));
            Assert.Contains(
                ", state=" + java + ", ",
                new ClassicGroupDescription("g", "consumer", "range", null, member, null).ToString(),
                StringComparison.Ordinal);
        }
    }

    private static ConsumerGroupDescription Description(GroupType type, GroupState state) =>
        new ConsumerGroupDescription("g", false, null, "range", type, state, null, null, null, null);
}
