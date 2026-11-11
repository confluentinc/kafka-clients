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

using Xunit;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The wire names of the four admin enums whose Java <c>toString()</c> is a display name, not the constant
/// name: every member goes out as Java's <c>toString()</c> and, where the server parses it, comes back.
/// </summary>
public sealed class TranslateAdminEnumNameTests
{
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
    public void TransactionState_GoesOutAsJavasToString_AndParsesBack()
    {
        Assert.Equal(Enum.GetValues(typeof(TransactionState)).Length, s_transactionStates.Length);
        foreach ((TransactionState member, string java) in s_transactionStates)
        {
            Assert.Equal(java, TransactionState.ToString(member));
            Assert.Equal(member, Assert.Single(TranslateAdmin.TransactionStates(new[] { java })));
            Assert.Equal(
                java,
                TranslateAdmin.TransactionListingToProto(new TransactionListing("t", 1, member)).State);
        }

        Assert.Equal(
            TransactionState.UNKNOWN, Assert.Single(TranslateAdmin.TransactionStates(new[] { "PREPARE_ABORT" })));
        Assert.Equal(TransactionState.UNKNOWN, Assert.Single(TranslateAdmin.TransactionStates(new[] { "ongoing" })));
        Assert.Equal(TransactionState.UNKNOWN, Assert.Single(TranslateAdmin.TransactionStates(new[] { "" })));
    }

    [Fact]
    public void GroupState_GoesOutAsJavasToString_AndParsesBackIgnoringCase()
    {
        Assert.Equal(Enum.GetValues(typeof(GroupState)).Length, s_groupStates.Length);
        foreach ((GroupState member, string java) in s_groupStates)
        {
            Assert.Equal(java, GroupState.ToString(member));
            Assert.Equal(member, Assert.Single(TranslateAdmin.GroupStates(new[] { java })));
            Assert.Equal(
                member, Assert.Single(TranslateAdmin.GroupStates(new[] { java.ToUpperInvariant() })));
            Assert.Equal(
                java,
                TranslateAdmin.GroupListingToProto(new GroupListing("g", null, "consumer", member)).GroupState);
            Assert.Equal(
                java,
                TranslateAdmin.ConsumerGroupDescriptionToProto(Description(GroupType.CONSUMER, member)).GroupState);
        }

        Assert.Equal(GroupState.UNKNOWN, Assert.Single(TranslateAdmin.GroupStates(new[] { "PREPARING_REBALANCE" })));
    }

    [Theory]
    [InlineData("A\u00DFigning", GroupState.ASSIGNING)]
    [InlineData("\u017Ftable", GroupState.STABLE)]
    [InlineData("Ass\u0131gn\u0131ng", GroupState.ASSIGNING)]
    [InlineData("\uFB06able", GroupState.STABLE)]
    [InlineData("", GroupState.UNKNOWN)]
    public void GroupState_ParsesAsJavasParse_UpperCasingBothSides(string name, GroupState expected) =>
        Assert.Equal(expected, Assert.Single(TranslateAdmin.GroupStates(new[] { name })));

    [Theory]
    [InlineData("Cla\u017F\u017Fic", GroupType.UNKNOWN)]
    [InlineData("cla\u00DFic", GroupType.UNKNOWN)]
    [InlineData("CONSUMER", GroupType.CONSUMER)]
    [InlineData("", GroupType.UNKNOWN)]
    public void GroupType_ParsesAsJavasParse_LowerCasingBothSides(string name, GroupType expected) =>
        Assert.Equal(expected, Assert.Single(TranslateAdmin.GroupTypes(new[] { name })));

    [Fact]
    public void GroupType_GoesOutAsJavasToString_AndParsesBackIgnoringCase()
    {
        Assert.Equal(Enum.GetValues(typeof(GroupType)).Length, s_groupTypes.Length);
        foreach ((GroupType member, string java) in s_groupTypes)
        {
            Assert.Equal(java, GroupType.ToString(member));
            Assert.Equal(member, Assert.Single(TranslateAdmin.GroupTypes(new[] { java })));
            Assert.Equal(member, Assert.Single(TranslateAdmin.GroupTypes(new[] { java.ToLowerInvariant() })));
            Assert.Equal(
                java,
                TranslateAdmin.GroupListingToProto(new GroupListing("g", member, "consumer", null)).GroupType);
            Assert.Equal(
                java,
                TranslateAdmin.ConsumerGroupDescriptionToProto(Description(member, GroupState.STABLE)).GroupType);
        }
    }

    [Fact]
    public void ClassicGroupState_GoesOutAsJavasToString()
    {
        Assert.Equal(Enum.GetValues(typeof(ClassicGroupState)).Length, s_classicGroupStates.Length);
        foreach ((ClassicGroupState member, string java) in s_classicGroupStates)
        {
            Assert.Equal(java, ClassicGroupState.ToString(member));
            Assert.Equal(
                java,
                TranslateAdmin.ClassicGroupDescriptionToProto(
                    new ClassicGroupDescription("g", "consumer", "range", null, member, null)).State);
        }
    }

    private static ConsumerGroupDescription Description(GroupType type, GroupState state) =>
        new ConsumerGroupDescription("g", false, null, "range", type, state, null, null, null, null);
}
