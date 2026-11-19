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
using System.Collections.Generic;
using System.Globalization;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// An enum value no member defines, cast in and passed to <c>ElectLeaders</c>, <c>ListOffsets</c> and
/// <c>ListTransactions</c> on the four admin clients: the first two throw what the enum's own lookup
/// (<c>ElectionType.ValueOf</c>, <c>IsolationLevel.ForId</c>) throws for its number, the third
/// <see cref="ArgumentOutOfRangeException"/>, each before anything is sent.
/// </summary>
public sealed class PublicAdminUndefinedEnumValueTests
{
    private const string SyncMock = nameof(MockAdminClient);
    private const string AsyncMock = nameof(AsyncMockAdminClient);
    private const string SyncReal = nameof(KafkaAdminClient);
    private const string AsyncReal = nameof(AsyncKafkaAdminClient);

    private static readonly string[] s_clients = { SyncMock, AsyncMock, SyncReal, AsyncReal };

    private static readonly int[] s_electionTypes = { -1, 2, 5, 127, 128, 255, 256, 257, int.MaxValue, int.MinValue };

    private static readonly int[] s_isolationLevels = { -1, 2, 7, 128, 255, 256, 513, int.MaxValue, int.MinValue };

    private static readonly int[] s_transactionStates = { -1, 8, 99, int.MaxValue };

    private static readonly Dictionary<TopicPartition, OffsetSpec> s_offsets = new Dictionary<TopicPartition, OffsetSpec>
    {
        [new TopicPartition("undefined-enum", 0)] = OffsetSpec.Latest(),
    };

    public static TheoryData<string, int> ElectionTypes => Rows(s_electionTypes);

    public static TheoryData<string, int> IsolationLevels => Rows(s_isolationLevels);

    public static TheoryData<string, int> TransactionStates => Rows(s_transactionStates);

    [Theory]
    [MemberData(nameof(ElectionTypes))]
    public void ElectLeaders_AnUndefinedElectionType_ThrowsWhatValueOfThrows(string client, int value)
    {
        string expected = "Value " + value.ToString(CultureInfo.InvariantCulture) + " must be one of [PREFERRED, UNCLEAN]";

        OnClient(
            client,
            admin => AssertIllegalArgument(expected, () => admin.ElectLeaders((ElectionType)value, null)),
            admin => AssertIllegalArgument(expected, () => admin.ElectLeaders((ElectionType)value, null)));
    }

    [Theory]
    [MemberData(nameof(IsolationLevels))]
    public void ListOffsets_AnUndefinedIsolationLevel_ThrowsWhatForIdThrows(string client, int value)
    {
        string expected = "Unknown isolation level " + value.ToString(CultureInfo.InvariantCulture);

        OnClient(
            client,
            admin => AssertIllegalArgument(
                expected, () => admin.ListOffsets(s_offsets, new ListOffsetsOptions((IsolationLevel)value))),
            admin => AssertIllegalArgument(
                expected, () => admin.ListOffsets(s_offsets, new ListOffsetsOptions((IsolationLevel)value))));
    }

    [Theory]
    [MemberData(nameof(TransactionStates))]
    public void ListTransactions_AnUndefinedFilterState_IsRejected(string client, int value)
    {
        ListTransactionsOptions options = new ListTransactionsOptions()
            .FilterStates(new[] { TransactionState.ONGOING, (TransactionState)value });
        string expected = new ArgumentOutOfRangeException(
            "options",
            (TransactionState)value,
            "ListTransactionsOptions.FilteredStates must contain only defined TransactionState members.").Message;

        OnClient(
            client,
            admin => AssertOutOfRange(expected, (TransactionState)value, () => admin.ListTransactions(options)),
            admin => AssertOutOfRange(expected, (TransactionState)value, () => admin.ListTransactions(options)));
    }

    private static TheoryData<string, int> Rows(int[] values)
    {
        TheoryData<string, int> rows = new TheoryData<string, int>();
        foreach (string client in s_clients)
        {
            foreach (int value in values)
            {
                rows.Add(client, value);
            }
        }

        return rows;
    }

    private static void OnClient(string client, Action<IAdmin> sync, Action<IAsyncAdmin> async)
    {
        switch (client)
        {
            case SyncMock:
                {
                    MockAdminClient admin = MockAdminClient.Create().Build();
                    using IDisposable adminDisposal = TestTimeout.Disposal(admin);
                    sync(admin);
                    break;
                }

            case AsyncMock:
                {
                    AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
                    using IDisposable adminDisposal = TestTimeout.Disposal(admin);
                    async(admin);
                    break;
                }

            case SyncReal:
                {
                    IAdmin admin = IAdmin.Create(Unreachable());
                    using IDisposable adminDisposal = TestTimeout.Disposal(admin);
                    sync(admin);
                    break;
                }

            default:
                {
                    Assert.Equal(AsyncReal, client);
                    IAsyncAdmin admin = IAsyncAdmin.Create(Unreachable());
                    using IDisposable adminDisposal = TestTimeout.Disposal(admin);
                    async(admin);
                    break;
                }
        }
    }

    private static void AssertIllegalArgument(string expected, Func<object> call) =>
        Assert.Equal(expected, Assert.Throws<IllegalArgumentException>(call).Message);

    private static void AssertOutOfRange(string expected, TransactionState value, Func<object> call)
    {
        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(call);
        Assert.Equal("options", failure.ParamName);
        Assert.Equal(value, failure.ActualValue);
        Assert.Equal(expected, failure.Message);
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?> { ["bootstrap.servers"] = "127.0.0.1:1" };
}
