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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>ListTopics</c>, <c>CreatePartitions</c> and <c>DeleteRecords</c> end to end against the mocks. Java's mock
/// refuses <c>createPartitions</c> and a non-empty <c>deleteRecords</c>, so the tests assert its own message per key;
/// an empty <c>deleteRecords</c> succeeds. With <see cref="NewPartitions"/>' null versus empty assignments and the C
/// options.
/// </summary>
public sealed class PublicAdminListPartitionsRecordsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    [Fact]
    public async Task ListTopics_ReturnsTheCreatedTopics_AndTheThreeProjectionsAgree()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(
                new[] { new NewTopic("p2b-list-a", 1, 1), new NewTopic("p2b-list-b", 2, 1) }).All().Get(),
            s_deadline);

        AsyncListTopicsResult result = admin.ListTopics();

        IReadOnlyDictionary<string, TopicListing> map =
            await TestTimeout.Run(() => result.NamesToListings().Get(), s_deadline);
        IReadOnlyCollection<TopicListing> listings = await TestTimeout.Run(() => result.Listings().Get(), s_deadline);
        IReadOnlyCollection<string> names = await TestTimeout.Run(() => result.Names().Get(), s_deadline);

        Assert.Contains("p2b-list-a", map.Keys);
        Assert.Contains("p2b-list-b", map.Keys);

        Assert.Equal(map.Count, listings.Count);
        Assert.Equal(map.Count, names.Count);
        Assert.Equal(
            map.Keys.OrderBy(name => name, StringComparer.Ordinal),
            names.OrderBy(name => name, StringComparer.Ordinal));
        Assert.Equal(
            map.Values.Select(listing => listing.Name).OrderBy(name => name, StringComparer.Ordinal),
            listings.Select(listing => listing.Name).OrderBy(name => name, StringComparer.Ordinal));

        TopicListing a = map["p2b-list-a"];
        Assert.Equal("p2b-list-a", a.Name);
        Assert.False(a.IsInternal);
        Assert.NotEqual(Uuid.Zero, a.TopicId);
    }

    [Fact]
    public async Task CreatePartitions_AgainstTheMock_FailsPerTopicWithJavasExactMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p2b-grow", 1, 1) }).All().Get(), s_deadline);

        AsyncCreatePartitionsResult result = admin.CreatePartitions(
            new Dictionary<string, NewPartitions> { ["p2b-grow"] = NewPartitions.IncreaseTo(3) });

        Task perTopic = Assert.Single(result.Values).Value.Get();
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => perTopic, s_deadline));

        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => result.All().Get(), s_deadline));
    }

    [Fact]
    public async Task DeleteRecords_WithAnEmptyRequest_SucceedsWithAnEmptyResult()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncDeleteRecordsResult result = admin.DeleteRecords(
            new Dictionary<TopicPartition, RecordsToDelete>());

        Assert.Empty(result.LowWatermarks);

        await TestTimeout.Run(() => result.All().Get(), s_deadline);
        Assert.Equal(TaskStatus.RanToCompletion, result.All().Get().Status);
    }

    [Fact]
    public async Task DeleteRecords_AgainstTheMock_FailsPerPartitionWithJavasExactMessage()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        TopicPartition partition = new TopicPartition("p2b-delete", 0);
        AsyncDeleteRecordsResult result = admin.DeleteRecords(
            new Dictionary<TopicPartition, RecordsToDelete>
            {
                [partition] = RecordsToDelete.BeforeOffset(10),
            });

        Task<DeletedRecords> perPartition = Assert.Single(result.LowWatermarks).Value.Get();
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => perPartition, s_deadline));

        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        Assert.Equal(partition, Assert.Single(result.LowWatermarks).Key);
    }

    [Fact]
    public void NewPartitions_NullAndEmptyAssignments_ProduceDifferentRequests()
    {
        NewPartitions broker = NewPartitions.IncreaseTo(3);
        NewPartitions empty = NewPartitions.IncreaseTo(3, Array.Empty<IReadOnlyList<int>>());
        NewPartitions explicitAssignment = NewPartitions.IncreaseTo(3, new[] { new[] { 0 } });

        Assert.Null(broker.Assignments);
        Assert.NotNull(empty.Assignments);
        Assert.Empty(empty.Assignments!);
        Assert.Single(explicitAssignment.Assignments!);

        Assert.False(NewPartitionsMarshal.HasAssignments(broker));
        Assert.True(NewPartitionsMarshal.HasAssignments(empty));
        Assert.True(NewPartitionsMarshal.HasAssignments(explicitAssignment));

        // Java's increaseTo(n, null) is increaseTo(n): a null list asks the broker to choose, not for no assignments.
        NewPartitions viaNull = NewPartitions.IncreaseTo(3, null);
        Assert.Null(viaNull.Assignments);
        Assert.False(NewPartitionsMarshal.HasAssignments(viaNull));

        Assert.Empty(empty.Assignments!);

        Assert.Equal((3, (IReadOnlyList<IReadOnlyList<int>>?)null), ReadBack(broker));
        Assert.Equal((3, (IReadOnlyList<IReadOnlyList<int>>?)null), ReadBack(viaNull));
        (int emptyCount, IReadOnlyList<IReadOnlyList<int>>? emptySent) = ReadBack(empty);
        Assert.Equal(3, emptyCount);
        Assert.NotNull(emptySent);
        Assert.Empty(emptySent!);
        (int explicitCount, IReadOnlyList<IReadOnlyList<int>>? explicitSent) = ReadBack(explicitAssignment);
        Assert.Equal(3, explicitCount);
        Assert.Equal(new[] { 0 }, Assert.Single(explicitSent!));
        (int orderedCount, IReadOnlyList<IReadOnlyList<int>>? orderedSent) =
            ReadBack(NewPartitions.IncreaseTo(5, new[] { new[] { 2, 1 }, new[] { 3 } }));
        Assert.Equal(5, orderedCount);
        Assert.Equal(2, orderedSent!.Count);
        Assert.Equal(new[] { 2, 1 }, orderedSent[0]);
        Assert.Equal(new[] { 3 }, orderedSent[1]);

        List<IReadOnlyList<int>> mutable = new List<IReadOnlyList<int>> { new[] { 1, 2 } };
        NewPartitions copied = NewPartitions.IncreaseTo(4, mutable);
        mutable.Clear();
        Assert.Equal(new[] { 1, 2 }, Assert.Single(copied.Assignments!));
    }

    [Fact]
    public async Task RepeatedKeys_YieldOneEntry_AndTheFirstEntryIsSent()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        const string Topic = "p2b-dedup";

        Dictionary<string, NewPartitions> grow = new Dictionary<string, NewPartitions>(ReferenceComparer<string>.Instance)
        {
            [Copy(Topic)] = NewPartitions.IncreaseTo(2),
            [Copy(Topic)] = NewPartitions.IncreaseTo(3),
        };
        Assert.Equal(2, grow.Count);
        Assert.Equal(Topic, Assert.Single(admin.CreatePartitions(grow).Values).Key);

        Dictionary<TopicPartition, RecordsToDelete> delete =
            new Dictionary<TopicPartition, RecordsToDelete>(TopicIdentityComparer.Instance)
            {
                [new TopicPartition(Copy(Topic), 0)] = RecordsToDelete.BeforeOffset(1),
                [new TopicPartition(Copy(Topic), 0)] = RecordsToDelete.BeforeOffset(2),
            };
        Assert.Equal(2, delete.Count);
        Assert.Equal(new TopicPartition(Topic, 0), Assert.Single(admin.DeleteRecords(delete).LowWatermarks).Key);

        Dictionary<TopicPartition, OffsetSpec> offsets =
            new Dictionary<TopicPartition, OffsetSpec>(TopicIdentityComparer.Instance)
            {
                [new TopicPartition(Copy(Topic), 0)] = OffsetSpec.ForTimestamp(1),
                [new TopicPartition(Copy(Topic), 0)] = OffsetSpec.Latest(),
            };
        Assert.Equal(2, offsets.Count);
        AsyncListOffsetsResult listed = admin.ListOffsets(offsets);
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => listed.PartitionResult(new TopicPartition(Topic, 0)).Get(), s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal("Not implement yet", failure.Message);
    }

    [Fact]
    public void NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        Assert.Equal((-1, (sbyte)0, (sbyte)1), ReadBack((CreatePartitionsOptions?)null));
        Assert.Equal((-1, (sbyte)0, (sbyte)1), ReadBack(new CreatePartitionsOptions()));
        Assert.Empty(new CreatePartitionsOptions().Setters.InOrder);

        Assert.Equal(-1, ReadBack((DeleteRecordsOptions?)null));
        Assert.Equal(-1, ReadBack(new DeleteRecordsOptions()));
        Assert.Empty(new DeleteRecordsOptions().Setters.InOrder);
    }

    [Fact]
    public void EachSetter_ReachesTheCOptions_AndASetterRunTwiceSendsItsLastValue()
    {
        Assert.Equal((0, (sbyte)0, (sbyte)1), ReadBack(new CreatePartitionsOptions { TimeoutMs = -5 }));
        Assert.Equal((-1, (sbyte)1, (sbyte)1), ReadBack(new CreatePartitionsOptions { ValidateOnly = true }));
        Assert.Equal((-1, (sbyte)0, (sbyte)0), ReadBack(new CreatePartitionsOptions { RetryOnQuotaViolation = false }));

        CreatePartitionsOptions create = new CreatePartitionsOptions
        {
            TimeoutMs = 10,
            ValidateOnly = true,
            RetryOnQuotaViolation = false,
        };
        create.TimeoutMs = 20;
        create.ValidateOnly = false;
        create.RetryOnQuotaViolation = true;
        Assert.Equal((20, (sbyte)0, (sbyte)1), ReadBack(create));
        create.TimeoutMs = null;
        Assert.Equal((-1, (sbyte)0, (sbyte)1), ReadBack(create));

        Assert.Equal(0, ReadBack(new DeleteRecordsOptions { TimeoutMs = -5 }));
        DeleteRecordsOptions delete = new DeleteRecordsOptions { TimeoutMs = 10 };
        delete.TimeoutMs = 30;
        Assert.Equal(30, ReadBack(delete));
        delete.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(delete));
    }

    [Fact]
    public void Preconditions_AreRejectedBeforeAnyNativeCall()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Throws<ArgumentNullException>(() => admin.CreatePartitions(null!));
        Assert.Throws<ArgumentNullException>(() => admin.DeleteRecords(null!));

        ArgumentException nullEntry = Assert.Throws<ArgumentException>(
            () => admin.CreatePartitions(new Dictionary<string, NewPartitions> { ["t"] = null! }));
        Assert.Equal("newPartitions", nullEntry.ParamName);
        Assert.Contains("must not be null", nullEntry.Message, StringComparison.Ordinal);

        ArgumentException nullRecords = Assert.Throws<ArgumentException>(
            () => admin.DeleteRecords(
                new Dictionary<TopicPartition, RecordsToDelete> { [new TopicPartition("t", 0)] = null! }));
        Assert.Equal("recordsToDelete", nullRecords.ParamName);

        // A `default(TopicPartition)` has a null Topic, which the ABI would skip.
        ArgumentException nullTopic = Assert.Throws<ArgumentException>(
            () => admin.DeleteRecords(
                new Dictionary<TopicPartition, RecordsToDelete>
                {
                    [default] = RecordsToDelete.BeforeOffset(1),
                }));
        Assert.Equal("recordsToDelete", nullTopic.ParamName);

        // A negative timeout is not a precondition: it is sent as 0, as Java clamps the deadline.
        foreach (Action negative in new Action[]
                 {
                     () => admin.ListTopics(new ListTopicsOptions { TimeoutMs = -1 }),
                     () => admin.CreatePartitions(
                         new Dictionary<string, NewPartitions>(),
                         new CreatePartitionsOptions { TimeoutMs = -1 }),
                     () => admin.DeleteRecords(
                         new Dictionary<TopicPartition, RecordsToDelete>(),
                         new DeleteRecordsOptions { TimeoutMs = -1 }),
                 })
        {
            Assert.Null(Record.Exception(negative));
        }
    }

    [Fact]
    public void AfterClose_EveryNewRpcThrowsObjectDisposed()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.ListTopics());
        Assert.Throws<ObjectDisposedException>(
            () => admin.CreatePartitions(new Dictionary<string, NewPartitions>()));
        Assert.Throws<ObjectDisposedException>(
            () => admin.DeleteRecords(new Dictionary<TopicPartition, RecordsToDelete>()));
    }

    [Fact]
    public async Task NonAsciiTopic_RoundTripsThroughListTopics()
    {
        const string Topic = "témas-日本語-🎉";

        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }).All().Get(), s_deadline);

        IReadOnlyDictionary<string, TopicListing> map =
            await TestTimeout.Run(() => admin.ListTopics().NamesToListings().Get(), s_deadline);

        Assert.True(map.ContainsKey(Topic));
        Assert.Equal(Topic, map[Topic].Name);
    }

    [Fact]
    public async Task TfmSmoke_TheThreeNewRpcsWorkOnThisFramework()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p2b-smoke", 1, 1) }).All().Get(), s_deadline);

        Assert.NotEmpty(await TestTimeout.Run(() => admin.ListTopics().Names().Get(), s_deadline));

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(
                () => admin.CreatePartitions(
                    new Dictionary<string, NewPartitions> { ["p2b-smoke"] = NewPartitions.IncreaseTo(2) }).All().Get(),
                s_deadline));

        await TestTimeout.Run(
            () => admin.DeleteRecords(new Dictionary<TopicPartition, RecordsToDelete>()).All().Get(), s_deadline);
    }

    private static (int TotalCount, IReadOnlyList<IReadOnlyList<int>>? Assignments) ReadBack(NewPartitions partitions)
    {
        IntPtr native = NewPartitionsMarshal.Build(partitions);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int totalCount = Native.NewPartitionsTotalCount(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr outer = Native.NewPartitionsAssignments(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (outer == IntPtr.Zero)
            {
                return (totalCount, null);
            }

            try
            {
                int size = NativeMethods.ListSize(outer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                List<IReadOnlyList<int>> assignments = new List<IReadOnlyList<int>>(size);
                for (int i = 0; i < size; i++)
                {
                    IntPtr brokers = NativeMethods.ListGet(outer, i, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    int count = NativeMethods.ListSize(brokers, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    int[] ids = new int[count];
                    for (int j = 0; j < count; j++)
                    {
                        IntPtr id = NativeMethods.ListGet(brokers, j, ref outErr);
                        KafkaException.ThrowIfFfiFailed(ref outErr);
                        ids[j] = Marshal.ReadInt32(id);
                    }

                    assignments.Add(ids);
                }

                return (totalCount, assignments);
            }
            finally
            {
                IntPtr destroyErr = IntPtr.Zero;
                NativeMethods.ListDestroy(outer, ref destroyErr);
                KafkaException.ThrowIfFfiFailed(ref destroyErr);
            }
        }
        finally
        {
            NewPartitionsMarshal.Destroy(native);
        }
    }

    private static (int TimeoutMs, sbyte ValidateOnly, sbyte RetryOnQuotaViolation) ReadBack(
        CreatePartitionsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewCreatePartitionsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.CreatePartitionsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte validateOnly = Native.CreatePartitionsOptionsValidateOnly(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte retry = Native.CreatePartitionsOptionsShouldRetryOnQuotaViolation(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, validateOnly, retry);
        }
        finally
        {
            NativeAdminClient.FreeCreatePartitionsOptions(native);
        }
    }

    private static int ReadBack(DeleteRecordsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDeleteRecordsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.DeleteRecordsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeDeleteRecordsOptions(native);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string Copy(string value) => new string(value.ToCharArray());

    /// <summary>Reference equality, so two equal strings stay two keys.</summary>
    private sealed class ReferenceComparer<T> : IEqualityComparer<T>
        where T : class
    {
        public static readonly ReferenceComparer<T> Instance = new ReferenceComparer<T>();

        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);

        public int GetHashCode(T obj) => RuntimeHelpers.GetHashCode(obj);
    }

    /// <summary>Compares the topic by string identity, so two equal partitions stay two keys.</summary>
    private sealed class TopicIdentityComparer : IEqualityComparer<TopicPartition>
    {
        public static readonly TopicIdentityComparer Instance = new TopicIdentityComparer();

        public bool Equals(TopicPartition x, TopicPartition y) =>
            ReferenceEquals(x.Topic, y.Topic) && x.Partition == y.Partition;

        public int GetHashCode(TopicPartition obj) => RuntimeHelpers.GetHashCode(obj.Topic) ^ obj.Partition;
    }

    /// <summary>The getters the binding never reads, declared for the read-back tests.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitions_total_count_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int NewPartitionsTotalCount(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_NewPartitions_assignments_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr NewPartitionsAssignments(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int CreatePartitionsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsOptions_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte CreatePartitionsOptionsValidateOnly(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_CreatePartitionsOptions_should_retry_on_quota_violation_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte CreatePartitionsOptionsShouldRetryOnQuotaViolation(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DeleteRecordsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DeleteRecordsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);
    }
}
