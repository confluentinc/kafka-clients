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
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The end-to-end behaviour of M15/P5's RPC 4.6 (<c>alterConsumerGroupOffsets</c>) against
/// <see cref="MockAdminClient"/> — no broker.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The mock has no success path, and that is FAITHFUL, not a gap.</b> Java's
/// <c>MockAdminClient.alterConsumerGroupOffsets</c> throws
/// <c>UnsupportedOperationException("Not implement yet")</c> (Java's own typo —
/// <c>MockAdminClient.java:1213</c>), and the core surfaces that as a resolved map whose
/// <b>every requested partition</b> carries the identical "unsupported" error — Java's
/// <c>Map&lt;TopicPartition, Errors&gt;</c> has no top-level-fault channel of its own, so a
/// call-level refusal is expressed as "every partition failed the same way" rather than as
/// a faulted aggregate <see cref="System.Threading.Tasks.Task"/> (unlike
/// <see cref="ElectLeadersResult"/>, whose mock refusal below <b>does</b> fault the
/// call-level <see cref="System.Threading.Tasks.Task"/> — a different Java return-type
/// shape, per <c>AdminCallbacks.OnAlterConsumerGroupOffsets</c>'s remarks). So
/// <see cref="AlterConsumerGroupOffsetsResult.PartitionResult(TopicPartition)"/> throws the
/// per-partition "Not implement yet" error verbatim, while
/// <see cref="AlterConsumerGroupOffsetsResult.All"/> throws its own <b>aggregate</b> message
/// (carrying the same code) — asserted below.
/// </para>
/// <para>
/// The map-value semantics are additionally tested against the public type directly, with a
/// map the test supplies — the same accommodation
/// <c>PublicAdminElectionsReassignmentsTests.ElectLeadersResult_APerPartitionFailureIsAValue_AndAllReportsTheFirst</c>
/// makes for <see cref="ElectLeadersResult"/>, which shares this result's shape.
/// </para>
/// </remarks>
public sealed class PublicAdminAlterConsumerGroupOffsetsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code Kafka assigns to <c>UNSUPPORTED_VERSION</c>.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>The code Kafka assigns to <c>GROUP_AUTHORIZATION_FAILED</c>.</summary>
    private const int GroupAuthorizationFailedCode = 30;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws (with its own typo) and the
    /// Rust mock translates verbatim.
    /// </summary>
    private const string NotImplemented = "Not implement yet";

    /// <summary>
    /// <c>alterConsumerGroupOffsets</c> reaches the core and its single awaitable resolves
    /// to a map where the requested partition carries the mock's documented refusal —
    /// verbatim from <see cref="AlterConsumerGroupOffsetsResult.PartitionResult(TopicPartition)"/>,
    /// aggregated (same code, own message) from <see cref="AlterConsumerGroupOffsetsResult.All"/>.
    /// </summary>
    [Fact]
    public async Task AlterConsumerGroupOffsets_SurfacesTheMocksDocumentedRefusal()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        TopicPartition tp = new TopicPartition("p5-acgo", 0);
        AlterConsumerGroupOffsetsResult result = admin.AlterConsumerGroupOffsets(
            "p5-group",
            new Dictionary<TopicPartition, OffsetAndMetadata> { [tp] = new OffsetAndMetadata(10) });

        KafkaException fromPartitionResult = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(tp)), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromPartitionResult.Code);
        Assert.Equal(NotImplemented, fromPartitionResult.Message);

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Contains(tp.ToString(), fromAll.Message, StringComparison.Ordinal);
    }

    /// <summary>A closed client rejects the call before reaching the core.</summary>
    [Fact]
    public async Task AlterConsumerGroupOffsets_ThrowsAfterDispose()
    {
        MockAdminClient admin = new MockAdminClient(1);
        await admin.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => admin.AlterConsumerGroupOffsets(
            "p5-group",
            new Dictionary<TopicPartition, OffsetAndMetadata>
            {
                [new TopicPartition("p5-disposed", 0)] = new OffsetAndMetadata(0),
            }));
    }

    /// <summary>
    /// ⚠⚠ <b>A per-partition failure is a map VALUE on a SUCCESSFUL task</b> — Java's
    /// <c>Map&lt;TopicPartition, Errors&gt;</c> (<c>AlterConsumerGroupOffsetsResult.java:33</c>)
    /// — and <see cref="AlterConsumerGroupOffsetsResult.PartitionResult"/> /
    /// <see cref="AlterConsumerGroupOffsetsResult.All"/> are what turn a non-null entry into
    /// a fault.
    /// </summary>
    [Fact]
    public async Task PartitionResult_SucceedsOnNull_AndThrowsThePartitionsOwnError()
    {
        TopicPartition good = new TopicPartition("p5-partition", 0);
        TopicPartition bad = new TopicPartition("p5-partition", 1);

        KafkaException error = new KafkaException(11, "partition failure", isRetriable: false);
        Dictionary<TopicPartition, KafkaException?> outcomes = new Dictionary<TopicPartition, KafkaException?>
        {
            [good] = null,
            [bad] = error,
        };

        AlterConsumerGroupOffsetsResult result = new AlterConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(outcomes));

        await TestTimeout.Run(() => result.PartitionResult(good), s_deadline);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(bad)), s_deadline);
        Assert.Same(error, thrown);
    }

    /// <summary>
    /// A partition never named in the request faults with Java's exact
    /// <c>IllegalArgumentException</c> message, translated to <see cref="ArgumentException"/>
    /// (<c>AlterConsumerGroupOffsetsResult.java:44-46</c>-equivalent wording).
    /// </summary>
    [Fact]
    public async Task PartitionResult_UnknownPartition_ThrowsWithJavasExactMessage()
    {
        TopicPartition known = new TopicPartition("p5-known", 0);
        TopicPartition unknown = new TopicPartition("p5-unknown", 7);

        AlterConsumerGroupOffsetsResult result = new AlterConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(
                new Dictionary<TopicPartition, KafkaException?> { [known] = null }));

        ArgumentException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<ArgumentException>(() => result.PartitionResult(unknown)), s_deadline);
        Assert.Equal(
            "Alter offset for partition \"" + unknown + "\" was not attempted",
            thrown.Message);
    }

    /// <summary>
    /// <see cref="AlterConsumerGroupOffsetsResult.All"/> collects <b>every</b> failed
    /// partition into its message (unlike <see cref="ElectLeadersResult.All"/>, which
    /// reports only the first) while the thrown exception's code/retriable flag come from
    /// the <b>first</b> failure encountered.
    /// </summary>
    [Fact]
    public async Task All_ListsEveryFailedPartition_AndCarriesTheFirstFailuresCode()
    {
        TopicPartition good = new TopicPartition("p5-all", 0);
        TopicPartition bad = new TopicPartition("p5-all", 1);
        TopicPartition worse = new TopicPartition("p5-all", 2);

        KafkaException first = new KafkaException(11, "first failure", isRetriable: true);
        KafkaException second = new KafkaException(12, "second failure", isRetriable: false);

        Dictionary<TopicPartition, KafkaException?> outcomes = new Dictionary<TopicPartition, KafkaException?>
        {
            [good] = null,
            [bad] = first,
            [worse] = second,
        };

        AlterConsumerGroupOffsetsResult mixed = new AlterConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(outcomes));

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(mixed.All), s_deadline);

        Assert.Equal(first.Code, thrown.Code);
        Assert.Equal(first.IsRetriable, thrown.IsRetriable);
        Assert.Contains(bad.ToString(), thrown.Message, StringComparison.Ordinal);
        Assert.Contains(worse.ToString(), thrown.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(good.ToString(), thrown.Message, StringComparison.Ordinal);

        AlterConsumerGroupOffsetsResult clean = new AlterConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(
                new Dictionary<TopicPartition, KafkaException?> { [good] = null }));
        await TestTimeout.Run(clean.All, s_deadline);
    }

    /// <summary>
    /// <see cref="AlterConsumerGroupOffsetsResult.All"/> keeps the classification the core
    /// gave the first failure. The failure is built from a core error handle,
    /// <c>kafka_common_Error_new(30, message)</c> (<c>GROUP_AUTHORIZATION_FAILED</c>), and
    /// mapped through <c>KafkaException.FromHandle</c>, as production maps each partition's
    /// owned error (<c>AdminCallbacks.CompletePerKeyFanIn</c>, which the
    /// <c>alterConsumerGroupOffsets</c> trampoline <c>OnAlterConsumerGroupOffsets</c> calls).
    /// The test first asserts the core's answers for that failure: code 30,
    /// <see cref="KafkaException.IsInvalidConfigurationError"/> and
    /// <see cref="KafkaException.IsAuthorizationError"/> true, and
    /// <see cref="KafkaException.IsRetriable"/> and the other hierarchy predicates false.
    /// It then asserts that the exception <see cref="AlterConsumerGroupOffsetsResult.All"/>
    /// throws has code 30, the aggregate message naming the failed partition, and the
    /// failure's flags.
    /// </summary>
    [Fact]
    public async Task All_KeepsTheFirstFailuresCoreClassification()
    {
        TopicPartition good = new TopicPartition("p5-classified", 0);
        TopicPartition bad = new TopicPartition("p5-classified", 1);

        KafkaException failure = FromNewError(GroupAuthorizationFailedCode, "group authorization failed");
        Assert.Equal(GroupAuthorizationFailedCode, failure.Code);
        Assert.Equal(
            Flags(
                isRetriable: false,
                isTransactionAbortableError: false,
                isApplicationRecoverableError: false,
                isInvalidConfigurationError: true,
                isAuthorizationError: true,
                isOutOfOrderSequenceError: false),
            Flags(failure));

        AlterConsumerGroupOffsetsResult result = new AlterConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(
                new Dictionary<TopicPartition, KafkaException?> { [good] = null, [bad] = failure }));

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);

        Assert.Equal(GroupAuthorizationFailedCode, thrown.Code);
        Assert.Equal(
            "Failed altering group offsets for the following partitions: [" + bad + "]",
            thrown.Message);
        Assert.Equal(Flags(failure), Flags(thrown));
    }

    /// <summary>
    /// <see cref="AlterConsumerGroupOffsetsResult.All"/> passes each flag of the first
    /// failure to the same parameter of the rebuilt exception. The flag parameters of the
    /// internal constructor
    /// <c>KafkaException(int, string?, bool, bool, bool, bool, bool, bool, Exception?)</c>
    /// are numbered in order: <c>isRetriable</c> 1, <c>isTransactionAbortableError</c> 2,
    /// <c>isApplicationRecoverableError</c> 3,
    /// <c>isInvalidConfigurationError</c> 4, <c>isAuthorizationError</c> 5 and
    /// <c>isOutOfOrderSequenceError</c> 6. In row <paramref name="bit"/>, one failure sets
    /// each flag to that bit of its number, and another failure, with another code, sets
    /// each flag to the opposite value. The flags are set directly, with no relation to
    /// the code. The test asserts that the thrown exception's code and flags equal those
    /// of the failure the map enumerates first. Two different numbers differ in some bit,
    /// so for any two flag parameters some row sets them differently, and a rebuild that
    /// passes one flag in another's parameter fails that row. A code or flag taken from the
    /// other failure fails every row.
    /// </summary>
    /// <param name="bit">Which bit of each flag's number the first failure uses.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task All_PassesEachFlagOfTheFirstFailureToItsOwnParameter(int bit)
    {
        TopicPartition good = new TopicPartition("p5-flags", 0);
        TopicPartition bad = new TopicPartition("p5-flags", 1);
        TopicPartition worse = new TopicPartition("p5-flags", 2);

        Dictionary<TopicPartition, KafkaException?> outcomes = new Dictionary<TopicPartition, KafkaException?>
        {
            [good] = null,
            [bad] = WithFlags(11, bit, opposite: false),
            [worse] = WithFlags(12, bit, opposite: true),
        };
        KafkaException first = outcomes.First(entry => entry.Value is not null).Value!;

        AlterConsumerGroupOffsetsResult result = new AlterConsumerGroupOffsetsResult(
            Task.FromResult<IReadOnlyDictionary<TopicPartition, KafkaException?>>(outcomes));

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);

        Assert.Equal(first.Code, thrown.Code);
        Assert.Equal(Flags(first), Flags(thrown));
    }

    /// <summary>A call-level failure of the aggregate future propagates from both accessors.</summary>
    [Fact]
    public async Task AFaultedFuture_PropagatesFromBothAccessors()
    {
        KafkaException callLevel = new KafkaException(35, "call failed", isRetriable: false);
        TaskCompletionSource<IReadOnlyDictionary<TopicPartition, KafkaException?>> source =
            new TaskCompletionSource<IReadOnlyDictionary<TopicPartition, KafkaException?>>();
        source.SetException(callLevel);

        AlterConsumerGroupOffsetsResult result = new AlterConsumerGroupOffsetsResult(source.Task);

        KafkaException fromPartitionResult = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(
                () => result.PartitionResult(new TopicPartition("p5-fault", 0))),
            s_deadline);
        Assert.Same(callLevel, fromPartitionResult);

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Same(callLevel, fromAll);
    }

    /// <summary>
    /// Builds an owned error with <c>kafka_common_Error_new</c> and maps it through
    /// <c>KafkaException.FromHandle</c>, which reads every value out with
    /// <c>KafkaException.FromBorrowedHandle</c> and then frees the handle. The core copies
    /// the message, so its pin may end after the call.
    /// </summary>
    private static KafkaException FromNewError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);

        KafkaException? ex = KafkaException.FromHandle(error);
        Assert.NotNull(ex);
        return ex!;
    }

    /// <summary>
    /// A failure carrying <paramref name="code"/> in which the flag numbered <c>n</c> (the
    /// numbering of <see cref="All_PassesEachFlagOfTheFirstFailureToItsOwnParameter(int)"/>)
    /// is bit <paramref name="bit"/> of <c>n</c>, inverted when
    /// <paramref name="opposite"/> is set. The constructor it calls keeps each flag as
    /// given.
    /// </summary>
    private static KafkaException WithFlags(int code, int bit, bool opposite)
    {
        bool Flag(int number) => (((number >> bit) & 1) == 1) != opposite;

        return new KafkaException(
            code,
            "flag pattern",
            isRetriable: Flag(1),
            isTransactionAbortableError: Flag(2),
            isApplicationRecoverableError: Flag(3),
            isInvalidConfigurationError: Flag(4),
            isAuthorizationError: Flag(5),
            isOutOfOrderSequenceError: Flag(6));
    }

    /// <summary>
    /// The retriable flag and the hierarchy predicates of <paramref name="ex"/>, each named,
    /// in the order of the constructor's parameters.
    /// </summary>
    private static string Flags(KafkaException ex) =>
        Flags(
            ex.IsRetriable,
            ex.IsTransactionAbortableError,
            ex.IsApplicationRecoverableError,
            ex.IsInvalidConfigurationError,
            ex.IsAuthorizationError,
            ex.IsOutOfOrderSequenceError);

    private static string Flags(
        bool isRetriable,
        bool isTransactionAbortableError,
        bool isApplicationRecoverableError,
        bool isInvalidConfigurationError,
        bool isAuthorizationError,
        bool isOutOfOrderSequenceError) =>
        string.Format(
            CultureInfo.InvariantCulture,
            "IsRetriable={0} IsTransactionAbortableError={1} IsApplicationRecoverableError={2} "
                + "IsInvalidConfigurationError={3} IsAuthorizationError={4} IsOutOfOrderSequenceError={5}",
            isRetriable,
            isTransactionAbortableError,
            isApplicationRecoverableError,
            isInvalidConfigurationError,
            isAuthorizationError,
            isOutOfOrderSequenceError);
}
