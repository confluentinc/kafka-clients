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
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Pins <b>which ABI accessor</b> each composite-key and optional-error reader is built
/// on — the one axis of <c>electLeaders</c>' walk that no behavioural test can reach.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>Why this file exists (M15/P4 round 1, finding 70.2).</b> <c>electLeaders</c>'
/// readers are only executed on the success branch of a real
/// <c>kafka_admin_ElectLeadersResult_t</c>, and none is obtainable without a broker:
/// Java's <c>MockAdminClient.electLeaders</c> throws
/// <c>UnsupportedOperationException("Not implemented yet")</c>
/// (<c>MockAdminClient.java:797</c>) and the core mirrors that faithfully. Measured on the
/// shipped commit: replacing <c>AdminCallbacks.ElectLeadersKey</c> with a constant, and
/// swapping its value reader's <c>FromBorrowedHandle</c> for <c>FromHandle</c>, each left
/// the suite at <b>1231/1231 green</b>.
/// </para>
/// <para>
/// The reader <em>bodies</em> are now shared factories
/// (<see cref="AdminCallbacks.TopicPartitionKey"/>,
/// <see cref="AdminCallbacks.BorrowedOptionalError"/>) that <c>deleteRecords</c> and
/// <c>alterPartitionReassignments</c> drive over real result roots, so a defect in a body
/// is caught there. What remains unreachable is the <b>wiring</b>: that
/// <c>electLeaders</c>' instances capture <em>its</em> accessors and not another RPC's.
/// This file reads that off the delegate itself.
/// </para>
/// <para>
/// ⚠⚠ <b>EVERY reader built on a shared factory must appear here, and the obligation is
/// now a pre-write checklist item (PLAN §6.1 item 9) rather than a review finding.</b>
/// M15/P4 Stage 2 added two <see cref="AdminCallbacks.TopicPartitionKey"/> readers —
/// <c>ListPartitionReassignmentsKey</c> and <c>ListOffsetsKey</c> — and did not extend
/// this file, which reopened the exact gap it was written to close (finding 70.12).
/// Measured before the fix: cross-wiring <c>ListOffsetsKey</c> to <c>deleteRecords</c>'
/// accessors left the suite at <b>1297/1297 green</b>, because the sibling accessors are
/// layout-compatible — the wrong call returns a plausible answer. A shared factory makes
/// that mistake <em>easier</em> to write, not harder, which is why the guard is
/// per-reader and not per-factory.
/// </para>
/// <para>
/// ⚠ <b>The accessors are identified by their ABI <c>EntryPoint</c>, not by their C#
/// name</b> — the entry point is the contract with the core, and a C# rename that kept the
/// wrong symbol would still be a defect. Two properties are asserted per reader: the exact
/// set of symbols it captures, and — across readers — that no two RPCs capture the same
/// set, which is what a copy-paste wiring error produces.
/// </para>
/// <para>
/// ⚠ <b>Reading captured state is deliberate, and it is guarded against becoming vacuous
/// by TWO different mechanisms — which one fires depends on the refactor, and the
/// difference was measured, not reasoned about.</b> A factory-built reader closes over its
/// accessors, so its <see cref="Delegate.Target"/> is a display class whose fields hold
/// them. Probed on net10.0:
/// </para>
/// <list type="table">
/// <item>
/// <term>factory-built closure (the shipped shape)</term>
/// <description><c>&lt;&gt;c__DisplayClass…</c> — the fields hold the accessors.</description>
/// </item>
/// <item>
/// <term>the factory inlined back into a <c>static</c> lambda</term>
/// <description>
/// <c>&lt;&gt;c</c> — <b>NOT <see langword="null"/></b>. A non-capturing lambda is cached
/// on the compiler's singleton, so <see cref="CapturedEntryPoints"/> reaches the field
/// scan and finds no <see cref="Delegate"/>; <b><see cref="Assert.NotEmpty{T}"/> is what
/// fails</b>. Measured by mutation: constant-folding <c>ElectLeadersKey</c> to a
/// <c>static</c> lambda gives <b>2 RED</b>, both "Collection was empty" — and the throw
/// below fired <b>0</b> times.
/// </description>
/// </item>
/// <item>
/// <term>a plain <c>static</c> method group</term>
/// <description>
/// <see langword="null"/> — and <em>this</em> is the shape the <c>?? throw</c> in
/// <see cref="CapturedEntryPoints"/> catches, so that clause is reachable rather than
/// dead. Measured the same way, as the exact complement: rebinding
/// <c>ElectLeadersKey</c> to a static method group gives <b>2 RED</b>, both the throw's
/// own message — and "Collection was empty" fired <b>0</b> times.
/// </description>
/// </item>
/// </list>
/// <para>
/// ⚠ An earlier version of this remark credited the <c>?? throw</c> with catching the
/// static-lambda refactor. That was an unmeasured danger model in a code comment — PLAN
/// §6.1 item 7 — and it was false: the guard held, but by the other mechanism. Both are
/// kept, and they are exhaustive <em>structurally</em> rather than by enumeration: a
/// reader that captures no P/Invoke delegate has either a null
/// <see cref="Delegate.Target"/>, and throws, or a non-null one whose fields yield an
/// empty set, and fails the assertion. The table above is which branch the three known
/// shapes take, not the argument that there are only three.
/// </para>
/// </remarks>
public sealed class AdminP4ReaderWiringTests
{
    /// <summary>
    /// Each composite-key reader captures <b>its own</b> result type's
    /// <c>get_topic</c> / <c>get_partition</c> pair.
    /// </summary>
    /// <remarks>
    /// The three share one body, so this is the assertion that keeps them distinguishable:
    /// pointing <c>electLeaders</c>' reader at <c>deleteRecords</c>' accessors would be
    /// invisible to every behavioural test and is caught here.
    /// </remarks>
    [Theory]
    [InlineData(
        nameof(AdminCallbacks.ElectLeadersKey),
        "kafka_admin_ElectLeadersResult_get_topic",
        "kafka_admin_ElectLeadersResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.AlterPartitionReassignmentsKey),
        "kafka_admin_AlterPartitionReassignmentsResult_get_topic",
        "kafka_admin_AlterPartitionReassignmentsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.DeleteRecordsKey),
        "kafka_admin_DeleteRecordsResult_get_topic",
        "kafka_admin_DeleteRecordsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.ListPartitionReassignmentsKey),
        "kafka_admin_ListPartitionReassignmentsResult_get_topic",
        "kafka_admin_ListPartitionReassignmentsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.ListOffsetsKey),
        "kafka_admin_ListOffsetsResult_get_topic",
        "kafka_admin_ListOffsetsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.AlterConsumerGroupOffsetsKey),
        "kafka_admin_AlterConsumerGroupOffsetsResult_get_topic",
        "kafka_admin_AlterConsumerGroupOffsetsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.DeleteConsumerGroupOffsetsKey),
        "kafka_admin_DeleteConsumerGroupOffsetsResult_get_topic",
        "kafka_admin_DeleteConsumerGroupOffsetsResult_get_partition")]
    public void EachCompositeKeyReader_CapturesItsOwnAccessors(
        string readerName, string getTopic, string getPartition) =>
        Assert.Equal(
            new[] { getPartition, getTopic }.OrderBy(name => name, StringComparer.Ordinal),
            CapturedEntryPoints(Reader(readerName)));

    /// <summary>
    /// <c>electLeaders</c>' optional-error <b>value</b> reader captures
    /// <c>kafka_admin_ElectLeadersResult_get_error</c> — its own, not the byte-identical
    /// twin's.
    /// </summary>
    [Fact]
    public void ElectLeadersOptionalError_CapturesElectLeadersOwnErrorAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_ElectLeadersResult_get_error" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.ElectLeadersOptionalError))));

    /// <summary>
    /// <c>alterConsumerGroupOffsets</c>' optional-error <b>value</b> reader captures
    /// <c>kafka_admin_AlterConsumerGroupOffsetsResult_get_error</c> — its own, not the
    /// byte-identical twin's.
    /// </summary>
    [Fact]
    public void AlterConsumerGroupOffsetsOptionalError_CapturesItsOwnErrorAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_AlterConsumerGroupOffsetsResult_get_error" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.AlterConsumerGroupOffsetsOptionalError))));

    /// <summary>
    /// <c>deleteConsumerGroupOffsets</c>' optional-error <b>value</b> reader captures
    /// <c>kafka_admin_DeleteConsumerGroupOffsetsResult_get_error</c> — its own, not the
    /// byte-identical twin's.
    /// </summary>
    [Fact]
    public void DeleteConsumerGroupOffsetsOptionalError_CapturesItsOwnErrorAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_DeleteConsumerGroupOffsetsResult_get_error" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.DeleteConsumerGroupOffsetsOptionalError))));

    /// <summary>
    /// <c>removeMembersFromConsumerGroup</c>' optional-error <b>value</b> reader captures
    /// <c>kafka_admin_RemoveMembersFromConsumerGroupResult_get_error</c> — its own, not the
    /// byte-identical twin's.
    /// </summary>
    [Fact]
    public void RemoveMembersFromConsumerGroupOptionalError_CapturesItsOwnErrorAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_RemoveMembersFromConsumerGroupResult_get_error" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.RemoveMembersFromConsumerGroupOptionalError))));

    /// <summary>
    /// No two of the factory-built readers capture the same accessors — the control that
    /// makes the per-reader assertions above more than three restatements of one source
    /// line.
    /// </summary>
    /// <remarks>
    /// A copy-paste wiring error produces exactly this collision, and it is the failure the
    /// shared factory makes <em>easier</em> to write than the three separate lambdas did.
    /// </remarks>
    [Fact]
    public void NoTwoFactoryBuiltReaders_CaptureTheSameAccessors()
    {
        string[] readers =
        {
            nameof(AdminCallbacks.ElectLeadersKey),
            nameof(AdminCallbacks.AlterPartitionReassignmentsKey),
            nameof(AdminCallbacks.DeleteRecordsKey),
            nameof(AdminCallbacks.ListPartitionReassignmentsKey),
            nameof(AdminCallbacks.ListOffsetsKey),
            nameof(AdminCallbacks.ElectLeadersOptionalError),
            nameof(AdminCallbacks.AlterConsumerGroupOffsetsKey),
            nameof(AdminCallbacks.AlterConsumerGroupOffsetsOptionalError),
            nameof(AdminCallbacks.DeleteConsumerGroupOffsetsKey),
            nameof(AdminCallbacks.DeleteConsumerGroupOffsetsOptionalError),
            nameof(AdminCallbacks.RemoveMembersFromConsumerGroupOptionalError),
        };

        List<string> signatures = readers
            .Select(name => string.Join("|", CapturedEntryPoints(Reader(name))))
            .ToList();

        // Control-positive: every reader really did yield a non-empty signature, so a
        // decoder that silently returned nothing could not make this pass.
        Assert.All(signatures, signature => Assert.NotEqual(string.Empty, signature));
        Assert.Equal(readers.Length, signatures.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// ⚠⚠ <b>The tracked set is COMPLETE: every factory-built reader on
    /// <see cref="AdminCallbacks"/> is covered by the assertions above.</b> This is
    /// PLAN §6.1 item 9 enforced mechanically rather than by checklist.
    /// </summary>
    /// <remarks>
    /// <para>
    /// M15/P4 Stage 2 added two readers and did not extend this file; the gap was found in
    /// review (70.12), not by the suite. A checklist prevents that only if it is read. This
    /// discovers the readers instead: any <see cref="AdminCallbacks"/> field holding a
    /// delegate that <em>closes over</em> at least one <c>DllImport</c> is factory-built by
    /// construction, because that capture is exactly what the factories do — so a new
    /// reader added without a row here turns this red on its first run.
    /// </para>
    /// <para>
    /// ⚠ Hand-written <c>static</c> lambdas — <c>TopicListingValue</c>,
    /// <c>PartitionReassignmentValue</c> and their kin — capture nothing and are correctly
    /// out of scope: their wiring is visible in one line at the field, which is the
    /// property the factories removed and this file restores.
    /// </para>
    /// </remarks>
    [Fact]
    public void TheTrackedSet_CoversEveryFactoryBuiltReader()
    {
        string[] discovered = typeof(AdminCallbacks)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(field => typeof(Delegate).IsAssignableFrom(field.FieldType))
            .Where(field => CapturesAnyImport((Delegate?)field.GetValue(null)))
            .Select(field => field.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        // Control-positive: the discovery really finds delegates, so an empty result
        // could not make this pass vacuously.
        Assert.NotEmpty(discovered);

        Assert.Equal(
            new[]
            {
                nameof(AdminCallbacks.AlterConsumerGroupOffsetsKey),
                nameof(AdminCallbacks.AlterConsumerGroupOffsetsOptionalError),
                nameof(AdminCallbacks.AlterPartitionReassignmentsKey),
                nameof(AdminCallbacks.DeleteConsumerGroupOffsetsKey),
                nameof(AdminCallbacks.DeleteConsumerGroupOffsetsOptionalError),
                nameof(AdminCallbacks.DeleteRecordsKey),
                nameof(AdminCallbacks.ElectLeadersKey),
                nameof(AdminCallbacks.ElectLeadersOptionalError),
                nameof(AdminCallbacks.ListOffsetsKey),
                nameof(AdminCallbacks.ListPartitionReassignmentsKey),
                nameof(AdminCallbacks.RemoveMembersFromConsumerGroupOptionalError),
            },
            discovered);
    }

    /// <summary>
    /// Whether a delegate closes over at least one <c>DllImport</c> — the signature of a
    /// factory-built reader.
    /// </summary>
    private static bool CapturesAnyImport(Delegate? candidate)
    {
        object? target = candidate?.Target;
        if (target is null)
        {
            return false;
        }

        return target.GetType()
            .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Select(field => field.GetValue(target))
            .OfType<Delegate>()
            .Any(captured => captured.Method.GetCustomAttribute<DllImportAttribute>() is not null);
    }

    /// <summary>
    /// The ABI <c>EntryPoint</c>s of every P/Invoke a reader closes over, sorted.
    /// </summary>
    /// <exception cref="Xunit.Sdk.XunitException">
    /// The reader captures nothing. Thrown explicitly for a <c>static</c> <b>method
    /// group</b> (<see cref="Delegate.Target"/> is <see langword="null"/>); a
    /// non-capturing <b>lambda</b> instead reaches <see cref="Assert.NotEmpty{T}"/> below,
    /// because its target is the compiler's <c>&lt;&gt;c</c> singleton rather than
    /// <see langword="null"/>. See the measured table in the type remarks.
    /// </exception>
    private static string[] CapturedEntryPoints(Delegate reader)
    {
        // Null target == a static method group. A non-capturing LAMBDA does not land here —
        // its target is <>c — and is caught by the Assert.NotEmpty below instead.
        object target = reader.Target
            ?? throw new Xunit.Sdk.XunitException(
                "the reader is a static method group, so it captures nothing and its wiring is "
                + "no longer readable here — this file needs a different mechanism, not deleting");

        string[] entryPoints = target.GetType()
            .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Select(field => field.GetValue(target))
            .OfType<Delegate>()
            .Select(captured => captured.Method.GetCustomAttribute<DllImportAttribute>()?.EntryPoint)
            .Where(entryPoint => entryPoint is not null)
            .Select(entryPoint => entryPoint!)
            .OrderBy(entryPoint => entryPoint, StringComparer.Ordinal)
            .ToArray();

        // ⚠ THE mechanism for a non-capturing lambda — see the type remarks' measured table.
        Assert.NotEmpty(entryPoints);
        return entryPoints;
    }

    private static Delegate Reader(string fieldName) =>
        (Delegate)typeof(AdminCallbacks)
            .GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;
}
