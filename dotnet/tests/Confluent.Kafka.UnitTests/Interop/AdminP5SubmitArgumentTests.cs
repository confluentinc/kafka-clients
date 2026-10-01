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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What <see cref="ListGroupsOptions"/> actually becomes at the <c>list_groups_async</c>
/// P/Invoke — the M15/P5 twin of <see cref="AdminP3SubmitArgumentTests"/>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The three filters are invisible to a behavioural test.</b> The Rust
/// <c>MockAdminClient</c> ignores the options it is handed —
/// <c>fn list_groups(&amp;self, _options: ListGroupsOptions)</c> — so it answers a
/// filtered call and an unfiltered one identically. Their values are therefore read here,
/// at the seam, where they are facts rather than inferences; the same reason the P3 file
/// gives for <c>DescribeCluster</c>'s booleans.
/// </para>
/// <para>
/// ⚠⚠ <b>The defect this file exists to catch is a shared count.</b> The submit takes
/// <em>three</em> name arrays that are <b>not parallel</b> — three independent filter
/// axes whose lengths are unrelated — so every axis must travel with its own count and its
/// own array. Sizing one axis from another's count silently narrows or widens the request,
/// and because the mock ignores the filters entirely, nothing downstream would notice. So
/// every case here drives axes of <b>different</b> lengths, and asserts each axis's count
/// <em>and</em> its decoded contents, rather than sampling one.
/// </para>
/// <para>
/// ⚠ <b>The names are decoded inside the submit, not after it.</b> Production pins every
/// name for the call only and unpins in its <c>finally</c> (ffi §A4), so a pointer read
/// after <c>ListGroups</c> returns is a use-after-unpin. Reading them in the stand-in is
/// also the only way to observe that the pins are live for the whole call.
/// </para>
/// <para>
/// ⚠ <b>No <c>MarshalAs(I1)</c> claim is made here</b>, and none is needed: this submit
/// has no boolean. An injected submit never crosses the P/Invoke at all, so this file
/// cannot observe marshalling — that is
/// <c>AdminNativeMethodsMarshallingTests</c>' business.
/// </para>
/// <para>
/// ⚠ <b>One argument list and one stand-in per RPC.</b> The file grew beyond the
/// three-array original — <c>describe_consumer_groups_async</c>, for one, takes one array
/// and one <c>bool</c>. Each section keeps its own capture harness and carrier type
/// deliberately: the delegates differ in arity, so
/// every argument after the first array — the callback pointer included — sits at a
/// different position in each, and a shared harness would hide that.
/// </para>
/// </remarks>
public sealed class AdminP5SubmitArgumentTests
{
    /// <summary>
    /// A <see langword="null"/> timeout must become a <b>negative</b> <c>timeout_ms</c>,
    /// which the ABI reads as "unset, use the client default" — <b>not</b> <c>0</c>, which
    /// would mean "time out immediately". Both spellings of "no timeout" agree.
    /// </summary>
    [Fact]
    public void NullTimeout_MapsToANegative_NotZero()
    {
        Assert.True(
            Capture(options: null).TimeoutMs < 0,
            "a null timeout must map to a NEGATIVE timeout_ms (unset), not 0");

        Assert.True(
            Capture(new ListGroupsOptions()).TimeoutMs < 0,
            "an explicit options object with a null timeout must map the same way");
    }

    /// <summary>
    /// An explicit timeout is forwarded verbatim, and <c>0</c> stays <c>0</c> — a real
    /// request ("do not wait"), distinct from <see langword="null"/>.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(45_678)]
    public void ExplicitTimeout_IsForwardedVerbatim(int timeoutMs) =>
        Assert.Equal(timeoutMs, Capture(new ListGroupsOptions { TimeoutMs = timeoutMs }).TimeoutMs);

    /// <summary>
    /// <see langword="null"/> options send Java's defaults: <b>three</b> empty axes, each
    /// with its own count of <c>0</c>, which the ABI reads as "do not filter on this one".
    /// </summary>
    /// <remarks>
    /// ⚠ Empty is <b>not</b> "match nothing" — it is Java's <c>Set.of()</c> field
    /// initializer (<c>ListGroupsOptions.java:30-32</c>), so the no-options call must list
    /// groups of every state, protocol type and type. An axis that arrived with a
    /// <em>non-zero</em> count here would filter a request the caller never filtered.
    /// </remarks>
    [Fact]
    public void NullOptions_SendThreeEmptyAxes()
    {
        Captured captured = Capture(options: null);

        AssertAxis(captured.GroupStates, Array.Empty<string>());
        AssertAxis(captured.ProtocolTypes, Array.Empty<string>());
        AssertAxis(captured.Types, Array.Empty<string>());
    }

    /// <summary>
    /// A default-constructed options object is indistinguishable at the seam from
    /// <see langword="null"/> options — the claim <see cref="IAdmin.ListGroups"/> makes.
    /// </summary>
    [Fact]
    public void EmptyOptions_SendTheSameThreeEmptyAxes()
    {
        Captured captured = Capture(new ListGroupsOptions());

        AssertAxis(captured.GroupStates, Array.Empty<string>());
        AssertAxis(captured.ProtocolTypes, Array.Empty<string>());
        AssertAxis(captured.Types, Array.Empty<string>());
    }

    /// <summary>
    /// ⚠⚠ <b>THE test for this shape.</b> All three filters reach the submit at once, at
    /// three <b>different</b> lengths, each in its own array beside its own count.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The lengths (1, 3, 2) are deliberately pairwise distinct: any axis sized from
    /// another's count, or handed another axis's array, changes a value asserted below.
    /// Equal lengths would let a crossed count pass.
    /// </para>
    /// <para>
    /// The enums cross as Java's <c>toString()</c> names, never as ordinals — neither
    /// <see cref="GroupState"/> nor <see cref="GroupType"/> has a numeric id in Java — so
    /// the expected values are spelled out as the strings the ABI parses.
    /// </para>
    /// </remarks>
    [Fact]
    public void AllThreeFilters_ReachTheSubmit_EachWithItsOwnArrayAndCount()
    {
        Captured captured = Capture(new ListGroupsOptions
        {
            GroupStates = new[] { GroupState.Stable },
            ProtocolTypes = new[] { "", "consumer", "connect" },
            Types = new[] { GroupType.Classic, GroupType.Consumer },
        });

        AssertAxis(captured.GroupStates, new[] { "Stable" });
        AssertAxis(captured.ProtocolTypes, new[] { "", "connect", "consumer" });
        AssertAxis(captured.Types, new[] { "Classic", "Consumer" });
    }

    /// <summary>
    /// Setting <b>one</b> axis leaves the other two empty — the asymmetric case a
    /// shared count hides, since an axis sized from a sibling's count would arrive
    /// populated (or truncated) without anyone asking.
    /// </summary>
    [Theory]
    [InlineData(Axis.GroupStates)]
    [InlineData(Axis.ProtocolTypes)]
    [InlineData(Axis.Types)]
    public void OneAxisSet_LeavesTheOtherTwoEmpty(Axis axis)
    {
        Captured captured = Capture(axis switch
        {
            Axis.GroupStates => new ListGroupsOptions
            {
                GroupStates = new[] { GroupState.Empty, GroupState.Dead },
            },
            Axis.ProtocolTypes => new ListGroupsOptions { ProtocolTypes = new[] { "consumer" } },
            _ => new ListGroupsOptions { Types = new[] { GroupType.Share } },
        });

        AssertAxis(
            captured.GroupStates,
            axis == Axis.GroupStates ? new[] { "Dead", "Empty" } : Array.Empty<string>());
        AssertAxis(
            captured.ProtocolTypes,
            axis == Axis.ProtocolTypes ? new[] { "consumer" } : Array.Empty<string>());
        AssertAxis(
            captured.Types,
            axis == Axis.Types ? new[] { "Share" } : Array.Empty<string>());
    }

    /// <summary>
    /// Java's three static factories reach the seam as the filters they document —
    /// including <c>forConsumerGroups()</c>'s <b>two</b> axes, whose empty protocol type
    /// is a member of the set and not a placeholder.
    /// </summary>
    [Fact]
    public void JavasFactories_ReachTheSubmitAsTheFiltersTheyDocument()
    {
        Captured consumer = Capture(ListGroupsOptions.ForConsumerGroups());
        AssertAxis(consumer.ProtocolTypes, new[] { "", "consumer" });
        AssertAxis(consumer.Types, new[] { "Classic", "Consumer" });
        AssertAxis(consumer.GroupStates, Array.Empty<string>());

        Captured share = Capture(ListGroupsOptions.ForShareGroups());
        AssertAxis(share.Types, new[] { "Share" });
        AssertAxis(share.ProtocolTypes, Array.Empty<string>());

        Captured streams = Capture(ListGroupsOptions.ForStreamsGroups());
        AssertAxis(streams.Types, new[] { "Streams" });
        AssertAxis(streams.ProtocolTypes, Array.Empty<string>());
    }

    /// <summary>
    /// A <see cref="GroupState"/> or <see cref="GroupType"/> value no member defines —
    /// reachable in C# only by a cast, which Java's enum-typed <c>Set</c> cannot express —
    /// is rejected <b>before</b> anything is submitted, pinned or rooted (ffi §B5).
    /// </summary>
    /// <remarks>
    /// The blame names the caller's parameter, not the marshaller: <c>GroupMarshal</c>'s
    /// encode direction is deliberately partial and does not know which property to
    /// mention. Nothing reaches the submit, so there is nothing to unwind — and no
    /// <c>GCHandle</c> was rooted to leak.
    /// </remarks>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void UndefinedEnumValue_IsRejectedBeforeAnythingIsSubmitted(bool onTheStateAxis)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        ListGroupsOptions options = onTheStateAxis
            ? new ListGroupsOptions { GroupStates = new[] { (GroupState)(-1) } }
            : new ListGroupsOptions { Types = new[] { (GroupType)99 } };

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListGroups(
                options,
                (handle, states, stateCount, protocols, protocolCount, types, typeCount, timeoutMs, callback, userData) =>
                    submitted = true));

        Assert.False(submitted, "an undefined enum value must never reach the submit");
        Assert.Equal("options", failure.ParamName);
        Assert.Contains(
            onTheStateAxis ? "ListGroupsOptions.GroupStates" : "ListGroupsOptions.Types",
            failure.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// A negative timeout is rejected before the submit too, and by the shared validator —
    /// so <c>listGroups</c> reports it exactly as every other admin RPC does.
    /// </summary>
    [Fact]
    public void NegativeTimeout_IsRejectedBeforeAnythingIsSubmitted()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListGroups(
                new ListGroupsOptions { TimeoutMs = -1 },
                (handle, states, stateCount, protocols, protocolCount, types, typeCount, timeoutMs, callback, userData) =>
                    submitted = true));

        Assert.False(submitted);
        Assert.Equal("options", failure.ParamName);
        Assert.Contains("ListGroupsOptions.TimeoutMs", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>Which of the three independent filter axes a parameterised case sets.</summary>
    public enum Axis
    {
        /// <summary>Java's <c>inGroupStates</c>, encoded as <c>GroupState.toString()</c>.</summary>
        GroupStates,

        /// <summary>Java's <c>withProtocolTypes</c>, already strings.</summary>
        ProtocolTypes,

        /// <summary>Java's <c>withTypes</c>, encoded as <c>GroupType.toString()</c>.</summary>
        Types,
    }

    /// <summary>
    /// Asserts one axis arrived with <b>both</b> its own count and its own array contents.
    /// </summary>
    /// <remarks>
    /// The count is asserted separately from the decoded contents on purpose: a count
    /// borrowed from another axis is a different defect from an array borrowed from
    /// another axis, and either one alone would pass the other's assertion.
    /// </remarks>
    /// <param name="actual">What the stand-in submit recorded for this axis.</param>
    /// <param name="expected">The names, in ordinal order (the axes de-duplicate through a set).</param>
    private static void AssertAxis(CapturedAxis actual, string[] expected)
    {
        Assert.Equal(expected.Length, actual.Count);
        Assert.Equal(expected.Length, actual.Names.Count);
        Assert.Equal(expected, actual.Names.OrderBy(name => name, StringComparer.Ordinal));
    }

    /// <summary>
    /// Runs the production submit with a stand-in that records the three axes instead of
    /// calling native, then completes the operation through the <b>production</b>
    /// trampoline so the <c>GCHandle</c> and the span-the-op reference are released before
    /// the client is disposed.
    /// </summary>
    /// <param name="options">The options under test, or <see langword="null"/>.</param>
    private static Captured Capture(ListGroupsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        ListGroupsResult result = admin.ListGroups(
            options,
            (handle, states, stateCount, protocols, protocolCount, types, typeCount, timeoutMs, callback, userData) =>
            {
                captured.TimeoutMs = timeoutMs;

                // ⚠ Decoded HERE: production unpins every name the moment this returns.
                captured.GroupStates = new CapturedAxis(stateCount, Decode(states));
                captured.ProtocolTypes = new CapturedAxis(protocolCount, Decode(protocols));
                captured.Types = new CapturedAxis(typeCount, Decode(types));
                captured.UserData = userData;
            });

        AdminCallbacks.ListGroups(IntPtr.Zero, CapturedError(), captured.UserData);

        // Observe the fault the trampoline just delivered. All() awaits an already-faulted
        // source, so it completes synchronously and this read is race-free.
        Assert.NotNull(result.All().Exception);
        return captured;
    }

    /// <summary>
    /// Decodes one axis's pointer array in full — <b>by the array's own length</b>, never
    /// by a count, so a mismatch between the two is reported by the assertion rather than
    /// by an index-out-of-range inside the stand-in.
    /// </summary>
    /// <param name="names">The pinned, NUL-terminated UTF-8 names for one axis.</param>
    private static IReadOnlyList<string> Decode(IntPtr[] names) =>
        names.Select(name => Utf8Marshal.PtrToString(name) ?? "<null>").ToArray();

    /// <summary>
    /// An <b>owned</b> error for the trampoline to consume, standing in for the one native
    /// would hand the callback. The trampoline frees it via
    /// <see cref="KafkaException.FromHandle"/>, so it must never be freed here.
    /// </summary>
    private static IntPtr CapturedError()
    {
        using Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("captured");
        IntPtr error = NativeMethods.KafkaErrorNew(1, message.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    /// <summary>
    /// Settles a shape-4a operation by firing one per-key failure for every key the submit
    /// carried. The <c>value</c> slot is NULL on a failing key; each error is <b>owned</b>
    /// and the trampoline frees it.
    /// </summary>
    /// <param name="fire">The production trampoline for this RPC.</param>
    /// <param name="keys">Every key the submit sent, in submit order.</param>
    /// <param name="userData">The operation's <c>GCHandle</c> pointer.</param>
    private static void FirePerKeyFailures(
        Action<IntPtr, IntPtr, IntPtr, IntPtr> fire, IEnumerable<string> keys, IntPtr userData)
    {
        foreach (string key in keys)
        {
            using Utf8Marshal.PinnedUtf8String pinnedKey = Utf8Marshal.Pin(key);
            fire(pinnedKey.Pointer, IntPtr.Zero, CapturedError(), userData);
        }
    }

    /// <summary>One filter axis as it crossed the seam: its own count, its own names.</summary>
    private sealed class CapturedAxis
    {
        internal CapturedAxis(int count, IReadOnlyList<string> names)
        {
            Count = count;
            Names = names;
        }

        /// <summary>The <c>*_count</c> argument this axis travelled with.</summary>
        internal int Count { get; }

        /// <summary>Everything the axis's pointer array actually held.</summary>
        internal IReadOnlyList<string> Names { get; }
    }

    private sealed class Captured
    {
        internal int TimeoutMs { get; set; }

        internal CapturedAxis GroupStates { get; set; } =
            new CapturedAxis(int.MinValue, Array.Empty<string>());

        internal CapturedAxis ProtocolTypes { get; set; } =
            new CapturedAxis(int.MinValue, Array.Empty<string>());

        internal CapturedAxis Types { get; set; } =
            new CapturedAxis(int.MinValue, Array.Empty<string>());

        internal IntPtr UserData { get; set; }
    }

    // ------------------------------------------------------------------------------------
    // describeConsumerGroups — a third argument list at the same seam.
    //
    // ⚠⚠ ONE array and ONE bool, not two arrays. The submit takes the group-id array, its
    // count, the timeout, then `includeAuthorizedOperations` — so every argument after the
    // single array sits at a different position from the RPC above, INCLUDING the
    // callback pointer. A stand-in copied from that neighbour would bind the callback
    // slot to the wrong parameter; these cases exist to catch exactly that.
    //
    // ⚠ And the group ids are NOT options — they are a required argument the caller
    // supplies directly, so their rejection paths (null collection, null element,
    // duplicates) name `groupIds`, not `options`, unlike every filter axis above.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// The group ids and their count reach the submit intact, and the count is the array's
    /// own length rather than the caller's collection size.
    /// </summary>
    [Fact]
    public void Describe_GroupIdsAndCount_ReachTheSubmit()
    {
        DescribeCaptured captured = CaptureDescribe(new[] { "alpha", "beta", "gamma" }, options: null);

        Assert.Equal(3, captured.GroupIds.Count);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, captured.GroupIds.Names);
    }

    /// <summary>
    /// The id list is deduplicated <b>ordinally</b> before it is pinned, so the count the
    /// submit sees shrinks — and two ids differing only in case are NOT duplicates.
    /// </summary>
    [Fact]
    public void Describe_DuplicateGroupIds_AreCollapsedOrdinallyBeforeTheSubmit()
    {
        DescribeCaptured captured = CaptureDescribe(
            new[] { "g", "g", "G", "g" },
            options: null);

        Assert.Equal(2, captured.GroupIds.Count);
        Assert.Equal(new[] { "g", "G" }, captured.GroupIds.Names);
    }

    /// <summary>
    /// <c>IncludeAuthorizedOperations</c> crosses in <b>both</b> states. A flag that is
    /// hardcoded, dropped, or inverted is invisible downstream — the Rust mock fails every
    /// key regardless — so it is read here, at the seam.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Describe_IncludeAuthorizedOperations_CrossesInBothStates(bool include)
    {
        DescribeCaptured captured = CaptureDescribe(
            new[] { "g1" },
            new DescribeConsumerGroupsOptions { IncludeAuthorizedOperations = include });

        Assert.Equal(include, captured.IncludeAuthorizedOperations);
    }

    /// <summary>
    /// No options at all: the flag defaults to <see langword="false"/> and the timeout
    /// travels as the negative "unset" value, exactly as the two RPCs above do.
    /// </summary>
    [Fact]
    public void Describe_NullOptions_SendNoFlagAndAnUnsetTimeout()
    {
        DescribeCaptured captured = CaptureDescribe(new[] { "g1" }, options: null);

        Assert.False(captured.IncludeAuthorizedOperations);
        Assert.True(
            captured.TimeoutMs < 0,
            $"an absent timeout must cross as the negative unset value, not {captured.TimeoutMs}");
    }

    /// <summary>An explicit timeout is forwarded verbatim, zero included.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(45_678)]
    public void Describe_ExplicitTimeout_IsForwardedVerbatim(int timeoutMs)
    {
        DescribeCaptured captured = CaptureDescribe(
            new[] { "g1" },
            new DescribeConsumerGroupsOptions { TimeoutMs = timeoutMs });

        Assert.Equal(timeoutMs, captured.TimeoutMs);
    }

    /// <summary>A negative timeout is refused before anything is pinned or submitted.</summary>
    [Fact]
    public void Describe_NegativeTimeout_IsRejectedBeforeAnythingIsSubmitted()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeConsumerGroups(
                new[] { "g1" },
                new DescribeConsumerGroupsOptions { TimeoutMs = -1 },
                (handle, groupIds, count, timeoutMs, include, callback, userData) => submitted = true));

        Assert.False(submitted);
        Assert.Equal("options", failure.ParamName);
        Assert.Contains(
            "DescribeConsumerGroupsOptions.TimeoutMs",
            failure.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// A null id collection and a null element inside one are both refused, and both name
    /// <c>groupIds</c> — the required argument — not <c>options</c>.
    /// </summary>
    [Fact]
    public void Describe_BadGroupIds_AreRejectedBeforeAnythingIsSubmitted()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        NativeAdminClient.NativeDescribeConsumerGroupsSubmit submit =
            (handle, groupIds, count, timeoutMs, include, callback, userData) => submitted = true;

        ArgumentNullException missing = Assert.Throws<ArgumentNullException>(
            () => { admin.DescribeConsumerGroups(null!, null, submit); });
        Assert.Equal("groupIds", missing.ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => { admin.DescribeConsumerGroups(new string?[] { "g1", null }!, null, submit); });
        Assert.Equal("groupIds", nullElement.ParamName);

        Assert.False(submitted, "a malformed id list must never reach the submit");
    }

    /// <summary>
    /// Drives one <c>describe_consumer_groups_async</c> submit and reports every argument
    /// that crossed, then settles the operation through the production trampoline so the
    /// per-operation <c>GCHandle</c> is released before the client is disposed.
    /// </summary>
    /// <param name="groupIds">The ids to request.</param>
    /// <param name="options">The options under test, or <see langword="null"/>.</param>
    private static DescribeCaptured CaptureDescribe(
        IReadOnlyCollection<string> groupIds,
        DescribeConsumerGroupsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        DescribeCaptured captured = new DescribeCaptured();
        DescribeConsumerGroupsResult result = admin.DescribeConsumerGroups(
            groupIds,
            options,
            (handle, ids, count, timeoutMs, include, callback, userData) =>
            {
                captured.TimeoutMs = timeoutMs;
                captured.IncludeAuthorizedOperations = include;

                // ⚠ Decoded HERE: production unpins every id the moment this returns.
                captured.GroupIds = new CapturedAxis(count, Decode(ids));
                captured.UserData = userData;
            });

        FirePerKeyFailures(
            AdminCallbacks.DescribeConsumerGroups.Invoke, groupIds, captured.UserData);

        Assert.NotEmpty(result.DescribedGroups);
        Assert.All(result.DescribedGroups.Values, task => Assert.NotNull(task.Exception));
        return captured;
    }

    /// <summary>
    /// What crossed the <c>describe_consumer_groups_async</c> seam. Its own type, again:
    /// this RPC's single array plus a boolean share no shape with either neighbour's
    /// filter axes.
    /// </summary>
    private sealed class DescribeCaptured
    {
        internal int TimeoutMs { get; set; } = int.MinValue;

        internal bool IncludeAuthorizedOperations { get; set; }

        internal CapturedAxis GroupIds { get; set; } =
            new CapturedAxis(int.MinValue, Array.Empty<string>());

        internal IntPtr UserData { get; set; }
    }

    // ------------------------------------------------------------------------------------
    // describeClassicGroups — the same seam, argument-for-argument.
    //
    // ⚠ Covered separately rather than parameterized over the neighbour above, because the
    // two submits are DIFFERENT delegate types carrying DIFFERENT callbacks: a
    // DescribeClassicGroupsCallback hands back a DescribeClassicGroupsResult_t root, and the
    // whole point of the split is that it can never reach the consumer RPC's destroy. A
    // shared harness would have to erase exactly the distinction being protected.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// The group ids and their count reach the submit intact, and ordinal deduplication
    /// happens before the pin — so two ids differing only in case stay two ids.
    /// </summary>
    [Fact]
    public void DescribeClassic_GroupIdsAndCount_ReachTheSubmitDeduplicatedOrdinally()
    {
        DescribeCaptured captured = CaptureDescribeClassic(
            new[] { "alpha", "beta", "alpha", "Alpha" },
            options: null);

        Assert.Equal(3, captured.GroupIds.Count);
        Assert.Equal(new[] { "alpha", "beta", "Alpha" }, captured.GroupIds.Names);
    }

    /// <summary>
    /// <c>IncludeAuthorizedOperations</c> crosses in <b>both</b> states. A flag that is
    /// hardcoded, dropped, or inverted is invisible downstream — the Rust mock fails every
    /// key regardless — so it is read here, at the seam.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void DescribeClassic_IncludeAuthorizedOperations_CrossesInBothStates(bool include)
    {
        DescribeCaptured captured = CaptureDescribeClassic(
            new[] { "g1" },
            new DescribeClassicGroupsOptions { IncludeAuthorizedOperations = include });

        Assert.Equal(include, captured.IncludeAuthorizedOperations);
    }

    /// <summary>
    /// No options at all: the flag defaults to <see langword="false"/> and the timeout
    /// travels as the negative "unset" value. An explicit timeout is forwarded verbatim.
    /// </summary>
    [Fact]
    public void DescribeClassic_TimeoutAndFlag_DefaultUnsetAndForwardVerbatim()
    {
        DescribeCaptured unset = CaptureDescribeClassic(new[] { "g1" }, options: null);

        Assert.False(unset.IncludeAuthorizedOperations);
        Assert.True(
            unset.TimeoutMs < 0,
            $"an absent timeout must cross as the negative unset value, not {unset.TimeoutMs}");

        // Zero included: it is a legal timeout, not a stand-in for "unset".
        Assert.Equal(
            0,
            CaptureDescribeClassic(
                new[] { "g1" },
                new DescribeClassicGroupsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            45_678,
            CaptureDescribeClassic(
                new[] { "g1" },
                new DescribeClassicGroupsOptions { TimeoutMs = 45_678 }).TimeoutMs);
    }

    /// <summary>
    /// Every precondition is refused before anything is pinned or submitted (ffi §B5), and
    /// each names the argument it is about — the options for the timeout, <c>groupIds</c>
    /// for both the missing collection and the null element inside one.
    /// </summary>
    [Fact]
    public void DescribeClassic_BadArguments_AreRejectedBeforeAnythingIsSubmitted()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        NativeAdminClient.NativeDescribeClassicGroupsSubmit submit =
            (handle, groupIds, count, timeoutMs, include, callback, userData) => submitted = true;

        ArgumentOutOfRangeException badTimeout = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.DescribeClassicGroups(
                new[] { "g1" },
                new DescribeClassicGroupsOptions { TimeoutMs = -1 },
                submit));
        Assert.Equal("options", badTimeout.ParamName);
        Assert.Contains(
            "DescribeClassicGroupsOptions.TimeoutMs",
            badTimeout.Message,
            StringComparison.Ordinal);

        ArgumentNullException missing = Assert.Throws<ArgumentNullException>(
            () => { admin.DescribeClassicGroups(null!, null, submit); });
        Assert.Equal("groupIds", missing.ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => { admin.DescribeClassicGroups(new string?[] { "g1", null }!, null, submit); });
        Assert.Equal("groupIds", nullElement.ParamName);

        Assert.False(submitted, "a malformed request must never reach the submit");
    }

    /// <summary>
    /// Drives one <c>describe_classic_groups_async</c> submit and reports every argument
    /// that crossed, then settles the operation through the production trampoline so the
    /// per-operation <c>GCHandle</c> is released before the client is disposed.
    /// </summary>
    /// <param name="groupIds">The ids to request.</param>
    /// <param name="options">The options under test, or <see langword="null"/>.</param>
    private static DescribeCaptured CaptureDescribeClassic(
        IReadOnlyCollection<string> groupIds,
        DescribeClassicGroupsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        DescribeCaptured captured = new DescribeCaptured();
        DescribeClassicGroupsResult result = admin.DescribeClassicGroups(
            groupIds,
            options,
            (handle, ids, count, timeoutMs, include, callback, userData) =>
            {
                captured.TimeoutMs = timeoutMs;
                captured.IncludeAuthorizedOperations = include;

                // ⚠ Decoded HERE: production unpins every id the moment this returns.
                captured.GroupIds = new CapturedAxis(count, Decode(ids));
                captured.UserData = userData;
            });

        // ⚠ This RPC's own trampoline, not the consumer one: a submit-level error is OWNED,
        // and routing it through the wrong trampoline is what the split delegate prevents.
        FirePerKeyFailures(
            AdminCallbacks.DescribeClassicGroups.Invoke, groupIds, captured.UserData);

        Assert.NotEmpty(result.DescribedGroups);
        Assert.All(result.DescribedGroups.Values, task => Assert.NotNull(task.Exception));
        return captured;
    }

    // ------------------------------------------------------------------------------------
    // listConsumerGroupOffsets — the one JAGGED submit on this surface.
    //
    // ⚠⚠ Two levels of indirection, and a discriminant that cannot be derived. Per group id
    // the request carries a topic-partition selection as two parallel INNER arrays behind
    // `topics[i]` / `partitions[i]`, sized by `partition_counts[i]`; `all_partitions[i]`
    // says whether the selection exists at all. "All partitions" and "an empty explicit
    // selection" BOTH cross with a count of 0, so the flag is the only thing telling them
    // apart — and they are opposite requests. Every case below therefore asserts the flag
    // and the count together, per index.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>THE test for this shape.</b> A null <c>TopicPartitions</c> ("all partitions")
    /// and an <em>empty</em> one ("nothing") both travel with a count of <c>0</c> and a NULL
    /// inner pointer, and are distinguished <b>only</b> by <c>all_partitions[i]</c> —
    /// <see langword="true"/> for the first, <see langword="false"/> for the second.
    /// </summary>
    /// <remarks>
    /// A flag derived from the count would make every empty selection read as "all", which
    /// returns a group's entire committed state to a caller who asked for none of it. Both
    /// groups are driven in one call so a per-index mix-up cannot pass either.
    /// </remarks>
    [Fact]
    public void Offsets_AllVersusEmptySelection_DifferOnlyByTheFlag()
    {
        OffsetsCaptured captured = CaptureOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["all"] = new ListConsumerGroupOffsetsSpec(),
                ["none"] = new ListConsumerGroupOffsetsSpec
                {
                    TopicPartitions = Array.Empty<TopicPartition>(),
                },
            },
            options: null);

        int all = captured.IndexOf("all");
        int none = captured.IndexOf("none");

        Assert.True(captured.AllPartitions[all], "a null selection means ALL partitions");
        Assert.False(captured.AllPartitions[none], "an empty selection means NO partitions");

        Assert.Equal(0, captured.PartitionCounts[all]);
        Assert.Equal(0, captured.PartitionCounts[none]);
        Assert.True(captured.InnerPointersNull[all]);
        Assert.True(captured.InnerPointersNull[none]);
    }

    /// <summary>
    /// An explicit selection reaches the submit through both levels of indirection, at
    /// <b>different</b> lengths per group and beside an all-partitions group — so an inner
    /// array sized from a neighbour's count, or read from the wrong index, changes a value
    /// asserted here.
    /// </summary>
    /// <remarks>
    /// The two parallel inner arrays are decoded together, pair by pair: a topic array read
    /// against the other group's partition array would still decode, and only a paired
    /// assertion catches it.
    /// </remarks>
    [Fact]
    public void Offsets_JaggedSelections_ReachTheSubmitPairedAndPerGroup()
    {
        OffsetsCaptured captured = CaptureOffsets(
            new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
            {
                ["one"] = new ListConsumerGroupOffsetsSpec
                {
                    TopicPartitions = new[] { new TopicPartition("t-one", 7) },
                },
                ["three"] = new ListConsumerGroupOffsetsSpec
                {
                    TopicPartitions = new[]
                    {
                        new TopicPartition("alpha", 0),
                        new TopicPartition("beta", 11),
                        new TopicPartition("alpha", 2),
                    },
                },
                ["all"] = new ListConsumerGroupOffsetsSpec(),
            },
            options: null);

        Assert.Equal(3, captured.GroupCount);
        Assert.Equal(3, captured.GroupIds.Count);

        int one = captured.IndexOf("one");
        Assert.False(captured.AllPartitions[one]);
        Assert.Equal(1, captured.PartitionCounts[one]);
        Assert.Equal(new[] { new TopicPartition("t-one", 7) }, captured.Selections[one]);

        int three = captured.IndexOf("three");
        Assert.False(captured.AllPartitions[three]);
        Assert.Equal(3, captured.PartitionCounts[three]);
        Assert.Equal(
            new[]
            {
                new TopicPartition("alpha", 0),
                new TopicPartition("beta", 11),
                new TopicPartition("alpha", 2),
            },
            captured.Selections[three]);

        int all = captured.IndexOf("all");
        Assert.True(captured.AllPartitions[all]);
        Assert.Equal(0, captured.PartitionCounts[all]);
        Assert.True(captured.InnerPointersNull[all]);
    }

    /// <summary>
    /// <c>RequireStable</c> crosses in <b>both</b> states. The Rust mock ignores it, so a
    /// flag that is hardcoded, dropped or inverted is invisible downstream — it is read
    /// here, at the seam.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Offsets_RequireStable_CrossesInBothStates(bool requireStable)
    {
        OffsetsCaptured captured = CaptureOffsets(
            OneGroup(),
            new ListConsumerGroupOffsetsOptions { RequireStable = requireStable });

        Assert.Equal(requireStable, captured.RequireStable);
    }

    /// <summary>
    /// No options at all: <c>RequireStable</c> defaults to <see langword="false"/> and the
    /// timeout travels as the negative "unset" value. An explicit timeout is forwarded
    /// verbatim, <c>0</c> included — it is a real request, not a stand-in for "unset".
    /// </summary>
    [Fact]
    public void Offsets_TimeoutAndFlag_DefaultUnsetAndForwardVerbatim()
    {
        OffsetsCaptured unset = CaptureOffsets(OneGroup(), options: null);

        Assert.False(unset.RequireStable);
        Assert.True(
            unset.TimeoutMs < 0,
            $"an absent timeout must cross as the negative unset value, not {unset.TimeoutMs}");

        Assert.True(
            CaptureOffsets(OneGroup(), new ListConsumerGroupOffsetsOptions()).TimeoutMs < 0,
            "an explicit options object with a null timeout must map the same way");

        Assert.Equal(
            0,
            CaptureOffsets(
                OneGroup(),
                new ListConsumerGroupOffsetsOptions { TimeoutMs = 0 }).TimeoutMs);
        Assert.Equal(
            45_678,
            CaptureOffsets(
                OneGroup(),
                new ListConsumerGroupOffsetsOptions { TimeoutMs = 45_678 }).TimeoutMs);
    }

    /// <summary>
    /// Every precondition is refused before anything is pinned, rooted or submitted
    /// (ffi §B5), and each names the argument it is about.
    /// </summary>
    /// <remarks>
    /// The null group id and the repeated one are unreachable through a
    /// <see cref="Dictionary{TKey, TValue}"/>, but the parameter is an <b>interface</b> a
    /// caller may implement — so they are driven through a stand-in implementation that can
    /// produce them. A repeat is rejected rather than collapsed: the per-key bridge would
    /// otherwise carry whichever spec happened to win.
    /// </remarks>
    [Fact]
    public void Offsets_BadArguments_AreRejectedBeforeAnythingIsSubmitted()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        bool submitted = false;
        NativeAdminClient.NativeListConsumerGroupOffsetsSubmit submit =
            (handle, ids, all, topics, partitions, counts, count, timeoutMs, stable, callback, userData) =>
                submitted = true;

        ArgumentOutOfRangeException badTimeout = Assert.Throws<ArgumentOutOfRangeException>(
            () => admin.ListConsumerGroupOffsets(
                OneGroup(),
                new ListConsumerGroupOffsetsOptions { TimeoutMs = -1 },
                submit));
        Assert.Equal("options", badTimeout.ParamName);
        Assert.Contains(
            "ListConsumerGroupOffsetsOptions.TimeoutMs",
            badTimeout.Message,
            StringComparison.Ordinal);

        ArgumentNullException missing = Assert.Throws<ArgumentNullException>(
            () => { admin.ListConsumerGroupOffsets(null!, null, submit); });
        Assert.Equal("groupSpecs", missing.ParamName);

        ArgumentException nullGroupId = Assert.Throws<ArgumentException>(
            () => admin.ListConsumerGroupOffsets(
                new GroupSpecEntries(
                    new KeyValuePair<string, ListConsumerGroupOffsetsSpec>(
                        null!, new ListConsumerGroupOffsetsSpec())),
                null,
                submit));
        Assert.Equal("groupSpecs", nullGroupId.ParamName);

        ArgumentException duplicate = Assert.Throws<ArgumentException>(
            () => admin.ListConsumerGroupOffsets(
                new GroupSpecEntries(
                    new KeyValuePair<string, ListConsumerGroupOffsetsSpec>(
                        "g1", new ListConsumerGroupOffsetsSpec()),
                    new KeyValuePair<string, ListConsumerGroupOffsetsSpec>(
                        "g1", new ListConsumerGroupOffsetsSpec())),
                null,
                submit));
        Assert.Equal("groupSpecs", duplicate.ParamName);
        Assert.Contains("'g1'", duplicate.Message, StringComparison.Ordinal);

        ArgumentException nullSpec = Assert.Throws<ArgumentException>(
            () => admin.ListConsumerGroupOffsets(
                new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                {
                    ["g1"] = null!,
                },
                null,
                submit));
        Assert.Equal("groupSpecs", nullSpec.ParamName);

        // A `default(TopicPartition)` carries a null topic, which the ABI would read as an
        // absent name rather than as the request the caller wrote.
        ArgumentException nullTopic = Assert.Throws<ArgumentException>(
            () => admin.ListConsumerGroupOffsets(
                new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
                {
                    ["g1"] = new ListConsumerGroupOffsetsSpec
                    {
                        TopicPartitions = new[] { default(TopicPartition) },
                    },
                },
                null,
                submit));
        Assert.Equal("groupSpecs", nullTopic.ParamName);

        Assert.False(submitted, "a malformed request must never reach the submit");
    }

    /// <summary>One group asking for all of its partitions — the minimal well-formed call.</summary>
    private static IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> OneGroup() =>
        new Dictionary<string, ListConsumerGroupOffsetsSpec>(StringComparer.Ordinal)
        {
            ["g1"] = new ListConsumerGroupOffsetsSpec(),
        };

    /// <summary>
    /// Drives one <c>list_consumer_group_offsets_async</c> submit and reports every argument
    /// that crossed — <b>including both levels of the jagged selection</b> — then settles the
    /// operation through the production trampoline so the per-operation <c>GCHandle</c> is
    /// released before the client is disposed.
    /// </summary>
    /// <param name="groupSpecs">The per-group selections to request.</param>
    /// <param name="options">The options under test, or <see langword="null"/>.</param>
    private static OffsetsCaptured CaptureOffsets(
        IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec> groupSpecs,
        ListConsumerGroupOffsetsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        OffsetsCaptured captured = new OffsetsCaptured();
        ListConsumerGroupOffsetsResult result = admin.ListConsumerGroupOffsets(
            groupSpecs,
            options,
            (handle, ids, all, topics, partitions, counts, count, timeoutMs, stable, callback, userData) =>
            {
                captured.TimeoutMs = timeoutMs;
                captured.RequireStable = stable;
                captured.GroupCount = count;
                captured.UserData = userData;

                // ⚠ Decoded HERE, both levels: production unpins every group id, every topic
                // name and every inner array the moment this returns (ffi §A4), so reading
                // any of it afterwards would be a use-after-unpin.
                captured.GroupIds = Decode(ids);
                captured.AllPartitions = all.ToArray();
                captured.PartitionCounts = counts.ToArray();

                List<bool> innerNull = new List<bool>(count);
                List<IReadOnlyList<TopicPartition>> selections =
                    new List<IReadOnlyList<TopicPartition>>(count);
                for (int i = 0; i < count; i++)
                {
                    innerNull.Add(topics[i] == IntPtr.Zero && partitions[i] == IntPtr.Zero);

                    List<TopicPartition> pairs = new List<TopicPartition>(counts[i]);
                    for (int j = 0; j < counts[i]; j++)
                    {
                        IntPtr topic = Marshal.ReadIntPtr(topics[i], j * IntPtr.Size);
                        pairs.Add(new TopicPartition(
                            Utf8Marshal.PtrToString(topic) ?? "<null>",
                            Marshal.ReadInt32(partitions[i], j * sizeof(int))));
                    }

                    selections.Add(pairs);
                }

                captured.InnerPointersNull = innerNull;
                captured.Selections = selections;
            });

        FirePerKeyFailures(
            AdminCallbacks.ListConsumerGroupOffsets.Invoke, captured.GroupIds, captured.UserData);

        Assert.NotEmpty(captured.GroupIds);
        Assert.All(
            captured.GroupIds,
            groupId => Assert.NotNull(result.PartitionsToOffsetAndMetadata(groupId).Exception));
        return captured;
    }

    /// <summary>
    /// What crossed the <c>list_consumer_group_offsets_async</c> seam: the flat per-group
    /// arrays plus the decoded contents of both inner levels, indexed the same way.
    /// </summary>
    private sealed class OffsetsCaptured
    {
        internal int TimeoutMs { get; set; } = int.MinValue;

        internal bool RequireStable { get; set; }

        internal int GroupCount { get; set; } = int.MinValue;

        internal IReadOnlyList<string> GroupIds { get; set; } = Array.Empty<string>();

        /// <summary>The discriminant, per group index — the only "all versus none" signal.</summary>
        internal IReadOnlyList<bool> AllPartitions { get; set; } = Array.Empty<bool>();

        internal IReadOnlyList<int> PartitionCounts { get; set; } = Array.Empty<int>();

        /// <summary>Whether both inner pointers were NULL, per group index.</summary>
        internal IReadOnlyList<bool> InnerPointersNull { get; set; } = Array.Empty<bool>();

        /// <summary>The decoded (topic, partition) pairs, per group index.</summary>
        internal IReadOnlyList<IReadOnlyList<TopicPartition>> Selections { get; set; } =
            Array.Empty<IReadOnlyList<TopicPartition>>();

        internal IntPtr UserData { get; set; }

        /// <summary>
        /// The index a group id crossed at, so every per-group assertion is made against the
        /// group it names rather than against an assumed enumeration order.
        /// </summary>
        /// <param name="groupId">The requested group id.</param>
        internal int IndexOf(string groupId)
        {
            int index = GroupIds.ToList().IndexOf(groupId);
            Assert.True(index >= 0, $"group id '{groupId}' never reached the submit");
            return index;
        }
    }

    /// <summary>
    /// A <see cref="IReadOnlyDictionary{TKey, TValue}"/> that can yield what a real
    /// dictionary cannot — a null key, or the same key twice — so the preconditions guarding
    /// against a hostile implementation of the parameter type are reachable from a test.
    /// </summary>
    /// <remarks>
    /// Only <see cref="Count"/> and enumeration are exercised by production; the rest of the
    /// interface is present because the interface requires it, and says so if called.
    /// </remarks>
    private sealed class GroupSpecEntries : IReadOnlyDictionary<string, ListConsumerGroupOffsetsSpec>
    {
        private readonly KeyValuePair<string, ListConsumerGroupOffsetsSpec>[] _entries;

        internal GroupSpecEntries(params KeyValuePair<string, ListConsumerGroupOffsetsSpec>[] entries)
        {
            _entries = entries;
        }

        public int Count => _entries.Length;

        public IEnumerable<string> Keys => _entries.Select(entry => entry.Key);

        public IEnumerable<ListConsumerGroupOffsetsSpec> Values =>
            _entries.Select(entry => entry.Value);

        public ListConsumerGroupOffsetsSpec this[string key] => throw new NotSupportedException();

        public bool ContainsKey(string key) => throw new NotSupportedException();

        public bool TryGetValue(string key, out ListConsumerGroupOffsetsSpec value) =>
            throw new NotSupportedException();

        public IEnumerator<KeyValuePair<string, ListConsumerGroupOffsetsSpec>> GetEnumerator() =>
            ((IEnumerable<KeyValuePair<string, ListConsumerGroupOffsetsSpec>>)_entries).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => _entries.GetEnumerator();
    }
}
