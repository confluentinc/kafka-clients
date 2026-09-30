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
using System.Threading;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P13.3 (c) — the shared key-string precondition, <see cref="AdminStrings"/>: its
/// exact accept/reject boundary, and that a rejection happens <b>before</b> the native submit
/// (ffi §B5), leaving nothing held that would defer the client's release.
/// </summary>
public sealed class AdminStringsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_releaseBound = TimeSpan.FromSeconds(5);

    private const string InvalidStringMessage =
        "An admin request string must not contain a NUL character or an unpaired UTF-16 " +
        "surrogate: such a string cannot be passed to the native client unchanged.";

    /// <summary>
    /// The strings under test, by label. ⚠ <b>Looked up by label, never passed as theory
    /// data</b>: xUnit serializes <c>InlineData</c> strings to hand them to the runner, and that
    /// round trip replaces a lone surrogate with U+FFFD — so a surrogate case passed inline
    /// silently tests a valid string instead (observed: every lone-surrogate row went green
    /// against a guard that was never reached).
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string?> s_accepted =
        new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["null"] = null,
            ["empty"] = "",
            ["ascii"] = "plain",
            ["latin"] = "délété",
            ["pair"] = "\uD83C\uDF88",
            ["pair-inside"] = "a\uD83C\uDF88b",
            ["two-pairs"] = "\uD83C\uDF88\uD83D\uDE00",
            ["replacement-char"] = "\uFFFD",
        };

    /// <inheritdoc cref="s_accepted"/>
    private static readonly IReadOnlyDictionary<string, string> s_rejected =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["nul-only"] = "\0",
            ["nul-last"] = "a\0",
            ["nul-first"] = "\0a",
            ["nul-inside"] = "a\0b",
            ["lone-high"] = "\uD800",
            ["lone-low"] = "\uDC00",
            ["high-at-end"] = "a\uD83C",
            ["high-then-ascii"] = "\uD83Ca",
            ["reversed-pair"] = "\uDF88\uD83C",
            ["pair-then-low"] = "\uD83C\uDF88\uDF88",
            ["high-then-pair"] = "\uD83C\uD83C\uDF88",
        };

    /// <summary>The labels of <see cref="s_accepted"/>.</summary>
    public static IEnumerable<object[]> AcceptedLabels() => s_accepted.Keys.Select(label => new object[] { label });

    /// <summary>The labels of <see cref="s_rejected"/>.</summary>
    public static IEnumerable<object[]> RejectedLabels() => s_rejected.Keys.Select(label => new object[] { label });

    /// <summary>
    /// The label tables really hold what their names say — the guard against the serialization
    /// trap above creeping back through a table edit: each rejected string has a NUL or an
    /// unpaired surrogate, and no accepted one does.
    /// </summary>
    [Fact]
    public void LabelTables_HoldTheStringsTheirNamesSay()
    {
        Assert.Equal(11, s_rejected.Count);
        Assert.Equal(8, s_accepted.Count);
        Assert.Equal(1, s_rejected["lone-high"].Length);
        Assert.True(char.IsHighSurrogate(s_rejected["lone-high"][0]));
        Assert.True(char.IsLowSurrogate(s_rejected["lone-low"][0]));
        Assert.Equal('\0', s_rejected["nul-only"][0]);
        Assert.True(char.IsSurrogatePair(s_accepted["pair"]![0], s_accepted["pair"]![1]));
    }

    /// <summary>
    /// Every string whose UTF-8 form round-trips — including a well-formed surrogate pair at
    /// either end — passes, and so does <see langword="null"/>, which is each site's own
    /// precondition to decide.
    /// </summary>
    [Theory]
    [MemberData(nameof(AcceptedLabels))]
    public void Validate_AcceptsAStringThatCrossesUnchanged(string label)
    {
        AdminStrings.Validate(s_accepted[label], "p");
    }

    /// <summary>
    /// Each way a string can fail to round-trip: a NUL anywhere, a lone low surrogate, a high
    /// surrogate at the end or followed by a non-low char, and a pair in the wrong order.
    /// </summary>
    [Theory]
    [MemberData(nameof(RejectedLabels))]
    public void Validate_RejectsAStringTheAbiWouldChange(string label)
    {
        ArgumentException rejected =
            Assert.Throws<ArgumentException>(() => AdminStrings.Validate(s_rejected[label], "keys"));

        Assert.Equal("keys", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>createTopics</c> (an inline loop site): the rejection happens before production's
    /// submit is reached — the stand-in never runs — and nothing was pinned, allocated or
    /// ref-counted, so the client's native release is not deferred.
    /// </summary>
    [Fact]
    public void CreateTopics_RejectsBeforeTheSubmit_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.CreateTopics(
            new[] { new NewTopic("ok", 1, 1), new NewTopic("a\0b", 1, 1) },
            options: null,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retry, callback, userData) => submitted = true));

        Assert.Equal("newTopics", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
        Assert.False(submitted);
        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected call must hold no client reference");
    }

    /// <summary>
    /// <c>deleteTopics</c> by name (a de-dup-helper site): the same ordering, and the second of
    /// two keys that would collapse is where the rejection lands — the first is valid.
    /// </summary>
    [Fact]
    public void DeleteTopics_RejectsBeforeTheSubmit_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { "fine", "x\uDC00" }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) => submitted = true,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) => submitted = true));

        Assert.Equal("topics", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
        Assert.False(submitted);
        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected call must hold no client reference");
    }

    private static bool DisposeAndAwaitRelease(NativeAdminClient admin, SafeAdminHandle handle)
    {
        TestTimeout.Run(admin.Dispose, s_deadline);
        return SpinWait.SpinUntil(() => handle.IsClosed, s_releaseBound);
    }
}
