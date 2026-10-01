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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What the options POCO actually becomes at the P/Invoke. The public tests can only
/// observe that a call <em>completes</em> — the mock ignores the options it is handed —
/// so the argument values themselves are read here, at the seam, where they are facts
/// rather than inferences.
/// </summary>
/// <remarks>
/// The single most important assertion is the timeout mapping: a <see langword="null"/>
/// timeout must become a <b>negative</b> <c>timeout_ms</c>, which the ABI reads as
/// "unset, use the client default" — <b>not</b> <c>0</c>, which would mean "time out
/// immediately".
/// </remarks>
public sealed class AdminSubmitArgumentTests
{
    private const string Topic = "submit-args-topic";

    [Fact]
    public void NullOptions_SendJavasDefaults_AndANegativeTimeout()
    {
        Captured captured = Submit(options: null);

        Assert.True(captured.TimeoutMs < 0, "a null timeout must map to a NEGATIVE timeout_ms (unset), not 0");
        Assert.False(captured.ValidateOnly);
        Assert.True(captured.RetryOnQuotaViolation);
        Assert.Equal(1, captured.Count);
    }

    [Fact]
    public void NullTimeoutOnAnExplicitOptions_StillMapsToANegative()
    {
        Captured captured = Submit(new CreateTopicsOptions { TimeoutMs = null });

        Assert.True(captured.TimeoutMs < 0);
    }

    [Fact]
    public void ExplicitTimeout_IsForwardedVerbatim()
    {
        Captured captured = Submit(new CreateTopicsOptions { TimeoutMs = 12_345 });

        Assert.Equal(12_345, captured.TimeoutMs);
    }

    [Fact]
    public void ZeroTimeout_IsForwardedAsZero_NotTreatedAsUnset()
    {
        // Zero is a real request ("do not wait"), distinct from null; conflating the two
        // is the mistake the negative-means-unset convention invites.
        Captured captured = Submit(new CreateTopicsOptions { TimeoutMs = 0 });

        Assert.Equal(0, captured.TimeoutMs);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void BothBools_AreForwardedIndependently(bool validateOnly, bool retryOnQuotaViolation)
    {
        Captured captured = Submit(new CreateTopicsOptions
        {
            ValidateOnly = validateOnly,
            RetryOnQuotaViolation = retryOnQuotaViolation,
        });

        Assert.Equal(validateOnly, captured.ValidateOnly);
        Assert.Equal(retryOnQuotaViolation, captured.RetryOnQuotaViolation);
    }

    /// <summary>
    /// The input <c>NewTopic_t</c> entries are non-null when the submit runs (the ABI
    /// copies out during the call) and are destroyed after it returns — the caller
    /// retains ownership, so forgetting leaks one handle per topic per call.
    /// </summary>
    [Fact]
    public void InputEntries_AreLiveDuringTheSubmit()
    {
        Captured captured = Submit(options: null);

        Assert.Equal(1, captured.Count);
        Assert.NotEqual(IntPtr.Zero, captured.FirstTopicHandle);
    }

    /// <summary>
    /// Runs the production submit with a stand-in that records the arguments instead of
    /// calling native, then completes the operation through the production trampoline so
    /// nothing is left in flight.
    /// </summary>
    private static Captured Submit(CreateTopicsOptions? options)
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        Captured captured = new Captured();
        CreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic(Topic, 1, 1) },
            options,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, callback, userData) =>
            {
                captured.Count = count;
                captured.FirstTopicHandle = count > 0 ? topics[0] : IntPtr.Zero;
                captured.TimeoutMs = timeoutMs;
                captured.ValidateOnly = validateOnly;
                captured.RetryOnQuotaViolation = retryOnQuotaViolation;
                captured.UserData = userData;
            });

        // Complete it so the operation's GCHandle and span-the-op reference are released
        // before the client is disposed.
        using (Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("captured"))
        using (Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(Topic))
        {
            AdminCallbacks.CreateTopics(
                key.Pointer,
                IntPtr.Zero,
                NativeMethods.KafkaErrorNew(1, message.Pointer),
                captured.UserData);
        }

        Assert.NotNull(result.Values[Topic].Exception);
        return captured;
    }

    private sealed class Captured
    {
        internal int Count { get; set; }

        internal IntPtr FirstTopicHandle { get; set; }

        internal int TimeoutMs { get; set; }

        internal bool ValidateOnly { get; set; }

        internal bool RetryOnQuotaViolation { get; set; }

        internal IntPtr UserData { get; set; }
    }
}
