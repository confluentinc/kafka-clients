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
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// Error inspection, and the poll-failure bound: the escalation that stops an unattended
/// soak from consuming nothing for two weeks while looking alive.
/// <para>
/// Note what is NOT constructible here: the binding's <c>KafkaException</c> constructor
/// that sets <c>Code</c> / <c>IsRetriable</c> / <c>IsFatal</c> is <c>internal</c> to
/// <c>Confluent.Kafka</c>, and only <c>Confluent.Kafka.UnitTests</c> is a friend. So the
/// tests here pin the TYPE DISCRIMINATION over exceptions they can build, and drive the
/// retriable/non-retriable matrix through the decision's own <c>(bool, string)</c>
/// overload — which is the overload production calls into (definition-of-done.md §12),
/// not a parallel reimplementation.
/// </para>
/// </summary>
public sealed class SoakErrorClassificationTests
{
    [Fact]
    public void ErrorMessagePrefersTheExceptionMessage()
    {
        Assert.Equal("not the coordinator", SoakClient.ErrorMessage(new KafkaException("not the coordinator")));
        Assert.Equal("plain failure", SoakClient.ErrorMessage(new InvalidOperationException("plain failure")));
    }

    [Fact]
    public void ErrorCodeIsReadFromAKafkaException()
    {
        // A KafkaException always carries a code (0 through the public constructor);
        // what matters is that it is NOT null, so the classifier can branch on it.
        Assert.NotNull(SoakClient.ErrorCode(new KafkaException("x")));
    }

    [Theory]
    [InlineData("no code here")]
    [InlineData("also none")]
    public void ErrorCodeIsNullWithoutAKafkaException(string message)
    {
        Assert.Null(SoakClient.ErrorCode(new InvalidOperationException(message)));
    }

    [Fact]
    public void ErrorIsRetriableIsFalseForANonKafkaException()
    {
        Assert.False(SoakClient.ErrorIsRetriable(new InvalidOperationException("no such property")));
        Assert.False(SoakClient.ErrorIsRetriable(new KafkaException("not flagged retriable")));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(19)]
    public void RetriablePollFailuresBelowTheBoundKeepGoing(int consecutive)
    {
        Assert.False(SoakClient.PollFailureIsTerminal(true, "NetworkException", consecutive, 20, out string reason));
        Assert.Equal(string.Empty, reason);
    }

    [Fact]
    public void RetriablePollFailuresAtTheBoundTerminate()
    {
        Assert.True(SoakClient.PollFailureIsTerminal(true, "NetworkException", 20, 20, out string reason));
        Assert.Contains("20 consecutive", reason, StringComparison.Ordinal);
        Assert.DoesNotContain("non-retriable", reason, StringComparison.Ordinal);
        Assert.Contains("NetworkException", reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE CRITICAL REGRESSION GUARD for the rolling profiles. Every <i>client-side</i>
    /// error — Timeout, Wakeup, IllegalState — reports <c>UnknownServerError</c>, which
    /// <c>Errors::is_retriable()</c> excludes. A routine poll timeout during a broker roll
    /// therefore looks non-retriable, and escalating on the first one would kill the soak
    /// precisely when it is supposed to be proving it survives.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public void ASingleNonRetriablePollFailureIsNotFatal(int consecutive)
    {
        Assert.True(consecutive < SoakClient.NonRetriablePollFailureLimit);
        Assert.False(SoakClient.PollFailureIsTerminal(false, "Timeout waiting for the coordinator", consecutive, 20, out _));
    }

    [Fact]
    public void RepeatedNonRetriablePollFailuresTerminateSooner()
    {
        Assert.True(SoakClient.PollFailureIsTerminal(
            false, "TopicAuthorizationFailed", SoakClient.NonRetriablePollFailureLimit, 20, out string reason));
        Assert.Contains("non-retriable", reason, StringComparison.Ordinal);
        Assert.Contains("TopicAuthorizationFailed", reason, StringComparison.Ordinal);
    }

    /// <summary>
    /// A configured bound of 2 must not be RAISED to 3 by the non-retriable tier — the
    /// tier is <c>min(limit, configured)</c>, never a floor.
    /// </summary>
    [Fact]
    public void TheNonRetriableTierNeverExceedsTheConfiguredBound()
    {
        Assert.True(SoakClient.PollFailureIsTerminal(false, "auth", 2, 2, out _));
    }

    /// <summary>
    /// The Exception overload production actually calls must forward both facts — a
    /// non-Kafka exception is non-retriable, and its message reaches the fatal reason.
    /// Without this the matrix above would be testing a function nothing calls.
    /// </summary>
    [Fact]
    public void TheExceptionOverloadForwardsToTheDecision()
    {
        Exception ex = new InvalidOperationException("Timeout waiting for the coordinator");

        Assert.False(SoakClient.PollFailureIsTerminal(ex, 1, 20, out _));
        Assert.True(SoakClient.PollFailureIsTerminal(ex, SoakClient.NonRetriablePollFailureLimit, 20, out string reason));
        Assert.Contains("non-retriable", reason, StringComparison.Ordinal);
        Assert.Contains("Timeout waiting for the coordinator", reason, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------
    // 74.3 — a poll failure inside the shutdown window is not counted
    // ---------------------------------------------------------------------------

    /// <summary>
    /// ⚠ THE 74.3 GUARD. Python checks <c>if not self.run: break</c> <b>before</b>
    /// <c>_classify_error</c> (<c>soakclient.py:1689-1691</c>). Without that ordering a
    /// genuine broker error that merely resolves the poll inside the shutdown window is
    /// counted as a consumer error — and possibly a disconnect or a coordinator move —
    /// inflating the SUMMARY's <c>errors=</c> on an otherwise clean shutdown.
    /// </summary>
    [Fact]
    public void APollFailureDuringShutdownIsSuppressed()
    {
        Assert.Equal(
            SoakClient.PollFailureAction.Suppress,
            SoakClient.ClassifyPollFailure(true, true, "NetworkException", 1, 20, out string reason));
        Assert.Equal(string.Empty, reason);
    }

    /// <summary>
    /// Suppression outranks the terminal bound. The run is ending anyway, and recording
    /// a fatal reason here would turn a clean exit into
    /// <see cref="SoakExitCodes.ConsumerWedged"/> — so an error that WOULD abort is
    /// still suppressed once shutdown has been requested. This is the case a naive
    /// "check the token last" fix would get wrong while still passing the test above.
    /// </summary>
    [Fact]
    public void ShutdownSuppressesEvenAnOtherwiseTerminalFailure()
    {
        Assert.Equal(
            SoakClient.PollFailureAction.Abort,
            SoakClient.ClassifyPollFailure(false, true, "NetworkException", 20, 20, out _));

        Assert.Equal(
            SoakClient.PollFailureAction.Suppress,
            SoakClient.ClassifyPollFailure(true, true, "NetworkException", 20, 20, out string reason));
        Assert.Equal(string.Empty, reason);
    }

    /// <summary>
    /// Outside the shutdown window the policy is unchanged — it still delegates to the
    /// two-tier bound, so the fix added a case rather than replacing one.
    /// </summary>
    [Theory]
    [InlineData(true, 1, 20, "Continue")]
    [InlineData(true, 19, 20, "Continue")]
    [InlineData(true, 20, 20, "Abort")]
    [InlineData(false, 1, 20, "Continue")]
    [InlineData(false, 2, 20, "Continue")]
    [InlineData(false, 3, 20, "Abort")]
    public void OutsideShutdownThePolicyIsTheTwoTierBound(bool retriable, int consecutive, int max, string expected)
    {
        Assert.Equal(
            expected,
            SoakClient.ClassifyPollFailure(false, retriable, "err", consecutive, max, out _).ToString());
    }

    [Fact]
    public void AnAbortStillCarriesItsFatalReason()
    {
        Assert.Equal(
            SoakClient.PollFailureAction.Abort,
            SoakClient.ClassifyPollFailure(false, false, "TopicAuthorizationFailed", 3, 20, out string reason));
        Assert.Contains("non-retriable", reason, StringComparison.Ordinal);
        Assert.Contains("TopicAuthorizationFailed", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void TheMetricTokenSurvivesPrometheusNameTranslation()
    {
        // Prometheus metric names must match [a-zA-Z_:][a-zA-Z0-9_:]*, and the
        // OTLP->Prometheus translation replaces every other character with '_'. A token
        // carrying anything else would arrive under a name nobody can guess.
        Assert.Matches("^[a-zA-Z_][a-zA-Z0-9_]*$", SoakClient.SoakClientToken);
        Assert.Equal("kafka.client.soak.rust_dotnet.", SoakClient.MetricPrefix);
    }
}
