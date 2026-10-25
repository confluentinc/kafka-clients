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
/// Error inspection and the poll-failure bound that stops an unattended soak from consuming nothing
/// while looking alive; the retriable matrix goes through the decision's flag-and-message overload,
/// the one its <c>Exception</c> overload forwards to.
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

    [Fact]
    public void TheNonRetriableTierNeverExceedsTheConfiguredBound()
    {
        Assert.True(SoakClient.PollFailureIsTerminal(false, "auth", 2, 2, out _));
    }

    [Fact]
    public void TheExceptionOverloadForwardsToTheDecision()
    {
        Exception ex = new InvalidOperationException("Timeout waiting for the coordinator");

        Assert.False(SoakClient.PollFailureIsTerminal(ex, 1, 20, out _));
        Assert.True(SoakClient.PollFailureIsTerminal(ex, SoakClient.NonRetriablePollFailureLimit, 20, out string reason));
        Assert.Contains("non-retriable", reason, StringComparison.Ordinal);
        Assert.Contains("Timeout waiting for the coordinator", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void APollFailureDuringShutdownIsSuppressed()
    {
        Assert.Equal(
            SoakClient.PollFailureAction.Suppress,
            SoakClient.ClassifyPollFailure(true, true, "NetworkException", 1, 20, out string reason));
        Assert.Equal(string.Empty, reason);
    }

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
        Assert.Matches("^[a-zA-Z_][a-zA-Z0-9_]*$", SoakClient.SoakClientToken);
        Assert.Equal("kafka.client.soak.rust_dotnet.", SoakClient.MetricPrefix);
    }
}
