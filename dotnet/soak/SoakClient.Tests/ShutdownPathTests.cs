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
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// The shutdown path's two invariants: the final metrics window can fail without taking the
/// SUMMARY verdict or the exit-code contract with it, and the delivery-accounting drain is bounded.
/// </summary>
public sealed class ShutdownPathTests
{
    private const string Prefix = "kafka.client.soak.rust_dotnet.";

    /// <summary>A writer whose <c>WriteLine</c> always fails — a full disk, in effect.</summary>
    private sealed class ThrowingWriter : StringWriter
    {
        public override void WriteLine(string? value) => throw new IOException("no space left on device");
    }

    private static SoakMetrics NewMetrics(TextWriter writer) =>
        new SoakMetrics(
            writer,
            new Dictionary<string, string>(StringComparer.Ordinal),
            Prefix,
            null,
            new SoakLogger(SoakLogLevel.Fatal));

    [Fact]
    public void FinalizeMetricsAbsorbsAFailingWriter()
    {
        using var writer = new ThrowingWriter();
        using SoakMetrics metrics = NewMetrics(writer);
        bool sampled = false;

        Exception? thrown = Xunit.Record.Exception(
            () => SoakClient.FinalizeMetrics(metrics, () => sampled = true, new SoakLogger(SoakLogLevel.Fatal)));

        Assert.Null(thrown);
        Assert.True(sampled, "the resource sample must still be taken before the write fails");
    }

    [Fact]
    public void FinalizeMetricsAbsorbsAFailingResourceSample()
    {
        using var writer = new StringWriter();
        using SoakMetrics metrics = NewMetrics(writer);

        Exception? thrown = Xunit.Record.Exception(() => SoakClient.FinalizeMetrics(
            metrics,
            () => throw new InvalidOperationException("Process.Refresh failed"),
            new SoakLogger(SoakLogLevel.Fatal)));

        Assert.Null(thrown);
    }

    [Fact]
    public void FinalizeMetricsWritesTheFinalWindowWhenNothingFails()
    {
        using var writer = new StringWriter();
        using SoakMetrics metrics = NewMetrics(writer);

        SoakClient.FinalizeMetrics(metrics, () => { }, new SoakLogger(SoakLogLevel.Fatal));

        Assert.Contains("\"prefix\": \"" + Prefix + "\"", writer.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0L, null, SoakExitCodes.Ok)]
    [InlineData(1L, null, SoakExitCodes.MessageLoss)]
    [InlineData(0L, "poll wedged", SoakExitCodes.ConsumerWedged)]
    [InlineData(7L, "poll wedged", SoakExitCodes.MessageLoss)]
    public void ExitCodeForRanksMessageLossFirst(long missed, string? fatalReason, int expected)
    {
        Assert.Equal(expected, SoakExitCodes.ExitCodeFor(missed, fatalReason));
    }

    [Fact]
    public async Task DrainCounterIsBoundedWhenTheCounterNeverReachesZero()
    {
        var clock = Stopwatch.StartNew();
        long remaining = await SoakClient.DrainCounterAsync(() => 3, TimeSpan.FromMilliseconds(150));
        clock.Stop();

        Assert.Equal(3, remaining);
        Assert.True(
            clock.Elapsed < TimeSpan.FromSeconds(5),
            "the drain overran its bound by a wide margin (" + clock.Elapsed + ")");
    }

    [Fact]
    public async Task DrainCounterReturnsImmediatelyWhenAlreadyZero()
    {
        int reads = 0;
        var clock = Stopwatch.StartNew();
        long remaining = await SoakClient.DrainCounterAsync(
            () => { reads++; return 0; },
            TimeSpan.FromSeconds(30));
        clock.Stop();

        Assert.Equal(0, remaining);
        Assert.Equal(1, reads);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5), "a drained counter must not wait");
    }

    [Fact]
    public async Task DrainCounterWaitsForALateContinuation()
    {
        long pending = 2;
        using var released = new ManualResetEventSlim(false);

        var releaser = new Thread(() =>
        {
            Thread.Sleep(40);
            Interlocked.Exchange(ref pending, 0);
            released.Set();
        })
        {
            IsBackground = true,
        };
        releaser.Start();

        long remaining = await SoakClient.DrainCounterAsync(
            () => Interlocked.Read(ref pending),
            TimeSpan.FromSeconds(10));

        Assert.True(released.IsSet, "the drain returned before the late continuation ran");
        Assert.Equal(0, remaining);
        releaser.Join();
    }

    [Fact]
    public void TheDeliveryDrainBoundIsPositiveAndBounded()
    {
        Assert.InRange(SoakClient.DeliveryDrainBoundMs, 1, 60000);
    }
}
