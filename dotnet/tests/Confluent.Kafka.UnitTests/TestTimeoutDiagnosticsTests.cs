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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the diagnostics a <see cref="TestTimeout"/> timeout ends with: whether the action started and when, then the
/// thread pool's and the process's thread counts.
/// </summary>
public sealed class TestTimeoutDiagnosticsTests
{
    private static readonly TimeSpan s_short = TimeSpan.FromMilliseconds(200);
    private const string PoolAndProcess = @"thread pool: (\d+ threads, \d+ queued work items, )?\d+ of \d+ workers busy, minimum \d+; process: (\d+|unknown) threads, \d+ processors\.$";

    [Fact]
    public void AProbeThatNeverStarted_SaysSo()
    {
        TestTimeout.StartProbe probe = new TestTimeout.StartProbe();

        Assert.Matches(@"^Diagnostics: the action never started in \d+ ms; " + PoolAndProcess, TestTimeout.StartProbe.Describe(probe));
    }

    [Fact]
    public void AProbeThatStarted_SaysWhenAndHowLongTheActionRan()
    {
        TestTimeout.StartProbe probe = new TestTimeout.StartProbe();
        probe.Start();

        Assert.Matches(
            @"^Diagnostics: the action started \d+ ms after it was queued and ran \d+ ms; " + PoolAndProcess,
            TestTimeout.StartProbe.Describe(probe));
    }

    [Fact]
    public void WithoutAProbe_OnlyThePoolAndTheProcessAreDescribed()
    {
        Assert.Matches("^Diagnostics: " + PoolAndProcess, TestTimeout.StartProbe.Describe(null));
    }

    [Fact]
    public void Run_ThatTimesOut_EndsItsMessageWithTheDiagnostics()
    {
        ManualResetEventSlim gate = new ManualResetEventSlim();
        try
        {
            TimeoutException timedOut = Assert.Throws<TimeoutException>(() => TestTimeout.Run(() => gate.Wait(), s_short));

            Assert.Matches(
                $@"^Operation did not complete within {Escaped(s_short)} — treated as a hang \(fail fast\)\. Diagnostics: the action "
                + @"(never started in \d+ ms|started \d+ ms after it was queued and ran \d+ ms); " + PoolAndProcess,
                timedOut.Message);
        }
        finally
        {
            gate.Set();
        }
    }

    [Fact]
    public async Task TheAsyncRun_ThatTimesOut_DescribesThePoolAndTheProcess()
    {
        TimeoutException timedOut = await Assert.ThrowsAsync<TimeoutException>(
            () => TestTimeout.Run(() => new TaskCompletionSource<bool>().Task, s_short));

        Assert.Matches(
            $@"^Operation did not complete within {Escaped(s_short)} — treated as a hang \(fail fast\)\. Diagnostics: "
            + PoolAndProcess,
            timedOut.Message);
    }

    private static string Escaped(TimeSpan timeout) => Regex.Escape(timeout.ToString());
}
