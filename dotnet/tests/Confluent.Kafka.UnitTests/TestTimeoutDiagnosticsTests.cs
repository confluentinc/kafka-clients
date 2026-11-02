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

    [Fact]
    public void NativeStacks_AreSampledOnlyOnMacOS_ForALongBound_IntoAGivenFolder()
    {
        TimeSpan longBound = TimeSpan.FromSeconds(30);

        Assert.True(TestTimeout.NativeStacks.ShouldSample(longBound, "/tmp/hangs", isMacOs: true));
        Assert.True(TestTimeout.NativeStacks.ShouldSample(TestTimeout.NativeStacks.MinimumBound, "/tmp/hangs", isMacOs: true));
        Assert.False(TestTimeout.NativeStacks.ShouldSample(s_short, "/tmp/hangs", isMacOs: true));
        Assert.False(TestTimeout.NativeStacks.ShouldSample(longBound, null, isMacOs: true));
        Assert.False(TestTimeout.NativeStacks.ShouldSample(longBound, string.Empty, isMacOs: true));
        Assert.False(TestTimeout.NativeStacks.ShouldSample(longBound, "/tmp/hangs", isMacOs: false));
    }

    [Fact]
    public void NativeStacks_KeepTheThreadsInTheNativeLibrary_TheClosingOneFirst_WithoutAddresses()
    {
        string[] sample =
        {
            "Analysis of sampling dotnet (pid 1) every 1 millisecond",
            "Call graph:",
            "    10 Thread_1   DispatchQueue_1: com.apple.main-thread  (serial)",
            "    + 10 start  (in dyld) + 1904  [0x1]",
            "    +   10 main  (in dotnet) + 10  [0x2]",
            "    10 Thread_2: tokio-runtime-worker",
            "    + 10 thread_start  (in libsystem_pthread.dylib) + 8  [0x3]",
            "    +   10 tokio::runtime::scheduler::multi_thread::worker::run::h1  (in libconfluent_kafka.dylib) + 20  [0x4]",
            "    +     10 __psynch_cvwait  (in libsystem_kernel.dylib) + 8  [0x5]",
            "    10 Thread_3: .NET TP Worker",
            "    + 10 ???  (in <unknown binary>)  [0x6]",
            "    +   10 kafka_admin_Admin_destroy_err  (in libconfluent_kafka.dylib) + 48  [0x7]",
            "    +   ! 10 tokio::runtime::blocking::shutdown::Receiver::wait::h2  (in libconfluent_kafka.dylib) + 52  [0x8]",
            "    +   !   10 __psynch_cvwait  (in libsystem_kernel.dylib) + 8  [0x9]",
            string.Empty,
            "Total number in stack (recursive counted multiple, when >=5):",
            "    10 Thread_9: not in the call graph  (in libconfluent_kafka.dylib)",
        };

        string expected = string.Join(
            Environment.NewLine,
            "10 Thread_3: .NET TP Worker",
            "  10 ???  (in <unknown binary>)",
            "  10 kafka_admin_Admin_destroy_err  (in libconfluent_kafka.dylib)",
            "  10 tokio::runtime::blocking::shutdown::Receiver::wait::h2  (in libconfluent_kafka.dylib)",
            "  10 __psynch_cvwait  (in libsystem_kernel.dylib)",
            "10 Thread_2: tokio-runtime-worker",
            "  10 thread_start  (in libsystem_pthread.dylib)",
            "  10 tokio::runtime::scheduler::multi_thread::worker::run::h1  (in libconfluent_kafka.dylib)",
            "  10 __psynch_cvwait  (in libsystem_kernel.dylib)");
        Assert.Equal(expected, TestTimeout.NativeStacks.Summarize(sample));
    }

    [Fact]
    public void NativeStacks_WithoutAThreadInTheNativeLibrary_SaySo()
    {
        string[] sample =
        {
            "Call graph:",
            "    10 Thread_1   DispatchQueue_1: com.apple.main-thread  (serial)",
            "    + 10 start  (in dyld) + 1904  [0x1]",
            string.Empty,
        };

        Assert.Equal("(no thread inside the native library)", TestTimeout.NativeStacks.Summarize(sample));
    }

    private static string Escaped(TimeSpan timeout) => Regex.Escape(timeout.ToString());
}
