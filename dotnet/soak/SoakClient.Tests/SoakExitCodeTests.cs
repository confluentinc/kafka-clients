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
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// The exit-code contract shared with <c>run.sh</c>, and the shutdown watchdog that
/// produces one of them.
/// </summary>
public sealed class SoakExitCodeTests
{
    [Fact]
    public void ExitCodesArePinned()
    {
        Assert.Equal(0, SoakExitCodes.Ok);
        Assert.Equal(1, SoakExitCodes.MessageLoss);
        Assert.Equal(2, SoakExitCodes.Fatal);
        Assert.Equal(3, SoakExitCodes.TransientStartup);
        Assert.Equal(4, SoakExitCodes.ConsumerWedged);
    }

    [Fact]
    public void ExitCodesAreDistinct()
    {
        var codes = new List<int>
        {
            SoakExitCodes.Ok,
            SoakExitCodes.MessageLoss,
            SoakExitCodes.Fatal,
            SoakExitCodes.TransientStartup,
            SoakExitCodes.ConsumerWedged,
        };

        Assert.Equal(codes.Count, new HashSet<int>(codes).Count);
    }

    /// <summary>
    /// <c>run.sh</c> keys "never restart" off this exact number; drift would silently
    /// restore the crash loop.
    /// </summary>
    [Fact]
    public void RunShAgreesOnTheFatalExitCode()
    {
        string runSh = File.ReadAllText(Path.Combine(SoakTestPaths.SoakDirectory(), "run.sh"));
        Match match = Regex.Match(runSh, @"^EXIT_FATAL=(\d+)$", RegexOptions.Multiline);

        Assert.True(match.Success, "run.sh no longer defines EXIT_FATAL");
        Assert.Equal(SoakExitCodes.Fatal, int.Parse(match.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// A wedged shutdown (backpressure during a broker roll) is transient: <c>run.sh</c>
    /// must see <see cref="SoakExitCodes.ConsumerWedged"/>, not
    /// <see cref="SoakExitCodes.Fatal"/>, or a real broker roll gets treated as permanent
    /// and the soak never comes back on its own.
    /// </summary>
    [Fact]
    public void ShutdownWatchdogHardExitsWithConsumerWedgedNotFatal()
    {
        var exitCodes = new List<int>();
        using var shutdownStarted = new ManualResetEventSlim(false);
        using var exited = new ManualResetEventSlim(false);

        shutdownStarted.Set();      // shutdown already underway
        // `exited` is deliberately never set: the shutdown is wedged.
        ShutdownWatchdog.Run(shutdownStarted, exited, 0.05, exitCodes.Add);

        Assert.Equal(new[] { SoakExitCodes.ConsumerWedged }, exitCodes);
        Assert.DoesNotContain(SoakExitCodes.Fatal, exitCodes);
    }

    [Fact]
    public void ShutdownWatchdogDoesNotFireWhenShutdownCompletesInTime()
    {
        var exitCodes = new List<int>();
        using var shutdownStarted = new ManualResetEventSlim(false);
        using var exited = new ManualResetEventSlim(false);

        shutdownStarted.Set();
        exited.Set();               // shutdown finished well within the timeout
        ShutdownWatchdog.Run(shutdownStarted, exited, 5.0, exitCodes.Add);

        Assert.Empty(exitCodes);
    }
}

/// <summary>Locates the checked-in soak directory from the test assembly's output path.</summary>
internal static class SoakTestPaths
{
    /// <summary>
    /// Walks up from the test binaries until a directory containing <c>run.sh</c> is
    /// found. Deliberately structural rather than a hard-coded relative depth, so moving
    /// the output one level does not silently turn the run.sh contract test into a
    /// file-not-found.
    /// </summary>
    internal static string SoakDirectory()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "run.sh")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "could not locate the soak directory (no ancestor of " + AppContext.BaseDirectory + " contains run.sh)");
    }
}
