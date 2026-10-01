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
//
// Soak test producer-consumer end-to-end client for long term validation of the Rust
// Kafka client through its .NET binding.
//
// Modelled on bindings/python/soak/soakclient.py, which is itself modelled on
// confluent-kafka-python's tests/soak/soakclient.py. The structure (SoakRecord, the two
// loops, prefix-routed configuration, per-partition high-water-mark bookkeeping,
// counters/gauges, resource sampling, periodic status lines) is ported faithfully; the
// client call sites are not, because this binding mirrors the Java API.
//
// Usage:
//   SOAK_TESTID=<id> SOAK_TOPIC=<topic> SOAK_CONFIG_FILE=<file> dotnet SoakClient.dll
//   dotnet SoakClient.dll --check      # startup preflight, creates nothing
//
// A unique topic should be used for each soak instance.
//
// Exit codes (a contract with run.sh, which keys its restart policy off them) live in
// SoakExitCodes and are pinned by SoakExitCodeTests.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Hard-exits if shutdown wedges.
/// <para>
/// The producer has no <c>wakeup()</c>: a send parked on backpressure, <c>Flush</c> and
/// <c>Close</c> are all uninterruptible. Four soaks share one box, so a wedged shutdown
/// must not need a human.
/// </para>
/// <para>
/// Exits <see cref="SoakExitCodes.ConsumerWedged"/>, <b>not</b>
/// <see cref="SoakExitCodes.Fatal"/>: a shutdown that wedges on backpressure during a
/// broker roll is transient, not a permanent failure, and <c>run.sh</c> only restarts
/// non-fatal codes.
/// </para>
/// </summary>
internal static class ShutdownWatchdog
{
    /// <summary>
    /// Waits for shutdown to start, then for it to finish; hard-exits through
    /// <paramref name="exit"/> if it does not finish within
    /// <paramref name="timeoutSeconds"/>. The exit action is a parameter purely so the
    /// unit tests can assert the code without terminating the test host.
    /// </summary>
    internal static void Run(
        ManualResetEventSlim shutdownStarted,
        ManualResetEventSlim exited,
        double timeoutSeconds,
        Action<int> exit)
    {
        shutdownStarted.Wait();
        if (!exited.Wait(TimeSpan.FromSeconds(timeoutSeconds)))
        {
            Console.Error.WriteLine("Shutdown watchdog expired, hard-exiting");
            Console.Error.Flush();
            exit(SoakExitCodes.ConsumerWedged);
        }
    }
}

/// <summary>The soak client's entry point.</summary>
internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        // Process RSS before any Kafka client exists — the analog of the Python soak's
        // post-import baseline. The difference between this and the post-construction
        // baseline is what the client itself costs at startup.
        double rssAtStartupMiB = new ProcessResourceSampler().CurrentRssMiB();

        if (args.Length > 0 && string.Equals(args[0], "--check", StringComparison.Ordinal))
        {
            return Check();
        }

        SoakOptions options;
        IReadOnlyDictionary<string, string> fileConfig;
        try
        {
            options = SoakOptions.FromEnvironment();
            fileConfig = ReadConfigFile(options.ConfigFile);
        }
        catch (Exception ex) when (ex is ArgumentException or IOException)
        {
            // Configuration rejected at startup (a missing variable, a malformed config
            // line, an unreadable file). A stack trace adds nothing here.
            Console.Error.WriteLine("soakclient: configuration error: " + ex.Message);
            return SoakExitCodes.Fatal;
        }

        SoakClient soak;
        try
        {
            soak = await SoakClient.CreateAsync(options, fileConfig, rssAtStartupMiB).ConfigureAwait(false);
        }
        catch (ArgumentException ex)
        {
            // An unknown configuration key, rejected before the topic is created.
            Console.Error.WriteLine("soakclient: configuration error: " + ex.Message);
            return SoakExitCodes.Fatal;
        }
        catch (SoakFatalStartupException ex)
        {
            Console.Error.WriteLine("soakclient: fatal startup error: " + ex.Message);
            return SoakExitCodes.Fatal;
        }
        catch (SoakTransientStartupException ex)
        {
            Console.Error.WriteLine("soakclient: transient startup error: " + ex.Message);
            return SoakExitCodes.TransientStartup;
        }
        catch (Exception ex)
        {
            // Unclassified: keep the stack trace, since this is the case nobody has
            // diagnosed yet, but exit "transient" so the supervisor retries a few times
            // under its rapid-failure bound rather than stopping dead on something that
            // might be a flapping broker.
            Console.Error.WriteLine("soakclient: unexpected startup failure: " + ex);
            return SoakExitCodes.TransientStartup;
        }

        using var shutdownStarted = new ManualResetEventSlim(false);
        using var exited = new ManualResetEventSlim(false);

        var watchdog = new Thread(() => ShutdownWatchdog.Run(
            shutdownStarted,
            exited,
            options.ShutdownTimeoutSeconds,
            // ⚠ Environment.Exit runs AppDomain.ProcessExit handlers on the CALLING
            // thread, so a blocking handler would wedge the very path that exists to
            // un-wedge a wedged shutdown. SoakSignals installs the soak's ONLY ProcessExit
            // handler, and it does nothing but set a flag and invoke a non-blocking
            // callback (see the comment there) — which is what makes this safe. Whatever
            // happens, the process must exit with exactly ConsumerWedged.
            Environment.Exit))
        {
            IsBackground = true,
            Name = "watchdog",
        };
        watchdog.Start();

        SoakSignals.Install(() =>
        {
            Console.Out.WriteLine("Termination signal received, shutting down...");
            Console.Out.Flush();
            shutdownStarted.Set();
            soak.RequestStop();
        });

        DateTime? deadline = options.RuntimeSeconds > 0
            ? DateTime.UtcNow.AddSeconds(options.RuntimeSeconds)
            : (DateTime?)null;

        try
        {
            // Initial resource sample, as the reference does before its loop: without it
            // the first metrics window carries no memory/CPU gauges at all, because the
            // sampler and the window roll on the same cadence. Inside the try because
            // SampleResources touches Process.Refresh / GC.GetTotalMemory, which Python's
            // resource.getrusage() analog cannot throw from (74.1).
            soak.SampleResources();

            while (!soak.StopToken.IsCancellationRequested)
            {
                TimeSpan waitFor = TimeSpan.FromSeconds(10);
                if (deadline is DateTime limit)
                {
                    TimeSpan remaining = limit - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                    {
                        soak.Logger.Info(string.Format(
                            CultureInfo.InvariantCulture,
                            "Runtime limit of {0:F0}s reached",
                            options.RuntimeSeconds));
                        break;
                    }

                    if (remaining < waitFor)
                    {
                        waitFor = remaining;
                    }
                }

                soak.StopToken.WaitHandle.WaitOne(waitFor);
                soak.SampleResources();
            }
        }
        catch (Exception ex)
        {
            soak.Logger.Error("Fatal exception " + ex);
        }

        shutdownStarted.Set();

        // ⚠ 74.1 — the OUTER half of the shutdown guard. TerminateAsync has its own
        // no-throw boundary around the metrics close (SoakClient.FinalizeMetrics), so
        // this is the backstop for everything else on the teardown path: whatever
        // happens, the process must exit with one of SoakExitCodes' five, because run.sh
        // keys its restart policy off exactly those numbers. An escaping exception here
        // would exit with the runtime's unhandled-exception code, which run.sh reads as
        // an ordinary restartable failure — silently bypassing the never-restart and
        // message-loss distinctions the contract encodes.
        try
        {
            await soak.TerminateAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("soakclient: shutdown failed: " + ex);
        }
        finally
        {
            // In the finally: the soak's work IS over even if teardown threw, so leaving
            // `exited` unset would let the watchdog hard-exit ConsumerWedged 60 s later
            // and overwrite a verdict that was already decided.
            exited.Set();
        }

        try
        {
            soak.Dispose();
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("soakclient: dispose failed: " + ex);
        }

        return SoakExitCodes.ExitCodeFor(soak.MissedCount, soak.FatalReason);
    }

    /// <summary>
    /// The startup preflight <c>run.sh</c> blocks on. Proves the assembly resolves, the
    /// native library loads and the configuration is acceptable — then exits.
    /// <para>
    /// ⚠ It uses the MOCKS (PLAN D10), not a real client against a fake broker address:
    /// the mocks go through the same P/Invoke surface with no network, no DNS and no
    /// timeout risk, which is what a preflight the supervisor waits on needs.
    /// </para>
    /// </summary>
    private static int Check()
    {
        try
        {
            SoakOptions options = SoakOptions.FromEnvironment();
            IReadOnlyDictionary<string, string> fileConfig = ReadConfigFile(options.ConfigFile);

            var conf = new Dictionary<string, string>(fileConfig, StringComparer.Ordinal);
            if (options.Brokers is not null)
            {
                conf["bootstrap.servers"] = options.Brokers;
            }

            // Validate exactly what the real startup path validates, so a typo fails here
            // — before a topic is created and before a multi-day run begins on a default
            // value — rather than seconds later.
            var pconf = SoakConfig.FilterConfig(conf, new[] { "consumer.", "admin." }, "producer.");
            (pconf, _) = SoakConfig.RouteSharedConfig(pconf, SoakConfig.ProducerConfigKeys, SoakConfig.ConsumerConfigKeys);
            SoakConfig.ValidateConfig(pconf, SoakConfig.ProducerConfigKeys, "producer");

            var cconf = SoakConfig.FilterConfig(conf, new[] { "producer.", "admin." }, "consumer.");
            (cconf, _) = SoakConfig.RouteSharedConfig(cconf, SoakConfig.ConsumerConfigKeys, SoakConfig.ProducerConfigKeys);
            SoakConfig.ValidateConfig(cconf, SoakConfig.ConsumerConfigKeys, "consumer");

            using (var producer = new AsyncMockProducer<byte[], SoakRecord>(
                Serdes.ByteArray, new SoakRecordSerializer(options.PayloadSize)))
            {
                using var consumer = new MockConsumer<byte[], SoakRecord>(
                    Serdes.ByteArray, new SoakRecordDeserializer());
            }

            Console.Out.WriteLine(string.Format(
                CultureInfo.InvariantCulture,
                "soakclient: --check OK (testid={0}, topic={1}, variant={2}, payload={3} B, rate={4} msg/s)",
                options.TestId,
                options.Topic,
                options.Variant,
                options.PayloadSize,
                options.Rate));
            return SoakExitCodes.Ok;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("soakclient: --check FAILED: " + ex);
            return SoakExitCodes.Fatal;
        }
    }

    private static IReadOnlyDictionary<string, string> ReadConfigFile(string? path)
    {
        if (path is null)
        {
            return new Dictionary<string, string>(StringComparer.Ordinal);
        }

        using var reader = new StreamReader(path);
        return SoakConfig.ParseConfigFile(reader);
    }
}
