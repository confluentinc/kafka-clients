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
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Hard-exits with <see cref="SoakExitCodes.ConsumerWedged"/> when shutdown wedges, as Python's
/// watchdog: a send parked on backpressure, <c>Flush</c> and the untimed <c>Close</c> cannot be
/// interrupted, and a wedge during a broker roll is transient, so <c>run.sh</c> restarts it.
/// </summary>
internal static class ShutdownWatchdog
{
    // exit is a parameter so the tests can assert the code without ending the test host.
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

/// <summary>
/// The soak client's entry point, a port of the Python soak's <c>main</c>; <c>--check</c> runs
/// only the startup preflight.
/// </summary>
internal static class Program
{
    private static async Task<int> Main(string[] args)
    {
        // RSS before any client exists, as Python's post-import baseline; the gap to the
        // post-construction baseline is the client's startup cost.
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
            // Unclassified, as Python: keep the stack trace, but exit transient so the supervisor
            // retries under its rapid-failure bound.
            Console.Error.WriteLine("soakclient: unexpected startup failure: " + ex);
            return SoakExitCodes.TransientStartup;
        }

        using var shutdownStarted = new ManualResetEventSlim(false);
        using var exited = new ManualResetEventSlim(false);

        var watchdog = new Thread(() => ShutdownWatchdog.Run(
            shutdownStarted,
            exited,
            options.ShutdownTimeoutSeconds,
            // Safe: Environment.Exit runs ProcessExit handlers on the watchdog thread, and the
            // soak's only one, in SoakSignals, never blocks.
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
            // Initial resource sample, as Python: without it the first window has no memory or CPU
            // gauges. Inside the try, as sampling can throw.
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

        // The outer shutdown guard: whatever teardown throws, the process exits with one of
        // SoakExitCodes' codes, which run.sh's restart policy keys off.
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
            // Set even if teardown threw: the verdict is decided, and the watchdog must not
            // overwrite it with ConsumerWedged.
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

    // The preflight run.sh blocks on uses the mocks, not a real client: the same P/Invoke surface
    // with no network, DNS or timeout risk.
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

            // The real startup path's validation, so a typo fails here, before a topic is created.
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
