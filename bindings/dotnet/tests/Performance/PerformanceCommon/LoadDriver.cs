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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;

namespace Confluent.Kafka.Performance;

/// <summary>
/// The optional in-container / local load driver for the consumer benchmark — the C# analog of
/// <c>consumer_performance_test.py</c>'s <c>spawn_producer</c>. When <c>KAFKA_BIN</c> is set the consumer
/// engine self-spawns <c>kafka-producer-perf-test.sh</c> to feed the topic with CreateTime-timestamped
/// records; otherwise an external producer must feed it. The SASL config is written in Java form to a
/// <c>.properties</c> file (a raw <c>--producer-props</c> token cannot carry the jaas value's multiple
/// <c>=</c> signs).
/// </summary>
public sealed class LoadDriver
{
    private readonly Process _process;
    private readonly string _propsPath;
    private readonly StreamWriter _log;
    private readonly object _logLock = new();

    private LoadDriver(Process process, string propsPath, StreamWriter log)
    {
        _process = process;
        _propsPath = propsPath;
        _log = log;
    }

    /// <summary>
    /// Starts the load driver when <c>KAFKA_BIN</c> is set, else returns <see langword="null"/>. Sizes the
    /// record count to cover warmup + measure + a 30 s pad (Python's <c>pad</c>), unless <c>NUM_MESSAGES</c>
    /// bounds it.
    /// </summary>
    public static LoadDriver? MaybeStart(ConsumerBenchmarkConfig config)
    {
        if (string.IsNullOrEmpty(config.KafkaBin))
        {
            return null;
        }

        int pad = config.WarmupSeconds + config.TestDurationSeconds + 30;
        long total = config.NumMessages > 0 ? config.NumMessages : (long)config.Throughput * pad;

        var props = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = config.BootstrapServers,
            ["acks"] = "1",
        };
        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Java))
        {
            props[kv.Key] = kv.Value;
        }

        string propsPath = Path.Combine(Path.GetTempPath(), $"producer_perf_{Guid.NewGuid():N}.properties");
        using (var writer = new StreamWriter(propsPath, append: false))
        {
            foreach (KeyValuePair<string, string> kv in props)
            {
                // A Java .properties value must be one logical line; collapse whitespace runs (the jaas
                // value carries cosmetic "\n\t" separators) to single spaces before writing.
                string oneLine = CollapseWhitespace(kv.Value);
                writer.WriteLine($"{kv.Key}={oneLine}");
            }
        }

        var startInfo = new ProcessStartInfo
        {
            FileName = Path.Combine(config.KafkaBin!, "kafka-producer-perf-test.sh"),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        startInfo.ArgumentList.Add("--topic");
        startInfo.ArgumentList.Add(config.Topic);
        startInfo.ArgumentList.Add("--num-records");
        startInfo.ArgumentList.Add(total.ToString(CultureInfo.InvariantCulture));
        startInfo.ArgumentList.Add("--record-size");
        startInfo.ArgumentList.Add(config.MessageSize.ToString(CultureInfo.InvariantCulture));
        startInfo.ArgumentList.Add("--throughput");
        startInfo.ArgumentList.Add(config.Throughput.ToString(CultureInfo.InvariantCulture));
        startInfo.ArgumentList.Add("--producer.config");
        startInfo.ArgumentList.Add(propsPath);

        Console.WriteLine($">>> Launching producer: throughput={config.Throughput} msg/s, {config.MessageSize} bytes, ~{total} records");

        // Capture the feeder's stdout+stderr to producer.log (cwd is the results dir) instead of leaving
        // the pipes undrained — the C# analog of Python's spawn_producer opening "producer.log" and
        // merging stderr into it. A silent producer failure looks identical to "no data" on the consumer
        // side, so its output must remain inspectable; undrained, the OS pipe buffer (~64 KiB) fills once
        // kafka-producer-perf-test.sh's periodic progress lines accumulate, and the feeder BLOCKS on the
        // next write — starving the very consumer benchmark it exists to feed.
        var log = new StreamWriter(Path.Combine(Environment.CurrentDirectory, "producer.log"), append: false)
        {
            AutoFlush = true,
        };
        var process = new Process { StartInfo = startInfo };
        var driver = new LoadDriver(process, propsPath, log);
        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                lock (driver._logLock)
                {
                    log.WriteLine(e.Data);
                }
            }
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is not null)
            {
                lock (driver._logLock)
                {
                    log.WriteLine(e.Data);
                }
            }
        };
        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        return driver;
    }

    /// <summary>Kills the load-driver process and removes its temporary properties file (best-effort).</summary>
    public void Stop()
    {
        try
        {
            if (!_process.HasExited)
            {
                _process.Kill();
            }

            // The parameterless overload additionally waits for the redirected-output event handlers to
            // finish draining the pipes (MSDN: needed for that guarantee; the timed overload alone does
            // not give it). Best-effort: a process Kill() should already have made this fast.
            _process.WaitForExit(5000);
            _process.WaitForExit();
        }
        catch (Exception)
        {
            // Best-effort teardown (Python swallows the same).
        }
        finally
        {
            lock (_logLock)
            {
                _log.Flush();
            }

            _log.Dispose();
            _process.Dispose();
            try
            {
                File.Delete(_propsPath);
            }
            catch (IOException)
            {
                // Best-effort cleanup.
            }
        }
    }

    private static string CollapseWhitespace(string value)
    {
        var sb = new StringBuilder(value.Length);
        bool inWhitespace = false;
        foreach (char c in value)
        {
            if (char.IsWhiteSpace(c))
            {
                inWhitespace = true;
                continue;
            }

            if (inWhitespace && sb.Length > 0)
            {
                sb.Append(' ');
            }

            inWhitespace = false;
            sb.Append(c);
        }

        return sb.ToString();
    }
}
