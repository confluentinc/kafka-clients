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
/// The consumer benchmark's optional load driver, Python's <c>spawn_producer</c>: with <c>KAFKA_BIN</c> set it runs
/// <c>kafka-producer-perf-test.sh</c> to feed the topic; otherwise an external producer must feed it.
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
        if (config.CompressionType is not null)
        {
            props["compression.type"] = config.CompressionType;
        }

        foreach (KeyValuePair<string, string> kv in SaslConfig.FromEnv(SaslForm.Java))
        {
            props[kv.Key] = kv.Value;
        }

        string propsPath = Path.Combine(Path.GetTempPath(), $"producer_perf_{Guid.NewGuid():N}.properties");
        using (var writer = new StreamWriter(propsPath, append: false))
        {
            foreach (KeyValuePair<string, string> kv in props)
            {
                // A .properties value must be one line; the jaas value carries "\n\t" separators.
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

        Console.WriteLine($">>> Launching producer: throughput={config.Throughput} msg/s, {config.MessageSize} bytes, ~{total} records, compression={config.CompressionType ?? "default"}");

        // Drained into producer.log, as Python's: an undrained pipe fills and blocks the feeder,
        // and a feeder failure must stay inspectable.
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

    public void Stop()
    {
        try
        {
            if (!_process.HasExited)
            {
                _process.Kill();
            }

            // Only the parameterless overload also waits for the output handlers to drain.
            _process.WaitForExit(5000);
            _process.WaitForExit();
        }
        catch (Exception)
        {
            // Best-effort teardown, as in Python.
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
