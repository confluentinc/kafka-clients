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
/// Fills the EOS source topic before the measured interval with Kafka's <c>kafka-producer-perf-test.sh</c> from
/// <c>KAFKA_BIN</c>, the producer the consumer benchmarks use for their input. A failure only warns: the run then
/// relies on what the source topic already holds.
/// </summary>
public static class SourceSeeder
{
    public static bool Seed(TransactionalBenchmarkConfig config, long totalRecords)
    {
        string binPath = Path.Combine(config.KafkaBin ?? string.Empty, "kafka-producer-perf-test.sh");
        int recordSize = Math.Max(1, config.Producer.MessageSize);
        string propsPath;
        try
        {
            propsPath = WriteProperties(config.BootstrapServers);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            Console.Error.WriteLine($"WARN: could not write the seed producer config ({e.Message}); assuming SOURCE_TOPIC is pre-populated");
            return false;
        }

        string kind = config.SeedThroughput < 0 ? "PEAK/unbounded" : "fixed msg/s";
        Console.WriteLine($">>> Seeding source topic '{config.SourceTopic}' via {binPath}: throughput={config.SeedThroughput} ({kind}), {recordSize} bytes, {totalRecords} records");

        var start = new ProcessStartInfo(binPath)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        start.ArgumentList.Add("--topic");
        start.ArgumentList.Add(config.SourceTopic ?? string.Empty);
        start.ArgumentList.Add("--num-records");
        start.ArgumentList.Add(totalRecords.ToString(CultureInfo.InvariantCulture));
        start.ArgumentList.Add("--record-size");
        start.ArgumentList.Add(recordSize.ToString(CultureInfo.InvariantCulture));
        start.ArgumentList.Add("--throughput");
        start.ArgumentList.Add(config.SeedThroughput.ToString(CultureInfo.InvariantCulture));
        start.ArgumentList.Add("--producer.config");
        start.ArgumentList.Add(propsPath);

        try
        {
            // The output is kept, so a silent seeding failure (no data on the consumer side) stays inspectable.
            using var log = new StreamWriter("seed_producer.log", append: false, Encoding.UTF8);
            using var process = new Process { StartInfo = start };
            object gate = new();
            process.OutputDataReceived += (_, e) => Append(log, gate, e.Data);
            process.ErrorDataReceived += (_, e) => Append(log, gate, e.Data);
            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            process.WaitForExit();
            if (process.ExitCode != 0)
            {
                Console.Error.WriteLine($"WARN: kafka-producer-perf-test.sh exited {process.ExitCode}; see seed_producer.log (assuming SOURCE_TOPIC has data)");
                return false;
            }

            return true;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"WARN: could not spawn kafka-producer-perf-test.sh from KAFKA_BIN ({e.Message}); assuming SOURCE_TOPIC is pre-populated");
            return false;
        }
        finally
        {
            TryDelete(propsPath);
        }
    }

    // The spawned tool is a Java producer whatever this exe's client is, so SASL goes in the Java form. The file
    // holds the SASL secret, so it is readable by its owner only.
    private static string WriteProperties(string bootstrapServers)
    {
        var props = new List<KeyValuePair<string, string>>
        {
            new("bootstrap.servers", bootstrapServers),
            new("acks", "1"),
        };
        props.AddRange(SaslConfig.FromEnv(SaslForm.Java));

        string path = Path.Combine(Path.GetTempPath(), $"txn_seed_{Guid.NewGuid():N}.properties");
        var options = new FileStreamOptions { Mode = FileMode.CreateNew, Access = FileAccess.Write };
        if (!OperatingSystem.IsWindows())
        {
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        }

        using var writer = new StreamWriter(path, Encoding.ASCII, options);
        writer.Write(PropertiesText(props));
        return path;
    }

    /// <summary>Java <c>.properties</c> text for the pairs, each value escaped so <c>Properties.load</c> reads it back unchanged.</summary>
    public static string PropertiesText(IEnumerable<KeyValuePair<string, string>> props)
    {
        var text = new StringBuilder();
        foreach (KeyValuePair<string, string> kv in props)
        {
            text.Append(kv.Key).Append('=');
            AppendEscapedValue(text, kv.Value);
            text.Append('\n');
        }

        return text.ToString();
    }

    // Properties.load reads ISO-8859-1, treats '\' as an escape, ends a value at a line break and drops its leading
    // whitespace; '=', ':', '#' and '!' are literal inside a value.
    private static void AppendEscapedValue(StringBuilder text, string value)
    {
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            switch (c)
            {
                case '\\':
                    text.Append(@"\\");
                    break;
                case '\n':
                    text.Append(@"\n");
                    break;
                case '\r':
                    text.Append(@"\r");
                    break;
                case '\t':
                    text.Append(@"\t");
                    break;
                case '\f':
                    text.Append(@"\f");
                    break;
                case ' ' when i == 0:
                    text.Append(@"\ ");
                    break;
                default:
                    if (c < 0x20 || c > 0x7E)
                    {
                        text.Append(@"\u").Append(((int)c).ToString("X4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        text.Append(c);
                    }

                    break;
            }
        }
    }

    private static void Append(StreamWriter log, object gate, string? line)
    {
        if (line is null)
        {
            return;
        }

        lock (gate)
        {
            log.WriteLine(line);
        }
    }

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            // A leftover temp file is harmless.
        }
    }
}
