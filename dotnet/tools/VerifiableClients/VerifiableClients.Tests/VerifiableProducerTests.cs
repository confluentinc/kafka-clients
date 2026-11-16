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
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.Tools.Tests;

/// <summary>
/// Python's <c>test_verifiable_producer.py</c>: the Rust tool's tests over <c>MockProducer</c>, and the tool run
/// end to end against an unreachable broker.
/// </summary>
public sealed class VerifiableProducerTests : IDisposable
{
    private const string Tool = "VerifiableProducer";

    private readonly string _tmpPath = Directory.CreateDirectory(
        Path.Combine(Path.GetTempPath(), "verifiable-producer-tests-" + Guid.NewGuid().ToString("N"))).FullName;

    public void Dispose() => Directory.Delete(_tmpPath, recursive: true);

    [UnixFact]
    public async Task ScriptShutsDownCleanlyOnSigterm()
    {
        string config = WriteConfig("max.block.ms=200\n");
        using ToolProcess process = ToolProcess.Start(
            Tool, "--topic", "t", "--bootstrap-server", "localhost:1", "--throughput", "0", "--command-config", config);
        var names = new List<string>();
        while (names.Count < 3)
        {
            names.Add(Name(await process.ReadLineAsync()));
        }

        Assert.Equal(new[] { "startup_complete", "producer_send_error", "producer_send_error" }, names);
        process.Signal("TERM");
        (int exitCode, string stdout, string stderr) = await process.WaitForExitAsync();

        Assert.True(exitCode == 0, stderr);
        List<JsonElement> lines = Lines(stdout);
        Assert.Equal(new[] { "shutdown_complete", "tool_data" }, lines.Select(Name));
        Assert.Equal(
            (2L, 0L, 0L),
            (lines[1].GetProperty("sent").GetInt64(), lines[1].GetProperty("acked").GetInt64(),
                lines[1].GetProperty("target_throughput").GetInt64()));
    }

    [UnixFact]
    public async Task ScriptSigtermReleasesABlockedSend()
    {
        string config = WriteConfig("max.block.ms=60000\n");
        using ToolProcess process = ToolProcess.Start(
            Tool, "--topic", "t", "--bootstrap-server", "localhost:1", "--command-config", config);
        Assert.Equal("startup_complete", Name(await process.ReadLineAsync()));
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        var signalled = Stopwatch.StartNew();
        process.Signal("TERM");
        (int exitCode, string stdout, string stderr) = await process.WaitForExitAsync();
        TimeSpan shutdown = signalled.Elapsed;

        Assert.True(exitCode == 0, stderr);
        Assert.True(shutdown < TimeSpan.FromSeconds(20), stderr);
        List<JsonElement> lines = Lines(stdout);
        List<string> names = lines.Select(Name).ToList();
        Assert.Equal(
            new[] { "producer_send_error", "shutdown_complete", "tool_data" }, names.OrderBy(n => n, StringComparer.Ordinal));
        Assert.True(names.IndexOf("shutdown_complete") < names.IndexOf("tool_data"), string.Join(",", names));
        JsonElement error = lines[names.IndexOf("producer_send_error")];
        Assert.Equal(
            ("0", "Producer closed while send in progress"),
            (error.GetProperty("value").GetString(), error.GetProperty("message").GetString()));
        JsonElement summary = lines[names.IndexOf("tool_data")];
        Assert.Equal((1L, 0L), (summary.GetProperty("sent").GetInt64(), summary.GetProperty("acked").GetInt64()));
    }

    private static string Name(string line) => Name(JsonDocument.Parse(line).RootElement);

    private static string Name(JsonElement line) => line.GetProperty("name").GetString()!;

    private static List<JsonElement> Lines(string stdout) =>
        stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonDocument.Parse(line).RootElement)
            .ToList();

    private string WriteConfig(string contents)
    {
        string path = Path.Combine(_tmpPath, "p.properties");
        File.WriteAllText(path, contents);
        return path;
    }
}
