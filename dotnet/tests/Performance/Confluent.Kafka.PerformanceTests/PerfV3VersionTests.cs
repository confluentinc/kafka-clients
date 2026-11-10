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
using System.Threading.Tasks;

using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// Covers <c>MODE=version</c>: PerfV3 prints its client, library and runtime lines and exits 0 without a broker, and
/// the runtime it reports is the one its project file pins (Workstation GC, concurrent GC, tiered PGO).
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class PerfV3VersionTests
{
    // Runtime overrides a caller's environment could carry, which would change what the child reports.
    private static readonly string[] s_runtimeOverrides =
    {
        "DOTNET_TieredPGO", "DOTNET_gcServer", "DOTNET_gcConcurrent",
        "COMPlus_TieredPGO", "COMPlus_gcServer", "COMPlus_gcConcurrent",
    };

    private readonly ITestOutputHelper _output;

    public PerfV3VersionTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Theory]
    [InlineData("False", "dotnet-rust")]
    [InlineData("True", "dotnet-rust-async")]
    public async Task Version_PrintsTheClientAndThePinnedRuntime(string asyncMode, string client)
    {
        PerfEngineFixture.Apply(new Dictionary<string, string>(StringComparer.Ordinal));
        var env = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["MODE"] = "version",
            ["ASYNC"] = asyncMode,
        };

        (int exitCode, string stdout, string stderr) =
            await PerfV3Process.RunAsync(_output, env, s_runtimeOverrides).ConfigureAwait(true);

        Assert.True(exitCode == 0, $"MODE=version exited {exitCode}; stdout: {stdout}; stderr: {stderr}");
        string[] lines = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        Assert.Equal($"client: {client}", lines[0]);
        Assert.StartsWith("confluent_kafka_version: ", lines[1], StringComparison.Ordinal);
        Assert.Equal($"dotnet_runtime: {Environment.Version}", lines[2]);
        Assert.StartsWith("framework: ", lines[3], StringComparison.Ordinal);
        Assert.Equal("server_gc: False", lines[4]);
        Assert.Equal("concurrent_gc: True", lines[5]);
        Assert.Equal("tiered_pgo: True", lines[6]);
        Assert.Equal(7, lines.Length);
    }
}
