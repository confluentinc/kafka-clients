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
/// Guards <c>PerfEnv.Has</c> and <c>PerfEnv.ParseInt</c>, Python's <c>'NAME' in os.environ</c> and
/// <c>int(os.environ['NAME'])</c>: a set value parses, an unset one throws where <c>GetInt</c> falls back, and an
/// empty one counts as set and fails to parse. .NET deletes a variable set to an empty value in-process, so the empty
/// case runs PerfV3 as a child process with an empty <c>BATCH_SIZE</c>.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class PerfEnvParseIntTests
{
    // MAX_REQUEST_SIZE is read right after BATCH_SIZE: its unparsable value ends the child there, before any broker.
    private const string StopMarker = "x";

    private const string EmptyValueFailure = "System.FormatException: The input string '' was not in a correct format.";

    private const string StopMarkerFailure = "System.FormatException: The input string 'x' was not in a correct format.";

    private readonly ITestOutputHelper _output;

    public PerfEnvParseIntTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void ParseInt_ParsesASetValue()
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal) { ["KEY_SIZE"] = "12" });

        Assert.True(PerfEnv.Has("KEY_SIZE"));
        Assert.Equal(12, PerfEnv.ParseInt("KEY_SIZE"));
    }

    [Fact]
    public void ParseInt_Unset_ThrowsWhereGetIntFallsBack()
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal));

        Assert.False(PerfEnv.Has("KEY_SIZE"));
        Assert.Equal(5, PerfEnv.GetInt("KEY_SIZE", 5));
        FormatException e = Assert.Throws<FormatException>(() => PerfEnv.ParseInt("KEY_SIZE"));
        Assert.Equal("The input string '' was not in a correct format.", e.Message);
    }

    [Fact]
    public async Task Has_EmptyValue_CountsAsSetAndFailsToParse_InAPerfV3Child()
    {
        (int exitCode, string stderr) = await RunProducerConfigAsync(batchSize: string.Empty);

        Assert.True(exitCode != 0, $"PerfV3 exited 0 with an empty BATCH_SIZE; stderr: {stderr}");
        Assert.Contains(EmptyValueFailure, stderr, StringComparison.Ordinal);
        Assert.DoesNotContain(StopMarkerFailure, stderr, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Has_Unset_UsesTheDefault_InAPerfV3Child()
    {
        (int exitCode, string stderr) = await RunProducerConfigAsync(batchSize: null);

        Assert.True(exitCode != 0, $"PerfV3 exited 0 past the MAX_REQUEST_SIZE stop marker; stderr: {stderr}");
        Assert.Contains(StopMarkerFailure, stderr, StringComparison.Ordinal);
        Assert.DoesNotContain(EmptyValueFailure, stderr, StringComparison.Ordinal);
    }

    private static void Apply(IReadOnlyDictionary<string, string> values) => PerfEngineFixture.Apply(values);

    private async Task<(int ExitCode, string Stderr)> RunProducerConfigAsync(string? batchSize)
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal));

        var env = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["MODE"] = "producer",
            ["CREATE_TOPIC"] = "False",
            ["USE_DEFAULTS"] = "False",
            ["MAX_REQUEST_SIZE"] = StopMarker,
        };
        if (batchSize is not null)
        {
            env["BATCH_SIZE"] = batchSize;
        }

        IReadOnlyCollection<string> removeKeys = batchSize is null ? new[] { "BATCH_SIZE" } : Array.Empty<string>();
        (int exitCode, string stdout, string stderr) =
            await PerfV3Process.RunAsync(_output, env, removeKeys).ConfigureAwait(false);
        _output.WriteLine(stdout);
        _output.WriteLine(stderr);
        return (exitCode, stderr);
    }
}
