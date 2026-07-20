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

using System.Runtime.InteropServices;
using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.ShareConsumer.UnitTests;

/// <summary>
/// TFM-sentinel smoke test. Proves the xUnit harness runs and that the test
/// assembly is correctly wired to the library (ProjectReference +
/// InternalsVisibleTo). It intentionally references no library type yet — the
/// M0/P0 skeleton has none (design/history/M0) — but the assembly wiring and
/// build across the TFM matrix (net8.0, net10.0) must be sound.
/// </summary>
public sealed class TfmSentinelTests
{
    private readonly ITestOutputHelper _output;

    public TfmSentinelTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void HarnessRuns_OnConfiguredTargetFramework()
    {
        string targetFramework =
#if NET10_0_OR_GREATER
            "net10.0";
#elif NET8_0
            "net8.0";
#else
            "unknown";
#endif

        _output.WriteLine($"Target framework: {targetFramework}");
        _output.WriteLine($"Runtime: {RuntimeInformation.FrameworkDescription}");

        // The build compiled under a TFM we recognize (proves the matrix wiring).
        Assert.NotEqual("unknown", targetFramework);

        // The harness is really executing on a runtime (proves the test runs).
        Assert.NotEmpty(RuntimeInformation.FrameworkDescription);
    }
}
