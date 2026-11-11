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
using System.Reflection;
using System.Runtime;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Performance;

/// <summary>
/// The <c>MODE=version</c> report: one <c>key: value</c> line per item, the client's library versions first, then the
/// runtime the harness runs on, as the process actually sees it.
/// </summary>
public static class RuntimeInfo
{
    /// <summary>The producer <c>client</c> string results.json carries for this exe and <c>ASYNC</c>.</summary>
    public static string ProducerClient(string baseClient) =>
        baseClient + (PerfEnv.GetBool("ASYNC", false) ? "-async" : string.Empty);

    /// <summary>An assembly's informational version (the SDK appends the source commit), else its assembly version.</summary>
    public static string AssemblyVersion(Assembly assembly)
    {
        string? informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        return string.IsNullOrEmpty(informational) ? assembly.GetName().Version?.ToString() ?? "unknown" : informational;
    }

    public static void Print(IEnumerable<KeyValuePair<string, string>> clientItems)
    {
        foreach (KeyValuePair<string, string> item in clientItems)
        {
            Console.WriteLine($"{item.Key}: {item.Value}");
        }

        Console.WriteLine($"dotnet_runtime: {Environment.Version}");
        Console.WriteLine($"framework: {RuntimeInformation.FrameworkDescription}");
        Console.WriteLine($"server_gc: {Bool(GCSettings.IsServerGC)}");
        Console.WriteLine($"concurrent_gc: {Bool(GCSettings.LatencyMode != GCLatencyMode.Batch)}");
        Console.WriteLine($"tiered_pgo: {TieredPgo()}");
    }

    // DOTNET_TieredPGO overrides the runtimeconfig.json setting the csproj's TieredPGO writes; neither set is the
    // runtime's default, which is on.
    private static string TieredPgo()
    {
        string? env = PerfEnv.GetStringOrNull("DOTNET_TieredPGO");
        if (!string.IsNullOrEmpty(env))
        {
            return Bool(env != "0");
        }

        object? configured = AppContext.GetData("System.Runtime.TieredPGO");
        if (configured is not null)
        {
            string text = Convert.ToString(configured, CultureInfo.InvariantCulture) ?? string.Empty;
            return Bool(!string.Equals(text, "false", StringComparison.OrdinalIgnoreCase) && text != "0");
        }

        return Bool(true);
    }

    private static string Bool(bool value) => value ? "True" : "False";
}
