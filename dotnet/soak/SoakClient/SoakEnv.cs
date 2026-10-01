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
// PROVENANCE: forked from bindings/dotnet/tests/Performance/PerformanceCommon/PerfEnv.cs
// (see MetricPrimitives.cs's header for why the soak forks rather than references), with
// GetDouble added — the soak carries several float-valued tunables (rate, poll timeout,
// commit interval) that the perf harness has no counterpart for.

using System;
using System.Globalization;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Environment-variable parsing, matching the perf suite's conventions exactly so the
/// same idioms drive every .NET tool in this repo: booleans are the case-sensitive
/// string <c>"True"</c> (Python's <c>os.getenv(name, "False") == "True"</c>), and numbers
/// parse with invariant culture.
/// </summary>
internal static class SoakEnv
{
    /// <summary>Returns the value of <paramref name="name"/>, or <paramref name="fallback"/> when unset / empty.</summary>
    internal static string GetString(string name, string fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrEmpty(value) ? fallback : value!;
    }

    /// <summary>Returns the value of <paramref name="name"/>, or <see langword="null"/> when unset / empty.</summary>
    internal static string? GetStringOrNull(string name)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        return string.IsNullOrEmpty(value) ? null : value;
    }

    /// <summary>Parses <paramref name="name"/> as an int, or returns <paramref name="fallback"/> when unset / empty.</summary>
    internal static int GetInt(string name, int fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            return fallback;
        }

        return int.Parse(value!, CultureInfo.InvariantCulture);
    }

    /// <summary>Parses <paramref name="name"/> as a double, or returns <paramref name="fallback"/> when unset / empty.</summary>
    internal static double GetDouble(string name, double fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            return fallback;
        }

        return double.Parse(value!, NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Parses <paramref name="name"/> as the case-sensitive string <c>"True"</c>; any
    /// other value — including <c>"true"</c> or <c>"1"</c> — is <see langword="false"/>,
    /// matching Python and the perf suite.
    /// </summary>
    internal static bool GetBool(string name, bool fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            return fallback;
        }

        return string.Equals(value, "True", StringComparison.Ordinal);
    }
}
