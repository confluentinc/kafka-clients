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
using System.Globalization;

namespace Confluent.Kafka.Performance;

/// <summary>
/// Environment-variable parsing with the Python perf suite's conventions: a boolean is true only for the
/// case-sensitive <c>"True"</c> (Python's <c>os.getenv(name, "False") == "True"</c>), and numbers parse invariantly.
/// </summary>
public static class PerfEnv
{
    public static string GetString(string name, string fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        return value is null ? fallback : value;
    }

    public static string? GetStringOrNull(string name) => Environment.GetEnvironmentVariable(name);

    // Set, an empty value included, as Python's `name in os.environ`.
    public static bool Has(string name) => Environment.GetEnvironmentVariable(name) is not null;

    public static int ParseInt(string name) =>
        int.Parse(Environment.GetEnvironmentVariable(name) ?? string.Empty, CultureInfo.InvariantCulture);

    public static int GetInt(string name, int fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            return fallback;
        }

        return int.Parse(value, CultureInfo.InvariantCulture);
    }

    public static long GetLong(string name, long fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (string.IsNullOrEmpty(value))
        {
            return fallback;
        }

        return long.Parse(value, CultureInfo.InvariantCulture);
    }

    public static bool GetBool(string name, bool fallback)
    {
        string? value = Environment.GetEnvironmentVariable(name);
        if (value is null)
        {
            return fallback;
        }

        return value == "True";
    }
}
