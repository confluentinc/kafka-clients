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

using System.Globalization;

namespace Confluent.Kafka.Performance;

/// <summary>
/// Numeric formatting for the shared <c>metrics.jsonl</c> schema, kept byte-compatible with the
/// Python (<c>performance_common.py</c>) and Rust (<c>tests/performance/producer_perf_test.rs</c>)
/// siblings so <c>tools/performance_metrics_plot</c> parses every language's output identically.
/// </summary>
/// <remarks>
/// <para>
/// The plot tool floats every numeric field (<c>_to_float</c> / <c>float(...)</c>) and only special-cases
/// the string <c>"-inf"</c>, so the hard contract is: emit <c>"-inf"</c> for an unset maximum / measurement
/// bound, and an integer-parseable string for the window / measurement millisecond bounds. Both siblings
/// already diverge on int-vs-float text (Python's <c>str(total/count)</c> yields <c>"2048.0"</c>; Rust's
/// <c>f64::to_string</c> yields <c>"2048"</c>) and both parse fine — so this uses invariant-culture numeric
/// text and never a locale-specific separator.
/// </para>
/// </remarks>
internal static class PerfFormat
{
    /// <summary>The <c>-inf</c> sentinel emitted for an unset max / measurement bound (Python <c>str(-math.inf)</c>).</summary>
    internal const string NegInf = "-inf";

    /// <summary>
    /// Formats a metric value as its <c>metrics.jsonl</c> string: <c>"-inf"</c> for negative infinity
    /// (the unset sentinel), <c>"inf"</c> / <c>"nan"</c> for the other non-finite forms, otherwise the
    /// round-trippable invariant-culture text (an integral value renders without a trailing <c>.0</c>).
    /// </summary>
    internal static string Num(double value)
    {
        if (double.IsNegativeInfinity(value))
        {
            return NegInf;
        }

        if (double.IsPositiveInfinity(value))
        {
            return "inf";
        }

        if (double.IsNaN(value))
        {
            return "nan";
        }

        return value.ToString("R", CultureInfo.InvariantCulture);
    }

    /// <summary>Formats a long as invariant-culture decimal text (the window / count fields).</summary>
    internal static string Num(long value) => value.ToString(CultureInfo.InvariantCulture);
}
