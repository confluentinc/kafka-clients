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
using System.Text;

namespace Confluent.Kafka.Performance;

/// <summary>
/// The text forms of <c>metrics.jsonl</c> and <c>results.json</c> that Python's and Rust's perf tests write and
/// <c>tools/performance_metrics_plot</c> parses: <c>"-inf"</c> for an unset bound, invariant-culture numbers, and
/// Python's <c>json</c> float and string forms.
/// </summary>
internal static class PerfFormat
{
    internal const string NegInf = "-inf";

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

    internal static string Num(long value) => value.ToString(CultureInfo.InvariantCulture);

    internal static string JsonFloat(double value)
    {
        double rounded = Math.Round(value, 2, MidpointRounding.ToEven);
        if (rounded == Math.Floor(rounded) && Math.Abs(rounded) < 1e15)
        {
            return ((long)rounded).ToString(CultureInfo.InvariantCulture) + ".0";
        }

        return rounded.ToString("0.##", CultureInfo.InvariantCulture);
    }

    internal static string JsonString(string value)
    {
        var sb = new StringBuilder(value.Length + 2);
        sb.Append('"');
        foreach (char c in value)
        {
            switch (c)
            {
                case '"':
                    sb.Append("\\\"");
                    break;
                case '\\':
                    sb.Append("\\\\");
                    break;
                case '\n':
                    sb.Append("\\n");
                    break;
                case '\r':
                    sb.Append("\\r");
                    break;
                case '\t':
                    sb.Append("\\t");
                    break;
                default:
                    if (c < 0x20 || c > 0x7e)
                    {
                        sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    }
                    else
                    {
                        sb.Append(c);
                    }

                    break;
            }
        }

        sb.Append('"');
        return sb.ToString();
    }
}
