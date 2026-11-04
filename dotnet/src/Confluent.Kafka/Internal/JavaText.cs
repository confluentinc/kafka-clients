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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using System.Text;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The text Java gives a value in a string concatenation (<c>String.valueOf</c>) and an exception
/// (<c>Throwable.toString()</c>), so the messages built from them read as Java's.
/// </summary>
internal static class JavaText
{
    private const double Log10Of2 = 0.30102999566398120;

    internal static string ValueOf(object? value)
    {
        switch (value)
        {
            case null:
                return "null";
            case string text:
                return text;
            case bool flag:
                return flag ? "true" : "false";
            case double number:
                return DoubleToString(number);
            case float number:
                return FloatToString(number);
            case Exception exception:
                return ToString(exception);
            case IDictionary map:
                return MapToString(map);
        }

        Type type = value.GetType();
        if (Implements(type, typeof(IDictionary<,>), typeof(IReadOnlyDictionary<,>)))
        {
            return MapToString((IEnumerable)value);
        }

        if (value is ICollection || Implements(type, typeof(ICollection<>), typeof(IReadOnlyCollection<>)))
        {
            return CollectionToString((IEnumerable)value);
        }

        return value is IFormattable formattable
            ? formattable.ToString(null, CultureInfo.InvariantCulture)
            : value.ToString() ?? "null";
    }

    internal static string ToString(Exception exception) =>
        exception is IJavaThrowable java ? java.JavaToString() : ThrowableToString(exception, exception.Message);

    internal static string ThrowableToString(Exception exception, string? message)
    {
        Type type = exception.GetType();
        string name = type.FullName ?? type.Name;
        return message is null ? name : name + ": " + message;
    }

    internal static string DoubleToString(double value)
    {
        if (double.IsNaN(value))
        {
            return "NaN";
        }

        if (double.IsInfinity(value))
        {
            return value > 0 ? "Infinity" : "-Infinity";
        }

        long bits = BitConverter.DoubleToInt64Bits(value);
        bool negative = bits < 0;
        int biased = (int)((bits >> 52) & 0x7FF);
        long fraction = bits & 0xFFFFFFFFFFFFFL;
        if (biased == 0 && fraction == 0)
        {
            return negative ? "-0.0" : "0.0";
        }

        return biased == 0
            ? Shortest(negative, fraction, -1074, lowerGapHalved: false)
            : Shortest(negative, fraction | (1L << 52), biased - 1075, lowerGapHalved: fraction == 0 && biased > 1);
    }

    internal static string FloatToString(float value)
    {
        if (float.IsNaN(value))
        {
            return "NaN";
        }

        if (float.IsInfinity(value))
        {
            return value > 0 ? "Infinity" : "-Infinity";
        }

        int bits = BitConverter.ToInt32(BitConverter.GetBytes(value), 0);
        bool negative = bits < 0;
        int biased = (bits >> 23) & 0xFF;
        int fraction = bits & 0x7FFFFF;
        if (biased == 0 && fraction == 0)
        {
            return negative ? "-0.0" : "0.0";
        }

        return biased == 0
            ? Shortest(negative, fraction, -149, lowerGapHalved: false)
            : Shortest(negative, fraction | (1 << 23), biased - 150, lowerGapHalved: fraction == 0 && biased > 1);
    }

    private static bool Implements(Type type, Type first, Type second)
    {
        foreach (Type candidate in type.GetInterfaces())
        {
            if (candidate.IsGenericType)
            {
                Type definition = candidate.GetGenericTypeDefinition();
                if (definition == first || definition == second)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static string MapToString(IDictionary map)
    {
        StringBuilder text = new StringBuilder("{");
        IDictionaryEnumerator entries = map.GetEnumerator();
        while (entries.MoveNext())
        {
            AppendEntry(text, map, entries.Key, entries.Value);
        }

        return text.Append('}').ToString();
    }

    private static string MapToString(IEnumerable map)
    {
        StringBuilder text = new StringBuilder("{");
        foreach (object? entry in map)
        {
            Type type = entry!.GetType();
            object? key = type.GetProperty("Key")!.GetValue(entry);
            AppendEntry(text, map, key, type.GetProperty("Value")!.GetValue(entry));
        }

        return text.Append('}').ToString();
    }

    private static void AppendEntry(StringBuilder text, object map, object? key, object? value)
    {
        if (text.Length > 1)
        {
            text.Append(", ");
        }

        text.Append(ReferenceEquals(key, map) ? "(this Map)" : ValueOf(key)).Append('=')
            .Append(ReferenceEquals(value, map) ? "(this Map)" : ValueOf(value));
    }

    private static string CollectionToString(IEnumerable collection)
    {
        StringBuilder text = new StringBuilder("[");
        string separator = string.Empty;
        foreach (object? element in collection)
        {
            text.Append(separator)
                .Append(ReferenceEquals(element, collection) ? "(this Collection)" : ValueOf(element));
            separator = ", ";
        }

        return text.Append(']').ToString();
    }

    // Java's Double.toString / Float.toString as specified since JDK 19: of the decimals that round to the
    // value, those with the fewest digits (one or two digits when one suffices), and of those the closest, an
    // even last digit breaking a tie. Exact integer arithmetic, because .NET Framework's own formatting does
    // not always give the shortest digits.
    private static string Shortest(bool negative, long significand, int exponent, bool lowerGapHalved)
    {
        BigInteger value = significand;
        int magnitude = DecimalExponent(value, exponent);

        // The interval of decimals that round to the value, in units of 2^(exponent - 2); IEEE 754 rounds a
        // tie to the even significand, so the ends belong to it only when the significand is even.
        int binary = exponent - 2;
        BigInteger scaled = value * 4;
        BigInteger low = scaled - (lowerGapHalved ? 1 : 2);
        BigInteger high = scaled + 2;
        bool endsIncluded = significand % 2 == 0;

        int digits = 1;
        BigInteger min;
        BigInteger max;
        while (!Fits(low, high, binary, magnitude + 1 - digits, endsIncluded, out min, out max))
        {
            digits++;
        }

        int unit = magnitude + 1 - digits;
        if (digits == 1)
        {
            unit--;
            Fits(low, high, binary, unit, endsIncluded, out min, out max);
        }

        BigInteger below = Divide(scaled, binary, unit, out BigInteger remainder, out BigInteger divisor);
        BigInteger above = below + 1;
        int half = (remainder * 2).CompareTo(divisor);
        BigInteger chosen = above > max || (below >= min && (half < 0 || (half == 0 && below.IsEven)))
            ? below
            : above;

        return Format(negative, chosen.ToString(CultureInfo.InvariantCulture), unit);
    }

    private static bool Fits(
        BigInteger low,
        BigInteger high,
        int binary,
        int unit,
        bool endsIncluded,
        out BigInteger min,
        out BigInteger max)
    {
        BigInteger lowQuotient = Divide(low, binary, unit, out BigInteger lowRemainder, out _);
        min = lowRemainder.IsZero && endsIncluded ? lowQuotient : lowQuotient + 1;
        BigInteger highQuotient = Divide(high, binary, unit, out BigInteger highRemainder, out _);
        max = highRemainder.IsZero && !endsIncluded ? highQuotient - 1 : highQuotient;
        return min <= max;
    }

    private static BigInteger Divide(
        BigInteger value,
        int binary,
        int unit,
        out BigInteger remainder,
        out BigInteger divisor)
    {
        BigInteger dividend = (value << Math.Max(binary, 0)) * BigInteger.Pow(10, Math.Max(-unit, 0));
        divisor = (BigInteger.One << Math.Max(-binary, 0)) * BigInteger.Pow(10, Math.Max(unit, 0));
        return BigInteger.DivRem(dividend, divisor, out remainder);
    }

    private static int DecimalExponent(BigInteger value, int binary)
    {
        int estimate = (int)Math.Floor(Math.Log10((double)value) + (binary * Log10Of2));
        while (CompareToPowerOfTen(value, binary, estimate) < 0)
        {
            estimate--;
        }

        while (CompareToPowerOfTen(value, binary, estimate + 1) >= 0)
        {
            estimate++;
        }

        return estimate;
    }

    private static int CompareToPowerOfTen(BigInteger value, int binary, int power)
    {
        BigInteger left = (value << Math.Max(binary, 0)) * BigInteger.Pow(10, Math.Max(-power, 0));
        BigInteger right = (BigInteger.One << Math.Max(-binary, 0)) * BigInteger.Pow(10, Math.Max(power, 0));
        return left.CompareTo(right);
    }

    private static string Format(bool negative, string digits, int unit)
    {
        int length = digits.Length;
        while (digits[length - 1] == '0')
        {
            length--;
        }

        string significant = digits.Substring(0, length);
        int scientific = digits.Length - 1 + unit;
        StringBuilder text = new StringBuilder(negative ? "-" : string.Empty);
        if (scientific >= 7 || scientific < -3)
        {
            text.Append(significant[0]).Append('.').Append(length > 1 ? significant.Substring(1) : "0")
                .Append('E').Append(scientific.ToString(CultureInfo.InvariantCulture));
        }
        else if (scientific >= 0)
        {
            string whole = significant.PadRight(scientific + 1, '0');
            text.Append(whole, 0, scientific + 1).Append('.')
                .Append(whole.Length > scientific + 1 ? whole.Substring(scientific + 1) : "0");
        }
        else
        {
            text.Append("0.").Append('0', -scientific - 1).Append(significant);
        }

        return text.ToString();
    }
}
