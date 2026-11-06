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
using System.Reflection;
using System.Text;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The texts Java gives a value: <see cref="ValueOf"/>, Java's <c>String.valueOf</c> in a string concatenation, and
/// <see cref="ToString(Exception)"/>, <c>Throwable.toString()</c>, so the messages built from them read as Java's;
/// and <see cref="CoreText"/>, the text the core parses for a configuration value (exact digits, a
/// <see langword="double"/> as <c>"G17"</c>; the core rounds), kept apart from the message text. All are in the
/// invariant culture.
/// </summary>
internal static class JavaText
{
    private const double Log10Of2 = 0.30102999566398120;

    internal static string CoreText(object value, ConfigType? keyType)
    {
        if (keyType == ConfigType.List && value is IList list)
        {
            var elements = new string[list.Count];
            for (int i = 0; i < elements.Length; i++)
            {
                elements[i] = CoreElement(list[i]);
            }

            return string.Join(",", elements);
        }

        if (keyType is null && value is not string && CollectionText(value, CoreElement) is { } text)
        {
            return text;
        }

        return CoreScalar(value);
    }

    internal static string ValueOf(object? value) => value switch
    {
        null => "null",
        bool flag => flag ? "true" : "false",
        string text => text,
        char character => character.ToString(),
        double number => DoubleToString(number),
        float number => FloatToString(number),
        decimal number => DecimalText(number, javaNotation: true),
        Exception exception => ToString(exception),
        _ => CollectionText(value, ValueOf) ?? InvariantText(value),
    };

    internal static string ToString(Exception exception) =>
        exception is IExceptionText text ? text.TypeAndMessage() : TypeAndMessage(exception, exception.Message);

    internal static string TypeAndMessage(Exception exception, string? message)
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

    private static string? CollectionText(object value, Func<object?, string> element)
    {
        if (value is IDictionary dictionary)
        {
            return MapText(dictionary, element);
        }

        if (value is not IEnumerable sequence)
        {
            return null;
        }

        // An array prints as [a, b], where Java's String.valueOf prints its class and hash code.
        bool collection = value is ICollection;
        foreach (Type contract in value.GetType().GetInterfaces())
        {
            if (!contract.IsGenericType)
            {
                continue;
            }

            Type definition = contract.GetGenericTypeDefinition();
            if (definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>))
            {
                var pairsText = (Func<IEnumerable, Func<object?, string>, string>)typeof(JavaText)
                    .GetMethod(nameof(PairsText), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(contract.GetGenericArguments())
                    .CreateDelegate(typeof(Func<IEnumerable, Func<object?, string>, string>));
                return pairsText(sequence, element);
            }

            collection |= definition == typeof(ICollection<>) || definition == typeof(IReadOnlyCollection<>);
        }

        return collection ? SequenceText(sequence, element) : null;
    }

    private static string CoreElement(object? element) => element is null ? "null" : CoreText(element, null);

    private static string CoreScalar(object value) => value switch
    {
        string text => text,
        bool flag => flag ? "true" : "false",
        double number => DoubleCoreText(number),
        float number => DoubleCoreText(number),
#if NET5_0_OR_GREATER
        Half number => DoubleCoreText((double)number),
#endif
        decimal number => DecimalText(number, javaNotation: false),
        Type type => type.FullName ?? type.ToString(),
        _ => InvariantText(value),
    };

    private static string DoubleCoreText(double value)
    {
        if (double.IsNaN(value))
        {
            return "NaN";
        }

        if (double.IsInfinity(value))
        {
            return value > 0 ? "Infinity" : "-Infinity";
        }

        if (value == 0)
        {
            return IsNegative(value) ? "-0" : "0";
        }

        // "G17" on every runtime: "R" does not always round-trip on .NET Framework, and 17 digits identify a double.
        return value.ToString("G17", CultureInfo.InvariantCulture);
    }

    private static string InvariantText(object value) => (value switch
    {
        IntPtr pointer => pointer.ToInt64().ToString(CultureInfo.InvariantCulture),
        UIntPtr pointer => pointer.ToUInt64().ToString(CultureInfo.InvariantCulture),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    }) ?? "null";

    private static string SequenceText(IEnumerable collection, Func<object?, string> element)
    {
        var text = new StringBuilder("[");
        string separator = string.Empty;
        foreach (object? item in collection)
        {
            text.Append(separator).Append(ReferenceEquals(item, collection) ? "(this Collection)" : element(item));
            separator = ", ";
        }

        return text.Append(']').ToString();
    }

    private static string MapText(IDictionary dictionary, Func<object?, string> element)
    {
        var text = new StringBuilder("{");
        string separator = string.Empty;
        foreach (DictionaryEntry entry in dictionary)
        {
            text.Append(separator).Append(Element(entry.Key, dictionary, element)).Append('=').Append(Element(entry.Value, dictionary, element));
            separator = ", ";
        }

        return text.Append('}').ToString();
    }

    private static string PairsText<TKey, TValue>(IEnumerable pairs, Func<object?, string> element)
    {
        var text = new StringBuilder("{");
        string separator = string.Empty;
        foreach (KeyValuePair<TKey, TValue> pair in (IEnumerable<KeyValuePair<TKey, TValue>>)pairs)
        {
            text.Append(separator).Append(Element(pair.Key, pairs, element)).Append('=').Append(Element(pair.Value, pairs, element));
            separator = ", ";
        }

        return text.Append('}').ToString();
    }

    private static string Element(object? item, object map, Func<object?, string> element) =>
        ReferenceEquals(item, map) ? "(this Map)" : element(item);

    private static bool IsNegative(double value) => BitConverter.DoubleToInt64Bits(value) < 0;

    private static string DecimalText(decimal value, bool javaNotation)
    {
        int[] parts = decimal.GetBits(value);
        int scale = (parts[3] >> 16) & 0xFF;
        BigInteger unscaled = ((BigInteger)(uint)parts[2] << 64) | ((BigInteger)(uint)parts[1] << 32) | (uint)parts[0];
        string digits = unscaled.ToString(CultureInfo.InvariantCulture);
        var text = new StringBuilder(digits.Length + 8);
        if (parts[3] < 0 && !unscaled.IsZero)
        {
            text.Append('-');
        }

        int adjusted = digits.Length - 1 - scale;
        if (javaNotation && adjusted < -6)
        {
            text.Append(digits[0]);
            if (digits.Length > 1)
            {
                text.Append('.').Append(digits, 1, digits.Length - 1);
            }

            return text.Append('E').Append(adjusted.ToString(CultureInfo.InvariantCulture)).ToString();
        }

        if (scale == 0)
        {
            return text.Append(digits).ToString();
        }

        if (digits.Length > scale)
        {
            return text.Append(digits, 0, digits.Length - scale).Append('.')
                .Append(digits, digits.Length - scale, scale).ToString();
        }

        return text.Append("0.").Append('0', scale - digits.Length).Append(digits).ToString();
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
