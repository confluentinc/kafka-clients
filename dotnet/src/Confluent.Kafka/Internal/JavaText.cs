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
/// The two texts the binding writes for a configuration value, kept apart: <see cref="CoreText"/>, the text the core
/// parses (exact digits, a <see langword="double"/> as <c>"G17"</c>; the core rounds), and <see cref="ValueOf"/>,
/// Java's <c>String.valueOf</c>, for the binding's own messages only. Both are in the invariant culture.
/// </summary>
internal static class JavaText
{
    private const long DoubleMinNormalBits = 0x0010000000000000L;
    private const float FloatMinNormal = 1.17549435E-38f;
    private const int DoubleSubnormalShift = 1074;
    private const int FloatSubnormalShift = 149;

    private static readonly double s_twoToTheFloatSubnormalShift =
        BitConverter.Int64BitsToDouble((1023L + FloatSubnormalShift) << 52);

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
        _ => CollectionText(value, ValueOf) ?? InvariantText(value),
    };

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
            text.Append(separator).Append(element(item));
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
            text.Append(separator).Append(element(entry.Key)).Append('=').Append(element(entry.Value));
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
            text.Append(separator).Append(element(pair.Key)).Append('=').Append(element(pair.Value));
            separator = ", ";
        }

        return text.Append('}').ToString();
    }

    private static string DoubleToString(double value)
    {
        if (double.IsNaN(value) || double.IsInfinity(value) || value == 0)
        {
            return JavaSpecialText(value);
        }

        double magnitude = Math.Abs(value);
        string text = magnitude.ToString("R", CultureInfo.InvariantCulture);
        if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed)
            || parsed != magnitude)
        {
            text = magnitude.ToString("G17", CultureInfo.InvariantCulture);
        }

        (string digits, int exponent) = DecimalDigits(text);
        long bits = BitConverter.DoubleToInt64Bits(magnitude);
        if (digits.Length == 1 && bits < DoubleMinNormalBits)
        {
            (digits, exponent) = ClosestOfTwoDigits(bits, DoubleSubnormalShift, digits, exponent);
        }

        return JavaNotation(value < 0, digits, exponent);
    }

    private static string FloatToString(float value)
    {
        if (float.IsNaN(value) || float.IsInfinity(value) || value == 0)
        {
            return JavaSpecialText(value);
        }

        float magnitude = Math.Abs(value);
        string text = magnitude.ToString("R", CultureInfo.InvariantCulture);
        if (!float.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out float parsed)
            || parsed != magnitude)
        {
            text = magnitude.ToString("G9", CultureInfo.InvariantCulture);
        }

        (string digits, int exponent) = DecimalDigits(text);
        if (digits.Length == 1 && magnitude < FloatMinNormal)
        {
            long significand = (long)(magnitude * s_twoToTheFloatSubnormalShift);
            (digits, exponent) = ClosestOfTwoDigits(significand, FloatSubnormalShift, digits, exponent);
        }

        return JavaNotation(value < 0, digits, exponent);
    }

    private static string JavaSpecialText(double value)
    {
        if (double.IsNaN(value))
        {
            return "NaN";
        }

        if (double.IsInfinity(value))
        {
            return value > 0 ? "Infinity" : "-Infinity";
        }

        return IsNegative(value) ? "-0.0" : "0.0";
    }

    private static bool IsNegative(double value) => BitConverter.DoubleToInt64Bits(value) < 0;

    private static (string Digits, int Exponent) DecimalDigits(string text)
    {
        int e = text.IndexOfAny(new[] { 'E', 'e' });
        string mantissa = e < 0 ? text : text.Substring(0, e);
        int power = e < 0 ? 0 : int.Parse(text.Substring(e + 1), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        int point = mantissa.IndexOf('.');
        string all = point < 0 ? mantissa : mantissa.Remove(point, 1);
        int integerDigits = point < 0 ? mantissa.Length : point;
        int leading = 0;
        while (leading < all.Length - 1 && all[leading] == '0')
        {
            leading++;
        }

        string digits = all.Substring(leading).TrimEnd('0');
        return (digits.Length == 0 ? "0" : digits, integerDigits - leading - 1 + power);
    }

    private static (string Digits, int Exponent) ClosestOfTwoDigits(long significand, int shift, string digits, int exponent)
    {
        int unitExponent = exponent - 2;
        BigInteger unitsPerOne = BigInteger.Pow(10, -unitExponent);
        BigInteger halfUlp = unitsPerOne;
        BigInteger twiceValue = 2 * significand * unitsPerOne;
        BigInteger scale = BigInteger.One << (shift + 1);
        bool evenSignificand = significand % 2 == 0;

        BigInteger best = (digits[0] - '0') * 100;
        BigInteger bestGap = BigInteger.Abs((best * scale) - twiceValue);
        for (int step = 10; step >= 1; step /= 10)
        {
            for (int candidateDigits = 10; candidateDigits < 100; candidateDigits++)
            {
                BigInteger candidate = candidateDigits * step;
                BigInteger gap = BigInteger.Abs((candidate * scale) - twiceValue);
                bool readsBack = gap < halfUlp || (gap == halfUlp && evenSignificand);
                if (readsBack && gap < bestGap)
                {
                    best = candidate;
                    bestGap = gap;
                }
            }
        }

        string all = best.ToString(CultureInfo.InvariantCulture);
        return (all.TrimEnd('0'), all.Length - 1 + unitExponent);
    }

    private static string JavaNotation(bool negative, string digits, int exponent)
    {
        var text = new StringBuilder(digits.Length + 8);
        if (negative)
        {
            text.Append('-');
        }

        if (exponent >= -3 && exponent < 7)
        {
            if (exponent < 0)
            {
                text.Append("0.").Append('0', -exponent - 1).Append(digits);
            }
            else if (digits.Length > exponent + 1)
            {
                text.Append(digits, 0, exponent + 1).Append('.').Append(digits, exponent + 1, digits.Length - exponent - 1);
            }
            else
            {
                text.Append(digits).Append('0', exponent + 1 - digits.Length).Append(".0");
            }
        }
        else
        {
            text.Append(digits[0]).Append('.').Append(digits.Length > 1 ? digits.Substring(1) : "0")
                .Append('E').Append(exponent.ToString(CultureInfo.InvariantCulture));
        }

        return text.ToString();
    }

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
}
