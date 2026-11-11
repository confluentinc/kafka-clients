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
using System.Globalization;

namespace Confluent.Kafka.Internal;

/// <summary>
/// Java's <c>java.util.UUID.fromString</c>: five dash-separated hexadecimal groups, each read by
/// <c>Long.parseLong(CharSequence, int, int, int)</c> (its digits are <c>Character.digit</c>'s) and masked to its
/// width, so <c>1-1-1-1-1</c> is <c>00000001-0001-0001-0001-000000000001</c> and the layouts only
/// <see cref="Guid.Parse(string)"/> reads (braces, parentheses, 32 digits without dashes, surrounding spaces) are
/// refused. Every error is an <see cref="IllegalArgumentException"/> with Java's message, as Java's
/// <c>NumberFormatException</c> is one.
/// </summary>
internal static class JavaUuid
{
    private const int Radix = 16;

    internal static Guid FromString(string name)
    {
        if (name.Length > 36)
        {
            throw new IllegalArgumentException("UUID string too large");
        }

        int dash1 = name.IndexOf('-', 0);
        int dash2 = name.IndexOf('-', dash1 + 1);
        int dash3 = name.IndexOf('-', dash2 + 1);
        int dash4 = name.IndexOf('-', dash3 + 1);
        int dash5 = name.IndexOf('-', dash4 + 1);
        if (dash4 < 0 || dash5 >= 0)
        {
            throw new IllegalArgumentException("Invalid UUID string: " + name);
        }

        long mostSigBits = ParseLong(name, 0, dash1, Radix) & 0xFFFFFFFFL;
        mostSigBits <<= 16;
        mostSigBits |= ParseLong(name, dash1 + 1, dash2, Radix) & 0xFFFFL;
        mostSigBits <<= 16;
        mostSigBits |= ParseLong(name, dash2 + 1, dash3, Radix) & 0xFFFFL;
        long leastSigBits = ParseLong(name, dash3 + 1, dash4, Radix) & 0xFFFFL;
        leastSigBits <<= 48;
        leastSigBits |= ParseLong(name, dash4 + 1, name.Length, Radix) & 0xFFFFFFFFFFFFL;

        return new Guid(
            (int)(mostSigBits >> 32),
            (short)(mostSigBits >> 16),
            (short)mostSigBits,
            (byte)(leastSigBits >> 56),
            (byte)(leastSigBits >> 48),
            (byte)(leastSigBits >> 40),
            (byte)(leastSigBits >> 32),
            (byte)(leastSigBits >> 24),
            (byte)(leastSigBits >> 16),
            (byte)(leastSigBits >> 8),
            (byte)leastSigBits);
    }

    private static long ParseLong(string s, int beginIndex, int endIndex, int radix)
    {
        if (beginIndex == endIndex)
        {
            throw new IllegalArgumentException(
                "For input string: \"\"" + (radix == 10 ? string.Empty : " under radix " + radix.ToString(CultureInfo.InvariantCulture)));
        }

        bool negative = false;
        int i = beginIndex;
        long limit = -long.MaxValue;
        char firstChar = s[i];
        if (firstChar < '0')
        {
            if (firstChar == '-')
            {
                negative = true;
                limit = long.MinValue;
            }
            else if (firstChar != '+')
            {
                throw ErrorAt(s, beginIndex, endIndex, i);
            }

            i++;
            if (i == endIndex)
            {
                throw ErrorAt(s, beginIndex, endIndex, i);
            }
        }

        long multmin = limit / radix;
        long result = 0;
        while (i < endIndex)
        {
            int digit = Digit(s[i], radix);
            if (digit < 0 || result < multmin)
            {
                throw ErrorAt(s, beginIndex, endIndex, i);
            }

            result *= radix;
            if (result < limit + digit)
            {
                throw ErrorAt(s, beginIndex, endIndex, i);
            }

            i++;
            result -= digit;
        }

        return negative ? result : -result;
    }

    private static IllegalArgumentException ErrorAt(string s, int beginIndex, int endIndex, int errorIndex) =>
        new IllegalArgumentException(
            "Error at index " + (errorIndex - beginIndex).ToString(CultureInfo.InvariantCulture) + " in: \""
            + s.Substring(beginIndex, endIndex - beginIndex) + "\"");

    private static int Digit(char ch, int radix)
    {
        int value = -1;
        if (ch >= 'a' && ch <= 'z')
        {
            value = ch - 'a' + 10;
        }
        else if (ch >= 'A' && ch <= 'Z')
        {
            value = ch - 'A' + 10;
        }
        else if (ch >= 'Ａ' && ch <= 'Ｚ')
        {
            value = ch - 'Ａ' + 10;
        }
        else if (ch >= 'ａ' && ch <= 'ｚ')
        {
            value = ch - 'ａ' + 10;
        }
        else if (CharUnicodeInfo.GetUnicodeCategory(ch) == UnicodeCategory.DecimalDigitNumber)
        {
            value = CharUnicodeInfo.GetDecimalDigitValue(ch);
        }

        return value < radix ? value : -1;
    }
}
