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

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A Java charset the String and UUID serdes can be configured with, found by Java's name or one of its aliases,
/// ignoring ASCII case: UTF-8, UTF-16, UTF-16BE, UTF-16LE, UTF-32, UTF-32BE, UTF-32LE, US-ASCII and ISO-8859-1. Each
/// encodes as Java's <c>String.getBytes(charset)</c> and decodes as <c>new String(bytes, charset)</c>: UTF-16 writes a
/// big-endian <c>FE FF</c> mark before a non-empty string and reads big-endian unless a mark says otherwise; UTF-32
/// writes big-endian with no mark; a character a charset cannot encode becomes Java's replacement (<c>'?'</c> for
/// UTF-8, US-ASCII and ISO-8859-1, <c>U+FFFD</c> for the UTF-16 and UTF-32 charsets), and each malformed sequence, as
/// the JDK delimits it, decodes to one <c>U+FFFD</c>.
/// </summary>
internal sealed class JavaCharset
{
    internal static readonly JavaCharset Utf8 = new JavaCharset(Kind.Utf8);

    private const char Replacement = '\uFFFD';

    private const int ByteOrderNone = 0;

    private const int ByteOrderBig = 1;

    private const int ByteOrderLittle = 2;

    // Java's names and aliases (the JDK's charsets list), in ASCII lowercase.
    private static readonly Dictionary<string, JavaCharset> s_byName = Table(
        (Utf8, new[] { "utf-8", "utf8", "unicode-1-1-utf-8" }),
        (new JavaCharset(Kind.Utf16), new[] { "utf-16", "utf_16", "utf16", "unicode", "unicodebig" }),
        (new JavaCharset(Kind.Utf16Be), new[] { "utf-16be", "utf_16be", "iso-10646-ucs-2", "x-utf-16be", "unicodebigunmarked" }),
        (new JavaCharset(Kind.Utf16Le), new[] { "utf-16le", "utf_16le", "x-utf-16le", "unicodelittleunmarked" }),
        (new JavaCharset(Kind.Utf32), new[] { "utf-32", "utf_32", "utf32" }),
        (new JavaCharset(Kind.Utf32Be), new[] { "utf-32be", "utf_32be", "x-utf-32be" }),
        (new JavaCharset(Kind.Utf32Le), new[] { "utf-32le", "utf_32le", "x-utf-32le" }),
        (new JavaCharset(Kind.UsAscii), new[]
        {
            "us-ascii", "iso-ir-6", "ansi_x3.4-1986", "iso_646.irv:1991", "ascii", "iso646-us", "us", "ibm367", "cp367",
            "csascii", "646", "iso_646.irv:1983", "ansi_x3.4-1968", "ascii7",
        }),
        (new JavaCharset(Kind.Iso88591), new[]
        {
            "iso-8859-1", "iso-ir-100", "iso_8859-1", "latin1", "l1", "ibm819", "cp819", "csisolatin1", "819", "ibm-819",
            "iso8859_1", "iso_8859-1:1987", "iso_8859_1", "8859_1", "iso8859-1",
        }));

    private readonly Kind _kind;

    private JavaCharset(Kind kind) => _kind = kind;

    private enum Kind
    {
        Utf8,
        Utf16,
        Utf16Be,
        Utf16Le,
        Utf32,
        Utf32Be,
        Utf32Le,
        UsAscii,
        Iso88591,
    }

    // A Java name or alias, ignoring ASCII case only, as Java's Charset.forName.
    internal static JavaCharset? ForName(string name)
    {
        char[] lower = new char[name.Length];
        for (int i = 0; i < name.Length; i++)
        {
            char c = name[i];
            lower[i] = c >= 'A' && c <= 'Z' ? (char)(c + ('a' - 'A')) : c;
        }

        return s_byName.TryGetValue(new string(lower), out JavaCharset? charset) ? charset : null;
    }

    // The serdes' configure reads property, or fallback when property is absent or null, and takes the value only when
    // it is a string, as Java's instanceof String.
    internal static string? ConfiguredName(IReadOnlyDictionary<string, object?> configs, string property, string fallback)
    {
        if (configs is null)
        {
            throw new ArgumentNullException(nameof(configs));
        }

        if (!configs.TryGetValue(property, out object? value) || value is null)
        {
            configs.TryGetValue(fallback, out value);
        }

        return value as string;
    }

    // The String serdes' configure refuses an unknown name as Java's: "Unsupported encoding X", caused by
    // Charset.forName's IllegalArgumentException.
    internal static JavaCharset Configured(string name) =>
        ForName(name) ?? throw new SerializationException("Unsupported encoding " + name, new IllegalArgumentException(name));

    // Java's text.getBytes(charset).
    internal byte[] Encode(string text) => _kind switch
    {
        Kind.Utf8 => Utf8Marshal.QuestionMarkUtf8.GetBytes(text),
        Kind.Utf16 => text.Length == 0 ? Array.Empty<byte>() : EncodeUtf16(text, bigEndian: true, mark: true),
        Kind.Utf16Be => EncodeUtf16(text, bigEndian: true, mark: false),
        Kind.Utf16Le => EncodeUtf16(text, bigEndian: false, mark: false),
        Kind.Utf32 or Kind.Utf32Be => EncodeUtf32(text, bigEndian: true),
        Kind.Utf32Le => EncodeUtf32(text, bigEndian: false),
        Kind.UsAscii => EncodeSingleByte(text, 0x7F),
        _ => EncodeSingleByte(text, 0xFF),
    };

    // Java's new String(data, charset).
    internal string Decode(ReadOnlySpan<byte> data) => _kind switch
    {
        Kind.Utf8 => DecodeUtf8(data),
        Kind.Utf16 => DecodeUtf16(data, ByteOrderNone),
        Kind.Utf16Be => DecodeUtf16(data, ByteOrderBig),
        Kind.Utf16Le => DecodeUtf16(data, ByteOrderLittle),
        Kind.Utf32 => DecodeUtf32(data, ByteOrderNone),
        Kind.Utf32Be => DecodeUtf32(data, ByteOrderBig),
        Kind.Utf32Le => DecodeUtf32(data, ByteOrderLittle),
        Kind.UsAscii => DecodeSingleByte(data, 0x7F),
        _ => DecodeSingleByte(data, 0xFF),
    };

    private static Dictionary<string, JavaCharset> Table(params (JavaCharset Charset, string[] Names)[] entries)
    {
        Dictionary<string, JavaCharset> table = new Dictionary<string, JavaCharset>(StringComparer.Ordinal);
        foreach ((JavaCharset charset, string[] names) in entries)
        {
            foreach (string name in names)
            {
                table.Add(name, charset);
            }
        }

        return table;
    }

    private static bool IsPairAt(string text, int index) =>
        char.IsHighSurrogate(text[index]) && index + 1 < text.Length && char.IsLowSurrogate(text[index + 1]);

    // A valid surrogate pair is kept; an unpaired surrogate becomes U+FFFD, as Java's UnicodeEncoder replaces it.
    private static byte[] EncodeUtf16(string text, bool bigEndian, bool mark)
    {
        byte[] bytes = new byte[(text.Length + (mark ? 1 : 0)) * 2];
        int at = 0;
        if (mark)
        {
            Put16('\uFEFF', bytes, ref at, bigEndian);
        }

        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];
            if (IsPairAt(text, i))
            {
                Put16(c, bytes, ref at, bigEndian);
                Put16(text[++i], bytes, ref at, bigEndian);
            }
            else
            {
                Put16(char.IsSurrogate(c) ? Replacement : c, bytes, ref at, bigEndian);
            }
        }

        return bytes;
    }

    private static void Put16(char c, byte[] bytes, ref int at, bool bigEndian)
    {
        bytes[at + (bigEndian ? 0 : 1)] = (byte)(c >> 8);
        bytes[at + (bigEndian ? 1 : 0)] = (byte)c;
        at += 2;
    }

    // A valid surrogate pair is one code point; an unpaired surrogate becomes U+FFFD, as Java's UTF-32 encoder does.
    private static byte[] EncodeUtf32(string text, bool bigEndian)
    {
        int count = 0;
        for (int i = 0; i < text.Length; i++)
        {
            if (IsPairAt(text, i))
            {
                i++;
            }

            count++;
        }

        byte[] bytes = new byte[count * 4];
        int at = 0;
        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];
            int codePoint = IsPairAt(text, i)
                ? char.ConvertToUtf32(c, text[++i])
                : char.IsSurrogate(c) ? Replacement : c;
            bytes[at + (bigEndian ? 0 : 3)] = (byte)(codePoint >> 24);
            bytes[at + (bigEndian ? 1 : 2)] = (byte)(codePoint >> 16);
            bytes[at + (bigEndian ? 2 : 1)] = (byte)(codePoint >> 8);
            bytes[at + (bigEndian ? 3 : 0)] = (byte)codePoint;
            at += 4;
        }

        return bytes;
    }

    // Java's String.getBytes for US-ASCII and ISO-8859-1: a character above the charset's range becomes '?', a valid
    // surrogate pair one '?'.
    private static byte[] EncodeSingleByte(string text, int max)
    {
        int count = 0;
        for (int i = 0; i < text.Length; i++)
        {
            if (text[i] > max && IsPairAt(text, i))
            {
                i++;
            }

            count++;
        }

        byte[] bytes = new byte[count];
        int at = 0;
        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];
            if (c <= max)
            {
                bytes[at++] = (byte)c;
                continue;
            }

            if (IsPairAt(text, i))
            {
                i++;
            }

            bytes[at++] = (byte)'?';
        }

        return bytes;
    }

    // .NET decodes valid UTF-8 as Java does and writes U+FFFD for every malformed sequence, so a result without U+FFFD
    // is Java's. Any other is decoded again by the JDK's rules, which delimit a malformed sequence differently.
    private static string DecodeUtf8(ReadOnlySpan<byte> data)
    {
        string text = Utf8Marshal.GetString(data);
        return text.IndexOf(Replacement) < 0 ? text : DecodeUtf8AsJava(data);
    }

    // Java's String.decodeUTF8_UTF16 with replacement: each malformed sequence is one U+FFFD. Unlike .NET, a surrogate
    // written in UTF-8 (ED A0..BF xx) is one U+FFFD, and so is a sequence the data cuts off.
    private static string DecodeUtf8AsJava(ReadOnlySpan<byte> src)
    {
        char[] dst = new char[src.Length];
        int dp = 0;
        int sp = 0;
        int sl = src.Length;
        while (sp < sl)
        {
            int b1 = src[sp++];
            if (b1 < 0x80)
            {
                dst[dp++] = (char)b1;
            }
            else if (b1 >= 0xC2 && b1 <= 0xDF)
            {
                if (sp < sl)
                {
                    int b2 = src[sp++];
                    if (IsNotContinuation(b2))
                    {
                        dst[dp++] = Replacement;
                        sp--;
                    }
                    else
                    {
                        dst[dp++] = (char)(((b1 & 0x1F) << 6) | (b2 & 0x3F));
                    }

                    continue;
                }

                dst[dp++] = Replacement;
                break;
            }
            else if (b1 >= 0xE0 && b1 <= 0xEF)
            {
                if (sp + 1 < sl)
                {
                    int b2 = src[sp++];
                    int b3 = src[sp++];
                    if (IsMalformed3Prefix(b1, b2) || IsNotContinuation(b3))
                    {
                        dst[dp++] = Replacement;
                        sp -= IsMalformed3Prefix(b1, b2) ? 2 : 1;
                    }
                    else
                    {
                        char c = (char)(((b1 & 0x0F) << 12) | ((b2 & 0x3F) << 6) | (b3 & 0x3F));
                        dst[dp++] = char.IsSurrogate(c) ? Replacement : c;
                    }

                    continue;
                }

                if (sp < sl && IsMalformed3Prefix(b1, src[sp]))
                {
                    dst[dp++] = Replacement;
                    continue;
                }

                dst[dp++] = Replacement;
                break;
            }
            else if (b1 >= 0xF0 && b1 <= 0xF7)
            {
                if (sp + 2 < sl)
                {
                    int b2 = src[sp++];
                    int b3 = src[sp++];
                    int b4 = src[sp++];
                    int uc = ((b1 & 0x07) << 18) | ((b2 & 0x3F) << 12) | ((b3 & 0x3F) << 6) | (b4 & 0x3F);
                    if (IsNotContinuation(b2) || IsNotContinuation(b3) || IsNotContinuation(b4)
                        || uc < 0x10000 || uc > 0x10FFFF)
                    {
                        dst[dp++] = Replacement;
                        sp -= (b1 > 0xF4 || IsMalformed4Prefix(b1, b2)) ? 3 : (IsNotContinuation(b3) ? 2 : 1);
                    }
                    else
                    {
                        uc -= 0x10000;
                        dst[dp++] = (char)(0xD800 + (uc >> 10));
                        dst[dp++] = (char)(0xDC00 + (uc & 0x3FF));
                    }

                    continue;
                }

                if (b1 > 0xF4 || (sp < sl && IsMalformed4Prefix(b1, src[sp])))
                {
                    dst[dp++] = Replacement;
                    continue;
                }

                sp++;
                dst[dp++] = Replacement;
                if (sp < sl && IsNotContinuation(src[sp]))
                {
                    continue;
                }

                break;
            }
            else
            {
                dst[dp++] = Replacement;
            }
        }

        return new string(dst, 0, dp);
    }

    private static bool IsNotContinuation(int b) => (b & 0xC0) != 0x80;

    private static bool IsMalformed3Prefix(int b1, int b2) =>
        (b1 == 0xE0 && (b2 & 0xE0) == 0x80) || IsNotContinuation(b2);

    private static bool IsMalformed4Prefix(int b1, int b2) =>
        (b1 == 0xF0 && (b2 < 0x90 || b2 > 0xBF)) || (b1 == 0xF4 && (b2 & 0xF0) != 0x80) || IsNotContinuation(b2);

    // Java's String for US-ASCII and ISO-8859-1: a byte above the charset's range is malformed and becomes U+FFFD.
    private static string DecodeSingleByte(ReadOnlySpan<byte> data, int max)
    {
        if (data.IsEmpty)
        {
            return string.Empty;
        }

        char[] chars = new char[data.Length];
        for (int i = 0; i < data.Length; i++)
        {
            chars[i] = data[i] <= max ? (char)data[i] : Replacement;
        }

        return new string(chars);
    }

    // Java's UnicodeDecoder: with no expected order, a leading FE FF or FF FE mark is read and dropped, and the order
    // is big-endian without one; a high surrogate not followed by a low one is malformed with its next unit (one
    // U+FFFD for both), a lone low surrogate is malformed alone, and bytes left at the end are one U+FFFD.
    private static string DecodeUtf16(ReadOnlySpan<byte> data, int expected)
    {
        if (data.IsEmpty)
        {
            return string.Empty;
        }

        char[] chars = new char[(data.Length / 2) + 1];
        int count = 0;
        int order = expected;
        int i = 0;
        while (data.Length - i > 1)
        {
            if (order == ByteOrderNone)
            {
                char first = (char)((data[i] << 8) | data[i + 1]);
                if (first == '\uFEFF' || first == '\uFFFE')
                {
                    order = first == '\uFEFF' ? ByteOrderBig : ByteOrderLittle;
                    i += 2;
                    continue;
                }

                order = ByteOrderBig;
            }

            char c = Unit16(data, i, order);
            if (!char.IsSurrogate(c))
            {
                chars[count++] = c;
                i += 2;
            }
            else if (char.IsLowSurrogate(c))
            {
                chars[count++] = Replacement;
                i += 2;
            }
            else if (data.Length - i < 4)
            {
                break;
            }
            else
            {
                char low = Unit16(data, i + 2, order);
                if (char.IsLowSurrogate(low))
                {
                    chars[count++] = c;
                    chars[count++] = low;
                }
                else
                {
                    chars[count++] = Replacement;
                }

                i += 4;
            }
        }

        if (i < data.Length)
        {
            chars[count++] = Replacement;
        }

        return new string(chars, 0, count);
    }

    private static char Unit16(ReadOnlySpan<byte> data, int at, int order) =>
        order == ByteOrderBig
            ? (char)((data[at] << 8) | data[at + 1])
            : (char)((data[at + 1] << 8) | data[at]);

    // Java's UTF_32Coder.Decoder: a leading big-endian mark is dropped unless the expected order is little-endian, a
    // leading little-endian mark unless it is big-endian; the order is then the expected one, or big-endian. A unit
    // above U+10FFFF is malformed (U+FFFD); a surrogate value is kept as a char, as Java keeps any BMP value; bytes
    // left at the end are one U+FFFD.
    private static string DecodeUtf32(ReadOnlySpan<byte> data, int expected)
    {
        if (data.IsEmpty)
        {
            return string.Empty;
        }

        char[] chars = new char[((data.Length / 4) * 2) + 1];
        int count = 0;
        int i = 0;
        int order = expected == ByteOrderNone ? ByteOrderBig : expected;
        if (data.Length >= 4)
        {
            uint first = Unit32(data, 0, ByteOrderBig);
            if (first == 0x0000FEFFu && expected != ByteOrderLittle)
            {
                order = ByteOrderBig;
                i = 4;
            }
            else if (first == 0xFFFE0000u && expected != ByteOrderBig)
            {
                order = ByteOrderLittle;
                i = 4;
            }
        }

        while (data.Length - i >= 4)
        {
            uint codePoint = Unit32(data, i, order);
            if (codePoint <= 0xFFFFu)
            {
                chars[count++] = (char)codePoint;
            }
            else if (codePoint <= 0x10FFFFu)
            {
                codePoint -= 0x10000u;
                chars[count++] = (char)(0xD800u + (codePoint >> 10));
                chars[count++] = (char)(0xDC00u + (codePoint & 0x3FFu));
            }
            else
            {
                chars[count++] = Replacement;
            }

            i += 4;
        }

        if (i < data.Length)
        {
            chars[count++] = Replacement;
        }

        return new string(chars, 0, count);
    }

    private static uint Unit32(ReadOnlySpan<byte> data, int at, int order) =>
        order == ByteOrderBig
            ? ((uint)data[at] << 24) | ((uint)data[at + 1] << 16) | ((uint)data[at + 2] << 8) | data[at + 3]
            : ((uint)data[at + 3] << 24) | ((uint)data[at + 2] << 16) | ((uint)data[at + 1] << 8) | data[at];
}
