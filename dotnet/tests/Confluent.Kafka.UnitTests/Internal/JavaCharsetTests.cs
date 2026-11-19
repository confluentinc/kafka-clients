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
using System.Text;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Internal;

/// <summary>
/// The charsets the String and UUID serdes can be configured with: Java's names and aliases, ignoring ASCII case only,
/// and Java's bytes for each, read from the JDK's encoders and decoders (<c>UnicodeEncoder</c>,
/// <c>UnicodeDecoder</c>, <c>UTF_32Coder</c>, <c>String</c>'s US-ASCII and ISO-8859-1 paths).
/// </summary>
public sealed class JavaCharsetTests
{
    private static readonly Dictionary<string, string[]> s_names = new Dictionary<string, string[]>
    {
        ["UTF-8"] = new[] { "UTF-8", "UTF8", "unicode-1-1-utf-8" },
        ["UTF-16"] = new[] { "UTF-16", "UTF_16", "utf16", "unicode", "UnicodeBig" },
        ["UTF-16BE"] = new[] { "UTF-16BE", "UTF_16BE", "ISO-10646-UCS-2", "X-UTF-16BE", "UnicodeBigUnmarked" },
        ["UTF-16LE"] = new[] { "UTF-16LE", "UTF_16LE", "X-UTF-16LE", "UnicodeLittleUnmarked" },
        ["UTF-32"] = new[] { "UTF-32", "UTF_32", "UTF32" },
        ["UTF-32BE"] = new[] { "UTF-32BE", "UTF_32BE", "X-UTF-32BE" },
        ["UTF-32LE"] = new[] { "UTF-32LE", "UTF_32LE", "X-UTF-32LE" },
        ["US-ASCII"] = new[]
        {
            "US-ASCII", "iso-ir-6", "ANSI_X3.4-1986", "ISO_646.irv:1991", "ASCII", "ISO646-US", "us", "IBM367", "cp367",
            "csASCII", "646", "iso_646.irv:1983", "ANSI_X3.4-1968", "ascii7",
        },
        ["ISO-8859-1"] = new[]
        {
            "ISO-8859-1", "iso-ir-100", "ISO_8859-1", "latin1", "l1", "IBM819", "cp819", "csISOLatin1", "819", "IBM-819",
            "ISO8859_1", "ISO_8859-1:1987", "ISO_8859_1", "8859_1", "ISO8859-1",
        },
    };

    [Fact]
    public void ForName_FindsEveryJavaNameAndAlias_InAnyAsciiCase()
    {
        foreach (KeyValuePair<string, string[]> entry in s_names)
        {
            JavaCharset? charset = JavaCharset.ForName(entry.Key);
            Assert.NotNull(charset);
            foreach (string name in entry.Value)
            {
                Assert.Same(charset, JavaCharset.ForName(name));
                Assert.Same(charset, JavaCharset.ForName(name.ToUpperInvariant()));
                Assert.Same(charset, JavaCharset.ForName(name.ToLowerInvariant()));
            }
        }

        Assert.Same(JavaCharset.Utf8, JavaCharset.ForName("utf-8"));
    }

    [Fact]
    public void ForName_RefusesEveryOtherName_ARealJavaCharsetIncluded()
    {
        string[] refused =
        {
            "windows-1252", "x-UTF-16LE-BOM", "UTF_8", "UTF-7", string.Empty, " UTF-8", "UTF-8 ",
            "\u0131so-8859-1", "u\u017F-ascii", "encoding-does-not-exist",
        };

        foreach (string name in refused)
        {
            Assert.Null(JavaCharset.ForName(name));
        }
    }

    [Fact]
    public void Utf8_UnpairedSurrogateIsAQuestionMark_AndMalformedInputIsUFFFD()
    {
        JavaCharset utf8 = JavaCharset.Utf8;

        Assert.Equal(Hex("61 3F 62 F0 9F 98 80"), utf8.Encode("a\uD800b\uD83D\uDE00"));
        Assert.Equal("a\uFFFDb", utf8.Decode(Hex("61 FF 62")));
        Assert.Equal(string.Empty, utf8.Decode(Array.Empty<byte>()));
    }

    [Theory]
    [InlineData("ED A0 80", "FFFD")]
    [InlineData("ED A0 80 ED B0 80", "FFFD FFFD")]
    [InlineData("41 ED A0", "0041 FFFD")]
    [InlineData("ED BF", "FFFD")]
    [InlineData("ED A0 41", "FFFD 0041")]
    [InlineData("E0 80 80", "FFFD FFFD FFFD")]
    [InlineData("E0 A0", "FFFD")]
    [InlineData("E0 80", "FFFD FFFD")]
    [InlineData("C0 80", "FFFD FFFD")]
    [InlineData("C2", "FFFD")]
    [InlineData("C2 41", "FFFD 0041")]
    [InlineData("80", "FFFD")]
    [InlineData("F8 80", "FFFD FFFD")]
    [InlineData("F0 90", "FFFD")]
    [InlineData("F0 90 80", "FFFD")]
    [InlineData("F0 90 41", "FFFD 0041")]
    [InlineData("F0 90 80 41", "FFFD 0041")]
    [InlineData("F0 80 80 80", "FFFD FFFD FFFD FFFD")]
    [InlineData("F4 90 80 80", "FFFD FFFD FFFD FFFD")]
    [InlineData("F5 80 80 80", "FFFD FFFD FFFD FFFD")]
    [InlineData("F5", "FFFD")]
    [InlineData("FF F0 9F 98 80", "FFFD D83D DE00")]
    [InlineData("EF BF BD 41 C3 A9", "FFFD 0041 00E9")]
    public void Utf8_MalformedInput_IsOneUFFFDPerSequence_AsJavasStringDelimitsIt(string bytes, string units)
    {
        Assert.Equal(units, Units(JavaCharset.Utf8.Decode(Hex(bytes))));
    }

#if NET8_0_OR_GREATER
    [Fact]
    public void Utf8_ValidInput_AllocatesOnlyTheString()
    {
        byte[] data = Encoding.UTF8.GetBytes(new string('\u00E9', 2048) + new string('a', 2048));
        long expected = 4096 * sizeof(char);

        long fewest = long.MaxValue;
        for (int i = 0; i < 5; i++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            string text = JavaCharset.Utf8.Decode(data);
            long after = GC.GetAllocatedBytesForCurrentThread();
            Assert.Equal(4096, text.Length);
            fewest = Math.Min(fewest, after - before);
        }

        Assert.True(fewest < expected + 256, $"decoding allocated {fewest} B for a {expected} B string");
    }
#endif

    [Fact]
    public void Utf16_WritesABigEndianMarkBeforeANonEmptyString_AndReadsAMarkOrBigEndian()
    {
        JavaCharset utf16 = Charset("UTF-16");

        Assert.Equal(Hex("FE FF 00 68 00 69"), utf16.Encode("hi"));
        Assert.Equal(Array.Empty<byte>(), utf16.Encode(string.Empty));
        Assert.Equal(Hex("FE FF D8 3D DE 00"), utf16.Encode("\uD83D\uDE00"));
        Assert.Equal(Hex("FE FF FF FD 00 61 FF FD"), utf16.Encode("\uD800a\uDC00"));
        Assert.Equal("hi", utf16.Decode(Hex("00 68 00 69")));
        Assert.Equal("hi", utf16.Decode(Hex("FE FF 00 68 00 69")));
        Assert.Equal("hi", utf16.Decode(Hex("FF FE 68 00 69 00")));
        Assert.Equal(string.Empty, utf16.Decode(Hex("FE FF")));
        Assert.Equal("\uFEFF", utf16.Decode(Hex("FE FF FE FF")));
    }

    [Fact]
    public void Utf16_MalformedInput_IsReplacedAsJavasUnicodeDecoderReplacesIt()
    {
        JavaCharset utf16 = Charset("UTF-16BE");

        Assert.Equal("\uD83D\uDE00", utf16.Decode(Hex("D8 3D DE 00")));
        Assert.Equal("\uFFFD", utf16.Decode(Hex("D8 00 00 61")));
        Assert.Equal("\uFFFDa", utf16.Decode(Hex("DC 00 00 61")));
        Assert.Equal("a\uFFFD", utf16.Decode(Hex("00 61 00")));
        Assert.Equal("a\uFFFD", utf16.Decode(Hex("00 61 D8 00")));
        Assert.Equal("a\uFFFD", utf16.Decode(Hex("00 61 D8 00 DC")));
        Assert.Equal("\uFFFD", utf16.Decode(Hex("61")));
    }

    [Fact]
    public void Utf16BeAndLe_WriteNoMark_AndKeepALeadingMarkAsACharacter()
    {
        JavaCharset be = Charset("UTF-16BE");
        JavaCharset le = Charset("UTF-16LE");

        Assert.Equal(Hex("00 68 00 69"), be.Encode("hi"));
        Assert.Equal(Hex("68 00 69 00"), le.Encode("hi"));
        Assert.Equal(Hex("FF FD"), be.Encode("\uD800"));
        Assert.Equal(Hex("FD FF"), le.Encode("\uD800"));
        Assert.Equal("\uFEFFh", be.Decode(Hex("FE FF 00 68")));
        Assert.Equal("\uFEFFh", le.Decode(Hex("FF FE 68 00")));
        Assert.Equal("\uD83D\uDE00", le.Decode(Hex("3D D8 00 DE")));
    }

    [Fact]
    public void Utf32_WritesBigEndianWithNoMark_AndReadsAMarkOrBigEndian()
    {
        JavaCharset utf32 = Charset("UTF-32");

        Assert.Equal(Hex("00 00 00 61 00 01 F6 00"), utf32.Encode("a\uD83D\uDE00"));
        Assert.Equal(Hex("00 00 FF FD 00 00 00 61"), utf32.Encode("\uDC00a"));
        Assert.Equal(Array.Empty<byte>(), utf32.Encode(string.Empty));
        Assert.Equal("a", utf32.Decode(Hex("00 00 00 61")));
        Assert.Equal("a", utf32.Decode(Hex("00 00 FE FF 00 00 00 61")));
        Assert.Equal("a", utf32.Decode(Hex("FF FE 00 00 61 00 00 00")));
        Assert.Equal("\uD83D\uDE00", utf32.Decode(Hex("00 01 F6 00")));
    }

    [Fact]
    public void Utf32_KeepsASurrogateValue_ReplacesAValueAboveU10FFFF_AndATrailingRemainder()
    {
        JavaCharset utf32 = Charset("UTF-32");

        Assert.Equal("\uD800", utf32.Decode(Hex("00 00 D8 00")));
        Assert.Equal("\uFFFDa", utf32.Decode(Hex("00 11 00 00 00 00 00 61")));
        Assert.Equal("\uFFFD", utf32.Decode(Hex("80 00 00 00")));
        Assert.Equal("a\uFFFD", utf32.Decode(Hex("00 00 00 61 00 00 00")));
        Assert.Equal("\uFFFD", utf32.Decode(Hex("00 00 FE")));
    }

    [Fact]
    public void Utf32BeAndLe_DropOnlyTheirOwnMark()
    {
        JavaCharset be = Charset("UTF-32BE");
        JavaCharset le = Charset("UTF-32LE");

        Assert.Equal(Hex("00 00 00 61"), be.Encode("a"));
        Assert.Equal(Hex("61 00 00 00"), le.Encode("a"));
        Assert.Equal(Hex("FD FF 00 00"), le.Encode("\uD800"));
        Assert.Equal("a", be.Decode(Hex("00 00 FE FF 00 00 00 61")));
        Assert.Equal("\uFFFDa", be.Decode(Hex("FF FE 00 00 00 00 00 61")));
        Assert.Equal("a", le.Decode(Hex("FF FE 00 00 61 00 00 00")));
        Assert.Equal("\uFFFDa", le.Decode(Hex("00 00 FE FF 61 00 00 00")));
    }

    [Fact]
    public void UsAscii_ACharacterAbove7F_IsAQuestionMark_APairOne_AndAByteAbove7F_IsUFFFD()
    {
        JavaCharset ascii = Charset("US-ASCII");

        Assert.Equal(Hex("3F 3F 3F 78"), ascii.Encode("\u00E9\uD83D\uDE00\uD800x"));
        Assert.Equal(Hex("61 3F 62 7F"), ascii.Encode("a\uDC00b\u007F"));
        Assert.Equal("a\uFFFD\u007F", ascii.Decode(Hex("61 80 7F")));
    }

    [Fact]
    public void Iso88591_ACharacterAboveFF_IsAQuestionMark_APairOne_AndEveryByteIsItsCharacter()
    {
        JavaCharset latin1 = Charset("ISO-8859-1");

        Assert.Equal(Hex("E9 FF 3F 3F 3F"), latin1.Encode("\u00E9\u00FF\u20AC\uD83D\uDE00\uD800"));
        Assert.Equal("\u0080\u00FF", latin1.Decode(Hex("80 FF")));
    }

    private static JavaCharset Charset(string name) => JavaCharset.ForName(name)!;

    private static string Units(string text)
    {
        string[] units = new string[text.Length];
        for (int i = 0; i < text.Length; i++)
        {
            units[i] = ((int)text[i]).ToString("X4", CultureInfo.InvariantCulture);
        }

        return string.Join(" ", units);
    }

    private static byte[] Hex(string hex)
    {
        string[] parts = hex.Split(' ');
        byte[] bytes = new byte[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            bytes[i] = Convert.ToByte(parts[i], 16);
        }

        return bytes;
    }
}
