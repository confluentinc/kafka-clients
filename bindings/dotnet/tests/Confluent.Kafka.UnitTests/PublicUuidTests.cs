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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="Uuid"/>'s text form is the identity Kafka prints: the URL-safe, unpadded
/// base64 encoding of 16 big-endian bytes (Java's <c>Uuid.toString()</c>), and the exact
/// form the C ABI exchanges topic ids in. Getting the byte order or the alphabet wrong
/// would still round-trip through itself, so these tests pin it against <b>known
/// vectors</b> rather than against a round trip alone.
/// </summary>
public sealed class PublicUuidTests
{
    [Theory]
    // Two big-endian longs → 16 bytes → base64url without padding, 22 chars.
    [InlineData(0L, 0L, "AAAAAAAAAAAAAAAAAAAAAA")]
    [InlineData(1L, 2L, "AAAAAAAAAAEAAAAAAAAAAg")]
    [InlineData(-1L, -1L, "_____________________w")]
    [InlineData(0x0123456789ABCDEFL, 0x1122334455667788L, "ASNFZ4mrze8RIjNEVWZ3iA")]
    public void ToString_MatchesJavasBase64UrlForm(long mostSignificant, long leastSignificant, string expected)
    {
        Assert.Equal(expected, new Uuid(mostSignificant, leastSignificant).ToString());
    }

    [Theory]
    [InlineData(0L, 0L)]
    [InlineData(1L, 2L)]
    [InlineData(-1L, -1L)]
    [InlineData(long.MinValue, long.MaxValue)]
    [InlineData(0x0123456789ABCDEFL, 0x1122334455667788L)]
    public void Parse_InvertsToString(long mostSignificant, long leastSignificant)
    {
        Uuid original = new Uuid(mostSignificant, leastSignificant);

        Uuid parsed = Uuid.Parse(original.ToString());

        Assert.Equal(original, parsed);
        Assert.Equal(mostSignificant, parsed.MostSignificantBits);
        Assert.Equal(leastSignificant, parsed.LeastSignificantBits);
    }

    /// <summary>
    /// The URL-safe alphabet is load-bearing: standard base64 would emit <c>+</c> and
    /// <c>/</c>, which are not what a broker or another Kafka client prints.
    /// </summary>
    [Fact]
    public void ToString_UsesTheUrlSafeAlphabet_AndNoPadding()
    {
        // -1/-1 is all ones, which standard base64 renders with '+' and '/'.
        string text = new Uuid(-1L, -1L).ToString();

        Assert.Equal(22, text.Length);
        Assert.DoesNotContain("+", text, StringComparison.Ordinal);
        Assert.DoesNotContain("/", text, StringComparison.Ordinal);
        Assert.DoesNotContain("=", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Zero_IsTheAllZeroIdentifier()
    {
        Assert.Equal(new Uuid(0L, 0L), Uuid.Zero);
        Assert.Equal(0L, Uuid.Zero.MostSignificantBits);
        Assert.Equal(0L, Uuid.Zero.LeastSignificantBits);
    }

    [Fact]
    public void EqualityAndHashing_ComparesBothHalves()
    {
        Uuid a = new Uuid(1L, 2L);
        Uuid b = new Uuid(1L, 2L);
        Uuid c = new Uuid(1L, 3L);

        Assert.True(a == b);
        Assert.False(a == c);
        Assert.True(a != c);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.True(a.Equals((object)b));
        Assert.False(a.Equals("not a uuid"));
    }

    /// <summary>
    /// The two conditions Java's <c>fromString</c> rejects, with the same messages.
    /// </summary>
    [Fact]
    public void Parse_RejectsWhatJavaRejects()
    {
        Assert.Equal("value", Assert.Throws<ArgumentNullException>(() => Uuid.Parse(null!)).ParamName);

        // 23 characters — longer than a base64 UUID can be.
        ArgumentException tooLong = Assert.Throws<ArgumentException>(
            () => Uuid.Parse("AAAAAAAAAAAAAAAAAAAAAAA"));
        Assert.StartsWith(
            "Input string with prefix `AAAAAAAAAAAAAAAAAAAAAA` is too long to be decoded as a base64 UUID",
            tooLong.Message,
            StringComparison.Ordinal);

        // Well-formed base64, but only 3 bytes.
        ArgumentException wrongLength = Assert.Throws<ArgumentException>(() => Uuid.Parse("AAAA"));
        Assert.StartsWith(
            "Input string `AAAA` decoded as 3 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
            wrongLength.Message,
            StringComparison.Ordinal);

        ArgumentException notBase64 = Assert.Throws<ArgumentException>(() => Uuid.Parse("!!!!"));
        Assert.StartsWith(
            "Input string `!!!!` is not a valid base64 UUID",
            notBase64.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void TryParse_ReportsFailureWithoutThrowing()
    {
        Assert.False(Uuid.TryParse(null, out Uuid fromNull));
        Assert.Equal(Uuid.Zero, fromNull);

        Assert.False(Uuid.TryParse("AAAAAAAAAAAAAAAAAAAAAAA", out _));
        Assert.False(Uuid.TryParse("AAAA", out _));
        Assert.False(Uuid.TryParse("!!!!", out _));

        Assert.True(Uuid.TryParse("AAAAAAAAAAEAAAAAAAAAAg", out Uuid parsed));
        Assert.Equal(new Uuid(1L, 2L), parsed);
    }
}
