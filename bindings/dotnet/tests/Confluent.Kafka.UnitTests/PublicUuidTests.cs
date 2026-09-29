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
    /// The three conditions Java's <c>fromString</c> rejects. The two length conditions
    /// carry Java's own messages verbatim, <b>including Java's bound of 24</b>
    /// (<c>Uuid.java:131</c>) rather than the canonical 22 — see
    /// <see cref="Parse_AcceptsThePaddedFormJavaAccepts"/> for why the two extra
    /// characters are not slack.
    /// </summary>
    [Fact]
    public void Parse_RejectsWhatJavaRejects()
    {
        Assert.Equal("value", Assert.Throws<ArgumentNullException>(() => Uuid.Parse(null!)).ParamName);

        // 25 characters — past Java's own bound, so Java reports it as too long, quoting
        // the first 24 (Uuid.java:132-133).
        string twentyFive = new string('A', 25);
        ArgumentException tooLong = Assert.Throws<ArgumentException>(() => Uuid.Parse(twentyFive));
        Assert.StartsWith(
            "Input string with prefix `" + new string('A', 24) + "` is too long to be decoded as a base64 UUID",
            tooLong.Message,
            StringComparison.Ordinal);

        // 23 characters is NOT "too long" to Java: it decodes — to 17 bytes — so Java
        // reports the byte count instead. Gating at 22 would report the wrong condition.
        string twentyThree = new string('A', 23);
        ArgumentException wrongCount = Assert.Throws<ArgumentException>(() => Uuid.Parse(twentyThree));
        Assert.StartsWith(
            "Input string `" + twentyThree + "` decoded as 17 bytes, which is not equal to the expected 16 bytes of a base64-encoded UUID",
            wrongCount.Message,
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

    /// <summary>
    /// <b>The alphabet is not a formality.</b> Java decodes with
    /// <c>Base64.getUrlDecoder()</c>, which <b>rejects</b> the standard alphabet's
    /// <c>+</c> and <c>/</c>; <c>Convert.FromBase64String</c> accepts them. Left
    /// unscreened, <c>Parse</c> would admit an id Java rejects — and, worse, one whose
    /// <c>ToString()</c> prints a <em>different</em> string from the text it was parsed
    /// from, since <c>ToString</c> always emits the URL-safe alphabet.
    /// </summary>
    [Fact]
    public void Parse_RejectsTheStandardBase64Alphabet()
    {
        // Derived from a canonical form so the two spellings provably encode the SAME 16
        // bytes and differ in nothing but the alphabet: -1/-1 is all ones, whose
        // URL-safe form is all '_', and whose standard form is all '/'.
        string urlSafe = new Uuid(-1L, -1L).ToString();
        string standard = urlSafe.Replace('_', '/');
        Assert.NotEqual(urlSafe, standard);

        ArgumentException slash = Assert.Throws<ArgumentException>(() => Uuid.Parse(standard));
        Assert.StartsWith(
            "Input string `" + standard + "` is not a valid base64 UUID",
            slash.Message,
            StringComparison.Ordinal);
        Assert.Equal("value", slash.ParamName);

        Assert.False(Uuid.TryParse(standard, out Uuid rejected));
        Assert.Equal(Uuid.Zero, rejected);

        // '+' is rejected for the same reason. This one has 16 decodable bytes and a
        // canonical URL-safe twin, so the alphabet is the ONLY thing wrong with it.
        Assert.Throws<ArgumentException>(() => Uuid.Parse("AAAAAAAAAAAAAAAAAAAA+A"));
        Assert.Equal("AAAAAAAAAAAAAAAAAAAA-A", Uuid.Parse("AAAAAAAAAAAAAAAAAAAA-A").ToString());

        // …while the URL-safe spelling of the rejected value parses and round-trips.
        Assert.Equal(new Uuid(-1L, -1L), Uuid.Parse(urlSafe));
    }

    /// <summary>
    /// Java's bound is <c>length() &gt; 24</c>, not 22, because
    /// <c>Base64.getUrlDecoder()</c> accepts the <b>padded</b> 24-character form and
    /// decodes it to exactly 16 bytes — so <c>fromString</c> accepts it too. Gating at 22
    /// would reject an input Java parses.
    /// </summary>
    [Fact]
    public void Parse_AcceptsThePaddedFormJavaAccepts()
    {
        Uuid expected = new Uuid(1L, 2L);

        Assert.Equal(expected, Uuid.Parse("AAAAAAAAAAEAAAAAAAAAAg=="));
        Assert.True(Uuid.TryParse("AAAAAAAAAAEAAAAAAAAAAg==", out Uuid parsed));
        Assert.Equal(expected, parsed);

        // ToString still emits the canonical unpadded form.
        Assert.Equal("AAAAAAAAAAEAAAAAAAAAAg", parsed.ToString());
    }

    /// <summary>
    /// <b>Padding that is present but incomplete is a rejection, not something to
    /// repair.</b> Accepting the padded 24-character form (above) means tolerating an
    /// <c>=</c>, and the way <c>Parse</c> reaches
    /// <c>Convert.FromBase64String</c> is by padding to a multiple of four — which would
    /// just as happily <em>complete</em> a terminal unit that is short of its own padding.
    /// <c>"…Ag="</c> is two data characters and a single <c>=</c> where two are required;
    /// Java's <c>Base64.Decoder.decode0</c> labels that case
    /// <c>xx= shiftto==6&amp;&amp;sp==sl missing last =</c> and throws
    /// <c>"Input byte array has wrong 4-byte ending unit"</c>. Repaired instead of
    /// rejected, it yields an id that prints a <em>different</em> string from the text it
    /// was parsed from — the same symptom that makes the alphabet screen worth having.
    /// </summary>
    [Fact]
    public void Parse_RejectsPaddingJavaRejects()
    {
        // Derived from a value Parse accepts, by deleting one '=', so the padding shape is
        // provably the ONLY thing wrong with it.
        string padded = new Uuid(1L, 2L) + "==";
        Assert.Equal(24, padded.Length);
        Assert.Equal(new Uuid(1L, 2L), Uuid.Parse(padded));

        string shortOfPadding = padded.Substring(0, padded.Length - 1);
        Assert.Equal(23, shortOfPadding.Length);

        ArgumentException rejected =
            Assert.Throws<ArgumentException>(() => Uuid.Parse(shortOfPadding));
        Assert.StartsWith(
            "Input string `" + shortOfPadding + "` is not a valid base64 UUID",
            rejected.Message,
            StringComparison.Ordinal);
        Assert.Equal("value", rejected.ParamName);

        Assert.False(Uuid.TryParse(shortOfPadding, out Uuid fromTryParse));
        Assert.Equal(Uuid.Zero, fromTryParse);

        // …and the screen is narrow: unpadded text still gets its padding synthesized.
        Assert.Equal(new Uuid(1L, 2L), Uuid.Parse(padded.Substring(0, 22)));
    }

    [Fact]
    public void TryParse_ReportsFailureWithoutThrowing()
    {
        Assert.False(Uuid.TryParse(null, out Uuid fromNull));
        Assert.Equal(Uuid.Zero, fromNull);

        Assert.False(Uuid.TryParse("AAAAAAAAAAAAAAAAAAAAAAAAA", out _));
        Assert.False(Uuid.TryParse("AAAAAAAAAAAAAAAAAAAAAAA", out _));
        Assert.False(Uuid.TryParse("AAAA", out _));
        Assert.False(Uuid.TryParse("!!!!", out _));

        Assert.True(Uuid.TryParse("AAAAAAAAAAEAAAAAAAAAAg", out Uuid parsed));
        Assert.Equal(new Uuid(1L, 2L), parsed);
    }
}
