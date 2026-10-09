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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

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

    /// <summary>
    /// Java's reserved constants (<c>Uuid.java:37-52</c>): <c>ONE_UUID</c> is
    /// <c>(0, 1)</c>, <c>METADATA_TOPIC_ID</c> <b>is</b> <c>ONE_UUID</c>, and
    /// <c>RESERVED</c> holds exactly <c>ZERO_UUID</c> and <c>ONE_UUID</c> — read-only, so a
    /// caller cannot un-reserve one by casting.
    /// </summary>
    [Fact]
    public void ReservedConstants_MirrorJava()
    {
        Assert.Equal(new Uuid(0L, 1L), Uuid.One);
        Assert.Equal("AAAAAAAAAAAAAAAAAAAAAQ", Uuid.One.ToString());
        Assert.Equal(Uuid.One, Uuid.MetadataTopicId);

        Assert.Equal(2, Uuid.Reserved.Count);
        Assert.Contains(Uuid.Zero, Uuid.Reserved);
        Assert.Contains(Uuid.One, Uuid.Reserved);
        Assert.Equal(
            new HashSet<Uuid> { Uuid.Zero, Uuid.One },
            new HashSet<Uuid>(Uuid.Reserved));

        ICollection<Uuid> asCollection = Assert.IsAssignableFrom<ICollection<Uuid>>(Uuid.Reserved);
        Assert.True(asCollection.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => asCollection.Remove(Uuid.Zero));
        Assert.Throws<NotSupportedException>(() => asCollection.Add(new Uuid(0L, 2L)));
    }

    /// <summary>
    /// The public shape of the members G1-7 adds: Java's three constants as static
    /// read-only properties of Java's types, <c>randomUuid()</c> as one public
    /// parameterless static (the source-taking overload is the <b>internal</b> test seam,
    /// not surface), and exactly Java's interface list — <c>Comparable&lt;Uuid&gt;</c> is
    /// the only one Java implements, so no non-generic <see cref="IComparable"/> either.
    /// </summary>
    [Fact]
    public void Shape_AddsJavasConstants_RandomUuid_AndComparable()
    {
        Assert.Equal(
            new HashSet<Type> { typeof(IEquatable<Uuid>), typeof(IComparable<Uuid>) },
            new HashSet<Type>(typeof(Uuid).GetInterfaces()));

        foreach ((string name, Type type) in new[]
        {
            (nameof(Uuid.One), typeof(Uuid)),
            (nameof(Uuid.MetadataTopicId), typeof(Uuid)),
            (nameof(Uuid.Reserved), typeof(IReadOnlyCollection<Uuid>)),
        })
        {
            PropertyInfo property = typeof(Uuid).GetProperty(name, BindingFlags.Public | BindingFlags.Static)!;
            Assert.NotNull(property);
            Assert.Equal(type, property.PropertyType);
            Assert.Null(property.SetMethod);
        }

        MethodInfo randomUuid = Assert.Single(
            typeof(Uuid).GetMethods(BindingFlags.Public | BindingFlags.Static),
            method => method.Name == nameof(Uuid.RandomUuid));
        Assert.Equal(typeof(Uuid), randomUuid.ReturnType);
        Assert.Empty(randomUuid.GetParameters());

        MethodInfo compareTo = typeof(Uuid).GetMethod(nameof(Uuid.CompareTo), new[] { typeof(Uuid) })!;
        Assert.Equal(typeof(int), compareTo.ReturnType);
    }

    /// <summary>
    /// ⚠ <b>Signed, as Java compares</b> (<c>Uuid.java:154-167</c>): a <c>long</c> with the
    /// high bit set is <em>negative</em>, so it sorts before <see cref="Uuid.Zero"/>. An
    /// unsigned comparison — the natural reading of "128-bit value" — would put
    /// <c>long.MinValue</c> last instead. The return values are Java's exact
    /// <c>1</c> / <c>-1</c> / <c>0</c>, not merely their signs.
    /// </summary>
    [Fact]
    public void CompareTo_IsJavasSignedOrder_MostSignificantFirst()
    {
        Uuid[] ascending =
        {
            new Uuid(long.MinValue, 0L),
            new Uuid(-1L, 0L),
            new Uuid(0L, long.MinValue),
            new Uuid(0L, -1L),
            new Uuid(0L, 0L),
            new Uuid(0L, 1L),
            new Uuid(0L, long.MaxValue),
            new Uuid(1L, long.MinValue),
            new Uuid(1L, 0L),
            new Uuid(long.MaxValue, 0L),
        };

        for (int i = 0; i < ascending.Length; i++)
        {
            for (int j = 0; j < ascending.Length; j++)
            {
                int expected = i < j ? -1 : i > j ? 1 : 0;
                Assert.Equal(expected, ascending[i].CompareTo(ascending[j]));

                // CompareTo == 0 exactly when Equals — Java's consistent-with-equals.
                Assert.Equal(expected == 0, ascending[i].Equals(ascending[j]));
            }
        }

        // The most significant half decides before the least significant one is read.
        Assert.Equal(-1, new Uuid(0L, long.MaxValue).CompareTo(new Uuid(1L, long.MinValue)));
        Assert.Equal(1, new Uuid(long.MaxValue, long.MinValue).CompareTo(new Uuid(long.MinValue, long.MaxValue)));

        // And a sort uses it: a scrambled copy sorts back into Java's order.
        List<Uuid> scrambled = new List<Uuid>
        {
            ascending[4], ascending[9], ascending[0], ascending[6], ascending[2],
            ascending[8], ascending[1], ascending[5], ascending[3], ascending[7],
        };
        scrambled.Sort();
        Assert.Equal(ascending, scrambled);
    }

    /// <summary>
    /// <see cref="Uuid.RandomUuid()"/> over many draws: never reserved, never
    /// <c>'-'</c>-leading, and always Java's <c>UUID.randomUUID()</c> layout — version
    /// nibble <c>4</c> and the IETF variant bits <c>10</c>.
    /// </summary>
    /// <remarks>
    /// This also discriminates the <em>wiring</em> between the public method and the seam:
    /// one raw version-4 draw in 64 prints with a leading <c>'-'</c> (the first six bits are
    /// fully random), so a public method that bypassed the seam's loop would fail the
    /// <c>'-'</c> assertion within 1000 draws with probability
    /// <c>1 - (63/64)^1000 &gt; 0.9999998</c>.
    /// </remarks>
    [Fact]
    public void RandomUuid_IsVersion4_NeverReserved_AndNeverDashLeading()
    {
        HashSet<Uuid> seen = new HashSet<Uuid>();
        for (int i = 0; i < 1000; i++)
        {
            Uuid uuid = Uuid.RandomUuid();

            Assert.DoesNotContain(uuid, Uuid.Reserved);
            Assert.NotEqual('-', uuid.ToString()[0]);
            Assert.Equal(4L, (uuid.MostSignificantBits >> 12) & 0xF);
            Assert.Equal(2UL, (ulong)uuid.LeastSignificantBits >> 62);
            Assert.True(seen.Add(uuid), "a random 122-bit draw repeated");
        }
    }

    /// <summary>
    /// Java's loop, through the seam the public method uses: the source is drawn until a
    /// candidate is neither reserved nor <c>'-'</c>-leading. <c>0xF8…</c> encodes to a
    /// leading <c>'-'</c> (its first six bits are <c>111110</c>, index 62 of the URL-safe
    /// alphabet). All four candidates are consumed, and the fourth is returned.
    /// </summary>
    [Fact]
    public void RandomUuid_Seam_SkipsReservedAndDashLeadingCandidates()
    {
        Uuid dashLeading = new Uuid(unchecked((long)0xF800000000000000UL), 1L);
        Uuid valid = new Uuid(0x0123456789ABCDEFL, 0x1122334455667788L);
        Assert.Equal('-', dashLeading.ToString()[0]);

        Queue<Uuid> candidates = new Queue<Uuid>(new[] { Uuid.Zero, Uuid.One, dashLeading, valid });

        Uuid chosen = Uuid.RandomUuid(candidates.Dequeue);

        Assert.Equal(valid, chosen);
        Assert.Empty(candidates);

        // Only '-' is rejected — '_' (index 63, 0xFC…) is Java-legal and returned at once.
        Uuid underscoreLeading = new Uuid(unchecked((long)0xFC00000000000000UL), 1L);
        Assert.Equal('_', underscoreLeading.ToString()[0]);
        Queue<Uuid> single = new Queue<Uuid>(new[] { underscoreLeading, valid });
        Assert.Equal(underscoreLeading, Uuid.RandomUuid(single.Dequeue));
        Assert.Single(single);
    }

    /// <summary>
    /// The bit layout <c>java.util.UUID.randomUUID()</c> stamps onto its 16 random bytes —
    /// <c>bytes[6]</c> version <c>0100</c>, <c>bytes[8]</c> variant <c>10</c>, everything
    /// else untouched — checked at both extremes so a wrong mask cannot hide behind random
    /// input: all-ones keeps every other bit set, all-zeros keeps every other bit clear.
    /// </summary>
    [Fact]
    public void FromVersion4Bytes_StampsJavasVersionAndVariantBits()
    {
        byte[] ones = Enumerable.Repeat((byte)0xFF, 16).ToArray();
        Uuid fromOnes = Uuid.FromVersion4Bytes(ones);
        Assert.Equal(unchecked((long)0xFFFFFFFFFFFF4FFFUL), fromOnes.MostSignificantBits);
        Assert.Equal(unchecked((long)0xBFFFFFFFFFFFFFFFUL), fromOnes.LeastSignificantBits);

        Uuid fromZeros = Uuid.FromVersion4Bytes(new byte[16]);
        Assert.Equal(0x0000000000004000L, fromZeros.MostSignificantBits);
        Assert.Equal(long.MinValue, fromZeros.LeastSignificantBits);
    }
}
