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
using System.Collections.ObjectModel;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Numerics;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Internal;

/// <summary>
/// Tests <see cref="JavaText"/>: <see cref="JavaText.CoreText"/>, the text the core parses (exact digits, a double as
/// "G17", the special values as literals, a list joined with commas), and <see cref="JavaText.ValueOf"/>, Java's
/// <c>String.valueOf</c> for the binding's messages (Java's spellings, <c>Double.toString</c>,
/// <c>Float.toString</c>, <c>BigDecimal.toString</c>, <c>[a, b]</c> and <c>{k=v}</c>), both in the invariant culture, and
/// <see cref="JavaText.ToUpperRoot"/>, Java's <c>toUpperCase(Locale.ROOT)</c> for the letters whose upper case is ASCII.
/// </summary>
public sealed class JavaTextTests
{
    [Fact]
    public void ValueOf_Null_IsJavasNull() => Assert.Equal("null", JavaText.ValueOf(null));

    [Theory]
    [InlineData(true, "true")]
    [InlineData(false, "false")]
    public void ValueOf_Bool_IsJavasSpelling(bool value, string expected) =>
        Assert.Equal(expected, JavaText.ValueOf(value));

    [Theory]
    [InlineData("")]
    [InlineData("plain")]
    [InlineData("  padded  ")]
    [InlineData("café-Ω-日本語")]
    public void ValueOf_String_IsItself(string value) => Assert.Equal(value, JavaText.ValueOf(value));

    [Fact]
    public void ValueOf_Char_IsItself() => Assert.Equal("x", JavaText.ValueOf('x'));

    [Fact]
    public void ValueOf_Integers_AreTheirInvariantDigits()
    {
        Assert.Equal("-1", JavaText.ValueOf(-1));
        Assert.Equal("-32768", JavaText.ValueOf(short.MinValue));
        Assert.Equal("-9223372036854775808", JavaText.ValueOf(long.MinValue));
        Assert.Equal("-128", JavaText.ValueOf(sbyte.MinValue));
        Assert.Equal(
            "-123456789012345678901234567890",
            JavaText.ValueOf(BigInteger.Parse("-123456789012345678901234567890", CultureInfo.InvariantCulture)));
    }

    [Fact]
    public void ValueOf_AnyOtherObject_IsItsToString()
    {
        Assert.Equal("a value", JavaText.ValueOf(new Named("a value")));
        Assert.Equal("null", JavaText.ValueOf(new Named(null)));
        Assert.Equal("00:01:00", JavaText.ValueOf(TimeSpan.FromMinutes(1)));
        Assert.Equal("Monday", JavaText.ValueOf(DayOfWeek.Monday));
    }

    [Theory]
    [InlineData("de-DE")]
    [InlineData("sv-SE")]
    public void ValueOf_FormatsInTheInvariantCulture(string culture)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try
        {
            Assert.NotEqual("-1.5", (-1.5).ToString(CultureInfo.CurrentCulture));

            Assert.Equal("-1", JavaText.ValueOf(-1));
            Assert.Equal("-1.5", JavaText.ValueOf(-1.5));
            Assert.Equal("-1", JavaText.ValueOf(new BigInteger(-1)));
            Assert.Equal("-1.5E-5", JavaText.ValueOf(-1.5e-5));
            Assert.Equal("-0.5", JavaText.ValueOf(-0.5f));
            Assert.Equal("-12.50", JavaText.ValueOf(-12.50m));
            Assert.Equal("-1E-7", JavaText.ValueOf(-0.0000001m));
            Assert.Equal("[-1, -0.5]", JavaText.ValueOf(new object[] { -1, -0.5 }));
            Assert.Equal("-5", JavaText.ValueOf(new IntPtr(-5)));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void TestJavaStrIsStringValueOf()
    {
        var vectors = new (object? Value, string Text)[]
        {
            (null, "null"),
            (true, "true"),
            (false, "false"),
            (5, "5"),
            ("s", "s"),
            (1.0, "1.0"),
            (0.001, "0.001"),
            (1e7, "1.0E7"),
            (1.5e-5, "1.5E-5"),
            (-2.5e20, "-2.5E20"),
            (double.NaN, "NaN"),
            (double.PositiveInfinity, "Infinity"),
            (-0.0, "-0.0"),
            (new List<object?> { 1, null }, "[1, null]"),
            (new Dictionary<string, int> { ["a"] = 1 }, "{a=1}"),
            (new TopicPartition("t", 0), "t-0"),
        };

        foreach ((object? value, string text) in vectors)
        {
            Assert.Equal(text, JavaText.ValueOf(value));
        }
    }

    [Fact]
    public void TestJavaStrPrintsADoubleAsDoubleToString()
    {
        var vectors = new (double Value, string Text)[]
        {
            (5e-324, "4.9E-324"),
            (-5e-324, "-4.9E-324"),
            (1e-323, "9.9E-324"),
            (2.5e-323, "2.5E-323"),
            (5e-323, "4.9E-323"),
            (9e-323, "8.9E-323"),
            (1e-322, "9.9E-323"),
            (1.6e-322, "1.6E-322"),
            (2.2250738585072014e-308, "2.2250738585072014E-308"),
            (1.7976931348623157e308, "1.7976931348623157E308"),
            (1e23, "1.0E23"),
            (6.6939e20, "6.6939E20"),
            (2e22, "2.0E22"),
        };

        foreach ((double value, string text) in vectors)
        {
            AssertDoubleToString(text, value);
        }
    }

    [Fact]
    public void TestConvertValueToStringDouble_TheMessageTextIsJavasDoubleToString()
    {
        AssertDoubleToString("3.125", 3.125d);
        AssertDoubleToString("1.7976931348623157E308", double.MaxValue);
        AssertDoubleToString("1.024E8", 102400000d);
        AssertDoubleToString("-1.024E8", -102400000d);
    }

    [Fact]
    public void TestConvertValueToStringList_TheMessageTextIsJavasStringValueOf() =>
        Assert.Equal("[a, bc, d]", JavaText.ValueOf(new List<string> { "a", "bc", "d" }));

    [Fact]
    public void ValueOf_Double_UsesJavasPlainOrScientificNotation()
    {
        AssertDoubleToString("0.1", 0.1);
        AssertDoubleToString("100.0", 100.0);
        AssertDoubleToString("42.5", 42.5);
        AssertDoubleToString("1234567.0", 1234567.0);
        AssertDoubleToString("9999999.999999998", 9999999.999999998);
        AssertDoubleToString("1.2345678E7", 12345678.0);
        AssertDoubleToString("9.999E-4", 0.0009999);
        AssertDoubleToString("1.0E-5", 1e-5);
        AssertDoubleToString("0.0", 0.0);
        AssertDoubleToString("-Infinity", double.NegativeInfinity);
        AssertDoubleToString("4.9E-324", double.Epsilon);
    }

    [Fact]
    public void ValueOf_Float_IsJavasFloatToString()
    {
        AssertFloatToString("0.1", 0.1f);
        AssertFloatToString("3.125", 3.125f);
        AssertFloatToString("100.0", 100f);
        AssertFloatToString("1.0E7", 1e7f);
        AssertFloatToString("1.0E-5", 1e-5f);
        AssertFloatToString("1.4E-45", float.Epsilon);
        AssertFloatToString("-1.4E-45", -float.Epsilon);
        AssertFloatToString("3.4028235E38", float.MaxValue);
        AssertFloatToString("-0.0", -0.0f);
        AssertFloatToString("NaN", float.NaN);
        AssertFloatToString("-Infinity", float.NegativeInfinity);
    }

    [Fact]
    public void ValueOf_Decimal_IsJavasBigDecimalToString()
    {
        Assert.Equal("1E-7", JavaText.ValueOf(0.0000001m));
        Assert.Equal("1.0E-7", JavaText.ValueOf(0.00000010m));
        Assert.Equal("-1.23E-7", JavaText.ValueOf(-0.000000123m));
        Assert.Equal("0E-7", JavaText.ValueOf(0.0000000m));
        Assert.Equal("1E-28", JavaText.ValueOf(0.0000000000000000000000000001m));
        Assert.Equal("0.000001", JavaText.ValueOf(0.000001m));
        Assert.Equal("0.1", JavaText.ValueOf(0.1m));
        Assert.Equal("-12.50", JavaText.ValueOf(-12.50m));
        Assert.Equal("123", JavaText.ValueOf(123m));
        Assert.Equal("79228162514264337593543950335", JavaText.ValueOf(decimal.MaxValue));
        Assert.Equal("0.00", JavaText.ValueOf(new decimal(0, 0, 0, isNegative: true, scale: 2)));
    }

    [Fact]
    public void ValueOf_Collections_AreJavasListAndMapText()
    {
        Assert.Equal("[a, b]", JavaText.ValueOf(new[] { "a", "b" }));
        Assert.Equal("[]", JavaText.ValueOf(Array.Empty<int>()));
        Assert.Equal("[1, null, true, 0.5, 1.0E7]", JavaText.ValueOf(new List<object?> { 1, null, true, 0.5, 1e7 }));
        Assert.Equal("[3]", JavaText.ValueOf(new HashSet<int> { 3 }));
        Assert.Equal("{k=1.5, n=null}", JavaText.ValueOf(new Dictionary<string, object?> { ["k"] = 1.5, ["n"] = null }));
        Assert.Equal("[[1, 2], []]", JavaText.ValueOf(new List<int[]> { new[] { 1, 2 }, Array.Empty<int>() }));
    }

    [Fact]
    public void ValueOf_GenericCollections_AreJavasCollectionText()
    {
        var set = new HashSet<int> { 1, 2 };
        var readOnly = new ReadOnlyCollection<string>(new[] { "a", "b" });
        var bag = new ReadOnlyBag(1, 2);
        var mutableBag = new MutableBag(1, 2);

        Assert.Equal("[1, 2]", JavaText.ValueOf(set));
        Assert.Equal("[a, b]", JavaText.ValueOf(readOnly));
        Assert.Equal("[1, 2]", JavaText.ValueOf(bag));
        Assert.Equal("[1, 2]", JavaText.ValueOf(mutableBag));
        Assert.Equal("[1, 2]", JavaText.CoreText(set, null));
        Assert.Equal("[a, b]", JavaText.CoreText(readOnly, null));
        Assert.Equal("[1, 2]", JavaText.CoreText(bag, null));
        Assert.Equal("[1, 2]", JavaText.CoreText(mutableBag, null));
    }

    [Fact]
    public void ValueOf_GenericDictionaries_AreJavasMapText()
    {
        IDictionary<string, object?> expando = new ExpandoObject();
        expando["a"] = 1;
        var dictionary = new Dictionary<string, object?> { ["k"] = 1.5, ["n"] = null };
        var readOnly = new ReadOnlyMap(new Dictionary<string, object?> { ["k"] = 1.5, ["n"] = null });

        Assert.Equal("{a=1}", JavaText.ValueOf(expando));
        Assert.Equal("{k=1.5, n=null}", JavaText.ValueOf(dictionary));
        Assert.Equal("{k=1.5, n=null}", JavaText.ValueOf(readOnly));
        Assert.Equal("{a=1}", JavaText.CoreText(expando, null));
        Assert.Equal("{k=1.5, n=null}", JavaText.CoreText(dictionary, null));
        Assert.Equal("{k=1.5, n=null}", JavaText.CoreText(readOnly, null));
    }

    [Fact]
    public void ValueOf_Headers_IsTheirOwnToString()
    {
        Headers headers = new Headers().Add("a", new byte[] { 1 });

        Assert.Equal("RecordHeaders(headers = [RecordHeader(key = a, value = [1])], isReadOnly = false)", JavaText.ValueOf(headers));
        Assert.Equal("RecordHeaders(headers = [RecordHeader(key = a, value = [1])], isReadOnly = false)", JavaText.CoreText(headers, null));
    }

    [Fact]
    public void ValueOf_ASequenceThatIsNotACollection_IsItsToString_NeverEnumerated()
    {
        int projected = 0;
        IEnumerable<string> hosts = new List<string> { "a", "b" }.Select(host =>
        {
            projected++;
            return host;
        });
        var unenumerable = new Unenumerable("a lazy sequence");

        Assert.Equal(hosts.ToString(), JavaText.ValueOf(hosts));
        Assert.Equal(hosts.ToString(), JavaText.CoreText(hosts, null));
        Assert.Equal(0, projected);
        Assert.Equal("a lazy sequence", JavaText.ValueOf(unenumerable));
        Assert.Equal("a lazy sequence", JavaText.CoreText(unenumerable, null));
    }

    [Fact]
    public void ValueOf_BigInteger_IsItsDigits() =>
        Assert.Equal(
            "1000000000000000000000000000000",
            JavaText.ValueOf(BigInteger.Pow(10, 30)));

    [Fact]
    public void ValueOf_NumbersWithoutAJavaClass_AreTheirInvariantDigits()
    {
        Assert.Equal("255", JavaText.ValueOf((byte)255));
        Assert.Equal("65535", JavaText.ValueOf(ushort.MaxValue));
        Assert.Equal("4294967295", JavaText.ValueOf(uint.MaxValue));
        Assert.Equal("18446744073709551615", JavaText.ValueOf(ulong.MaxValue));
        Assert.Equal("-5", JavaText.ValueOf(new IntPtr(-5)));
        Assert.Equal("5", JavaText.ValueOf(new UIntPtr(5)));
#if NET5_0_OR_GREATER
        Assert.Equal("0.5", JavaText.ValueOf((Half)0.5));
#endif
#if NET7_0_OR_GREATER
        Assert.Equal("-170141183460469231731687303715884105728", JavaText.ValueOf(Int128.MinValue));
        Assert.Equal("340282366920938463463374607431768211455", JavaText.ValueOf(UInt128.MaxValue));
#endif
    }

    [Fact]
    public void CoreText_Integers_AreTheirInvariantDigits()
    {
        Assert.Equal("-5", JavaText.CoreText((sbyte)-5, ConfigType.Double));
        Assert.Equal("255", JavaText.CoreText((byte)255, ConfigType.Double));
        Assert.Equal("-32768", JavaText.CoreText(short.MinValue, ConfigType.Short));
        Assert.Equal("65535", JavaText.CoreText(ushort.MaxValue, ConfigType.Double));
        Assert.Equal("-2147483648", JavaText.CoreText(int.MinValue, ConfigType.Int));
        Assert.Equal("4294967295", JavaText.CoreText(uint.MaxValue, ConfigType.Double));
        Assert.Equal("-9223372036854775808", JavaText.CoreText(long.MinValue, ConfigType.Long));
        Assert.Equal("18446744073709551615", JavaText.CoreText(ulong.MaxValue, ConfigType.Double));
        Assert.Equal("1" + new string('0', 400), JavaText.CoreText(BigInteger.Pow(10, 400), ConfigType.Double));
        Assert.Equal("-9007199254740993", JavaText.CoreText(-(BigInteger.Pow(2, 53) + 1), ConfigType.Double));
#if NET7_0_OR_GREATER
        Assert.Equal("-170141183460469231731687303715884105728", JavaText.CoreText(Int128.MinValue, ConfigType.Double));
        Assert.Equal("340282366920938463463374607431768211455", JavaText.CoreText(UInt128.MaxValue, ConfigType.Double));
#endif
    }

    [Fact]
    public void CoreText_Decimal_IsItsExactDigits_NeverAnExponent()
    {
        Assert.Equal("0.1", JavaText.CoreText(0.1m, ConfigType.Double));
        Assert.Equal("-12.50", JavaText.CoreText(-12.50m, ConfigType.Double));
        Assert.Equal("0.0000001", JavaText.CoreText(0.0000001m, ConfigType.Double));
        Assert.Equal("0.0000000000000000000000000001", JavaText.CoreText(0.0000000000000000000000000001m, ConfigType.Double));
        Assert.Equal("2090639523243.3333527855107988", JavaText.CoreText(2090639523243.3333527855107988m, ConfigType.Double));
        Assert.Equal("79228162514264337593543950335", JavaText.CoreText(decimal.MaxValue, ConfigType.Double));
        Assert.Equal("0.000", JavaText.CoreText(new decimal(0, 0, 0, isNegative: true, scale: 3), ConfigType.Double));
    }

    [Fact]
    public void CoreText_Decimal_IsTheInvariantToString()
    {
        foreach (decimal value in new[] { 0.1m, -12.50m, 0.0000001m, decimal.MinValue, 1m, -0.000000000000000000000000001m })
        {
            Assert.Equal(value.ToString(CultureInfo.InvariantCulture), JavaText.CoreText(value, ConfigType.Double));
        }
    }

    [Fact]
    public void CoreText_Double_IsG17()
    {
        Assert.Equal("0.10000000000000001", JavaText.CoreText(0.1, ConfigType.Double));
        Assert.Equal("1E+21", JavaText.CoreText(1e21, ConfigType.Double));
        Assert.Equal("42.5", JavaText.CoreText(42.5, ConfigType.Double));
        Assert.Equal("-1", JavaText.CoreText(-1.0, ConfigType.Double));
        Assert.Equal("0", JavaText.CoreText(0.0, ConfigType.Double));
        Assert.Equal("1.7976931348623157E+308", JavaText.CoreText(double.MaxValue, ConfigType.Double));
        Assert.Equal("4.9406564584124654E-324", JavaText.CoreText(double.Epsilon, ConfigType.Double));
        Assert.Equal("1.0000000000000001E-05", JavaText.CoreText(1e-5, ConfigType.Double));
    }

    [Fact]
    public void CoreText_SpecialValuesAndNegativeZero_AreLiterals()
    {
        Assert.Equal("NaN", JavaText.CoreText(double.NaN, ConfigType.Double));
        Assert.Equal("Infinity", JavaText.CoreText(double.PositiveInfinity, ConfigType.Double));
        Assert.Equal("-Infinity", JavaText.CoreText(double.NegativeInfinity, ConfigType.Double));
        Assert.Equal("-0", JavaText.CoreText(-0.0, ConfigType.Double));
        Assert.Equal("NaN", JavaText.CoreText(float.NaN, ConfigType.Double));
        Assert.Equal("Infinity", JavaText.CoreText(float.PositiveInfinity, ConfigType.Double));
        Assert.Equal("-Infinity", JavaText.CoreText(float.NegativeInfinity, ConfigType.Double));
        Assert.Equal("-0", JavaText.CoreText(-0.0f, ConfigType.Double));
    }

    [Fact]
    public void CoreText_Float_IsWidenedToDoubleFirst()
    {
        Assert.Equal("0.10000000149011612", JavaText.CoreText(0.1f, ConfigType.Double));
        Assert.Equal("1.4012984643248171E-45", JavaText.CoreText(float.Epsilon, ConfigType.Double));
        Assert.Equal("3.4028234663852886E+38", JavaText.CoreText(float.MaxValue, ConfigType.Double));
    }

#if NET5_0_OR_GREATER
    [Fact]
    public void CoreText_Half_IsWidenedToDoubleFirst()
    {
        Assert.Equal("5.9604644775390625E-08", JavaText.CoreText(Half.Epsilon, ConfigType.Double));
        Assert.Equal("0.5", JavaText.CoreText((Half)0.5, ConfigType.Double));
        Assert.Equal("65504", JavaText.CoreText(Half.MaxValue, ConfigType.Double));
        Assert.Equal("NaN", JavaText.CoreText(Half.NaN, ConfigType.Double));
        Assert.Equal("-Infinity", JavaText.CoreText(Half.NegativeInfinity, ConfigType.Double));
        Assert.Equal("-0", JavaText.CoreText(Half.NegativeZero, ConfigType.Double));
    }
#endif

    [Fact]
    public void CoreText_EveryNumberText_IsOneTheCoreDoubleParserAccepts()
    {
        Assert.False(IsJavaDoubleText("−1"));
        Assert.False(IsJavaDoubleText("0,5"));
        Assert.False(IsJavaDoubleText("∞"));

        var values = new List<object>
        {
            (sbyte)-5, byte.MaxValue, short.MinValue, ushort.MaxValue, int.MinValue, uint.MaxValue, long.MinValue,
            ulong.MaxValue, BigInteger.Pow(10, 400), -BigInteger.Pow(10, 400), 0.1m, -12.50m, decimal.MinValue,
            0.0000000000000000000000000001m, 0.1, 1e21, -1e-300, double.MaxValue, double.Epsilon, -0.0, 0.0,
            double.NaN, double.PositiveInfinity, double.NegativeInfinity, 0.1f, float.Epsilon, float.MaxValue,
            -0.0f, float.NaN,
        };
#if NET5_0_OR_GREATER
        values.Add(Half.Epsilon);
        values.Add(Half.NegativeZero);
        values.Add(Half.MinValue);
#endif
#if NET7_0_OR_GREATER
        values.Add(Int128.MinValue);
        values.Add(UInt128.MaxValue);
#endif
        var random = new Random(20261105);
        byte[] bytes = new byte[8];
        for (int i = 0; i < 2000; i++)
        {
            random.NextBytes(bytes);
            values.Add(BitConverter.ToDouble(bytes, 0));
            values.Add(BitConverter.ToSingle(bytes, 0));
        }

        foreach (object value in values)
        {
            string text = JavaText.CoreText(value, ConfigType.Double);
            Assert.True(IsJavaDoubleText(text), $"'{text}' for {value.GetType()} is not a text the core's double parser accepts");
        }
    }

    [Fact]
    public void CoreText_ListKey_JoinsTheElementsByTheSameRules()
    {
        Assert.Equal(
            "a,1,0.5,true,null,-0,[x, y]",
            JavaText.CoreText(new List<object?> { "a", 1, 0.5, true, null, -0.0, new[] { "x", "y" } }, ConfigType.List));
        Assert.Equal("", JavaText.CoreText(new List<string>(), ConfigType.List));
    }

    [Fact]
    public void CoreText_ListKey_AStringIsSentUnchanged() =>
        Assert.Equal(" a , b, c", JavaText.CoreText(" a , b, c", ConfigType.List));

    [Fact]
    public void TestConvertValueToStringBoolean_TheCoreText() =>
        Assert.Equal("true", JavaText.CoreText(true, ConfigType.Boolean));

    [Fact]
    public void TestConvertValueToStringShort_TheCoreText() =>
        Assert.Equal("32767", JavaText.CoreText(short.MaxValue, ConfigType.Short));

    [Fact]
    public void TestConvertValueToStringInt_TheCoreText() =>
        Assert.Equal("2147483647", JavaText.CoreText(int.MaxValue, ConfigType.Int));

    [Fact]
    public void TestConvertValueToStringLong_TheCoreText() =>
        Assert.Equal("9223372036854775807", JavaText.CoreText(long.MaxValue, ConfigType.Long));

    [Fact]
    public void TestConvertValueToStringString_TheCoreText() =>
        Assert.Equal("foobar", JavaText.CoreText("foobar", ConfigType.String));

    [Fact]
    public void TestConvertValueToStringPassword_TheRawStringIsSentUnchanged() =>
        Assert.Equal("foobar", JavaText.CoreText("foobar", ConfigType.Password));

    [Fact]
    public void TestConvertValueToStringList_TheCoreText() =>
        Assert.Equal("a,bc,d", JavaText.CoreText(new List<string> { "a", "bc", "d" }, ConfigType.List));

    [Fact]
    public void TestConvertValueToStringClass_TheCoreTextIsTheFullName() =>
        Assert.Equal(
            "Confluent.Kafka.UnitTests.Internal.JavaTextTests",
            JavaText.CoreText(typeof(JavaTextTests), ConfigType.Class));

    [Fact]
    public void TestConvertValueToStringNestedClass_TheCoreTextIsTheFullName() =>
        Assert.Equal(
            "Confluent.Kafka.UnitTests.Internal.JavaTextTests+Named",
            JavaText.CoreText(typeof(Named), ConfigType.Class));

    [Fact]
    public void TestConvertValueToStringNullType_AStringIsSentUnchanged() =>
        Assert.Equal("foobar", JavaText.CoreText("foobar", null));

    [Fact]
    public void CoreText_KeyNotInTheTable_CollectionsAndDictionariesAsJavaPrintsThem()
    {
        Assert.Equal("[1, a, null, true, -0]", JavaText.CoreText(new List<object?> { 1, "a", null, true, -0.0 }, null));
        Assert.Equal("[[1], {}]", JavaText.CoreText(new object[] { new[] { 1 }, new Dictionary<string, int>() }, null));
        Assert.Equal("{k=1, n=null}", JavaText.CoreText(new Dictionary<string, object?> { ["k"] = 1, ["n"] = null }, null));
    }

    [Fact]
    public void CoreText_KeyNotInTheTable_ScalarsByTheSameRules()
    {
        Assert.Equal("0.10000000000000001", JavaText.CoreText(0.1, null));
        Assert.Equal("false", JavaText.CoreText(false, null));
        Assert.Equal("-12.50", JavaText.CoreText(-12.50m, null));
        Assert.Equal("System.String", JavaText.CoreText(typeof(string), null));
        Assert.Equal("00:01:00", JavaText.CoreText(TimeSpan.FromMinutes(1), null));
        Assert.Equal("x", JavaText.CoreText('x', null));
        Assert.Equal("a value", JavaText.CoreText(new Named("a value"), null));
        Assert.Equal("-5", JavaText.CoreText(new IntPtr(-5), null));
    }

    [Fact]
    public void CoreText_AToStringThatReturnsNull_IsJavasNull()
    {
        var values = new object[] { new Named(null), new NullFormattable() };

        Assert.Equal("null", JavaText.CoreText(values[0], null));
        Assert.Equal("null", JavaText.CoreText(values[1], null));
        Assert.Equal("[null, null]", JavaText.CoreText(values, null));
        Assert.Equal("null,null", JavaText.CoreText(values, ConfigType.List));
        Assert.Equal("null", JavaText.ValueOf(values[1]));
        Assert.Equal("[null, null]", JavaText.ValueOf(values));
    }

    [Theory]
    [InlineData("de-DE")]
    [InlineData("sv-SE")]
    public void CoreText_IsCultureInvariant(string culture)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try
        {
            Assert.NotEqual("0.5|-1", (0.5).ToString(CultureInfo.CurrentCulture) + "|" + (-1).ToString(CultureInfo.CurrentCulture));

            Assert.Equal("0.5", JavaText.CoreText(0.5, ConfigType.Double));
            Assert.Equal("-1", JavaText.CoreText(-1, ConfigType.Int));
            Assert.Equal("-1", JavaText.CoreText(-1L, ConfigType.Long));
            Assert.Equal("-1.5", JavaText.CoreText(-1.5, ConfigType.Double));
            Assert.Equal("-12.50", JavaText.CoreText(-12.50m, ConfigType.Double));
            Assert.Equal("-1", JavaText.CoreText(new BigInteger(-1), ConfigType.Double));
            Assert.Equal("-1E+21", JavaText.CoreText(-1e21, ConfigType.Double));
            Assert.Equal("-1,0.5", JavaText.CoreText(new List<object> { -1, 0.5 }, ConfigType.List));
            Assert.Equal("[-1, 0.5]", JavaText.CoreText(new List<object> { -1, 0.5 }, null));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Theory]
    [InlineData("\u0131", "I")]
    [InlineData("\u017F", "S")]
    [InlineData("\u00DF", "SS")]
    [InlineData("\uFB00", "FF")]
    [InlineData("\uFB01", "FI")]
    [InlineData("\uFB02", "FL")]
    [InlineData("\uFB03", "FFI")]
    [InlineData("\uFB04", "FFL")]
    [InlineData("\uFB05", "ST")]
    [InlineData("\uFB06", "ST")]
    public void ToUpperRoot_MapsEachLetterWhoseJavaUpperCaseIsAscii(string letter, string upper)
    {
        Assert.Equal(upper, JavaText.ToUpperRoot(letter));
        Assert.Equal("A" + upper + "B", JavaText.ToUpperRoot("a" + letter + "b"));
    }

    [Theory]
    [InlineData('\u0131', "I")]
    [InlineData('\u017F', "S")]
    [InlineData('\u00DF', "SS")]
    [InlineData('\uFB00', "FF")]
    [InlineData('\uFB01', "FI")]
    [InlineData('\uFB02', "FL")]
    [InlineData('\uFB03', "FFI")]
    [InlineData('\uFB04', "FFL")]
    [InlineData('\uFB05', "ST")]
    [InlineData('\uFB06', "ST")]
    [InlineData('a', null)]
    public void AsciiUpperSpecial_IsJavasAsciiUpperCaseOfEachLetter(char letter, string? upper) =>
        Assert.Equal(upper, JavaText.AsciiUpperSpecial(letter));

    [Fact]
    public void AsciiUpperSpecial_ExpandsExactlyTheLettersTheFastPathLooksFor()
    {
        var expanded = new List<char>();
        for (int c = char.MinValue; c <= char.MaxValue; c++)
        {
            if (JavaText.AsciiUpperSpecial((char)c) is not null)
            {
                expanded.Add((char)c);
            }
        }

        Assert.Equal(10, expanded.Count);
        Assert.Equal(expanded, JavaText.s_asciiUpperSpecials.OrderBy(c => c));
    }

    [Fact]
    public void ToUpperRoot_MapsSeveralLettersInOneName() =>
        Assert.Equal("FFIISSST_X", JavaText.ToUpperRoot("\uFB03\u0131\u00DF\uFB06_x"));

    [Theory]
    [InlineData("")]
    [InlineData("sasl_ssl")]
    [InlineData("SASL_SSL")]
    [InlineData("caf\u00E9-\u03C9")]
    [InlineData("\u0149one")]
    [InlineData("\u1E96")]
    public void ToUpperRoot_WithoutTheLetters_IsTheInvariantUpperCase(string value) =>
        Assert.Equal(value.ToUpperInvariant(), JavaText.ToUpperRoot(value));

    [Fact]
    public void ToUpperRoot_LeavesSurrogatePairsWhole()
    {
        const string Emoji = "\uD83D\uDE00";
        const string DeseretSmallLongI = "\uD801\uDC28";

        Assert.Equal("SS" + Emoji + "I", JavaText.ToUpperRoot("\u00DF" + Emoji + "\u0131"));

        string upper = JavaText.ToUpperRoot("\u00DF" + DeseretSmallLongI);
        Assert.Equal(("SS" + DeseretSmallLongI).ToUpperInvariant(), upper);
        Assert.Equal(4, upper.Length);
        Assert.True(char.IsSurrogatePair(upper, 2));
    }

    private static void AssertDoubleToString(string javaText, double value)
    {
        string text = JavaText.ValueOf(value);
#if NETFRAMEWORK
        // .NET Framework's round-trip digits can be longer than Java's; the text then still reads back as the value.
        if (text != javaText)
        {
            double parsed = double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
            Assert.Equal(BitConverter.DoubleToInt64Bits(value), BitConverter.DoubleToInt64Bits(parsed));
            return;
        }
#endif
        Assert.Equal(javaText, text);
    }

    private static void AssertFloatToString(string javaText, float value)
    {
        string text = JavaText.ValueOf(value);
#if NETFRAMEWORK
        if (text != javaText)
        {
            Assert.Equal(value, float.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture));
            return;
        }
#endif
        Assert.Equal(javaText, text);
    }

    private static bool IsJavaDoubleText(string text)
    {
        string body = text.Length > 0 && (text[0] == '-' || text[0] == '+') ? text.Substring(1) : text;
        if (body == "NaN" || body == "Infinity")
        {
            return true;
        }

        return body.Length > 0
            && (IsAsciiDigit(body[0]) || body[0] == '.')
            && body.All(c => IsAsciiDigit(c) || c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-');
    }

    private static bool IsAsciiDigit(char c) => c >= '0' && c <= '9';

    /// <summary>An object whose <see cref="object.ToString"/> returns the text it was given.</summary>
    private sealed class Named
    {
        private readonly string? _text;

        internal Named(string? text) => _text = text;

        public override string? ToString() => _text;
    }

    /// <summary>A formattable value whose <c>ToString(format, provider)</c> returns <see langword="null"/>.</summary>
    private sealed class NullFormattable : IFormattable
    {
        public string ToString(string? format, IFormatProvider? formatProvider) => null!;

        public override string ToString() => "not the formattable text";
    }

    /// <summary>A sequence that fails when enumerated, whose <see cref="object.ToString"/> returns the text it was given.</summary>
    private sealed class Unenumerable : IEnumerable<int>
    {
        private readonly string _text;

        internal Unenumerable(string text) => _text = text;

        public IEnumerator<int> GetEnumerator() => throw new InvalidOperationException("enumerated");

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public override string ToString() => _text;
    }

    /// <summary>A collection that is only an <see cref="IReadOnlyCollection{T}"/>.</summary>
    private sealed class ReadOnlyBag : IReadOnlyCollection<int>
    {
        private readonly int[] _items;

        internal ReadOnlyBag(params int[] items) => _items = items;

        public int Count => _items.Length;

        public IEnumerator<int> GetEnumerator() => ((IEnumerable<int>)_items).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>A collection that is only an <see cref="ICollection{T}"/>.</summary>
    private sealed class MutableBag : ICollection<int>
    {
        private readonly List<int> _items;

        internal MutableBag(params int[] items) => _items = new List<int>(items);

        public int Count => _items.Count;

        public bool IsReadOnly => false;

        public void Add(int item) => _items.Add(item);

        public void Clear() => _items.Clear();

        public bool Contains(int item) => _items.Contains(item);

        public void CopyTo(int[] array, int arrayIndex) => _items.CopyTo(array, arrayIndex);

        public bool Remove(int item) => _items.Remove(item);

        public IEnumerator<int> GetEnumerator() => _items.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>A dictionary that is only an <see cref="IReadOnlyDictionary{TKey, TValue}"/>.</summary>
    private sealed class ReadOnlyMap : IReadOnlyDictionary<string, object?>
    {
        private readonly Dictionary<string, object?> _entries;

        internal ReadOnlyMap(Dictionary<string, object?> entries) => _entries = entries;

        public int Count => _entries.Count;

        public IEnumerable<string> Keys => _entries.Keys;

        public IEnumerable<object?> Values => _entries.Values;

        public object? this[string key] => _entries[key];

        public bool ContainsKey(string key) => _entries.ContainsKey(key);

        public bool TryGetValue(string key, out object? value) => _entries.TryGetValue(key, out value);

        public IEnumerator<KeyValuePair<string, object?>> GetEnumerator() => _entries.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
