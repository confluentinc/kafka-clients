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
using System.Globalization;
using System.Reflection;

using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Checks <see cref="SecurityProtocol"/>, <see cref="SslClientAuth"/> and <see cref="GroupProtocol"/> and their
/// static classes against Java's enums: the constants, their order and ids, and the lookups by id and by name,
/// which upper-case the name as Java's <c>toUpperCase(Locale.ROOT)</c> does where its upper case is ASCII and refuse
/// what Java's <c>valueOf</c> refuses, with Java's message (any other special-cased letter keeps its invariant upper
/// case there).
/// </summary>
public sealed class PublicConfigEnumsTests
{
    private const string SecurityProtocolClass = "org.apache.kafka.common.security.auth.SecurityProtocol";
    private const string GroupProtocolClass = "org.apache.kafka.clients.consumer.GroupProtocol";

    [Fact]
    public void ConfigClasses_test_security_protocol_TheConstantsAreJavasInOrderValuedByTheirIds()
    {
        Assert.Equal(typeof(short), Enum.GetUnderlyingType(typeof(SecurityProtocol)));
        Assert.Equal(
            new[] { "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL" },
            Enum.GetNames(typeof(SecurityProtocol)));
        Assert.Equal((short)0, (short)SecurityProtocol.PLAINTEXT);
        Assert.Equal((short)1, (short)SecurityProtocol.SSL);
        Assert.Equal((short)2, (short)SecurityProtocol.SASL_PLAINTEXT);
        Assert.Equal((short)3, (short)SecurityProtocol.SASL_SSL);
    }

    [Theory]
    [InlineData(SecurityProtocol.PLAINTEXT, "PLAINTEXT")]
    [InlineData(SecurityProtocol.SSL, "SSL")]
    [InlineData(SecurityProtocol.SASL_PLAINTEXT, "SASL_PLAINTEXT")]
    [InlineData(SecurityProtocol.SASL_SSL, "SASL_SSL")]
    public void ConfigClasses_test_security_protocol_ToStringIsJavasName(SecurityProtocol protocol, string name) =>
        Assert.Equal(name, protocol.ToString());

    [Fact]
    public void ConfigClasses_test_security_protocol_NamesAreJavasInOrder_AndReadOnly()
    {
        IReadOnlyList<string> names = SecurityProtocols.Names();

        Assert.Equal(new[] { "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL" }, names);
        Assert.Same(names, SecurityProtocols.Names());
        AssertReadOnly(names, "TLS");
    }

    [Theory]
    [InlineData((short)0, SecurityProtocol.PLAINTEXT)]
    [InlineData((short)1, SecurityProtocol.SSL)]
    [InlineData((short)2, SecurityProtocol.SASL_PLAINTEXT)]
    [InlineData((short)3, SecurityProtocol.SASL_SSL)]
    public void ConfigClasses_test_security_protocol_ForIdFindsTheProtocol(short id, SecurityProtocol expected) =>
        Assert.Equal(expected, SecurityProtocols.ForId(id));

    [Theory]
    [InlineData((short)9)]
    [InlineData((short)4)]
    [InlineData((short)-1)]
    [InlineData(short.MaxValue)]
    public void ConfigClasses_test_security_protocol_ForIdIsNullForAnUnknownId(short id) =>
        Assert.Null(SecurityProtocols.ForId(id));

    [Theory]
    [InlineData("sasl_plaintext", SecurityProtocol.SASL_PLAINTEXT)]
    [InlineData("PLAINTEXT", SecurityProtocol.PLAINTEXT)]
    [InlineData("Ssl", SecurityProtocol.SSL)]
    [InlineData("sasl_SSL", SecurityProtocol.SASL_SSL)]
    public void ConfigClasses_test_security_protocol_ForNameIgnoresCase(string name, SecurityProtocol expected) =>
        Assert.Equal(expected, SecurityProtocols.ForName(name));

    [Theory]
    [InlineData("tls", "TLS")]
    [InlineData("1", "1")]
    [InlineData("0", "0")]
    [InlineData("SSL,PLAINTEXT", "SSL,PLAINTEXT")]
    [InlineData("ssl, plaintext", "SSL, PLAINTEXT")]
    [InlineData(" SSL", " SSL")]
    [InlineData("SSL ", "SSL ")]
    [InlineData("", "")]
    public void ConfigClasses_test_security_protocol_ForNameRefusesAnUnknownNameWithJavasMessage(
        string name, string upper)
    {
        var e = Assert.Throws<IllegalArgumentException>(() => SecurityProtocols.ForName(name));

        Assert.Equal("No enum constant " + SecurityProtocolClass + "." + upper, e.Message);
        Assert.Null(e.ParamName);
    }

    [Theory]
    [InlineData("sasl_pla\u0131ntext", SecurityProtocol.SASL_PLAINTEXT)]
    [InlineData("SASL_\u00DFL", SecurityProtocol.SASL_SSL)]
    [InlineData("sa\u017Fl_ssl", SecurityProtocol.SASL_SSL)]
    public void ConfigClasses_test_security_protocol_ForNameUpperCasesTheLettersJavaMapsToAscii(
        string name, SecurityProtocol expected) =>
        Assert.Equal(expected, SecurityProtocols.ForName(name));

    [Theory]
    [InlineData("\uFB00x", "FFX")]
    [InlineData("\uFB01x", "FIX")]
    [InlineData("\uFB02x", "FLX")]
    [InlineData("\uFB03x", "FFIX")]
    [InlineData("\uFB04x", "FFLX")]
    public void ConfigClasses_test_security_protocol_ForNameRefusesALigatureWithJavasUpperCaseInTheMessage(
        string name, string upper)
    {
        var e = Assert.Throws<IllegalArgumentException>(() => SecurityProtocols.ForName(name));

        Assert.Equal("No enum constant " + SecurityProtocolClass + "." + upper, e.Message);
    }

    [Fact]
    public void ConfigClasses_test_security_protocol_ForNameRefusesAnotherSpecialCasedLetter_WithItsInvariantUpperCase()
    {
        var e = Assert.Throws<IllegalArgumentException>(() => SecurityProtocols.ForName("\u0149one"));

        Assert.Equal("No enum constant " + SecurityProtocolClass + "." + "\u0149one".ToUpperInvariant(), e.Message);
    }

    [Fact]
    public void ConfigClasses_test_security_protocol_ForNameRefusesNull()
    {
        var e = Assert.Throws<ArgumentNullException>(() => SecurityProtocols.ForName(null!));

        Assert.Equal("name", e.ParamName);
        Assert.Equal(new ArgumentNullException("name").Message, e.Message);
    }

    [Fact]
    public void ConfigClasses_test_ssl_client_auth_TheConstantsAreJavasInOrder()
    {
        Assert.Equal(new[] { "REQUIRED", "REQUESTED", "NONE" }, Enum.GetNames(typeof(SslClientAuth)));
        Assert.Equal(
            new[] { SslClientAuth.REQUIRED, SslClientAuth.REQUESTED, SslClientAuth.NONE },
            (SslClientAuth[])Enum.GetValues(typeof(SslClientAuth)));
    }

    [Fact]
    public void ConfigClasses_test_ssl_client_auth_ValuesAreJavasInOrder_AndReadOnly()
    {
        IReadOnlyList<SslClientAuth> values = SslClientAuths.VALUES;

        Assert.Equal(new[] { SslClientAuth.REQUIRED, SslClientAuth.REQUESTED, SslClientAuth.NONE }, values);
        Assert.Equal(SslClientAuth.REQUIRED, values[0]);
        Assert.Same(values, SslClientAuths.VALUES);
        AssertReadOnly(values, SslClientAuth.NONE);
    }

    [Fact]
    public void ConfigClasses_test_ssl_client_auth_ValuesIsAStaticReadOnlyField_NotAProperty()
    {
        FieldInfo? field = typeof(SslClientAuths).GetField(
            nameof(SslClientAuths.VALUES), BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly);

        Assert.True(field is { IsInitOnly: true, IsLiteral: false }, "SslClientAuths.VALUES is not a static readonly field.");
        Assert.Equal(typeof(IReadOnlyList<SslClientAuth>), field!.FieldType);
        Assert.Empty(typeof(SslClientAuths).GetProperties(
            BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly));
    }

    [Theory]
    [InlineData(SslClientAuth.REQUIRED, "REQUIRED")]
    [InlineData(SslClientAuth.REQUESTED, "REQUESTED")]
    [InlineData(SslClientAuth.NONE, "NONE")]
    public void ConfigClasses_test_ssl_client_auth_ToStringIsTheConstantName(SslClientAuth auth, string name) =>
        Assert.Equal(name, auth.ToString());

    [Fact]
    public void ConfigClasses_test_ssl_client_auth_ForConfigOfNullIsNone() =>
        Assert.Equal(SslClientAuth.NONE, SslClientAuths.ForConfig(null));

    [Theory]
    [InlineData("required", SslClientAuth.REQUIRED)]
    [InlineData("ReQuEsTeD", SslClientAuth.REQUESTED)]
    [InlineData("none", SslClientAuth.NONE)]
    [InlineData("NONE", SslClientAuth.NONE)]
    public void ConfigClasses_test_ssl_client_auth_ForConfigIgnoresCase(string key, SslClientAuth expected) =>
        Assert.Equal(expected, SslClientAuths.ForConfig(key));

    [Theory]
    [InlineData("other")]
    [InlineData("0")]
    [InlineData("2")]
    [InlineData("REQUIRED,NONE")]
    [InlineData(" required")]
    [InlineData("")]
    public void ConfigClasses_test_ssl_client_auth_ForConfigIsNullForAnUnknownName(string key) =>
        Assert.Null(SslClientAuths.ForConfig(key));

    [Theory]
    [InlineData("reque\uFB06ed", SslClientAuth.REQUESTED)]
    [InlineData("reque\uFB05ed", SslClientAuth.REQUESTED)]
    [InlineData("requ\u0131red", SslClientAuth.REQUIRED)]
    [InlineData("reque\u017Fted", SslClientAuth.REQUESTED)]
    public void ConfigClasses_test_ssl_client_auth_ForConfigUpperCasesTheLettersJavaMapsToAscii(
        string key, SslClientAuth expected) =>
        Assert.Equal(expected, SslClientAuths.ForConfig(key));

    [Theory]
    [InlineData("\uFB00x")]
    [InlineData("\uFB01x")]
    [InlineData("\uFB02x")]
    [InlineData("\uFB03x")]
    [InlineData("\uFB04x")]
    [InlineData("\u0149one")]
    public void ConfigClasses_test_ssl_client_auth_ForConfigIsNullForALigatureOrAnotherSpecialCasedLetter(string key) =>
        Assert.Null(SslClientAuths.ForConfig(key));

    [Fact]
    public void ConfigClasses_test_group_protocol_TheConstantsAreJavasInOrder()
    {
        Assert.Equal(new[] { "CLASSIC", "CONSUMER" }, Enum.GetNames(typeof(GroupProtocol)));
        Assert.Equal(
            new[] { GroupProtocol.CLASSIC, GroupProtocol.CONSUMER },
            (GroupProtocol[])Enum.GetValues(typeof(GroupProtocol)));
    }

    [Theory]
    [InlineData(GroupProtocol.CLASSIC, "CLASSIC")]
    [InlineData(GroupProtocol.CONSUMER, "CONSUMER")]
    public void ConfigClasses_test_group_protocol_ToStringIsJavasName(GroupProtocol protocol, string name) =>
        Assert.Equal(name, protocol.ToString());

    [Theory]
    [InlineData("consumer", GroupProtocol.CONSUMER)]
    [InlineData("Classic", GroupProtocol.CLASSIC)]
    [InlineData("CLASSIC", GroupProtocol.CLASSIC)]
    public void ConfigClasses_test_group_protocol_OfIgnoresCase(string name, GroupProtocol expected) =>
        Assert.Equal(expected, GroupProtocols.Of(name));

    [Theory]
    [InlineData("share", "SHARE")]
    [InlineData("1", "1")]
    [InlineData("0", "0")]
    [InlineData("CLASSIC,CONSUMER", "CLASSIC,CONSUMER")]
    [InlineData(" CLASSIC", " CLASSIC")]
    [InlineData("", "")]
    public void ConfigClasses_test_group_protocol_OfRefusesAnUnknownNameWithJavasMessage(
        string name, string upper)
    {
        var e = Assert.Throws<IllegalArgumentException>(() => GroupProtocols.Of(name));

        Assert.Equal("No enum constant " + GroupProtocolClass + "." + upper, e.Message);
        Assert.Null(e.ParamName);
    }

    [Theory]
    [InlineData("cla\u00DFic")]
    [InlineData("class\u0131c")]
    [InlineData("cla\u017F\u017Fic")]
    public void ConfigClasses_test_group_protocol_OfUpperCasesTheLettersJavaMapsToAscii(string name) =>
        Assert.Equal(GroupProtocol.CLASSIC, GroupProtocols.Of(name));

    [Theory]
    [InlineData("\uFB00x", "FFX")]
    [InlineData("\uFB01x", "FIX")]
    [InlineData("\uFB02x", "FLX")]
    [InlineData("\uFB03x", "FFIX")]
    [InlineData("\uFB04x", "FFLX")]
    public void ConfigClasses_test_group_protocol_OfRefusesALigatureWithJavasUpperCaseInTheMessage(
        string name, string upper)
    {
        var e = Assert.Throws<IllegalArgumentException>(() => GroupProtocols.Of(name));

        Assert.Equal("No enum constant " + GroupProtocolClass + "." + upper, e.Message);
    }

    [Fact]
    public void ConfigClasses_test_group_protocol_OfRefusesAnotherSpecialCasedLetter_WithItsInvariantUpperCase()
    {
        var e = Assert.Throws<IllegalArgumentException>(() => GroupProtocols.Of("\u0149one"));

        Assert.Equal("No enum constant " + GroupProtocolClass + "." + "\u0149one".ToUpperInvariant(), e.Message);
    }

    [Fact]
    public void ConfigClasses_test_group_protocol_OfRefusesNull()
    {
        var e = Assert.Throws<ArgumentNullException>(() => GroupProtocols.Of(null!));

        Assert.Equal("name", e.ParamName);
        Assert.Equal(new ArgumentNullException("name").Message, e.Message);
    }

    [Theory]
    [InlineData("tr-TR")]
    [InlineData("az-Latn-AZ")]
    public void ForNameOfAndForConfig_UpperCaseTheNameInTheInvariantCulture(string culture)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try
        {
            Assert.NotEqual("I", "i".ToUpper(CultureInfo.CurrentCulture));

            Assert.Equal(SecurityProtocol.SASL_PLAINTEXT, SecurityProtocols.ForName("sasl_plaintext"));
            Assert.Equal(GroupProtocol.CLASSIC, GroupProtocols.Of("classic"));
            Assert.Equal(SslClientAuth.REQUIRED, SslClientAuths.ForConfig("required"));
            Assert.Equal(SslClientAuth.REQUESTED, SslClientAuths.ForConfig("requested"));

            var protocol = Assert.Throws<IllegalArgumentException>(() => SecurityProtocols.ForName("plaintext_tls"));
            Assert.Equal("No enum constant " + SecurityProtocolClass + ".PLAINTEXT_TLS", protocol.Message);
            var group = Assert.Throws<IllegalArgumentException>(() => GroupProtocols.Of("streaming"));
            Assert.Equal("No enum constant " + GroupProtocolClass + ".STREAMING", group.Message);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    private static void AssertReadOnly<T>(IReadOnlyList<T> collection, T element)
    {
        T first = collection[0];

        IList<T> typed = Assert.IsAssignableFrom<IList<T>>(collection);
        Assert.True(typed.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => typed[0] = element);
        Assert.Throws<NotSupportedException>(() => typed.Add(element));
        Assert.Throws<NotSupportedException>(() => typed.Insert(0, element));
        Assert.Throws<NotSupportedException>(() => typed.Remove(first));
        Assert.Throws<NotSupportedException>(() => typed.RemoveAt(0));
        Assert.Throws<NotSupportedException>(() => typed.Clear());

        IList untyped = Assert.IsAssignableFrom<IList>(collection);
        Assert.True(untyped.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => untyped[0] = element);
        Assert.Throws<NotSupportedException>(() => untyped.Add(element));
        Assert.Throws<NotSupportedException>(() => untyped.Insert(0, element));
        Assert.Throws<NotSupportedException>(() => untyped.Remove(first));
        Assert.Throws<NotSupportedException>(() => untyped.RemoveAt(0));
        Assert.Throws<NotSupportedException>(() => untyped.Clear());

        Assert.Equal(first, collection[0]);
        Assert.IsType<ReadOnlyCollection<T>>(collection);
    }
}
