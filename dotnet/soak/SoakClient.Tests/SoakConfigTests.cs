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
using System.IO;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// The startup configuration routing, validation and JAAS parsing, which refuse an unlisted key
/// (the core would accept it with one INFO line and use the default); the messages are asserted.
/// </summary>
public sealed class SoakConfigTests
{
    private static Dictionary<string, string> Conf(params (string Key, string Value)[] entries)
    {
        var conf = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach ((string key, string value) in entries)
        {
            conf[key] = value;
        }

        return conf;
    }

    [Fact]
    public void ValidateAcceptsKnownKeys()
    {
        var conf = Conf(
            ("bootstrap.servers", "localhost:9092"),
            ("linger.ms", "5"),
            ("compression.type", "lz4"),
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "PLAIN"),
            ("sasl.jaas.config", "org.apache...PlainLoginModule required;"));

        SoakConfig.ValidateConfig(conf, SoakConfig.ProducerConfigKeys, "producer");
    }

    [Fact]
    public void ValidateAcceptsSslPrefix()
    {
        SoakConfig.ValidateConfig(Conf(("ssl.truststore.location", "/x")), SoakConfig.ProducerConfigKeys, "producer");
        SoakConfig.ValidateConfig(Conf(("ssl.keystore.password", "x")), SoakConfig.ConsumerConfigKeys, "consumer");
    }

    [Fact]
    public void ValidateRejectsUnknownKeyAndNamesIt()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => SoakConfig.ValidateConfig(Conf(("sasl.username", "u")), SoakConfig.ProducerConfigKeys, "producer"));

        Assert.Contains("sasl.username", ex.Message, StringComparison.Ordinal);
        Assert.Contains("producer", ex.Message, StringComparison.Ordinal);
        Assert.Contains("only logs a warning for unrecognised keys", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateRejectsEveryUnknownKey()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => SoakConfig.ValidateConfig(
            Conf(("bootstrap.servers", "x"), ("sasl.password", "p"), ("lingerr.ms", "5")),
            SoakConfig.ProducerConfigKeys,
            "producer"));

        Assert.Contains("sasl.password", ex.Message, StringComparison.Ordinal);
        Assert.Contains("lingerr.ms", ex.Message, StringComparison.Ordinal);

        string rejected = ex.Message.Split("Accepted", StringSplitOptions.None)[0];
        Assert.DoesNotContain("bootstrap.servers", rejected, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateRejectsConsumerTypo()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => SoakConfig.ValidateConfig(Conf(("group.protocoll", "consumer")), SoakConfig.ConsumerConfigKeys, "consumer"));
        Assert.Contains("group.protocoll", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateAcceptsAKeyTheKafka44CoreAdded()
    {
        SoakConfig.ValidateConfig(Conf(("partitioner.rack.aware", "true")), SoakConfig.ProducerConfigKeys, "producer");
        SoakConfig.ValidateConfig(Conf(("metadata.cluster.check.enable", "true")), SoakConfig.ConsumerConfigKeys, "consumer");
    }

    [Fact]
    public void ValidateRejectsAConsumerKeyTheCoreDoesNotDefine()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => SoakConfig.ValidateConfig(Conf(("interceptor.classes", "x")), SoakConfig.ConsumerConfigKeys, "consumer"));

        Assert.StartsWith("unknown consumer configuration key(s): interceptor.classes. ", ex.Message, StringComparison.Ordinal);
        Assert.Contains("only logs a warning for unrecognised keys", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void GroupProtocolIsAnAcceptedConsumerKey()
    {
        Assert.Contains("group.protocol", SoakConfig.ConsumerConfigKeys);
    }

    [Fact]
    public void RouteSharedConfigMovesTheOtherClientsKeys()
    {
        var conf = Conf(("bootstrap.servers", "x"), ("group.id", "g"), ("linger.ms", "5"));

        (Dictionary<string, string> kept, IReadOnlyList<string> routed) =
            SoakConfig.RouteSharedConfig(conf, SoakConfig.ProducerConfigKeys, SoakConfig.ConsumerConfigKeys);

        Assert.Equal(new[] { "group.id" }, routed);
        Assert.Equal(2, kept.Count);
        Assert.Equal("x", kept["bootstrap.servers"]);
        Assert.Equal("5", kept["linger.ms"]);

        SoakConfig.ValidateConfig(kept, SoakConfig.ProducerConfigKeys, "producer");
    }

    [Fact]
    public void RouteSharedConfigMovesInTheOtherDirectionToo()
    {
        var conf = Conf(("bootstrap.servers", "x"), ("group.id", "g"), ("linger.ms", "5"));

        (Dictionary<string, string> kept, IReadOnlyList<string> routed) =
            SoakConfig.RouteSharedConfig(conf, SoakConfig.ConsumerConfigKeys, SoakConfig.ProducerConfigKeys);

        Assert.Equal(new[] { "linger.ms" }, routed);
        Assert.True(kept.ContainsKey("group.id"));
        SoakConfig.ValidateConfig(kept, SoakConfig.ConsumerConfigKeys, "consumer");
    }

    [Fact]
    public void SharedConnectionKeysStayWithTheProducer()
    {
        var conf = Conf(
            ("metadata.recovery.strategy", "none"),
            ("metadata.recovery.rebootstrap.trigger.ms", "60000"),
            ("socket.connection.setup.timeout.ms", "1000"),
            ("socket.connection.setup.timeout.max.ms", "5000"));

        (Dictionary<string, string> kept, IReadOnlyList<string> routed) =
            SoakConfig.RouteSharedConfig(conf, SoakConfig.ProducerConfigKeys, SoakConfig.ConsumerConfigKeys);

        Assert.Empty(routed);
        Assert.Equal(conf.Count, kept.Count);
        foreach (KeyValuePair<string, string> entry in conf)
        {
            Assert.Equal(entry.Value, kept[entry.Key]);
        }

        SoakConfig.ValidateConfig(kept, SoakConfig.ProducerConfigKeys, "producer");
    }

    [Theory]
    [InlineData("config.providers")]
    [InlineData("enable.metrics.push")]
    [InlineData("partition.assignment.strategy")]
    [InlineData("security.providers")]
    public void ValidateRejectsKeysTheConsumerNoLongerDefines(string key)
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => SoakConfig.ValidateConfig(Conf((key, "x")), SoakConfig.ConsumerConfigKeys, "consumer"));

        Assert.StartsWith($"unknown consumer configuration key(s): {key}. ", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RouteSharedConfigKeepsKeysUnknownToBoth()
    {
        (Dictionary<string, string> kept, IReadOnlyList<string> routed) = SoakConfig.RouteSharedConfig(
            Conf(("nonsense.key", "1")), SoakConfig.ProducerConfigKeys, SoakConfig.ConsumerConfigKeys);

        Assert.Empty(routed);
        Assert.Equal("1", kept["nonsense.key"]);
        Assert.Throws<ArgumentException>(
            () => SoakConfig.ValidateConfig(kept, SoakConfig.ProducerConfigKeys, "producer"));
    }

    [Fact]
    public void FilterConfigStripsPrefixAndDropsOthers()
    {
        var conf = Conf(
            ("bootstrap.servers", "x"),
            ("producer.linger.ms", "5"),
            ("consumer.group.id", "g"),
            ("admin.client.id", "a"));

        Dictionary<string, string> pconf = SoakConfig.FilterConfig(conf, new[] { "consumer.", "admin." }, "producer.");
        Assert.Equal(2, pconf.Count);
        Assert.Equal("x", pconf["bootstrap.servers"]);
        Assert.Equal("5", pconf["linger.ms"]);

        Dictionary<string, string> cconf = SoakConfig.FilterConfig(conf, new[] { "producer.", "admin." }, "consumer.");
        Assert.Equal(2, cconf.Count);
        Assert.Equal("x", cconf["bootstrap.servers"]);
        Assert.Equal("g", cconf["group.id"]);

        Dictionary<string, string> aconf = SoakConfig.FilterConfig(conf, new[] { "consumer.", "producer." }, "admin.");
        Assert.Equal(2, aconf.Count);
        Assert.Equal("a", aconf["client.id"]);
    }

    [Fact]
    public void ParseConfigFileSkipsCommentsAndBlanks()
    {
        const string Text =
            "# a comment\n"
            + "\n"
            + "bootstrap.servers=host:9092\n"
            + "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p=q\";\n";

        using var reader = new StringReader(Text);
        Dictionary<string, string> conf = SoakConfig.ParseConfigFile(reader);

        Assert.Equal("host:9092", conf["bootstrap.servers"]);

        Assert.EndsWith("password=\"p=q\";", conf["sasl.jaas.config"], StringComparison.Ordinal);
    }

    [Fact]
    public void ParseConfigFileRejectsALineWithoutASeparator()
    {
        using var reader = new StringReader("bootstrap.servers\n");
        ArgumentException ex = Assert.Throws<ArgumentException>(() => SoakConfig.ParseConfigFile(reader));
        Assert.Contains("Configuration lines must be `name=value..`", ex.Message, StringComparison.Ordinal);
        Assert.Contains("bootstrap.servers", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseConfigFileRejectsALineStartingWithTheSeparator()
    {
        using var reader = new StringReader("=value\n");
        Assert.Throws<ArgumentException>(() => SoakConfig.ParseConfigFile(reader));
    }

    [Fact]
    public void JaasCredentialsExtraction()
    {
        const string Jaas = "org.apache.kafka.common.security.plain.PlainLoginModule required \n\t"
            + "username=\"API_KEY\" \n\tpassword=\"API_SECRET\";";

        (string? username, string? password) = SoakConfig.JaasCredentials(Jaas);
        Assert.Equal("API_KEY", username);
        Assert.Equal("API_SECRET", password);
    }

    [Fact]
    public void JaasCredentialsAcceptsSingleQuotesAndBareValues()
    {
        Assert.Equal("k", SoakConfig.JaasField("required username='k' password=p;", "username"));
        Assert.Equal("p", SoakConfig.JaasField("required username='k' password=p;", "password"));
        Assert.Equal("k", SoakConfig.JaasField("required username = \"k\";", "username"));
    }

    [Fact]
    public void JaasCredentialsDoesNotMatchALongerFieldName()
    {
        string jaas = "required serviceName=\"wrong\" username=\"right\";";
        Assert.Equal("right", SoakConfig.JaasField(jaas, "username"));
    }

    [Fact]
    public void JaasCredentialsDoesNotMatchAKeyInsideAQuotedValue()
    {
        const string Jaas = "PlainLoginModule required password=\"username=x\" username=\"right\";";
        (string? username, string? password) = SoakConfig.JaasCredentials(Jaas);
        Assert.Equal("right", username);
        Assert.Equal("username=x", password);
    }

    [Fact]
    public void JaasCredentialsHandlesEscapedQuoteInValue()
    {
        const string Jaas = "PlainLoginModule required password=\"a\\\"b\" username=\"right\";";
        (string? username, string? password) = SoakConfig.JaasCredentials(Jaas);
        Assert.Equal("right", username);
        Assert.Equal("a\\\"b", password);
    }

    [Fact]
    public void JaasCredentialsValueCutByARawLineBreakIsAbsent()
    {
        (string? username, string? password) =
            SoakConfig.JaasCredentials("PlainLoginModule required username=\"u\" password=\"ab\ncd\";");
        Assert.Equal("u", username);
        Assert.Null(password);

        (username, password) =
            SoakConfig.JaasCredentials("PlainLoginModule required username=\"u\" password=\"ab\rcd\";");
        Assert.Equal("u", username);
        Assert.Null(password);

        (username, password) =
            SoakConfig.JaasCredentials("PlainLoginModule required username=\"u\"\n password=\"abcd\";");
        Assert.Equal("u", username);
        Assert.Equal("abcd", password);
    }

    [Fact]
    public void JaasCredentialsAbsent()
    {
        (string? username, string? password) = SoakConfig.JaasCredentials("org.apache...PlainLoginModule required;");
        Assert.Null(username);
        Assert.Null(password);
    }

    [Fact]
    public void JaasCredentialsPreservesSpecialCharactersInTheSecret()
    {
        const string Jaas = "required username=\"K/EY+1\" password=\"a+b/c=d==\";";
        (string? username, string? password) = SoakConfig.JaasCredentials(Jaas);
        Assert.Equal("K/EY+1", username);
        Assert.Equal("a+b/c=d==", password);
    }

    [Fact]
    public void CheckAdminCredentialsRefusesSaslWithoutCredentials()
    {
        SoakFatalStartupException ex = Assert.Throws<SoakFatalStartupException>(() => SoakConfig.CheckAdminCredentials(
            Conf(
                ("bootstrap.servers", "host:9092"),
                ("security.protocol", "SASL_SSL"),
                ("sasl.mechanism", "PLAIN"))));

        Assert.NotNull(ex.Message);
        Assert.Contains("sasl.jaas.config", ex.Message, StringComparison.Ordinal);
        Assert.Contains("username and password", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CheckAdminCredentialsRefusesAHalfParsedJaas()
    {
        SoakFatalStartupException ex = Assert.Throws<SoakFatalStartupException>(() => SoakConfig.CheckAdminCredentials(
            Conf(
                ("bootstrap.servers", "host:9092"),
                ("security.protocol", "SASL_SSL"),
                ("sasl.mechanism", "PLAIN"),
                ("sasl.jaas.config", "org.apache...PlainLoginModule required username=\"only\";"))));

        Assert.Contains("password", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CheckAdminCredentialsTriggersOnSaslMechanismAlone()
    {
        Assert.Throws<SoakFatalStartupException>(() => SoakConfig.CheckAdminCredentials(
            Conf(("bootstrap.servers", "host:9092"), ("sasl.mechanism", "PLAIN"))));
    }

    [Fact]
    public void CheckAdminCredentialsRefusesSaslMechanismWithPlaintextProtocol()
    {
        SoakFatalStartupException ex = Assert.Throws<SoakFatalStartupException>(() => SoakConfig.CheckAdminCredentials(
            Conf(
                ("bootstrap.servers", "host:9092"),
                ("security.protocol", "PLAINTEXT"),
                ("sasl.mechanism", "PLAIN"),
                ("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"u\" password=\"p\";"))));

        Assert.Contains("not a SASL protocol", ex.Message, StringComparison.Ordinal);
        Assert.Contains("WITHOUT SASL", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CheckAdminCredentialsRefusesSaslMechanismWithoutProtocol()
    {
        SoakFatalStartupException ex = Assert.Throws<SoakFatalStartupException>(() => SoakConfig.CheckAdminCredentials(
            Conf(("bootstrap.servers", "host:9092"), ("sasl.mechanism", "PLAIN"))));

        Assert.Contains("not a SASL protocol", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CheckAdminCredentialsRefusesJaasCredsWithPlaintextProtocol()
    {
        SoakFatalStartupException ex = Assert.Throws<SoakFatalStartupException>(() => SoakConfig.CheckAdminCredentials(
            Conf(
                ("bootstrap.servers", "host:9092"),
                ("security.protocol", "PLAINTEXT"),
                ("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"u\" password=\"p\";"))));

        Assert.Contains("not a SASL protocol", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CheckAdminCredentialsAllowsPlaintextWithoutCredentials()
    {
        SoakConfig.CheckAdminCredentials(Conf(("bootstrap.servers", "host:9092")));
        SoakConfig.CheckAdminCredentials(Conf(
            ("bootstrap.servers", "host:9092"), ("security.protocol", "PLAINTEXT")));
    }

    [Fact]
    public void CheckAdminCredentialsAllowsSaslWithFullCredentials()
    {
        SoakConfig.CheckAdminCredentials(Conf(
            ("bootstrap.servers", "host:9092"),
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "PLAIN"),
            ("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                + "username=\"u\" password=\"p\";")));
    }
}
