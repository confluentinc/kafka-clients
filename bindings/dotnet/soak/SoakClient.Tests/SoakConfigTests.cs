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
/// The startup configuration handling that stands in for the Rust client's silent
/// acceptance of unknown keys. Error MESSAGES are asserted, not merely the throw:
/// <c>definition-of-done.md §3</c> — the message names the offending keys and the
/// accepted set, and that is what an operator acts on.
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
        // Both Rust configs route every `ssl.*` key to apply_ssl_config_key().
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

        // A key that IS accepted must not appear in the rejection list (it still appears
        // later, in the "Accepted ... keys" catalog).
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

    /// <summary>
    /// Every profile must set <c>group.protocol=consumer</c>: the client defaults to
    /// `classic` and construction fails for it (KIP-848 only). If the key were not
    /// accepted, every soak config would be rejected at startup.
    /// </summary>
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

        // And the kept half now validates, which is the whole point of routing.
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

        // Values may contain '=' — only the FIRST one separates, so a JAAS string needs
        // no escaping.
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
}
