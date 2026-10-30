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
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The declared public signatures of <c>CreateTopics</c>' result and of <see cref="ConfigEntry"/> against the Java
/// classes they mirror.
/// </summary>
/// <remarks>
/// These are reflection assertions: a widened signature still compiles and passes every behavioural test, as
/// <c>Task&lt;TopicMetadataAndConfig&gt;</c> where <c>Task</c> was meant, or <c>int</c> where <c>short</c> was.
/// </remarks>
public sealed class PublicAdminShapeParityTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void CreateTopicsResult_Values_ErasesTheMetadataLikeJava()
    {
        PropertyInfo values = typeof(CreateTopicsResult).GetProperty(nameof(CreateTopicsResult.Values))!;

        Assert.Equal(typeof(IReadOnlyDictionary<string, KafkaFuture>), values.PropertyType);

        PropertyInfo asyncValues = typeof(AsyncCreateTopicsResult).GetProperty(nameof(AsyncCreateTopicsResult.Values))!;

        Assert.Equal(typeof(IReadOnlyDictionary<string, AsyncKafkaFuture>), asyncValues.PropertyType);
    }

    [Fact]
    public void ReplicationFactor_IsIntOnTheResultSide_AndShortOnTheRequestSide()
    {
        Assert.Equal(
            typeof(KafkaFuture<int>),
            typeof(CreateTopicsResult)
                .GetMethod(nameof(CreateTopicsResult.ReplicationFactor), new[] { typeof(string) })!
                .ReturnType);
        Assert.Equal(
            typeof(AsyncKafkaFuture<int>),
            typeof(AsyncCreateTopicsResult)
                .GetMethod(nameof(AsyncCreateTopicsResult.ReplicationFactor), new[] { typeof(string) })!
                .ReturnType);

        Assert.Equal(
            typeof(int),
            typeof(TopicMetadataAndConfig)
                .GetMethod(nameof(TopicMetadataAndConfig.ReplicationFactor), Type.EmptyTypes)!
                .ReturnType);

        ConstructorInfo metadataCtor = typeof(TopicMetadataAndConfig).GetConstructor(
            new[] { typeof(Uuid), typeof(int), typeof(int), typeof(Config) })!;
        Assert.NotNull(metadataCtor);

        // The request side is not widened: Java's NewTopic carries a short.
        Assert.Equal(
            typeof(short?),
            typeof(NewTopic).GetProperty(nameof(NewTopic.ReplicationFactor))!.PropertyType);
    }

    [Fact]
    public void ConfigEntry_PublishesBothConstructorsJavaHas()
    {
        ConstructorInfo[] publicCtors = typeof(ConfigEntry).GetConstructors();
        Assert.Equal(2, publicCtors.Length);
        Assert.Empty(typeof(ConfigEntry).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance));

        ConstructorInfo twoArg = Assert.Single(publicCtors, ctor => ctor.GetParameters().Length == 2);
        AssertParameters(
            twoArg,
            (typeof(string), "name"),
            (typeof(string), "value"));

        ConstructorInfo eightArg = Assert.Single(publicCtors, ctor => ctor.GetParameters().Length == 8);
        AssertParameters(
            eightArg,
            (typeof(string), "name"),
            (typeof(string), "value"),
            (typeof(ConfigEntry.ConfigSource), "source"),
            (typeof(bool), "isSensitive"),
            (typeof(bool), "isReadOnly"),
            (typeof(IReadOnlyList<ConfigEntry.ConfigSynonym>), "synonyms"),
            (typeof(ConfigEntry.ConfigType?), "type"),
            (typeof(string), "documentation"));

        Assert.Empty(typeof(ConfigEntry.ConfigSynonym).GetConstructors());
    }

    [Fact]
    public void ConfigEntry_TheEightArgumentConstructor_DerivesIsDefault_AndHasValueEquality()
    {
        ConfigEntry defaulted = Full(ConfigEntry.ConfigSource.DefaultConfig);
        Assert.True(defaulted.IsDefault);
        Assert.Equal(ConfigEntry.ConfigSource.DefaultConfig, defaulted.Source);
        Assert.Equal("k", defaulted.Name);
        Assert.Equal("v", defaulted.Value);
        Assert.True(defaulted.IsSensitive);
        Assert.False(defaulted.IsReadOnly);
        Assert.Empty(defaulted.Synonyms);
        Assert.Equal(ConfigEntry.ConfigType.String, defaulted.Type);
        Assert.Equal("doc", defaulted.Documentation);

        foreach (ConfigEntry.ConfigSource source in
            (ConfigEntry.ConfigSource[])Enum.GetValues(typeof(ConfigEntry.ConfigSource)))
        {
            Assert.Equal(source == ConfigEntry.ConfigSource.DefaultConfig, Full(source).IsDefault);
        }

        ConfigEntry same = Full(ConfigEntry.ConfigSource.DefaultConfig);
        Assert.Equal(defaulted, same);
        Assert.Equal(defaulted.GetHashCode(), same.GetHashCode());
        Assert.NotEqual(defaulted, Full(ConfigEntry.ConfigSource.StaticBrokerConfig));

        ArgumentNullException nullName = Assert.Throws<ArgumentNullException>(() => new ConfigEntry(
            null!, "v", ConfigEntry.ConfigSource.DefaultConfig, false, false,
            Array.Empty<ConfigEntry.ConfigSynonym>(), ConfigEntry.ConfigType.String, null));
        Assert.Equal("name", nullName.ParamName);
        Assert.Equal(new ArgumentNullException("name").Message, nullName.Message);

        ArgumentNullException nullSynonyms = Assert.Throws<ArgumentNullException>(() => new ConfigEntry(
            "k", "v", ConfigEntry.ConfigSource.DefaultConfig, false, false,
            null!, ConfigEntry.ConfigType.String, null));
        Assert.Equal("synonyms", nullSynonyms.ParamName);
        Assert.Equal(new ArgumentNullException("synonyms").Message, nullSynonyms.Message);
    }

    [Fact]
    public void ConfigEntry_TheEightArgumentConstructor_RejectsANullSynonymElement()
    {
        ConfigEntry.ConfigSynonym synonym =
            new ConfigEntry.ConfigSynonym("a", "1", ConfigEntry.ConfigSource.StaticBrokerConfig);
        string expected =
            new ArgumentNullException("synonyms", "Config synonyms must not contain a null element.").Message;

        foreach (IReadOnlyList<ConfigEntry.ConfigSynonym> synonyms in new IReadOnlyList<ConfigEntry.ConfigSynonym>[]
        {
            new ConfigEntry.ConfigSynonym[1],
            new[] { synonym, null! },
        })
        {
            ArgumentNullException nullElement = Assert.Throws<ArgumentNullException>(
                () => WithSynonyms(synonyms));
            Assert.Equal("synonyms", nullElement.ParamName);
            Assert.Equal(expected, nullElement.Message);
        }
    }

    [Fact]
    public void ConfigEntry_TheEightArgumentConstructor_CopiesTheSynonyms()
    {
        ConfigEntry.ConfigSynonym synonym =
            new ConfigEntry.ConfigSynonym("a", "1", ConfigEntry.ConfigSource.StaticBrokerConfig);
        List<ConfigEntry.ConfigSynonym> callers = new List<ConfigEntry.ConfigSynonym> { synonym };

        ConfigEntry entry = WithSynonyms(callers);
        ConfigEntry twin = WithSynonyms(new[] { synonym });
        int hash = entry.GetHashCode();

        callers[0] = new ConfigEntry.ConfigSynonym("b", "2", ConfigEntry.ConfigSource.DefaultConfig);
        callers.Add(null!);

        Assert.Equal(new[] { synonym }, entry.Synonyms);
        Assert.False(entry.Synonyms is ConfigEntry.ConfigSynonym[], "Synonyms must not expose the backing array");
        Assert.Equal(twin, entry);
        Assert.Equal(hash, entry.GetHashCode());
        Assert.Equal(
            "ConfigEntry(name=k, value=v, source=DEFAULT_CONFIG, isSensitive=false, isReadOnly=false, "
                + "synonyms=[ConfigSynonym(name=a, value=1, source=STATIC_BROKER_CONFIG)], type=STRING, "
                + "documentation=doc)",
            entry.ToString());
    }

    private static ConfigEntry WithSynonyms(IReadOnlyList<ConfigEntry.ConfigSynonym> synonyms) =>
        new ConfigEntry(
            "k",
            "v",
            ConfigEntry.ConfigSource.DefaultConfig,
            isSensitive: false,
            isReadOnly: false,
            synonyms,
            ConfigEntry.ConfigType.String,
            "doc");

    private static ConfigEntry Full(ConfigEntry.ConfigSource source) =>
        new ConfigEntry(
            "k",
            "v",
            source,
            isSensitive: true,
            isReadOnly: false,
            Array.Empty<ConfigEntry.ConfigSynonym>(),
            ConfigEntry.ConfigType.String,
            "doc");

    private static void AssertParameters(ConstructorInfo ctor, params (Type Type, string Name)[] expected)
    {
        ParameterInfo[] parameters = ctor.GetParameters();
        Assert.Equal(expected.Length, parameters.Length);
        for (int index = 0; index < expected.Length; index++)
        {
            Assert.Equal(expected[index].Type, parameters[index].ParameterType);
            Assert.Equal(expected[index].Name, parameters[index].Name);
        }
    }

    [Fact]
    public async Task Values_ReportsCreationOnly_MetadataComesFromTheTypedAccessors()
    {
        const string Topic = "shape-parity-topic";

        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(2).Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncCreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic(Topic, 3, 2) });

        Task creation = result.Values[Topic].Get();
        await TestTimeout.Run(() => creation, s_deadline);

        int replicationFactor = 0;
        await TestTimeout.Run(async () => replicationFactor = await result.ReplicationFactor(Topic).Get(), s_deadline);
        Assert.Equal(2, replicationFactor);
    }
}
