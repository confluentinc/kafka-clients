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
/// Pins the <b>declared</b> public signatures of the admin surface against the Java
/// class they mirror. These are reflection assertions on purpose: a widened signature
/// still compiles and still passes every behavioural test — the compiler happily accepts
/// <c>Task&lt;TopicMetadataAndConfig&gt;</c> where <c>Task</c> was meant, and
/// <c>int</c> where <c>short</c> was — so nothing else in the suite can turn red when
/// the shape drifts.
/// </summary>
/// <remarks>
/// <para>
/// This exists because P1 is the shape checkpoint: every signature here is one P2…P8
/// will copy, and every one of them is <b>breaking to change after publish</b>. All
/// three were wrong in P1's first draft, each in the direction of publishing more than
/// Java does.
/// </para>
/// </remarks>
public sealed class PublicAdminShapeParityTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Java's <c>values()</c> returns <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>
    /// (<c>CreateTopicsResult.java:43-48</c>) — the metadata is erased with
    /// <c>thenApply(v -&gt; null)</c> and the private
    /// <c>Map&lt;String, KafkaFuture&lt;TopicMetadataAndConfig&gt;&gt;</c> is never
    /// published. Publishing it would widen Java's surface, which is the opposite of
    /// restoring it.
    /// </summary>
    [Fact]
    public void CreateTopicsResult_Values_ErasesTheMetadataLikeJava()
    {
        PropertyInfo values = typeof(CreateTopicsResult).GetProperty(nameof(CreateTopicsResult.Values))!;

        Assert.Equal(typeof(IReadOnlyDictionary<string, Task>), values.PropertyType);
    }

    /// <summary>
    /// Java's <b>result</b>-side replication factor is <c>int</c> everywhere:
    /// <c>KafkaFuture&lt;Integer&gt; replicationFactor(String)</c>
    /// (<c>CreateTopicsResult.java:104</c>), <c>int replicationFactor()</c>
    /// (<c>:141</c>) and the <c>(Uuid, int, int, Config)</c> constructor (<c>:115</c>).
    /// The C ABI agrees — <c>int32_t</c> — so a <c>short</c> here would force an
    /// unchecked narrowing of a value the binding does not control. <c>short</c> belongs
    /// to the <b>request</b> side only (<c>NewTopic.replicationFactor()</c>), and
    /// <see cref="NewTopic"/> keeps it.
    /// </summary>
    [Fact]
    public void ReplicationFactor_IsIntOnTheResultSide_AndShortOnTheRequestSide()
    {
        Assert.Equal(
            typeof(Task<int>),
            typeof(CreateTopicsResult)
                .GetMethod(nameof(CreateTopicsResult.ReplicationFactor), new[] { typeof(string) })!
                .ReturnType);

        Assert.Equal(
            typeof(int),
            typeof(TopicMetadataAndConfig)
                .GetMethod(nameof(TopicMetadataAndConfig.ReplicationFactor), Type.EmptyTypes)!
                .ReturnType);

        ConstructorInfo metadataCtor = typeof(TopicMetadataAndConfig).GetConstructor(
            new[] { typeof(Uuid), typeof(int), typeof(int), typeof(Config) })!;
        Assert.NotNull(metadataCtor);

        // The request side is deliberately NOT widened: Java's NewTopic carries `short`.
        Assert.Equal(
            typeof(short?),
            typeof(NewTopic).GetProperty(nameof(NewTopic.ReplicationFactor))!.PropertyType);
    }

    /// <summary>
    /// ⚠ Java's <c>ConfigEntry</c> has exactly two public constructors — the 2-argument
    /// <c>(name, value)</c> (<c>ConfigEntry.java:44</c>) and the 8-argument one
    /// (<c>:59</c>) — and <b>both are public here</b>, with Java's parameter types, names
    /// and order (M15/P13.2, finding G2-4). Neither takes <c>isDefault</c>: it derives from
    /// <c>source</c> (<c>:102-104</c>).
    /// </summary>
    /// <remarks>
    /// The flag-taking form the result marshaller uses stays <see langword="internal"/>, and
    /// so does <see cref="ConfigEntry.ConfigSynonym"/>'s constructor, because Java's is
    /// package-private (<c>:243</c>). This replaces a test that pinned the 2-argument form
    /// as the only public one, from before the maintainer's decision to publish the second.
    /// </remarks>
    [Fact]
    public void ConfigEntry_PublishesBothConstructorsJavaHas()
    {
        ConstructorInfo[] publicCtors = typeof(ConfigEntry).GetConstructors();
        Assert.Equal(2, publicCtors.Length);

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
            (typeof(ConfigEntry.ConfigType), "type"),
            (typeof(string), "documentation"));

        Assert.Empty(typeof(ConfigEntry.ConfigSynonym).GetConstructors());
    }

    /// <summary>
    /// The published 8-argument constructor's behaviour: <c>IsDefault</c> derives from the
    /// source, the entry has value equality, and the two reference parameters it rejects are
    /// rejected with their own names.
    /// </summary>
    /// <remarks>
    /// ⚠ A null <c>synonyms</c> is rejected where Java stores it — the recorded deviation
    /// on the constructor. The message is the runtime's own for the parameter (it differs
    /// between .NET Framework and .NET), so it is asserted against the runtime's rendering
    /// rather than a literal.
    /// </remarks>
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

    /// <summary>
    /// ⚠ A null synonym <b>element</b> is rejected by the published 8-argument constructor
    /// (M15/P13.2, Critic 85 finding 85.2) — the same stricter-than-Java deviation as the
    /// null list (D8), recorded on the constructor.
    /// </summary>
    /// <remarks>
    /// Java stores the element, and its <c>equals</c> / <c>hashCode</c> / <c>toString</c>
    /// tolerate it; here all three read every element, so an accepted null made each of them
    /// throw <see cref="NullReferenceException"/>. Both a leading and a trailing null are
    /// covered, since the check runs per element. The message is the constructor's own, so
    /// it is asserted through the runtime's rendering of that message with the parameter
    /// name (the suffix differs between .NET Framework and .NET).
    /// </remarks>
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

    /// <summary>
    /// The published 8-argument constructor <b>copies</b> the synonyms (M15/P13.2, Critic 85
    /// finding 85.2), so the null-element check cannot be bypassed by mutating the caller's
    /// list afterwards, and the entry's equality and hash cannot change under a key.
    /// </summary>
    /// <remarks>
    /// Java stores the caller's list (<c>ConfigEntry.java:73</c>); the copy is part of the
    /// recorded deviation. <see cref="ConfigEntry.Synonyms"/> is also not the backing array
    /// itself, so it cannot be cast back to one and written through.
    /// </remarks>
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
            "ConfigEntry(name=k, value=v, source=DefaultConfig, isSensitive=false, isReadOnly=false, "
                + "synonyms=[ConfigSynonym(name=a, value=1, source=StaticBrokerConfig)], type=String, "
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

    /// <summary>
    /// The erasure is not merely a declaration: awaiting <c>Values[topic]</c> tells you
    /// the topic was created and hands back nothing, and the metadata comes from the four
    /// typed accessors — exactly the two-step a Java caller performs.
    /// </summary>
    [Fact]
    public async Task Values_ReportsCreationOnly_MetadataComesFromTheTypedAccessors()
    {
        const string Topic = "shape-parity-topic";

        using MockAdminClient admin = new MockAdminClient(2);

        CreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic(Topic, 3, 2) });

        Task creation = result.Values[Topic];
        await TestTimeout.Run(() => creation, s_deadline);

        int replicationFactor = 0;
        await TestTimeout.Run(async () => replicationFactor = await result.ReplicationFactor(Topic), s_deadline);
        Assert.Equal(2, replicationFactor);
    }
}
