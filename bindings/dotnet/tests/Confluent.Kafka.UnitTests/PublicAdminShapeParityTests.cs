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
    /// Java's <c>ConfigEntry</c> has exactly two public constructors and <b>neither takes
    /// <c>isDefault</c></b> — it is derived from <c>source</c>
    /// (<c>ConfigEntry.java:102-104</c>). Only the 2-argument form is expressible over
    /// today's flattened ABI, so that is the only public one here; the flag-taking form
    /// is <see langword="internal"/>, for the result marshaller alone. Publishing it
    /// would let a caller build an entry whose <c>IsDefault</c> contradicts the
    /// <c>Source</c> a later phase adds — a state Java cannot represent.
    /// </summary>
    [Fact]
    public void ConfigEntry_PublishesOnlyTheConstructorJavaHas()
    {
        ConstructorInfo[] publicCtors = typeof(ConfigEntry).GetConstructors();

        ConstructorInfo only = Assert.Single(publicCtors);
        ParameterInfo[] parameters = only.GetParameters();

        Assert.Equal(2, parameters.Length);
        Assert.Equal(typeof(string), parameters[0].ParameterType);
        Assert.Equal(typeof(string), parameters[1].ParameterType);
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
