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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Errors;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A <see langword="null"/> <see cref="Uuid"/> on the four admin clients and the admin value classes. A topic-id
/// collection keeps it, as Java's does; <c>DeleteTopics</c> and <c>DescribeTopics</c> refuse it at the call, on every
/// client, after the closed check, so no request is made. A value class that Java lets hold it keeps it, and prints it
/// as Java does.
/// </summary>
public sealed class PublicAdminNullTopicIdTests
{
    private const string Topic = "nid";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly Uuid s_id = new Uuid(7L, 9L);

    private static readonly Options[] s_everyOptions = { Options.Absent, Options.Given, Options.Null };

    // How an RPC's options are passed: the overload without them, given, or null.
    private enum Options
    {
        Absent,
        Given,
        Null,
    }

    public static IEnumerable<object[]> EveryClient() => new[]
    {
        new object[] { "KafkaAdminClient" },
        new object[] { "AsyncKafkaAdminClient" },
        new object[] { "MockAdminClient" },
        new object[] { "AsyncMockAdminClient" },
    };

    public static IEnumerable<object[]> EveryMock() => new[]
    {
        new object[] { "MockAdminClient" },
        new object[] { "AsyncMockAdminClient" },
    };

    // Java's ofTopicIds copies the collection as given.
    [Fact]
    public void OfTopicIds_ANullElement_IsKept()
    {
        TopicCollection.TopicIdCollection ids = TopicCollection.OfTopicIds(new[] { s_id, null! });

        Assert.Equal(new Uuid[] { s_id, null! }, ids.TopicIds());
        Assert.True(ids.Equals(ids));
        _ = ids.GetHashCode();
        Assert.NotNull(ids.ToString());
    }

    // Java's real client dereferences the id at the call (topicId.equals(Uuid.ZERO_UUID)); its mock fails the
    // key's future. The C ABI cannot carry it, so all four refuse it at the call, before the ids are read for the
    // early return: an all-zero id beside it changes nothing.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void DeleteTopics_ANullId_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));

        foreach (Uuid[] ids in new[] { new[] { s_id, null! }, new[] { Uuid.ZERO_UUID, null! }, new Uuid[] { null! } })
        {
            TopicCollection topics = TopicCollection.OfTopicIds(ids);
            foreach (Options options in s_everyOptions)
            {
                AssertMustNotContainNull(
                    Refusal(
                        client,
                        a => options == Options.Absent
                            ? a.DeleteTopics(topics)
                            : a.DeleteTopics(topics, OptionsOf(options, () => new DeleteTopicsOptions())),
                        a => options == Options.Absent
                            ? a.DeleteTopics(topics)
                            : a.DeleteTopics(topics, OptionsOf(options, () => new DeleteTopicsOptions()))));
            }
        }
    }

    // Java's real client fails the key's future with InvalidTopicException, its mock with
    // UnknownTopicIdException; the C ABI cannot carry it, so all four refuse it at the call.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void DescribeTopics_ANullId_IsArgumentNullException(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));

        foreach (Uuid[] ids in new[] { new[] { s_id, null! }, new[] { Uuid.ZERO_UUID, null! }, new Uuid[] { null! } })
        {
            TopicCollection topics = TopicCollection.OfTopicIds(ids);
            foreach (Options options in s_everyOptions)
            {
                AssertMustNotContainNull(
                    Refusal(
                        client,
                        a => options == Options.Absent
                            ? a.DescribeTopics(topics)
                            : a.DescribeTopics(topics, OptionsOf(options, () => new DescribeTopicsOptions())),
                        a => options == Options.Absent
                            ? a.DescribeTopics(topics)
                            : a.DescribeTopics(topics, OptionsOf(options, () => new DescribeTopicsOptions()))));
            }
        }
    }

    // On the mocks the refused call reaches nothing, so the topic its other id names is still there.
    [Theory]
    [MemberData(nameof(EveryMock))]
    public async Task MockDeleteTopics_ANullId_DeletesNothing(string kind)
    {
        using IDisposable admin = TestTimeout.Disposal(Create(kind, out IDisposable client));
        Uuid id = await CreateTopic(client);
        TopicCollection topics = TopicCollection.OfTopicIds(new[] { id, null! });

        AssertMustNotContainNull(Refusal(client, a => a.DeleteTopics(topics), a => a.DeleteTopics(topics)));
        AssertMustNotContainNull(Refusal(client, a => a.DescribeTopics(topics), a => a.DescribeTopics(topics)));

        TopicDescription described = await Describe(client, id);
        Assert.Equal(Topic, described.Name());
        Assert.Equal(id, described.TopicId());
    }

    // The closed check still runs first.
    [Theory]
    [MemberData(nameof(EveryClient))]
    public void AClosedClient_ReportsItsClosedCheck_BeforeANullId(string kind)
    {
        IDisposable client = Create(kind, out _);
        TestTimeout.Dispose(client);
        TopicCollection topics = TopicCollection.OfTopicIds(new[] { s_id, null! });

        foreach (Exception? error in new[]
                 {
                     Refusal(client, a => a.DeleteTopics(topics), a => a.DeleteTopics(topics)),
                     Refusal(client, a => a.DescribeTopics(topics), a => a.DescribeTopics(topics)),
                     Refusal(client, a => a.DeleteTopics(topics, null!), a => a.DeleteTopics(topics, null!)),
                 })
        {
            Assert.Equal(
                "Cannot accept new calls when AdminClient is closing.",
                Assert.IsType<IllegalStateException>(error).Message);
        }
    }

    // Java stores it, and its toString prints it.
    [Fact]
    public void TopicListing_ANullId_IsKept_AndPrintsNull()
    {
        TopicListing listing = new TopicListing("t", null!, false);

        Assert.Null(listing.TopicId());
        Assert.Equal("(name=t, topicId=null, internal=false)", listing.ToString());
        _ = listing.GetHashCode();
        Assert.Equal(
            "(name=t, topicId=AAAAAAAAAAAAAAAAAAAAAA, internal=true)",
            new TopicListing("t", Uuid.ZERO_UUID, true).ToString());
    }

    // Java stores it; equals, hashCode and toString do not read it.
    [Fact]
    public void TopicDescription_ANullId_IsKept_AndIsNotCompared()
    {
        TopicDescription withNull =
            new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>(), Array.Empty<AclOperation>(), null!);
        TopicDescription withZero = new TopicDescription("t", false, Array.Empty<TopicPartitionInfo>());

        Assert.Null(withNull.TopicId());
        Assert.Same(Uuid.ZERO_UUID, withZero.TopicId());
        Assert.True(withNull.Equals(withZero));
        Assert.True(withZero.Equals(withNull));
        Assert.Equal(withZero.GetHashCode(), withNull.GetHashCode());
        Assert.Equal(withZero.ToString(), withNull.ToString());
    }

    // Java stores it; the error form sets ZERO_UUID, as Java's and Python's constructors do, and its TopicId()
    // throws the stored exception before reading it.
    [Fact]
    public void TopicMetadataAndConfig_ANullId_IsKept_AndTheErrorFormHoldsZeroUuid()
    {
        CreateTopicsResult.TopicMetadataAndConfig withNull =
            new CreateTopicsResult.TopicMetadataAndConfig(null!, 3, 2, new Config(Array.Empty<ConfigEntry>()));
        Assert.Null(withNull.TopicId());
        Assert.Equal(3, withNull.NumPartitions());

        InvalidTopicException stored = new InvalidTopicException("no metadata");
        CreateTopicsResult.TopicMetadataAndConfig fromError = new CreateTopicsResult.TopicMetadataAndConfig(stored);
        FieldInfo topicId = typeof(CreateTopicsResult.TopicMetadataAndConfig).GetField(
            "_topicId", BindingFlags.NonPublic | BindingFlags.Instance)!;
        Assert.Same(Uuid.ZERO_UUID, topicId.GetValue(fromError));
        Assert.Same(stored, Assert.Throws<InvalidTopicException>(() => fromError.TopicId()));
    }

    // Python's stubs type every topic id these members take or return as a Uuid, never Uuid | None.
    [Fact]
    public void TopicIdAnnotations_FollowPythonsStubs()
    {
        ConstructorInfo listing =
            typeof(TopicListing).GetConstructor(new[] { typeof(string), typeof(Uuid), typeof(bool) })!;
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(listing.GetParameters()[1]));
        Assert.Equal(
            NullableAnnotation.NotAnnotated,
            NullableAnnotation.Flag(typeof(TopicListing).GetMethod(nameof(TopicListing.TopicId))!));

        ParameterInfo descriptionId = Array.Find(
            typeof(TopicDescription).GetConstructors(),
            ctor => ctor.GetParameters().Length == 5)!.GetParameters()[4];
        Assert.Equal(typeof(Uuid), descriptionId.ParameterType);
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(descriptionId));
        Assert.Equal(
            NullableAnnotation.NotAnnotated,
            NullableAnnotation.Flag(typeof(TopicDescription).GetMethod(nameof(TopicDescription.TopicId))!));

        Type metadata = typeof(CreateTopicsResult.TopicMetadataAndConfig);
        ParameterInfo metadataId = metadata.GetConstructor(
            new[] { typeof(Uuid), typeof(int), typeof(int), typeof(Config) })!.GetParameters()[0];
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(metadataId));
        Assert.Equal(
            NullableAnnotation.NotAnnotated,
            NullableAnnotation.Flag(metadata.GetMethod(nameof(CreateTopicsResult.TopicMetadataAndConfig.TopicId))!));

        ParameterInfo ids = typeof(TopicCollection).GetMethod(nameof(TopicCollection.OfTopicIds))!.GetParameters()[0];
        Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(ids, 1));
    }

    private static T OptionsOf<T>(Options options, Func<T> create)
        where T : class =>
        options == Options.Given ? create() : null!;

    private static IDisposable Create(string kind, out IDisposable client)
    {
        client = kind switch
        {
            "KafkaAdminClient" => IAdmin.Create(Unreachable()),
            "AsyncKafkaAdminClient" => IAsyncAdmin.Create(Unreachable()),
            "MockAdminClient" => MockAdminClient.Create().Build(),
            _ => AsyncMockAdminClient.Create().Build(),
        };
        return client;
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?> { ["bootstrap.servers"] = "127.0.0.1:1" };

    // What a call throws at the call; an admin RPC returns its Result at once on every client.
    private static Exception? Refusal(IDisposable client, Func<IAdmin, object> sync, Func<IAsyncAdmin, object> async) =>
        Record.Exception(() => _ = client is IAsyncAdmin asyncAdmin ? async(asyncAdmin) : sync((IAdmin)client));

    private static void AssertMustNotContainNull(Exception? error)
    {
        ArgumentNullException refused = Assert.IsType<ArgumentNullException>(error);
        Assert.Equal("topics", refused.ParamName);
        Assert.Equal(new ArgumentNullException("topics", "topics must not contain null").Message, refused.Message);
    }

    private static async Task<Uuid> CreateTopic(IDisposable client)
    {
        NewTopic[] topics = { new NewTopic(Topic, 1, 1) };
        return client is MockAdminClient sync
            ? await TestTimeout.Run(
                () => Task.Run(() => sync.CreateTopics(topics).TopicId(Topic).Get(s_deadline)), s_deadline)
            : await TestTimeout.Run(
                () => ((AsyncMockAdminClient)client).CreateTopics(topics).TopicId(Topic).Get(), s_deadline);
    }

    private static async Task<TopicDescription> Describe(IDisposable client, Uuid id)
    {
        TopicCollection topics = TopicCollection.OfTopicIds(new[] { id });
        return client is MockAdminClient sync
            ? await TestTimeout.Run(
                () => Task.Run(() => sync.DescribeTopics(topics).TopicIdValues()![id].Get(s_deadline)), s_deadline)
            : await TestTimeout.Run(
                () => ((AsyncMockAdminClient)client).DescribeTopics(topics).TopicIdValues()![id].Get(), s_deadline);
    }
}
