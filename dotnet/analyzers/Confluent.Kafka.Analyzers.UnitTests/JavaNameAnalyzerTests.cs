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
using System.Threading.Tasks;

using Microsoft.CodeAnalysis.Testing;

using Xunit;

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.JavaNameAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0013 reports a public name that is not the name of the Java type or member it translates, and CKRDA0014 a
/// line of the baseline, the allowlist or the pairs that matches nothing.
/// </summary>
public sealed class JavaNameAnalyzerTests
{
    private const string Node =
        "type\torg.apache.kafka.common.Node\tclass\t-\n"
        + "ctor\torg.apache.kafka.common.Node\tNode()\n"
        + "method\torg.apache.kafka.common.Node\tinstance\thost()\n";

    private const string Callback =
        "type\torg.apache.kafka.clients.producer.Callback\tinterface\t-\n"
        + "method\torg.apache.kafka.clients.producer.Callback\tinstance\tonCompletion(RecordMetadata metadata, Exception exception)\n";

    [Fact]
    public Task AProjectGivenNoJavaList_IsNotChecked() =>
        Verifier.VerifyAsync("""
            namespace Confluent.Kafka.Common
            {
                public sealed class Widget
                {
                    public int Size => 0;
                }
            }
            """);

    [Fact]
    public Task NamesThatFollowJava_AreNotReported() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class Node
                {
                    public const int NO_ID = -1;

                    public Node(int id, string host)
                    {
                    }

                    public int Id() => 0;

                    public string Host() => "";

                    public override string ToString() => "";

                    public override bool Equals(object obj) => false;

                    public override int GetHashCode() => 0;
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.Node\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.Node\tNode(int id, String host)\n"
                + "method\torg.apache.kafka.common.Node\tinstance\tid()\n"
                + "method\torg.apache.kafka.common.Node\tinstance\thost()\n"
                + "field\torg.apache.kafka.common.Node\tNO_ID\n"));

    [Fact]
    public Task APropertyWhereJavaHasAMethod_IsReportedOnce() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class Node
                {
                    public string {|#0:Host|} => "";
                }
            }
            """,
            Files(Node),
            Name(0, "property P:Confluent.Kafka.Common.Node.Host: Java's Node.host() is a method"));

    [Fact]
    public Task AMemberJavaDoesNotHave_IsReported() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class Node
                {
                    public string Host() => "";

                    public void {|#0:Frob|}()
                    {
                    }
                }
            }
            """,
            Files(Node),
            Name(0, "unmatched M:Confluent.Kafka.Common.Node.Frob: Java's Node has no method of this name"));

    [Fact]
    public Task ATypeJavaDoesNotHave_IsReported_AndItsMembersAreNotChecked() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class {|#0:Widget|}
                {
                    public int Size => 0;
                }
            }
            """,
            Files(Node),
            Name(0, "unmatched T:Confluent.Kafka.Common.Widget: Java has no type of this name, so its members are not checked"));

    [Fact]
    public Task AnInterface_IsPairedWithoutItsIPrefix() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Producer
            {
                public interface ICallback
                {
                    void OnCompletion(object metadata, System.Exception exception);
                }
            }
            """,
            Files(Callback));

    [Fact]
    public Task AnAsyncPeer_HasItsJavaClassesNameAfterAsync_AndItsMembersAreCompared() =>
        Verifier.VerifyWithFilesAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;

            namespace Confluent.Kafka.Producer
            {
                public sealed class {|#0:AsyncKafkaProducer|}
                {
                    public AsyncKafkaProducer(object configs)
                    {
                    }

                    public Task Flush(CancellationToken cancellationToken = default) => Task.CompletedTask;

                    public void {|#2:Frob|}()
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.producer.KafkaProducer\tclass\t-\n"
                + "ctor\torg.apache.kafka.clients.producer.KafkaProducer\tKafkaProducer(Map configs)\n"
                + "method\torg.apache.kafka.clients.producer.KafkaProducer\tinstance\tclose()\n"
                + "method\torg.apache.kafka.clients.producer.KafkaProducer\tinstance\tflush()\n"),
            Name(0, "missing T:Confluent.Kafka.Producer.AsyncKafkaProducer org.apache.kafka.clients.producer.KafkaProducer#close(): "
                + "has no counterpart of Java's org.apache.kafka.clients.producer.KafkaProducer#close()"),
            Name(2, "unmatched M:Confluent.Kafka.Producer.AsyncKafkaProducer.Frob: Java's KafkaProducer has no method of this name"));

    [Fact]
    public Task ARenamedTypeWithNoPair_IsReported_AndItsMembersAreNotChecked() =>
        Verifier.VerifyWithFilesAsync(
            DeliveryCallback,
            Files(Callback),
            Name(0, "unmatched T:Confluent.Kafka.Producer.IDeliveryCallback: Java has no type of this name, so its members are "
                + "not checked"));

    [Fact]
    public Task ARenamedTypeWithAPair_IsStillReported_AndItsMembersAreChecked() =>
        Verifier.VerifyWithFilesAsync(
            DeliveryCallback,
            Files(Callback, pairs: "Confluent.Kafka.Producer.IDeliveryCallback org.apache.kafka.clients.producer.Callback\n"),
            Name(0, "unmatched T:Confluent.Kafka.Producer.IDeliveryCallback: Java has no type of this name; its members are "
                + "compared with org.apache.kafka.clients.producer.Callback"),
            Name(1, "unmatched M:Confluent.Kafka.Producer.IDeliveryCallback.Extra: Java's Callback has no method of this name"));

    [Fact]
    public Task ANestedTypeCSharpMovesOut_KeepsItsJavaName() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Consumer
            {
                public sealed class CloseOptions
                {
                    private CloseOptions()
                    {
                    }

                    public GroupMembershipOperation GroupMembershipOperation() => default;
                }

                public enum GroupMembershipOperation
                {
                    LEAVE_GROUP,
                    REMAIN_IN_GROUP,
                }
            }
            """,
            Files(CloseOptions));

    [Fact]
    public Task ANestedTypeMovedOutWithoutCause_IsReported() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Consumer
            {
                public sealed class {|#0:CloseOptions|}
                {
                    private CloseOptions()
                    {
                    }
                }

                public enum {|#1:GroupMembershipOperation|}
                {
                    LEAVE_GROUP,
                    REMAIN_IN_GROUP,
                }
            }
            """,
            Files(CloseOptions),
            Name(0, "missing T:Confluent.Kafka.Consumer.CloseOptions org.apache.kafka.clients.consumer.CloseOptions"
                + "#groupMembershipOperation(): has no counterpart of Java's org.apache.kafka.clients.consumer.CloseOptions"
                + "#groupMembershipOperation()"),
            Name(1, "unmatched T:Confluent.Kafka.Consumer.GroupMembershipOperation: Java has no type of this name; its members "
                + "are compared with org.apache.kafka.clients.consumer.CloseOptions$GroupMembershipOperation"));

    [Fact]
    public Task AJavaMemberDotNetLacks_IsReportedOnTheType() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class {|#0:Node|}
                {
                    public string Host() => "";
                }
            }
            """,
            Files(Node + "method\torg.apache.kafka.common.Node\tinstance\thasRack()\n"),
            Name(0, "missing T:Confluent.Kafka.Common.Node org.apache.kafka.common.Node#hasRack(): has no counterpart of "
                + "Java's org.apache.kafka.common.Node#hasRack()"));

    [Fact]
    public Task OptionalParameters_AcceptEveryJavaOverloadTheyCover() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Consumer
            {
                public sealed class {|#0:MockConsumer|}
                {
                    public void EnforceRebalance(string reason = null)
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.consumer.MockConsumer\tclass\t-\n"
                + "ctor\torg.apache.kafka.clients.consumer.MockConsumer\tMockConsumer()\n"
                + "method\torg.apache.kafka.clients.consumer.MockConsumer\tinstance\tenforceRebalance()\n"
                + "method\torg.apache.kafka.clients.consumer.MockConsumer\tinstance\tenforceRebalance(String reason)\n"
                + "method\torg.apache.kafka.clients.consumer.MockConsumer\tinstance\tenforceRebalance(String reason, int x)\n"),
            Name(0, "missing T:Confluent.Kafka.Consumer.MockConsumer org.apache.kafka.clients.consumer.MockConsumer"
                + "#enforceRebalance(String,int): has no counterpart of Java's org.apache.kafka.clients.consumer.MockConsumer"
                + "#enforceRebalance(String,int)"));

    [Fact]
    public Task ParameterNames_FollowJava_TheInterfaceDeclarationFirst() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Consumer
            {
                public sealed class KafkaConsumer
                {
                    public void {|#0:Subscribe|}(object topics, object listener)
                    {
                    }

                    public void {|#1:Subscribe|}(object topics, object callback, int extra, int more)
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.consumer.Consumer\tinterface\t-\n"
                + "method\torg.apache.kafka.clients.consumer.Consumer\tinstance\tsubscribe(Collection topics, "
                + "ConsumerRebalanceListener callback)\n"
                + "type\torg.apache.kafka.clients.consumer.KafkaConsumer\tclass\torg.apache.kafka.clients.consumer.Consumer\n"
                + "ctor\torg.apache.kafka.clients.consumer.KafkaConsumer\tKafkaConsumer()\n"
                + "method\torg.apache.kafka.clients.consumer.KafkaConsumer\tinstance\tsubscribe(Collection topics, "
                + "ConsumerRebalanceListener listener)\n"),
            Name(0, "parameter M:Confluent.Kafka.Consumer.KafkaConsumer.Subscribe(System.Object,System.Object): names its "
                + "parameters (topics, listener) where Java's KafkaConsumer.subscribe names them (topics, callback)"),
            Name(1, "overload M:Confluent.Kafka.Consumer.KafkaConsumer.Subscribe(System.Object,System.Object,System.Int32,"
                + "System.Int32): Java's KafkaConsumer.subscribe has no overload with 4 parameter(s)"));

    [Fact]
    public Task GetEnumerator_IsDotNetsFormOfIterable() =>
        Verifier.VerifyWithFilesAsync(
            Records,
            Files(
                "type\torg.apache.kafka.clients.consumer.ConsumerRecords\tclass\tIterable\n"
                + "ctor\torg.apache.kafka.clients.consumer.ConsumerRecords\tConsumerRecords()\n"
                + "method\torg.apache.kafka.clients.consumer.ConsumerRecords\tinstance\tcount()\n"
                + "method\torg.apache.kafka.clients.consumer.ConsumerRecords\tinstance\titerator()\n"),
            Name(0, "property P:Confluent.Kafka.Consumer.ConsumerRecords.Count: Java's ConsumerRecords.count() is a method"));

    [Fact]
    public Task GetEnumerator_WhereJavaIsNotIterable_IsReported() =>
        Verifier.VerifyWithFilesAsync(
            Records,
            Files(
                "type\torg.apache.kafka.clients.consumer.ConsumerRecords\tclass\t-\n"
                + "ctor\torg.apache.kafka.clients.consumer.ConsumerRecords\tConsumerRecords()\n"
                + "method\torg.apache.kafka.clients.consumer.ConsumerRecords\tinstance\tcount()\n"),
            Name(0, "property P:Confluent.Kafka.Consumer.ConsumerRecords.Count: Java's ConsumerRecords.count() is a method"),
            Name(1, "unmatched M:Confluent.Kafka.Consumer.ConsumerRecords.GetEnumerator: Java's ConsumerRecords has no method "
                + "of this name"));

    [Fact]
    public Task IEquatableEquals_IsNotAnOverload_AndObjectEqualsKeepsItsBaseParameterName() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public readonly struct TopicPartition : System.IEquatable<TopicPartition>
                {
                    public TopicPartition(string topic, int partition)
                    {
                    }

                    public bool Equals(TopicPartition other) => true;

                    public override bool Equals(object obj) => false;

                    public override int GetHashCode() => 0;
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.TopicPartition\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.TopicPartition\tTopicPartition(String topic, int partition)\n"
                + "method\torg.apache.kafka.common.TopicPartition\tinstance\tequals(Object o)\n"
                + "method\torg.apache.kafka.common.TopicPartition\tinstance\thashCode()\n"));

    [Fact]
    public Task AnEnum_HasJavasConstants_AndCoversJavasBuiltInEnumMembers() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public enum {|#0:IsolationLevel|}
                {
                    READ_UNCOMMITTED,
                    {|#1:ReadCommitted|},
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.IsolationLevel\tenum\t-\n"
                + "constant\torg.apache.kafka.common.IsolationLevel\tREAD_COMMITTED\n"
                + "constant\torg.apache.kafka.common.IsolationLevel\tREAD_UNCOMMITTED\n"
                + "method\torg.apache.kafka.common.IsolationLevel\tinstance\tid()\n"
                + "method\torg.apache.kafka.common.IsolationLevel\tinstance\tname()\n"
                + "method\torg.apache.kafka.common.IsolationLevel\tinstance\tordinal()\n"
                + "method\torg.apache.kafka.common.IsolationLevel\tstatic\tvalueOf(String ?)\n"
                + "method\torg.apache.kafka.common.IsolationLevel\tstatic\tvalues()\n"),
            Name(0, "missing T:Confluent.Kafka.Common.IsolationLevel org.apache.kafka.common.IsolationLevel#READ_COMMITTED: has no "
                + "counterpart of Java's org.apache.kafka.common.IsolationLevel#READ_COMMITTED"),
            Name(0, "missing T:Confluent.Kafka.Common.IsolationLevel org.apache.kafka.common.IsolationLevel#id(): has no "
                + "counterpart of Java's org.apache.kafka.common.IsolationLevel#id()"),
            Name(1, "unmatched F:Confluent.Kafka.Common.IsolationLevel.ReadCommitted: Java's IsolationLevel has no field or "
                + "constant of this name"));

    [Fact]
    public Task AJavaNestedTypeDotNetLacks_IsReportedOnItsOuterType() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class {|#0:Outer|}
                {
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.Outer\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.Outer\tOuter()\n"
                + "type\torg.apache.kafka.common.Outer$Inner\tclass\t-\n"),
            Name(0, "missing T:Confluent.Kafka.Common.Outer org.apache.kafka.common.Outer$Inner: has no counterpart of Java's "
                + "org.apache.kafka.common.Outer$Inner"));

    [Fact]
    public Task TheBaseline_HidesItsFindings_AndALineMatchingNothingIsReported() =>
        Verifier.VerifyWithFilesAsync(
            HostProperty,
            Files(
                Node,
                baseline: "# to fix\nproperty P:Confluent.Kafka.Common.Node.Host\nunmatched M:Confluent.Kafka.Common.Node.Gone\n"),
            Stale(JavaNameAnalyzer.BaselineFile, 3, "unmatched M:Confluent.Kafka.Common.Node.Gone"));

    [Fact]
    public Task TheAllowlist_HidesItsFindings_AndALineMatchingNothingIsReported() =>
        Verifier.VerifyWithFilesAsync(
            HostProperty,
            Files(
                Node,
                allowlist: "property P:Confluent.Kafka.Common.Node.Host\n\nmissing T:Confluent.Kafka.Common.Node x#y()\n"),
            Stale(JavaNameAnalyzer.AllowlistFile, 3, "missing T:Confluent.Kafka.Common.Node x#y()"));

    [Fact]
    public Task APairNoTypeNeeds_IsReported() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class Node
                {
                    public string Host() => "";
                }
            }
            """,
            Files(
                Node,
                pairs: "Confluent.Kafka.Common.Node org.apache.kafka.common.Node\n"
                    + "Confluent.Kafka.Common.Gone org.apache.kafka.common.Node\n"),
            Stale(JavaNameAnalyzer.PairsFile, 1, "Confluent.Kafka.Common.Node org.apache.kafka.common.Node"),
            Stale(JavaNameAnalyzer.PairsFile, 2, "Confluent.Kafka.Common.Gone org.apache.kafka.common.Node"));

    [Fact]
    public Task TheJavaListTheRustCommandWrites_IsReadAsWritten() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class TopicPartition
                {
                    public TopicPartition(string topic, int partition)
                    {
                    }

                    public string {|#0:Topic|} => "";

                    public int Partition() => 0;

                    public override bool Equals(object obj) => false;

                    public override int GetHashCode() => 0;

                    public override string ToString() => "";
                }

                public enum {|#1:IsolationLevel|}
                {
                    READ_UNCOMMITTED,
                    READ_COMMITTED,
                }
            }
            """,
            Files(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, JavaNameAnalyzer.JavaApiFile))),
            Name(0, "property P:Confluent.Kafka.Common.TopicPartition.Topic: Java's TopicPartition.topic() is a method"),
            Name(1, "missing T:Confluent.Kafka.Common.IsolationLevel org.apache.kafka.common.IsolationLevel#forId(byte): has no "
                + "counterpart of Java's org.apache.kafka.common.IsolationLevel#forId(byte)"),
            Name(1, "missing T:Confluent.Kafka.Common.IsolationLevel org.apache.kafka.common.IsolationLevel#id(): has no "
                + "counterpart of Java's org.apache.kafka.common.IsolationLevel#id()"),
            Name(1, "missing T:Confluent.Kafka.Common.IsolationLevel org.apache.kafka.common.IsolationLevel#toString(): has no "
                + "counterpart of Java's org.apache.kafka.common.IsolationLevel#toString()"));

    private const string DeliveryCallback = """
        namespace Confluent.Kafka.Producer
        {
            public interface {|#0:IDeliveryCallback|}
            {
                void OnCompletion(object metadata, System.Exception exception);

                void {|#1:Extra|}();
            }
        }
        """;

    private const string CloseOptions =
        "type\torg.apache.kafka.clients.consumer.CloseOptions\tclass\t-\n"
        + "method\torg.apache.kafka.clients.consumer.CloseOptions\tinstance\tgroupMembershipOperation()\n"
        + "type\torg.apache.kafka.clients.consumer.CloseOptions$GroupMembershipOperation\tenum\t-\n"
        + "constant\torg.apache.kafka.clients.consumer.CloseOptions$GroupMembershipOperation\tLEAVE_GROUP\n"
        + "constant\torg.apache.kafka.clients.consumer.CloseOptions$GroupMembershipOperation\tREMAIN_IN_GROUP\n"
        + "method\torg.apache.kafka.clients.consumer.CloseOptions$GroupMembershipOperation\tinstance\tname()\n";

    [Fact]
    public Task OnlyALastOptionalCancellationToken_IsLeftOutOfTheComparison() =>
        Verifier.VerifyWithFilesAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;

            namespace Confluent.Kafka.Common
            {
                public sealed class {|#2:Node|}
                {
                    public Task Host(CancellationToken cancellationToken = default) => Task.CompletedTask;

                    public Task {|#0:Port|}(int timeoutMs = 0) => Task.CompletedTask;

                    public Task {|#1:Rack|}(CancellationToken cancellationToken) => Task.CompletedTask;
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.Node\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.Node\tNode()\n"
                + "method\torg.apache.kafka.common.Node\tinstance\thost()\n"
                + "method\torg.apache.kafka.common.Node\tinstance\tport()\n"
                + "method\torg.apache.kafka.common.Node\tinstance\track()\n"),
            Name(0, "parameter M:Confluent.Kafka.Common.Node.Port(System.Int32): adds the .NET-only parameter(s) timeoutMs to "
                + "Java's Node.port()"),
            Name(1, "overload M:Confluent.Kafka.Common.Node.Rack(System.Threading.CancellationToken): Java's Node.rack has no "
                + "overload with 1 parameter(s)"),
            Name(2, "missing T:Confluent.Kafka.Common.Node org.apache.kafka.common.Node#rack(): has no counterpart of Java's "
                + "org.apache.kafka.common.Node#rack()"));

    [Fact]
    public Task ARenamedParameterBesideAnAddedOne_IsOneParameterFinding() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class Node
                {
                    public string {|#0:Host|}(int nodeId, bool quick = false) => "";
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.Node\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.Node\tNode()\n"
                + "method\torg.apache.kafka.common.Node\tinstance\thost(int id)\n"),
            Name(0, "parameter M:Confluent.Kafka.Common.Node.Host(System.Int32,System.Boolean): names its parameters (nodeId) "
                + "where Java's Node.host names them (id), and adds the .NET-only parameter(s) quick"));

    [Fact]
    public Task OverloadsWithTheSameNamesAndOtherTypes_AreEachChecked_AndJavasPairsWithTheSyncForm() =>
        Verifier.VerifyWithFilesAsync(
            """
            using System.Threading.Tasks;

            namespace Confluent.Kafka.Producer
            {
                public interface ICallback
                {
                    void OnCompletion();
                }

                public interface IAsyncCallback
                {
                    Task OnCompletion();
                }

                public sealed class KafkaProducer
                {
                    public void {|#0:Send|}(object record, IAsyncCallback callback)
                    {
                    }

                    public void Send(object record, ICallback callback)
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.producer.Callback\tinterface\t-\n"
                + "method\torg.apache.kafka.clients.producer.Callback\tinstance\tonCompletion()\n"
                + "type\torg.apache.kafka.clients.producer.KafkaProducer\tclass\t-\n"
                + "ctor\torg.apache.kafka.clients.producer.KafkaProducer\tKafkaProducer()\n"
                + "method\torg.apache.kafka.clients.producer.KafkaProducer\tinstance\tsend(ProducerRecord record, Callback callback)\n"),
            Name(0, "overload M:Confluent.Kafka.Producer.KafkaProducer.Send(System.Object,Confluent.Kafka.Producer.IAsyncCallback): "
                + "Java's KafkaProducer.send has no overload with 2 parameter(s)"));

    [Fact]
    public Task AJavaStaticMethod_IsRequiredOnlyOnTheTypeThatDeclaresIt() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Admin
            {
                public interface {|#0:IAdmin|}
                {
                    void Close();
                }

                public sealed class KafkaAdminClient : IAdmin
                {
                    public void Close()
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.admin.Admin\tinterface\t-\n"
                + "method\torg.apache.kafka.clients.admin.Admin\tinstance\tclose()\n"
                + "method\torg.apache.kafka.clients.admin.Admin\tstatic\tcreate(Map conf)\n"
                + "type\torg.apache.kafka.clients.admin.AdminClient\tabstract class\torg.apache.kafka.clients.admin.Admin\n"
                + "ctor\torg.apache.kafka.clients.admin.AdminClient\tAdminClient()\n"
                + "method\torg.apache.kafka.clients.admin.AdminClient\tstatic\tcreate(Map conf)\n"
                + "type\torg.apache.kafka.clients.admin.KafkaAdminClient\tclass\torg.apache.kafka.clients.admin.AdminClient\n"
                + "ctor\torg.apache.kafka.clients.admin.KafkaAdminClient\tKafkaAdminClient()\n"),
            Name(0, "missing T:Confluent.Kafka.Admin.IAdmin org.apache.kafka.clients.admin.Admin#create(Map): has no counterpart "
                + "of Java's org.apache.kafka.clients.admin.Admin#create(Map)"));

    [Fact]
    public Task AJdkException_IsInTheRootNamespace_AndAnExceptionNamesItsCauseInnerException() =>
        Verifier.VerifyWithFilesAsync(
            """
            using System;

            namespace Confluent.Kafka
            {
                public class IllegalStateException : InvalidOperationException
                {
                    public IllegalStateException()
                    {
                    }

                    public IllegalStateException(string message)
                        : base(message)
                    {
                    }

                    public IllegalStateException(string message, Exception innerException)
                        : base(message, innerException)
                    {
                    }

                    public IllegalStateException(Exception innerException)
                        : base(null, innerException)
                    {
                    }
                }
            }

            namespace Confluent.Kafka.Common.Config
            {
                public class ConfigException : Exception
                {
                    public ConfigException(string message)
                        : base(message)
                    {
                    }

                    public {|#0:ConfigException|}(string key, object value)
                    {
                    }
                }
            }
            """,
            Files(
                "type\tjava.lang.IllegalStateException\tclass\tjava.lang.RuntimeException\n"
                + "ctor\tjava.lang.IllegalStateException\tIllegalStateException()\n"
                + "ctor\tjava.lang.IllegalStateException\tIllegalStateException(String s)\n"
                + "ctor\tjava.lang.IllegalStateException\tIllegalStateException(String message, Throwable cause)\n"
                + "ctor\tjava.lang.IllegalStateException\tIllegalStateException(Throwable cause)\n"
                + "type\torg.apache.kafka.common.config.ConfigException\tclass\tRuntimeException\n"
                + "ctor\torg.apache.kafka.common.config.ConfigException\tConfigException(String message)\n"
                + "ctor\torg.apache.kafka.common.config.ConfigException\tConfigException(String name, Object value)\n"),
            Name(0, "parameter M:Confluent.Kafka.Common.Config.ConfigException.#ctor(System.String,System.Object): names its "
                + "parameters (key, value) where Java's ConfigException constructor names them (name, value)"));

    [Fact]
    public Task AJavaEnumsMethods_SitInAStaticClassNamedAfterTheEnum() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Admin
            {
                public enum {|#1:ScramMechanism|}
                {
                    UNKNOWN,
                    SCRAM_SHA_256,
                }

                public static class ScramMechanisms
                {
                    public static string MechanismName(this ScramMechanism mechanism) => "";

                    public static ScramMechanism FromType(byte type) => default;

                    public static ScramMechanism {|#0:FromName|}(string name) => default;
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.admin.ScramMechanism\tenum\t-\n"
                + "constant\torg.apache.kafka.clients.admin.ScramMechanism\tUNKNOWN\n"
                + "constant\torg.apache.kafka.clients.admin.ScramMechanism\tSCRAM_SHA_256\n"
                + "method\torg.apache.kafka.clients.admin.ScramMechanism\tinstance\tmechanismName()\n"
                + "method\torg.apache.kafka.clients.admin.ScramMechanism\tinstance\tname()\n"
                + "method\torg.apache.kafka.clients.admin.ScramMechanism\tstatic\tfromMechanismName(String mechanismName)\n"
                + "method\torg.apache.kafka.clients.admin.ScramMechanism\tstatic\tfromType(byte type)\n"),
            Name(0, "unmatched M:Confluent.Kafka.Admin.ScramMechanisms.FromName(System.String): Java's ScramMechanism has no "
                + "method of this name"),
            Name(1, "missing T:Confluent.Kafka.Admin.ScramMechanism org.apache.kafka.clients.admin.ScramMechanism#fromMechanismName(String): "
                + "has no counterpart of Java's org.apache.kafka.clients.admin.ScramMechanism#fromMechanismName(String)"));

    [Fact]
    public Task TheCompanionClassesPropertiesFieldsAndEvents_AreCheckedAsTheEnums() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Admin
            {
                public enum ScramMechanism
                {
                    UNKNOWN,
                }

                public static class ScramMechanisms
                {
                    public static int {|#2:Limit|};

                    public static int {|#0:Extra|} => 0;

                    public static event System.Action {|#1:Changed|}
                    {
                        add
                        {
                        }

                        remove
                        {
                        }
                    }

                    public static string MechanismName(this ScramMechanism mechanism) => "";
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.admin.ScramMechanism\tenum\t-\n"
                + "constant\torg.apache.kafka.clients.admin.ScramMechanism\tUNKNOWN\n"
                + "method\torg.apache.kafka.clients.admin.ScramMechanism\tinstance\tmechanismName()\n"),
            Name(0, "unmatched P:Confluent.Kafka.Admin.ScramMechanisms.Extra: Java's ScramMechanism has no method or field of this name"),
            Name(1, "unmatched E:Confluent.Kafka.Admin.ScramMechanisms.Changed: Java has no events"),
            Name(2, "unmatched F:Confluent.Kafka.Admin.ScramMechanisms.Limit: Java's ScramMechanism has no field or constant of this name"));

    [Fact]
    public Task AJavaAbstractClassesConstructors_AreProtected_AndATypeNothingDerivesFromHasNone() =>
        Verifier.VerifyWithFilesAsync(
            """
            using System;

            namespace Confluent.Kafka.Common.Errors
            {
                public abstract class {|#0:RetriableException|} : Exception
                {
                    protected RetriableException(string message)
                        : base(message)
                    {
                    }

                    protected RetriableException(string message, Exception innerException)
                        : base(message, innerException)
                    {
                    }
                }
            }

            namespace Confluent.Kafka.Common
            {
                public readonly struct KafkaFuture
                {
                    public void Get()
                    {
                    }

                    public void Get(TimeSpan timeout)
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.errors.RetriableException\tabstract class\tRuntimeException\n"
                + "ctor\torg.apache.kafka.common.errors.RetriableException\tRetriableException(String message)\n"
                + "ctor\torg.apache.kafka.common.errors.RetriableException\tRetriableException(String message, Throwable cause)\n"
                + "ctor\torg.apache.kafka.common.errors.RetriableException\tRetriableException(Throwable cause)\n"
                + "type\torg.apache.kafka.common.KafkaFuture\tabstract class\t-\n"
                + "ctor\torg.apache.kafka.common.KafkaFuture\tKafkaFuture()\n"
                + "method\torg.apache.kafka.common.KafkaFuture\tinstance\tget()\n"
                + "method\torg.apache.kafka.common.KafkaFuture\tinstance\tget(long timeout, TimeUnit unit)\n"),
            Name(0, "missing T:Confluent.Kafka.Common.Errors.RetriableException "
                + "org.apache.kafka.common.errors.RetriableException#RetriableException(Throwable): has no counterpart of "
                + "Java's org.apache.kafka.common.errors.RetriableException#RetriableException(Throwable)"));

    [Fact]
    public Task APropertiesOverload_IsItsMapOverload_AndARegexPatternOverloadIsNotTranslated() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Consumer
            {
                public sealed class KafkaConsumer
                {
                    public KafkaConsumer(object configs, object keyDeserializer, object valueDeserializer)
                    {
                    }

                    public void Subscribe(System.Collections.Generic.IReadOnlyCollection<string> topics)
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.consumer.KafkaConsumer\tclass\t-\n"
                + "ctor\torg.apache.kafka.clients.consumer.KafkaConsumer\t"
                + "KafkaConsumer(Map configs, Deserializer keyDeserializer, Deserializer valueDeserializer)\n"
                + "ctor\torg.apache.kafka.clients.consumer.KafkaConsumer\t"
                + "KafkaConsumer(Properties properties, Deserializer keyDeserializer, Deserializer valueDeserializer)\n"
                + "method\torg.apache.kafka.clients.consumer.KafkaConsumer\tinstance\tsubscribe(Collection topics)\n"
                + "method\torg.apache.kafka.clients.consumer.KafkaConsumer\tinstance\tsubscribe(Pattern pattern)\n"));

    [Fact]
    public Task EqualityOperators_OnATypeThatOverridesEquals_AreDotNetsEquals() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public readonly struct Uuid : System.IEquatable<Uuid>
                {
                    public bool Equals(Uuid other) => true;

                    public override bool Equals(object obj) => false;

                    public override int GetHashCode() => 0;

                    public static bool operator ==(Uuid left, Uuid right) => left.Equals(right);

                    public static bool operator !=(Uuid left, Uuid right) => !left.Equals(right);
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.Uuid\tclass\t-\n"
                + "method\torg.apache.kafka.common.Uuid\tinstance\tequals(Object o)\n"
                + "method\torg.apache.kafka.common.Uuid\tinstance\thashCode()\n"));

    [Fact]
    public Task APairedType_StandsForTheNestedJavaTypeItNames() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common.Config
            {
                public enum {|#0:ConfigResourceType|}
                {
                    BROKER,
                }

                public sealed class ConfigResource
                {
                    public ConfigResourceType Type() => default;
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.config.ConfigResource\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.config.ConfigResource\tConfigResource()\n"
                + "method\torg.apache.kafka.common.config.ConfigResource\tinstance\ttype()\n"
                + "type\torg.apache.kafka.common.config.ConfigResource$Type\tenum\t-\n"
                + "constant\torg.apache.kafka.common.config.ConfigResource$Type\tBROKER\n",
                pairs: "Confluent.Kafka.Common.Config.ConfigResourceType org.apache.kafka.common.config.ConfigResource$Type\n"),
            Name(0, "unmatched T:Confluent.Kafka.Common.Config.ConfigResourceType: Java has no type of this name; its members "
                + "are compared with org.apache.kafka.common.config.ConfigResource$Type"));

    [Fact]
    public Task ARenamedParameter_IsMatchedOnlyWhereItsTypeFits() =>
        Verifier.VerifyWithFilesAsync(
            """
            using System.Collections.Generic;

            namespace Confluent.Kafka.Consumer
            {
                public interface IConsumerRebalanceListener
                {
                }

                public interface IAsyncConsumerRebalanceListener
                {
                }

                public sealed class {|#2:KafkaConsumer|}
                {
                    public void {|#0:Subscribe|}(IReadOnlyCollection<string> topics, IConsumerRebalanceListener listener)
                    {
                    }

                    public void {|#1:Subscribe|}(IReadOnlyCollection<string> topics, IAsyncConsumerRebalanceListener listener)
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.consumer.ConsumerRebalanceListener\tinterface\t-\n"
                + "type\torg.apache.kafka.clients.consumer.KafkaConsumer\tclass\t-\n"
                + "ctor\torg.apache.kafka.clients.consumer.KafkaConsumer\tKafkaConsumer()\n"
                + "method\torg.apache.kafka.clients.consumer.KafkaConsumer\tinstance\t"
                + "subscribe(Collection topics, ConsumerRebalanceListener callback)\n"
                + "method\torg.apache.kafka.clients.consumer.KafkaConsumer\tinstance\t"
                + "subscribe(SubscriptionPattern pattern, ConsumerRebalanceListener callback)\n"),
            Name(0, "parameter M:Confluent.Kafka.Consumer.KafkaConsumer.Subscribe(System.Collections.Generic.IReadOnlyCollection{System.String},"
                + "Confluent.Kafka.Consumer.IConsumerRebalanceListener): names its parameters (topics, listener) where Java's "
                + "KafkaConsumer.subscribe names them (topics, callback)"),
            Name(1, "overload M:Confluent.Kafka.Consumer.KafkaConsumer.Subscribe(System.Collections.Generic.IReadOnlyCollection{System.String},"
                + "Confluent.Kafka.Consumer.IAsyncConsumerRebalanceListener): Java's KafkaConsumer.subscribe has no overload with 2 parameter(s)"),
            Name(2, "missing T:Confluent.Kafka.Consumer.KafkaConsumer "
                + "org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(SubscriptionPattern,ConsumerRebalanceListener): has no "
                + "counterpart of Java's org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(SubscriptionPattern,ConsumerRebalanceListener)"));

    [Fact]
    public Task AProtectedConstructor_CanStandForAProtectedJavaOne() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common.Errors
            {
                public abstract class InvalidMetadataException : System.Exception
                {
                    protected InvalidMetadataException()
                    {
                    }

                    protected InvalidMetadataException(string message)
                        : base(message)
                    {
                    }
                }
            }
            """,
            Files("type\torg.apache.kafka.common.errors.InvalidMetadataException\tabstract class\tRuntimeException\n"));

    [Fact]
    public Task AJavaSupertypesMembers_AreRequired_WhenTheDotNetTypeDoesNotDeriveFromItsCounterpart() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Admin
            {
                public abstract class AbstractOptions
                {
                    protected AbstractOptions()
                    {
                    }

                    public int TimeoutMs() => 0;
                }

                public sealed class {|#0:CreateTopicsOptions|}
                {
                    public bool ValidateOnly() => false;
                }

                public sealed class DeleteTopicsOptions : AbstractOptions
                {
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.admin.AbstractOptions\tabstract class\t-\n"
                + "ctor\torg.apache.kafka.clients.admin.AbstractOptions\tAbstractOptions()\n"
                + "method\torg.apache.kafka.clients.admin.AbstractOptions\tinstance\ttimeoutMs()\n"
                + "type\torg.apache.kafka.clients.admin.CreateTopicsOptions\tclass\torg.apache.kafka.clients.admin.AbstractOptions\n"
                + "ctor\torg.apache.kafka.clients.admin.CreateTopicsOptions\tCreateTopicsOptions()\n"
                + "method\torg.apache.kafka.clients.admin.CreateTopicsOptions\tinstance\tvalidateOnly()\n"
                + "type\torg.apache.kafka.clients.admin.DeleteTopicsOptions\tclass\torg.apache.kafka.clients.admin.AbstractOptions\n"
                + "ctor\torg.apache.kafka.clients.admin.DeleteTopicsOptions\tDeleteTopicsOptions()\n"),
            Name(0, "missing T:Confluent.Kafka.Admin.CreateTopicsOptions org.apache.kafka.clients.admin.AbstractOptions#timeoutMs(): "
                + "has no counterpart of Java's org.apache.kafka.clients.admin.AbstractOptions#timeoutMs()"));

    [Fact]
    public Task AStaticMethod_DoesNotStandForAJavaInstanceMethod_NorTheOtherWay() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class {|#2:Node|}
                {
                    public static string {|#0:Host|}() => "";

                    public Node {|#1:NoNode|}() => null;
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.Node\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.Node\tNode()\n"
                + "method\torg.apache.kafka.common.Node\tinstance\thost()\n"
                + "method\torg.apache.kafka.common.Node\tstatic\tnoNode()\n"),
            Name(0, "overload M:Confluent.Kafka.Common.Node.Host: Java's Node.host has no overload with 0 parameter(s)"),
            Name(1, "overload M:Confluent.Kafka.Common.Node.NoNode: Java's Node.noNode has no overload with 0 parameter(s)"),
            Name(2, "missing T:Confluent.Kafka.Common.Node org.apache.kafka.common.Node#host(): has no counterpart of Java's "
                + "org.apache.kafka.common.Node#host()"),
            Name(2, "missing T:Confluent.Kafka.Common.Node org.apache.kafka.common.Node#noNode(): has no counterpart of Java's "
                + "org.apache.kafka.common.Node#noNode()"));

    [Fact]
    public Task OverloadsDifferingOnlyByATypeParameterOrARef_AreTwoOverloads() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class Node
                {
                    public string Host() => "";

                    public T {|#0:Host|}<T>() => default;

                    public int Port(int id) => 0;

                    public int {|#1:Port|}(ref int id) => 0;
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.Node\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.Node\tNode()\n"
                + "method\torg.apache.kafka.common.Node\tinstance\thost()\n"
                + "method\torg.apache.kafka.common.Node\tinstance\tport(int id)\n"),
            Name(0, "overload M:Confluent.Kafka.Common.Node.Host``1: Java's Node.host has no overload with 0 parameter(s)"),
            Name(1, "overload M:Confluent.Kafka.Common.Node.Port(System.Int32@): Java's Node.port has no overload with 1 parameter(s)"));

    [Fact]
    public Task AnOverloadWithJavasNames_DoesNotStandForOneOfOtherTypes() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class {|#0:Node|}
                {
                    public Node(int id)
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.Node\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.Node\tNode(int id)\n"
                + "ctor\torg.apache.kafka.common.Node\tNode(String id)\n"),
            Name(0, "missing T:Confluent.Kafka.Common.Node org.apache.kafka.common.Node#Node(String): has no counterpart of Java's "
                + "org.apache.kafka.common.Node#Node(String)"));

    [Fact]
    public Task AnImplementationThatRenamesItsInterfacesParameter_IsOneOverload_WithAParameterFinding() =>
        Verifier.VerifyWithFilesAsync(
            """
            using System.Collections.Generic;

            namespace Confluent.Kafka.Consumer
            {
                public interface IConsumer
                {
                    void Subscribe(IReadOnlyCollection<string> topics, object callback);
                }

                public sealed class KafkaConsumer : IConsumer
                {
                    public void {|#0:Subscribe|}(IReadOnlyCollection<string> topics, object listener)
                    {
                    }
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.consumer.Consumer\tinterface\t-\n"
                + "method\torg.apache.kafka.clients.consumer.Consumer\tinstance\tsubscribe(Collection topics, ConsumerRebalanceListener callback)\n"
                + "type\torg.apache.kafka.clients.consumer.KafkaConsumer\tclass\torg.apache.kafka.clients.consumer.Consumer\n"
                + "ctor\torg.apache.kafka.clients.consumer.KafkaConsumer\tKafkaConsumer()\n"
                + "method\torg.apache.kafka.clients.consumer.KafkaConsumer\tinstance\t"
                + "subscribe(Collection topics, ConsumerRebalanceListener listener)\n"),
            Name(0, "parameter M:Confluent.Kafka.Consumer.KafkaConsumer.Subscribe(System.Collections.Generic.IReadOnlyCollection{System.String},"
                + "System.Object): names its parameters (topics, listener) where Java's KafkaConsumer.subscribe names them (topics, callback)"));

    [Fact]
    public Task AShorterDotNetOverload_IsComparedWithALongerJavaOneWithoutFailing() =>
        Verifier.VerifyWithFilesAsync(
            """
            namespace Confluent.Kafka.Common
            {
                public sealed class {|#0:Node|}
                {
                    public int Port(int id) => 0;
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.common.Node\tclass\t-\n"
                + "ctor\torg.apache.kafka.common.Node\tNode()\n"
                + "method\torg.apache.kafka.common.Node\tinstance\tport(int id, int fallback)\n"
                + "method\torg.apache.kafka.common.Node\tinstance\tport(int id)\n"),
            Name(0, "missing T:Confluent.Kafka.Common.Node org.apache.kafka.common.Node#port(int,int): has no counterpart of "
                + "Java's org.apache.kafka.common.Node#port(int,int)"));

    [Fact]
    public Task AJavaOverloadDifferingOnlyByAnOptional_IsTheSameOverload_AndTheOptionalTypesFitTheirNullables() =>
        Verifier.VerifyWithFilesAsync(
            """
            using System;
            using System.Collections.Generic;

            namespace Confluent.Kafka.Admin
            {
                public sealed class ProducerState
                {
                    public ProducerState(long producerId, int? coordinatorEpoch, long? lastTimestamp)
                    {
                    }

                    public int Reassignments(IReadOnlyCollection<int>? partitions = null, object? options = null) => 0;

                    public int Deserialize(string topic, ReadOnlySpan<byte> data) => 0;
                }
            }
            """,
            Files(
                "type\torg.apache.kafka.clients.admin.ProducerState\tclass\t-\n"
                + "ctor\torg.apache.kafka.clients.admin.ProducerState\t"
                + "ProducerState(long producerId, OptionalInt coordinatorEpoch, OptionalLong lastTimestamp)\n"
                + "method\torg.apache.kafka.clients.admin.ProducerState\tinstance\treassignments(Optional partitions, Options options)\n"
                + "method\torg.apache.kafka.clients.admin.ProducerState\tinstance\treassignments(Set partitions, Options options)\n"
                + "method\torg.apache.kafka.clients.admin.ProducerState\tinstance\tdeserialize(String topic, byte[] data)\n"));

    private const string Records = """
        using System.Collections;
        using System.Collections.Generic;

        namespace Confluent.Kafka.Consumer
        {
            public sealed class ConsumerRecords : IEnumerable<int>
            {
                public int {|#0:Count|} => 0;

                public IEnumerator<int> {|#1:GetEnumerator|}() => null;

                IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
            }
        }
        """;

    private const string HostProperty = """
        namespace Confluent.Kafka.Common
        {
            public sealed class Node
            {
                public string Host => "";
            }
        }
        """;

    private static (string Name, string Content)[] Files(
        string api,
        string? baseline = null,
        string? allowlist = null,
        string? pairs = null)
    {
        List<(string Name, string Content)> files = new List<(string Name, string Content)> { (JavaNameAnalyzer.JavaApiFile, api) };
        if (baseline is not null)
        {
            files.Add((JavaNameAnalyzer.BaselineFile, baseline));
        }

        if (allowlist is not null)
        {
            files.Add((JavaNameAnalyzer.AllowlistFile, allowlist));
        }

        if (pairs is not null)
        {
            files.Add((JavaNameAnalyzer.PairsFile, pairs));
        }

        return files.ToArray();
    }

    private static DiagnosticResult Name(int location, string message) =>
        Verifier.Diagnostic(DiagnosticIds.JavaName).WithLocation(location).WithMessage(message);

    private static DiagnosticResult Stale(string file, int line, string entry) =>
        Verifier.Diagnostic(DiagnosticIds.StaleJavaNameLine)
            .WithSpan(file, line, 1, line, entry.Length + 1)
            .WithMessage($"'{entry}' in {file} matches nothing; delete it");
}
