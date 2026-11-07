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
    public Task AnAsyncPeer_IsReported_AndItsMembersAreComparedWithItsJavaClass() =>
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

                    public Task {|#1:Flush|}(CancellationToken cancellationToken = default) => Task.CompletedTask;

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
            Name(0, "unmatched T:Confluent.Kafka.Producer.AsyncKafkaProducer: Java has no type of this name; its members "
                + "are compared with org.apache.kafka.clients.producer.KafkaProducer"),
            Name(0, "missing T:Confluent.Kafka.Producer.AsyncKafkaProducer org.apache.kafka.clients.producer.KafkaProducer#close(): "
                + "has no counterpart of Java's org.apache.kafka.clients.producer.KafkaProducer#close()"),
            Name(1, "parameter M:Confluent.Kafka.Producer.AsyncKafkaProducer.Flush(System.Threading.CancellationToken): adds the "
                + ".NET-only parameter(s) cancellationToken to Java's KafkaProducer.flush()"),
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
