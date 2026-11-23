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

using System.Threading.Tasks;

using Microsoft.CodeAnalysis.Testing;

using Xunit;

using Verifier = Confluent.Kafka.Analyzers.UnitTests.AnalyzerVerifier<Confluent.Kafka.Analyzers.AsyncCancellationTokenAnalyzer>;

namespace Confluent.Kafka.Analyzers.UnitTests;

/// <summary>
/// CKRDA0016 reports a public method returning a Task or ValueTask that does not end with
/// <c>CancellationToken cancellationToken = default</c>, and leaves alone the callbacks, the futures, the members that
/// come from outside the assembly and the members that are not public.
/// </summary>
public sealed class AsyncCancellationTokenAnalyzerTests
{
    [Theory]
    [InlineData("Task Flush(CancellationToken cancellationToken = default);")]
    [InlineData("Task<long> Position(string topic, TimeSpan timeout, CancellationToken cancellationToken = default);")]
    [InlineData("ValueTask Send(byte[] record, CancellationToken cancellationToken = default);")]
    [InlineData("ValueTask<int> Count(CancellationToken cancellationToken = default);")]
    [InlineData("void Wakeup();")]
    [InlineData("int Count();")]
    public Task AnInterfaceMember_ThatTakesTheToken_OrIsNotAsync_IsNotReported(string member) =>
        Verifier.VerifyAsync(Interface(member));

    [Theory]
    [InlineData("Task {|#0:Flush|}();", "IClient.Flush returns a Task")]
    [InlineData("Task<long> {|#0:Position|}(string topic);", "IClient.Position returns a Task")]
    [InlineData("ValueTask {|#0:Send|}(byte[] record);", "IClient.Send returns a ValueTask")]
    [InlineData("ValueTask<int> {|#0:Count|}();", "IClient.Count returns a ValueTask")]
    [InlineData("Task {|#0:Once|}();", "IClient.Once returns a Task")]
    public Task AMemberWithoutTheToken_IsReported(string member, string start) =>
        Verifier.VerifyAsync(
            Interface(member),
            Finding(0, start + "; end it with CancellationToken cancellationToken = default"));

    [Fact]
    public Task ATokenThatIsNotLast_IsReported() =>
        Verifier.VerifyAsync(
            Interface("Task {|#0:Seek|}(string topic, CancellationToken cancellationToken, long offset);"),
            Finding(0, "IClient.Seek: the CancellationToken must be the last parameter"));

    [Fact]
    public Task ATokenOfAnotherName_IsReported() =>
        Verifier.VerifyAsync(
            Interface("Task {|#0:Commit|}(CancellationToken token = default);"),
            Finding(0, "IClient.Commit: name the CancellationToken cancellationToken"));

    [Fact]
    public Task ARequiredToken_IsReported() =>
        Verifier.VerifyAsync(
            Interface("Task {|#0:Commit|}(CancellationToken cancellationToken);"),
            Finding(0, "IClient.Commit: the CancellationToken must be optional: CancellationToken cancellationToken = default"));

    [Fact]
    public Task ARequiredTokenOfAnotherName_IsReportedTwice() =>
        Verifier.VerifyAsync(
            Interface("Task {|#0:Commit|}(CancellationToken token);"),
            Finding(0, "IClient.Commit: name the CancellationToken cancellationToken"),
            Finding(0, "IClient.Commit: the CancellationToken must be optional: CancellationToken cancellationToken = default"));

    [Fact]
    public Task AnImplementation_ThatDropsTheDefault_IsReported() =>
        Verifier.VerifyAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;

            public interface IClient
            {
                Task Flush(CancellationToken cancellationToken = default);
            }

            public sealed class Client<TKey> : IClient
            {
                public Task {|#0:Flush|}(CancellationToken cancellationToken) => Task.CompletedTask;
            }
            """,
            Finding(0, "Client<TKey>.Flush: the CancellationToken must be optional: CancellationToken cancellationToken = default"));

    [Fact]
    public Task AStaticOrProtectedMember_IsChecked() =>
        Verifier.VerifyAsync(
            """
            using System.Threading.Tasks;

            public abstract class Client
            {
                public static Task {|#0:Create|}() => Task.CompletedTask;

                protected abstract Task {|#1:Run|}();
            }
            """,
            Finding(0, "Client.Create returns a Task; end it with CancellationToken cancellationToken = default"),
            Finding(1, "Client.Run returns a Task; end it with CancellationToken cancellationToken = default"));

    [Theory]
    [InlineData("Task OnCompletion(string metadata, Exception? exception);")]
    [InlineData("Task OnPartitionsRevoked(IReadOnlyCollection<string> partitions);")]
    public Task ACallback_IsNotChecked(string member) =>
        Verifier.VerifyAsync(Interface(member));

    [Fact]
    public Task ACallbacksDefaultAsAnExtensionMethod_IsNotChecked() =>
        Verifier.VerifyAsync(
            """
            using System.Collections.Generic;
            using System.Threading.Tasks;

            public interface IListener
            {
                Task OnPartitionsRevoked(IReadOnlyCollection<string> partitions);
            }

            public static class ListenerExtensions
            {
                public static Task OnPartitionsLost(this IListener listener, IReadOnlyCollection<string> partitions) =>
                    listener.OnPartitionsRevoked(partitions);
            }
            """);

    [Fact]
    public Task TheFutures_AreNotChecked() =>
        Verifier.VerifyAsync(
            """
            using System;
            using System.Threading;
            using System.Threading.Tasks;

            namespace Confluent.Kafka.Common
            {
                public readonly struct AsyncKafkaFuture<T>
                {
                    public Task<T> Get() => throw new NotImplementedException();

                    public Task<T> Get(TimeSpan timeout) => throw new NotImplementedException();

                    public Task<T> Get(CancellationToken cancellationToken) => throw new NotImplementedException();

                    public Task<T> ToCompletionStage() => throw new NotImplementedException();
                }

                public readonly struct AsyncKafkaFuture
                {
                    public Task Get() => Task.CompletedTask;
                }
            }
            """);

    [Fact]
    public Task AFutureOfTheSameNameElsewhere_IsChecked() =>
        Verifier.VerifyAsync(
            """
            using System.Threading.Tasks;

            namespace Other
            {
                public sealed class AsyncKafkaFuture
                {
                    public Task {|#0:Get|}() => Task.CompletedTask;
                }
            }
            """,
            Finding(0, "AsyncKafkaFuture.Get returns a Task; end it with CancellationToken cancellationToken = default"));

    [Fact]
    public Task AMemberFromOutsideTheAssembly_IsNotChecked() =>
        Verifier.VerifyAsync(
            """
            using System;
            using System.IO;
            using System.Threading;
            using System.Threading.Tasks;

            public sealed class Client : IAsyncDisposable
            {
                public ValueTask DisposeAsync() => default;
            }

            public sealed class Buffer : MemoryStream
            {
                public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
            }
            """);

    [Fact]
    public Task AMemberThatIsNotPublic_IsNotChecked() =>
        Verifier.VerifyAsync(
            """
            using System.Threading;
            using System.Threading.Tasks;

            public interface IClient
            {
                Task Flush(CancellationToken cancellationToken = default);
            }

            public sealed class Client : IClient
            {
                Task IClient.Flush(CancellationToken cancellationToken) => Task.CompletedTask;

                internal Task Drain() => Task.CompletedTask;

                private Task Pump() => Task.CompletedTask;

                public void Run()
                {
                    static Task Local() => Task.CompletedTask;
                    _ = Local();
                }
            }

            internal sealed class Helper
            {
                public Task Run() => Task.CompletedTask;
            }

            public sealed class Outer
            {
                internal sealed class Inner
                {
                    public Task Run() => Task.CompletedTask;
                }
            }
            """);

    private static DiagnosticResult Finding(int location, string message) =>
        Verifier.Diagnostic(DiagnosticIds.AsyncCancellationToken).WithLocation(location).WithMessage(message);

    private static string Interface(string member) =>
        $$"""
        #nullable enable
        using System;
        using System.Collections.Generic;
        using System.Threading;
        using System.Threading.Tasks;

        public interface IClient
        {
            {{member}}
        }
        """;
}
