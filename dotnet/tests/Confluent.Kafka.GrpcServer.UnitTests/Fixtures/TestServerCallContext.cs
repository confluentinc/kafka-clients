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
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// A hand-written <see cref="ServerCallContext"/> for driving a servicer method directly (PLAN
/// §7.1): it records response-header writes into the call's <see cref="RecordingStreamWriter"/>
/// and exposes a cancellable <see cref="ServerCallContext.CancellationToken"/>, standing in for
/// the harness going away (T17).
/// </summary>
internal sealed class TestServerCallContext : ServerCallContext, IDisposable
{
    private readonly RecordingStreamWriter _stream;
    private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();

    internal TestServerCallContext(RecordingStreamWriter stream)
    {
        _stream = stream;
    }

    /// <inheritdoc/>
    protected override string MethodCore => "/confluent.kafka.test.ChaosWorkloadService/Run";

    /// <inheritdoc/>
    protected override string HostCore => "127.0.0.1";

    /// <inheritdoc/>
    protected override string PeerCore => "ipv4:127.0.0.1:1";

    /// <inheritdoc/>
    protected override DateTime DeadlineCore => DateTime.MaxValue;

    /// <inheritdoc/>
    protected override Metadata RequestHeadersCore { get; } = new Metadata();

    /// <inheritdoc/>
    protected override CancellationToken CancellationTokenCore => _cancellation.Token;

    /// <inheritdoc/>
    protected override Metadata ResponseTrailersCore { get; } = new Metadata();

    /// <inheritdoc/>
    protected override Status StatusCore { get; set; }

    /// <inheritdoc/>
    protected override WriteOptions? WriteOptionsCore { get; set; }

    /// <inheritdoc/>
    protected override AuthContext AuthContextCore { get; } =
        new AuthContext(null, new Dictionary<string, List<AuthProperty>>());

    /// <summary>Cancels the call, as the harness dropping the RPC would.</summary>
    internal void Cancel() => _cancellation.Cancel();

    /// <inheritdoc/>
    public void Dispose() => _cancellation.Dispose();

    /// <inheritdoc/>
    protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
        throw new NotSupportedException("the chaos servicer never propagates its call context");

    /// <inheritdoc/>
    protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders)
    {
        _stream.RecordHeaders();
        return Task.CompletedTask;
    }
}
