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

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// Entry point for the .NET gRPC backend used by the Rust multilanguage integration-test
/// harness. Hosts BOTH a producer and a consumer servicer on Kestrel serving h2c (HTTP/2
/// cleartext, no TLS) — the Rust client dials <c>http://</c>.
/// </summary>
/// <remarks>
/// <b>Flavor selector (M8/P2; producer M12/P1).</b> <c>CONSUMER_FLAVOR=async</c> hosts the
/// asynchronous servicers (<see cref="AsyncProducerServiceImpl"/> over <c>AsyncKafkaProducer</c>
/// + <see cref="AsyncConsumerServiceImpl"/> over <c>AsyncKafkaConsumer</c>); anything else
/// (including unset, the sync default) hosts the synchronous servicers
/// (<see cref="ProducerServiceImpl"/> + <see cref="ConsumerServiceImpl"/>). Each image bakes its
/// flavor via <c>ENV CONSUMER_FLAVOR</c> (Dockerfile.grpc = <c>sync</c>, Dockerfile.grpc.async =
/// <c>async</c>), mirroring the <c>python</c> / <c>python_async</c> image pair — the harness
/// injects no env. One server per flavor hosts both services (Python-parity — <c>grpc_server.py</c>
/// registers both); the env name stays <c>CONSUMER_FLAVOR</c> to avoid Dockerfile churn.
/// </remarks>
internal static class Program
{
    private const int DefaultPort = 50053;

    internal static void Main(string[] args)
    {
        int port = ResolvePort();
        bool useAsync = ResolveAsyncFlavor();

        WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

        // h2c: no TLS. Grpc.AspNetCore defaults to negotiating HTTP/2 over TLS, so the
        // endpoint's protocol is pinned to HTTP/2 without HTTPS to match the harness'
        // plaintext http:// dial (backend_pool.rs Endpoint::from_shared).
        builder.WebHost.ConfigureKestrel(options =>
        {
            options.ListenAnyIP(port, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
        });

        builder.Services.AddGrpc();

        // Each servicer MUST be a singleton: it owns the id -> producer/consumer map that every
        // RPC shares (a CreateProducer/CreateConsumer id must be resolvable by the following
        // Send/Poll/... calls). ASP.NET Core gRPC otherwise activates a fresh servicer per
        // request, so the map would be empty on every call after Create* (Python registers one
        // servicer instance per service — grpc_server.py). Registering them here makes
        // MapGrpcService resolve those single instances. The CONSUMER_FLAVOR selector (see the
        // type remarks) picks the sync or async servicers; both flavors host BOTH services.
        if (useAsync)
        {
            builder.Services.AddSingleton<AsyncProducerServiceImpl>();
            builder.Services.AddSingleton<AsyncConsumerServiceImpl>();
        }
        else
        {
            builder.Services.AddSingleton<ProducerServiceImpl>();
            builder.Services.AddSingleton<ConsumerServiceImpl>();
        }

        WebApplication app = builder.Build();
        if (useAsync)
        {
            app.MapGrpcService<AsyncProducerServiceImpl>();
            app.MapGrpcService<AsyncConsumerServiceImpl>();
        }
        else
        {
            app.MapGrpcService<ProducerServiceImpl>();
            app.MapGrpcService<ConsumerServiceImpl>();
        }

        app.Start();

        // The Rust BackendPool waits for the substring "listening" on STDERR before
        // connecting (backend_pool.rs WaitFor::message_on_stderr). Kestrel logs its own
        // "Now listening on ..." to STDOUT via ILogger, which the harness does not watch —
        // so emit our own line to STDERR after startup. Keep this line + stream stable.
        Console.Error.WriteLine($"listening on 0.0.0.0:{port}");
        Console.Error.Flush();

        try
        {
            app.WaitForShutdown();
        }
        finally
        {
            // Registry drain (M9/P4 M5). The servicers hold a consumer_id -> consumer map that
            // only the Close RPC ever empties, so any scenario that skips Close leaves a live
            // native consumer (tokio runtime + ConsumerNetworkThread + dispatcher thread) in
            // it — and this backend process is SHARED across scenarios, so they accumulate.
            //
            // Resolved and disposed EXPLICITLY rather than left to DI: this host is torn down
            // by WaitForShutdown() alone, with no app.Dispose()/DisposeAsync(), so DI has
            // nothing to call. Implementing IDisposable on the servicers without a path that
            // actually invokes it would fix nothing (plan §8.2). In a `finally` so the sweep
            // still runs if WaitForShutdown throws.
            DrainServicer(app, useAsync);
        }
    }

    /// <summary>
    /// Disposes the singleton servicer so its consumer registry is drained at shutdown
    /// (M9/P4 M5). Resolves from the host's own service provider — the same singleton every RPC
    /// used. Best-effort: a shutdown-time failure must not turn a passing harness run into a
    /// non-zero exit, so it is logged to STDERR and swallowed.
    /// </summary>
    /// <remarks>
    /// Uses the <b>synchronous</b> <see cref="IDisposable.Dispose"/> on both flavors, including
    /// the async servicer (which implements both). <see cref="Main"/> is synchronous, so
    /// awaiting <c>DisposeAsync</c> here would mean
    /// <c>.AsTask().GetAwaiter().GetResult()</c> — the sync-over-async footgun
    /// ffi-marshalling.md §B7 forbids. The async servicer's <c>Dispose</c> is the documented
    /// blocking fallback and drains the same registry via each consumer's blocking
    /// <c>Dispose</c>.
    /// </remarks>
    private static void DrainServicer(WebApplication app, bool useAsync)
    {
        try
        {
            if (useAsync)
            {
                app.Services.GetRequiredService<AsyncConsumerServiceImpl>().Dispose();
            }
            else
            {
                app.Services.GetRequiredService<ConsumerServiceImpl>().Dispose();
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"shutdown drain failed: {ex}");
        }
    }

    private static int ResolvePort()
    {
        string? value = Environment.GetEnvironmentVariable("GRPC_PORT");
        if (!string.IsNullOrEmpty(value) && int.TryParse(value, out int parsed))
        {
            return parsed;
        }

        return DefaultPort;
    }

    /// <summary>
    /// Reads <c>CONSUMER_FLAVOR</c>: <c>async</c> (case-insensitive) selects the async
    /// servicer; anything else (including unset) selects the sync servicer (the M8/P1
    /// default). The flavor is baked per image, not injected by the harness (PLAN §4).
    /// </summary>
    private static bool ResolveAsyncFlavor()
    {
        string? value = Environment.GetEnvironmentVariable("CONSUMER_FLAVOR");
        return string.Equals(value, "async", StringComparison.OrdinalIgnoreCase);
    }
}
