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
/// Entry point for the .NET consumer gRPC backend used by the Rust multilanguage
/// integration-test harness. Hosts a consumer servicer on Kestrel serving h2c
/// (HTTP/2 cleartext, no TLS) — the Rust client dials <c>http://</c>.
/// </summary>
/// <remarks>
/// <b>Flavor selector (M8/P2).</b> <c>CONSUMER_FLAVOR=async</c> hosts the asynchronous
/// <see cref="AsyncConsumerServiceImpl"/> (over <c>AsyncKafkaConsumer</c>); anything else
/// (including unset, the sync default) hosts the synchronous <see cref="ConsumerServiceImpl"/>
/// (M8/P1). Each image bakes its flavor via <c>ENV CONSUMER_FLAVOR</c> (Dockerfile.grpc =
/// <c>sync</c>, Dockerfile.grpc.async = <c>async</c>), mirroring the <c>python</c> /
/// <c>python_async</c> image pair — the harness injects no env.
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

        // The servicer MUST be a singleton: it owns the id -> consumer map that every RPC
        // shares (a CreateConsumer id must be resolvable by the following Subscribe / Poll /
        // ...). ASP.NET Core gRPC otherwise activates a fresh servicer per request, so the
        // map would be empty on every call after CreateConsumer (Python registers one
        // servicer instance — grpc_server.py). Registering it here makes MapGrpcService
        // resolve that single instance. The CONSUMER_FLAVOR selector (see the type remarks)
        // picks the sync or async servicer; both own the same shape of id -> consumer map.
        if (useAsync)
        {
            builder.Services.AddSingleton<AsyncConsumerServiceImpl>();
        }
        else
        {
            builder.Services.AddSingleton<ConsumerServiceImpl>();
        }

        WebApplication app = builder.Build();
        if (useAsync)
        {
            app.MapGrpcService<AsyncConsumerServiceImpl>();
        }
        else
        {
            app.MapGrpcService<ConsumerServiceImpl>();
        }

        app.Start();

        // The Rust BackendPool waits for the substring "listening" on STDERR before
        // connecting (backend_pool.rs WaitFor::message_on_stderr). Kestrel logs its own
        // "Now listening on ..." to STDOUT via ILogger, which the harness does not watch —
        // so emit our own line to STDERR after startup. Keep this line + stream stable.
        Console.Error.WriteLine($"listening on 0.0.0.0:{port}");
        Console.Error.Flush();

        app.WaitForShutdown();
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
