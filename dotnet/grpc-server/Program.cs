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
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Confluent.Kafka.GrpcServer;

/// <summary>
/// Entry point for the .NET gRPC backend used by the Rust multilanguage integration-test
/// harness. Hosts a producer and a consumer servicer (plus the admin servicer, sync flavor
/// only — M15/P12 D1) on Kestrel serving h2c (HTTP/2 cleartext, no TLS) — the Rust client
/// dials <c>http://</c>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Flavor selector (M8/P2; producer M12/P1).</b> <c>CONSUMER_FLAVOR=async</c> hosts the
/// asynchronous servicers (<see cref="AsyncProducerServiceImpl"/> over <c>AsyncKafkaProducer</c>
/// + <see cref="AsyncConsumerServiceImpl"/> over <c>AsyncKafkaConsumer</c>); anything else
/// (including unset, the sync default) hosts the synchronous servicers
/// (<see cref="ProducerServiceImpl"/> + <see cref="ConsumerServiceImpl"/> +
/// <see cref="AdminServiceImpl"/>; admin is sync-only, M15/P12 D1). Each image bakes its
/// flavor via <c>ENV CONSUMER_FLAVOR</c> (Dockerfile.grpc = <c>sync</c>, Dockerfile.grpc.async =
/// <c>async</c>), mirroring the <c>python</c> / <c>python_async</c> image pair; in native mode
/// the harness sets it explicitly for each backend kind instead (backend_pool.rs
/// <c>native_command</c>). One server per flavor hosts the producer and consumer services
/// together (Python-parity — <c>grpc_server.py</c> registers them in one server; Python also
/// registers admin in its async server, where .NET does not); the env name stays
/// <c>CONSUMER_FLAVOR</c> to avoid Dockerfile churn.
/// </para>
/// <para>
/// <b>Listen address and readiness line (M17/P1)</b> — the same contract as the Python and C
/// servers. The server binds <c>GRPC_HOST:GRPC_PORT</c>. <c>GRPC_HOST</c> defaults to
/// <c>127.0.0.1</c> because the server is unauthenticated; it accepts an IP literal
/// (<c>0.0.0.0</c> binds every IPv4 interface) or <c>localhost</c> (the IPv4 loopback). Both
/// images set <c>GRPC_HOST=0.0.0.0</c> so the server is reachable through the container's
/// published port. <c>GRPC_PORT</c> defaults to 50053 and must be an integer in 0–65535; 0 binds
/// an ephemeral port, which is how the harness's native mode starts it. An invalid value of
/// either is reported on STDERR and the process exits non-zero before binding. After startup the
/// server writes <c>listening on {host}:{port}</c> to STDERR, where <c>port</c> is the port
/// Kestrel actually bound (not the requested 0). The harness waits for that line and parses the
/// port from its last <c>:</c>-separated field (backend_pool.rs <c>parse_listening_port</c>),
/// so keep its format and stream stable.
/// </para>
/// </remarks>
internal static class Program
{
    private const string DefaultHost = "127.0.0.1";
    private const int DefaultPort = 50053;

    /// <summary>
    /// Validates <c>GRPC_HOST</c> / <c>GRPC_PORT</c>, binds, reports the bound port on STDERR
    /// (see the type remarks for the contract), then serves until shutdown.
    /// </summary>
    /// <returns>0 after a clean shutdown; 1 when <c>GRPC_HOST</c> or <c>GRPC_PORT</c> is
    /// invalid, or when the bound port cannot be read after startup.</returns>
    internal static int Main(string[] args)
    {
        if (!TryResolveHost(out string host, out IPAddress? address) || !TryResolvePort(out int port))
        {
            return 1;
        }

        bool useAsync = ResolveAsyncFlavor();

        WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

        // h2c: no TLS. Grpc.AspNetCore defaults to negotiating HTTP/2 over TLS, so the
        // endpoint's protocol is pinned to HTTP/2 without HTTPS to match the harness'
        // plaintext http:// dial (backend_pool.rs Endpoint::from_shared).
        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(address, port, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
        });

        builder.Services.AddGrpc();

        // Each servicer MUST be a singleton: it owns the id -> producer/consumer map that every
        // RPC shares (a CreateProducer/CreateConsumer id must be resolvable by the following
        // Send/Poll/... calls). ASP.NET Core gRPC otherwise activates a fresh servicer per
        // request, so the map would be empty on every call after Create* (Python registers one
        // servicer instance per service — grpc_server.py). Registering them here makes
        // MapGrpcService resolve those single instances. The CONSUMER_FLAVOR selector (see the
        // type remarks) picks the sync or async servicers; both flavors host the producer and
        // consumer services, and only the sync flavor hosts admin.
        if (useAsync)
        {
            builder.Services.AddSingleton<AsyncProducerServiceImpl>();
            builder.Services.AddSingleton<AsyncConsumerServiceImpl>();
        }
        else
        {
            builder.Services.AddSingleton<ProducerServiceImpl>();
            builder.Services.AddSingleton<ConsumerServiceImpl>();

            // Admin is registered in the SYNC branch ONLY (M15/P12 D1): .NET has no
            // IAsyncAdmin, so an async arm would drive the same AdminServiceImpl over the
            // same single IAdmin — byte-identical managed code for zero extra coverage.
            builder.Services.AddSingleton<AdminServiceImpl>();
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
            app.MapGrpcService<AdminServiceImpl>();
        }

        app.Start();

        // The Rust BackendPool waits for a "listening" line on STDERR and parses the bound port
        // from its last ':'-separated field (backend_pool.rs start_native / parse_listening_port;
        // the container mode waits for the same substring). Kestrel logs its own
        // "Now listening on ..." to STDOUT via ILogger, which the harness does not use, so emit
        // our own line to STDERR after startup. The port is read back from the server rather
        // than echoed from GRPC_PORT, because GRPC_PORT=0 asks Kestrel for an ephemeral port.
        // Keep this line + stream stable.
        int boundPort = ResolveBoundPort(app);
        if (boundPort == 0)
        {
            // Unreachable while Kestrel reports its bound endpoints: a failed bind makes Start()
            // throw instead. Shut down cleanly (the drain below still runs) and fail, as the C
            // server does when its selected port stays 0.
            Console.Error.WriteLine($"dotnet server: could not determine the port bound for {host}:{port}");
            app.Lifetime.StopApplication();
        }
        else
        {
            Console.Error.WriteLine($"listening on {host}:{boundPort}");
            Console.Error.Flush();
        }

        try
        {
            app.WaitForShutdown();
        }
        finally
        {
            // Registry drain (M9/P4 M5; producers added M11/P8). The servicers hold a
            // consumer_id -> consumer and a producer_id -> producer map that only the Close RPC
            // ever empties, so any scenario that skips Close leaves a live native client (a
            // consumer's tokio runtime + ConsumerNetworkThread + dispatcher thread; a producer's
            // runtime + Sender task + send-pump thread) in it — and this backend process is SHARED
            // across scenarios, so they accumulate.
            //
            // Resolved and disposed EXPLICITLY rather than left to DI: this host is torn down
            // by WaitForShutdown() alone, with no app.Dispose()/DisposeAsync(), so DI has
            // nothing to call. Implementing IDisposable on the servicers without a path that
            // actually invokes it would fix nothing (plan §8.2). In a `finally` so the sweep
            // still runs if WaitForShutdown throws.
            DrainServicer(app, useAsync);
        }

        return boundPort == 0 ? 1 : 0;
    }

    /// <summary>
    /// Disposes the singleton servicers so their consumer AND producer registries are drained at
    /// shutdown (M9/P4 M5; producers added in M11/P8, Minor 14). Resolves from the host's own
    /// service provider — the same singletons every RPC used. Best-effort: a shutdown-time failure
    /// must not turn a passing harness run into a non-zero exit, so it is logged to STDERR and
    /// swallowed.
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
                app.Services.GetRequiredService<AsyncProducerServiceImpl>().Dispose();
            }
            else
            {
                app.Services.GetRequiredService<ConsumerServiceImpl>().Dispose();
                app.Services.GetRequiredService<ProducerServiceImpl>().Dispose();
                app.Services.GetRequiredService<AdminServiceImpl>().Dispose();
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"shutdown drain failed: {ex}");
        }
    }

    /// <summary>
    /// Reads <c>GRPC_HOST</c>: unset selects <see cref="DefaultHost"/>; <c>localhost</c> selects
    /// the IPv4 loopback; anything else must be an IP literal. An invalid value is reported on
    /// STDERR and returns <see langword="false"/>, so the server exits non-zero before binding
    /// (the C server's GRPC_PORT check, applied to the host too).
    /// </summary>
    private static bool TryResolveHost(out string host, [NotNullWhen(true)] out IPAddress? address)
    {
        host = Environment.GetEnvironmentVariable("GRPC_HOST") ?? DefaultHost;
        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            address = IPAddress.Loopback;
            return true;
        }

        if (IPAddress.TryParse(host, out address))
        {
            return true;
        }

        Console.Error.WriteLine(
            $"dotnet server: invalid GRPC_HOST '{host}': expected an IP literal (0.0.0.0 for every interface) or 'localhost'");
        return false;
    }

    /// <summary>
    /// Reads <c>GRPC_PORT</c>: unset selects <see cref="DefaultPort"/>; otherwise it must be a
    /// plain decimal integer in 0–65535, where 0 asks for an ephemeral port. An invalid value
    /// (including an empty one) is reported on STDERR and returns <see langword="false"/>, so the
    /// server exits non-zero instead of silently binding the default — the C server's
    /// <c>strtol</c> check and the Python server's <c>int()</c>.
    /// </summary>
    private static bool TryResolvePort(out int port)
    {
        string? value = Environment.GetEnvironmentVariable("GRPC_PORT");
        if (value is null)
        {
            port = DefaultPort;
            return true;
        }

        if (int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out port)
            && port <= IPEndPoint.MaxPort)
        {
            return true;
        }

        Console.Error.WriteLine($"dotnet server: invalid GRPC_PORT '{value}': expected an integer in 0-65535");
        return false;
    }

    /// <summary>
    /// The port Kestrel actually bound, read from the server's addresses feature
    /// (<see cref="WebApplication.Urls"/>) after <c>Start()</c>; 0 when none is reported. With
    /// <c>GRPC_PORT=0</c> this is the ephemeral port the OS assigned.
    /// </summary>
    private static int ResolveBoundPort(WebApplication app)
    {
        foreach (string url in app.Urls)
        {
            if (Uri.TryCreate(url, UriKind.Absolute, out Uri? uri) && uri.Port > 0)
            {
                return uri.Port;
            }
        }

        return 0;
    }

    /// <summary>
    /// Reads <c>CONSUMER_FLAVOR</c>: <c>async</c> (case-insensitive) selects the async
    /// servicers; anything else (including unset) selects the sync servicers (the M8/P1
    /// default). Container mode: the flavor is baked per image (<c>ENV CONSUMER_FLAVOR</c> in
    /// Dockerfile.grpc / Dockerfile.grpc.async). Native mode (M17/P1): the harness sets it
    /// explicitly per backend kind (<c>rust/tests/common/backend_pool.rs</c> <c>native_command</c>).
    /// </summary>
    private static bool ResolveAsyncFlavor()
    {
        string? value = Environment.GetEnvironmentVariable("CONSUMER_FLAVOR");
        return string.Equals(value, "async", StringComparison.OrdinalIgnoreCase);
    }
}
