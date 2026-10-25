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
/// Entry point of the .NET gRPC test server: hosts the producer, consumer, admin and chaos servicers on
/// <c>GRPC_HOST:GRPC_PORT</c> over h2c, the sync servicers by default and the async ones when
/// <c>CONSUMER_FLAVOR=async</c>. The host defaults to 127.0.0.1 because the server is unauthenticated.
/// </summary>
internal static class Program
{
    private const string DefaultHost = "127.0.0.1";
    private const int DefaultPort = 50053;

    internal static int Main(string[] args)
    {
        if (!TryResolveHost(out string host, out IPAddress? address) || !TryResolvePort(out int port))
        {
            return 1;
        }

        bool useAsync = ResolveAsyncFlavor();

        WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

        // h2c: HTTP/2 without TLS, for the harness's plaintext http:// dial.
        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(address, port, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
        });

        builder.Services.AddGrpc();

        // Singletons: each servicer owns the id map or workload registry that every RPC shares, and
        // ASP.NET Core would otherwise create a servicer per request.
        builder.Services.AddSingleton<GroupMetadataStore>();
        if (useAsync)
        {
            builder.Services.AddSingleton<AsyncProducerServiceImpl>();
            builder.Services.AddSingleton<AsyncConsumerServiceImpl>();

            builder.Services.AddSingleton<AsyncAdminServiceImpl>();

            builder.Services.AddSingleton<AsyncChaosWorkloadServiceImpl>();
        }
        else
        {
            builder.Services.AddSingleton<ProducerServiceImpl>();
            builder.Services.AddSingleton<ConsumerServiceImpl>();

            builder.Services.AddSingleton<AdminServiceImpl>();

            builder.Services.AddSingleton<ChaosWorkloadServiceImpl>();
        }

        WebApplication app = builder.Build();
        if (useAsync)
        {
            app.MapGrpcService<AsyncProducerServiceImpl>();
            app.MapGrpcService<AsyncConsumerServiceImpl>();
            app.MapGrpcService<AsyncAdminServiceImpl>();
            app.MapGrpcService<AsyncChaosWorkloadServiceImpl>();
        }
        else
        {
            app.MapGrpcService<ProducerServiceImpl>();
            app.MapGrpcService<ConsumerServiceImpl>();
            app.MapGrpcService<AdminServiceImpl>();
            app.MapGrpcService<ChaosWorkloadServiceImpl>();
        }

        app.Start();

        // The harness waits for this STDERR line and parses the port from its last ':'-separated
        // field, so keep its format and stream. The port is the one Kestrel bound: GRPC_PORT=0 asks
        // for an ephemeral one.
        int boundPort = ResolveBoundPort(app);
        if (boundPort == 0)
        {
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
            // Disposed explicitly: WaitForShutdown alone tears this host down, so DI never disposes
            // the servicers, and a scenario that skips Close leaves its client in a registry.
            DrainServicer(app, useAsync);
        }

        return boundPort == 0 ? 1 : 0;
    }

    // The sync Dispose on both flavours: Main is synchronous, and awaiting DisposeAsync here would
    // be sync-over-async. A failure is logged, not thrown, so a passing run does not exit non-zero.
    private static void DrainServicer(WebApplication app, bool useAsync)
    {
        try
        {
            if (useAsync)
            {
                app.Services.GetRequiredService<AsyncConsumerServiceImpl>().Dispose();
                app.Services.GetRequiredService<AsyncProducerServiceImpl>().Dispose();
                app.Services.GetRequiredService<AsyncAdminServiceImpl>().Dispose();

                app.Services.GetRequiredService<AsyncChaosWorkloadServiceImpl>().Dispose();
            }
            else
            {
                app.Services.GetRequiredService<ConsumerServiceImpl>().Dispose();
                app.Services.GetRequiredService<ProducerServiceImpl>().Dispose();
                app.Services.GetRequiredService<AdminServiceImpl>().Dispose();

                app.Services.GetRequiredService<ChaosWorkloadServiceImpl>().Dispose();
            }
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"shutdown drain failed: {ex}");
        }
    }

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

    private static bool ResolveAsyncFlavor()
    {
        string? value = Environment.GetEnvironmentVariable("CONSUMER_FLAVOR");
        return string.Equals(value, "async", StringComparison.OrdinalIgnoreCase);
    }
}
