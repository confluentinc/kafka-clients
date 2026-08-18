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
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// A single-node KRaft Kafka broker for the in-suite perf smoke — the C# analog of the Python perf
/// suite's <c>conftest.kafka_broker</c> fixture, spun via the <c>Testcontainers</c> .NET NuGet. It runs
/// <c>apache/kafka:4.2.0</c> (matching the Rust integration harness image) with KIP-848
/// (<c>KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer</c>) and two PLAINTEXT listeners:
/// <list type="bullet">
///   <item>EXTERNAL — advertised to the host on a fixed mapped port (the PerfV3 subprocess connects here).</item>
///   <item>INTERNAL — advertised as <c>localhost:9094</c> for clients running inside the container
///     (the consumer smoke's <c>kafka-producer-perf-test.sh</c> load, and topic creation).</item>
/// </list>
/// The broker is started <b>lazily</b> (not in <see cref="InitializeAsync"/>) so a Docker-unavailable env
/// does not fail the fixture — instead <see cref="TryStartAsync"/> returns <see langword="false"/> and each
/// test skips cleanly (a logged pass; xUnit 2.9.x has no dynamic <c>Assert.Skip</c>). Shared across the
/// smoke tests via <see cref="IClassFixture{TFixture}"/> so the broker starts once.
/// </summary>
public sealed class KafkaBrokerFixture : IAsyncLifetime
{
    // Matches bindings/python/test/performance/conftest.py (which matches tests/common/kafka_cluster.rs).
    private const string KafkaImage = "apache/kafka:4.2.0";
    private const string ClusterId = "5L6g3nShT-eMCtK--X86sw";

    /// <summary>Bootstrap for clients running INSIDE the container (topic creation, the in-container load producer).</summary>
    public const string InternalBootstrap = "localhost:9094";

    /// <summary>The Kafka CLI dir inside the container image.</summary>
    public const string KafkaBin = "/opt/kafka/bin";

    private readonly SemaphoreSlim _gate = new(1, 1);
    private IContainer? _container;
    private string? _externalBootstrap;
    private bool _startAttempted;
    private string? _skipReason;

    /// <summary>Bootstrap for clients on the HOST (the PerfV3 subprocess) — set once the broker starts.</summary>
    public string? ExternalBootstrap => _externalBootstrap;

    /// <summary>Lazy start (see the type remarks) — nothing to do here.</summary>
    public Task InitializeAsync() => Task.CompletedTask;

    /// <summary>Stops the broker container if it was started (best-effort).</summary>
    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            try
            {
                await _container.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Best-effort teardown; the Testcontainers resource reaper also reaps it.
            }
        }

        _gate.Dispose();
    }

    /// <summary>
    /// Starts the broker once (cached). Returns <see langword="true"/> when a broker is available, or
    /// <see langword="false"/> — logging a skip notice — when Docker is unavailable or the container fails
    /// to start (mirrors conftest's <c>importorskip</c> + <c>try: start except: skip</c>).
    /// </summary>
    public async Task<bool> TryStartAsync(ITestOutputHelper output)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_startAttempted)
            {
                if (_skipReason is not null)
                {
                    output.WriteLine(_skipReason);
                }

                return _container is not null;
            }

            _startAttempted = true;

            if (!DockerAvailable())
            {
                _skipReason = "SKIP: Docker is not available; the in-suite perf smoke needs Docker (CI-only in this env).";
                output.WriteLine(_skipReason);
                return false;
            }

            int hostPort = FreePort();
            IContainer container = new ContainerBuilder(KafkaImage)
                .WithPortBinding(hostPort, 9092)
                .WithEnvironment("CLUSTER_ID", ClusterId)
                .WithEnvironment("KAFKA_NODE_ID", "1")
                .WithEnvironment("KAFKA_PROCESS_ROLES", "broker,controller")
                .WithEnvironment("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
                .WithEnvironment("KAFKA_LISTENERS", "EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:9094,CONTROLLER://0.0.0.0:9093")
                .WithEnvironment("KAFKA_ADVERTISED_LISTENERS", $"EXTERNAL://127.0.0.1:{hostPort},INTERNAL://localhost:9094")
                .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
                .WithEnvironment("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
                .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .WithEnvironment("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
                .WithEnvironment("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
                .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                // KIP-848 (new consumer group protocol) — required by the Rust binding.
                .WithEnvironment("KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Kafka Server started"))
                .Build();

            try
            {
                using var startCts = new CancellationTokenSource(TimeSpan.FromSeconds(180));
                await container.StartAsync(startCts.Token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _skipReason = $"SKIP: could not start Kafka testcontainer: {e.Message}";
                output.WriteLine(_skipReason);
                try
                {
                    await container.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort cleanup of the partially-started container.
                }

                return false;
            }

            _container = container;
            _externalBootstrap = $"127.0.0.1:{hostPort}";

            // Give the coordinator a moment to settle before tests subscribe (conftest sleeps 2 s).
            await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _gate.Release();
        }
    }

    /// <summary>Creates <paramref name="topic"/> via <c>kafka-topics.sh</c> inside the container (conftest's <c>create_topic</c>).</summary>
    public async Task CreateTopicAsync(string topic, int partitions)
    {
        ExecResult result = await ExecAsync(new[]
        {
            $"{KafkaBin}/kafka-topics.sh", "--bootstrap-server", InternalBootstrap,
            "--create", "--if-not-exists", "--topic", topic,
            "--partitions", partitions.ToString(CultureInfo.InvariantCulture), "--replication-factor", "1",
        }).ConfigureAwait(false);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"create_topic failed (rc={result.ExitCode}): {result.Stderr}");
        }
    }

    /// <summary>
    /// Starts <c>kafka-producer-perf-test.sh</c> inside the container, <b>detached</b>, producing
    /// CreateTime-timestamped records against the INTERNAL listener (conftest's <c>produce_perf_in_container</c>).
    /// Backgrounded via <c>nohup ... &amp;</c> in a shell so the exec returns immediately while the load keeps
    /// running for the consumer's lifetime (Testcontainers' <c>ExecAsync</c> has no detach flag).
    /// </summary>
    public async Task ProducePerfInContainerAsync(string topic, long numRecords, int recordSize, int throughput)
    {
        string cmd =
            $"nohup {KafkaBin}/kafka-producer-perf-test.sh " +
            $"--topic {topic} " +
            $"--num-records {numRecords.ToString(CultureInfo.InvariantCulture)} " +
            $"--record-size {recordSize.ToString(CultureInfo.InvariantCulture)} " +
            $"--throughput {throughput.ToString(CultureInfo.InvariantCulture)} " +
            $"--producer-props bootstrap.servers={InternalBootstrap} acks=1 " +
            ">/tmp/perf-prod.log 2>&1 &";

        // The shell backgrounds the producer and exits, so this exec returns promptly.
        await ExecAsync(new[] { "sh", "-c", cmd }).ConfigureAwait(false);
    }

    private async Task<ExecResult> ExecAsync(IList<string> command)
    {
        if (_container is null)
        {
            throw new InvalidOperationException("Broker container is not started.");
        }

        return await _container.ExecAsync(command).ConfigureAwait(false);
    }

    private static bool DockerAvailable()
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo("docker", "info")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            });

            if (process is null)
            {
                return false;
            }

            if (!process.WaitForExit(15000))
            {
                try
                {
                    process.Kill(entireProcessTree: true);
                }
                catch (Exception)
                {
                    // Ignore — we already know Docker is not responding.
                }

                return false;
            }

            return process.ExitCode == 0;
        }
        catch (Exception)
        {
            // docker CLI missing (Win32Exception) or any other failure -> treat as unavailable.
            return false;
        }
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }
}
