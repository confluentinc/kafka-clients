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
/// A single-node KRaft broker in Docker, as Python's perf <c>conftest.kafka_broker</c>, for the perf smoke and the
/// broker integration tests. It starts once, on first use; without Docker a test is skipped, or fails under
/// <c>PERF_REQUIRE_DOCKER=True</c> (set by the perf Makefile target).
/// </summary>
public sealed class KafkaBrokerFixture : IAsyncLifetime
{
    // The image of Python's perf conftest and the Rust integration harness.
    private const string KafkaImage = "apache/kafka:4.2.0";
    private const string ClusterId = "5L6g3nShT-eMCtK--X86sw";

    public const string InternalBootstrap = "localhost:9094";

    public const string KafkaBin = "/opt/kafka/bin";

    private readonly SemaphoreSlim _gate = new(1, 1);
    private IContainer? _container;
    private string? _externalBootstrap;
    private bool _startAttempted;
    private string? _unavailableReason;

    public string? ExternalBootstrap => _externalBootstrap;

    public Task InitializeAsync() => Task.CompletedTask;

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

    public async Task EnsureStartedAsync(ITestOutputHelper output)
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_startAttempted)
            {
                if (_unavailableReason is not null)
                {
                    output.WriteLine(_unavailableReason);
                    FailOrSkip(_unavailableReason);
                }

                return;
            }

            _startAttempted = true;

            if (!DockerAvailable())
            {
                _unavailableReason = "Docker is not available; the broker-backed tests need Docker.";
                output.WriteLine(_unavailableReason);
                FailOrSkip(_unavailableReason);
                return;
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
                .WithEnvironment("KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Kafka Server started"))
                .Build();

            try
            {
                // One budget for the image pull, the start and the readiness wait, which StartAsync does together;
                // Python bounds only the readiness wait.
                using var startCts = new CancellationTokenSource(TimeSpan.FromSeconds(300));
                await container.StartAsync(startCts.Token).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _unavailableReason = $"could not start Kafka testcontainer: {e.Message}";
                output.WriteLine(_unavailableReason);
                try
                {
                    await container.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Best-effort cleanup of the partially-started container.
                }

                FailOrSkip(_unavailableReason);
                return;
            }

            _container = container;
            _externalBootstrap = $"127.0.0.1:{hostPort}";

            // The coordinator settles before tests subscribe, as Python's conftest waits 2 s.
            await Task.Delay(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private static void FailOrSkip(string reason)
    {
        // As Python's flags, only the case-sensitive "True" enables it.
        if (PerfEnv.GetBool("PERF_REQUIRE_DOCKER", false))
        {
            Assert.Fail(reason);
        }

        Skip.If(true, reason);
    }

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

    public async Task AddPartitionsAsync(string topic, int partitions)
    {
        ExecResult result = await ExecAsync(new[]
        {
            $"{KafkaBin}/kafka-topics.sh", "--bootstrap-server", InternalBootstrap,
            "--alter", "--topic", topic, "--partitions", partitions.ToString(CultureInfo.InvariantCulture),
        }).ConfigureAwait(false);

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"adding partitions failed (rc={result.ExitCode}): {result.Stderr}");
        }
    }

    public async Task ProducePerfInContainerAsync(string topic, long numRecords, int recordSize, int throughput)
    {
        // Detached, so the load outlives this exec; each consumer smoke stops it with StopPerfInContainerAsync.
        string cmd =
            $"nohup {KafkaBin}/kafka-producer-perf-test.sh " +
            $"--topic {topic} " +
            $"--num-records {numRecords.ToString(CultureInfo.InvariantCulture)} " +
            $"--record-size {recordSize.ToString(CultureInfo.InvariantCulture)} " +
            $"--throughput {throughput.ToString(CultureInfo.InvariantCulture)} " +
            $"--producer-props bootstrap.servers={InternalBootstrap} acks=1 " +
            ">/tmp/perf-prod.log 2>&1 &";

        await ExecAsync(new[] { "sh", "-c", cmd }).ConfigureAwait(false);
    }

    public async Task<long> StopPerfInContainerAsync(string topic)
    {
        // Unlike Python's no-op stop, this kills the feeder so retries do not stack; the topic pattern ends in a space,
        // as one smoke topic prefixes another.
        ExecResult result = await ExecAsync(new[]
        {
            "sh", "-c", $"pkill -f 'ProducerPerformance.*--topic {topic} '",
        }).ConfigureAwait(false);

        return result.ExitCode ?? -1;
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
                    // Best-effort: Docker is already known not to respond.
                }

                return false;
            }

            return process.ExitCode == 0;
        }
        catch (Exception)
        {
            // A missing docker CLI or any other failure means Docker is unavailable.
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
