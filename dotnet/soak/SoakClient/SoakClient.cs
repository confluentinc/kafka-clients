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
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

namespace Confluent.Kafka.Soak;

/// <summary>
/// The soak: a producer sending at a fixed rate and a consumer reading the records back, each an
/// async loop over the async clients (Python's soak runs two threads), printing counters every
/// ~10 seconds.
/// </summary>
internal sealed class SoakClient : IDisposable
{
    // rust_dotnet survives the OTLP-to-Prometheus name translation unchanged, where rust(dotnet)
    // would arrive as rust_dotnet__; one constant drives the metric prefix and the host tag.
    internal const string SoakClientToken = "rust_dotnet";

    internal const string MetricPrefix = "kafka.client.soak." + SoakClientToken + ".";

    // Above 1, as Python's soak: a non-retriable client-side error (a wakeup, an illegal state) can
    // be routine during a broker roll, so one does not end the run.
    internal const int NonRetriablePollFailureLimit = 3;

    internal const int DeliveryDrainBoundMs = 5000;

    private const int DrainPollIntervalMs = 10;

    // Past this much pacing debt (a Send blocked up to max.block.ms) the schedule re-anchors
    // instead of bursting the backlog; one second still absorbs a GC pause or a slow ack.
    internal const double MaxPacingCatchUpSeconds = 1.0;

    // The protocol error codes that classify the errors a broker roll produces, as Python's soak;
    // an error these do not list is classified by its message.
    private static readonly IReadOnlyCollection<int> s_coordinatorErrorCodes = new HashSet<int>
    {
        14, // CoordinatorLoadInProgress
        15, // CoordinatorNotAvailable
        16, // NotCoordinator
    };

    private static readonly IReadOnlyCollection<int> s_disconnectErrorCodes = new HashSet<int>
    {
        5,  // LeaderNotAvailable
        6,  // NotLeaderOrFollower
        7,  // RequestTimedOut
        8,  // BrokerNotAvailable
        13, // NetworkException
    };

    private static readonly string[] s_disconnectMessageMarkers = { "disconnect", "connection", "timed out", "timeout" };

    // Authentication / authorization failures never clear by retrying; everything else at
    // startup (broker unreachable, metadata timeout) might.
    private static readonly IReadOnlyCollection<int> s_authErrorCodes = new HashSet<int>
    {
        29, // TopicAuthorizationFailed
        30, // GroupAuthorizationFailed
        31, // ClusterAuthorizationFailed
        33, // UnsupportedSaslMechanism
        34, // IllegalSaslState
        58, // SaslAuthenticationFailed
    };

    private const int TopicAlreadyExistsCode = 36;
    private const int UnknownTopicOrPartitionCode = 3;

    private readonly SoakOptions _options;
    private readonly SoakLogger _logger;
    private readonly SoakMetrics _metrics;
    private readonly ProcessResourceSampler _resources;
    private readonly CancellationTokenSource _stop;
    private readonly IAsyncProducer<byte[], SoakRecord> _producer;
    private readonly IAsyncConsumer<byte[], SoakRecord> _consumer;
    private readonly string _hostname;
    private readonly long _disprate;
    private readonly double _statusIntervalSeconds;
    private readonly double _rssAtStartupMiB;
    private readonly double _baselineRssMiB;
    private readonly DateTime _startTimeUtc = DateTime.UtcNow;

    private long _producerMsgId;
    private long _deliveredCount;
    private long _deliveryErrorCount;
    private long _outstanding;
    private long _pendingDeliveries;
    private long _msgCount;
    private long _msgDuplicateCount;
    private long _msgMissedCount;
    private long _msgErrorCount;
    private long _consumerErrorCount;
    private long _rebalanceCount;
    private long _disconnectCount;
    private long _coordinatorMoveCount;
    private string? _lastCommitted;
    private string? _fatalReason;
    private Task _producerTask = Task.CompletedTask;
    private Task _consumerTask = Task.CompletedTask;
    private bool _disposed;

    internal SoakClient(
        SoakOptions options,
        SoakLogger logger,
        SoakMetrics metrics,
        ProcessResourceSampler resources,
        CancellationTokenSource stop,
        IAsyncProducer<byte[], SoakRecord> producer,
        IAsyncConsumer<byte[], SoakRecord> consumer,
        string hostname,
        double rssAtStartupMiB,
        double baselineRssMiB)
    {
        _options = options;
        _logger = logger;
        _metrics = metrics;
        _resources = resources;
        _stop = stop;
        _producer = producer;
        _consumer = consumer;
        _hostname = hostname;
        _rssAtStartupMiB = rssAtStartupMiB;
        _baselineRssMiB = baselineRssMiB;

        // Status lines every ~10 s of messages, as Python's soak.
        _disprate = Math.Max(1, (long)(options.Rate * 10));
        _statusIntervalSeconds = _disprate / options.Rate;
    }

    internal long MissedCount => Interlocked.Read(ref _msgMissedCount);

    internal string? FatalReason => Volatile.Read(ref _fatalReason);

    internal (long Delivered, long Failed, long Outstanding, long PendingDeliveries) ProducerCounts => (
        Interlocked.Read(ref _deliveredCount),
        Interlocked.Read(ref _deliveryErrorCount),
        Interlocked.Read(ref _outstanding),
        Interlocked.Read(ref _pendingDeliveries));

    internal SoakLogger Logger => _logger;

    internal CancellationToken StopToken => _stop.Token;

    // Python's SoakClient.__init__ order: every config is validated before any side effect. A
    // factory, not a constructor, as topic creation awaits.
    internal static async Task<SoakClient> CreateAsync(
        SoakOptions options,
        IReadOnlyDictionary<string, string> fileConfig,
        double rssAtStartupMiB)
    {
        var logger = new SoakLogger(options.LogLevel);

        string host = SoakEnv.GetStringOrNull("HOSTNAME") ?? Environment.MachineName;
        string hostname = string.Format(
            CultureInfo.InvariantCulture,
            "{0}-{1}-{2}",
            SoakClientToken,
            host,
            options.Topic);

        var baseTags = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["host"] = hostname,
            ["testid"] = options.TestId,
            ["variant"] = options.Variant,
        };

        OtelSink? sink = OtelSink.Create(baseTags, logger);
        var metrics = new SoakMetrics(options.MetricsFile, baseTags, logger, MetricPrefix, sink);

        logger.Info(string.Format(
            CultureInfo.InvariantCulture,
            "SoakClient id {0} (variant {1}, rate {2} msg/s, payload {3} B, metrics -> {4}, otel {5})",
            hostname,
            options.Variant,
            options.Rate,
            options.PayloadSize,
            options.MetricsFile,
            metrics.OtelEnabled ? "exporting" : "not exporting (JSONL only)"));
        LogBuildManifest(logger);

        var conf = new Dictionary<string, string>(fileConfig, StringComparer.Ordinal);
        if (options.Brokers is not null)
        {
            conf["bootstrap.servers"] = options.Brokers;
        }

        if (!conf.ContainsKey("group.id") && !conf.ContainsKey("consumer.group.id"))
        {
            conf["group.id"] = string.Format(
                CultureInfo.InvariantCulture,
                "soakclient-{0}-{1}",
                hostname,
                Environment.Version.ToString());
        }

        SoakClient? built = null;
        try
        {
            // The admin config is routed but not validated against a key list, as Python's soak;
            // only its SASL credentials are checked (below).
            var aconf = SoakConfig.FilterConfig(conf, new[] { "consumer.", "producer." }, "admin.");
            aconf["client.id"] = options.TestId;

            var pconf = SoakConfig.FilterConfig(conf, new[] { "consumer.", "admin." }, "producer.");
            pconf["client.id"] = options.TestId;
            (pconf, IReadOnlyList<string> producerRouted) = SoakConfig.RouteSharedConfig(
                pconf, SoakConfig.ProducerConfigKeys, SoakConfig.ConsumerConfigKeys);
            if (producerRouted.Count > 0)
            {
                logger.Info("producer: not a producer key, routed to the consumer only: " + string.Join(", ", producerRouted));
            }

            SoakConfig.ValidateConfig(pconf, SoakConfig.ProducerConfigKeys, "producer");

            var cconf = SoakConfig.FilterConfig(conf, new[] { "producer.", "admin." }, "consumer.");
            cconf["client.id"] = options.TestId;
            (cconf, IReadOnlyList<string> consumerRouted) = SoakConfig.RouteSharedConfig(
                cconf, SoakConfig.ConsumerConfigKeys, SoakConfig.ProducerConfigKeys);
            if (consumerRouted.Count > 0)
            {
                logger.Info("consumer: not a consumer key, routed to the producer only: " + string.Join(", ", consumerRouted));
            }

            SoakConfig.ValidateConfig(cconf, SoakConfig.ConsumerConfigKeys, "consumer");

            SoakConfig.CheckAdminCredentials(aconf);

            await CreateTopicAsync(aconf, options, logger).ConfigureAwait(false);

            // Both clients are constructed before either loop starts, so a failure here
            // cannot leave a producer running with no consumer.
            logger.Info("producer: using client.id " + pconf["client.id"]);
            var producer = new AsyncKafkaProducer<byte[], SoakRecord>(
                pconf, Serdes.ByteArray, new SoakRecordSerializer(options.PayloadSize));

            logger.Info("consumer: using group.id " + (cconf.TryGetValue("group.id", out string? groupId) ? groupId : "<unset>"));
            var consumer = new AsyncKafkaConsumer<byte[], SoakRecord>(
                cconf, Serdes.ByteArray, new SoakRecordDeserializer());

            var resources = new ProcessResourceSampler();

            // RSS baseline after the clients exist: memory.rss.delta is measured from here, and its
            // gap to the startup baseline is the client's own startup cost.
            double baselineRssMiB = resources.CurrentRssMiB();

            built = new SoakClient(
                options, logger, metrics, resources, new CancellationTokenSource(),
                producer, consumer, hostname, rssAtStartupMiB, baselineRssMiB);
        }
        catch
        {
            metrics.Close();
            throw;
        }

        // Counters shown even at zero; producer.errorcb and consumer.errorcb have no source here
        // (no error callback) but keep the Python soak's dashboards whole.
        foreach (string name in new[]
        {
            "producer.drerr", "producer.errorcb", "consumer.error", "consumer.msgdup",
            "consumer.msgerr", "consumer.missedmsg", "consumer.errorcb", "consumer.rebalance",
            "consumer.disconnect", "consumer.coordinator_move",
        })
        {
            built.IncrCounter(name, 0);
        }

        logger.Info(string.Format(
            CultureInfo.InvariantCulture,
            "baseline RSS: {0:F3} MiB at startup, {1:F3} MiB after client construction (client cost {2:F3} MiB)",
            built._rssAtStartupMiB,
            built._baselineRssMiB,
            built._baselineRssMiB - built._rssAtStartupMiB));

        metrics.SetMeasurementStart(SoakMetrics.NowMs());
        metrics.StartCollecting(options.MetricsIntervalSeconds);

        built._producerTask = Task.Run(() => built.ProducerLoopAsync());
        built._consumerTask = Task.Run(() => built.ConsumerLoopAsync());
        return built;
    }

    internal static int? ErrorCode(Exception ex) => ex is KafkaException kafka ? kafka.Code : (int?)null;

    internal static string ErrorMessage(Exception ex) => ex.Message;

    internal static bool ErrorIsRetriable(Exception ex) => ex is KafkaException kafka && kafka.IsRetriable;

    // Bounded, as Python's soak: endless retrying would never reach the SUMMARY verdict, and
    // run.sh's restart re-joins the group. Non-retriable errors have their own, lower bound.
    internal static bool PollFailureIsTerminal(Exception ex, int consecutive, int maxPollFailures, out string fatalReason) =>
        PollFailureIsTerminal(ErrorIsRetriable(ex), ErrorMessage(ex), consecutive, maxPollFailures, out fatalReason);

    /// <summary>
    /// What the consumer loop does with a poll failure.
    /// </summary>
    internal enum PollFailureAction
    {
        Continue = 0,

        Suppress = 1,

        Abort = 2,
    }

    // Shutdown is checked first, as Python's soak: an error during shutdown is neither counted nor
    // allowed to turn a clean exit into ConsumerWedged.
    internal static PollFailureAction ClassifyPollFailure(
        bool stopRequested,
        bool retriable,
        string message,
        int consecutive,
        int maxPollFailures,
        out string fatalReason)
    {
        fatalReason = string.Empty;
        if (stopRequested)
        {
            return PollFailureAction.Suppress;
        }

        return PollFailureIsTerminal(retriable, message, consecutive, maxPollFailures, out fatalReason)
            ? PollFailureAction.Abort
            : PollFailureAction.Continue;
    }

    internal static bool PollFailureIsTerminal(bool retriable, string message, int consecutive, int maxPollFailures, out string fatalReason)
    {
        int limit = retriable ? maxPollFailures : Math.Min(NonRetriablePollFailureLimit, maxPollFailures);
        if (consecutive < limit)
        {
            fatalReason = string.Empty;
            return false;
        }

        fatalReason = string.Format(
            CultureInfo.InvariantCulture,
            "consumer poll failed {0} consecutive times ({1}retriable), last error: {2}",
            consecutive,
            retriable ? string.Empty : "non-",
            message);
        return true;
    }

    internal static double NextPacingCheckpoint(double checkpoint, double now, double intervalSeconds)
    {
        double advanced = checkpoint + intervalSeconds;

        return advanced < now - MaxPacingCatchUpSeconds
            ? now + intervalSeconds
            : advanced;
    }

    internal void IncrCounter(string metricName, long increment, IReadOnlyDictionary<string, string>? tags = null) =>
        _metrics.IncrCounter(metricName, increment, tags);

    internal void SetGauge(string metricName, double value, IReadOnlyDictionary<string, string>? tags = null) =>
        _metrics.SetGauge(metricName, value, tags);

    // Called from the signal handler, so nothing here blocks.
    internal void RequestStop()
    {
        // One CancellationTokenSource is the stop: a second cancel is a no-op, so Python's
        // _wakeup_sent guard is not ported.
        try
        {
            _stop.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already torn down; a second stop request is a no-op.
        }
    }

    internal void Abort() => RequestStop();

    internal async Task TerminateAsync()
    {
        _logger.Info(string.Format(
            CultureInfo.InvariantCulture,
            "Terminating (ran for {0:F0}s)",
            (DateTime.UtcNow - _startTimeUtc).TotalSeconds));
        RequestStop();

        await Task.WhenAll(_producerTask, _consumerTask).ConfigureAwait(false);

        try
        {
            // CancellationToken.None: teardown must not be cancelled by the very token
            // that triggered it.
            await _producer.Close(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.Warning("producer: close failed: " + ErrorMessage(ex));
        }

        // Close resolves every delivery Task, but OnDelivery runs later on the thread pool, so wait
        // for it (bounded) before the final window, or the SUMMARY can read delivered < produced.
        // The producer loop has ended, so the count only falls.
        long undrained = await DrainCounterAsync(
            () => Interlocked.Read(ref _pendingDeliveries),
            TimeSpan.FromMilliseconds(DeliveryDrainBoundMs)).ConfigureAwait(false);
        if (undrained > 0)
        {
            _logger.Warning(string.Format(
                CultureInfo.InvariantCulture,
                "producer: {0} delivery accounting continuation(s) did not run within {1} ms; "
                + "the SUMMARY's delivered= may be short by that many",
                undrained,
                DeliveryDrainBoundMs));
        }

        FinalizeMetrics(_metrics, SampleResources, _logger);
        FinalReport();
    }

    internal static async Task<long> DrainCounterAsync(Func<long> read, TimeSpan bound)
    {
        double deadline = MonotonicSeconds() + bound.TotalSeconds;
        long remaining = read();
        while (remaining > 0 && MonotonicSeconds() < deadline)
        {
            await Task.Delay(DrainPollIntervalMs, CancellationToken.None).ConfigureAwait(false);
            remaining = read();
        }

        return remaining;
    }

    // A no-throw boundary: a throw here (a full disk) would lose the SUMMARY line and exit with a
    // code run.sh does not know. Sampling is inside it, as Process.Refresh can throw.
    internal static void FinalizeMetrics(SoakMetrics metrics, Action sampleResources, SoakLogger logger)
    {
        try
        {
            sampleResources();
            metrics.SetMeasurementEnd(SoakMetrics.NowMs());
            metrics.StopCollecting();
            metrics.WriteFinal();
            metrics.Close();
        }
        catch (Exception ex)
        {
            logger.Error("metrics: final window failed, continuing to the verdict: " + ex.Message);
        }
    }

    internal void FinalReport()
    {
        long produced = Interlocked.Read(ref _producerMsgId);
        long delivered = Interlocked.Read(ref _deliveredCount);
        long consumed = Interlocked.Read(ref _msgCount);
        long duplicates = Interlocked.Read(ref _msgDuplicateCount);
        long missed = Interlocked.Read(ref _msgMissedCount);
        long errors = Interlocked.Read(ref _deliveryErrorCount)
            + Interlocked.Read(ref _msgErrorCount)
            + Interlocked.Read(ref _consumerErrorCount);

        string verdict;
        if (missed > 0)
        {
            verdict = "FAIL (message loss)";
        }
        else if (FatalReason is string reason)
        {
            verdict = "ABORTED (" + reason + ")";
        }
        else
        {
            verdict = "PASS";
        }

        _logger.Info(string.Format(
            CultureInfo.InvariantCulture,
            "SUMMARY variant={0} testid={1} topic={2} produced={3} delivered={4} consumed={5} "
            + "duplicates={6} missed={7} errors={8} rebalances={9} disconnects={10} "
            + "coordinator_moves={11} verdict={12}",
            _options.Variant,
            _options.TestId,
            _options.Topic,
            produced,
            delivered,
            consumed,
            duplicates,
            missed,
            errors,
            Interlocked.Read(ref _rebalanceCount),
            Interlocked.Read(ref _disconnectCount),
            Interlocked.Read(ref _coordinatorMoveCount),
            verdict));
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _producer.Dispose();

        // Normally a no-op (the consume loop's finally closed it); otherwise a graceful close of up
        // to the core's 30 s default against an unreachable coordinator.
        _consumer.Dispose();
        _metrics.Dispose();
        _stop.Dispose();
    }

    internal void SampleResources()
    {
        ResourceSample sample = _resources.Sample();

        if (sample.HasCpuDeltas)
        {
            SetGauge("cpu.user", sample.UserCpuPercent);
            SetGauge("cpu.system", sample.SystemCpuPercent);
            SetGauge("memory.rss.max", sample.MaxRssMiB);
            _logger.Info(string.Format(
                CultureInfo.InvariantCulture,
                "User CPU: {0:F1}%, System CPU: {1:F1}%, MaxRSS {2:F3} MiB",
                sample.UserCpuPercent,
                sample.SystemCpuPercent,
                sample.MaxRssMiB));
        }

        SetGauge("memory.rss", sample.RssMiB);

        // Re-emitted every window: a gauge sampled once averages 0 in later windows, which reads as
        // a 0 MiB baseline.
        SetGauge("memory.rss.baseline_imports", _rssAtStartupMiB);
        SetGauge("memory.rss.baseline_constructed", _baselineRssMiB);
        SetGauge("memory.rss.delta", sample.RssMiB - _baselineRssMiB);

        // Named memory.gc_heap, not Python's memory.tracemalloc: the managed heap, so RSS climbing
        // while it stays flat points at native growth.
        SetGauge("memory.gc_heap", sample.GcHeapMiB);
        SetGauge("memory.gc_heap.peak", sample.GcHeapPeakMiB);

        SetGauge("producer.outq", Interlocked.Read(ref _outstanding));
    }

    private async Task ProducerLoopAsync()
    {
        Thread.CurrentThread.Name ??= "producer";
        try
        {
            // Batches of max(1, rate/100) records, as Python's soak.
            int batch = Math.Max(1, (int)(_options.Rate / 100));
            double batchIntervalSeconds = batch / _options.Rate;
            double nextStatus = MonotonicSeconds() + _statusIntervalSeconds;

            // An absolute deadline, unlike Python's relative sleep: Task.Delay rounds to whole
            // milliseconds, which a relative sleep turns into a rate error (+1.8% at 1000 msg/s).
            double nextBatchTime = MonotonicSeconds() + batchIntervalSeconds;

            while (!_stop.IsCancellationRequested)
            {
                for (int i = 0; i < batch; i++)
                {
                    if (_stop.IsCancellationRequested)
                    {
                        break;
                    }

                    await ProduceRecordAsync().ConfigureAwait(false);
                }

                double now = MonotonicSeconds();
                if (now > nextStatus)
                {
                    ProducerStatus();
                    nextStatus = now + _statusIntervalSeconds;
                }

                double remaining = nextBatchTime - MonotonicSeconds();
                if (remaining > 0)
                {
                    // Cancellable, so shutdown aborts the pacing sleep.
                    await DelayAsync(TimeSpan.FromSeconds(remaining)).ConfigureAwait(false);
                }

                nextBatchTime = NextPacingCheckpoint(
                    nextBatchTime, MonotonicSeconds(), batchIntervalSeconds);
            }

            // Flush cannot be interrupted, which is why Main arms the shutdown watchdog.
            _logger.Info("producer: flushing");
            await _producer.Flush(CancellationToken.None).ConfigureAwait(false);
            ProducerStatus();
        }
        catch (Exception ex)
        {
            _logger.Fatal("producer: fatal exception: " + ex);
            Abort();
        }
    }

    internal async Task ProduceRecordAsync()
    {
        long msgId = Interlocked.Increment(ref _producerMsgId) - 1;

        int txCnt = 0;
        while (!_stop.IsCancellationRequested && txCnt < _options.MaxSendAttempts)
        {
            txCnt++;
            var record = new SoakRecord(msgId, SoakMetrics.NowMs(), txCnt);
            var producerRecord = new ProducerRecord<byte[], SoakRecord>(_options.Topic, record);

            Interlocked.Increment(ref _outstanding);
            double sentAt = MonotonicSeconds();

            Task<RecordMetadata> task;
            try
            {
                // CancellationToken.None: the stop token would end the first stage with
                // OperationCanceledException for a record the core still delivers (Python's send
                // takes none). The first stage is awaited: it is the producer's backpressure.
                task = (await _producer.Send(producerRecord, CancellationToken.None).ConfigureAwait(false)).Get();
            }
            catch (Exception ex)
            {
                // txcnt counts send attempts: a retriable throw from Send or its first stage (a
                // full buffer after max.block.ms too) is one attempt.
                Interlocked.Decrement(ref _outstanding);
                if (!ErrorIsRetriable(ex) || txCnt >= _options.MaxSendAttempts)
                {
                    CountSendFailure(msgId, ex);
                    return;
                }

                _logger.Warning(string.Format(
                    CultureInfo.InvariantCulture,
                    "producer: send attempt {0} for msgid {1} failed (retriable): {2}",
                    txCnt,
                    msgId,
                    ErrorMessage(ex)));
                await DelayAsync(TimeSpan.FromSeconds(0.1)).ConfigureAwait(false);
                continue;
            }

            // Python's future.when_complete(action=...), carrying sentAt; OnDelivery reads
            // task.Exception, so no fault goes unobserved. Counted first, so shutdown can wait for
            // every continuation.
            Interlocked.Increment(ref _pendingDeliveries);
            _ = task.ContinueWith(
                completed => OnDelivery(completed, sentAt),
                CancellationToken.None,
                TaskContinuationOptions.None,
                TaskScheduler.Default);

            IncrCounter("producer.send", 1);
            return;
        }
    }

    private void OnDelivery(Task<RecordMetadata> task, double sentAtSeconds)
    {
        // Total: an escape would fault an unobserved continuation Task.
        try
        {
            Interlocked.Decrement(ref _outstanding);

            if (task.IsFaulted)
            {
                AggregateException? aggregate = task.Exception;
                Exception error = aggregate?.Flatten().InnerExceptions.FirstOrDefault()
                    ?? new KafkaException("delivery failed with no exception recorded");
                RecordDeliveryFailure(error);
                return;
            }

            if (task.IsCanceled)
            {
                RecordDeliveryFailure(new OperationCanceledException("send cancelled"));
                return;
            }

            RecordDelivery(task.Result, (MonotonicSeconds() - sentAtSeconds) * 1000.0);
        }
        catch (Exception ex)
        {
            _logger.Error("producer: delivery accounting failed: " + ex);
        }
        finally
        {
            // Last: the shutdown drain waits for this accounting, so release only after it.
            Interlocked.Decrement(ref _pendingDeliveries);
        }
    }

    private void RecordDelivery(RecordMetadata metadata, double latencyMs)
    {
        long delivered = Interlocked.Increment(ref _deliveredCount);
        IncrCounter("producer.drok", 1);
        SetGauge(
            "producer.latency",
            latencyMs,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["partition"] = metadata.Partition.ToString(CultureInfo.InvariantCulture),
            });

        if (delivered % _disprate == 0)
        {
            _logger.Debug(string.Format(
                CultureInfo.InvariantCulture,
                "producer: delivered message to {0} [{1}] at offset {2} in {3:F1} ms",
                metadata.Topic,
                metadata.Partition,
                metadata.Offset,
                latencyMs));
        }
    }

    private void RecordDeliveryFailure(Exception ex)
    {
        Interlocked.Increment(ref _deliveryErrorCount);
        int? code = ErrorCode(ex);
        _logger.Warning(string.Format(
            CultureInfo.InvariantCulture,
            "producer: delivery failed: {0} [code {1}]",
            ErrorMessage(ex),
            code.HasValue ? code.Value.ToString(CultureInfo.InvariantCulture) : "None"));
        IncrCounter("producer.drerr", 1);
        IncrCounter(
            "producer.delivery.failure",
            1,
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["err"] = code.HasValue ? code.Value.ToString(CultureInfo.InvariantCulture) : "None",
            });
    }

    private void CountSendFailure(long msgId, Exception ex)
    {
        Interlocked.Increment(ref _deliveryErrorCount);
        _logger.Error(string.Format(
            CultureInfo.InvariantCulture,
            "producer: giving up on msgid {0}: {1}",
            msgId,
            ErrorMessage(ex)));
        IncrCounter("producer.drerr", 1);
    }

    private void ProducerStatus()
    {
        _logger.Info(string.Format(
            CultureInfo.InvariantCulture,
            "producer: {0} messages produced, {1} delivered, {2} failed, 0 error_cbs, {3} outstanding",
            Interlocked.Read(ref _producerMsgId),
            Interlocked.Read(ref _deliveredCount),
            Interlocked.Read(ref _deliveryErrorCount),
            Interlocked.Read(ref _outstanding)));
    }

    private async Task ConsumerLoopAsync()
    {
        Thread.CurrentThread.Name ??= "consumer";
        var hwmarks = new HighWaterMarks();
        var pending = new Dictionary<TopicPartition, OffsetAndMetadata>();

        try
        {
            await _consumer.Subscribe(new[] { _options.Topic }, _stop.Token).ConfigureAwait(false);

            var assignment = new HashSet<TopicPartition>();
            double now = MonotonicSeconds();
            double nextStatus = now + _statusIntervalSeconds;
            double nextCommit = now + _options.CommitIntervalSeconds;
            double lastProgress = now;
            bool stalled = false;
            int pollFailures = 0;

            while (!_stop.IsCancellationRequested)
            {
                now = MonotonicSeconds();
                if (now > nextStatus)
                {
                    ConsumerStatus();
                    nextStatus = now + _statusIntervalSeconds;
                }

                ConsumerRecords<byte[], SoakRecord> records;
                try
                {
                    records = await _consumer
                        .Poll(TimeSpan.FromSeconds(_options.PollTimeoutSeconds), _stop.Token)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // The stop token's cancel ends the poll as OperationCanceledException, where
                    // Python's soak checks _is_wakeup(ex).
                    break;
                }
                catch (Exception ex)
                {
                    PollFailureAction action = ClassifyPollFailure(
                        _stop.IsCancellationRequested,
                        ErrorIsRetriable(ex),
                        ErrorMessage(ex),
                        pollFailures + 1,
                        _options.MaxPollFailures,
                        out string fatalReason);

                    if (action == PollFailureAction.Suppress)
                    {
                        break;
                    }

                    ClassifyError("consumer: poll", ex);
                    pollFailures++;
                    if (action == PollFailureAction.Abort)
                    {
                        Volatile.Write(ref _fatalReason, fatalReason);
                        _logger.Fatal("consumer: " + fatalReason
                            + " — aborting so the run is restarted rather than silently consuming nothing");
                        Abort();
                        break;
                    }

                    await DelayAsync(TimeSpan.FromSeconds(0.5)).ConfigureAwait(false);
                    continue;
                }

                pollFailures = 0;
                assignment = CheckAssignment(assignment);

                if (records.Count > 0)
                {
                    if (stalled)
                    {
                        double recoveryMs = (MonotonicSeconds() - lastProgress) * 1000.0;
                        SetGauge("consumer.recovery_ms", recoveryMs);
                        _logger.Warning(string.Format(
                            CultureInfo.InvariantCulture,
                            "consumer: recovered after {0:F1} ms without records",
                            recoveryMs));
                        stalled = false;
                    }

                    lastProgress = MonotonicSeconds();

                    foreach (ConsumerRecord<byte[], SoakRecord> record in records)
                    {
                        ConsumeRecord(record, hwmarks, pending);
                    }
                }
                else if (!stalled && (MonotonicSeconds() - lastProgress) > _options.StallThresholdSeconds)
                {
                    stalled = true;
                    _logger.Warning(string.Format(
                        CultureInfo.InvariantCulture,
                        "consumer: no records for {0:F1} s (assignment: {1} partitions)",
                        MonotonicSeconds() - lastProgress,
                        assignment.Count));
                }

                if (MonotonicSeconds() > nextCommit)
                {
                    await CommitAsync(pending, _stop.Token).ConfigureAwait(false);
                    nextCommit = MonotonicSeconds() + _options.CommitIntervalSeconds;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown landed outside the poll (Subscribe, or a commit not already retried): a
            // clean stop, not a fatal error.
            _logger.Info("consumer: stopped by shutdown");
        }
        catch (Exception ex)
        {
            _logger.Fatal("consumer: fatal exception: " + ex);
            Abort();
        }
        finally
        {
            // Best-effort final commit, so a restart does not replay this window; the token is
            // already cancelled, hence None.
            try
            {
                await CommitAsync(pending, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.Warning("consumer: final commit failed: " + ErrorMessage(ex));
            }

            try
            {
                await _consumer.Close(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.Warning("consumer: close failed: " + ErrorMessage(ex));
            }

            ConsumerStatus();
        }
    }

    private void ConsumeRecord(
        ConsumerRecord<byte[], SoakRecord> record,
        HighWaterMarks hwmarks,
        Dictionary<TopicPartition, OffsetAndMetadata> pending)
    {
        // Both malformed shapes count once: a null value, for which the deserializer is not called,
        // and a record the total deserializer marked malformed.
        SoakRecord? soakRecord = record.Value;
        if (soakRecord is null || soakRecord.IsMalformed)
        {
            string reason = soakRecord?.MalformedReason ?? SoakRecord.EmptyPayloadReason;
            _logger.Info(string.Format(
                CultureInfo.InvariantCulture,
                "consumer: Failed to deserialize message in {0} [{1}] at offset {2}: {3}",
                record.Topic,
                record.Partition,
                record.Offset,
                reason));
            Interlocked.Increment(ref _msgErrorCount);
            IncrCounter("consumer.msgerr", 1);

            return;
        }

        long msgCount = Interlocked.Increment(ref _msgCount);
        IncrCounter("consumer.msg", 1);

        // In milliseconds: SetGauge converts to seconds on export only.
        double latencyMs = SoakMetrics.NowMs() - soakRecord.SendTimeMs;
        var partitionTag = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["partition"] = record.Partition.ToString(CultureInfo.InvariantCulture),
        };
        SetGauge("consumer.e2e_latency", latencyMs, partitionTag);
        _metrics.ObserveMessage(record.SerializedValueSize, latencyMs);

        if (msgCount % _disprate == 0)
        {
            _logger.Info(string.Format(
                CultureInfo.InvariantCulture,
                "consumer: {0} messages consumed: Message {1} [{2}] at offset {3} (msgid {4}, txcnt {5}, latency {6:F1} ms)",
                msgCount,
                record.Topic,
                record.Partition,
                record.Offset,
                soakRecord.MsgId,
                soakRecord.TxCnt,
                latencyMs));
        }

        string hwKey = string.Format(CultureInfo.InvariantCulture, "{0}-{1}", record.Topic, record.Partition);
        (long duplicates, long missed) = hwmarks.Observe(hwKey, record.Offset);
        if (duplicates > 0)
        {
            _logger.Warning(string.Format(
                CultureInfo.InvariantCulture,
                "consumer: Old or duplicate message {0} [{1}] at offset {2}: wanted a higher offset ({3} duplicate(s), last committed {4})",
                record.Topic,
                record.Partition,
                record.Offset,
                duplicates,
                Volatile.Read(ref _lastCommitted) ?? "None"));
            Interlocked.Add(ref _msgDuplicateCount, duplicates);

            IncrCounter("consumer.msgdup", duplicates);
        }
        else if (missed > 0)
        {
            _logger.Warning(string.Format(
                CultureInfo.InvariantCulture,
                "consumer: Lost messages, now at {0} [{1}] offset {2}: {3} message(s) missed (last committed {4})",
                record.Topic,
                record.Partition,
                record.Offset,
                missed,
                Volatile.Read(ref _lastCommitted) ?? "None"));
            Interlocked.Add(ref _msgMissedCount, missed);
            IncrCounter("consumer.missedmsg", missed);
        }

        pending[new TopicPartition(record.Topic, record.Partition)] = new OffsetAndMetadata(record.Offset + 1);
    }

    private HashSet<TopicPartition> CheckAssignment(HashSet<TopicPartition> previous)
    {
        // The assignment is polled, as Python's soak, rather than reported by a rebalance listener;
        // the poll doubles as a liveness probe on Assignment().
        HashSet<TopicPartition> current;
        try
        {
            current = new HashSet<TopicPartition>(_consumer.Assignment());
        }
        catch (Exception ex)
        {
            _logger.Warning("consumer: assignment() failed: " + ErrorMessage(ex));
            return previous;
        }

        if (!current.SetEquals(previous))
        {
            Interlocked.Increment(ref _rebalanceCount);
            IncrCounter("consumer.rebalance", 1);
            SetGauge("consumer.assignment_size", current.Count);
            _logger.Info(string.Format(
                CultureInfo.InvariantCulture,
                "consumer: assignment changed: {0} partition(s): {1}",
                current.Count,
                string.Join(", ", current.Select(tp => tp.ToString()).OrderBy(s => s, StringComparer.Ordinal))));
        }

        return current;
    }

    private async Task CommitAsync(Dictionary<TopicPartition, OffsetAndMetadata> pending, CancellationToken cancellationToken)
    {
        if (pending.Count == 0)
        {
            return;
        }

        var offsets = new Dictionary<TopicPartition, OffsetAndMetadata>(pending);
        try
        {
            // The confirming commit (Java's commitSync), as Python's soak.
            await _consumer.Commit(offsets, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // As Python's soak: a commit aborted by the shutdown's cancel (which wakes one blocking
            // call) is retried once, not counted as a failure.
            _logger.Info("consumer: commit aborted by shutdown; retrying once");
            try
            {
                await _consumer.Commit(offsets, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception retryEx)
            {
                ClassifyError("consumer: offset commit failed", retryEx);
                return;
            }
        }
        catch (Exception ex)
        {
            ClassifyError("consumer: offset commit failed", ex);
            return;
        }

        Volatile.Write(
            ref _lastCommitted,
            string.Join(", ", offsets.Select(entry => string.Format(
                CultureInfo.InvariantCulture,
                "{0}-{1}={2}",
                entry.Key.Topic,
                entry.Key.Partition,
                entry.Value.Offset))));
        pending.Clear();
    }

    private void ClassifyError(string where, Exception ex)
    {
        Interlocked.Increment(ref _consumerErrorCount);
        IncrCounter("consumer.error", 1);

        int? code = ErrorCode(ex);
        string message = ErrorMessage(ex).ToLowerInvariant();
        if (code.HasValue && s_coordinatorErrorCodes.Contains(code.Value))
        {
            Interlocked.Increment(ref _coordinatorMoveCount);
            IncrCounter("consumer.coordinator_move", 1);
            _logger.Warning(string.Format(
                CultureInfo.InvariantCulture,
                "{0}: coordinator moved (code {1}): {2}",
                where,
                code.Value,
                ErrorMessage(ex)));
        }
        else if ((code.HasValue && s_disconnectErrorCodes.Contains(code.Value))
            || s_disconnectMessageMarkers.Any(marker => message.Contains(marker, StringComparison.Ordinal)))
        {
            Interlocked.Increment(ref _disconnectCount);
            IncrCounter("consumer.disconnect", 1);
            _logger.Warning(string.Format(
                CultureInfo.InvariantCulture,
                "{0}: broker/connection error (code {1}): {2}",
                where,
                code.HasValue ? code.Value.ToString(CultureInfo.InvariantCulture) : "None",
                ErrorMessage(ex)));
        }
        else
        {
            _logger.Error(string.Format(
                CultureInfo.InvariantCulture,
                "{0}: error (code {1}): {2}",
                where,
                code.HasValue ? code.Value.ToString(CultureInfo.InvariantCulture) : "None",
                ErrorMessage(ex)));
        }
    }

    private void ConsumerStatus()
    {
        _logger.Info(string.Format(
            CultureInfo.InvariantCulture,
            "consumer: {0} messages consumed, {1} duplicates, {2} missed, {3} message errors, {4} consumer errors, 0 error_cbs",
            Interlocked.Read(ref _msgCount),
            Interlocked.Read(ref _msgDuplicateCount),
            Interlocked.Read(ref _msgMissedCount),
            Interlocked.Read(ref _msgErrorCount),
            Interlocked.Read(ref _consumerErrorCount)));
    }

    private static Task CreateTopicAsync(
        IReadOnlyDictionary<string, string> adminConfig,
        SoakOptions options,
        SoakLogger logger) =>
        CreateTopicAsync(
            adminConfig, options, logger, config => new AsyncKafkaAdminClient(config), Task.Delay, Console.Out);

    internal static async Task CreateTopicAsync(
        IReadOnlyDictionary<string, string> adminConfig,
        SoakOptions options,
        SoakLogger logger,
        Func<IReadOnlyDictionary<string, string>, IAsyncAdmin> createAdmin,
        Func<TimeSpan, Task> delay,
        TextWriter output)
    {
        using IAsyncAdmin admin = createAdmin(adminConfig);

        if (options.RecreateTopic)
        {
            logger.Warning("SOAK_RECREATE_TOPIC: deleting and re-creating " + options.Topic);
            await RecreateTopicAsync(admin, options.Topic, options.Partitions, delay, output).ConfigureAwait(false);
            return;
        }

        // Create-if-absent by default: run.sh restarts the client repeatedly and a restart
        // must never discard the topic.
        var newTopic = new NewTopic(
            options.Topic,
            options.Partitions < 0 ? (int?)null : options.Partitions,
            options.ReplicationFactor < 0 ? (short?)null : (short)options.ReplicationFactor);

        IReadOnlyDictionary<string, AsyncKafkaFuture>? created = null;
        try
        {
            created = admin.CreateTopics(new[] { newTopic }).Values;
        }
        catch (Exception ex)
        {
            HandleCreateError(options.Topic, ex);
        }

        foreach (KeyValuePair<string, AsyncKafkaFuture> entry in created ?? new Dictionary<string, AsyncKafkaFuture>())
        {
            try
            {
                await entry.Value.Get().ConfigureAwait(false);
                logger.Info(string.Format(
                    CultureInfo.InvariantCulture,
                    "Created topic {0} (partitions={1}, rf={2})",
                    entry.Key,
                    options.Partitions,
                    options.ReplicationFactor));
            }
            catch (Exception ex)
            {
                HandleCreateError(entry.Key, ex);
            }
        }

        void HandleCreateError(string topic, Exception ex)
        {
            int? code = ErrorCode(ex);
            if (code == TopicAlreadyExistsCode)
            {
                logger.Info(string.Format(CultureInfo.InvariantCulture, "Topic {0} already exists: good", topic));
            }
            else if (code is int auth && s_authErrorCodes.Contains(auth))
            {
                throw new SoakFatalStartupException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "authentication/authorization failed creating topic '{0}': {1}. Check sasl.jaas.config "
                        + "(username/password) and the API key's ACLs. Restarting will not fix this.",
                        topic,
                        ErrorMessage(ex)),
                    ex);
            }
            else
            {
                throw new SoakTransientStartupException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "could not create or verify topic '{0}': {1}. If the cluster is reachable this may clear on retry.",
                        topic,
                        ErrorMessage(ex)),
                    ex);
            }
        }
    }

    private static async Task RecreateTopicAsync(
        IAsyncAdmin admin,
        string topic,
        int partitions,
        Func<TimeSpan, Task> delay,
        TextWriter output)
    {
        Print(">>> CREATE_TOPIC: deleting topic '" + topic + "' (ignored if absent) ...");
        IReadOnlyDictionary<string, AsyncKafkaFuture>? deleted = null;
        try
        {
            deleted = admin.DeleteTopics(
                TopicCollection.OfTopicNames(new[] { topic }),
                new DeleteTopicsOptions { TimeoutMs = 30000 }).TopicNameValues;
        }
        catch (Exception ex)
        {
            Classify("delete", topic, ex, UnknownTopicOrPartitionCode);
        }

        foreach (KeyValuePair<string, AsyncKafkaFuture> entry in deleted ?? new Dictionary<string, AsyncKafkaFuture>())
        {
            try
            {
                await entry.Value.Get().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Classify("delete", entry.Key, ex, UnknownTopicOrPartitionCode);
                continue;
            }

            Print(">>> deleted '" + entry.Key + "'");
        }

        Print(">>> waiting 10s after delete ...");
        await delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

        Print(string.Format(
            CultureInfo.InvariantCulture,
            ">>> CREATE_TOPIC: creating topic '{0}' (partitions={1}, rf=broker-default) ...",
            topic,
            partitions < 0 ? "broker-default" : partitions.ToString(CultureInfo.InvariantCulture)));
        var newTopic = new NewTopic(topic, partitions < 0 ? (int?)null : partitions, null);
        IReadOnlyDictionary<string, AsyncKafkaFuture>? created = null;
        try
        {
            created = admin.CreateTopics(new[] { newTopic }).Values;
        }
        catch (Exception ex)
        {
            Classify("create", topic, ex, TopicAlreadyExistsCode);
        }

        foreach (KeyValuePair<string, AsyncKafkaFuture> entry in created ?? new Dictionary<string, AsyncKafkaFuture>())
        {
            try
            {
                await entry.Value.Get().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Classify("create", entry.Key, ex, TopicAlreadyExistsCode);
                continue;
            }

            Print(">>> created '" + entry.Key + "'");
        }

        Print(">>> waiting 10s after create ...");
        await delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

        void Print(string line)
        {
            output.WriteLine(line);
            output.Flush();
        }

        void Classify(string op, string name, Exception ex, int benignCode)
        {
            int? code = ErrorCode(ex);
            if (code == benignCode)
            {
                Print(">>> '" + name + "' " + (op == "delete" ? "did not exist" : "already exists") + " (ok)");
            }
            else if (code is int auth && s_authErrorCodes.Contains(auth))
            {
                throw new SoakFatalStartupException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "authentication/authorization failed on {0} of topic '{1}': {2}. Check sasl.jaas.config "
                        + "(username/password) and the API key's ACLs. Restarting will not fix this.",
                        op,
                        name,
                        ErrorMessage(ex)),
                    ex);
            }
            else
            {
                throw new SoakTransientStartupException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "could not {0} topic '{1}': {2}. If the cluster is reachable this may clear on retry.",
                        op,
                        name,
                        ErrorMessage(ex)),
                    ex);
            }
        }
    }

    private static void LogBuildManifest(SoakLogger logger)
    {
        // Log the manifest build.sh wrote, so a 2-week run is traceable to an exact commit.
        string path = SoakEnv.GetStringOrNull("SOAK_BUILD_MANIFEST")
            ?? Path.Combine(AppContext.BaseDirectory, "build-manifest.json");
        string manifest;
        try
        {
            manifest = File.ReadAllText(path).Replace("\r", string.Empty, StringComparison.Ordinal)
                .Replace("\n", " ", StringComparison.Ordinal);
        }
        catch (Exception ex)
        {
            logger.Warning(string.Format(
                CultureInfo.InvariantCulture,
                "no build manifest at {0} ({1}); this build is not traceable to a commit",
                path,
                ex.Message));
            return;
        }

        logger.Info("build manifest: " + manifest);

        // Repeats build.sh's warning: this log outlives the manifest file.
        if (manifest.Contains("\"traceable\": false", StringComparison.Ordinal)
            || manifest.Contains("\"git_sha\": \"unknown\"", StringComparison.Ordinal))
        {
            logger.Warning(
                "BUILD IS NOT TRACEABLE TO A COMMIT: the manifest carries no git sha. Rebuild with "
                + "build.sh --src <dir> --sha <commit> if this run's results need to be attributed to code.");
        }
    }

    private async Task DelayAsync(TimeSpan delay)
    {
        try
        {
            await Task.Delay(delay, _stop.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Shutdown aborts the wait, which is the point.
        }
    }

    private static double MonotonicSeconds() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency;
}
