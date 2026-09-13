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

namespace Confluent.Kafka.Soak;

/// <summary>
/// A producer sending messages at a fixed rate and a consumer consuming them, each on
/// its own <see cref="Task"/>, printing their counters every ~10 seconds.
/// <para>
/// ⚠ Both loops are <c>async Task</c> joined with <c>Task.WhenAll</c> (PLAN D2), over the
/// <b>async</b> facade (<c>AsyncKafkaProducer</c> / <c>AsyncKafkaConsumer</c>) rather than
/// the sync one. The sync <c>IProducer.Send</c> blocks until the broker acks, which at
/// the HI profile (1000 msg/s) would serialise the whole send path; the Python soak's
/// <c>send()</c> returns a future and does not block on the ack, so the async facade is
/// also the faithful shape.
/// </para>
/// <para>
/// The key type is <c>byte[]</c> with <c>Serdes.ByteArray</c> and is always null —
/// Python's <c>ProducerRecord(topic, value)</c> has no key. The producer serializer is
/// invoke-on-null by design (Java-faithful), and <c>Serdes.ByteArray.Serialize</c>
/// returns null for null, which is the absent-key sentinel.
/// </para>
/// </summary>
internal sealed class SoakClient : IDisposable
{
    /// <summary>
    /// Identity of the thing being soaked: the Rust client driven through its .NET
    /// binding, as distinct from the Python-bound soak (<c>rust_python</c>), a future
    /// native-Rust soak (<c>rust</c>) and the librdkafka soaks.
    /// <para>
    /// Prometheus metric names must match <c>[a-zA-Z_:][a-zA-Z0-9_:]*</c>, and the
    /// OTLP→Prometheus translation replaces every invalid character with <c>_</c>. A
    /// spelling like <c>rust(dotnet)</c> would arrive as <c>rust_dotnet__</c> — note the
    /// DOUBLE underscore left by the two parentheses, easy to typo in a dashboard query
    /// and impossible to guess. <c>rust_dotnet</c> survives the translation unchanged.
    /// One constant drives both the metric prefix and the host tag.
    /// </para>
    /// </summary>
    internal const string SoakClientToken = "rust_dotnet";

    /// <summary>The prefix every exported instrument carries.</summary>
    internal const string MetricPrefix = "kafka.client.soak." + SoakClientToken + ".";

    /// <summary>
    /// Consecutive non-retriable poll failures before the consumer is declared wedged.
    /// <para>
    /// Deliberately &gt; 1: every <i>client-side</i> error (Timeout, Wakeup, IllegalState)
    /// reports <c>UnknownServerError</c>, which <c>Errors::is_retriable()</c>
    /// (src/common/protocol/errors.rs) excludes, so one non-retriable poll error is a
    /// routine timeout during a broker roll — exactly what a run against a rolled cluster
    /// must survive — not a permanent failure.
    /// </para>
    /// </summary>
    internal const int NonRetriablePollFailureLimit = 3;

    /// <summary>
    /// How long shutdown waits for the queued delivery-accounting continuations to run
    /// before giving up and reporting a possibly-short <c>delivered=</c> (74.4). Bounded
    /// so a continuation that never runs cannot wedge shutdown.
    /// </summary>
    internal const int DeliveryDrainBoundMs = 5000;

    /// <summary>Poll interval for the bounded drain above.</summary>
    private const int DrainPollIntervalMs = 10;

    // Protocol error codes (src/common/protocol/errors.rs) used to classify the errors a
    // broker roll produces. Client-side errors (Wakeup, Timeout, ...) all report
    // UnknownServerError (-1), so they are classified by message instead.
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

    private SoakClient(
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

        // Messages between sample log lines, and — divided by the rate — the seconds
        // between status lines (~10 s, matching the Python soak's documented behaviour).
        _disprate = Math.Max(1, (long)(options.Rate * 10));
        _statusIntervalSeconds = _disprate / options.Rate;
    }

    /// <summary>Number of records the consumer found missing — the headline failure.</summary>
    internal long MissedCount => Interlocked.Read(ref _msgMissedCount);

    /// <summary>Why a loop gave up, or null. Reported in SUMMARY and turned into an exit code.</summary>
    internal string? FatalReason => Volatile.Read(ref _fatalReason);

    /// <summary>The soak's logger, so the caller can report on the same stream.</summary>
    internal SoakLogger Logger => _logger;

    /// <summary>Cancelled when the soak should stop; the main loop waits on it.</summary>
    internal CancellationToken StopToken => _stop.Token;

    /// <summary>
    /// Builds the client and starts both loops. The order here is load-bearing and
    /// mirrors <c>SoakClient.__init__</c>: route and validate <b>all</b> configs
    /// <b>before</b> anything with a side effect (a rejected key must not leave a topic
    /// behind) → create the topic → construct both clients (before either loop starts, so
    /// a failure cannot leave a producer running with no consumer) → seed the zero-valued
    /// counters → take the RSS baseline → start the sampler → start both loops.
    /// <para>
    /// ⚠ RECORDED DEVIATION: this is an async factory rather than a constructor, because
    /// topic creation awaits and a C# constructor cannot. The ordering contract above is
    /// unchanged.
    /// </para>
    /// </summary>
    internal static async Task<SoakClient> CreateAsync(
        SoakOptions options,
        IReadOnlyDictionary<string, string> fileConfig,
        double rssAtStartupMiB)
    {
        var logger = new SoakLogger(options.LogLevel);

        // A unique metrics host id so several soaks on one box stay distinct. Same token
        // as the metric prefix — see SoakClientToken.
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

        // Returns null (having logged why) unless a real exporting pipeline was
        // established, so the startup line below cannot claim telemetry that is not
        // happening.
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
            // Route and validate all three configs BEFORE anything with a side effect.
            //
            // ⚠ RECORDED ASYMMETRY, deliberately preserved from the Python original: the
            // admin config is routed but NOT strict-validated against a key catalog (only
            // the producer and consumer are). KafkaAdminClient takes the same Java-shaped
            // config those two take, so there is no translation layer either — PLAN D6
            // deletes Python's jaas_field / jaas_credentials / librdkafka_admin_config
            // outright, and `sasl.jaas.config` flows through untouched.
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

            // RSS baseline *after* client construction: the process's RSS includes the
            // .NET runtime, the GC heap and the native Rust core, so absolute RSS growth
            // is not by itself attributable to the client. memory.rss.delta is measured
            // from here; the difference between the two baselines is what the client
            // itself costs at startup.
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

        // Counters that must appear in the metrics even while they stay at zero.
        // producer.errorcb / consumer.errorcb have no source in this client (it exposes no
        // error callback) and are emitted only so the dashboards ported from the Python
        // soak keep their series.
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

        // Mark the measurement as started so the CPU/RSS aggregation accumulates over the
        // whole run rather than treating every window as warmup.
        metrics.SetMeasurementStart(SoakMetrics.NowMs());
        metrics.StartCollecting(options.MetricsIntervalSeconds);

        built._producerTask = Task.Run(() => built.ProducerLoopAsync());
        built._consumerTask = Task.Run(() => built.ConsumerLoopAsync());
        return built;
    }

    // -- error inspection ------------------------------------------------------------
    // Read by shape rather than by catching a specific type, so the classification stays
    // unit-testable and so a non-Kafka exception (an I/O failure, a cancellation) is
    // classified rather than crashing the loop.

    /// <summary>The protocol error code of a client error, or null.</summary>
    internal static int? ErrorCode(Exception ex) => ex is KafkaException kafka ? kafka.Code : (int?)null;

    /// <summary>The message of a client error.</summary>
    internal static string ErrorMessage(Exception ex) => ex.Message;

    /// <summary>Whether a client error advertises itself as retriable.</summary>
    internal static bool ErrorIsRetriable(Exception ex) => ex is KafkaException kafka && kafka.IsRetriable;

    /// <summary>
    /// Whether a run of consecutive poll failures should end the run.
    /// <para>
    /// Unbounded retrying is the worst outcome for an unattended soak: the process stays
    /// alive, the producer keeps producing, nothing is consumed, and the SUMMARY line
    /// that adjudicates message loss is never reached. So the storm is bounded and
    /// escalates to a stop, which lets the verdict be printed and exits non-zero;
    /// <c>run.sh</c> then restarts (re-authenticating and re-joining the group), and its
    /// own rapid-failure bound catches a permanent condition.
    /// </para>
    /// <para>
    /// Two tiers, because <c>IsRetriable</c> cannot be trusted as a never-going-to-work
    /// signal here — see <see cref="NonRetriablePollFailureLimit"/>.
    /// </para>
    /// </summary>
    internal static bool PollFailureIsTerminal(Exception ex, int consecutive, int maxPollFailures, out string fatalReason) =>
        PollFailureIsTerminal(ErrorIsRetriable(ex), ErrorMessage(ex), consecutive, maxPollFailures, out fatalReason);

    /// <summary>
    /// What the consumer loop does with a poll failure.
    /// </summary>
    internal enum PollFailureAction
    {
        /// <summary>Count it and keep polling.</summary>
        Continue = 0,

        /// <summary>Do not count it at all — shutdown is already under way (74.3).</summary>
        Suppress = 1,

        /// <summary>Count it, then end the run so the supervisor restarts it.</summary>
        Abort = 2,
    }

    /// <summary>
    /// The whole poll-failure policy, in one place, over the facts it depends on.
    /// <para>
    /// ⚠ 74.3 — the shutdown check comes <b>first</b>, mirroring Python's
    /// <c>if not self.run: break</c> placed <i>before</i> <c>_classify_error</c>
    /// (<c>soakclient.py:1689-1691</c>). The common cancellation path is already covered
    /// by the typed <c>OperationCanceledException</c> catch at the call site (the binding
    /// maps a token cancel to one even when the core reports the wakeup as an error), so
    /// what this suppresses is the residual: a <i>genuine</i> broker error that happens to
    /// resolve the poll inside the shutdown window. Without it that error increments
    /// <c>consumer.error</c> — and possibly <c>consumer.disconnect</c> /
    /// <c>consumer.coordinator_move</c> — inflating the SUMMARY's <c>errors=</c> on an
    /// otherwise clean shutdown.
    /// </para>
    /// <para>
    /// Note the ordering is what carries the behaviour: suppression must outrank the
    /// terminal bound, so an error that would otherwise abort the run is still suppressed
    /// once shutdown has been requested — the run is ending anyway, and a fatal reason
    /// recorded there would turn a clean exit into <see cref="SoakExitCodes.ConsumerWedged"/>.
    /// </para>
    /// </summary>
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

    /// <summary>
    /// The decision itself, over the two facts it actually depends on. Split out so the
    /// retriable/non-retriable matrix is testable: the binding's <c>KafkaException</c>
    /// constructor that sets <c>Code</c> / <c>IsRetriable</c> is <c>internal</c> to
    /// <c>Confluent.Kafka</c>, so no test outside that assembly can fabricate a
    /// <i>retriable</i> one. Production reaches this through the overload above, so the
    /// tests drive the same code the soak runs (definition-of-done.md §12).
    /// </summary>
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

    // -- instrumentation -------------------------------------------------------------

    /// <summary>Increments a metric counter.</summary>
    internal void IncrCounter(string metricName, long increment, IReadOnlyDictionary<string, string>? tags = null) =>
        _metrics.IncrCounter(metricName, increment, tags);

    /// <summary>Records a metric gauge observation.</summary>
    internal void SetGauge(string metricName, double value, IReadOnlyDictionary<string, string>? tags = null) =>
        _metrics.SetGauge(metricName, value, tags);

    // -- lifecycle -------------------------------------------------------------------

    /// <summary>Stops both loops. Safe to call from a signal handler: nothing here blocks.</summary>
    internal void RequestStop()
    {
        // ⚠ ONE CancellationTokenSource is the whole stop mechanism (PLAN D7). Python
        // additionally carries a `_wakeup_sent` Event, because each consumer.wakeup()
        // arms the token again and therefore aborts one more blocking operation, and
        // shutdown routinely delivers two signals (a Ctrl-C reaching the process group
        // plus run.sh's own SIGTERM). A CancellationToken is idempotent BY CONSTRUCTION —
        // cancelling a cancelled source is a no-op — so that hand-rolled guard has no job
        // here and is deliberately not ported. Wakeup() is likewise never called: the
        // binding maps a token cancel to it internally (ffi-marshalling.md §B7).
        try
        {
            _stop.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Already torn down; a second stop request is a no-op.
        }
    }

    /// <summary>Stops both loops after a loop hit a fatal error.</summary>
    internal void Abort() => RequestStop();

    /// <summary>Terminates the producer and consumer and writes the final report.</summary>
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

        // ⚠ 74.4 — join the delivery-accounting continuations BEFORE the final window.
        // OnDelivery owns _deliveredCount / producer.drok / producer.drerr /
        // producer.latency and runs as a thread-pool continuation, so Close() joining the
        // pump only guarantees every Task is RESOLVED — not that its continuation has
        // RUN. Without this the SUMMARY can print `produced=N delivered=N-k` on a
        // perfectly clean shutdown (which reads as message loss to a human even with
        // verdict=PASS), and any IncrCounter landing after _metrics.Close() is silently
        // dropped. Python cannot have this: its flush() serves the delivery callbacks
        // on the calling thread, so dr_cnt is complete before final_report().
        //
        // The producer loop has already completed (the WhenAll above), so no NEW
        // continuation can be attached here — the count only falls, which is what makes
        // the bounded wait terminate rather than livelock.
        long undrained = await DrainCounterAsync(
            () => Interlocked.Read(ref _pendingDeliveries),
            TimeSpan.FromMilliseconds(DeliveryDrainBoundMs)).ConfigureAwait(false);
        if (undrained > 0)
        {
            // Bounded on purpose: the shutdown watchdog is the backstop, never the
            // normal path, so a continuation that never runs costs one log line and a
            // slightly short SUMMARY rather than a wedged shutdown.
            _logger.Warning(string.Format(
                CultureInfo.InvariantCulture,
                "producer: {0} delivery accounting continuation(s) did not run within {1} ms; "
                + "the SUMMARY's delivered= may be short by that many",
                undrained,
                DeliveryDrainBoundMs));
        }

        // Final resource usage and metrics window.
        FinalizeMetrics(_metrics, SampleResources, _logger);
        FinalReport();
    }

    /// <summary>
    /// Waits, bounded, for <paramref name="read"/> to reach zero; returns whatever it
    /// still reads when the bound expires. Never throws and never waits longer than
    /// <paramref name="bound"/> — the property that matters, since this runs on the
    /// shutdown path where the only backstop is the hard-exit watchdog.
    /// </summary>
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

    /// <summary>
    /// Closes the metrics pipeline down as a TOTAL no-throw boundary.
    /// <para>
    /// ⚠ 74.1 — the rollover thread's identical <c>WriteRecord(Rollover())</c> is
    /// guarded and this one was not, so the exact failure that guard exists for (a full
    /// disk — "entirely plausible on a two-week run") propagated out of <c>Main</c>:
    /// <c>FinalReport()</c> never ran, so the run produced <b>no SUMMARY line at all</b>
    /// — the soak's single adjudication output, lost on exactly the run that needs
    /// explaining — and the process exited with the runtime's unhandled-exception code
    /// rather than one of <see cref="SoakExitCodes"/>' five, bypassing the never-restart
    /// / message-loss distinctions <c>run.sh</c>'s policy is built on.
    /// </para>
    /// <para>
    /// The .NET port also added two throw sources Python's <c>get_rusage()</c> does not
    /// have — <c>Process.Refresh()</c> and <c>GC.GetTotalMemory</c>, where
    /// <c>resource.getrusage()</c> effectively cannot fail — which is why
    /// <paramref name="sampleResources"/> is inside the guard and not before it.
    /// </para>
    /// </summary>
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

    /// <summary>One-line verdict: only gaps are a hard failure.</summary>
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

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _producer.Dispose();
        _consumer.Dispose();
        _metrics.Dispose();
        _stop.Dispose();
    }

    // -- resource usage --------------------------------------------------------------

    /// <summary>Samples process resources and publishes the cpu.* / memory.* gauges.</summary>
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

        // Re-emitted every window even though they never change: a gauge sampled once
        // reports average=0 in every later window (an empty bucket averages to 0), which
        // reads as "the baseline is 0 MiB" rather than "no sample here". Two constants per
        // 10 s is cheaper than that ambiguity, and it lets a dashboard compute
        // rss - baseline in any window.
        SetGauge("memory.rss.baseline_imports", _rssAtStartupMiB);
        SetGauge("memory.rss.baseline_constructed", _baselineRssMiB);
        SetGauge("memory.rss.delta", sample.RssMiB - _baselineRssMiB);

        // ⚠ RENAMED from Python's memory.tracemalloc / .peak (PLAN D9). These are the
        // tracemalloc ANALOG — GC.GetTotalMemory(false), managed heap only — so RSS
        // climbing while this stays flat points at native/Rust growth, which is the soak's
        // headline question and the exact diagnostic the Python RSS-spike investigation
        // needed. Naming a .NET gauge `tracemalloc` would assert a Python mechanism that
        // is not running, so a dashboard author maps memory.gc_heap onto
        // memory.tracemalloc rather than reading a false claim.
        SetGauge("memory.gc_heap", sample.GcHeapMiB);
        SetGauge("memory.gc_heap.peak", sample.GcHeapPeakMiB);

        SetGauge("producer.outq", Interlocked.Read(ref _outstanding));
    }

    // -- producer --------------------------------------------------------------------

    private async Task ProducerLoopAsync()
    {
        Thread.CurrentThread.Name ??= "producer";
        try
        {
            // Batched pacing, ported from the Python soak's --perf path: produce
            // max(1, rate/100) records then sleep off the batch's remaining time budget.
            // At the soak's 80 msg/s the batch is 1 — the batching only matters if the
            // rate is raised, where a per-message sleep is below what the OS can honour.
            int batch = Math.Max(1, (int)(_options.Rate / 100));
            double batchIntervalSeconds = batch / _options.Rate;
            double nextStatus = MonotonicSeconds() + _statusIntervalSeconds;

            while (!_stop.IsCancellationRequested)
            {
                double started = MonotonicSeconds();

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

                double remaining = batchIntervalSeconds - (MonotonicSeconds() - started);
                if (remaining > 0)
                {
                    // A cancellable wait, never a bare delay: the pacing sleep must abort
                    // on shutdown.
                    await DelayAsync(TimeSpan.FromSeconds(remaining)).ConfigureAwait(false);
                }
            }

            // Wait for outstanding messages to be delivered. main() arms a shutdown
            // watchdog before joining, because Flush is not interruptible.
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

    private async Task ProduceRecordAsync()
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
                // ⚠ CancellationToken.None, deliberately, and NOT the shutdown token.
                // On the producer a cancel "cancels the *wait*, never aborts an enqueued
                // send" (ffi-marshalling.md §4 / §A7), so passing the stop token here
                // would discard the RESULT of a record the core still delivers: every
                // clean shutdown would count a spurious producer.drerr for the send in
                // flight at the time, and the delivery it actually made would go
                // unrecorded. MEASURED before this was corrected — a 40 s end-to-end run
                // reported `produced=3115 delivered=3114 ... errors=1` with
                // "producer: delivery failed: send cancelled". The loop already stops on
                // the token, and Flush() then drains what is outstanding, which is
                // exactly how the Python soak behaves (its send() takes no cancellation
                // at all).
                task = _producer.Send(producerRecord, CancellationToken.None);
            }
            catch (Exception ex)
            {
                // txcnt counts SEND ATTEMPTS: the loop only re-runs when Send itself
                // throws a retriable error. Note this is rarer here than in Python: the
                // async Send throws synchronously only for preconditions and serialization
                // failures, and surfaces operational failures — including the
                // max.block.ms admission timeout — through the returned Task instead
                // (ffi-marshalling.md §A1). The loop is kept because the contract it
                // encodes ("a retriable throw is one attempt, not a lost record") still
                // holds for whatever does throw.
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

            // ⚠ EVERY returned Task MUST have its exception observed (PLAN D3). A
            // fire-and-forget Task<RecordMetadata> that faults and is never observed
            // raises TaskScheduler.UnobservedTaskException at GC; at 1000 msg/s with a
            // broker roll in progress that is a continuous stream of unobserved faults.
            // This continuation is the literal analog of Python's
            // `future.add_done_callback(lambda f, t=sent_at: self._on_delivery(f, t))` —
            // it carries the per-send `sentAt` in the closure exactly as the Python lambda
            // does, AND OnDelivery reads task.Exception on the faulted path, which is what
            // marks it observed.
            // Counted BEFORE the continuation is attached, released in OnDelivery's
            // finally, so shutdown can tell "every Task resolved" (which Close() gives
            // it) from "every accounting continuation RAN" (which it does not) — 74.4.
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
        // Total by construction: this runs as a continuation with nobody to catch it, so
        // an escape would fault an unobserved continuation Task — the very failure mode
        // the continuation exists to prevent.
        try
        {
            Interlocked.Decrement(ref _outstanding);

            if (task.IsFaulted)
            {
                // Reading task.Exception is what OBSERVES the fault (attaching a
                // continuation alone does not).
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
            // In the finally, and last: the drain's contract is "the accounting has run",
            // so releasing before the counters are written would let the SUMMARY be read
            // while this continuation is still updating it (74.4).
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

    // -- consumer --------------------------------------------------------------------

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
                    // ⚠ PLAN D7: a CancellationToken cancel maps to the consumer's
                    // wakeup() internally and surfaces as OperationCanceledException, so
                    // this typed catch replaces Python's `_is_wakeup(ex)` message sniff.
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
            // Shutdown landed on an operation outside the poll (Subscribe, or a commit
            // that was not already retried). That is a clean stop, not a fatal error:
            // logging it FATAL would put a scary line in the log on every tidy shutdown.
            _logger.Info("consumer: stopped by shutdown");
        }
        catch (Exception ex)
        {
            _logger.Fatal("consumer: fatal exception: " + ex);
            Abort();
        }
        finally
        {
            // Best-effort final commit so a restart does not replay this window. The token
            // is already cancelled by now, so commit with None — the "retried once" path
            // below exists for a commit interrupted mid-run, not for teardown.
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
        // ⚠ Two malformed shapes, one counter (PLAN D4). A null Value is an ABSENT value
        // (a tombstone): the binding returns default(TValue) for one and does NOT call the
        // deserializer, so the loop must recognise it here — Python's
        // `deserialize(None)` raises ValueError("empty payload (None)") and is counted the
        // same way. A non-null but unparseable payload arrives as a MARKED record, because
        // a throwing IDeserializer would fault the whole Poll rather than one record.
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

            // Corrupt payload: don't count it as consumed and don't let it drive
            // hwmark/dup logic — a bad payload is not a gap.
            return;
        }

        long msgCount = Interlocked.Increment(ref _msgCount);
        IncrCounter("consumer.msg", 1);

        // End-to-end latency from the payload's send time.
        //
        // Recorded in MILLISECONDS and exported in SECONDS. SetGauge applies that
        // conversion itself for everything in SoakMetrics.SecondsOnExport, because the
        // value passed here also feeds a 1 ms-resolution histogram: dividing at THIS call
        // site sends every sample to bucket 0 and reports p50/p90/p99/p999 as zero.
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

            // The REAL duplicate count, not a flat 1: a dashboard built on this counter
            // alone would otherwise read as far fewer duplicates than actually occurred.
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
        // ⚠ PLAN D5: the assignment is POLLED, matching the Python original, rather than
        // registered through IConsumerRebalanceListener. The listener would be a genuine
        // improvement — this binding ships one — but its three methods are sync `void`,
        // fire on the core's callback-dispatcher thread with a no-throw obligation, and
        // block the rebalance until they return; that is a foreign-thread surface the
        // Python original never had, inside a phase that is otherwise a straight port.
        // Polling also doubles as a liveness probe on Assignment() itself, which a
        // listener does not replace. Recorded as a candidate improvement, not a gap.
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
            // The CONFIRMING commit (Java commitSync): CommitAsync() is fire-and-forget
            // and carries no completion this soak could count, which is the whole point of
            // the reference soak's on_commit callback.
            await _consumer.Commit(offsets, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // A commit aborted by shutdown is retried once rather than counted as a
            // failure (PLAN D7): cancellation maps to wakeup(), which aborts exactly one
            // blocking operation, and a cancel only ever comes from this client's own
            // shutdown path. If it lands between two polls it aborts a commit instead,
            // which would otherwise lose the window and log a spurious error on every
            // clean shutdown.
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

    // -- setup helpers ---------------------------------------------------------------

    private static async Task CreateTopicAsync(
        IReadOnlyDictionary<string, string> adminConfig,
        SoakOptions options,
        SoakLogger logger)
    {
        using var admin = new KafkaAdminClient(adminConfig);

        if (options.RecreateTopic)
        {
            logger.Warning("SOAK_RECREATE_TOPIC: deleting and re-creating " + options.Topic);
            await DeleteTopicAsync(admin, options.Topic, logger).ConfigureAwait(false);

            // The two sleeps let the delete/create metadata propagate across the cluster,
            // exactly as the reference recreate_topic does.
            await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        }

        // Create-if-absent by default: run.sh restarts the client repeatedly and a restart
        // must never discard the topic.
        var newTopic = new NewTopic(
            options.Topic,
            options.Partitions <= 0 ? (int?)null : options.Partitions,
            options.ReplicationFactor <= 0 ? (short?)null : (short)options.ReplicationFactor);

        CreateTopicsResult result = admin.CreateTopics(new[] { newTopic });
        try
        {
            await result.Values[options.Topic].ConfigureAwait(false);
            logger.Info(string.Format(
                CultureInfo.InvariantCulture,
                "Created topic {0} (partitions={1}, rf={2})",
                options.Topic,
                options.Partitions,
                options.ReplicationFactor));
        }
        catch (KafkaException ex)
        {
            if (ex.Code == TopicAlreadyExistsCode)
            {
                logger.Info(string.Format(CultureInfo.InvariantCulture, "Topic {0} already exists: good", options.Topic));
            }
            else if (s_authErrorCodes.Contains(ex.Code))
            {
                throw new SoakFatalStartupException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "authentication/authorization failed creating topic '{0}': {1}. Check sasl.jaas.config "
                        + "(username/password) and the API key's ACLs. Restarting will not fix this.",
                        options.Topic,
                        ex.Message),
                    ex);
            }
            else
            {
                throw new SoakTransientStartupException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "could not create or verify topic '{0}': {1}. If the cluster is reachable this may clear on retry.",
                        options.Topic,
                        ex.Message),
                    ex);
            }
        }

        if (options.RecreateTopic)
        {
            await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        }
    }

    private static async Task DeleteTopicAsync(KafkaAdminClient admin, string topic, SoakLogger logger)
    {
        DeleteTopicsResult deleted = admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { topic }));
        IReadOnlyDictionary<string, Task>? values = deleted.TopicNameValues;
        if (values is null || !values.TryGetValue(topic, out Task? deletion))
        {
            return;
        }

        try
        {
            await deletion.ConfigureAwait(false);
            logger.Info("deleted topic " + topic);
        }
        catch (KafkaException ex)
        {
            // UnknownTopicOrPartition (3) — deleting an absent topic is not an error here.
            if (ex.Code == 3)
            {
                logger.Info(string.Format(CultureInfo.InvariantCulture, "topic {0} did not exist (ok)", topic));
                return;
            }

            throw;
        }

        await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
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

        // build.sh warns at build time; repeat it here, because this log is what someone
        // reads in two weeks when the manifest file is long gone.
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
