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
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Performance;

/// <summary>
/// The <c>MODE=transactional</c> engine, as Python's <c>transactional_producer_performance_test.py</c>: one worker per
/// transactional producer, each running transactions of <c>RECORDS_PER_TRANSACTION</c> records (<c>produce</c>) or of
/// the records it consumed from <c>SOURCE_TOPIC</c> (<c>eos</c>), committing them or aborting at <c>ABORT_RATE</c>.
/// </summary>
public sealed class TransactionalBenchmark
{
    /// <summary>The attempts a commit, abort or send-offsets call gets while it fails with a retriable error.</summary>
    public const int MaxTxnCallAttempts = 3;

    private const int CountedRunCapSlackSeconds = 10;
    private const int EosIdleTimeoutSeconds = 30;
    private const int EosStarvationWarnPolls = 10;
    private const int UsageExitCode = 64;

    private static readonly TimeSpan s_eosPollTimeout = TimeSpan.FromMilliseconds(500);

    private readonly ITransactionalClientFactory _factory;
    private readonly TransactionalBenchmarkConfig _config;
    private readonly ProducerBenchmarkConfig _producer;
    private readonly Metrics _metrics;
    private readonly PerfMessage[] _messages;
    private readonly object _lock = new();

    private readonly long[] _latencyHist = LatencyHistogram.New();
    private readonly long[] _commitHist = LatencyHistogram.New();
    private readonly long[] _abortHist = LatencyHistogram.New();
    private readonly TickStats _latency = new();
    private readonly TickStats _commitLatency = new();
    private readonly TickStats _abortLatency = new();

    private long _committedTransactions;
    private long _abortedTransactions;
    private long _abortedRecords;
    private long _errorAbortedTransactions;
    private long _completedMessages;
    private long _verified;
    private long _eosProcessed;
    private long _recordsSentTotal;
    private long _eosLastProgressTicks;
    private long _measuredStartTicks;
    private int _runFailed;
    private int _runCapped;

    private TransactionalBenchmark(ITransactionalClientFactory factory, TransactionalBenchmarkConfig config, Metrics metrics)
    {
        _factory = factory;
        _config = config;
        _producer = config.Producer;
        _metrics = metrics;
        _messages = MessageGenerator.Generate(_producer.KeySize, _producer.ValueSize, MessageGenerator.DefaultCount);
    }

    private bool RunFailed => Volatile.Read(ref _runFailed) != 0;

    /// <summary>Runs the whole benchmark and returns the exit code: 0 clean, 1 failed or over budget, 64 bad settings.</summary>
    public static async Task<int> Run(ITransactionalClientFactory factory, TransactionalBenchmarkConfig config)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(config);

        if (config.Eos)
        {
            if (config.SourceTopic is null)
            {
                Console.Error.WriteLine("TXN_MODE=eos requires SOURCE_TOPIC (the input topic to consume from). Aborting.");
                return UsageExitCode;
            }
        }
        else if (config.TxnMode != "produce")
        {
            Console.Error.WriteLine($"Unknown TXN_MODE '{config.TxnMode}' (expected 'produce' or 'eos'). Aborting.");
            return UsageExitCode;
        }

        ProducerBenchmarkConfig producer = config.Producer;
        if (producer.LimitRps is int rps)
        {
            Console.WriteLine($"Producing {producer.NumMessages} messages at {rps} msg/s (total)");
        }
        else if (producer.NumMessages > 0)
        {
            Console.WriteLine($"Producing {producer.NumMessages} messages at max rate");
        }
        else
        {
            Console.WriteLine($"Producing messages at max rate for {producer.TestDurationSeconds} seconds");
        }

        using Metrics metrics = Metrics.ForTransactional();
        metrics.StartCollecting(1);
        var benchmark = new TransactionalBenchmark(factory, config, metrics);
        return await benchmark.RunMeasured().ConfigureAwait(false);
    }

    /// <summary>Whether transaction <paramref name="txnIndex"/> aborts: evenly spread (Bresenham) at <paramref name="rate"/>.</summary>
    public static bool ShouldAbort(long txnIndex, double rate)
    {
        if (rate <= 0.0)
        {
            return false;
        }

        double i = txnIndex;
        return Math.Floor((i + 1.0) * rate) > Math.Floor(i * rate);
    }

    private async Task<int> RunMeasured()
    {
        Console.WriteLine($"Running transactional producer performance test {_factory.Label} (TXN_MODE={_config.TxnMode})...");
        PrintConfiguration();

        if (_producer.CreateTopic)
        {
            _factory.CreateTopic();
        }

        // The source is filled before the clock starts; without KAFKA_BIN it is assumed pre-populated.
        if (_config.Eos && _config.KafkaBin is not null)
        {
            SourceSeeder.Seed(_config, _config.SeedRecordCount());
        }

        if (_producer.WarmupSeconds > 0 && !await RunWarmup().ConfigureAwait(false))
        {
            _metrics.StopCollecting();
            Console.WriteLine("Run FAILED (see the ERROR lines above)");
            return 1;
        }

        _metrics.ResetCpuBaseline();
        long startMs = Metrics.NowMs();
        _metrics.SetMeasurementStart(startMs);
        Volatile.Write(ref _measuredStartTicks, Stopwatch.GetTimestamp());
        NoteEosProgress();
        Console.WriteLine($"Starting measured interval at {startMs} ms: {DateTime.UtcNow:o}");

        // Produce mode splits NUM_MESSAGES across the producers and starts only those with a share; EOS shares one
        // processed-record count, so every producer starts.
        int producers = _config.NumTransactionalProducers;
        var shares = new long[producers];
        if (_producer.NumMessages > 0 && !_config.Eos)
        {
            long baseShare = _producer.NumMessages / producers;
            long extra = _producer.NumMessages % producers;
            for (int k = 0; k < producers; k++)
            {
                shares[k] = baseShare + (k < extra ? 1 : 0);
            }
        }

        double perProducerRps = _producer.LimitRps is int rps ? (double)rps / producers : 0.0;
        var workers = new List<Task>(producers);
        for (int k = 0; k < producers; k++)
        {
            if (_producer.NumMessages > 0 && !_config.Eos && shares[k] == 0)
            {
                continue;
            }

            workers.Add(StartWorker(k, shares[k], perProducerRps));
        }

        await Task.WhenAll(workers).ConfigureAwait(false);

        long endMs = Metrics.NowMs();
        _metrics.SetMeasurementEnd(endMs);
        double measuredSecs = TicksToSeconds(Stopwatch.GetTimestamp() - Volatile.Read(ref _measuredStartTicks));

        if (Volatile.Read(ref _runCapped) != 0)
        {
            // Not a failure: results.json carries the real numbers and the driver flags the missed rate.
            long sent = _config.Eos ? Interlocked.Read(ref _eosProcessed) : Interlocked.Read(ref _recordsSentTotal);
            Console.WriteLine($"capped: sent {sent} of {_producer.NumMessages} records in {measuredSecs:F2} s (TEST_DURATION_SECONDS + {CountedRunCapSlackSeconds} s)");
        }

        long verified;
        long completed;
        long errorAborted;
        lock (_lock)
        {
            verified = _verified;
            completed = _completedMessages;
            errorAborted = _errorAbortedTransactions;
        }

        if (verified != completed && !PerfSignals.Terminating)
        {
            MarkRunFailed($"Verified messages {verified} does not match committed messages {completed}");
        }

        if (errorAborted > 0)
        {
            MarkRunFailed($"{errorAborted} transaction(s) aborted on an error, not by ABORT_RATE (see above)");
        }

        bool budgetExceeded = PrintSummary(measuredSecs);

        if (!PerfSignals.Terminating)
        {
            Console.WriteLine("Performing garbage collection...");
            GC.Collect();
        }

        Console.WriteLine("Waiting for final metrics collection...");
        if (!PerfSignals.Terminating)
        {
            Thread.Sleep(ProducerBenchmarkConfig.PostTestAwaitSeconds * 1000);
        }

        _metrics.StopCollecting();
        Console.WriteLine("Done");
        if (RunFailed)
        {
            Console.WriteLine("Run FAILED (see the ERROR lines above)");
        }

        return budgetExceeded || RunFailed ? 1 : 0;
    }

    // An awaited client runs each worker as a task; a blocking one gets a dedicated thread, on which every call of
    // the worker completes synchronously.
    private Task StartWorker(int index, long share, double rate)
    {
        Func<Task> worker = _config.Eos
            ? () => EosWorker(index, rate)
            : () => ProduceWorker(index, share, rate);
        return _factory.AwaitedControl
            ? Task.Run(worker)
            : Task.Factory.StartNew(
                () => worker().GetAwaiter().GetResult(),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
    }

    private async Task<bool> RunWarmup()
    {
        Console.WriteLine($"Warming up for {_producer.WarmupSeconds} seconds ...");
        ITransactionalProducer? producer = null;
        try
        {
            producer = _factory.CreateProducer(_config.TransactionalId + "-warmup");
            await producer.InitTransactions().ConfigureAwait(false);
            long end = Stopwatch.GetTimestamp() + (long)_producer.WarmupSeconds * Stopwatch.Frequency;
            long i = 0;
            while (Stopwatch.GetTimestamp() < end && !PerfSignals.Terminating)
            {
                await producer.BeginTransaction().ConfigureAwait(false);
                PerfMessage message = _messages[i % _messages.Length];
                object? pending = await producer.Send(_producer.TopicName, message.Key, message.Value).ConfigureAwait(false);
                if (pending is null)
                {
                    break;
                }

                await producer.CommitTransaction().ConfigureAwait(false);
                if (!await VerifyDelivered(producer, pending).ConfigureAwait(false))
                {
                    MarkRunFailed("Warmup failed due to message verification error");
                    return false;
                }

                await Sleep(0.1).ConfigureAwait(false);
                i++;
            }
        }
        catch (Exception e)
        {
            MarkRunFailed($"Warmup failed: {e.Message}");
            return false;
        }
        finally
        {
            if (producer is not null)
            {
                await CloseQuietly(producer, printMetrics: false).ConfigureAwait(false);
            }
        }

        Console.WriteLine("Warmup complete.");
        return true;
    }

    private async Task ProduceWorker(int index, long share, double rate)
    {
        string who = $"Producer {index}";
        ITransactionalProducer producer;
        try
        {
            producer = _factory.CreateProducer($"{_config.TransactionalId}-{index}");
        }
        catch (Exception e)
        {
            MarkRunFailed($"{who}: client creation failed: {e.Message}");
            return;
        }

        int n = _config.RecordsPerTransaction;
        var produceTicks = new long[n];
        var pending = new object?[n];
        long txnIndex = 0;
        long recordsSent = 0;
        try
        {
            await producer.InitTransactions().ConfigureAwait(false);
            while (!ProduceBoundReached(recordsSent, share))
            {
                // Paced at the transaction boundary only.
                await Pace(rate, recordsSent).ConfigureAwait(false);
                long beginTicks = Stopwatch.GetTimestamp();
                await producer.BeginTransaction().ConfigureAwait(false);

                SendOutcome sent = await SendRecords(producer, null, recordsSent, n, produceTicks, pending).ConfigureAwait(false);
                recordsSent += sent.Attempted;

                bool goOn;
                if (sent.Rejected is not null)
                {
                    // Never commit a partial transaction: abort it and fail the run.
                    MarkRunFailed($"{who}: produce rejected inside a transaction: {sent.Rejected.Message}");
                    goOn = await AbortAfterError(producer, who, sent.Attempted, true, sent.Rejected).ConfigureAwait(false);
                }
                else if (sent.Interrupted)
                {
                    goOn = await AbortAfterError(producer, who, sent.Attempted, false, null).ConfigureAwait(false);
                }
                else
                {
                    goOn = await EndTransaction(producer, who, txnIndex, beginTicks, n, produceTicks, pending).ConfigureAwait(false);
                }

                Array.Clear(pending);
                txnIndex++;
                if (!goOn)
                {
                    break;
                }
            }

            // The wait owed for the last transaction's records: without it a fast client ends a counted run early.
            if (!PerfSignals.Terminating && share > 0 && share <= recordsSent)
            {
                await Pace(rate, recordsSent).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            MarkRunFailed($"{who} failed: {e.Message}");
        }
        finally
        {
            Interlocked.Add(ref _recordsSentTotal, recordsSent);
            await CloseQuietly(producer, printMetrics: index == 0).ConfigureAwait(false);
        }
    }

    private async Task EosWorker(int index, double rate)
    {
        string who = $"EOS producer {index}";
        ITransactionalProducer producer;
        try
        {
            producer = _factory.CreateProducer($"{_config.TransactionalId}-{index}");
        }
        catch (Exception e)
        {
            MarkRunFailed($"{who}: producer creation failed: {e.Message}");
            return;
        }

        ITransactionalConsumer consumer;
        try
        {
            consumer = _factory.CreateConsumer();
        }
        catch (Exception e)
        {
            MarkRunFailed($"{who}: consumer creation failed: {e.Message}");
            await CloseQuietly(producer, printMetrics: false).ConfigureAwait(false);
            return;
        }

        int n = _config.RecordsPerTransaction;
        var produceTicks = new long[n];
        var pending = new object?[n];
        var offsets = new Dictionary<(string Topic, int Partition), long>();
        long txnIndex = 0;
        long recordsSent = 0;
        int emptyPolls = 0;
        bool starvationWarned = false;
        try
        {
            await producer.InitTransactions().ConfigureAwait(false);
            while (!EosBoundReached())
            {
                IReadOnlyList<SourceRecord> batch = await consumer.Poll(n, s_eosPollTimeout).ConfigureAwait(false);
                if (batch.Count == 0)
                {
                    // No empty transaction: loop, bounded by the run's bounds and, counting, by the idle net.
                    emptyPolls++;
                    if (emptyPolls >= EosStarvationWarnPolls && !starvationWarned)
                    {
                        Console.Error.WriteLine($"[WARN] EOS source starved: {emptyPolls} consecutive empty polls from the source topic — the source producer is not keeping up (the EOS throughput ceiling is min(source-produce-rate, txn-process-rate)); pre-populate/seed SOURCE_TOPIC with a high-throughput producer");
                        starvationWarned = true;
                    }

                    if (EosSourceExhausted(index))
                    {
                        break;
                    }

                    continue;
                }

                emptyPolls = 0;
                int batchRecords = batch.Count;
                await Pace(rate, recordsSent).ConfigureAwait(false);
                bool goOn = true;
                try
                {
                    long beginTicks = Stopwatch.GetTimestamp();
                    await producer.BeginTransaction().ConfigureAwait(false);
                    SendOutcome sent = await SendRecords(producer, batch, 0, batchRecords, produceTicks, pending).ConfigureAwait(false);
                    recordsSent += sent.Attempted;

                    if (sent.Rejected is not null)
                    {
                        // The source must not advance without its output: no offsets, no commit.
                        MarkRunFailed($"{who}: produce rejected inside a transaction: {sent.Rejected.Message}");
                        goOn = await AbortAfterError(producer, who, sent.Attempted, true, sent.Rejected).ConfigureAwait(false);
                    }
                    else if (sent.Interrupted)
                    {
                        goOn = await AbortAfterError(producer, who, sent.Attempted, false, null).ConfigureAwait(false);
                    }
                    else
                    {
                        offsets.Clear();
                        foreach (SourceRecord record in batch)
                        {
                            (string, int) key = (record.Topic, record.Partition);
                            long next = record.Offset + 1;
                            if (!offsets.TryGetValue(key, out long current) || next > current)
                            {
                                offsets[key] = next;
                            }
                        }

                        Exception? offsetsError = null;
                        try
                        {
                            await Retry(producer, () => producer.SendOffsetsToTransaction(offsets, consumer)).ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            offsetsError = e;
                        }

                        if (offsetsError is not null)
                        {
                            goOn = await OnControlError(producer, who, "send_offsets_to_transaction", offsetsError, batchRecords).ConfigureAwait(false);
                        }
                        else
                        {
                            goOn = await EndTransaction(producer, who, txnIndex, beginTicks, batchRecords, produceTicks, pending).ConfigureAwait(false);
                        }
                    }
                }
                finally
                {
                    // The batch's transaction is over, however it ended: its records count toward the shared bound.
                    Interlocked.Add(ref _eosProcessed, batchRecords);
                    Array.Clear(pending);
                    txnIndex++;
                    NoteEosProgress();
                }

                if (!goOn)
                {
                    break;
                }
            }

            if (!PerfSignals.Terminating && _producer.NumMessages > 0 && EosTargetReached())
            {
                await Pace(rate, recordsSent).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            MarkRunFailed($"{who} failed: {e.Message}");
        }
        finally
        {
            await CloseQuietly(producer, printMetrics: index == 0).ConfigureAwait(false);
            try
            {
                consumer.Close();
            }
            catch (Exception)
            {
                // As Python's close: a failed consumer close does not change the run's outcome.
            }

            consumer.Dispose();
        }
    }

    // Sends one transaction's records: the generated messages from messageBase on, or the polled source batch.
    private async Task<SendOutcome> SendRecords(
        ITransactionalProducer producer,
        IReadOnlyList<SourceRecord>? batch,
        long messageBase,
        int count,
        long[] produceTicks,
        object?[] pending)
    {
        string topic = _producer.TopicName;
        int attempted = 0;
        for (int r = 0; r < count; r++)
        {
            byte[]? key;
            byte[]? value;
            if (batch is null)
            {
                PerfMessage message = _messages[(messageBase + r) % _messages.Length];
                key = message.Key;
                value = message.Value;
            }
            else
            {
                key = batch[r].Key;
                value = batch[r].Value;
            }

            produceTicks[r] = Stopwatch.GetTimestamp();
            try
            {
                object? handle = await producer.Send(topic, key, value).ConfigureAwait(false);
                if (handle is null)
                {
                    return new SendOutcome(attempted, null, true);
                }

                pending[r] = handle;
            }
            catch (Exception e)
            {
                return new SendOutcome(attempted + 1, e, false);
            }

            attempted++;
        }

        return new SendOutcome(attempted, null, false);
    }

    // Aborts at ABORT_RATE, else commits. Returns false when the worker must stop.
    private async Task<bool> EndTransaction(
        ITransactionalProducer producer,
        string who,
        long txnIndex,
        long beginTicks,
        int records,
        long[] produceTicks,
        object?[] pending)
    {
        if (ShouldAbort(txnIndex, _config.AbortRate))
        {
            try
            {
                await Retry(producer, producer.AbortTransaction).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                MarkRunFailed($"{who} failed: abort_transaction: {e.Message}");
                return false;
            }

            // An aborted transaction's records count toward neither throughput nor latency.
            RecordAbort(Stopwatch.GetTimestamp() - beginTicks, records);
            return true;
        }

        Exception? commitError = null;
        try
        {
            await Retry(producer, producer.CommitTransaction).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            commitError = e;
        }

        if (commitError is not null)
        {
            return await OnControlError(producer, who, "commit_transaction", commitError, records).ConfigureAwait(false);
        }

        long commitTicks = Stopwatch.GetTimestamp();
        RecordCommit(commitTicks - beginTicks);
        for (int r = 0; r < records; r++)
        {
            bool ok = await VerifyDelivered(producer, pending[r]!).ConfigureAwait(false);
            RecordCommittedRecord(commitTicks - produceTicks[r], ok);
        }

        return true;
    }

    // A commit or send-offsets call that failed after its retries: still retriable, its outcome is unknown, so the
    // worker stops; otherwise the transaction is aborted (no abort is attempted after a fatal error).
    private async Task<bool> OnControlError(ITransactionalProducer producer, string who, string call, Exception error, int records)
    {
        if (producer.Classify(error) == TxnErrorKind.Retriable)
        {
            MarkRunFailed($"{who}: {call} still failing after {MaxTxnCallAttempts} attempts: {error.Message}");
            return false;
        }

        Console.WriteLine($"{call} error: {error.Message}");
        return await AbortAfterError(producer, who, records, true, error).ConfigureAwait(false);
    }

    // Aborts the open transaction after an error and counts it as an error-driven abort of `records` records
    // (`error` false: cut short by a signal). Returns false when the worker must stop: the error was fatal or the
    // abort failed.
    private async Task<bool> AbortAfterError(ITransactionalProducer producer, string who, int records, bool error, Exception? cause)
    {
        RecordErrorAbort(records, error);
        if (cause is not null && producer.Classify(cause) == TxnErrorKind.Fatal)
        {
            MarkRunFailed($"{who}: fatal error, the producer cannot go on: {cause.Message}");
            return false;
        }

        try
        {
            await Retry(producer, producer.AbortTransaction).ConfigureAwait(false);
            return true;
        }
        catch (Exception e)
        {
            MarkRunFailed($"{who}: abort_transaction error: {e.Message}");
            return false;
        }
    }

    // Runs a transaction call, again while it fails with a retriable error, up to MaxTxnCallAttempts attempts.
    private static async Task Retry(ITransactionalProducer producer, Func<ValueTask> call)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await call().ConfigureAwait(false);
                return;
            }
            catch (Exception e) when (attempt < MaxTxnCallAttempts
                && !PerfSignals.Terminating
                && producer.Classify(e) == TxnErrorKind.Retriable)
            {
                Console.WriteLine($"Retriable transaction error (attempt {attempt} of {MaxTxnCallAttempts}): {e.Message}");
            }
        }
    }

    private async Task<bool> VerifyDelivered(ITransactionalProducer producer, object pending)
    {
        try
        {
            PerfRecordMetadata meta = await producer.Delivered(pending).ConfigureAwait(false);
            if (!_producer.DoVerify)
            {
                return true;
            }

            // As Python: the librdkafka client wants a timestamp > 0, the Rust core >= 0.
            bool timestampOk = _producer.ClientVersion == "2" ? meta.Timestamp > 0 : meta.Timestamp >= 0;
            return meta.Offset >= 0 && meta.Partition >= 0 && meta.Topic == _producer.TopicName && timestampOk;
        }
        catch (Exception e)
        {
            Console.WriteLine($"Produce call resulted in exception: {e.Message}");
            return false;
        }
    }

    private static async Task CloseQuietly(ITransactionalProducer producer, bool printMetrics)
    {
        try
        {
            if (printMetrics)
            {
                producer.PrintCompressionRateAvg();
            }

            await producer.Close().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Producer close failed: {e.Message}");
        }
        finally
        {
            producer.Dispose();
        }
    }

    // Waits until the records sent so far are due at the producer's rate, measured from the interval's start.
    private Task Pace(double rate, long recordsSent)
    {
        if (rate <= 0)
        {
            return Task.CompletedTask;
        }

        double dueSeconds = recordsSent / rate;
        double elapsed = TicksToSeconds(Stopwatch.GetTimestamp() - Volatile.Read(ref _measuredStartTicks));
        return Sleep(dueSeconds - elapsed);
    }

    private async Task Sleep(double seconds)
    {
        if (seconds <= 0)
        {
            return;
        }

        TimeSpan wait = TimeSpan.FromSeconds(Math.Min(seconds, int.MaxValue / 1000.0));
        if (!_factory.AwaitedControl)
        {
            PerfSignals.Token.WaitHandle.WaitOne(wait);
            return;
        }

        try
        {
            await Task.Delay(wait, PerfSignals.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Terminating: the loop's bound check stops the worker.
        }
    }

    private bool ProduceBoundReached(long recordsSent, long share)
    {
        if (PerfSignals.Terminating)
        {
            return true;
        }

        return share > 0 ? recordsSent >= share || CountedRunCapped() : TimeBoundReached();
    }

    private bool EosBoundReached()
    {
        if (PerfSignals.Terminating)
        {
            return true;
        }

        return _producer.NumMessages > 0 ? EosTargetReached() || CountedRunCapped() : TimeBoundReached();
    }

    private bool EosTargetReached() => Interlocked.Read(ref _eosProcessed) >= _producer.NumMessages;

    private bool TimeBoundReached() => MeasuredSeconds() > _producer.TestDurationSeconds + 1;

    // A counted run stops at TEST_DURATION_SECONDS plus the slack, checked at a transaction boundary only.
    private bool CountedRunCapped()
    {
        if (MeasuredSeconds() < _producer.TestDurationSeconds + CountedRunCapSlackSeconds)
        {
            return false;
        }

        Volatile.Write(ref _runCapped, 1);
        return true;
    }

    // Counting, fails the run once no producer has made progress for EosIdleTimeoutSeconds short of NUM_MESSAGES.
    private bool EosSourceExhausted(int index)
    {
        if (_producer.NumMessages <= 0)
        {
            return false;
        }

        long idleTicks = Stopwatch.GetTimestamp() - Interlocked.Read(ref _eosLastProgressTicks);
        if (TicksToSeconds(idleTicks) < EosIdleTimeoutSeconds || EosTargetReached())
        {
            return false;
        }

        MarkRunFailed($"EOS producer {index}: source exhausted before NUM_MESSAGES was reached (no producer made progress for {EosIdleTimeoutSeconds}s, {Interlocked.Read(ref _eosProcessed)}/{_producer.NumMessages} processed)");
        return true;
    }

    private void NoteEosProgress() => Interlocked.Exchange(ref _eosLastProgressTicks, Stopwatch.GetTimestamp());

    private double MeasuredSeconds() => TicksToSeconds(Stopwatch.GetTimestamp() - Volatile.Read(ref _measuredStartTicks));

    private void MarkRunFailed(string reason)
    {
        Console.Error.WriteLine($"ERROR: {reason}");
        Volatile.Write(ref _runFailed, 1);
    }

    private void RecordCommit(long ticks)
    {
        double ms = TicksToMs(ticks);
        lock (_lock)
        {
            _metrics.AddTransaction();
            _metrics.AddCommitLatency(ms);
            _committedTransactions++;
            _commitLatency.Add(ticks);
            LatencyHistogram.Record(_commitHist, ms);
        }
    }

    private void RecordAbort(long ticks, int records)
    {
        double ms = TicksToMs(ticks);
        lock (_lock)
        {
            _metrics.AddAbortLatency(ms);
            _abortedTransactions++;
            _abortedRecords += records;
            _abortLatency.Add(ticks);
            LatencyHistogram.Record(_abortHist, ms);
        }
    }

    // An error-driven abort records no abort latency: only the ABORT_RATE path is measured.
    private void RecordErrorAbort(int records, bool error)
    {
        lock (_lock)
        {
            _abortedTransactions++;
            _abortedRecords += records;
            if (error)
            {
                _errorAbortedTransactions++;
            }
        }
    }

    private void RecordCommittedRecord(long ticks, bool ok)
    {
        double ms = TicksToMs(ticks);
        lock (_lock)
        {
            _metrics.AddLatency(ms);
            _metrics.AddMessages(1);
            _metrics.AddBytes(_producer.MessageSize);
            _completedMessages++;
            if (ok)
            {
                _verified++;
            }

            _latency.Add(ticks);
            LatencyHistogram.Record(_latencyHist, ms);
        }
    }

    private void PrintConfiguration()
    {
        Console.WriteLine($"Key size: {_producer.KeySize} bytes");
        Console.WriteLine($"Value size: {_producer.ValueSize} bytes");
        Console.WriteLine($"Records per transaction: {_config.RecordsPerTransaction}");
        Console.WriteLine($"Abort rate: {PythonFloat(_config.AbortRate)}");
        Console.WriteLine($"Transactional producers: {_config.NumTransactionalProducers}");
        Console.WriteLine($"Transactional id base: {_config.TransactionalId}");
        Console.WriteLine($"TXN_MODE: {_config.TxnMode}");
        if (_config.Eos)
        {
            Console.WriteLine($"Source topic: {_config.SourceTopic}");
            Console.WriteLine($"Consumer group id: {_config.GroupId}");
            Console.WriteLine(_config.KafkaBin is not null
                ? $"Source seeding: kafka-producer-perf-test.sh from {_config.KafkaBin} (throughput={_config.SeedThroughput}, num-records={_config.SeedRecordCount()})"
                : "Source seeding: none (KAFKA_BIN unset; SOURCE_TOPIC assumed pre-populated)");
        }

        Console.WriteLine($"Verify: {(_producer.DoVerify ? "True" : "False")}");
        Console.WriteLine("enable.idempotence: true (forced for transactions)");
        Console.WriteLine("Producer configuration:");
        foreach (KeyValuePair<string, string> kv in _factory.ProducerConfig(_config.TransactionalId + "-0"))
        {
            bool secret = kv.Key == "sasl.jaas.config" || kv.Key == "sasl.password";
            Console.WriteLine($"  {kv.Key}: {(secret ? "<hidden>" : kv.Value)}");
        }
    }

    // Prints the summary and writes results.json; returns whether the p99 per-record latency is over budget.
    private bool PrintSummary(double measuredSecs)
    {
        ExternalMetricsAggregations agg = _metrics.ExternalMetricsAggregations();
        bool external = agg.TotalExternalMetrics > 0;
        double avgCpu = external ? agg.AverageCpu : 0.0;
        double avgRssKib = external ? agg.AverageRss / 1024.0 : 0.0;

        long committed;
        long aborted;
        long abortedRecords;
        long errorAborted;
        long completed;
        string latencyJson;
        string commitJson;
        string abortJson;
        long p99;
        long commitP99;
        long abortP99;
        lock (_lock)
        {
            committed = _committedTransactions;
            aborted = _abortedTransactions;
            abortedRecords = _abortedRecords;
            errorAborted = _errorAbortedTransactions;
            completed = _completedMessages;
            latencyJson = Bucket(_latencyHist, _latency);
            commitJson = Bucket(_commitHist, _commitLatency);
            abortJson = Bucket(_abortHist, _abortLatency);
            p99 = LatencyHistogram.PercentileFromHist(_latencyHist, 0.99);
            commitP99 = LatencyHistogram.PercentileFromHist(_commitHist, 0.99);
            abortP99 = LatencyHistogram.PercentileFromHist(_abortHist, 0.99);
        }

        double msgRate = measuredSecs > 0 ? completed / measuredSecs : 0.0;
        double mibRate = measuredSecs > 0 ? completed * (double)_producer.MessageSize / (1024.0 * 1024.0) / measuredSecs : 0.0;
        double txnRate = measuredSecs > 0 ? committed / measuredSecs : 0.0;

        Console.WriteLine();
        Console.WriteLine($"Duration: {measuredSecs * 1000.0:F2} ms");
        if (external)
        {
            Console.WriteLine($"Average CPU: {avgCpu:F2} %");
            Console.WriteLine($"Average RSS: {avgRssKib:F2} KiB");
        }
        else
        {
            Console.WriteLine("No external metrics collected");
        }

        Console.WriteLine($"Committed transactions: {committed}");
        Console.WriteLine($"Aborted transactions: {aborted}");
        Console.WriteLine($"Aborted records: {abortedRecords}");
        Console.WriteLine($"Error-driven aborted transactions: {errorAborted}");
        Console.WriteLine($"Committed records: {completed}");
        if (_config.Eos)
        {
            Console.WriteLine($"EOS source records processed: {Interlocked.Read(ref _eosProcessed)}");
        }

        Console.WriteLine($"Average rate msg/s: {msgRate:F2} msg/s");
        Console.WriteLine($"Average rate MiB/s: {mibRate:F2} MiB/s");
        Console.WriteLine($"Transactions/s: {txnRate:F2}");
        Console.WriteLine($"p99 per-record latency: {p99} ms");
        Console.WriteLine($"p99 commit latency: {commitP99} ms");
        Console.WriteLine($"p99 abort latency: {abortP99} ms");

        var json = new StringBuilder(1024);
        json.Append("{\n");
        json.Append("  \"test\": \"producer\",\n");
        json.Append("  \"client\": ").Append(PerfFormat.JsonString(_factory.Client)).Append(",\n");
        json.Append("  \"topic\": ").Append(PerfFormat.JsonString(_producer.TopicName)).Append(",\n");
        json.Append("  \"messages_measured\": ").Append(PerfFormat.Num(completed)).Append(",\n");
        json.Append("  \"duration_s\": ").Append(PerfFormat.JsonFloat(measuredSecs)).Append(",\n");
        json.Append("  \"throughput_msg_s\": ").Append(PerfFormat.JsonFloat(msgRate)).Append(",\n");
        json.Append("  \"throughput_mib_s\": ").Append(PerfFormat.JsonFloat(mibRate)).Append(",\n");
        json.Append("  \"committed_transactions\": ").Append(PerfFormat.Num(committed)).Append(",\n");
        json.Append("  \"aborted_transactions\": ").Append(PerfFormat.Num(aborted)).Append(",\n");
        json.Append("  \"aborted_records\": ").Append(PerfFormat.Num(abortedRecords)).Append(",\n");
        json.Append("  \"error_aborted_transactions\": ").Append(PerfFormat.Num(errorAborted)).Append(",\n");
        json.Append("  \"transactions_per_s\": ").Append(PerfFormat.JsonFloat(txnRate)).Append(",\n");
        json.Append("  \"latency_ms\": ").Append(latencyJson).Append(",\n");
        json.Append("  \"commit_latency_ms\": ").Append(commitJson).Append(",\n");
        json.Append("  \"abort_latency_ms\": ").Append(abortJson).Append(",\n");
        json.Append("  \"cpu_avg_pct\": ").Append(PerfFormat.JsonFloat(avgCpu)).Append(",\n");
        json.Append("  \"rss_avg_kib\": ").Append(PerfFormat.JsonFloat(avgRssKib)).Append('\n');
        json.Append('}');

        // The machine-readable summary is the run's primary artifact: failing to write it fails the run.
        try
        {
            File.WriteAllText(_producer.ResultsFile, json.ToString());
            Console.WriteLine($"Results summary written to: {_producer.ResultsFile}");
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
        {
            MarkRunFailed($"Failed to write {_producer.ResultsFile}: {e.Message}");
        }

        if (_producer.P99LimitMs > 0 && p99 > _producer.P99LimitMs)
        {
            Console.WriteLine($"p99 per-record latency {p99} ms exceeds budget {_producer.P99LimitMs} ms");
            return true;
        }

        return false;
    }

    // One latency block: percentiles from the floor-ms histogram, min/avg/max exact.
    private static string Bucket(long[] hist, TickStats stats)
    {
        bool any = stats.Count > 0;
        var block = new StringBuilder(256);
        block.Append("{\n");
        block.Append("    \"min\": ").Append(PerfFormat.JsonFloat(any ? TicksToMs(stats.Min) : 0.0)).Append(",\n");
        block.Append("    \"avg\": ").Append(PerfFormat.JsonFloat(any ? TicksToMs(stats.Total) / stats.Count : 0.0)).Append(",\n");
        block.Append("    \"p50\": ").Append(PerfFormat.Num(LatencyHistogram.PercentileFromHist(hist, 0.50))).Append(",\n");
        block.Append("    \"p90\": ").Append(PerfFormat.Num(LatencyHistogram.PercentileFromHist(hist, 0.90))).Append(",\n");
        block.Append("    \"p95\": ").Append(PerfFormat.Num(LatencyHistogram.PercentileFromHist(hist, 0.95))).Append(",\n");
        block.Append("    \"p99\": ").Append(PerfFormat.Num(LatencyHistogram.PercentileFromHist(hist, 0.99))).Append(",\n");
        block.Append("    \"p999\": ").Append(PerfFormat.Num(LatencyHistogram.PercentileFromHist(hist, 0.999))).Append(",\n");
        block.Append("    \"max\": ").Append(PerfFormat.JsonFloat(any ? TicksToMs(stats.Max) : 0.0)).Append('\n');
        block.Append("  }");
        return block.ToString();
    }

    // As Python prints a float: an integral value keeps its ".0".
    private static string PythonFloat(double value)
    {
        string text = value.ToString("R", CultureInfo.InvariantCulture);
        return text.IndexOfAny(new[] { '.', 'E' }) >= 0 ? text : text + ".0";
    }

    private static double TicksToSeconds(long ticks) => ticks / (double)Stopwatch.Frequency;

    private static double TicksToMs(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;

    /// <summary>One transaction's send loop: records attempted, the rejection that stopped it, or a stop on a signal.</summary>
    private readonly struct SendOutcome
    {
        internal SendOutcome(int attempted, Exception? rejected, bool interrupted)
        {
            Attempted = attempted;
            Rejected = rejected;
            Interrupted = interrupted;
        }

        internal int Attempted { get; }

        internal Exception? Rejected { get; }

        internal bool Interrupted { get; }
    }

    /// <summary>A latency series' count, sum, min and max in stopwatch ticks, under the engine's lock.</summary>
    private sealed class TickStats
    {
        internal long Count { get; private set; }

        internal long Total { get; private set; }

        internal long Min { get; private set; } = long.MaxValue;

        internal long Max { get; private set; }

        internal void Add(long ticks)
        {
            Count++;
            Total += ticks;
            Min = Math.Min(Min, ticks);
            Max = Math.Max(Max, ticks);
        }
    }
}
