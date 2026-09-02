// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package io.confluent.kafka.perftest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Transactional producer performance test (Phase 1, {@code TXN_MODE=produce}).
 *
 * Sibling of {@link ProducerPerformanceTest}, driving the transactional
 * producer API. It shares the configuration contract, message shape and
 * metrics-file schema (metrics.jsonl / results.json, via {@link
 * TransactionalMetrics}) of the sibling transactional producer performance
 * tests (tests/performance/transactional_producer_perf_test.rs and
 * bindings/c/tests/transactional_producer_perf_test.c) so results are
 * comparable across implementations.
 *
 * <p>Two modes selected by {@code TXN_MODE}:
 * <ul>
 *   <li>{@code produce} (Phase 1) — per producer thread: {@code beginTransaction}
 *   -> {@code send} x RECORDS_PER_TRANSACTION -> {@code commitTransaction} (or
 *   {@code abortTransaction} per the deterministic abort rule);</li>
 *   <li>{@code eos} (Phase 2) — a read-process-write (exactly-once) pipeline:
 *   {@code poll} up to {@code max.poll.records} (= RECORDS_PER_TRANSACTION) from
 *   {@code SOURCE_TOPIC} -> {@code beginTransaction} -> produce each transformed
 *   (identity/echo) record to {@code TOPIC_NAME} ->
 *   {@code sendOffsetsToTransaction} (last-consumed-offset + 1 per partition,
 *   with {@code consumer.groupMetadata()}) -> {@code commitTransaction} (or
 *   {@code abortTransaction}). An empty poll is skipped (no empty transaction),
 *   so a run with no source data terminates on the duration / message bound.</li>
 * </ul>
 * Aborted transactions still produce their records before aborting; their
 * records count toward neither throughput nor latency, and in EOS mode their
 * consumed offsets are NOT committed and the consumer is NOT sought back (a
 * benchmark simplification vs. a real EOS app).
 *
 * <p>Latency (both modes): per-record latency = a record's {@code send()} ->
 * the moment its transaction's commit completes (committed txns only).
 * Per-transaction commit latency = {@code beginTransaction} ->
 * {@code commitTransaction} completes.
 *
 * <p>Configuration via environment variables (same as {@link
 * ProducerPerformanceTest}) plus the transaction knobs
 * {@code RECORDS_PER_TRANSACTION} (default 100), {@code ABORT_RATE} (default
 * 0.0), {@code NUM_TRANSACTIONAL_PRODUCERS} (default 1), {@code TRANSACTIONAL_ID}
 * (default {@code perf-txn}), {@code TXN_MODE} (default {@code produce}) and, for
 * EOS, {@code SOURCE_TOPIC} (required) plus {@code GROUP_ID} (default
 * {@code <TRANSACTIONAL_ID>-eos-consumer}, shared across producers). All
 * producers' consumers share ONE group id so the coordinator divides the source
 * partitions among them (canonical EOS scaling). {@code enable.idempotence} and
 * {@code acks=all} are forced on (required for transactions);
 * {@code enable.auto.commit} is forced false on the consumer.
 *
 * <p>This harness is bundled in the same shadowJar as {@link
 * ProducerPerformanceTest}; run it with
 * {@code java -cp build/libs/java-perf-test-all.jar
 * io.confluent.kafka.perftest.TransactionalProducerPerformanceTest}.
 *
 * <p>NOTE: unlike {@link ProducerPerformanceTest}, this Phase-1 harness does not
 * implement the optional {@code VERIFY_CONSUMED} consumer-side verification;
 * verification is limited to per-record {@code RecordMetadata} on the committed
 * path.
 */
public class TransactionalProducerPerformanceTest {

    // --- Mutable state (shared across producer threads) ----------------------
    private static volatile boolean terminating = false;
    private static final AtomicLong completedMessages = new AtomicLong(0); // committed records
    private static final AtomicLong verified = new AtomicLong(0);
    private static final AtomicLong committedTransactions = new AtomicLong(0);
    private static final AtomicLong abortedTransactions = new AtomicLong(0);
    private static final AtomicLong abortedRecords = new AtomicLong(0);

    // Summary histograms + running totals, guarded by HIST_LOCK (written by all
    // producer threads; contention is low — once per committed record / txn).
    private static final Object HIST_LOCK = new Object();
    private static long maxLatencyMs = 0;
    private static long totalLatencyMs = 0;
    private static long maxCommitLatencyMs = 0;
    private static long totalCommitLatencyMs = 0;

    // --- Config from env -----------------------------------------------------
    private static final int KEY_SIZE = envInt("KEY_SIZE", 0);
    private static final int VALUE_SIZE = envInt("VALUE_SIZE", 2048);
    private static final int MESSAGE_SIZE = KEY_SIZE + VALUE_SIZE;
    private static final String TOPIC_NAME = envOr("TOPIC_NAME", "test-topic");
    private static final long LIMIT_RPS = envLong("LIMIT_RPS", 0);
    private static final int WARMUP_SECONDS = envInt("WARMUP_SECONDS", 120);
    private static final int TEST_DURATION_SECONDS = envInt("TEST_DURATION_SECONDS", 600);
    private static final long NUM_MESSAGES = envLong("NUM_MESSAGES", 0);
    private static final boolean DO_VERIFY = envBool("DO_VERIFY", true);
    private static final boolean CREATE_TOPIC = envBool("CREATE_TOPIC", true);
    private static final int PARTITIONS = envInt("PARTITIONS", -1);
    private static final long P99_LIMIT_MS = envLong("P99_LIMIT_MS", 0);
    private static final String RESULTS_FILE = envOr("RESULTS_FILE", "results.json");
    private static final int MAX_LATENCY_MS = 10_000;
    private static final int POST_TEST_AWAIT_SECONDS = 10;
    private static final long[] latencyHist = new long[MAX_LATENCY_MS + 2];
    private static final long[] commitLatencyHist = new long[MAX_LATENCY_MS + 2];
    private static boolean latencyBudgetExceeded = false;

    // --- Transaction knobs ---
    private static final long RECORDS_PER_TRANSACTION = Math.max(1, envLong("RECORDS_PER_TRANSACTION", 100));
    private static final double ABORT_RATE = Math.min(1.0, Math.max(0.0, envDouble("ABORT_RATE", 0.0)));
    private static final int NUM_TRANSACTIONAL_PRODUCERS = Math.max(1, envInt("NUM_TRANSACTIONAL_PRODUCERS", 1));
    private static final String TRANSACTIONAL_ID = envOr("TRANSACTIONAL_ID", "perf-txn");
    private static final String TXN_MODE = envOr("TXN_MODE", "produce");

    // --- EOS-mode knobs (TXN_MODE=eos) ---
    private static final boolean EOS_MODE = "eos".equals(TXN_MODE);
    // Input topic consumed/transformed in EOS mode (required when TXN_MODE=eos).
    // Assumed pre-populated: this harness needs a reachable broker and does not
    // seed the source topic (unlike the Rust in-suite path).
    private static final String SOURCE_TOPIC = System.getenv("SOURCE_TOPIC");
    // Consumer group id shared across all producers' consumers, so the group
    // coordinator divides the source partitions among them (canonical EOS scaling).
    private static final String GROUP_ID = envOr("GROUP_ID", TRANSACTIONAL_ID + "-eos-consumer");
    // Per-poll timeout: bounds consumer.poll so a run with no source data cannot
    // block forever.
    private static final Duration EOS_POLL_TIMEOUT = Duration.ofMillis(500);

    public static void main(String[] args) throws Exception {
        installSignalHandler();

        // Two modes: `produce` (Phase 1) and `eos` (Phase 2). `eos` requires a
        // SOURCE_TOPIC; any other value is rejected.
        if (EOS_MODE) {
            if (SOURCE_TOPIC == null || SOURCE_TOPIC.isEmpty()) {
                System.err.println(
                    "TXN_MODE=eos requires SOURCE_TOPIC (the input topic to consume from). Aborting.");
                System.exit(2);
            }
        } else if (!"produce".equals(TXN_MODE)) {
            System.err.println("Unknown TXN_MODE '" + TXN_MODE + "' (expected 'produce' or 'eos'). Aborting.");
            System.exit(2);
        }

        if (LIMIT_RPS == 0) {
            if (NUM_MESSAGES > 0) {
                System.out.println("Producing " + NUM_MESSAGES + " messages at max rate");
            } else {
                System.out.println("Producing messages at max rate for " + TEST_DURATION_SECONDS + " seconds");
            }
        } else {
            System.out.println("Producing " + NUM_MESSAGES + " messages at " + LIMIT_RPS + " msg/s (total)");
        }

        TransactionalMetrics metrics = new TransactionalMetrics();
        metrics.startCollecting(1);

        System.out.println("Running transactional producer performance test java "
            + "(kafka-clients, TXN_MODE=" + TXN_MODE + ")...");

        Properties baseConf = configurationFromEnv();
        printConfiguration(baseConf);

        if (CREATE_TOPIC) {
            recreateTopic(baseConf, TOPIC_NAME, PARTITIONS);
        }

        // Pre-generate messages, shared across producers.
        final List<KeyValue> generated = generateMessages(10_000, KEY_SIZE, VALUE_SIZE);

        // === WARMUP === (single producer, a few single-record transactions).
        if (WARMUP_SECONDS > 0) {
            runWarmup(baseConf, generated);
        }

        // === MEASURED INTERVAL ===
        long beforeMs = System.currentTimeMillis();
        final long firstMessageNs = System.nanoTime();
        metrics.measurementStartMs = beforeMs;
        System.out.println("Starting measured interval at " + beforeMs + " ms");

        // Split the total rate / message target evenly across producers.
        final long perProducerNumMessages = NUM_MESSAGES > 0 ? NUM_MESSAGES / NUM_TRANSACTIONAL_PRODUCERS : 0;
        final long perProducerRps = LIMIT_RPS > 0 ? Math.max(LIMIT_RPS / NUM_TRANSACTIONAL_PRODUCERS, 1) : 0;

        List<Thread> threads = new ArrayList<>();
        for (int k = 0; k < NUM_TRANSACTIONAL_PRODUCERS; k++) {
            final int index = k;
            Runnable body = EOS_MODE
                ? () -> runEosProducer(index, baseConf, metrics, firstMessageNs,
                    perProducerNumMessages, perProducerRps)
                : () -> runProducer(index, baseConf, generated, metrics, firstMessageNs,
                    perProducerNumMessages, perProducerRps);
            Thread t = new Thread(body, "txn-producer-" + k);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        long afterMs = System.currentTimeMillis();
        metrics.measurementEndMs = afterMs;

        long committed = completedMessages.get();
        long verifiedCount = verified.get();
        if (verifiedCount != committed && !terminating) {
            System.out.println("Verified messages " + verifiedCount
                + " does not match committed messages " + committed);
        }

        double durationS = (afterMs - beforeMs) / 1000.0;
        double rate = durationS > 0 ? committed / durationS : 0.0;
        double mibRate = durationS > 0 ? (committed * (double) MESSAGE_SIZE) / (1024.0 * 1024.0) / durationS : 0.0;
        double txnRate = durationS > 0 ? committedTransactions.get() / durationS : 0.0;
        double avgLatencyMs = committed == 0 ? 0 : (double) totalLatencyMs / committed;
        long committedTxns = committedTransactions.get();
        double avgCommitMs = committedTxns == 0 ? 0 : (double) totalCommitLatencyMs / committedTxns;
        long p99Ms = percentileFromHist(latencyHist, 0.99);

        int samples = metrics.totalExternalMetrics;
        double avgCpu = samples > 0 ? metrics.totalCpu / samples : 0.0;
        double avgRssKib = samples > 0 ? metrics.totalRss / samples / 1024.0 : 0.0;

        System.out.println();
        System.out.println("Duration: " + (afterMs - beforeMs) + " ms");
        System.out.println("Average CPU: " + String.format("%.2f %%", avgCpu));
        System.out.println("Average RSS: " + String.format("%.2f KiB", avgRssKib));
        System.out.println("CPU efficiency: " + String.format("%.2f msg/(s * 1%% CPU)",
            rate / (avgCpu > 0 ? avgCpu : 1.0)));
        System.out.println("Memory efficiency: " + String.format("%.2f msg/(s * KB RSS)",
            rate / (avgRssKib > 0 ? avgRssKib : 1.0)));
        System.out.println("Committed transactions: " + committedTxns);
        System.out.println("Aborted transactions: " + abortedTransactions.get());
        System.out.println("Aborted records: " + abortedRecords.get());
        System.out.println("Committed records: " + committed);
        System.out.println("Average rate msg/s: " + String.format("%.2f msg/s", rate));
        System.out.println("Average rate MiB/s: " + String.format("%.2f MiB/s", mibRate));
        System.out.println("Transactions/s: " + String.format("%.2f", txnRate));
        System.out.println("Average per-record latency: " + String.format("%.2f ms", avgLatencyMs));
        System.out.println("Max per-record latency: " + maxLatencyMs + " ms");
        System.out.println("p99 per-record latency: " + p99Ms + " ms");
        System.out.println("Average commit latency: " + String.format("%.2f ms", avgCommitMs));
        System.out.println("p99 commit latency: " + percentileFromHist(commitLatencyHist, 0.99) + " ms");
        if (P99_LIMIT_MS > 0 && p99Ms > P99_LIMIT_MS && !terminating) {
            System.err.println("p99 per-record latency " + p99Ms + " ms exceeds " + P99_LIMIT_MS + " ms budget");
            latencyBudgetExceeded = true;
        }

        // Machine-readable summary (same file/keys/latency_ms shape as the sibling
        // tests) plus the transaction-specific fields. client = "java-txn".
        long minLatencyMs = 0;
        for (int i = 0; i < latencyHist.length; i++) {
            if (latencyHist[i] > 0) { minLatencyMs = i; break; }
        }
        long minCommitMs = 0;
        for (int i = 0; i < commitLatencyHist.length; i++) {
            if (commitLatencyHist[i] > 0) { minCommitMs = i; break; }
        }
        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("min", minLatencyMs);
        latency.put("avg", Math.round(avgLatencyMs * 100.0) / 100.0);
        latency.put("p50", percentileFromHist(latencyHist, 0.50));
        latency.put("p90", percentileFromHist(latencyHist, 0.90));
        latency.put("p95", percentileFromHist(latencyHist, 0.95));
        latency.put("p99", p99Ms);
        latency.put("p999", percentileFromHist(latencyHist, 0.999));
        latency.put("max", maxLatencyMs);
        Map<String, Object> commitLatency = new LinkedHashMap<>();
        commitLatency.put("min", minCommitMs);
        commitLatency.put("avg", Math.round(avgCommitMs * 100.0) / 100.0);
        commitLatency.put("p50", percentileFromHist(commitLatencyHist, 0.50));
        commitLatency.put("p90", percentileFromHist(commitLatencyHist, 0.90));
        commitLatency.put("p95", percentileFromHist(commitLatencyHist, 0.95));
        commitLatency.put("p99", percentileFromHist(commitLatencyHist, 0.99));
        commitLatency.put("p999", percentileFromHist(commitLatencyHist, 0.999));
        commitLatency.put("max", maxCommitLatencyMs);
        Map<String, Object> results = new LinkedHashMap<>();
        results.put("test", "producer");
        results.put("client", "java-txn");
        results.put("topic", TOPIC_NAME);
        results.put("messages_measured", committed);
        results.put("duration_s", Math.round(durationS * 100.0) / 100.0);
        results.put("throughput_msg_s", Math.round(rate * 100.0) / 100.0);
        results.put("throughput_mib_s", Math.round(mibRate * 100.0) / 100.0);
        results.put("committed_transactions", committedTxns);
        results.put("aborted_transactions", abortedTransactions.get());
        results.put("aborted_records", abortedRecords.get());
        results.put("transactions_per_s", Math.round(txnRate * 100.0) / 100.0);
        results.put("latency_ms", latency);
        results.put("commit_latency_ms", commitLatency);
        results.put("cpu_avg_pct", Math.round(avgCpu * 100.0) / 100.0);
        results.put("rss_avg_kib", Math.round(avgRssKib * 100.0) / 100.0);
        try {
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(new File(RESULTS_FILE), results);
            System.out.println("Results summary written to: " + RESULTS_FILE);
        } catch (IOException e) {
            System.err.println("Failed to write " + RESULTS_FILE + ": " + e);
        }

        if (!terminating) {
            System.out.println("Performing garbage collection...");
            System.gc();
        }
        System.out.println("Waiting for final metrics collection...");
        if (!terminating) {
            try {
                Thread.sleep(POST_TEST_AWAIT_SECONDS * 1000L);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        metrics.stopCollecting();
        System.out.println("Done");

        System.exit(latencyBudgetExceeded ? 1 : 0);
    }

    // === Per-producer worker =================================================

    private static void runProducer(int index, Properties baseConf, List<KeyValue> generated,
                                    TransactionalMetrics metrics, long firstMessageNs,
                                    long perProducerNumMessages, long perProducerRps) {
        Properties conf = new Properties();
        conf.putAll(baseConf);
        // Unique transactional.id per producer; transactions require idempotence.
        conf.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID + "-" + index);
        conf.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        conf.put(ProducerConfig.ACKS_CONFIG, "all");

        long rateCheckpoint = perProducerRps > 0 ? Math.max(perProducerRps / 10, 1) : 0;
        long nextCheckNs = firstMessageNs + 100_000_000L;
        long txnIndex = 0;
        long recordsSent = 0;
        int n = (int) RECORDS_PER_TRANSACTION;

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(conf)) {
            producer.initTransactions();

            boolean continueSending =
                perProducerNumMessages > 0 ? recordsSent < perProducerNumMessages : !terminating;
            while (continueSending) {
                long beginMs = System.currentTimeMillis();
                producer.beginTransaction();

                long[] produceMs = new long[n];
                List<Future<RecordMetadata>> futures = new ArrayList<>(n);
                for (int r = 0; r < n; r++) {
                    KeyValue m = generated.get((int) (recordsSent % generated.size()));
                    produceMs[r] = System.currentTimeMillis();
                    futures.add(producer.send(new ProducerRecord<>(TOPIC_NAME, m.key, m.value)));
                    recordsSent++;

                    if (perProducerRps > 0 && recordsSent % rateCheckpoint == 0) {
                        long now = System.nanoTime();
                        if (now < nextCheckNs) {
                            long sleepNs = nextCheckNs - now;
                            try {
                                Thread.sleep(sleepNs / 1_000_000L, (int) (sleepNs % 1_000_000L));
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        nextCheckNs += 100_000_000L;
                    }
                }

                if (shouldAbort(txnIndex, ABORT_RATE)) {
                    // Aborted transactions STILL produced their N records above;
                    // on abort they count toward neither throughput nor latency.
                    producer.abortTransaction();
                    abortedTransactions.incrementAndGet();
                    abortedRecords.addAndGet(n);
                } else {
                    producer.commitTransaction();
                    long commitMs = System.currentTimeMillis();
                    committedTransactions.incrementAndGet();

                    long commitLatency = commitMs - beginMs;
                    metrics.transactions.addMeasurement(1);
                    metrics.commitLatency.addMeasurement(commitLatency);
                    synchronized (HIST_LOCK) {
                        int ci = (int) Math.min(Math.max(commitLatency, 0), MAX_LATENCY_MS + 1);
                        commitLatencyHist[ci]++;
                        totalCommitLatencyMs += commitLatency;
                        if (commitLatency > maxCommitLatencyMs) maxCommitLatencyMs = commitLatency;
                    }

                    for (int r = 0; r < n; r++) {
                        try {
                            verifyRecordMetadata(futures.get(r).get());
                        } catch (Exception e) {
                            System.out.println("Produce call resulted in exception: " + e.getMessage());
                        }
                        completedMessages.incrementAndGet();
                        long latency = commitMs - produceMs[r];
                        metrics.latency.addMeasurement(latency);
                        metrics.messages.addMeasurement(1);
                        metrics.bytes.addMeasurement(MESSAGE_SIZE);
                        synchronized (HIST_LOCK) {
                            int li = (int) Math.min(Math.max(latency, 0), MAX_LATENCY_MS + 1);
                            latencyHist[li]++;
                            totalLatencyMs += latency;
                            if (latency > maxLatencyMs) maxLatencyMs = latency;
                        }
                    }
                }

                txnIndex++;

                if (perProducerNumMessages <= 0) {
                    long durationNs = System.nanoTime() - firstMessageNs;
                    if (durationNs > (long) TEST_DURATION_SECONDS * 1_000_000_000L) {
                        break;
                    }
                }
                continueSending = !terminating;
                if (perProducerNumMessages > 0) {
                    continueSending = continueSending && recordsSent < perProducerNumMessages;
                }
            }
        } catch (Exception e) {
            System.err.println("Producer " + index + " failed: " + e.getMessage());
        }
    }

    // === EOS (read-process-write) per-producer worker ========================

    private static void runEosProducer(int index, Properties baseConf, TransactionalMetrics metrics,
                                       long firstMessageNs, long perProducerNumMessages, long perProducerRps) {
        Properties pconf = new Properties();
        pconf.putAll(baseConf);
        // Unique transactional.id per producer; transactions require idempotence.
        pconf.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID + "-" + index);
        pconf.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        pconf.put(ProducerConfig.ACKS_CONFIG, "all");

        Properties cconf = consumerConfigFromEnv(baseConf);

        long rateCheckpoint = perProducerRps > 0 ? Math.max(perProducerRps / 10, 1) : 0;
        long nextCheckNs = firstMessageNs + 100_000_000L;
        long txnIndex = 0;
        long recordsSent = 0;

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(pconf);
             KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(cconf)) {
            producer.initTransactions();
            consumer.subscribe(Collections.singletonList(SOURCE_TOPIC));

            boolean continueSending =
                perProducerNumMessages > 0 ? recordsSent < perProducerNumMessages : !terminating;
            while (continueSending) {
                // --- consume (bounded poll; an empty poll is skipped, never
                // opens an empty transaction, and never blocks forever) ---
                ConsumerRecords<byte[], byte[]> records = consumer.poll(EOS_POLL_TIMEOUT);
                if (records.isEmpty()) {
                    if (perProducerNumMessages <= 0) {
                        long durationNs = System.nanoTime() - firstMessageNs;
                        if (durationNs > (long) TEST_DURATION_SECONDS * 1_000_000_000L) {
                            break;
                        }
                    }
                    continueSending = !terminating;
                    if (perProducerNumMessages > 0) {
                        continueSending = continueSending && recordsSent < perProducerNumMessages;
                    }
                    continue;
                }

                long beginMs = System.currentTimeMillis();
                producer.beginTransaction();

                List<Long> produceMs = new ArrayList<>();
                List<Future<RecordMetadata>> futures = new ArrayList<>();
                // Standard Kafka EOS offset bookkeeping: commit last-consumed
                // offset + 1 per TopicPartition.
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                long batch = 0;
                for (ConsumerRecord<byte[], byte[]> rec : records) {
                    produceMs.add(System.currentTimeMillis());
                    // Transform (identity/echo) and produce to the destination topic.
                    futures.add(producer.send(new ProducerRecord<>(TOPIC_NAME, rec.key(), rec.value())));

                    TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
                    long next = rec.offset() + 1;
                    OffsetAndMetadata existing = offsets.get(tp);
                    if (existing == null || next > existing.offset()) {
                        offsets.put(tp, new OffsetAndMetadata(next));
                    }

                    recordsSent++;
                    batch++;
                    if (perProducerRps > 0 && recordsSent % rateCheckpoint == 0) {
                        long now = System.nanoTime();
                        if (now < nextCheckNs) {
                            long sleepNs = nextCheckNs - now;
                            try {
                                Thread.sleep(sleepNs / 1_000_000L, (int) (sleepNs % 1_000_000L));
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                            }
                        }
                        nextCheckNs += 100_000_000L;
                    }
                }

                // Send the consumed offsets to the transaction (offset + 1 per
                // partition, with the consumer group metadata).
                producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

                if (shouldAbort(txnIndex, ABORT_RATE)) {
                    // Aborted transactions produced+consumed their records; on
                    // abort they count toward neither throughput nor latency, the
                    // consumed offsets are NOT committed, and the consumer is NOT
                    // sought back (a benchmark simplification; a real EOS app
                    // seeks to the committed position).
                    producer.abortTransaction();
                    abortedTransactions.incrementAndGet();
                    abortedRecords.addAndGet(batch);
                } else {
                    producer.commitTransaction();
                    long commitMs = System.currentTimeMillis();
                    committedTransactions.incrementAndGet();

                    long commitLatency = commitMs - beginMs;
                    metrics.transactions.addMeasurement(1);
                    metrics.commitLatency.addMeasurement(commitLatency);
                    synchronized (HIST_LOCK) {
                        int ci = (int) Math.min(Math.max(commitLatency, 0), MAX_LATENCY_MS + 1);
                        commitLatencyHist[ci]++;
                        totalCommitLatencyMs += commitLatency;
                        if (commitLatency > maxCommitLatencyMs) maxCommitLatencyMs = commitLatency;
                    }

                    for (int r = 0; r < futures.size(); r++) {
                        try {
                            verifyRecordMetadata(futures.get(r).get());
                        } catch (Exception e) {
                            System.out.println("Produce call resulted in exception: " + e.getMessage());
                        }
                        completedMessages.incrementAndGet();
                        long latency = commitMs - produceMs.get(r);
                        metrics.latency.addMeasurement(latency);
                        metrics.messages.addMeasurement(1);
                        metrics.bytes.addMeasurement(MESSAGE_SIZE);
                        synchronized (HIST_LOCK) {
                            int li = (int) Math.min(Math.max(latency, 0), MAX_LATENCY_MS + 1);
                            latencyHist[li]++;
                            totalLatencyMs += latency;
                            if (latency > maxLatencyMs) maxLatencyMs = latency;
                        }
                    }
                }

                txnIndex++;

                if (perProducerNumMessages <= 0) {
                    long durationNs = System.nanoTime() - firstMessageNs;
                    if (durationNs > (long) TEST_DURATION_SECONDS * 1_000_000_000L) {
                        break;
                    }
                }
                continueSending = !terminating;
                if (perProducerNumMessages > 0) {
                    continueSending = continueSending && recordsSent < perProducerNumMessages;
                }
            }
        } catch (Exception e) {
            System.err.println("EOS producer " + index + " failed: " + e.getMessage());
        }
    }

    /**
     * Consumer configuration for the EOS pipeline: byte-array deserializers, the
     * shared {@link #GROUP_ID}, {@code enable.auto.commit=false} (offsets flow
     * through the transaction), {@code isolation.level=read_committed} (canonical
     * EOS setting) and {@code max.poll.records} capped at
     * {@code RECORDS_PER_TRANSACTION}. SASL/security settings are carried over
     * from the producer configuration.
     */
    private static Properties consumerConfigFromEnv(Properties baseConf) {
        Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, baseConf.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        c.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Long.toString(RECORDS_PER_TRANSACTION));
        for (String k : new String[]{"security.protocol", "sasl.mechanism", "sasl.jaas.config"}) {
            if (baseConf.containsKey(k)) {
                c.put(k, baseConf.get(k));
            }
        }
        return c;
    }

    private static void runWarmup(Properties baseConf, List<KeyValue> generated) {
        System.out.println("Warming up for " + WARMUP_SECONDS + " seconds ...");
        Properties conf = new Properties();
        conf.putAll(baseConf);
        conf.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID + "-warmup");
        conf.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        conf.put(ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(conf)) {
            producer.initTransactions();
            long warmupEnd = System.nanoTime() + WARMUP_SECONDS * 1_000_000_000L;
            int i = 0;
            while (System.nanoTime() < warmupEnd && !terminating) {
                producer.beginTransaction();
                KeyValue m = generated.get(i % generated.size());
                Future<RecordMetadata> f = producer.send(new ProducerRecord<>(TOPIC_NAME, m.key, m.value));
                producer.commitTransaction();
                try {
                    verifyRecordMetadata(f.get());
                } catch (Exception e) {
                    System.out.println("Warmup failed due to message verification error: " + e.getMessage());
                    return;
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                i++;
            }
        } catch (Exception e) {
            System.out.println("Warmup failed: " + e.getMessage());
            return;
        }
        // Warmup records are excluded from the measured statistics (the measured
        // interval markers are set after this returns); reset the verified count.
        verified.set(0);
        System.out.println("Warmup complete.");
    }

    // Deterministic, evenly-spread abort selection (Bresenham). Per-producer
    // 0-based transaction index i; abort iff floor((i+1)*rate) > floor(i*rate).
    private static boolean shouldAbort(long txnIndex, double abortRate) {
        if (abortRate <= 0.0) {
            return false;
        }
        double i = (double) txnIndex;
        return Math.floor((i + 1.0) * abortRate) > Math.floor(i * abortRate);
    }

    // === Configuration =======================================================

    private static Properties configurationFromEnv() {
        Properties conf = new Properties();
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envOr("BOOTSTRAP_SERVERS", "localhost:9092"));
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        boolean useDefaults = "True".equals(System.getenv("USE_DEFAULTS"));
        if (!useDefaults) {
            conf.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");

            long batchSizeBytes;
            if (System.getenv("BATCH_SIZE") != null) {
                batchSizeBytes = envLong("BATCH_SIZE", 1024) * 1024L;
            } else {
                batchSizeBytes = 1024L * 1024L;
            }
            conf.put(ProducerConfig.BATCH_SIZE_CONFIG, Long.toString(batchSizeBytes));

            long maxRequestSizeBytes;
            if (System.getenv("MAX_REQUEST_SIZE") != null) {
                maxRequestSizeBytes = envLong("MAX_REQUEST_SIZE", 0) * 1024L;
            } else {
                maxRequestSizeBytes = batchSizeBytes * 64L;
            }
            maxRequestSizeBytes = Math.min(maxRequestSizeBytes, 8L * 1024 * 1024);
            conf.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Long.toString(maxRequestSizeBytes));

            conf.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, envOr("COMPRESSION_TYPE", "none"));

            if (System.getenv("MAX_IN_FLIGHT") != null) {
                conf.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, System.getenv("MAX_IN_FLIGHT"));
            }
            if (System.getenv("BUFFER_MEMORY") != null) {
                conf.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                    Long.toString(envLong("BUFFER_MEMORY", 32) * 1024L * 1024L));
            }
            conf.put(ProducerConfig.LINGER_MS_CONFIG, envOr("LINGER_MS", "5"));
        } else {
            System.out.println("USE_DEFAULTS: true (client defaults; tuning knobs omitted)");
        }

        // SASL
        String securityProtocol = System.getenv("SECURITY_PROTOCOL");
        String saslMechanism = System.getenv("SASL_MECHANISM");
        String username = System.getenv("SASL_USERNAME");
        String password = System.getenv("SASL_PASSWORD");
        boolean saslEnabled = securityProtocol != null
            && (securityProtocol.equals("SASL_PLAINTEXT") || securityProtocol.equals("SASL_SSL"))
            && saslMechanism != null && username != null && password != null;
        if (securityProtocol != null) {
            conf.put("security.protocol", securityProtocol);
        }
        if (saslEnabled) {
            conf.put("sasl.mechanism", saslMechanism);
            String loginModule = saslMechanism.startsWith("SCRAM")
                ? "org.apache.kafka.common.security.scram.ScramLoginModule"
                : "org.apache.kafka.common.security.plain.PlainLoginModule";
            conf.put("sasl.jaas.config",
                loginModule + " required\n\tusername=\"" + username
                + "\"\n\tpassword=\"" + password + "\";");
        }
        return conf;
    }

    private static void printConfiguration(Properties conf) {
        System.out.println("Key size: " + KEY_SIZE + " bytes");
        System.out.println("Value size: " + VALUE_SIZE + " bytes");
        System.out.println("Records per transaction: " + RECORDS_PER_TRANSACTION);
        System.out.println("Abort rate: " + ABORT_RATE);
        System.out.println("Transactional producers: " + NUM_TRANSACTIONAL_PRODUCERS);
        System.out.println("Transactional id base: " + TRANSACTIONAL_ID);
        System.out.println("TXN_MODE: " + TXN_MODE);
        if (EOS_MODE) {
            System.out.println("Source topic: " + SOURCE_TOPIC);
            System.out.println("Consumer group id: " + GROUP_ID);
        }
        System.out.println("Verify: " + DO_VERIFY);
        System.out.println("enable.idempotence: true (forced for transactions)");
        System.out.println("Producer configuration:");
        for (Map.Entry<Object, Object> e : conf.entrySet()) {
            String k = e.getKey().toString();
            if (k.equals("sasl.jaas.config") || k.equals("sasl.password")) {
                System.out.println("  " + k + ": <hidden>");
            } else {
                System.out.println("  " + k + ": " + e.getValue());
            }
        }
    }

    // === Message generation ==================================================

    static class KeyValue {
        final byte[] key;
        final byte[] value;
        KeyValue(byte[] k, byte[] v) { this.key = k; this.value = v; }
    }

    private static List<KeyValue> generateMessages(int n, int keySize, int valueSize) {
        Random rng = new Random();
        int valueRandBytes = valueSize / 2;
        byte[] valueConst = new byte[valueSize - valueRandBytes];
        rng.nextBytes(valueConst);
        int keyRandBytes = keySize > 0 ? keySize / 2 : 0;
        byte[] keyConst = keySize > 0 ? new byte[keySize - keyRandBytes] : null;
        if (keyConst != null) rng.nextBytes(keyConst);

        List<KeyValue> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            byte[] key = null;
            if (keySize > 0) {
                key = new byte[keySize];
                System.arraycopy(keyConst, 0, key, 0, keyConst.length);
                byte[] r = new byte[keyRandBytes];
                rng.nextBytes(r);
                System.arraycopy(r, 0, key, keyConst.length, keyRandBytes);
            }
            byte[] value = new byte[valueSize];
            System.arraycopy(valueConst, 0, value, 0, valueConst.length);
            byte[] r = new byte[valueRandBytes];
            rng.nextBytes(r);
            System.arraycopy(r, 0, value, valueConst.length, valueRandBytes);
            result.add(new KeyValue(key, value));
        }
        return result;
    }

    private static void verifyRecordMetadata(RecordMetadata r) {
        if (!DO_VERIFY) {
            verified.incrementAndGet();
            return;
        }
        if (r.offset() >= 0 && r.partition() >= 0
            && r.topic().equals(TOPIC_NAME) && r.timestamp() >= 0) {
            verified.incrementAndGet();
        }
    }

    /**
     * Delete the topic (ignoring "does not exist"), wait 10s, re-create it, wait
     * 10s — using the Java AdminClient. Identical to {@link
     * ProducerPerformanceTest}. Partition count and replication factor use the
     * broker default unless {@code partitions} &gt; 0.
     */
    private static void recreateTopic(Properties producerConf, String topic, int partitions)
            throws InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            producerConf.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        for (String k : new String[]{"security.protocol", "sasl.mechanism", "sasl.jaas.config"}) {
            if (producerConf.containsKey(k)) adminProps.put(k, producerConf.get(k));
        }
        try (Admin admin = Admin.create(adminProps)) {
            System.out.println(">>> CREATE_TOPIC: deleting topic '" + topic + "' (ignored if absent) ...");
            try {
                admin.deleteTopics(Collections.singletonList(topic)).all().get();
                System.out.println(">>> deleted '" + topic + "'");
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    System.out.println(">>> '" + topic + "' did not exist (ok)");
                } else {
                    throw new RuntimeException(e);
                }
            }
            System.out.println(">>> waiting 10s after delete ...");
            Thread.sleep(10_000);

            String pdesc = partitions > 0 ? String.valueOf(partitions) : "broker-default";
            System.out.println(">>> CREATE_TOPIC: creating topic '" + topic
                + "' (partitions=" + pdesc + ", rf=broker-default) ...");
            NewTopic newTopic = new NewTopic(topic,
                partitions > 0 ? Optional.of(partitions) : Optional.<Integer>empty(),
                Optional.<Short>empty());
            try {
                admin.createTopics(Collections.singletonList(newTopic)).all().get();
                System.out.println(">>> created '" + topic + "'");
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println(">>> '" + topic + "' already exists (ok)");
                } else {
                    throw new RuntimeException(e);
                }
            }
            System.out.println(">>> waiting 10s after create ...");
            Thread.sleep(10_000);
        }
    }

    // === Utility helpers =====================================================

    /** p-th percentile (0..1) latency in ms from the histogram (matches C/Rust). */
    private static long percentileFromHist(long[] hist, double p) {
        long total = 0;
        for (long c : hist) total += c;
        if (total == 0) return 0;
        long target = (long) Math.ceil(total * p);
        long cum = 0;
        for (int i = 0; i < hist.length; i++) {
            cum += hist[i];
            if (cum >= target) return i;
        }
        return hist.length - 1;
    }

    private static void installSignalHandler() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> terminating = true));
    }

    private static String envOr(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null ? defaultValue : v;
    }
    private static int envInt(String key, int defaultValue) {
        String v = System.getenv(key);
        if (v == null) return defaultValue;
        try { return Integer.parseInt(v); } catch (NumberFormatException e) { return defaultValue; }
    }
    private static long envLong(String key, long defaultValue) {
        String v = System.getenv(key);
        if (v == null) return defaultValue;
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return defaultValue; }
    }
    private static double envDouble(String key, double defaultValue) {
        String v = System.getenv(key);
        if (v == null) return defaultValue;
        try { return Double.parseDouble(v); } catch (NumberFormatException e) { return defaultValue; }
    }
    private static boolean envBool(String key, boolean defaultValue) {
        String v = System.getenv(key);
        return v == null ? defaultValue : v.equalsIgnoreCase("true") || v.equals("True");
    }
}
