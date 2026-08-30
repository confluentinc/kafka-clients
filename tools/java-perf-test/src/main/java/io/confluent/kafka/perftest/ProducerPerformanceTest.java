// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package io.confluent.kafka.perftest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Java equivalent of {@code bindings/python/test/performance/producer_performance_test.py}.
 *
 * Configuration via environment variables (matches the Python harness):
 *
 * <pre>
 *   BOOTSTRAP_SERVERS       Kafka bootstrap servers (default localhost:9092)
 *   TOPIC_NAME              topic to produce to (default test-topic)
 *   NUM_MESSAGES            total messages to produce (0 = time-based)
 *   LIMIT_RPS               rate limit in msg/s (0 = unlimited)
 *   KEY_SIZE                key size in bytes (0 = no key, default 0)
 *   VALUE_SIZE              value size in bytes (default 2048)
 *   BATCH_SIZE              batch.size in KB (default 977 ≈ 1MB; matches Python default)
 *   MAX_REQUEST_SIZE        max.request.size in KB (default 8 * BATCH_SIZE)
 *   BUFFER_MEMORY           buffer.memory in MB (default unset)
 *   LINGER_MS               linger.ms
 *   COMPRESSION_TYPE        none / gzip / snappy / lz4 / zstd (default none)
 *   ENABLE_IDEMPOTENCE      true / false (default false)
 *   USE_DEFAULTS            True / False (default False); omit all tuning knobs, use client defaults
 *   MAX_IN_FLIGHT           max.in.flight.requests.per.connection
 *   WARMUP_SECONDS          warmup duration (default 0)
 *   TEST_DURATION_SECONDS   measured interval (default 600)
 *   DO_VERIFY               verify RecordMetadata fields (default True)
 *   VERIFY_CONSUMED         consume back and verify partition placement (default False)
 *
 *   SECURITY_PROTOCOL       SSL / SASL_SSL / SASL_PLAINTEXT (optional)
 *   SASL_MECHANISM          PLAIN / SCRAM-SHA-256 / etc.
 *   SASL_USERNAME
 *   SASL_PASSWORD
 * </pre>
 *
 * Output format matches the Python harness ({@code metrics.jsonl} in cwd) so
 * the same plotting tools work on both runs.
 */
public class ProducerPerformanceTest {

    // --- Mutable state mirroring the Python module-level globals -------------
    private static volatile boolean terminating = false;
    private static long warmupSent = 0;
    private static long measuredSent = 0;
    private static long completedMessages = 0;
    private static long verified = 0;
    private static long maxLatencyMs = 0;
    private static long totalLatencyMs = 0;

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
    private static final boolean VERIFY_CONSUMED = envBool("VERIFY_CONSUMED", false);
    // When true (default), delete + re-create the topic before the run. -1
    // partitions => broker default (RF is always broker default, so this works
    // on Confluent Cloud where RF=1 is rejected).
    private static final boolean CREATE_TOPIC = envBool("CREATE_TOPIC", true);
    private static final int PARTITIONS = envInt("PARTITIONS", -1);
    // Per-message p99 latency budget in ms (0 = off). Matches C/Rust.
    private static final long P99_LIMIT_MS = envLong("P99_LIMIT_MS", 0);
    // Machine-readable summary file, matching the other producer perf tests.
    private static final String RESULTS_FILE = envOr("RESULTS_FILE", "results.json");
    private static final int MAX_LATENCY_MS = 10_000;
    // Seconds to keep sampling after the measured interval, so the JSONL
    // captures cooldown windows (carry measurement_end_ms). Matches C/Rust.
    private static final int POST_TEST_AWAIT_SECONDS = 10;
    // Latency histogram (ms) for the p99 computation; written only by the
    // single recorder thread and read after it is joined, so no locking needed.
    private static final long[] latencyHist = new long[MAX_LATENCY_MS + 2];
    private static boolean latencyBudgetExceeded = false;

    public static void main(String[] args) throws Exception {
        installSignalHandler();

        if (LIMIT_RPS == 0) {
            if (NUM_MESSAGES > 0) {
                System.out.println("Producing " + NUM_MESSAGES + " messages at max rate");
            } else {
                System.out.println("Producing messages at max rate for " + TEST_DURATION_SECONDS + " seconds");
            }
        } else {
            System.out.println("Producing " + NUM_MESSAGES + " messages at " + LIMIT_RPS + " msg/s");
        }

        Metrics metrics = new Metrics();
        metrics.startCollecting(1);

        System.out.println("Running sync producer performance test java (kafka-clients)...");

        Properties producerConf = configurationFromEnv();
        printConfiguration(producerConf);

        if (CREATE_TOPIC) {
            recreateTopic(producerConf, TOPIC_NAME, PARTITIONS);
        }

        // Pre-generate messages, just like the Python test.
        List<KeyValue> generated = generateMessages(10_000, KEY_SIZE, VALUE_SIZE);
        long messageSize = MESSAGE_SIZE;

        // Pre-test baseline offsets for the (optional) consumer verification.
        Map<Integer, Long> baselineEndOffsets = null;
        if (VERIFY_CONSUMED) {
            try {
                baselineEndOffsets = getTopicEndOffsets(producerConf, TOPIC_NAME);
                long total = baselineEndOffsets.values().stream().mapToLong(Long::longValue).sum();
                System.out.println("Baseline: topic " + TOPIC_NAME + " has " + total
                    + " pre-existing messages across " + baselineEndOffsets.size()
                    + " partitions; verifier will start from these offsets");
            } catch (Exception e) {
                System.out.println("Baseline capture failed: " + e.getMessage()
                    + ". Verification will be skipped.");
                baselineEndOffsets = null;
            }
        }

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConf)) {
            // === WARMUP ===
            if (WARMUP_SECONDS > 0) {
                System.out.println("Warming up for " + WARMUP_SECONDS + " seconds ...");
                long warmupEnd = System.nanoTime() + WARMUP_SECONDS * 1_000_000_000L;
                int i = 0;
                while (System.nanoTime() < warmupEnd && !terminating) {
                    KeyValue m = generated.get(i % generated.size());
                    Future<RecordMetadata> f = producer.send(
                        new ProducerRecord<>(TOPIC_NAME, m.key, m.value));
                    try {
                        verifyRecordMetadata(f.get());
                    } catch (Exception e) {
                        System.out.println("Warmup failed due to message verification error: " + e.getMessage());
                        return;
                    }
                    warmupSent++;
                    Thread.sleep(100);
                    i++;
                }
            }
            verified = 0;

            // === MEASURED INTERVAL ===
            // Produced messages whose ack we haven't yet processed go into a
            // bounded queue; a worker thread drains it and records latency.
            BlockingQueue<PendingProduce> pending = new LinkedBlockingQueue<>(
                Math.max(1, (int) ((1L << 31) - 1) / Math.max(1, MESSAGE_SIZE / 4)));

            AtomicBoolean recording = new AtomicBoolean(true);
            // Sample the handoff-queue depth each measured window (Little's Law check).
            metrics.setQueueSizeSupplier(pending::size);
            Thread recorder = startRecorder(pending, recording, metrics, messageSize);

            long beforeMs = System.currentTimeMillis();
            long firstMessageNs = System.nanoTime();
            long nextCheckNs = firstMessageNs + 1_000_000_000L;
            metrics.measurementStartMs = beforeMs;

            System.out.println("Starting measured interval at " + beforeMs + " ms: " + Instant.now());

            long messagesSent = 0;
            boolean continueSending = NUM_MESSAGES > 0 ? messagesSent < NUM_MESSAGES : !terminating;

            while (continueSending) {
                KeyValue m = generated.get((int) (messagesSent % generated.size()));
                long startTime = System.currentTimeMillis();
                PendingProduce pp = new PendingProduce(startTime);
                // Stamp per-record latency at ack time in the delivery callback —
                // the Java analog of the C harness's `test_v2_dr` done_ns. Runs on
                // the producer's I/O thread when the record is acknowledged.
                Callback cb = (metadata, exception) -> pp.completionMs = System.currentTimeMillis();
                Future<RecordMetadata> f = producer.send(
                    new ProducerRecord<>(TOPIC_NAME, m.key, m.value), cb);
                pp.future = f;
                pending.put(pp);
                messagesSent++;

                if (LIMIT_RPS > 0 && messagesSent % LIMIT_RPS == 0) {
                    long now = System.nanoTime();
                    if (now < nextCheckNs) {
                        long sleepNs = nextCheckNs - now;
                        Thread.sleep(sleepNs / 1_000_000L, (int) (sleepNs % 1_000_000L));
                    }
                    nextCheckNs += 1_000_000_000L;
                }

                if (messagesSent % 10_000 == 0) {
                    long durationNs = System.nanoTime() - firstMessageNs;
                    long extraSeconds = NUM_MESSAGES > 0 ? 10 : 1;
                    if (durationNs > (TEST_DURATION_SECONDS + extraSeconds) * 1_000_000_000L) {
                        System.out.println("Test duration reached, "
                            + (durationNs / 1e9) + " seconds. Interrupting...");
                        break;
                    }
                }

                continueSending = !terminating;
                if (NUM_MESSAGES > 0) {
                    continueSending = continueSending && messagesSent < NUM_MESSAGES;
                }
            }

            // Drain any in-flight before tearing down.
            recording.set(false);
            recorder.join();
            measuredSent = messagesSent;

            long afterMs = System.currentTimeMillis();
            metrics.measurementEndMs = afterMs;

            if (verified != completedMessages && !terminating) {
                System.out.println("Verified messages " + verified
                    + " does not match completed messages " + completedMessages);
            }

            double durationS = (afterMs - beforeMs) / 1000.0;
            double rate = completedMessages / durationS;
            double mibRate = (completedMessages * (double) MESSAGE_SIZE) / (1024.0 * 1024.0) / durationS;
            double avgLatencyMs = completedMessages == 0 ? 0 : (double) totalLatencyMs / completedMessages;
            long p99Ms = percentileFromHist(latencyHist, 0.99);

            // CPU/RSS averaged over the measured interval only (collector-side).
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
            System.out.println("Average future queue size: " + String.format("%.2f", metrics.getAverageQueueSize()));
            System.out.println("Max future queue size: " + metrics.getMaxQueueSize());
            System.out.println("Average rate msg/s: " + String.format("%.2f msg/s", rate));
            System.out.println("Average rate MiB/s: " + String.format("%.2f MiB/s", mibRate));
            System.out.println("Average latency: " + String.format("%.2f ms", avgLatencyMs));
            System.out.println("Max latency: " + maxLatencyMs + " ms");
            System.out.println("p99 latency: " + p99Ms + " ms");
            // Latency budget — only asserted when set (matches C/Rust).
            if (P99_LIMIT_MS > 0 && p99Ms > P99_LIMIT_MS && !terminating) {
                System.err.println("p99 latency " + p99Ms + " ms exceeds " + P99_LIMIT_MS + " ms budget");
                latencyBudgetExceeded = true;
            }

            // Machine-readable summary, kept in sync with the other producer
            // performance tests (same file name, keys and latency_ms shape as
            // the consumer performance test's results.json; `client`
            // identifies which implementation wrote it).
            long minLatencyMs = 0;
            for (int i = 0; i < latencyHist.length; i++) {
                if (latencyHist[i] > 0) { minLatencyMs = i; break; }
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
            Map<String, Object> results = new LinkedHashMap<>();
            results.put("test", "producer");
            results.put("client", "java");
            results.put("topic", TOPIC_NAME);
            results.put("messages_measured", completedMessages);
            results.put("duration_s", Math.round(durationS * 100.0) / 100.0);
            results.put("throughput_msg_s", Math.round(rate * 100.0) / 100.0);
            results.put("throughput_mib_s", Math.round(mibRate * 100.0) / 100.0);
            results.put("latency_ms", latency);
            results.put("cpu_avg_pct", Math.round(avgCpu * 100.0) / 100.0);
            results.put("rss_avg_kib", Math.round(avgRssKib * 100.0) / 100.0);
            try {
                new ObjectMapper().writerWithDefaultPrettyPrinter()
                    .writeValue(new File(RESULTS_FILE), results);
                System.out.println("Results summary written to: " + RESULTS_FILE);
            } catch (IOException e) {
                System.err.println("Failed to write " + RESULTS_FILE + ": " + e);
            }

            // Compression cross-check: the client's own compression-rate-avg
            // metric (org.apache.kafka.clients.producer.internals.
            // SenderMetricsRegistry), printed next to the harness's
            // independently-measured logical throughput so it can be
            // cross-checked against an out-of-band network-counter wire-bytes
            // measurement. Must run here, still inside the try-with-resources
            // — producer goes out of scope once this block closes.
            producer.metrics().forEach((name, metric) -> {
                if ("compression-rate-avg".equals(name.name())) {
                    System.out.println("[METRIC] compression-rate-avg = " + metric.metricValue());
                }
            });
        }

        if (!terminating) {
            System.out.println("Performing garbage collection...");
            System.gc();
        }
        // Post-test await: keep the collector sampling so the JSONL captures
        // cooldown windows (they carry measurement_end_ms and are excluded from
        // the measured statistics). Mirrors the C and Rust tests.
        System.out.println("Waiting for final metrics collection...");
        if (!terminating) {
            try {
                Thread.sleep(POST_TEST_AWAIT_SECONDS * 1000L);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        Map<String, Double> last = externalMetricsLastValues(metrics);
        System.out.printf("Final CPU: %.2f %%%n", last.get("last_cpu"));
        System.out.printf("Final RSS: %.2f KiB%n", last.get("last_rss") / 1024.0);
        metrics.stopCollecting();
        System.out.println("Done");

        int exitCode = latencyBudgetExceeded ? 1 : 0;
        if (VERIFY_CONSUMED && !terminating && baselineEndOffsets != null) {
            long expected = warmupSent + measuredSent;
            System.out.println("Verifying consumed messages from topic '" + TOPIC_NAME
                + "' starting at baseline offsets (expected = " + warmupSent + " warmup + "
                + measuredSent + " measured = " + expected + ")");
            try {
                exitCode = Math.max(exitCode, verifyConsumedMessages(producerConf, TOPIC_NAME,
                    baselineEndOffsets, expected, KEY_SIZE > 0));
            } catch (Exception e) {
                System.out.println("Verification failed with exception: " + e.getMessage());
                exitCode = 1;
            }
        } else if (!VERIFY_CONSUMED) {
            System.out.println("Consumer verification skipped (VERIFY_CONSUMED=False)");
        } else if (terminating) {
            System.out.println("Consumer verification skipped (terminated)");
        } else {
            System.out.println("Consumer verification skipped (baseline capture failed)");
        }

        System.exit(exitCode);
    }

    // === Configuration =======================================================

    private static Properties configurationFromEnv() {
        Properties conf = new Properties();
        conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envOr("BOOTSTRAP_SERVERS", "localhost:9092"));
        conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // With USE_DEFAULTS the client runs at its own defaults: only the
        // bootstrap servers, the (mandatory) serializers and SASL credentials
        // are set, and every performance-tuning knob is omitted. Mirrors the
        // Python, C and Rust producer performance tests' USE_DEFAULTS behavior.
        boolean useDefaults = "True".equals(System.getenv("USE_DEFAULTS"));
        if (!useDefaults) {
            conf.put(ProducerConfig.ACKS_CONFIG, "all");
            conf.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000");

            // batch.size: default 1 MiB (1024 KiB); env value is in KiB. Matches
            // the C and Rust perf tests.
            long batchSizeBytes;
            if (System.getenv("BATCH_SIZE") != null) {
                batchSizeBytes = envLong("BATCH_SIZE", 1024) * 1024L;
            } else {
                batchSizeBytes = 1024L * 1024L;
            }
            conf.put(ProducerConfig.BATCH_SIZE_CONFIG, Long.toString(batchSizeBytes));

            // max.request.size: default batch*64, capped at 8 MiB; env is in KiB.
            // Matches the C and Rust perf tests.
            long maxRequestSizeBytes;
            if (System.getenv("MAX_REQUEST_SIZE") != null) {
                maxRequestSizeBytes = envLong("MAX_REQUEST_SIZE", 0) * 1024L;
            } else {
                maxRequestSizeBytes = batchSizeBytes * 64L;
            }
            maxRequestSizeBytes = Math.min(maxRequestSizeBytes, 8L * 1024 * 1024);
            conf.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, Long.toString(maxRequestSizeBytes));

            conf.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, envOr("COMPRESSION_TYPE", "none"));
            conf.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, envOr("ENABLE_IDEMPOTENCE", "false"));

            if (System.getenv("MAX_IN_FLIGHT") != null) {
                conf.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                    System.getenv("MAX_IN_FLIGHT"));
            }
            if (System.getenv("BUFFER_MEMORY") != null) {
                conf.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                    Long.toString(envLong("BUFFER_MEMORY", 32) * 1024L * 1024L));
            }
            // linger.ms: default 5 (matches the C and Rust perf tests).
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
        System.out.println("Verify: " + DO_VERIFY);
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
        // Match Python: 50% randomness — half random, half constant per message.
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

    // === Completion recorder =================================================

    static class PendingProduce {
        // Assigned right after producer.send() returns; the delivery callback
        // that stamps completionMs only fires at ack (strictly after send
        // returns), so the field is set before it can be read via future.get().
        Future<RecordMetadata> future;
        final long startTimeMs;
        // Stamped in the delivery callback at ack time (onCompletion), the Java
        // analog of the C harness's `test_v2_dr` done_ns. Kafka fires callbacks
        // before completing the future, so a returned future.get() guarantees
        // this is set and visible (volatile) to the recorder thread. Measuring
        // latency here — rather than when the recorder reaches the future —
        // keeps the recorder's own drain backlog out of the reported latency.
        volatile long completionMs;
        PendingProduce(long s) { this.startTimeMs = s; }
    }

    private static Thread startRecorder(BlockingQueue<PendingProduce> q, AtomicBoolean recording,
                                        Metrics metrics, long messageSize) {
        Thread t = new Thread(() -> {
            while (recording.get() || !q.isEmpty()) {
                try {
                    PendingProduce p = q.poll(1, java.util.concurrent.TimeUnit.SECONDS);
                    if (p == null) continue;
                    try {
                        RecordMetadata r = p.future.get();
                        verifyRecordMetadata(r);
                    } catch (Exception e) {
                        System.out.println("Produce call resulted in exception: " + e.getMessage());
                    }
                    completedMessages++;
                    // Latency is stamped at ack in the delivery callback
                    // (p.completionMs), NOT read from this thread's clock, so the
                    // recorder's own drain backlog cannot inflate it. Mirrors the
                    // C harness computing done_ns - start_time.
                    long latency = p.completionMs - p.startTimeMs;
                    metrics.latency.addMeasurement(latency);
                    metrics.messages.addMeasurement(1);
                    metrics.bytes.addMeasurement(messageSize);
                    if (latency > maxLatencyMs) maxLatencyMs = latency;
                    totalLatencyMs += latency;
                    int ms = (int) Math.min(Math.max(latency, 0), MAX_LATENCY_MS + 1);
                    latencyHist[ms]++;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "completion-recorder");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void verifyRecordMetadata(RecordMetadata r) {
        if (!DO_VERIFY) {
            verified++;
            return;
        }
        if (r.offset() >= 0 && r.partition() >= 0
            && r.topic().equals(TOPIC_NAME) && r.timestamp() >= 0) {
            verified++;
        }
    }

    // === Consumer-side verification =========================================

    private static Map<Integer, Long> getTopicEndOffsets(Properties producerConf, String topic) {
        Properties conf = consumerConfig(producerConf, "perf-baseline-" + UUID.randomUUID());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(conf)) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic, Duration.ofSeconds(10));
            if (infos == null) return new HashMap<>();
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo p : infos) {
                tps.add(new TopicPartition(topic, p.partition()));
            }
            Map<TopicPartition, Long> ends = consumer.endOffsets(tps, Duration.ofSeconds(10));
            Map<Integer, Long> result = new HashMap<>();
            for (Map.Entry<TopicPartition, Long> e : ends.entrySet()) {
                result.put(e.getKey().partition(), e.getValue());
            }
            return result;
        }
    }

    private static int verifyConsumedMessages(Properties producerConf, String topic,
                                               Map<Integer, Long> baseline, long expectedCount,
                                               boolean hasKeys) {
        Properties conf = consumerConfig(producerConf, "perf-verify-" + UUID.randomUUID());
        long consumedCount = 0;
        long mismatchCount = 0;
        List<String> sampleMismatches = new ArrayList<>();

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(conf)) {
            List<PartitionInfo> infos = consumer.partitionsFor(topic, Duration.ofSeconds(10));
            if (infos == null || infos.isEmpty()) {
                System.out.println("Verification: cannot read metadata for " + topic);
                return 1;
            }
            int numPartitions = infos.size();
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo p : infos) tps.add(new TopicPartition(topic, p.partition()));
            consumer.assign(tps);

            Map<TopicPartition, Long> targets = consumer.endOffsets(tps, Duration.ofSeconds(10));
            Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(tps, Duration.ofSeconds(10));

            Map<Integer, Long> current = new HashMap<>();
            for (TopicPartition tp : tps) {
                long start = Math.max(beginnings.getOrDefault(tp, 0L),
                    baseline.getOrDefault(tp.partition(), 0L));
                consumer.seek(tp, start);
                current.put(tp.partition(), start);
            }

            long emptyBudgetNs = 60_000_000_000L; // 60 s
            long lastProgressNs = System.nanoTime();

            while (true) {
                boolean caughtUp = true;
                for (TopicPartition tp : tps) {
                    if (current.get(tp.partition()) < targets.get(tp)) {
                        caughtUp = false;
                        break;
                    }
                }
                if (caughtUp) break;

                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    if (System.nanoTime() - lastProgressNs > emptyBudgetNs) break;
                    continue;
                }
                boolean sawData = false;
                for (ConsumerRecord<byte[], byte[]> rec : records) {
                    sawData = true;
                    consumedCount++;
                    int p = rec.partition();
                    current.put(p, Math.max(current.get(p), rec.offset() + 1));
                    if (hasKeys) {
                        byte[] key = rec.key();
                        if (key == null) {
                            mismatchCount++;
                            if (sampleMismatches.size() < 5) {
                                sampleMismatches.add("partition=" + p + " offset=" + rec.offset() + " key=null (expected non-null)");
                            }
                            continue;
                        }
                        int expectedP = Utils.toPositive(Utils.murmur2(key)) % numPartitions;
                        if (expectedP != p) {
                            mismatchCount++;
                            if (sampleMismatches.size() < 5) {
                                sampleMismatches.add("partition=" + p + " expected=" + expectedP + " offset=" + rec.offset());
                            }
                        }
                    }
                }
                if (sawData) lastProgressNs = System.nanoTime();
            }
        }

        boolean countOk = consumedCount == expectedCount;
        boolean partitionsOk = !hasKeys || mismatchCount == 0;
        System.out.println("Consumer verification: consumed=" + consumedCount
            + " expected=" + expectedCount + " (count_ok=" + countOk + ")");
        if (hasKeys) {
            System.out.println("Partition verification: mismatches=" + mismatchCount + "/" + consumedCount
                + " (partitions_ok=" + partitionsOk + ")");
            if (!sampleMismatches.isEmpty()) {
                System.out.println("First mismatches:");
                for (String s : sampleMismatches) System.out.println("  " + s);
            }
        } else {
            System.out.println("Partition verification: skipped (no keys)");
        }
        return (countOk && partitionsOk) ? 0 : 1;
    }

    private static Properties consumerConfig(Properties producerConf, String groupId) {
        Properties conf = new Properties();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            producerConf.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        conf.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        conf.put("check.crcs", "true");
        // Forward security/SASL config from the producer side.
        for (String k : new String[]{"security.protocol", "sasl.mechanism", "sasl.jaas.config"}) {
            if (producerConf.containsKey(k)) conf.put(k, producerConf.get(k));
        }
        return conf;
    }

    /**
     * Delete the topic (ignoring "does not exist"), wait 10s, re-create it, wait
     * 10s — using the Java AdminClient. Partition count and replication factor
     * use the broker default (Optional.empty) unless {@code partitions} &gt; 0,
     * so this works on Confluent Cloud where RF=1 is rejected. The sleeps let the
     * delete/create metadata propagate across the cluster.
     */
    private static void recreateTopic(Properties producerConf, String topic, int partitions)
            throws InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            producerConf.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        // Reuse the producer's security/SASL config for the admin client.
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

    private static Map<String, Double> externalMetricsLastValues(Metrics m) {
        Map<String, Double> r = new HashMap<>();
        if (m.lastMetrics == null) {
            r.put("last_cpu", 0.0);
            r.put("last_rss", 0.0);
            return r;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> cpu = (Map<String, String>) m.lastMetrics.get("cpu");
        @SuppressWarnings("unchecked")
        Map<String, String> rss = (Map<String, String>) m.lastMetrics.get("rss");
        r.put("last_cpu", Double.parseDouble(cpu.get("average")));
        r.put("last_rss", Double.parseDouble(rss.get("average")));
        return r;
    }

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
    private static boolean envBool(String key, boolean defaultValue) {
        String v = System.getenv(key);
        return v == null ? defaultValue : v.equalsIgnoreCase("true") || v.equals("True");
    }
}
