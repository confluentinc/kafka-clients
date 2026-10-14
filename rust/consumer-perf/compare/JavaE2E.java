import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/* Time-based e2e latency benchmark, Java kafka-clients consumer.
 * Methodology matches the repo librdkafka_e2e.c (Run A): exact 1ms histogram,
 * per-batch clock, message-count warmup. Reports latency stddev (from the
 * histogram) and CURRENT RSS avg/min/max (/proc/self/status VmRSS) like B. */
public class JavaE2E {
    static final int MAXLAT = 600000;
    static long[] B = new long[MAXLAT + 1];
    static long cnt, sum, min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    static long[] iB = new long[MAXLAT + 1];
    static long icnt, isum, imin = Long.MAX_VALUE, imax = Long.MIN_VALUE;

    static void rec(long[] b, long lat, boolean iv) {
        int idx = (int) Math.min(Math.max(lat, 0), MAXLAT);
        b[idx]++;
        if (iv) { icnt++; isum += lat; if (lat < imin) imin = lat; if (lat > imax) imax = lat; }
        else { cnt++; sum += lat; if (lat < min) min = lat; if (lat > max) max = lat; }
    }
    static long pct(long[] b, long c, double p) {
        if (c == 0) return 0;
        long t = (long) Math.ceil(p / 100.0 * c), cum = 0;
        for (int i = 0; i <= MAXLAT; i++) { cum += b[i]; if (cum >= t) return i; }
        return MAXLAT;
    }
    /* Latency stddev from the exact histogram (1 ms quantized). */
    static double sd(long[] b, long c, long s) {
        if (c == 0) return 0.0;
        double mean = (double) s / c, var = 0.0;
        for (int i = 0; i <= MAXLAT; i++) if (b[i] != 0) { double d = i - mean; var += (double) b[i] * d * d; }
        return Math.sqrt(var / c);
    }
    static void resetIv() { iB = new long[MAXLAT + 1]; icnt = 0; isum = 0; imin = Long.MAX_VALUE; imax = Long.MIN_VALUE; }
    /* Current RSS in MB from /proc/self/status (VmRSS, kB). */
    static double readRssMb() {
        try (BufferedReader r = new BufferedReader(new FileReader("/proc/self/status"))) {
            String line;
            while ((line = r.readLine()) != null)
                if (line.startsWith("VmRSS:")) return Long.parseLong(line.trim().split("\\s+")[1]) / 1024.0;
        } catch (Exception e) { }
        return 0.0;
    }
    /* Interval process CPU%: utime+stime delta from /proc/self/stat over the
     * wall-clock delta, as a percent of one core (may exceed 100 on multi-core
     * work). Same method as the producer harness's Metrics.CPUBucket. */
    static long prevTicks = -1; static long prevNanos;
    static final double USER_HZ = 100.0; // Linux clock ticks per second
    static long readBusyTicks() {
        try (BufferedReader r = new BufferedReader(new FileReader("/proc/self/stat"))) {
            String s = r.readLine();
            // comm (field 2) is parenthesized and may contain spaces; parse
            // after the last ')'. utime/stime are fields 14/15, i.e. indices
            // 11/12 of the whitespace-split remainder.
            int rp = s.lastIndexOf(')');
            String[] f = s.substring(rp + 2).split("\\s+");
            return Long.parseLong(f[11]) + Long.parseLong(f[12]);
        } catch (Exception e) { return 0; }
    }
    static double sampleCpuPct() {
        long ticks = readBusyTicks(); long nowNs = System.nanoTime();
        double pct = 0.0;
        if (prevTicks >= 0) {
            double dt = (nowNs - prevNanos) / 1e9;
            if (dt > 0) pct = ((ticks - prevTicks) / USER_HZ) / dt * 100.0;
        }
        prevTicks = ticks; prevNanos = nowNs;
        return pct;
    }

    public static void main(String[] a) throws Exception {
        String bootstrap = "localhost:9092", topic = "bench", proto = "consumer", cfg = null, gid = null;
        int durationS = 1800, warmup = 200000, intervalS = 10, pollMs = 500, maxPoll = 2500;
        for (int i = 0; i < a.length; i++) switch (a[i]) {
            case "--bootstrap": bootstrap = a[++i]; break;
            case "--topic": topic = a[++i]; break;
            case "--group-protocol": proto = a[++i]; break;
            case "--client-config": cfg = a[++i]; break;
            case "--group-id": gid = a[++i]; break;
            case "--duration": durationS = Integer.parseInt(a[++i]); break;
            case "--warmup": warmup = Integer.parseInt(a[++i]); break;
            case "--interval": intervalS = Integer.parseInt(a[++i]); break;
            case "--poll-timeout": pollMs = Integer.parseInt(a[++i]); break;
            case "--max-poll-records": maxPoll = Integer.parseInt(a[++i]); break;
        }
        Properties p = new Properties();
        if (cfg != null) try (FileInputStream f = new FileInputStream(cfg)) { p.load(f); }
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, gid != null ? gid : "e2e-java-" + System.currentTimeMillis());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPoll));
        p.put("group.protocol", proto);

        System.out.printf("Java e2e: bootstrap=%s topic=%s proto=%s maxPoll=%d fetch.min=4MiB partFetch=4MiB crc=off dur=%ds warmup=%d%n",
                bootstrap, topic, proto, maxPoll, durationS, warmup);
        long durationMs = durationS * 1000L, intervalMs = intervalS * 1000L, totalBytes = 0;
        long seen = 0, measured = 0, measureStart = 0, deadline = 0, ivStart = 0; int ivIdx = 0;
        boolean warm = false;
        double rssSum = 0, rssMin = Double.MAX_VALUE, rssMax = 0; long rssN = 0;
        double cpuSum = 0, cpuMin = Double.MAX_VALUE, cpuMax = 0; long cpuN = 0;
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            c.subscribe(Collections.singletonList(topic));
            System.out.println("subscribed; waiting for assignment/records...");
            while (true) {
                ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(pollMs));
                long now = System.currentTimeMillis();
                if (recs.isEmpty()) { if (warm && now >= deadline) break; else continue; }
                for (ConsumerRecord<byte[], byte[]> r : recs) {
                    if (!warm) {
                        if (++seen >= warmup) { warm = true; measureStart = System.currentTimeMillis();
                            deadline = measureStart + durationMs; ivStart = measureStart;
                            sampleCpuPct(); // prime the CPU baseline at measure start
                            System.out.printf("warmup complete (%d msgs); measuring %ds now%n", warmup, durationS); }
                        continue;
                    }
                    long lat = now - r.timestamp();
                    rec(B, lat, false); rec(iB, lat, true);
                    measured++;
                    totalBytes += (r.value() != null ? r.value().length : 0) + (r.key() != null ? r.key().length : 0);
                }
                if (warm && now - ivStart >= intervalMs) {
                    double es = (now - ivStart) / 1000.0, thr = es > 0 ? icnt / es : 0;
                    double rss = readRssMb();
                    rssSum += rss; if (rss < rssMin) rssMin = rss; if (rss > rssMax) rssMax = rss; rssN++;
                    double cpu = sampleCpuPct();
                    cpuSum += cpu; if (cpu < cpuMin) cpuMin = cpu; if (cpu > cpuMax) cpuMax = cpu; cpuN++;
                    System.out.printf("[interval %d] t=%6.1fs msgs=%7d thr=%9.0f msg/s avg=%6.2f p50=%d p99=%d p999=%d max=%d ms cpu=%5.1f%% rss=%.1fMB%n",
                        ivIdx, (now - measureStart) / 1000.0, icnt, thr, icnt == 0 ? 0.0 : (double) isum / icnt,
                        pct(iB, icnt, 50), pct(iB, icnt, 99), pct(iB, icnt, 99.9), imax == Long.MIN_VALUE ? 0 : imax, cpu, rss);
                    resetIv(); ivStart = System.currentTimeMillis(); ivIdx++;
                }
                if (warm && System.currentTimeMillis() >= deadline) break;
            }
        }
        double durS = measureStart > 0 ? (System.currentTimeMillis() - measureStart) / 1000.0 : 0;
        double thrMsg = durS > 0 ? measured / durS : 0, thrMiB = durS > 0 ? (totalBytes / 1048576.0) / durS : 0;
        double sdv = sd(B, cnt, sum);
        double rssAvg = rssN > 0 ? rssSum / rssN : 0.0; if (rssN == 0) rssMin = 0.0;
        double cpuAvg = cpuN > 0 ? cpuSum / cpuN : 0.0; if (cpuN == 0) cpuMin = 0.0;
        System.out.println("======================================================================");
        System.out.println("SUMMARY - Java kafka-clients (KIP-848=" + proto + ", warmup " + warmup + " excluded)");
        System.out.printf("Measured messages: %d%n", cnt);
        System.out.printf("Duration:          %.2f s%n", durS);
        System.out.printf("Throughput:        %.0f msg/s  (%.2f MiB/s)%n", thrMsg, thrMiB);
        System.out.printf("E2E latency (ms):  min=%d avg=%.2f stddev=%.2f p50=%d p90=%d p95=%d p99=%d p99.9=%d max=%d%n",
            cnt == 0 ? 0 : min, cnt == 0 ? 0.0 : (double) sum / cnt, sdv, pct(B, cnt, 50), pct(B, cnt, 90),
            pct(B, cnt, 95), pct(B, cnt, 99), pct(B, cnt, 99.9), cnt == 0 ? 0 : max);
        System.out.printf("CPU (%% one core):  avg=%.1f min=%.1f max=%.1f (%d samples)%n", cpuAvg, cpuMin, cpuMax, cpuN);
        System.out.printf("RSS (MB, current): avg=%.1f min=%.1f max=%.1f (%d samples)%n", rssAvg, rssMin, rssMax, rssN);
        System.out.printf("{\"client\":\"java\",\"protocol\":\"%s\",\"messages\":%d,\"duration_s\":%.2f,\"throughput_msg_s\":%.2f,\"throughput_mib_s\":%.2f,\"lat_min_ms\":%d,\"lat_avg_ms\":%.2f,\"lat_stddev_ms\":%.2f,\"lat_p50_ms\":%d,\"lat_p90_ms\":%d,\"lat_p95_ms\":%d,\"lat_p99_ms\":%d,\"lat_p999_ms\":%d,\"lat_max_ms\":%d,\"cpu_avg_pct\":%.1f,\"cpu_min_pct\":%.1f,\"cpu_max_pct\":%.1f,\"rss_avg_mb\":%.1f,\"rss_min_mb\":%.1f,\"rss_max_mb\":%.1f}%n",
            proto, cnt, durS, thrMsg, thrMiB, cnt == 0 ? 0 : min, cnt == 0 ? 0.0 : (double) sum / cnt, sdv,
            pct(B, cnt, 50), pct(B, cnt, 90), pct(B, cnt, 95), pct(B, cnt, 99), pct(B, cnt, 99.9), cnt == 0 ? 0 : max,
            cpuAvg, cpuMin, cpuMax, rssAvg, rssMin, rssMax);
    }
}
