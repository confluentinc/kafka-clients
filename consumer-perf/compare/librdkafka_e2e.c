/* Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * End-to-end latency / CPU / memory benchmark for the *native* librdkafka C
 * consumer API (no Python/GIL in the loop).
 *
 * Mirrors the Python harness (compare/librdkafka_e2e.py) and the Rust
 * consumer-perf harness 1:1:
 *
 *   1. Subscribe with auto.offset.reset=latest, group.protocol=consumer
 *      (KIP-848; fall back to classic via --protocol classic) so only new
 *      records are seen.
 *   2. Wait until partitions are assigned (rebalance_cb assign), settle to the
 *      live edge (poll until empty), THEN fork+exec kafka-producer-perf-test.sh
 *      at a fixed throughput (guarded so it spawns once).
 *   3. Poll loop; e2e latency per record = now_wallclock_ms -
 *      rd_kafka_message_timestamp(rkmessage, &tstype) (ms). Skip `warmup`
 *      records, then measure `duration` seconds.
 *   4. Fixed-bucket 1ms histogram (0..MAX_LATENCY_MS) for O(1) percentiles, no
 *      per-record allocation in the hot loop.
 *   5. Every `interval` s: sample CPU% (percent of one core:
 *      (ru_utime+ru_stime) delta / wall delta * 100 via getrusage(RUSAGE_SELF))
 *      and RSS (ru_maxrss; on macOS this is BYTES, on Linux KILOBYTES). Print
 *      interval lines, reset interval histogram, append metrics.jsonl.
 *   6. After `duration` s of measurement: stop the producer, print + persist a
 *      summary with the same JSONL schema (client:"librdkafka-c").
 */

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <librdkafka/rdkafka.h>

#define MAX_LATENCY_MS 600000 /* match Rust/python harness ceiling */

/* ----------------------------- histogram ------------------------------ */

typedef struct {
    int64_t *buckets; /* MAX_LATENCY_MS + 1 entries */
    int64_t count;
    int64_t sum;
    int64_t min;
    int64_t max;
} hist_t;

static void hist_init(hist_t *h) {
    h->buckets = calloc(MAX_LATENCY_MS + 1, sizeof(int64_t));
    if (!h->buckets) {
        fprintf(stderr, "out of memory allocating histogram\n");
        exit(1);
    }
    h->count = 0;
    h->sum = 0;
    h->min = (int64_t)1 << 62;
    h->max = -((int64_t)1 << 62);
}

static void hist_reset(hist_t *h) {
    memset(h->buckets, 0, (size_t)(MAX_LATENCY_MS + 1) * sizeof(int64_t));
    h->count = 0;
    h->sum = 0;
    h->min = (int64_t)1 << 62;
    h->max = -((int64_t)1 << 62);
}

static void hist_free(hist_t *h) {
    free(h->buckets);
    h->buckets = NULL;
}

static void hist_record(hist_t *h, int64_t latency_ms) {
    int64_t idx = latency_ms;
    if (idx < 0)
        idx = 0;
    else if (idx > MAX_LATENCY_MS)
        idx = MAX_LATENCY_MS;
    h->buckets[idx]++;
    h->count++;
    h->sum += latency_ms > 0 ? latency_ms : 0;
    if (latency_ms < h->min)
        h->min = latency_ms;
    if (latency_ms > h->max)
        h->max = latency_ms;
}

static int64_t hist_min(const hist_t *h) { return h->count == 0 ? 0 : h->min; }
static int64_t hist_max(const hist_t *h) { return h->count == 0 ? 0 : h->max; }
static double hist_avg(const hist_t *h) {
    return h->count == 0 ? 0.0 : (double)h->sum / (double)h->count;
}
/* Latency stddev computed from the exact histogram (1 ms quantized). */
static double hist_stddev(const hist_t *h) {
    if (h->count == 0)
        return 0.0;
    double mean = (double)h->sum / (double)h->count;
    double var = 0.0;
    for (int64_t i = 0; i <= MAX_LATENCY_MS; i++) {
        if (h->buckets[i]) {
            double d = (double)i - mean;
            var += (double)h->buckets[i] * d * d;
        }
    }
    return sqrt(var / (double)h->count);
}

/* Percentile, mirrors python: target = ceil(pct/100 * count). */
static int64_t hist_percentile(const hist_t *h, double pct) {
    if (h->count == 0)
        return 0;
    int64_t target = (int64_t)ceil(pct / 100.0 * (double)h->count);
    int64_t cumulative = 0;
    for (int64_t i = 0; i <= MAX_LATENCY_MS; i++) {
        cumulative += h->buckets[i];
        if (cumulative >= target)
            return i;
    }
    return MAX_LATENCY_MS;
}

/* ------------------------------ clocks -------------------------------- */

static int64_t now_millis(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static double monotonic_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

/* ---------------------------- cpu sampler ----------------------------- */

typedef struct {
    double last_cpu_s; /* user + sys cpu seconds */
    double last_wall_s;
} cpu_sampler_t;

static double rusage_cpu_seconds(void) {
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    double u = (double)ru.ru_utime.tv_sec + (double)ru.ru_utime.tv_usec / 1e6;
    double s = (double)ru.ru_stime.tv_sec + (double)ru.ru_stime.tv_usec / 1e6;
    return u + s;
}

static void cpu_sampler_init(cpu_sampler_t *c) {
    c->last_cpu_s = rusage_cpu_seconds();
    c->last_wall_s = monotonic_s();
}

/* Returns CPU% of one core and writes RSS in MB to *rss_mb_out. */
static double cpu_sampler_sample(cpu_sampler_t *c, double *rss_mb_out) {
    double cpu = rusage_cpu_seconds();
    double wall = monotonic_s();
    double dcpu = cpu - c->last_cpu_s;
    double dwall = wall - c->last_wall_s;
    c->last_cpu_s = cpu;
    c->last_wall_s = wall;
    double cpu_pct = dwall > 0 ? (100.0 * dcpu / dwall) : 0.0;

    double rss_mb = 0.0;
#if defined(__APPLE__)
    /* macOS: ru_maxrss is bytes (peak — no cheap current-RSS syscall here). */
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    rss_mb = (double)ru.ru_maxrss / (1024.0 * 1024.0);
#else
    /* Linux: CURRENT RSS from /proc/self/statm (resident pages), matching the
     * B harness's current-RSS semantics rather than peak ru_maxrss. */
    FILE *sm = fopen("/proc/self/statm", "r");
    if (sm) {
        long total_pages = 0, res_pages = 0;
        if (fscanf(sm, "%ld %ld", &total_pages, &res_pages) == 2)
            rss_mb = (double)res_pages * (double)sysconf(_SC_PAGESIZE) /
                     (1024.0 * 1024.0);
        fclose(sm);
    }
#endif
    *rss_mb_out = rss_mb;
    return cpu_pct;
}

/* ------------------------------- args --------------------------------- */

typedef struct {
    const char *bootstrap;
    const char *topic;
    char group_id[256];
    int throughput;
    int duration;
    int message_size;
    int partitions;
    int warmup;
    int interval;
    int poll_timeout_ms;
    int join_timeout;
    int max_poll_records; /* batch size for rd_kafka_consume_batch_queue */
    int single_poll;      /* use rd_kafka_consumer_poll() one message at a time */
    const char *protocol; /* "consumer" or "classic" */
    const char *kafka_bin;
    const char *results_dir;
    /* Fetch-config overrides (NULL => use librdkafka default for that key). */
    const char *fetch_min_bytes;
    const char *fetch_wait_max_ms;
    const char *fetch_message_max_bytes;
    const char *fetch_max_bytes;
    const char *queued_min_messages;
    const char *queued_max_messages_kbytes;
    const char *extra_conf[32]; /* repeatable --conf k=v via rd_kafka_conf_set */
    int extra_conf_n;
    int no_produce; /* --no-produce: an external producer feeds the topic */
} args_t;

/* ----------------------- producer spawn (fork+exec) -------------------- */

static pid_t g_producer_pid = 0;

static pid_t spawn_producer(const args_t *a, int64_t total_records) {
    char bin_path[1024];
    snprintf(bin_path, sizeof(bin_path), "%s/kafka-producer-perf-test.sh",
             a->kafka_bin);

    char num_records[64], record_size[64], throughput[64], bootstrap_prop[512];
    snprintf(num_records, sizeof(num_records), "%lld", (long long)total_records);
    snprintf(record_size, sizeof(record_size), "%d", a->message_size);
    snprintf(throughput, sizeof(throughput), "%d", a->throughput);
    snprintf(bootstrap_prop, sizeof(bootstrap_prop), "bootstrap.servers=%s",
             a->bootstrap);

    printf(">>> Launching producer: throughput=%d fixed msg/s, %d bytes, "
           "~%lld records\n",
           a->throughput, a->message_size, (long long)total_records);
    fflush(stdout);

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return -1;
    }
    if (pid == 0) {
        /* Child: silence producer output, then exec the perf script. */
        int devnull = open("/dev/null", 1 /* O_WRONLY */);
        if (devnull >= 0) {
            dup2(devnull, 1);
            dup2(devnull, 2);
            if (devnull > 2)
                close(devnull);
        }
        char *const argv[] = {
            bin_path,
            (char *)"--topic",
            (char *)a->topic,
            (char *)"--num-records",
            num_records,
            (char *)"--record-size",
            record_size,
            (char *)"--throughput",
            throughput,
            (char *)"--producer-props",
            bootstrap_prop,
            (char *)"acks=1",
            NULL,
        };
        execv(bin_path, argv);
        perror("execv kafka-producer-perf-test.sh");
        _exit(127);
    }
    return pid;
}

static void kill_producer(void) {
    if (g_producer_pid > 0) {
        kill(g_producer_pid, SIGKILL);
        int status;
        waitpid(g_producer_pid, &status, 0);
        g_producer_pid = 0;
    }
}

/* --------------------------- rebalance cb ----------------------------- */

static volatile int g_assigned = 0;
static int64_t g_assigned_count = 0;

static void rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err,
                         rd_kafka_topic_partition_list_t *partitions,
                         void *opaque) {
    (void)opaque;
    /* KIP-848 (group.protocol=consumer) uses the COOPERATIVE rebalance
     * protocol, which requires the incremental_assign / incremental_unassign
     * APIs; rd_kafka_assign() is rejected ("must be made using
     * incremental_assign()"). Detect the protocol and call the right API. The
     * incremental APIs are also valid for the classic cooperative-sticky case.
     */
    const char *proto = rd_kafka_rebalance_protocol(rk);
    int cooperative = proto && !strcmp(proto, "COOPERATIVE");

    switch (err) {
    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        if (cooperative)
            rd_kafka_incremental_assign(rk, partitions);
        else
            rd_kafka_assign(rk, partitions);
        g_assigned = 1;
        g_assigned_count += partitions ? partitions->cnt : 0;
        printf("    assigned %d partition(s)\n",
               partitions ? partitions->cnt : 0);
        fflush(stdout);
        break;
    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        if (cooperative)
            rd_kafka_incremental_unassign(rk, partitions);
        else
            rd_kafka_assign(rk, NULL);
        g_assigned_count -= partitions ? partitions->cnt : 0;
        break;
    default:
        if (cooperative)
            rd_kafka_incremental_unassign(rk, partitions);
        else
            rd_kafka_assign(rk, NULL);
        break;
    }
}

/* --------------------------- stats cb --------------------------------- */
/* librdkafka only computes/serializes statistics when BOTH statistics.interval.ms>0
 * AND a stats callback is registered. This minimal cb just counts invocations and
 * returns 0 (librdkafka frees the JSON); the measured cost is librdkafka generating
 * + serializing the stats JSON every interval — the analog of Rust/Java metrics. */
static volatile long g_stats_cb_count = 0;
static int stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
    (void)rk; (void)json; (void)json_len; (void)opaque;
    g_stats_cb_count++;
    return 0; /* 0 => librdkafka frees json */
}

static int current_assignment_count(rd_kafka_t *rk) {
    rd_kafka_topic_partition_list_t *parts = NULL;
    if (rd_kafka_assignment(rk, &parts) != RD_KAFKA_RESP_ERR_NO_ERROR)
        return 0;
    int cnt = parts ? parts->cnt : 0;
    if (parts)
        rd_kafka_topic_partition_list_destroy(parts);
    return cnt;
}

/* ------------------------------- main --------------------------------- */

static rd_kafka_t *build_consumer(const args_t *a, char *errstr,
                                  size_t errstr_size) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

#define SET(k, v)                                                              \
    do {                                                                       \
        if (rd_kafka_conf_set(conf, (k), (v), errstr, errstr_size) !=          \
            RD_KAFKA_CONF_OK) {                                                \
            rd_kafka_conf_destroy(conf);                                       \
            return NULL;                                                       \
        }                                                                      \
    } while (0)

    SET("bootstrap.servers", a->bootstrap);
    SET("group.id", a->group_id);
    SET("client.id", "librdkafka-perf-c");
    SET("auto.offset.reset", "latest");
    SET("enable.auto.commit", "true");
    SET("group.protocol", a->protocol);

    /* Fetch knobs: apply override if provided, else fall back to the prior
     * default for fetch.min.bytes (1) and librdkafka's native defaults for the
     * rest. */
    SET("fetch.min.bytes", a->fetch_min_bytes ? a->fetch_min_bytes : "1");
    if (a->fetch_wait_max_ms)
        SET("fetch.wait.max.ms", a->fetch_wait_max_ms);
    if (a->fetch_message_max_bytes)
        SET("fetch.message.max.bytes", a->fetch_message_max_bytes);
    if (a->fetch_max_bytes)
        SET("fetch.max.bytes", a->fetch_max_bytes);
    if (a->queued_min_messages)
        SET("queued.min.messages", a->queued_min_messages);
    if (a->queued_max_messages_kbytes)
        SET("queued.max.messages.kbytes", a->queued_max_messages_kbytes);
    for (int ci = 0; ci < a->extra_conf_n; ci++) {
        char cbuf[1024];
        snprintf(cbuf, sizeof(cbuf), "%s", a->extra_conf[ci]);
        char *eq = strchr(cbuf, '=');
        if (eq) {
            *eq = '\0';
            SET(cbuf, eq + 1);
        }
    }
#undef SET

    rd_kafka_conf_set_rebalance_cb(conf, rebalance_cb);
    rd_kafka_conf_set_stats_cb(conf, stats_cb);

    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, errstr_size);
    /* On success rd_kafka_new takes ownership of conf. */
    return rk;
}

int main(int argc, char **argv) {
    args_t a;
    a.bootstrap = "localhost:9092";
    a.topic = "consumer-perf-bench";
    a.throughput = 300000;
    a.duration = 60;
    a.message_size = 1024;
    a.partitions = 12;
    a.warmup = 5000;
    a.interval = 5;
    a.poll_timeout_ms = 500;
    a.join_timeout = 120;
    a.max_poll_records = 500; /* match Rust max.poll.records=500 */
    a.single_poll = 0;
    a.protocol = "consumer";
    const char *home = getenv("HOME");
    static char kafka_bin_buf[1024];
    snprintf(kafka_bin_buf, sizeof(kafka_bin_buf), "%s/dev/opensource/kafka/bin",
             home ? home : ".");
    a.kafka_bin = kafka_bin_buf;
    a.results_dir = "consumer-perf/results";
    a.group_id[0] = '\0';
    a.fetch_min_bytes = NULL;
    a.fetch_wait_max_ms = NULL;
    a.fetch_message_max_bytes = NULL;
    a.fetch_max_bytes = NULL;
    a.queued_min_messages = NULL;
    a.queued_max_messages_kbytes = NULL;
    a.extra_conf_n = 0;
    a.no_produce = 0;

    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];
        const char *val = (i + 1 < argc) ? argv[i + 1] : NULL;
#define NEXT()                                                                 \
    (val ? (i++, val) : (fprintf(stderr, "missing value for %s\n", arg),       \
                         exit(2), (const char *)NULL))
        if (!strcmp(arg, "--bootstrap") || !strcmp(arg, "-b"))
            a.bootstrap = NEXT();
        else if (!strcmp(arg, "--topic") || !strcmp(arg, "-t"))
            a.topic = NEXT();
        else if (!strcmp(arg, "--group-id") || !strcmp(arg, "-g"))
            snprintf(a.group_id, sizeof(a.group_id), "%s", NEXT());
        else if (!strcmp(arg, "--throughput") || !strcmp(arg, "-r"))
            a.throughput = atoi(NEXT());
        else if (!strcmp(arg, "--duration") || !strcmp(arg, "-d"))
            a.duration = atoi(NEXT());
        else if (!strcmp(arg, "--message-size"))
            a.message_size = atoi(NEXT());
        else if (!strcmp(arg, "--partitions"))
            a.partitions = atoi(NEXT());
        else if (!strcmp(arg, "--warmup") || !strcmp(arg, "-w"))
            a.warmup = atoi(NEXT());
        else if (!strcmp(arg, "--interval"))
            a.interval = atoi(NEXT());
        else if (!strcmp(arg, "--poll-timeout-ms"))
            a.poll_timeout_ms = atoi(NEXT());
        else if (!strcmp(arg, "--join-timeout"))
            a.join_timeout = atoi(NEXT());
        else if (!strcmp(arg, "--max-poll-records"))
            a.max_poll_records = atoi(NEXT());
        else if (!strcmp(arg, "--single-poll"))
            a.single_poll = 1;
        else if (!strcmp(arg, "--fetch-min-bytes"))
            a.fetch_min_bytes = NEXT();
        else if (!strcmp(arg, "--fetch-wait-max-ms"))
            a.fetch_wait_max_ms = NEXT();
        else if (!strcmp(arg, "--fetch-message-max-bytes"))
            a.fetch_message_max_bytes = NEXT();
        else if (!strcmp(arg, "--fetch-max-bytes"))
            a.fetch_max_bytes = NEXT();
        else if (!strcmp(arg, "--queued-min-messages"))
            a.queued_min_messages = NEXT();
        else if (!strcmp(arg, "--queued-max-messages-kbytes"))
            a.queued_max_messages_kbytes = NEXT();
        else if (!strcmp(arg, "--protocol"))
            a.protocol = NEXT();
        else if (!strcmp(arg, "--kafka-bin"))
            a.kafka_bin = NEXT();
        else if (!strcmp(arg, "--results-dir"))
            a.results_dir = NEXT();
        else if (!strcmp(arg, "--offset-reset"))
            (void)NEXT(); /* accepted for parity; always latest */
        else if (!strcmp(arg, "--conf")) {
            if (a.extra_conf_n < 32)
                a.extra_conf[a.extra_conf_n++] = NEXT();
        } else if (!strcmp(arg, "--no-produce"))
            a.no_produce = 1;
        else {
            fprintf(stderr, "unknown arg: %s\n", arg);
            return 2;
        }
#undef NEXT
    }
    if (a.group_id[0] == '\0')
        snprintf(a.group_id, sizeof(a.group_id), "cmp-librdkafka-c-%lld",
                 (long long)now_millis());

    printf("======================================================================\n");
    printf("Consumer E2E Latency Benchmark - librdkafka (native C)\n");
    printf("======================================================================\n");
    printf("Bootstrap:   %s\n", a.bootstrap);
    printf("Topic:       %s\n", a.topic);
    printf("Group:       %s\n", a.group_id);
    printf("Throughput:  %d msg/s\n", a.throughput);
    printf("Duration:    %d s (after warmup)\n", a.duration);
    printf("Msg size:    %d bytes\n", a.message_size);
    printf("Warmup:      %d messages\n", a.warmup);
    printf("Interval:    %d s\n", a.interval);
    printf("rdkafka ver: %s\n", rd_kafka_version_str());
    printf("======================================================================\n");

    char errstr[512];
    rd_kafka_t *rk = build_consumer(&a, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "Failed to create consumer: %s\n", errstr);
        return 1;
    }

    /* Redirect poll to consumer queue. */
    rd_kafka_poll_set_consumer(rk);

    rd_kafka_topic_partition_list_t *topics =
        rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, a.topic, RD_KAFKA_PARTITION_UA);
    rd_kafka_resp_err_t serr = rd_kafka_subscribe(rk, topics);
    rd_kafka_topic_partition_list_destroy(topics);
    if (serr) {
        fprintf(stderr, "subscribe failed: %s\n", rd_kafka_err2str(serr));
        rd_kafka_destroy(rk);
        return 1;
    }
    printf("\n>>> Subscribed; waiting for partition assignment "
           "(group.protocol=%s)...\n",
           a.protocol);
    fflush(stdout);

    /* Batch consume from the consumer queue, matching the Rust consumer's
     * max.poll.records=500. rd_kafka_consume_batch_queue() on the consumer
     * queue services rebalance/error events the same way
     * rd_kafka_consumer_poll() does (poll_set_consumer routed the queue), so
     * the COOPERATIVE incremental_assign rebalance_cb still fires. */
    rd_kafka_queue_t *rkqu = rd_kafka_queue_get_consumer(rk);
    if (!rkqu) {
        fprintf(stderr, "failed to get consumer queue\n");
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
        return 1;
    }
    int max_batch = a.max_poll_records > 0 ? a.max_poll_records : 1;
    rd_kafka_message_t **rkmessages =
        calloc((size_t)max_batch, sizeof(rd_kafka_message_t *));
    if (!rkmessages) {
        fprintf(stderr, "out of memory allocating message batch array\n");
        rd_kafka_queue_destroy(rkqu);
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
        return 1;
    }

    double join_start = monotonic_s();
    double last_log = join_start;
    while (1) {
        rd_kafka_message_t *msg =
            rd_kafka_consumer_poll(rk, a.poll_timeout_ms);
        if (msg)
            rd_kafka_message_destroy(msg);
        if (g_assigned && current_assignment_count(rk) > 0) {
            printf("    assigned after %.1fs\n", monotonic_s() - join_start);
            fflush(stdout);
            break;
        }
        double nowm = monotonic_s();
        if (nowm - last_log >= 5) {
            printf("    [join] %.0fs elapsed, assignment=%d\n",
                   nowm - join_start, current_assignment_count(rk));
            fflush(stdout);
            last_log = nowm;
        }
        if (nowm - join_start >= a.join_timeout) {
            fprintf(stderr,
                    "ERROR: timed out (%ds) waiting for assignment with "
                    "group.protocol=%s\n",
                    a.join_timeout, a.protocol);
            rd_kafka_consumer_close(rk);
            rd_kafka_destroy(rk);
            return 2;
        }
    }

    /* Settle to the live edge (poll discarding until two consecutive empties). */
    printf(">>> Settling to the live edge (polling until empty) before "
           "producer...\n");
    fflush(stdout);
    double settle_deadline = monotonic_s() + 15;
    int empties = 0;
    while (1) {
        rd_kafka_message_t *msg =
            rd_kafka_consumer_poll(rk, a.poll_timeout_ms);
        if (!msg) {
            empties++;
            if (empties >= 2)
                break;
        } else if (msg->err) {
            empties++;
            rd_kafka_message_destroy(msg);
        } else {
            empties = 0;
            rd_kafka_message_destroy(msg);
        }
        if (monotonic_s() >= settle_deadline) {
            printf("    (settle timeout - proceeding)\n");
            break;
        }
    }
    printf("    at live edge; starting producer now.\n");
    fflush(stdout);

    int64_t total_records =
        (int64_t)a.throughput * (a.duration + 30) + a.warmup;
    if (!a.no_produce)
        g_producer_pid = spawn_producer(&a, total_records);
    if (g_producer_pid < 0) {
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
        return 1;
    }

    hist_t overall, interval_hist;
    hist_init(&overall);
    hist_init(&interval_hist);
    /* Records-per-batch distribution over NON-EMPTY batches during the
     * measurement window (post-warmup), mirroring the Rust harness. Reuses the
     * 1-unit-bucket histogram; batch sizes are bounded by max_poll_records. */
    hist_t rpp;
    hist_init(&rpp);
    cpu_sampler_t sampler;
    cpu_sampler_init(&sampler);
    /* Run-level current-RSS aggregation (avg/min/max over interval samples). */
    double rss_sum = 0.0, rss_min = 1e18, rss_max = 0.0;
    int rss_samples = 0;

    /* JSONL sink: <results_dir>/<group_id>/metrics.jsonl so each run lands in
     * its own directory (e.g. cmp-librdkafka-c2). */
    char run_dir[1024];
    snprintf(run_dir, sizeof(run_dir), "%s/%s", a.results_dir, a.group_id);
    char mkcmd[1100];
    snprintf(mkcmd, sizeof(mkcmd), "mkdir -p '%s'", run_dir);
    if (system(mkcmd) != 0)
        fprintf(stderr, "warning: mkdir -p %s failed\n", run_dir);
    char jsonl_path[1200];
    snprintf(jsonl_path, sizeof(jsonl_path), "%s/metrics.jsonl", run_dir);
    FILE *jsonl = fopen(jsonl_path, "w");
    if (!jsonl) {
        fprintf(stderr, "cannot open %s: %s\n", jsonl_path, strerror(errno));
        kill_producer();
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
        return 1;
    }

    int64_t messages_consumed = 0;
    /* Records-per-batch tracking (only non-empty batches counted, mirroring
     * how the Rust harness counts poll() calls that returned records). */
    int64_t batch_calls = 0;
    int64_t batch_records = 0;
    int warmup_complete = 0;
    int first_record_seen = 0;
    double measure_start = 0.0;
    double interval_start = 0.0;
    int interval_count = 0;
    double loop_start = monotonic_s();
    double no_data_deadline = loop_start + 120;

    printf("\n>>> Measuring (warmup %d msgs, then %d s)...\n\n", a.warmup,
           a.duration);
    fflush(stdout);

    while (1) {
        /* TWO-PHASE return-available consume (do NOT wait-to-fill the cap):
         *   1. Block up to poll_timeout for the FIRST message (size 1).
         *   2. If we got one, non-blocking drain (timeout 0) whatever else is
         *      already buffered locally, up to the remaining cap.
         * This makes the drained batch size reflect what is actually available
         * rather than artificially waiting until `max` messages accumulate, so
         * it is comparable to Rust's broker-reply-driven batches. */
        ssize_t n;
        if (a.single_poll) {
            /* Single-message consumer poll — the common rdkafka usage pattern
             * (one message per rd_kafka_consumer_poll call). */
            rd_kafka_message_t *m = rd_kafka_consumer_poll(rk, a.poll_timeout_ms);
            n = m ? 1 : 0;
            if (m)
                rkmessages[0] = m;
        } else {
            n = rd_kafka_consume_batch_queue(rkqu, a.poll_timeout_ms,
                                             rkmessages, 1);
            if (n > 0 && max_batch > 1) {
                ssize_t more = rd_kafka_consume_batch_queue(
                    rkqu, 0, rkmessages + n, (size_t)(max_batch - 1));
                if (more > 0)
                    n += more;
            }
        }
        if (n < 0) {
            fprintf(stderr, "batch consume error: %s\n",
                    rd_kafka_err2str(rd_kafka_last_error()));
            break;
        }
        if (n == 0) {
            if (!warmup_complete && monotonic_s() >= no_data_deadline &&
                messages_consumed == 0) {
                fprintf(stderr,
                        "ERROR: no records within 120s (producer not "
                        "running?)\n");
                break;
            }
            if (warmup_complete &&
                (monotonic_s() - measure_start) >= a.duration)
                break;
            continue;
        }

        /* One timestamp read per batch, mirroring the Rust harness which reads
         * the wall clock once per poll() and applies it to every record in the
         * returned batch. */
        int64_t poll_now = now_millis();
        batch_calls++;
        batch_records += (int64_t)n;
        /* Records-per-batch distribution, post-warmup only (mirrors the Rust
         * harness which records records-per-poll once warmup is complete). */
        if (warmup_complete)
            hist_record(&rpp, (int64_t)n);

        for (ssize_t mi = 0; mi < n; mi++) {
            rd_kafka_message_t *msg = rkmessages[mi];
            if (msg->err) {
                /* PARTITION_EOF and others: skip, mirror python. */
                rd_kafka_message_destroy(msg);
                continue;
            }

            if (!first_record_seen) {
                first_record_seen = 1;
                printf(">>> first records arrived %.1fs after producer start\n",
                       monotonic_s() - loop_start);
                fflush(stdout);
            }

            messages_consumed++;
            if (messages_consumed <= a.warmup) {
                if (messages_consumed == a.warmup) {
                    warmup_complete = 1;
                    measure_start = monotonic_s();
                    interval_start = measure_start;
                    printf("    warmup complete (%d msgs); measuring now\n",
                           a.warmup);
                    fflush(stdout);
                }
                rd_kafka_message_destroy(msg);
                continue;
            }

            rd_kafka_timestamp_type_t tstype;
            int64_t ts = rd_kafka_message_timestamp(msg, &tstype);
            if (ts > 0) {
                int64_t latency = poll_now - ts;
                hist_record(&overall, latency);
                hist_record(&interval_hist, latency);
            }
            rd_kafka_message_destroy(msg);
        }

        double now_mono = monotonic_s();
        if (warmup_complete && (now_mono - interval_start) >= a.interval) {
            double elapsed_s = now_mono - interval_start;
            double rss_mb = 0.0;
            double cpu = cpu_sampler_sample(&sampler, &rss_mb);
            rss_sum += rss_mb;
            if (rss_mb < rss_min) rss_min = rss_mb;
            if (rss_mb > rss_max) rss_max = rss_mb;
            rss_samples++;
            int64_t icount = interval_hist.count;
            double throughput =
                elapsed_s > 0 ? (double)icount / elapsed_s : 0.0;
            double avg = hist_avg(&interval_hist);
            int64_t p50 = hist_percentile(&interval_hist, 50);
            int64_t p99 = hist_percentile(&interval_hist, 99);
            int64_t p999 = hist_percentile(&interval_hist, 99.9);
            int64_t mx = hist_max(&interval_hist);
            double total_elapsed = now_mono - measure_start;
            printf("[interval %d] t=%6.1fs msgs=%7lld thr=%9.0f msg/s  "
                   "avg=%6.2f p50=%lld p99=%lld p999=%lld max=%lld ms  "
                   "cpu=%5.1f%% rss=%6.1fMB\n",
                   interval_count, total_elapsed, (long long)icount, throughput,
                   avg, (long long)p50, (long long)p99, (long long)p999,
                   (long long)mx, cpu, rss_mb);
            fflush(stdout);
            fprintf(jsonl,
                    "{\"type\":\"interval\",\"idx\":%d,\"elapsed_s\":%.2f,"
                    "\"interval_s\":%.2f,\"msgs\":%lld,"
                    "\"throughput_msg_s\":%.2f,\"lat_avg_ms\":%.2f,"
                    "\"lat_p50_ms\":%lld,\"lat_p99_ms\":%lld,"
                    "\"lat_p999_ms\":%lld,\"lat_max_ms\":%lld,"
                    "\"cpu_pct\":%.1f,\"rss_mb\":%.1f}\n",
                    interval_count, total_elapsed, elapsed_s, (long long)icount,
                    throughput, avg, (long long)p50, (long long)p99,
                    (long long)p999, (long long)mx, cpu, rss_mb);
            fflush(jsonl);
            hist_reset(&interval_hist);
            interval_start = monotonic_s();
            interval_count++;
        }

        if (warmup_complete && (monotonic_s() - measure_start) >= a.duration)
            break;
    }

    double measured_duration_s =
        measure_start > 0.0 ? (monotonic_s() - measure_start) : 0.0;

    kill_producer();
    rd_kafka_consumer_close(rk);

    /* Summary. */
    int64_t total_bytes = overall.count * a.message_size;
    double throughput_msg_s =
        measured_duration_s > 0 ? (double)overall.count / measured_duration_s
                                : 0.0;
    double throughput_mib_s =
        measured_duration_s > 0
            ? ((double)total_bytes / (1024.0 * 1024.0)) / measured_duration_s
            : 0.0;
    int64_t mn = hist_min(&overall);
    double avg = hist_avg(&overall);
    int64_t mx = hist_max(&overall);
    int64_t p50 = hist_percentile(&overall, 50);
    int64_t p90 = hist_percentile(&overall, 90);
    int64_t p95 = hist_percentile(&overall, 95);
    int64_t p99 = hist_percentile(&overall, 99);
    int64_t p999 = hist_percentile(&overall, 99.9);
    double sd = hist_stddev(&overall);
    double rss_avg = rss_samples > 0 ? rss_sum / rss_samples : 0.0;
    if (rss_samples == 0) rss_min = 0.0;
    double avg_records_per_batch =
        batch_calls > 0 ? (double)batch_records / (double)batch_calls : 0.0;
    /* Records-per-batch distribution (post-warmup, non-empty batches). */
    int64_t rpp_n = rpp.count;
    int64_t rpp_min = hist_min(&rpp);
    double rpp_mean = hist_avg(&rpp);
    int64_t rpp_p50 = hist_percentile(&rpp, 50);
    int64_t rpp_p99 = hist_percentile(&rpp, 99);
    int64_t rpp_max = hist_max(&rpp);

    printf("\n======================================================================\n");
    printf("SUMMARY - librdkafka-c (warmup %d excluded, group.protocol=%s)\n",
           a.warmup, a.protocol);
    printf("======================================================================\n");
    printf("Measured messages: %lld\n", (long long)overall.count);
    printf("Duration:          %.2f s\n", measured_duration_s);
    printf("Throughput:        %.0f msg/s  (%.2f MiB/s)\n", throughput_msg_s,
           throughput_mib_s);
    printf("Batch consume:     max_poll_records=%d, %lld non-empty batches, "
           "%lld records, avg %.1f records/batch\n",
           a.max_poll_records, (long long)batch_calls, (long long)batch_records,
           avg_records_per_batch);
    printf("Records/batch dist (post-warmup): n=%lld min=%lld mean=%.1f "
           "p50=%lld p99=%lld max=%lld\n",
           (long long)rpp_n, (long long)rpp_min, rpp_mean, (long long)rpp_p50,
           (long long)rpp_p99, (long long)rpp_max);
    printf("E2E latency (ms):  min=%lld avg=%.2f stddev=%.2f p50=%lld p90=%lld "
           "p95=%lld p99=%lld p99.9=%lld max=%lld\n",
           (long long)mn, avg, sd, (long long)p50, (long long)p90, (long long)p95,
           (long long)p99, (long long)p999, (long long)mx);
    printf("RSS (MB, current):  avg=%.1f min=%.1f max=%.1f (%d samples)\n",
           rss_avg, rss_min, rss_max, rss_samples);
    printf("======================================================================\n");

    fprintf(jsonl,
            "{\"type\":\"summary\",\"client\":\"librdkafka-c\",\"protocol\":"
            "\"%s\",\"messages\":%lld,\"duration_s\":%.2f,"
            "\"throughput_msg_s\":%.2f,\"throughput_mib_s\":%.2f,"
            "\"max_poll_records\":%d,\"batch_calls\":%lld,"
            "\"batch_records\":%lld,\"avg_records_per_batch\":%.2f,"
            "\"rpp_n\":%lld,\"rpp_min\":%lld,\"rpp_mean\":%.2f,"
            "\"rpp_p50\":%lld,\"rpp_p99\":%lld,\"rpp_max\":%lld,"
            "\"lat_min_ms\":%lld,\"lat_avg_ms\":%.2f,\"lat_stddev_ms\":%.2f,"
            "\"lat_p50_ms\":%lld,"
            "\"lat_p90_ms\":%lld,\"lat_p95_ms\":%lld,\"lat_p99_ms\":%lld,"
            "\"lat_p999_ms\":%lld,\"lat_max_ms\":%lld,"
            "\"rss_avg_mb\":%.1f,\"rss_min_mb\":%.1f,\"rss_max_mb\":%.1f}\n",
            a.protocol, (long long)overall.count, measured_duration_s,
            throughput_msg_s, throughput_mib_s, a.max_poll_records,
            (long long)batch_calls, (long long)batch_records,
            avg_records_per_batch, (long long)rpp_n, (long long)rpp_min,
            rpp_mean, (long long)rpp_p50, (long long)rpp_p99, (long long)rpp_max,
            (long long)mn, avg, sd, (long long)p50, (long long)p90, (long long)p95,
            (long long)p99, (long long)p999, (long long)mx,
            rss_avg, rss_min, rss_max);
    fflush(jsonl);
    fclose(jsonl);

    printf("\nResults written to: %s\n", run_dir);

    free(rkmessages);
    rd_kafka_queue_destroy(rkqu);
    hist_free(&overall);
    hist_free(&interval_hist);
    hist_free(&rpp);
    rd_kafka_destroy(rk);
    return 0;
}
