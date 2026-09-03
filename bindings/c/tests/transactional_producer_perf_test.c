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

// Transactional producer performance test for the C bindings (Phase 1,
// TXN_MODE=produce).
//
// Mirrors bindings/c/tests/producer_perf_test.c's v2 (librdkafka) path, driving
// the librdkafka transactional producer API. It shares the configuration
// contract, message shape and metrics-file schema (metrics.jsonl / results.json)
// of the sibling transactional producer performance tests
// (tests/performance/transactional_producer_perf_test.rs and
// TransactionalProducerPerformanceTest.java) so results are comparable across
// implementations.
//
// SCOPE (plan decision D1): this harness supports the librdkafka backend ONLY.
// The Rust client C-FFI (CLIENT_VERSION=3) has no transactional entry points,
// so a v3 transactional path is impossible. CLIENT_VERSION defaults to 2
// (librdkafka); if CLIENT_VERSION=3 is requested the harness prints an
// explanatory message and exits NON-ZERO — it does NOT silently run a
// non-transactional path.
//
// Produce-mode loop, per producer thread: rd_kafka_begin_transaction ->
// rd_kafka_producev x RECORDS_PER_TRANSACTION -> rd_kafka_commit_transaction (or
// rd_kafka_abort_transaction per the deterministic abort rule). Aborted
// transactions still produce their records before aborting; their records count
// toward neither throughput nor latency.
//
// Latency: per-record latency = a record's producev() -> the moment its
// transaction's commit completes (committed txns only). Per-transaction commit
// latency = begin_transaction -> commit_transaction completes. Per-transaction
// abort latency = begin_transaction -> abort_transaction completes
// (deterministic-abort path only; symmetric with commit latency). Emitted as
// abort_latency_ms in results.json and an abort_latency bucket per metrics.jsonl
// window (Kaushik: "Should we track abort latencies also?").
//
// EOS source throughput (Kaushik C:1612): the EOS throughput ceiling is
// min(source-produce-rate, txn-process-rate). eos runs require SOURCE_TOPIC to be
// PRE-POPULATED by a HIGH-THROUGHPUT non-transactional producer (idempotence
// off/default, large batch + linger, LIMIT_RPS=0) with enough records for the
// whole measured window — this C harness does NOT seed the source itself (it
// needs a reachable, pre-populated broker). The consumer never hangs on an
// under-fed source: the poll is bounded (EOS_POLL_TIMEOUT_MS) and an empty poll
// is skipped rather than opening an empty transaction.
//
// KIP-848 (Kaushik C:128): all consumers across the transactional perf harnesses
// (Rust / librdkafka-C / Java / Python) run on the KIP-848 consumer group
// protocol; the EOS consumer here sets group.protocol=consumer. Requires a
// KIP-848-capable broker (Kafka 4.x) + client (librdkafka >= 2.5).
//
// Configure via the same environment variables as producer_perf_test.c
// (BOOTSTRAP_SERVERS, NUM_MESSAGES, LIMIT_RPS, KEY_SIZE, VALUE_SIZE, BATCH_SIZE,
// MAX_REQUEST_SIZE, BUFFER_MEMORY, LINGER_MS, MAX_IN_FLIGHT, COMPRESSION_TYPE,
// USE_DEFAULTS, WARMUP_SECONDS, TEST_DURATION_SECONDS, DO_VERIFY, P99_LIMIT_MS,
// TOPIC_NAME, METRICS_FILE, RESULTS_FILE, SECURITY_PROTOCOL, SASL_MECHANISM,
// SASL_USERNAME, SASL_PASSWORD, SSL_CA_LOCATION, CREATE_TOPIC, PARTITIONS) plus
// the transaction knobs (RECORDS_PER_TRANSACTION, ABORT_RATE,
// NUM_TRANSACTIONAL_PRODUCERS, TRANSACTIONAL_ID, TXN_MODE). enable.idempotence
// and acks=all are forced on (required for transactions). Needs a reachable
// broker; opt-in (not run under ctest).

#include <librdkafka/rdkafka.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>
#include <limits.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <math.h>

#define GENERATED_MESSAGE_COUNT 10000
#define RANDOMNESS 0.5f
#define DEFAULT_KEY_SIZE 0
#define DEFAULT_VALUE_SIZE 2048
// Upper bound (ms) of the latency histograms used for the p99 assertion.
#define MAX_LATENCY_MS 10000
// Linux USER_HZ — clock ticks per second used to convert /proc/self/stat
// utime/stime into CPU seconds. Effectively always 100 on Linux.
#define USER_HZ 100.0

static long KEY_SIZE = DEFAULT_KEY_SIZE;
static long VALUE_SIZE = DEFAULT_VALUE_SIZE;
static long MESSAGE_SIZE = DEFAULT_KEY_SIZE + DEFAULT_VALUE_SIZE;
static int RANDOM_KEY = (int)(DEFAULT_KEY_SIZE * RANDOMNESS);
static int RANDOM_VALUE = (int)(DEFAULT_VALUE_SIZE * RANDOMNESS);
static long NUM_MESSAGES = 0;
static long LIMIT_RPS = 0;
static const char* TOPIC = "test-topic";
static bool DO_VERIFY = true;
// This harness is librdkafka-only (D1). Default backend is librdkafka (2).
static int CLIENT_VERSION = 2;
static int WARMUP_S = 120;
static int TEST_DURATION_S = 600;
static bool CREATE_TOPIC = true;
static int PARTITIONS = -1;
static const char *BOOTSTRAP_SERVERS = "localhost:9092";
static const char *SECURITY_PROTOCOL = NULL;
static const char *SASL_MECHANISM = NULL;
static const char *SASL_USERNAME = NULL;
static const char *SASL_PASSWORD = NULL;
static const char *SSL_CA_LOCATION = NULL;
static int BATCH_SIZE_KB = 1024;
static int MAX_REQUEST_SIZE_KB = -1;
// NULL means "not overridden": transactions force enable.idempotence=true, and
// librdkafka uses its idempotent default of 5 when the property is left unset.
// This mirrors the Rust/Java harnesses, which only apply MAX_IN_FLIGHT when the
// env var is present. Do NOT default this to a value > 5 — librdkafka rejects
// rd_kafka_new when a user-modified max.in.flight exceeds 5 under idempotence.
static const char *MAX_IN_FLIGHT = NULL;
static int BUFFER_MEMORY_MB = -1;
static const char *COMPRESSION_TYPE = "none";
static const char *LINGER_MS = "5";
static bool USE_DEFAULTS = false;
static bool sasl_enabled = false;
static int64_t buffer_memory_bytes;
static int64_t batch_size_bytes;
static int64_t max_request_size_bytes;

// --- transaction-specific knobs ---
static long RECORDS_PER_TRANSACTION = 100;
static double ABORT_RATE = 0.0;
static long NUM_TRANSACTIONAL_PRODUCERS = 1;
static const char *TRANSACTIONAL_ID = "perf-txn";
static const char *TXN_MODE = "produce";
// --- EOS-mode knobs (TXN_MODE=eos) ---
static bool EOS_MODE = false;
// Input topic consumed/transformed in EOS mode (required when TXN_MODE=eos).
// Must be pre-populated: the C harness needs a reachable broker and does not
// seed the source topic (unlike the Rust in-suite path).
static const char *SOURCE_TOPIC = NULL;
// Consumer group id shared across all producers' consumers, so the group
// coordinator divides the source partitions among them (canonical EOS scaling).
// Defaults to "<TRANSACTIONAL_ID>-eos-consumer"; overridable via GROUP_ID.
static const char *GROUP_ID = NULL;
// Per-poll timeout (ms): bounds rd_kafka_consumer_poll so a run with no source
// data cannot block forever.
#define EOS_POLL_TIMEOUT_MS 500

// Per-message p99 latency budget (ms). 0 disables the assertion.
static long P99_LIMIT_MS = 0;
static const char *RESULTS_FILE = "results.json";
static const char *METRICS_FILE = "metrics.jsonl";
static bool latency_budget_exceeded = false;
static bool interrupted = false;

// --- shared statistics, written by all producer threads (guarded by
// stats_mutex; contention is low: once per committed record for the latency
// histogram, once per transaction for the commit histogram). ---
static pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;
static long completed_messages = 0;   // committed records
static long verified = 0;             // committed records that passed verification
static long committed_transactions = 0;
static long aborted_transactions = 0;
static long aborted_records = 0;
static long total_latency = 0;        // per-record, ns
static long max_latency = 0;          // per-record, ns
static long total_commit_latency = 0; // per-transaction, ns
static long max_commit_latency = 0;   // per-transaction, ns
static long total_abort_latency = 0;  // per-transaction, ns (deterministic abort)
static long max_abort_latency = 0;    // per-transaction, ns (deterministic abort)
// Per-record, per-transaction commit and per-transaction abort latency
// histograms (ms) for the summary percentiles.
static long latency_hist[MAX_LATENCY_MS + 2];
static long commit_latency_hist[MAX_LATENCY_MS + 2];
static long abort_latency_hist[MAX_LATENCY_MS + 2];

typedef struct {
    uint8_t* key;
    uint8_t* value;
} Message;

// Per-message future: the producev() opaque. The delivery-report callback fills
// the metadata and release-stores `done`; the producer thread acquire-loads
// `done` after commit to verify. `topic` borrows rd_kafka_topic_name() (valid
// until rd_kafka_destroy). Per-record latency is computed at commit completion
// (commit_ns - produce_ts), so the delivery-report timing is not needed.
typedef struct {
    int64_t offset;
    int32_t partition;
    const char* topic;
    int64_t timestamp;
    rd_kafka_resp_err_t err;
    atomic_bool done;
} TxnFuture;

static long current_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000L) + (tv.tv_usec / 1000L);
}

static long current_time_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ts.tv_sec * 1000000000L) + ts.tv_nsec;
}

// Deterministic, evenly-spread abort selection (Bresenham). Per-producer 0-based
// transaction index i; abort iff floor((i+1)*rate) > floor(i*rate).
static bool should_abort(long txn_index, double abort_rate) {
    if (abort_rate <= 0.0) {
        return false;
    }
    double i = (double)txn_index;
    return floor((i + 1.0) * abort_rate) > floor(i * abort_rate);
}

static bool verify_rdkafka_message(TxnFuture* f) {
    if (!DO_VERIFY) {
        return true;
    }
    if (!f) {
        fprintf(stderr, "TxnFuture is NULL\n");
        return false;
    }
    if (f->offset < 0) {
        fprintf(stderr, "Invalid offset: %" PRId64 "\n", f->offset);
        return false;
    }
    if (f->partition < 0) {
        fprintf(stderr, "Invalid partition: %d\n", f->partition);
        return false;
    }
    if (!f->topic || strcmp(f->topic, TOPIC) != 0) {
        fprintf(stderr, "Topic mismatch: expected '%s', got '%s'\n",
                TOPIC, f->topic ? f->topic : "NULL");
        return false;
    }
    if (f->timestamp < 0) {
        fprintf(stderr, "Invalid timestamp: %" PRId64 "\n", f->timestamp);
        return false;
    }
    return true;
}

static void txn_dr_cb(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage,
                      void *opaque) {
    (void)rk; (void)opaque;
    TxnFuture *f = rkmessage->_private;
    if (!f) {
        return;
    }
    f->offset = rkmessage->offset;
    f->partition = rkmessage->partition;
    f->topic = rkmessage->rkt ? rd_kafka_topic_name(rkmessage->rkt) : NULL;
    f->timestamp = rd_kafka_message_timestamp(rkmessage, NULL);
    f->err = rkmessage->err;
    atomic_store_explicit(&f->done, true, memory_order_release);
}

// ---------------------------------------------------------------------------
// Process stats (CPU / RSS) — identical to producer_perf_test.c.
// ---------------------------------------------------------------------------

typedef struct {
    long prev_busy_ticks;
    long prev_time_ns;
} ProcSampler;

static ProcSampler proc_sampler;

static long read_busy_ticks() {
    FILE* fp = fopen("/proc/self/stat", "r");
    if (fp == NULL) return 0;
    char buf[4096];
    char* line = fgets(buf, sizeof(buf), fp);
    fclose(fp);
    if (line == NULL) return 0;
    char* rparen = strrchr(buf, ')');
    if (rparen == NULL) return 0;
    char* rest = rparen + 1;
    long utime = 0, stime = 0;
    int idx = 0;
    char* saveptr = NULL;
    for (char* tok = strtok_r(rest, " \t\n", &saveptr);
         tok != NULL;
         tok = strtok_r(NULL, " \t\n", &saveptr)) {
        if (idx == 11) {
            utime = atol(tok);
        } else if (idx == 12) {
            stime = atol(tok);
            break;
        }
        idx++;
    }
    return utime + stime;
}

static long read_rss_bytes() {
    FILE* fp = fopen("/proc/self/status", "r");
    if (fp == NULL) return 0;
    char line[256];
    long rss = 0;
    while (fgets(line, sizeof(line), fp) != NULL) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            rss = atol(line + 6) * 1024; // value is in kB
            break;
        }
    }
    fclose(fp);
    return rss;
}

static void proc_sampler_init(ProcSampler* s) {
    s->prev_busy_ticks = read_busy_ticks();
    s->prev_time_ns = current_time_ns();
}

static void proc_sampler_sample(ProcSampler* s, double* cpu_percent, long* rss_bytes) {
    long busy = read_busy_ticks();
    long now = current_time_ns();
    double dt = (double)(now - s->prev_time_ns) / 1e9;
    long dticks = busy - s->prev_busy_ticks;
    if (dticks < 0) dticks = 0;
    s->prev_busy_ticks = busy;
    s->prev_time_ns = now;
    *cpu_percent = dt > 0.0 ? ((double)dticks / USER_HZ) / dt * 100.0 : 0.0;
    *rss_bytes = read_rss_bytes();
}

// ---------------------------------------------------------------------------
// Metrics (per-second rollover to metrics.jsonl), with transaction additions.
// ---------------------------------------------------------------------------

typedef struct {
    double total;
    long count;
    double max;
} Bucket;

typedef struct {
    FILE* file;
    Bucket latency;
    long latency_pct_hist[MAX_LATENCY_MS + 2];
    Bucket bytes;
    Bucket messages;
    // Committed-transaction throughput + per-transaction commit latency.
    Bucket transactions;
    Bucket commit_latency;
    long commit_latency_pct_hist[MAX_LATENCY_MS + 2];
    // Per-transaction abort latency (deterministic-abort path only), symmetric
    // with commit_latency.
    Bucket abort_latency;
    long abort_latency_pct_hist[MAX_LATENCY_MS + 2];
    Bucket rss;
    Bucket cpu;
    long window_start_ms;
    long measurement_start_ms;
    long measurement_end_ms;
    pthread_t thread;
    bool running;
    pthread_mutex_t mutex;
    double last_cpu;
    double last_rss;
    long total_external_metrics;
    double total_cpu;
    double total_rss;
} Metrics;

static Metrics metrics;

static void bucket_init(Bucket* bucket) {
    bucket->total = 0.0;
    bucket->count = 0;
    bucket->max = -INFINITY;
}

static void bucket_add(Bucket* bucket, double measurement) {
    bucket->total += measurement;
    bucket->count++;
    if (measurement > bucket->max) {
        bucket->max = measurement;
    }
}

static double bucket_average(Bucket* bucket) {
    if (bucket->count == 0) return 0.0;
    return bucket->total / bucket->count;
}

static void metrics_init(Metrics* m) {
    m->file = fopen(METRICS_FILE, "w");
    if (m->file == NULL) {
        fprintf(stderr, "Failed to open metrics file %s\n", METRICS_FILE);
        exit(1);
    }
    bucket_init(&m->latency);
    bucket_init(&m->bytes);
    bucket_init(&m->messages);
    bucket_init(&m->transactions);
    bucket_init(&m->commit_latency);
    bucket_init(&m->abort_latency);
    bucket_init(&m->rss);
    bucket_init(&m->cpu);
    memset(m->latency_pct_hist, 0, sizeof(m->latency_pct_hist));
    memset(m->commit_latency_pct_hist, 0, sizeof(m->commit_latency_pct_hist));
    memset(m->abort_latency_pct_hist, 0, sizeof(m->abort_latency_pct_hist));
    m->window_start_ms = current_time_ms();
    m->measurement_start_ms = LONG_MIN;
    m->measurement_end_ms = LONG_MIN;
    m->running = false;
    m->total_external_metrics = 0;
    m->last_cpu = 0.0;
    m->last_rss = 0.0;
    m->total_cpu = 0.0;
    m->total_rss = 0.0;
    pthread_mutex_init(&m->mutex, NULL);
    proc_sampler_init(&proc_sampler);
}

static char *metrics_print_double(double f) {
    char *str = malloc(100 * sizeof(char));
    if (isinf(f)) {
        snprintf(str, 100, f > 0 ? "inf" : "-inf");
    } else {
        snprintf(str, 100, "%.10f", f);
    }
    return str;
}

static char *metrics_print_long(long f) {
    char *str = malloc(100 * sizeof(char));
    if (f == LONG_MIN) {
        snprintf(str, 100, "-inf");
    } else {
        snprintf(str, 100, "%ld", f);
    }
    return str;
}

static long percentile_from_hist(const long* hist, size_t len, double p) {
    long total = 0;
    for (size_t i = 0; i < len; i++) {
        total += hist[i];
    }
    if (total == 0) {
        return 0;
    }
    long target = (long)ceil((double)total * p);
    long cum = 0;
    for (size_t i = 0; i < len; i++) {
        cum += hist[i];
        if (cum >= target) {
            return (long)i;
        }
    }
    return (long)(len - 1);
}

static void metrics_rollover(Metrics* m) {
    pthread_mutex_lock(&m->mutex);

    long current_window_start = m->window_start_ms;
    m->window_start_ms = current_time_ms();

    double lat_avg = bucket_average(&m->latency);
    double lat_max = m->latency.max;
    double lat_total = m->latency.total;
    long lat_count = m->latency.count;

    double bytes_avg = bucket_average(&m->bytes);
    double bytes_max = m->bytes.max;
    double bytes_total = m->bytes.total;
    long bytes_count = m->bytes.count;

    double msgs_avg = bucket_average(&m->messages);
    double msgs_max = m->messages.max;
    double msgs_total = m->messages.total;
    long msgs_count = m->messages.count;

    double txn_avg = bucket_average(&m->transactions);
    double txn_max = m->transactions.max;
    double txn_total = m->transactions.total;
    long txn_count = m->transactions.count;

    double clat_avg = bucket_average(&m->commit_latency);
    double clat_max = m->commit_latency.max;
    double clat_total = m->commit_latency.total;
    long clat_count = m->commit_latency.count;

    double alat_avg = bucket_average(&m->abort_latency);
    double alat_max = m->abort_latency.max;
    double alat_total = m->abort_latency.total;
    long alat_count = m->abort_latency.count;

    double cpu;
    long rss;
    proc_sampler_sample(&proc_sampler, &cpu, &rss);
    if (cpu >= 0.0) bucket_add(&m->cpu, cpu);
    if (rss >= 0) bucket_add(&m->rss, (double)rss);

    double cpu_avg = bucket_average(&m->cpu);
    double cpu_max = m->cpu.max;
    double cpu_total = m->cpu.total;
    long cpu_count = m->cpu.count;

    double rss_avg = bucket_average(&m->rss);
    double rss_max = m->rss.max;
    double rss_total = m->rss.total;
    long rss_count = m->rss.count;

    m->last_cpu = cpu_max;
    m->last_rss = rss_max;

    if (m->measurement_start_ms != LONG_MIN && m->measurement_end_ms == LONG_MIN) {
        m->total_external_metrics++;
        m->total_cpu += cpu;
        m->total_rss += (double)rss;
    }

    long lat_p50 = percentile_from_hist(m->latency_pct_hist, MAX_LATENCY_MS + 2, 0.50);
    long lat_p90 = percentile_from_hist(m->latency_pct_hist, MAX_LATENCY_MS + 2, 0.90);
    long lat_p99 = percentile_from_hist(m->latency_pct_hist, MAX_LATENCY_MS + 2, 0.99);
    long lat_p999 = percentile_from_hist(m->latency_pct_hist, MAX_LATENCY_MS + 2, 0.999);
    memset(m->latency_pct_hist, 0, sizeof(m->latency_pct_hist));

    long clat_p50 = percentile_from_hist(m->commit_latency_pct_hist, MAX_LATENCY_MS + 2, 0.50);
    long clat_p90 = percentile_from_hist(m->commit_latency_pct_hist, MAX_LATENCY_MS + 2, 0.90);
    long clat_p99 = percentile_from_hist(m->commit_latency_pct_hist, MAX_LATENCY_MS + 2, 0.99);
    long clat_p999 = percentile_from_hist(m->commit_latency_pct_hist, MAX_LATENCY_MS + 2, 0.999);
    memset(m->commit_latency_pct_hist, 0, sizeof(m->commit_latency_pct_hist));

    long alat_p50 = percentile_from_hist(m->abort_latency_pct_hist, MAX_LATENCY_MS + 2, 0.50);
    long alat_p90 = percentile_from_hist(m->abort_latency_pct_hist, MAX_LATENCY_MS + 2, 0.90);
    long alat_p99 = percentile_from_hist(m->abort_latency_pct_hist, MAX_LATENCY_MS + 2, 0.99);
    long alat_p999 = percentile_from_hist(m->abort_latency_pct_hist, MAX_LATENCY_MS + 2, 0.999);
    memset(m->abort_latency_pct_hist, 0, sizeof(m->abort_latency_pct_hist));

    bucket_init(&m->latency);
    bucket_init(&m->bytes);
    bucket_init(&m->messages);
    bucket_init(&m->transactions);
    bucket_init(&m->commit_latency);
    bucket_init(&m->abort_latency);
    bucket_init(&m->rss);
    bucket_init(&m->cpu);

    pthread_mutex_unlock(&m->mutex);

    char *rss_avg_str = metrics_print_double(rss_avg);
    char *rss_max_str = metrics_print_double(rss_max);
    char *rss_total_str = metrics_print_double(rss_total);
    char *rss_count_str = metrics_print_long(rss_count);
    char *cpu_avg_str = metrics_print_double(cpu_avg);
    char *cpu_max_str = metrics_print_double(cpu_max);
    char *cpu_total_str = metrics_print_double(cpu_total);
    char *cpu_count_str = metrics_print_long(cpu_count);
    char *lat_avg_str = metrics_print_double(lat_avg);
    char *lat_max_str = metrics_print_double(lat_max);
    char *lat_total_str = metrics_print_double(lat_total);
    char *lat_count_str = metrics_print_long(lat_count);
    char *lat_p50_str = metrics_print_long(lat_p50);
    char *lat_p90_str = metrics_print_long(lat_p90);
    char *lat_p99_str = metrics_print_long(lat_p99);
    char *lat_p999_str = metrics_print_long(lat_p999);
    char *bytes_avg_str = metrics_print_double(bytes_avg);
    char *bytes_max_str = metrics_print_double(bytes_max);
    char *bytes_total_str = metrics_print_double(bytes_total);
    char *bytes_count_str = metrics_print_long(bytes_count);
    char *msgs_avg_str = metrics_print_double(msgs_avg);
    char *msgs_max_str = metrics_print_double(msgs_max);
    char *msgs_total_str = metrics_print_double(msgs_total);
    char *msgs_count_str = metrics_print_long(msgs_count);
    char *txn_avg_str = metrics_print_double(txn_avg);
    char *txn_max_str = metrics_print_double(txn_max);
    char *txn_total_str = metrics_print_double(txn_total);
    char *txn_count_str = metrics_print_long(txn_count);
    char *clat_avg_str = metrics_print_double(clat_avg);
    char *clat_max_str = metrics_print_double(clat_max);
    char *clat_total_str = metrics_print_double(clat_total);
    char *clat_count_str = metrics_print_long(clat_count);
    char *clat_p50_str = metrics_print_long(clat_p50);
    char *clat_p90_str = metrics_print_long(clat_p90);
    char *clat_p99_str = metrics_print_long(clat_p99);
    char *clat_p999_str = metrics_print_long(clat_p999);
    char *alat_avg_str = metrics_print_double(alat_avg);
    char *alat_max_str = metrics_print_double(alat_max);
    char *alat_total_str = metrics_print_double(alat_total);
    char *alat_count_str = metrics_print_long(alat_count);
    char *alat_p50_str = metrics_print_long(alat_p50);
    char *alat_p90_str = metrics_print_long(alat_p90);
    char *alat_p99_str = metrics_print_long(alat_p99);
    char *alat_p999_str = metrics_print_long(alat_p999);
    char *current_window_start_str = metrics_print_long(current_window_start);
    char *window_start_ms_str = metrics_print_long(m->window_start_ms);
    char *measurement_start_ms_str = metrics_print_long(m->measurement_start_ms);
    char *measurement_end_ms_str = metrics_print_long(m->measurement_end_ms);

    fprintf(m->file,
        "{\"rss\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
        "\"cpu\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
        "\"latency\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\","
        "\"p50\":\"%s\",\"p90\":\"%s\",\"p99\":\"%s\",\"p999\":\"%s\"},"
        "\"bytes\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
        "\"messages\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
        "\"transactions\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
        "\"commit_latency\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\","
        "\"p50\":\"%s\",\"p90\":\"%s\",\"p99\":\"%s\",\"p999\":\"%s\"},"
        "\"abort_latency\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\","
        "\"p50\":\"%s\",\"p90\":\"%s\",\"p99\":\"%s\",\"p999\":\"%s\"},"
        "\"window_start_ms\":\"%s\",\"window_end_ms\":\"%s\","
        "\"measurement_start_ms\":\"%s\",\"measurement_end_ms\":\"%s\"}\n",
        rss_avg_str, rss_max_str, rss_total_str, rss_count_str,
        cpu_avg_str, cpu_max_str, cpu_total_str, cpu_count_str,
        lat_avg_str, lat_max_str, lat_total_str, lat_count_str,
        lat_p50_str, lat_p90_str, lat_p99_str, lat_p999_str,
        bytes_avg_str, bytes_max_str, bytes_total_str, bytes_count_str,
        msgs_avg_str, msgs_max_str, msgs_total_str, msgs_count_str,
        txn_avg_str, txn_max_str, txn_total_str, txn_count_str,
        clat_avg_str, clat_max_str, clat_total_str, clat_count_str,
        clat_p50_str, clat_p90_str, clat_p99_str, clat_p999_str,
        alat_avg_str, alat_max_str, alat_total_str, alat_count_str,
        alat_p50_str, alat_p90_str, alat_p99_str, alat_p999_str,
        current_window_start_str, window_start_ms_str,
        measurement_start_ms_str, measurement_end_ms_str);
    fflush(m->file);

    free(rss_avg_str); free(rss_max_str); free(rss_total_str); free(rss_count_str);
    free(cpu_avg_str); free(cpu_max_str); free(cpu_total_str); free(cpu_count_str);
    free(lat_avg_str); free(lat_max_str); free(lat_total_str); free(lat_count_str);
    free(lat_p50_str); free(lat_p90_str); free(lat_p99_str); free(lat_p999_str);
    free(bytes_avg_str); free(bytes_max_str); free(bytes_total_str); free(bytes_count_str);
    free(msgs_avg_str); free(msgs_max_str); free(msgs_total_str); free(msgs_count_str);
    free(txn_avg_str); free(txn_max_str); free(txn_total_str); free(txn_count_str);
    free(clat_avg_str); free(clat_max_str); free(clat_total_str); free(clat_count_str);
    free(clat_p50_str); free(clat_p90_str); free(clat_p99_str); free(clat_p999_str);
    free(alat_avg_str); free(alat_max_str); free(alat_total_str); free(alat_count_str);
    free(alat_p50_str); free(alat_p90_str); free(alat_p99_str); free(alat_p999_str);
    free(current_window_start_str); free(window_start_ms_str);
    free(measurement_start_ms_str); free(measurement_end_ms_str);
}

static void metrics_external_metrics_aggregations(Metrics* m, double *average_cpu, double *average_rss) {
    pthread_mutex_lock(&m->mutex);
    *average_cpu = m->total_external_metrics > 0 ? m->total_cpu / m->total_external_metrics : 0.0;
    *average_rss = m->total_external_metrics > 0 ? m->total_rss / m->total_external_metrics : 0.0;
    pthread_mutex_unlock(&m->mutex);
}

static void metrics_external_metrics_last_values(Metrics* m, double *last_cpu, double *last_rss) {
    pthread_mutex_lock(&m->mutex);
    *last_cpu = m->last_cpu;
    *last_rss = m->last_rss;
    pthread_mutex_unlock(&m->mutex);
}

static void* metrics_thread_func(void* arg) {
    Metrics* m = (Metrics*)arg;
    while (m->running) {
        sleep(1);
        if (m->running) {
            metrics_rollover(m);
        }
    }
    return NULL;
}

static void metrics_start_collecting(Metrics* m) {
    if (m->running) return;
    m->running = true;
    pthread_create(&m->thread, NULL, metrics_thread_func, m);
}

static void metrics_stop_collecting(Metrics* m) {
    m->running = false;
    if (m->thread) {
        pthread_join(m->thread, NULL);
    }
    fclose(m->file);
}

// Record one committed record's per-record latency (ns) + throughput.
static void metrics_add_record(Metrics* m, long latency_ns) {
    double latency_ms = (double)latency_ns / 1e6;
    pthread_mutex_lock(&m->mutex);
    bucket_add(&m->latency, latency_ms);
    bucket_add(&m->bytes, (double)MESSAGE_SIZE);
    bucket_add(&m->messages, 1);
    long idx = (long)latency_ms;
    if (idx < 0) idx = 0;
    if (idx > MAX_LATENCY_MS + 1) idx = MAX_LATENCY_MS + 1;
    m->latency_pct_hist[idx]++;
    pthread_mutex_unlock(&m->mutex);
}

// Record one committed transaction's commit latency (ns).
static void metrics_add_commit(Metrics* m, long commit_latency_ns) {
    double clat_ms = (double)commit_latency_ns / 1e6;
    pthread_mutex_lock(&m->mutex);
    bucket_add(&m->transactions, 1);
    bucket_add(&m->commit_latency, clat_ms);
    long idx = (long)clat_ms;
    if (idx < 0) idx = 0;
    if (idx > MAX_LATENCY_MS + 1) idx = MAX_LATENCY_MS + 1;
    m->commit_latency_pct_hist[idx]++;
    pthread_mutex_unlock(&m->mutex);
}

// Record one aborted transaction's abort latency (ns) — begin -> abort completes,
// deterministic-abort path only (symmetric with metrics_add_commit).
static void metrics_add_abort(Metrics* m, long abort_latency_ns) {
    double alat_ms = (double)abort_latency_ns / 1e6;
    pthread_mutex_lock(&m->mutex);
    bucket_add(&m->abort_latency, alat_ms);
    long idx = (long)alat_ms;
    if (idx < 0) idx = 0;
    if (idx > MAX_LATENCY_MS + 1) idx = MAX_LATENCY_MS + 1;
    m->abort_latency_pct_hist[idx]++;
    pthread_mutex_unlock(&m->mutex);
}

static void metrics_set_measurement_start_ms(Metrics* m, long ms) {
    pthread_mutex_lock(&m->mutex);
    m->measurement_start_ms = ms;
    pthread_mutex_unlock(&m->mutex);
}

static void metrics_set_measurement_end_ms(Metrics* m, long ms) {
    pthread_mutex_lock(&m->mutex);
    m->measurement_end_ms = ms;
    pthread_mutex_unlock(&m->mutex);
}

// ---------------------------------------------------------------------------
// Message generation — identical to producer_perf_test.c.
// ---------------------------------------------------------------------------

static Message* generate_message_data() {
    Message* data = malloc(sizeof(Message) * GENERATED_MESSAGE_COUNT);

    uint8_t* static_key_bytes = NULL;
    if (KEY_SIZE > 0) {
        static_key_bytes = malloc(KEY_SIZE - RANDOM_KEY);
        for (int i = 0; i < KEY_SIZE - RANDOM_KEY; i++) {
            static_key_bytes[i] = rand() % 256;
        }
    }

    uint8_t* static_value_bytes = malloc(VALUE_SIZE - RANDOM_VALUE);
    for (int i = 0; i < VALUE_SIZE - RANDOM_VALUE; i++) {
        static_value_bytes[i] = rand() % 256;
    }

    for (int i = 0; i < GENERATED_MESSAGE_COUNT; i++) {
        uint8_t* random_key_bytes = NULL;
        if (RANDOM_KEY > 0) {
            random_key_bytes = malloc(RANDOM_KEY);
            for (int j = 0; j < RANDOM_KEY; j++) {
                random_key_bytes[j] = rand() % 256;
            }
        }
        uint8_t* random_value_bytes = malloc(RANDOM_VALUE);
        for (int j = 0; j < RANDOM_VALUE; j++) {
            random_value_bytes[j] = rand() % 256;
        }
        uint8_t* key_bytes = NULL;
        if (KEY_SIZE > 0) {
            key_bytes = malloc(KEY_SIZE);
            memcpy(key_bytes, static_key_bytes, KEY_SIZE - RANDOM_KEY);
            memcpy(key_bytes + KEY_SIZE - RANDOM_KEY, random_key_bytes, RANDOM_KEY);
        }
        uint8_t* value_bytes = malloc(VALUE_SIZE);
        memcpy(value_bytes, static_value_bytes, VALUE_SIZE - RANDOM_VALUE);
        memcpy(value_bytes + VALUE_SIZE - RANDOM_VALUE, random_value_bytes, RANDOM_VALUE);
        data[i].key = key_bytes;
        data[i].value = value_bytes;
        if (random_key_bytes) free(random_key_bytes);
        free(random_value_bytes);
    }

    if (static_key_bytes) free(static_key_bytes);
    free(static_value_bytes);
    return data;
}

static void free_message_data(Message* data) {
    for (int i = 0; i < GENERATED_MESSAGE_COUNT; i++) {
        if (data[i].key) free(data[i].key);
        free(data[i].value);
    }
    free(data);
}

// ---------------------------------------------------------------------------
// Producer creation
// ---------------------------------------------------------------------------

// Create a librdkafka transactional producer with a unique transactional.id.
static rd_kafka_t* create_producer(const char* transactional_id) {
    char buffer[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    char errstr[512];

    rd_kafka_conf_set(conf, "bootstrap.servers", BOOTSTRAP_SERVERS, NULL, 0);
    if (sasl_enabled) {
        rd_kafka_conf_set(conf, "security.protocol", SECURITY_PROTOCOL, NULL, 0);
        rd_kafka_conf_set(conf, "sasl.mechanism", SASL_MECHANISM, NULL, 0);
        rd_kafka_conf_set(conf, "sasl.username", SASL_USERNAME, NULL, 0);
        rd_kafka_conf_set(conf, "sasl.password", SASL_PASSWORD, NULL, 0);
    }
    if (SSL_CA_LOCATION) {
        rd_kafka_conf_set(conf, "ssl.ca.location", SSL_CA_LOCATION, NULL, 0);
    }

    // Transactions require idempotence and acks=all; force them on regardless of
    // USE_DEFAULTS. The transactional.id makes this a transactional producer.
    rd_kafka_conf_set(conf, "transactional.id", transactional_id, NULL, 0);
    rd_kafka_conf_set(conf, "enable.idempotence", "true", NULL, 0);
    rd_kafka_conf_set(conf, "acks", "all", NULL, 0);

    if (!USE_DEFAULTS) {
        if (buffer_memory_bytes > 0) {
            snprintf(buffer, sizeof(buffer), "%" PRId64, buffer_memory_bytes / 1024);
            rd_kafka_conf_set(conf, "queue.buffering.max.kbytes", buffer, NULL, 0);
        }
        snprintf(buffer, sizeof(buffer), "%" PRId64, batch_size_bytes);
        rd_kafka_conf_set(conf, "batch.size", buffer, NULL, 0);
        snprintf(buffer, sizeof(buffer), "%" PRId64, max_request_size_bytes);
        rd_kafka_conf_set(conf, "message.max.bytes", buffer, NULL, 0);
        rd_kafka_conf_set(conf, "compression.type", COMPRESSION_TYPE, NULL, 0);
        rd_kafka_conf_set(conf, "linger.ms", LINGER_MS, NULL, 0);
        // Only override max.in.flight when explicitly requested (MAX_IN_FLIGHT
        // env present, already clamped to <= 5 in main). Leaving it unset lets
        // librdkafka apply its idempotent default of 5, so the default config
        // path produces a working transactional producer.
        if (MAX_IN_FLIGHT != NULL) {
            rd_kafka_conf_set(conf, "max.in.flight.requests.per.connection", MAX_IN_FLIGHT, NULL, 0);
        }
    }

    rd_kafka_conf_set_dr_msg_cb(conf, txn_dr_cb);

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "Failed to create librdkafka producer: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
    }
    return rk;
}

// Log and free an rd_kafka_error_t; returns true if it was an error.
static bool check_txn_error(rd_kafka_error_t* error, const char* op) {
    if (!error) {
        return false;
    }
    fprintf(stderr, "%s failed: %s%s\n", op, rd_kafka_error_string(error),
            rd_kafka_error_is_fatal(error) ? " (fatal)" : "");
    rd_kafka_error_destroy(error);
    return true;
}

// Create a librdkafka consumer subscribed to SOURCE_TOPIC for the EOS pipeline.
// group.id is shared across producers (the coordinator divides partitions among
// them); auto-commit is disabled (offsets flow through the transaction);
// read_committed isolation is the canonical EOS setting.
static rd_kafka_t* create_consumer(const char* group_id) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    char errstr[512];

    rd_kafka_conf_set(conf, "bootstrap.servers", BOOTSTRAP_SERVERS, NULL, 0);
    if (sasl_enabled) {
        rd_kafka_conf_set(conf, "security.protocol", SECURITY_PROTOCOL, NULL, 0);
        rd_kafka_conf_set(conf, "sasl.mechanism", SASL_MECHANISM, NULL, 0);
        rd_kafka_conf_set(conf, "sasl.username", SASL_USERNAME, NULL, 0);
        rd_kafka_conf_set(conf, "sasl.password", SASL_PASSWORD, NULL, 0);
    }
    if (SSL_CA_LOCATION) {
        rd_kafka_conf_set(conf, "ssl.ca.location", SSL_CA_LOCATION, NULL, 0);
    }

    rd_kafka_conf_set(conf, "group.id", group_id, NULL, 0);
    // KIP-848 (Kaushik C:128): all consumers in the transactional perf harnesses
    // run on the new consumer group protocol, uniformly across Rust / C / Java /
    // Python. This requires a KIP-848-capable broker (Kafka 4.x) and a
    // KIP-848-capable client (librdkafka >= 2.5).
    rd_kafka_conf_set(conf, "group.protocol", "consumer", NULL, 0);
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", NULL, 0);
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", NULL, 0);
    rd_kafka_conf_set(conf, "isolation.level", "read_committed", NULL, 0);

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "Failed to create librdkafka consumer: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    // Route all queues to the consumer poll queue so rd_kafka_consumer_poll
    // serves everything.
    rd_kafka_poll_set_consumer(rk);

    rd_kafka_topic_partition_list_t* topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, SOURCE_TOPIC, RD_KAFKA_PARTITION_UA);
    rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, topics);
    rd_kafka_topic_partition_list_destroy(topics);
    if (err) {
        fprintf(stderr, "Failed to subscribe to '%s': %s\n", SOURCE_TOPIC, rd_kafka_err2str(err));
        rd_kafka_consumer_close(rk);
        rd_kafka_destroy(rk);
        return NULL;
    }
    return rk;
}

// ---------------------------------------------------------------------------
// Producer worker thread
// ---------------------------------------------------------------------------

typedef struct {
    int index;
    Message* messages;
    long first_message_time_ns; // shared measured-interval start (rate pacing)
    long num_messages;          // per-producer message target (0 = time-based)
} ProducerArgs;

// Rate limiter (per producer): sleep so this producer's rate stays under its
// share (LIMIT_RPS / NUM_TRANSACTIONAL_PRODUCERS). Mirrors producer_perf_test.c.
static void sleep_until_rate_limit_met(long records_sent, long first_message_time_ns,
                                       long per_producer_rps) {
    if (per_producer_rps <= 0) {
        return;
    }
    double limit_rpns = (double)per_producer_rps / 1e9;
    long elapsed = current_time_ns() - first_message_time_ns;
    double expected = elapsed * limit_rpns;
    if (records_sent > expected) {
        double sleep_time = (records_sent - expected) / (double)per_producer_rps;
        long usleep_time = (long)(sleep_time * 1000000);
        if (usleep_time > 0) usleep(usleep_time);
    }
}

static void* producer_thread_func(void* arg) {
    ProducerArgs* pa = (ProducerArgs*)arg;
    char txn_id[256];
    snprintf(txn_id, sizeof(txn_id), "%s-%d", TRANSACTIONAL_ID, pa->index);

    rd_kafka_t* rk = create_producer(txn_id);
    if (!rk) {
        return NULL;
    }

    if (check_txn_error(rd_kafka_init_transactions(rk, 60000), "init_transactions")) {
        rd_kafka_destroy(rk);
        return NULL;
    }

    long per_producer_rps = LIMIT_RPS > 0 ? (LIMIT_RPS / NUM_TRANSACTIONAL_PRODUCERS) : 0;
    if (LIMIT_RPS > 0 && per_producer_rps < 1) per_producer_rps = 1;
    long checkpoint_interval = per_producer_rps > 0 ? (per_producer_rps / 10 > 0 ? per_producer_rps / 10 : 1) : 0;
    long next_checkpoint = checkpoint_interval;

    long txn_index = 0;
    long records_sent = 0;
    long n = RECORDS_PER_TRANSACTION;
    TxnFuture** futures = malloc(sizeof(TxnFuture*) * n);
    long* produce_ts = malloc(sizeof(long) * n);

    bool continue_sending = pa->num_messages > 0 ? records_sent < pa->num_messages : !interrupted;
    while (continue_sending) {
        long begin_ns = current_time_ns();
        if (check_txn_error(rd_kafka_begin_transaction(rk), "begin_transaction")) {
            break;
        }

        // --- produce N records ---
        for (long r = 0; r < n; r++) {
            Message* message = &pa->messages[records_sent % GENERATED_MESSAGE_COUNT];
            TxnFuture* f = calloc(1, sizeof(TxnFuture));
            atomic_init(&f->done, false);
            futures[r] = f;
            produce_ts[r] = current_time_ns();

            rd_kafka_resp_err_t err;
            do {
                err = rd_kafka_producev(
                    rk, RD_KAFKA_V_TOPIC(TOPIC),
                    RD_KAFKA_V_KEY(KEY_SIZE > 0 ? message->key : NULL, KEY_SIZE),
                    RD_KAFKA_V_VALUE(message->value, VALUE_SIZE),
                    RD_KAFKA_V_OPAQUE(f),
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_BLOCK),
                    RD_KAFKA_V_END);
                if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL && !interrupted) {
                    // No dedicated poll thread: serve the queue to free space and
                    // fire pending delivery reports, then retry.
                    rd_kafka_poll(rk, 100);
                }
            } while (err == RD_KAFKA_RESP_ERR__QUEUE_FULL && !interrupted);

            records_sent++;
            if (per_producer_rps > 0 && records_sent >= next_checkpoint) {
                sleep_until_rate_limit_met(records_sent, pa->first_message_time_ns, per_producer_rps);
                next_checkpoint += checkpoint_interval;
            }
        }

        // --- commit or abort ---
        if (should_abort(txn_index, ABORT_RATE)) {
            // Aborted transactions STILL produced their N records above; on abort
            // they count toward neither throughput nor latency.
            check_txn_error(rd_kafka_abort_transaction(rk, 60000), "abort_transaction");
            // Per-transaction abort latency = begin -> abort completes (symmetric
            // with commit latency; deterministic-abort path only).
            long abort_ns = current_time_ns();
            long abort_latency = abort_ns - begin_ns;
            metrics_add_abort(&metrics, abort_latency);
            for (long r = 0; r < n; r++) {
                // Drain the delivery reports for the aborted batch before freeing
                // the opaque futures.
                while (!atomic_load_explicit(&futures[r]->done, memory_order_acquire)) {
                    rd_kafka_poll(rk, 100);
                }
                free(futures[r]);
            }
            pthread_mutex_lock(&stats_mutex);
            aborted_transactions++;
            aborted_records += n;
            total_abort_latency += abort_latency;
            if (abort_latency > max_abort_latency) max_abort_latency = abort_latency;
            long alat_ms = abort_latency / 1000000L;
            if (alat_ms < 0) alat_ms = 0;
            if (alat_ms > MAX_LATENCY_MS + 1) alat_ms = MAX_LATENCY_MS + 1;
            abort_latency_hist[alat_ms]++;
            pthread_mutex_unlock(&stats_mutex);
        } else {
            bool commit_failed =
                check_txn_error(rd_kafka_commit_transaction(rk, 60000), "commit_transaction");
            long commit_ns = current_time_ns();
            if (commit_failed) {
                for (long r = 0; r < n; r++) {
                    while (!atomic_load_explicit(&futures[r]->done, memory_order_acquire)) {
                        rd_kafka_poll(rk, 100);
                    }
                    free(futures[r]);
                }
                pthread_mutex_lock(&stats_mutex);
                aborted_transactions++;
                aborted_records += n;
                pthread_mutex_unlock(&stats_mutex);
            } else {
                // On return every record in the transaction is durable.
                long commit_latency = commit_ns - begin_ns;
                metrics_add_commit(&metrics, commit_latency);

                pthread_mutex_lock(&stats_mutex);
                committed_transactions++;
                total_commit_latency += commit_latency;
                if (commit_latency > max_commit_latency) max_commit_latency = commit_latency;
                long clat_ms = commit_latency / 1000000L;
                if (clat_ms < 0) clat_ms = 0;
                if (clat_ms > MAX_LATENCY_MS + 1) clat_ms = MAX_LATENCY_MS + 1;
                commit_latency_hist[clat_ms]++;
                pthread_mutex_unlock(&stats_mutex);

                for (long r = 0; r < n; r++) {
                    while (!atomic_load_explicit(&futures[r]->done, memory_order_acquire)) {
                        rd_kafka_poll(rk, 100);
                    }
                    bool ok;
                    if (futures[r]->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        fprintf(stderr, "Delivery failed: %s\n", rd_kafka_err2str(futures[r]->err));
                        ok = false;
                    } else {
                        ok = verify_rdkafka_message(futures[r]);
                    }

                    // Per-record latency = producev() -> commit completes.
                    long latency = commit_ns - produce_ts[r];
                    metrics_add_record(&metrics, latency);

                    pthread_mutex_lock(&stats_mutex);
                    completed_messages++;
                    if (ok) verified++;
                    if (latency > max_latency) max_latency = latency;
                    total_latency += latency;
                    long latency_ms = latency / 1000000L;
                    if (latency_ms < 0) latency_ms = 0;
                    if (latency_ms > MAX_LATENCY_MS + 1) latency_ms = MAX_LATENCY_MS + 1;
                    latency_hist[latency_ms]++;
                    pthread_mutex_unlock(&stats_mutex);

                    free(futures[r]);
                }
            }
        }

        txn_index++;

        // Duration bound for a time-based run (checked at a transaction boundary
        // so a transaction is never split).
        if (pa->num_messages <= 0) {
            long duration = current_time_ns() - pa->first_message_time_ns;
            if (duration > (long)TEST_DURATION_S * 1000000000L) {
                break;
            }
        }
        continue_sending = pa->num_messages > 0 ? records_sent < pa->num_messages : !interrupted;
    }

    free(futures);
    free(produce_ts);
    rd_kafka_destroy(rk);
    return NULL;
}

// ---------------------------------------------------------------------------
// EOS producer worker thread (TXN_MODE=eos): read-process-write pipeline.
// Owns both a transactional producer and a consumer subscribed to SOURCE_TOPIC.
// ---------------------------------------------------------------------------

// Accumulate the standard Kafka EOS commit offset (max consumed offset + 1) for
// (topic, partition) into `offsets`, upserting the entry.
static void eos_accumulate_offset(rd_kafka_topic_partition_list_t* offsets,
                                  const char* topic, int32_t partition, int64_t next) {
    rd_kafka_topic_partition_t* rktpar =
        rd_kafka_topic_partition_list_find(offsets, topic, partition);
    if (!rktpar) {
        rktpar = rd_kafka_topic_partition_list_add(offsets, topic, partition);
        rktpar->offset = next;
    } else if (next > rktpar->offset) {
        rktpar->offset = next;
    }
}

static void* eos_producer_thread_func(void* arg) {
    ProducerArgs* pa = (ProducerArgs*)arg;
    char txn_id[256];
    snprintf(txn_id, sizeof(txn_id), "%s-%d", TRANSACTIONAL_ID, pa->index);

    rd_kafka_t* producer = create_producer(txn_id);
    if (!producer) {
        return NULL;
    }
    if (check_txn_error(rd_kafka_init_transactions(producer, 60000), "init_transactions")) {
        rd_kafka_destroy(producer);
        return NULL;
    }
    rd_kafka_t* consumer = create_consumer(GROUP_ID);
    if (!consumer) {
        rd_kafka_destroy(producer);
        return NULL;
    }

    long per_producer_rps = LIMIT_RPS > 0 ? (LIMIT_RPS / NUM_TRANSACTIONAL_PRODUCERS) : 0;
    if (LIMIT_RPS > 0 && per_producer_rps < 1) per_producer_rps = 1;
    long checkpoint_interval = per_producer_rps > 0 ? (per_producer_rps / 10 > 0 ? per_producer_rps / 10 : 1) : 0;
    long next_checkpoint = checkpoint_interval;

    long txn_index = 0;
    long records_sent = 0;
    long n = RECORDS_PER_TRANSACTION;
    TxnFuture** futures = malloc(sizeof(TxnFuture*) * n);
    long* produce_ts = malloc(sizeof(long) * n);
    // Consecutive empty rounds, to warn (once) that the source is starving the
    // pipeline (Kaushik C:1612): the EOS ceiling is min(source-rate, txn-rate),
    // so an under-fed source shows up here rather than as a deadlock.
    long consecutive_empty_rounds = 0;
    bool starvation_warned = false;

    bool continue_sending = pa->num_messages > 0 ? records_sent < pa->num_messages : !interrupted;
    while (continue_sending) {
        // --- consume up to N records (one rd_kafka_consumer_poll per message;
        // stop on a timeout, i.e. no more data right now). `attempts` bounds the
        // loop so a stream of error events cannot spin forever. ---
        long batch = 0;
        long attempts = 0;
        long begin_ns = 0;
        rd_kafka_topic_partition_list_t* offsets = rd_kafka_topic_partition_list_new(1);
        while (batch < n && attempts < n && !interrupted) {
            attempts++;
            rd_kafka_message_t* msg = rd_kafka_consumer_poll(consumer, EOS_POLL_TIMEOUT_MS);
            if (!msg) {
                break; // poll timed out: no more source data available right now
            }
            if (msg->err) {
                // Non-data event (e.g. partition EOF / error): skip it.
                rd_kafka_message_destroy(msg);
                continue;
            }

            // The first real record of the batch opens the transaction.
            if (batch == 0) {
                begin_ns = current_time_ns();
                if (check_txn_error(rd_kafka_begin_transaction(producer), "begin_transaction")) {
                    rd_kafka_message_destroy(msg);
                    rd_kafka_topic_partition_list_destroy(offsets);
                    goto cleanup;
                }
            }

            // Transform (identity/echo) and produce to the destination topic.
            TxnFuture* f = calloc(1, sizeof(TxnFuture));
            atomic_init(&f->done, false);
            produce_ts[batch] = current_time_ns();
            rd_kafka_resp_err_t perr;
            do {
                perr = rd_kafka_producev(
                    producer, RD_KAFKA_V_TOPIC(TOPIC),
                    RD_KAFKA_V_KEY(msg->key, msg->key_len),
                    RD_KAFKA_V_VALUE(msg->payload, msg->len),
                    RD_KAFKA_V_OPAQUE(f),
                    // F_COPY: librdkafka copies the payload immediately, so the
                    // consumed message can be destroyed right after this call.
                    RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_BLOCK),
                    RD_KAFKA_V_END);
                if (perr == RD_KAFKA_RESP_ERR__QUEUE_FULL && !interrupted) {
                    rd_kafka_poll(producer, 100);
                }
            } while (perr == RD_KAFKA_RESP_ERR__QUEUE_FULL && !interrupted);
            futures[batch] = f;

            // Track the max consumed offset + 1 per (topic, partition).
            eos_accumulate_offset(offsets, rd_kafka_topic_name(msg->rkt),
                                  msg->partition, msg->offset + 1);

            rd_kafka_message_destroy(msg);
            records_sent++;
            batch++;

            if (per_producer_rps > 0 && records_sent >= next_checkpoint) {
                sleep_until_rate_limit_met(records_sent, pa->first_message_time_ns, per_producer_rps);
                next_checkpoint += checkpoint_interval;
            }
        }

        if (batch == 0) {
            // No source data this round; do not open a transaction. Warn once if
            // the source appears to be starving the pipeline (Kaushik C:1612),
            // then loop (bounded by the duration / message target below).
            rd_kafka_topic_partition_list_destroy(offsets);
            consecutive_empty_rounds++;
            if (consecutive_empty_rounds >= 10 && !starvation_warned) {
                fprintf(stderr,
                    "[WARN] EOS source starved: %ld consecutive empty polls from "
                    "the source topic — the source producer is not keeping up (the "
                    "EOS throughput ceiling is min(source-produce-rate, "
                    "txn-process-rate)); pre-populate SOURCE_TOPIC with a "
                    "high-throughput producer\n", consecutive_empty_rounds);
                starvation_warned = true;
            }
            if (pa->num_messages <= 0) {
                long duration = current_time_ns() - pa->first_message_time_ns;
                if (duration > (long)TEST_DURATION_S * 1000000000L) {
                    break;
                }
            }
            continue_sending = pa->num_messages > 0 ? records_sent < pa->num_messages : !interrupted;
            continue;
        }
        consecutive_empty_rounds = 0;

        // --- send the consumed offsets to the transaction (offset + 1 per
        // partition, with the consumer group metadata) ---
        rd_kafka_consumer_group_metadata_t* cgmd = rd_kafka_consumer_group_metadata(consumer);
        bool offsets_failed = check_txn_error(
            rd_kafka_send_offsets_to_transaction(producer, offsets, cgmd, 60000),
            "send_offsets_to_transaction");
        rd_kafka_consumer_group_metadata_destroy(cgmd);
        rd_kafka_topic_partition_list_destroy(offsets);

        // Decide the outcome: a failed send_offsets or the deterministic abort
        // rule both abort; otherwise commit.
        bool do_abort = offsets_failed || should_abort(txn_index, ABORT_RATE);
        if (do_abort) {
            // Aborted transactions produced+consumed their records; on abort they
            // count toward neither throughput nor latency, the consumed offsets
            // are NOT committed, and the consumer is NOT sought back (a benchmark
            // simplification; a real EOS app seeks to the committed position).
            check_txn_error(rd_kafka_abort_transaction(producer, 60000), "abort_transaction");
            long abort_ns = current_time_ns();
            for (long r = 0; r < batch; r++) {
                while (!atomic_load_explicit(&futures[r]->done, memory_order_acquire)) {
                    rd_kafka_poll(producer, 100);
                }
                free(futures[r]);
            }
            // Per-transaction abort latency = begin -> abort completes, recorded
            // only for a DETERMINISTIC abort (the abort-rate path), not an
            // error-driven abort (send_offsets failure) — symmetric with the
            // commit-latency series being committed-only.
            if (!offsets_failed) {
                long abort_latency = abort_ns - begin_ns;
                metrics_add_abort(&metrics, abort_latency);
                pthread_mutex_lock(&stats_mutex);
                total_abort_latency += abort_latency;
                if (abort_latency > max_abort_latency) max_abort_latency = abort_latency;
                long alat_ms = abort_latency / 1000000L;
                if (alat_ms < 0) alat_ms = 0;
                if (alat_ms > MAX_LATENCY_MS + 1) alat_ms = MAX_LATENCY_MS + 1;
                abort_latency_hist[alat_ms]++;
                pthread_mutex_unlock(&stats_mutex);
            }
            pthread_mutex_lock(&stats_mutex);
            aborted_transactions++;
            aborted_records += batch;
            pthread_mutex_unlock(&stats_mutex);
        } else {
            bool commit_failed =
                check_txn_error(rd_kafka_commit_transaction(producer, 60000), "commit_transaction");
            long commit_ns = current_time_ns();
            if (commit_failed) {
                for (long r = 0; r < batch; r++) {
                    while (!atomic_load_explicit(&futures[r]->done, memory_order_acquire)) {
                        rd_kafka_poll(producer, 100);
                    }
                    free(futures[r]);
                }
                pthread_mutex_lock(&stats_mutex);
                aborted_transactions++;
                aborted_records += batch;
                pthread_mutex_unlock(&stats_mutex);
            } else {
                // On return every produced record is durable and the consumed
                // offsets are committed atomically with the output.
                long commit_latency = commit_ns - begin_ns;
                metrics_add_commit(&metrics, commit_latency);

                pthread_mutex_lock(&stats_mutex);
                committed_transactions++;
                total_commit_latency += commit_latency;
                if (commit_latency > max_commit_latency) max_commit_latency = commit_latency;
                long clat_ms = commit_latency / 1000000L;
                if (clat_ms < 0) clat_ms = 0;
                if (clat_ms > MAX_LATENCY_MS + 1) clat_ms = MAX_LATENCY_MS + 1;
                commit_latency_hist[clat_ms]++;
                pthread_mutex_unlock(&stats_mutex);

                for (long r = 0; r < batch; r++) {
                    while (!atomic_load_explicit(&futures[r]->done, memory_order_acquire)) {
                        rd_kafka_poll(producer, 100);
                    }
                    bool ok;
                    if (futures[r]->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                        fprintf(stderr, "Delivery failed: %s\n", rd_kafka_err2str(futures[r]->err));
                        ok = false;
                    } else {
                        ok = verify_rdkafka_message(futures[r]);
                    }

                    // Per-record latency = producev() -> commit completes.
                    long latency = commit_ns - produce_ts[r];
                    metrics_add_record(&metrics, latency);

                    pthread_mutex_lock(&stats_mutex);
                    completed_messages++;
                    if (ok) verified++;
                    if (latency > max_latency) max_latency = latency;
                    total_latency += latency;
                    long latency_ms = latency / 1000000L;
                    if (latency_ms < 0) latency_ms = 0;
                    if (latency_ms > MAX_LATENCY_MS + 1) latency_ms = MAX_LATENCY_MS + 1;
                    latency_hist[latency_ms]++;
                    pthread_mutex_unlock(&stats_mutex);

                    free(futures[r]);
                }
            }
        }

        txn_index++;

        if (pa->num_messages <= 0) {
            long duration = current_time_ns() - pa->first_message_time_ns;
            if (duration > (long)TEST_DURATION_S * 1000000000L) {
                break;
            }
        }
        continue_sending = pa->num_messages > 0 ? records_sent < pa->num_messages : !interrupted;
    }

cleanup:
    free(futures);
    free(produce_ts);
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);
    rd_kafka_destroy(producer);
    return NULL;
}

// ---------------------------------------------------------------------------
// Topic (re)creation — librdkafka admin API. Identical to producer_perf_test.c.
// ---------------------------------------------------------------------------

static void recreate_topic(void) {
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", BOOTSTRAP_SERVERS, NULL, 0);
    if (SECURITY_PROTOCOL) rd_kafka_conf_set(conf, "security.protocol", SECURITY_PROTOCOL, NULL, 0);
    if (SASL_MECHANISM) rd_kafka_conf_set(conf, "sasl.mechanism", SASL_MECHANISM, NULL, 0);
    if (SASL_USERNAME) rd_kafka_conf_set(conf, "sasl.username", SASL_USERNAME, NULL, 0);
    if (SASL_PASSWORD) rd_kafka_conf_set(conf, "sasl.password", SASL_PASSWORD, NULL, 0);
    if (SSL_CA_LOCATION) rd_kafka_conf_set(conf, "ssl.ca.location", SSL_CA_LOCATION, NULL, 0);

    rd_kafka_t *admin = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!admin) {
        fprintf(stderr, "recreate_topic: failed to create admin client: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return;
    }
    rd_kafka_queue_t *queue = rd_kafka_queue_new(admin);

    printf(">>> CREATE_TOPIC: deleting topic '%s' (ignored if absent) ...\n", TOPIC);
    fflush(stdout);
    rd_kafka_DeleteTopic_t *del = rd_kafka_DeleteTopic_new(TOPIC);
    rd_kafka_AdminOptions_t *del_opts =
        rd_kafka_AdminOptions_new(admin, RD_KAFKA_ADMIN_OP_DELETETOPICS);
    rd_kafka_AdminOptions_set_request_timeout(del_opts, 30000, errstr, sizeof(errstr));
    rd_kafka_DeleteTopics(admin, &del, 1, del_opts, queue);
    rd_kafka_event_t *del_ev = rd_kafka_queue_poll(queue, 60000);
    if (del_ev) {
        const rd_kafka_DeleteTopics_result_t *res = rd_kafka_event_DeleteTopics_result(del_ev);
        size_t cnt = 0;
        const rd_kafka_topic_result_t **topics =
            res ? rd_kafka_DeleteTopics_result_topics(res, &cnt) : NULL;
        for (size_t i = 0; i < cnt; i++) {
            rd_kafka_resp_err_t e = rd_kafka_topic_result_error(topics[i]);
            if (e == RD_KAFKA_RESP_ERR_NO_ERROR)
                printf(">>> deleted '%s'\n", rd_kafka_topic_result_name(topics[i]));
            else if (e == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)
                printf(">>> '%s' did not exist (ok)\n", rd_kafka_topic_result_name(topics[i]));
            else
                fprintf(stderr, ">>> delete '%s' failed: %s\n",
                        rd_kafka_topic_result_name(topics[i]), rd_kafka_err2str(e));
        }
        rd_kafka_event_destroy(del_ev);
    } else {
        fprintf(stderr, ">>> delete timed out\n");
    }
    rd_kafka_AdminOptions_destroy(del_opts);
    rd_kafka_DeleteTopic_destroy(del);

    printf(">>> waiting 10s after delete ...\n");
    fflush(stdout);
    sleep(10);

    printf(">>> CREATE_TOPIC: creating topic '%s' (partitions=%d [-1=broker default], "
           "rf=broker default) ...\n", TOPIC, PARTITIONS);
    fflush(stdout);
    rd_kafka_NewTopic_t *nt = rd_kafka_NewTopic_new(TOPIC, PARTITIONS, -1, errstr, sizeof(errstr));
    if (!nt) {
        fprintf(stderr, "recreate_topic: NewTopic_new failed: %s\n", errstr);
    } else {
        rd_kafka_AdminOptions_t *cre_opts =
            rd_kafka_AdminOptions_new(admin, RD_KAFKA_ADMIN_OP_CREATETOPICS);
        rd_kafka_AdminOptions_set_request_timeout(cre_opts, 30000, errstr, sizeof(errstr));
        rd_kafka_CreateTopics(admin, &nt, 1, cre_opts, queue);
        rd_kafka_event_t *cre_ev = rd_kafka_queue_poll(queue, 60000);
        if (cre_ev) {
            const rd_kafka_CreateTopics_result_t *res = rd_kafka_event_CreateTopics_result(cre_ev);
            size_t cnt = 0;
            const rd_kafka_topic_result_t **topics =
                res ? rd_kafka_CreateTopics_result_topics(res, &cnt) : NULL;
            for (size_t i = 0; i < cnt; i++) {
                rd_kafka_resp_err_t e = rd_kafka_topic_result_error(topics[i]);
                if (e == RD_KAFKA_RESP_ERR_NO_ERROR)
                    printf(">>> created '%s'\n", rd_kafka_topic_result_name(topics[i]));
                else if (e == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS)
                    printf(">>> '%s' already exists (ok)\n", rd_kafka_topic_result_name(topics[i]));
                else
                    fprintf(stderr, ">>> create '%s' failed: %s\n",
                            rd_kafka_topic_result_name(topics[i]), rd_kafka_err2str(e));
            }
            rd_kafka_event_destroy(cre_ev);
        } else {
            fprintf(stderr, ">>> create timed out\n");
        }
        rd_kafka_AdminOptions_destroy(cre_opts);
        rd_kafka_NewTopic_destroy(nt);
    }

    printf(">>> waiting 10s after create ...\n");
    fflush(stdout);
    sleep(10);

    rd_kafka_queue_destroy(queue);
    rd_kafka_destroy(admin);
}

// ---------------------------------------------------------------------------
// Warmup — a few single-record transactions on one producer, to warm up
// connections. Mirrors the single-producer warmup of producer_perf_test.c.
// ---------------------------------------------------------------------------

static void run_warmup(Message* messages) {
    printf("Warming up for %d seconds ...\n", WARMUP_S);
    rd_kafka_t* rk = create_producer(TRANSACTIONAL_ID);
    if (!rk) {
        return;
    }
    if (check_txn_error(rd_kafka_init_transactions(rk, 60000), "warmup init_transactions")) {
        rd_kafka_destroy(rk);
        return;
    }
    long warmup_end_time = current_time_ns() + (long)WARMUP_S * 1000000000L;
    int i = 0;
    while (current_time_ns() < warmup_end_time) {
        if (check_txn_error(rd_kafka_begin_transaction(rk), "warmup begin_transaction")) break;
        Message* message = &messages[i % GENERATED_MESSAGE_COUNT];
        TxnFuture* f = calloc(1, sizeof(TxnFuture));
        atomic_init(&f->done, false);
        rd_kafka_producev(
            rk, RD_KAFKA_V_TOPIC(TOPIC),
            RD_KAFKA_V_KEY(KEY_SIZE > 0 ? message->key : NULL, KEY_SIZE),
            RD_KAFKA_V_VALUE(message->value, VALUE_SIZE),
            RD_KAFKA_V_OPAQUE(f),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_BLOCK),
            RD_KAFKA_V_END);
        check_txn_error(rd_kafka_commit_transaction(rk, 60000), "warmup commit_transaction");
        while (!atomic_load_explicit(&f->done, memory_order_acquire)) {
            rd_kafka_poll(rk, 100);
        }
        free(f);
        usleep(100000);
        i++;
    }
    rd_kafka_destroy(rk);
    printf("Warmup complete.\n");
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

static void run_test() {
    printf("Running C transactional producer performance test (librdkafka, TXN_MODE=%s)...\n", TXN_MODE);

    Message* messages = generate_message_data();
    metrics_init(&metrics);
    metrics_start_collecting(&metrics);

    if (WARMUP_S > 0) {
        run_warmup(messages);
    }

    long first_message_time_ns = current_time_ns();
    long before_ms = current_time_ms();
    metrics_set_measurement_start_ms(&metrics, before_ms);
    printf("Starting measured interval at %ld ms\n", before_ms);

    // Split the total message target evenly across producers.
    long per_producer_num_messages = NUM_MESSAGES > 0 ? NUM_MESSAGES / NUM_TRANSACTIONAL_PRODUCERS : 0;

    pthread_t* threads = malloc(sizeof(pthread_t) * NUM_TRANSACTIONAL_PRODUCERS);
    ProducerArgs* args = malloc(sizeof(ProducerArgs) * NUM_TRANSACTIONAL_PRODUCERS);
    void* (*worker)(void*) = EOS_MODE ? eos_producer_thread_func : producer_thread_func;
    for (long k = 0; k < NUM_TRANSACTIONAL_PRODUCERS; k++) {
        args[k].index = (int)k;
        args[k].messages = messages;
        args[k].first_message_time_ns = first_message_time_ns;
        args[k].num_messages = per_producer_num_messages;
        pthread_create(&threads[k], NULL, worker, &args[k]);
    }
    for (long k = 0; k < NUM_TRANSACTIONAL_PRODUCERS; k++) {
        pthread_join(threads[k], NULL);
    }
    free(threads);
    free(args);

    long after_ns = current_time_ns();
    long after_ms = current_time_ms();
    metrics_set_measurement_end_ms(&metrics, after_ms);

    if (verified != completed_messages) {
        fprintf(stderr, "Verified messages %ld does not match completed messages %ld\n",
            verified, completed_messages);
    }

    long total_time_ns = after_ns - first_message_time_ns;
    double total_time_s = total_time_ns / 1e9;
    double average_cpu, average_rss;
    double message_rate = total_time_s > 0 ? completed_messages / total_time_s : 0.0;
    double txn_rate = total_time_s > 0 ? committed_transactions / total_time_s : 0.0;
    metrics_external_metrics_aggregations(&metrics, &average_cpu, &average_rss);

    printf("End time: %ld ms\n", after_ms);
    printf("Duration: %.2f ms\n", (double)total_time_ns / 1e6);
    printf("Average CPU: %.2f %%\n", average_cpu);
    printf("Average RSS: %.2f KiB\n", average_rss / 1024.0);
    printf("CPU efficiency: %.2f msg/(s * 1%% CPU)\n",
        message_rate / (average_cpu > 0.0 ? average_cpu : 1.0));
    printf("Memory efficiency: %.2f msg/(s * KB RSS)\n",
        message_rate / (average_rss > 0.0 ? average_rss / 1024.0 : 1.0));
    printf("Committed transactions: %ld\n", committed_transactions);
    printf("Aborted transactions: %ld\n", aborted_transactions);
    printf("Aborted records: %ld\n", aborted_records);
    printf("Committed records: %ld\n", completed_messages);
    printf("Average time: %.2f ms\n",
        completed_messages > 0 ? (double)total_time_ns / completed_messages / 1e6 : 0.0);
    printf("Average rate msg/s: %.2f msg/s\n", message_rate);
    printf("Average rate MiB/s: %.2f MiB/s\n",
        total_time_s > 0 ? (completed_messages * MESSAGE_SIZE / (1024.0 * 1024.0)) / total_time_s : 0.0);
    printf("Transactions/s: %.2f\n", txn_rate);
    printf("Average per-record latency: %.2f ms\n",
        completed_messages > 0 ? (double)total_latency / completed_messages / 1e6 : 0.0);
    printf("Max per-record latency: %.2f ms\n", (double)max_latency / 1e6);
    printf("Average commit latency: %.2f ms\n",
        committed_transactions > 0 ? (double)total_commit_latency / committed_transactions / 1e6 : 0.0);
    long abort_lat_count = 0;
    for (size_t i = 0; i < MAX_LATENCY_MS + 2; i++) {
        abort_lat_count += abort_latency_hist[i];
    }
    printf("Average abort latency: %.2f ms\n",
        abort_lat_count > 0 ? (double)total_abort_latency / abort_lat_count / 1e6 : 0.0);

    long p50_ms = percentile_from_hist(latency_hist, MAX_LATENCY_MS + 2, 0.50);
    long p90_ms = percentile_from_hist(latency_hist, MAX_LATENCY_MS + 2, 0.90);
    long p95_ms = percentile_from_hist(latency_hist, MAX_LATENCY_MS + 2, 0.95);
    long p99_ms = percentile_from_hist(latency_hist, MAX_LATENCY_MS + 2, 0.99);
    long p999_ms = percentile_from_hist(latency_hist, MAX_LATENCY_MS + 2, 0.999);
    long min_latency_ms = 0;
    for (size_t i = 0; i < MAX_LATENCY_MS + 2; i++) {
        if (latency_hist[i] > 0) { min_latency_ms = (long)i; break; }
    }
    long cp50 = percentile_from_hist(commit_latency_hist, MAX_LATENCY_MS + 2, 0.50);
    long cp90 = percentile_from_hist(commit_latency_hist, MAX_LATENCY_MS + 2, 0.90);
    long cp95 = percentile_from_hist(commit_latency_hist, MAX_LATENCY_MS + 2, 0.95);
    long cp99 = percentile_from_hist(commit_latency_hist, MAX_LATENCY_MS + 2, 0.99);
    long cp999 = percentile_from_hist(commit_latency_hist, MAX_LATENCY_MS + 2, 0.999);
    long cmin_ms = 0;
    for (size_t i = 0; i < MAX_LATENCY_MS + 2; i++) {
        if (commit_latency_hist[i] > 0) { cmin_ms = (long)i; break; }
    }
    long ap50 = percentile_from_hist(abort_latency_hist, MAX_LATENCY_MS + 2, 0.50);
    long ap90 = percentile_from_hist(abort_latency_hist, MAX_LATENCY_MS + 2, 0.90);
    long ap95 = percentile_from_hist(abort_latency_hist, MAX_LATENCY_MS + 2, 0.95);
    long ap99 = percentile_from_hist(abort_latency_hist, MAX_LATENCY_MS + 2, 0.99);
    long ap999 = percentile_from_hist(abort_latency_hist, MAX_LATENCY_MS + 2, 0.999);
    long amin_ms = 0;
    for (size_t i = 0; i < MAX_LATENCY_MS + 2; i++) {
        if (abort_latency_hist[i] > 0) { amin_ms = (long)i; break; }
    }

    printf("p99 per-record latency: %ld ms\n", p99_ms);
    // Per-record latency budget — only asserted when set (P99_LIMIT_MS=0
    // disables it; per-record latency in produce mode is dominated by
    // transaction-fill time, so it defaults off). Mirrors the sibling tests.
    if (P99_LIMIT_MS > 0 && p99_ms > P99_LIMIT_MS) {
        fprintf(stderr, "p99 per-record latency %ld ms exceeds %ld ms budget\n", p99_ms, P99_LIMIT_MS);
        latency_budget_exceeded = true;
    }

    FILE *results_fp = fopen(RESULTS_FILE, "w");
    if (results_fp != NULL) {
        fprintf(results_fp,
            "{\n"
            "  \"test\": \"producer\",\n"
            "  \"client\": \"librdkafka-txn\",\n"
            "  \"topic\": \"%s\",\n"
            "  \"messages_measured\": %ld,\n"
            "  \"duration_s\": %.2f,\n"
            "  \"throughput_msg_s\": %.2f,\n"
            "  \"throughput_mib_s\": %.2f,\n"
            "  \"committed_transactions\": %ld,\n"
            "  \"aborted_transactions\": %ld,\n"
            "  \"aborted_records\": %ld,\n"
            "  \"transactions_per_s\": %.2f,\n"
            "  \"latency_ms\": {\"min\": %ld, \"avg\": %.2f, \"p50\": %ld, "
            "\"p90\": %ld, \"p95\": %ld, \"p99\": %ld, \"p999\": %ld, "
            "\"max\": %.2f},\n"
            "  \"commit_latency_ms\": {\"min\": %ld, \"avg\": %.2f, \"p50\": %ld, "
            "\"p90\": %ld, \"p95\": %ld, \"p99\": %ld, \"p999\": %ld, "
            "\"max\": %.2f},\n"
            "  \"abort_latency_ms\": {\"min\": %ld, \"avg\": %.2f, \"p50\": %ld, "
            "\"p90\": %ld, \"p95\": %ld, \"p99\": %ld, \"p999\": %ld, "
            "\"max\": %.2f},\n"
            "  \"cpu_avg_pct\": %.2f,\n"
            "  \"rss_avg_kib\": %.2f\n"
            "}\n",
            TOPIC,
            completed_messages,
            total_time_s,
            message_rate,
            total_time_s > 0 ? (completed_messages * MESSAGE_SIZE / (1024.0 * 1024.0)) / total_time_s : 0.0,
            committed_transactions,
            aborted_transactions,
            aborted_records,
            txn_rate,
            min_latency_ms,
            completed_messages > 0 ? (double)total_latency / completed_messages / 1e6 : 0.0,
            p50_ms, p90_ms, p95_ms, p99_ms, p999_ms,
            (double)max_latency / 1e6,
            cmin_ms,
            committed_transactions > 0 ? (double)total_commit_latency / committed_transactions / 1e6 : 0.0,
            cp50, cp90, cp95, cp99, cp999,
            (double)max_commit_latency / 1e6,
            amin_ms,
            abort_lat_count > 0 ? (double)total_abort_latency / abort_lat_count / 1e6 : 0.0,
            ap50, ap90, ap95, ap99, ap999,
            (double)max_abort_latency / 1e6,
            average_cpu,
            average_rss / 1024.0);
        fclose(results_fp);
        printf("Results summary written to: %s\n", RESULTS_FILE);
    } else {
        fprintf(stderr, "Failed to write %s\n", RESULTS_FILE);
    }

    free_message_data(messages);
}

int main(int argc, char** argv) {
    (void)argc; (void)argv;
    srand(time(NULL));
    double last_cpu = 0.0;
    double last_rss = 0.0;

    const char* client_version_env = getenv("CLIENT_VERSION");
    if (client_version_env != NULL) {
        CLIENT_VERSION = atoi(client_version_env);
    }
    // D1: the Rust C-FFI (v3) has no transactional API. This harness is
    // librdkafka-only. Refuse v3 loudly rather than run a non-transactional path.
    if (CLIENT_VERSION == 3) {
        fprintf(stderr,
            "CLIENT_VERSION=3 (Rust client C-FFI) is not supported by the transactional "
            "producer performance test: the Rust C-FFI exposes no transactional API "
            "(init/begin/commit/abort transactions). This harness supports the "
            "librdkafka backend only (CLIENT_VERSION=2, the default). Aborting.\n");
        return 2;
    }
    if (CLIENT_VERSION != 2) {
        fprintf(stderr, "Unsupported CLIENT_VERSION=%d (only 2/librdkafka is supported). Aborting.\n",
                CLIENT_VERSION);
        return 2;
    }

    const char* txn_mode_env = getenv("TXN_MODE");
    if (txn_mode_env != NULL) {
        TXN_MODE = txn_mode_env;
    }
    // Two modes: `produce` (Phase 1) and `eos` (Phase 2). `eos` requires a
    // SOURCE_TOPIC; any other value is rejected.
    if (strcmp(TXN_MODE, "eos") == 0) {
        EOS_MODE = true;
        SOURCE_TOPIC = getenv("SOURCE_TOPIC");
        if (SOURCE_TOPIC == NULL || SOURCE_TOPIC[0] == '\0') {
            fprintf(stderr,
                "TXN_MODE=eos requires SOURCE_TOPIC (the input topic to consume "
                "from). Aborting.\n");
            return 2;
        }
    } else if (strcmp(TXN_MODE, "produce") != 0) {
        fprintf(stderr, "Unknown TXN_MODE '%s' (expected 'produce' or 'eos'). Aborting.\n", TXN_MODE);
        return 2;
    }

    const char* do_verify_env = getenv("DO_VERIFY");
    if (do_verify_env != NULL && strcmp(do_verify_env, "False") == 0) {
        DO_VERIFY = false;
        printf("Verification disabled\n");
    }

    const char* warmup_seconds_env = getenv("WARMUP_SECONDS");
    if (warmup_seconds_env != NULL) WARMUP_S = atoi(warmup_seconds_env);
    const char* test_duration_env = getenv("TEST_DURATION_SECONDS");
    if (test_duration_env != NULL) TEST_DURATION_S = atoi(test_duration_env);
    const char* p99_limit_ms_env = getenv("P99_LIMIT_MS");
    if (p99_limit_ms_env != NULL) P99_LIMIT_MS = atol(p99_limit_ms_env);
    const char* results_file_env = getenv("RESULTS_FILE");
    if (results_file_env != NULL) RESULTS_FILE = results_file_env;
    const char* metrics_file_env = getenv("METRICS_FILE");
    if (metrics_file_env != NULL) METRICS_FILE = metrics_file_env;
    const char* bootstrap_servers_env = getenv("BOOTSTRAP_SERVERS");
    if (bootstrap_servers_env != NULL) BOOTSTRAP_SERVERS = bootstrap_servers_env;

    SECURITY_PROTOCOL = getenv("SECURITY_PROTOCOL");
    SASL_MECHANISM = getenv("SASL_MECHANISM");
    SASL_USERNAME = getenv("SASL_USERNAME");
    SASL_PASSWORD = getenv("SASL_PASSWORD");
    SSL_CA_LOCATION = getenv("SSL_CA_LOCATION");

    const char *create_topic_env = getenv("CREATE_TOPIC");
    if (create_topic_env != NULL &&
        (strcmp(create_topic_env, "False") == 0 || strcmp(create_topic_env, "0") == 0)) {
        CREATE_TOPIC = false;
    }
    const char *partitions_env = getenv("PARTITIONS");
    if (partitions_env != NULL) PARTITIONS = atoi(partitions_env);
    const char *max_in_flight_env = getenv("MAX_IN_FLIGHT");
    if (max_in_flight_env != NULL) {
        // Transactions force enable.idempotence=true; librdkafka rejects
        // rd_kafka_new when a user-modified max.in.flight exceeds 5
        // (RD_KAFKA_IDEMP_MAX_INFLIGHT). Clamp a >5 value to 5 with a clear
        // message rather than causing a silent zero-record run.
        if (atoi(max_in_flight_env) > 5) {
            fprintf(stderr,
                    "MAX_IN_FLIGHT=%s exceeds 5, which librdkafka rejects under "
                    "enable.idempotence=true (forced for transactions); "
                    "clamping to 5\n",
                    max_in_flight_env);
            MAX_IN_FLIGHT = "5";
        } else {
            MAX_IN_FLIGHT = max_in_flight_env;
        }
    }
    const char *compression_type_env = getenv("COMPRESSION_TYPE");
    if (compression_type_env != NULL) COMPRESSION_TYPE = compression_type_env;
    const char *buffer_memory_env = getenv("BUFFER_MEMORY");
    if (buffer_memory_env != NULL) BUFFER_MEMORY_MB = atoi(buffer_memory_env);
    const char *batch_size_env = getenv("BATCH_SIZE");
    if (batch_size_env != NULL) BATCH_SIZE_KB = atoi(batch_size_env);
    const char *max_request_size_env = getenv("MAX_REQUEST_SIZE");
    if (max_request_size_env != NULL) MAX_REQUEST_SIZE_KB = atoi(max_request_size_env);
    const char *linger_ms_env = getenv("LINGER_MS");
    if (linger_ms_env != NULL) LINGER_MS = linger_ms_env;

    const char *use_defaults_env = getenv("USE_DEFAULTS");
    if (use_defaults_env != NULL && strcmp(use_defaults_env, "True") == 0) {
        USE_DEFAULTS = true;
        printf("USE_DEFAULTS: true (client defaults; tuning knobs omitted)\n");
    }

    const char *key_size_env = getenv("KEY_SIZE");
    if (key_size_env != NULL) KEY_SIZE = atoi(key_size_env);
    const char *value_size_env = getenv("VALUE_SIZE");
    if (value_size_env != NULL) VALUE_SIZE = atoi(value_size_env);

    // Transaction knobs.
    const char *rpt_env = getenv("RECORDS_PER_TRANSACTION");
    if (rpt_env != NULL) RECORDS_PER_TRANSACTION = atol(rpt_env);
    if (RECORDS_PER_TRANSACTION < 1) RECORDS_PER_TRANSACTION = 1;
    const char *abort_rate_env = getenv("ABORT_RATE");
    if (abort_rate_env != NULL) ABORT_RATE = atof(abort_rate_env);
    if (ABORT_RATE < 0.0) ABORT_RATE = 0.0;
    if (ABORT_RATE > 1.0) ABORT_RATE = 1.0;
    const char *num_producers_env = getenv("NUM_TRANSACTIONAL_PRODUCERS");
    if (num_producers_env != NULL) NUM_TRANSACTIONAL_PRODUCERS = atol(num_producers_env);
    if (NUM_TRANSACTIONAL_PRODUCERS < 1) NUM_TRANSACTIONAL_PRODUCERS = 1;
    const char *txn_id_env = getenv("TRANSACTIONAL_ID");
    if (txn_id_env != NULL) TRANSACTIONAL_ID = txn_id_env;

    // EOS consumer group id: default "<TRANSACTIONAL_ID>-eos-consumer",
    // overridable via GROUP_ID. Shared across all producers' consumers so the
    // coordinator divides the source partitions among them.
    const char *group_id_env = getenv("GROUP_ID");
    if (group_id_env != NULL && group_id_env[0] != '\0') {
        GROUP_ID = group_id_env;
    } else {
        static char group_id_buf[512];
        snprintf(group_id_buf, sizeof(group_id_buf), "%s-eos-consumer", TRANSACTIONAL_ID);
        GROUP_ID = group_id_buf;
    }

    MESSAGE_SIZE = KEY_SIZE + VALUE_SIZE;
    RANDOM_KEY = (int)(KEY_SIZE * RANDOMNESS);
    RANDOM_VALUE = (int)(VALUE_SIZE * RANDOMNESS);

    if (SECURITY_PROTOCOL != NULL &&
        SASL_MECHANISM != NULL &&
        SASL_USERNAME != NULL &&
        SASL_PASSWORD != NULL) {
        sasl_enabled = true;
        printf("SASL authentication enabled\n");
    }

    // Byte-size derivations (librdkafka path — same rules as producer_perf_test.c).
    long max_bytes = BUFFER_MEMORY_MB > 0 ? BUFFER_MEMORY_MB * 1024L * 1024L : BUFFER_MEMORY_MB;
    buffer_memory_bytes = max_bytes > INT_MAX ? INT_MAX : max_bytes;
    batch_size_bytes = BATCH_SIZE_KB < 1024 ? 1024 * 1024 : BATCH_SIZE_KB * 1024L;
    if (MAX_REQUEST_SIZE_KB > 0) {
        max_request_size_bytes = MAX_REQUEST_SIZE_KB * 1024L;
    } else {
        max_request_size_bytes = batch_size_bytes * 64L;
    }
    if (max_request_size_bytes > 8 * 1024 * 1024) {
        max_request_size_bytes = 8 * 1024 * 1024;
    }

    printf("Key size: %ld bytes\n", KEY_SIZE);
    printf("Value size: %ld bytes\n", VALUE_SIZE);
    printf("Bootstrap servers: %s\n", BOOTSTRAP_SERVERS);
    printf("Records per transaction: %ld\n", RECORDS_PER_TRANSACTION);
    printf("Abort rate: %.4f\n", ABORT_RATE);
    printf("Transactional producers: %ld\n", NUM_TRANSACTIONAL_PRODUCERS);
    printf("Transactional id base: %s\n", TRANSACTIONAL_ID);
    printf("Enable idempotence: true (forced for transactions)\n");
    printf("Max in flight requests: %s\n",
           MAX_IN_FLIGHT != NULL ? MAX_IN_FLIGHT : "5 (idempotent default)");
    printf("Compression type: %s\n", COMPRESSION_TYPE);
    if (buffer_memory_bytes < 0)
        printf("Buffer memory: default\n");
    else
        printf("Buffer memory: %ld MB\n", buffer_memory_bytes / (1024L * 1024L));
    printf("Batch size: %" PRId64 " KB\n", batch_size_bytes / 1024);
    printf("Max request size: %" PRId64 " KB\n", max_request_size_bytes / 1024);
    printf("Linger ms: %s\n", LINGER_MS);
    printf("Using librdkafka transactional producer (v2)\n");

    const char *topic_name_env = getenv("TOPIC_NAME");
    if (topic_name_env != NULL) TOPIC = topic_name_env;
    printf("Using topic name (destination): %s\n", TOPIC);
    if (EOS_MODE) {
        printf("TXN_MODE: eos (read-process-write)\n");
        printf("Source topic: %s\n", SOURCE_TOPIC);
        printf("Consumer group id: %s\n", GROUP_ID);
    }

    if (CREATE_TOPIC) {
        // Only the destination topic is (re)created; the EOS source topic is the
        // user's responsibility and is assumed pre-populated.
        recreate_topic();
    }

    const char *num_messages_env = getenv("NUM_MESSAGES");
    if (num_messages_env != NULL) NUM_MESSAGES = atol(num_messages_env);

    const char* limit_rps_env = getenv("LIMIT_RPS");
    if (limit_rps_env != NULL) {
        LIMIT_RPS = atol(limit_rps_env);
        NUM_MESSAGES = LIMIT_RPS * TEST_DURATION_S;
        printf("Producing %ld messages at %ld msg/s (total)\n", NUM_MESSAGES, LIMIT_RPS);
    } else {
        if (NUM_MESSAGES == 0)
            printf("Producing at max rate for %d seconds\n", TEST_DURATION_S);
        else
            printf("Producing %ld messages at max rate\n", NUM_MESSAGES);
    }

    run_test();

    printf("Waiting for final metrics collection...\n");
    fflush(stdout);
    sleep(10);
    metrics_external_metrics_last_values(&metrics, &last_cpu, &last_rss);
    printf("Final CPU: %.2f %%\n", last_cpu);
    printf("Final RSS: %.2f KiB\n", last_rss / 1024.0);
    metrics_stop_collecting(&metrics);
    printf("Done\n");

    return latency_budget_exceeded ? 1 : 0;
}
