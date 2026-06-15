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

// Producer performance test for the C bindings.
//
// Ported from the librdkafka/GraalVM comparison test; the GraalVM
// (native-java) backend has been removed and replaced by the Confluent Kafka
// Rust client's C FFI (`confluent_kafka.h`). Two backends remain, selected via
// the CLIENT_VERSION environment variable:
//
//   * CLIENT_VERSION=2 -> librdkafka (reference baseline)
//   * CLIENT_VERSION=3 -> Rust client C bindings (default; client under test)
//
// It mirrors the configuration contract, message shape, and metrics-file
// schema (metrics.jsonl) of the Rust producer performance test
// (tests/integration/producer_perf_test.rs) so results are comparable across
// implementations. Configure via the same environment variables
// (BOOTSTRAP_SERVERS, NUM_MESSAGES, LIMIT_RPS, KEY_SIZE, VALUE_SIZE,
// BATCH_SIZE, MAX_REQUEST_SIZE, BUFFER_MEMORY, LINGER_MS, MAX_IN_FLIGHT,
// COMPRESSION_TYPE, ENABLE_IDEMPOTENCE, WARMUP_SECONDS, TEST_DURATION_SECONDS,
// DO_VERIFY, P99_LIMIT_MS, TOPIC_NAME, SECURITY_PROTOCOL, SASL_MECHANISM,
// SASL_USERNAME, SASL_PASSWORD, SSL_CA_LOCATION). P99_LIMIT_MS (ms) asserts a
// per-message p99 latency budget when > 0 (0 = off); a breach makes the test
// exit non-zero. Needs a reachable broker; opt-in (not run under ctest).

#include <confluent_kafka.h>
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
#include <math.h>

#define GENERATED_MESSAGE_COUNT 10000
#define RANDOMNESS 0.5f
#define DEFAULT_KEY_SIZE 0
#define DEFAULT_VALUE_SIZE 2048
// Upper bound (ms) of the latency histogram used for the p99 assertion.
#define MAX_LATENCY_MS 10000

static long KEY_SIZE = DEFAULT_KEY_SIZE;
static long VALUE_SIZE = DEFAULT_VALUE_SIZE;
static long MESSAGE_SIZE = DEFAULT_KEY_SIZE + DEFAULT_VALUE_SIZE;
static int RANDOM_KEY = (int)(DEFAULT_KEY_SIZE * RANDOMNESS);
static int RANDOM_VALUE = (int)(DEFAULT_VALUE_SIZE * RANDOMNESS);
static long NUM_MESSAGES = 0;
static long LIMIT_RPS = 0;
static long TOTAL_SIZE;
static double TOTAL_SIZE_MB;
static const char* TOPIC = "test-topic";
static bool DO_VERIFY = true;
static int CLIENT_VERSION = 3;
static int WARMUP_S = 120;
static int TEST_DURATION_S = 600;
static const char *BOOTSTRAP_SERVERS = "localhost:9092";
static const char *SECURITY_PROTOCOL = NULL;
static const char *SASL_MECHANISM = NULL;
static const char *SASL_USERNAME = NULL;
static const char *SASL_PASSWORD = NULL;
static const char *SSL_CA_LOCATION = NULL;
static const char *ENABLE_IDEMPOTENCE = "false";
static int BATCH_SIZE_KB = 1024;
static int MAX_REQUEST_SIZE_KB = -1;
static const char *MAX_IN_FLIGHT = "1000";
static int BUFFER_MEMORY_MB = -1;
static const char *COMPRESSION_TYPE = "none";
static const char *LINGER_MS = "5";
static bool sasl_enabled = false;
static int64_t buffer_memory_bytes;
int64_t batch_size_bytes;
int64_t max_request_size_bytes;

static long after_ns = 0;
static long total_latency = 0;
static long max_latency = 0;
static long completed_messages = 0;
static long first_message_time = 0;
static long verified = 0;
// Per-message p99 latency budget (ms). 0 disables the assertion.
static long P99_LIMIT_MS = 0;
static bool latency_budget_exceeded = false;
// Latency histogram (ms resolution) for the p99 computation, mirroring the Rust
// test. Written only by the single record-completed task and read after that
// task is joined, so no locking is needed.
static long latency_hist[MAX_LATENCY_MS + 2];
typedef void * test_Producer_t;
typedef void * test_Future_t;
static pthread_t record_thread;
static bool record_running = false;
static pthread_t poll_thread;
static bool poll_running = false;
static bool interrupted = false;

typedef struct {
    uint8_t* key;
    uint8_t* value;
} Message;

typedef struct {
    int64_t offset;
    int32_t partition;
    char* topic;
    int64_t timestamp;
    rd_kafka_resp_err_t err;
} V2RecordMetadata;

typedef struct {
    test_Future_t future;
    long start_time;
} ProducedMessage;

typedef struct {
    void** items;
    size_t head;
    size_t tail;
    size_t capacity;
    size_t cnt;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    bool closed;
} Queue;

// Metrics structures
typedef struct {
    double total;
    long count;
    double max;
} Bucket;

typedef struct {
    FILE* file;
    Bucket latency;
    // Per-window latency histogram (ms resolution) for the p50/p90/p99/p999
    // percentiles emitted each window; reset on every rollover.
    long latency_pct_hist[MAX_LATENCY_MS + 2];
    Bucket bytes;
    Bucket messages;
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

static test_Future_t (*test_send) (test_Producer_t producer,
    Message *message);
static bool (*test_verify_future) (test_Future_t future);

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

static bool verify_record_metadata(kafka_producer_RecordMetadata_t* metadata) {
    if (!DO_VERIFY) {
        verified++;
        return true;
    }

    if (!metadata) {
        fprintf(stderr, "RecordMetadata is NULL\n");
        return false;
    }

    int64_t offset = kafka_producer_RecordMetadata_offset(metadata);
    int32_t partition = kafka_producer_RecordMetadata_partition(metadata);
    const char* topic = kafka_producer_RecordMetadata_topic(metadata);
    int64_t timestamp = kafka_producer_RecordMetadata_timestamp(metadata);

    if (offset < 0) {
        fprintf(stderr, "Invalid offset: %" PRId64 "\n", offset);
        return false;
    }

    if (partition < 0) {
        fprintf(stderr, "Invalid partition: %d\n", partition);
        return false;
    }

    if (!topic || strcmp(topic, TOPIC) != 0) {
        fprintf(stderr, "Topic mismatch: expected '%s', got '%s'\n",
                TOPIC, topic ? topic : "NULL");
        return false;
    }

    if (timestamp < 0) {
        fprintf(stderr, "Invalid timestamp: %" PRId64 "\n", timestamp);
        return false;
    }

    verified++;
    return true;
}

// Rust client C bindings backend (v3): KafkaProducer over confluent_kafka.h.
// The producer owns an internal async runtime, so no polling thread is needed;
// send returns a FutureRecordMetadata whose _get blocks for the result.
static test_Future_t test_v3_send(test_Producer_t producer, Message *message) {
    kafka_producer_Producer_t *p = (kafka_producer_Producer_t *)producer;
    kafka_common_KafkaError_t *error = NULL;
    kafka_producer_FutureRecordMetadata_t *future = NULL;
    const uint8_t *key = KEY_SIZE > 0 ? message->key : NULL;
    int32_t key_len = KEY_SIZE > 0 ? (int32_t)KEY_SIZE : -1;
    do {
        error = NULL;
        future = kafka_producer_Producer_send(
            p, TOPIC, -1, -1,
            key, key_len,
            message->value, (int32_t)VALUE_SIZE,
            &error);

        if (error) {
            const char* msg = kafka_common_KafkaError_message(error);
            fprintf(stderr, "Send error: %s\n", msg ? msg : "Unknown error");
            kafka_common_KafkaError_destroy(error);
        }
    } while (error && !interrupted);
    return future;
}

static bool test_v3_verify_future(test_Future_t future) {
    kafka_producer_FutureRecordMetadata_t *f =
        (kafka_producer_FutureRecordMetadata_t *)future;
    kafka_common_KafkaError_t* error = NULL;
    kafka_producer_RecordMetadata_t* metadata =
        kafka_producer_FutureRecordMetadata_get(f, &error);

    if (error) {
        const char* msg = kafka_common_KafkaError_message(error);
        fprintf(stderr, "Send error: %s\n", msg ? msg : "Unknown error");
        kafka_common_KafkaError_destroy(error);
        kafka_producer_FutureRecordMetadata_destroy(f);
        return false;
    }

    // Verify the RecordMetadata
    if (!verify_record_metadata(metadata)) {
        fprintf(stderr, "RecordMetadata verification failed\n");
        kafka_producer_RecordMetadata_destroy(metadata);
        kafka_producer_FutureRecordMetadata_destroy(f);
        return false;
    }

    kafka_producer_RecordMetadata_destroy(metadata);
    kafka_producer_FutureRecordMetadata_destroy(f);
    return true;
}

static bool verify_rdkafka_message(V2RecordMetadata* rkmetadata) {
    if (!DO_VERIFY) {
        verified++;
        return true;
    }

    if (!rkmetadata) {
        fprintf(stderr, "rkmetadata is NULL\n");
        return false;
    }

    int64_t offset = rkmetadata->offset;
    int32_t partition = rkmetadata->partition;
    const char* topic = rkmetadata->topic;
    int64_t timestamp = rkmetadata->timestamp;

    if (offset < 0) {
        fprintf(stderr, "Invalid offset: %" PRId64 "\n", offset);
        return false;
    }

    if (partition < 0) {
        fprintf(stderr, "Invalid partition: %d\n", partition);
        return false;
    }

    if (!topic || strcmp(topic, TOPIC) != 0) {
        fprintf(stderr, "Topic mismatch: expected '%s', got '%s'\n",
                TOPIC, topic ? topic : "NULL");
        return false;
    }

    if (timestamp < 0) {
        fprintf(stderr, "Invalid timestamp: %" PRId64 "\n", timestamp);
        return false;
    }

    verified++;
    return true;
}

static void queue_init(Queue* q, size_t capacity);
static bool queue_push(Queue* q, void* item);
static void queue_destroy(Queue* q);
static bool queue_pop(Queue* q, void ** item, int timeout_ms);

static void test_v2_dr(rd_kafka_t *rk,
                      const rd_kafka_message_t *rkmessage,
                      void *opaque) {
    Queue *future = rkmessage->_private;
    V2RecordMetadata *rkmetadata = malloc(sizeof(V2RecordMetadata));
    rkmetadata->offset = rkmessage->offset;
    rkmetadata->partition = rkmessage->partition;
    rkmetadata->topic = strdup(rd_kafka_topic_name(rkmessage->rkt));
    rkmetadata->timestamp = rd_kafka_message_timestamp(rkmessage, NULL);
    rkmetadata->err = rkmessage->err;
    queue_push(future, (void *)rkmetadata);
}

static test_Future_t test_v2_send(test_Producer_t producer,  Message *message) {
    rd_kafka_t *rk = producer;
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
    Queue *future = calloc(1, sizeof(Queue));
    queue_init(future, 1);

    do {
        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL && !interrupted) {
            // Wait a bit before retrying
            usleep(1000);
        }
        err = rd_kafka_producev(
            rk, RD_KAFKA_V_TOPIC(TOPIC),
            RD_KAFKA_V_KEY(message->key, KEY_SIZE),
            RD_KAFKA_V_VALUE(message->value, VALUE_SIZE),
            RD_KAFKA_V_OPAQUE(future),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_BLOCK),
            RD_KAFKA_V_END);
    } while (err == RD_KAFKA_RESP_ERR__QUEUE_FULL && !interrupted);
    if (err) {
        queue_destroy(future);
        free(future);
        return NULL;
    }

    return future;
}

static bool test_v2_verify_future(test_Future_t future) {
    Queue *q = future;
    V2RecordMetadata *rkmetadata = NULL;
    if (!queue_pop(q, (void **)&rkmetadata, -1)) {
        fprintf(stderr, "Failed to get message from queue\n");
        goto fail;
    }

    if (rkmetadata->err != RD_KAFKA_RESP_ERR_NO_ERROR ||
        !verify_rdkafka_message(rkmetadata))
        goto fail;

    free(rkmetadata->topic);
    free(rkmetadata);
    queue_destroy(q);
    free(q);
    return true;
fail:
    if (rkmetadata) {
        free(rkmetadata->topic);
        free(rkmetadata);
    }
    queue_destroy(q);
    free(q);
    return false;
}

// Process stats (CPU / RSS) — interval sampling, not a lifetime average.
//
// Mirrors the Rust ProcSampler in tests/integration/producer_perf_test.rs.
// `/proc/self/stat` gives cumulative utime+stime in clock ticks; we diff over
// the metric-bucket window to get the interval CPU% for just that window (NOT
// the lifetime cputime/realtime average `ps -o %cpu` reports). RSS comes from
// `/proc/self/status` VmRSS (kB). Linux-only, which matches the run
// environment, and avoids forking a `ps` process every window.

// Linux USER_HZ — clock ticks per second used to convert /proc/self/stat
// utime/stime into CPU seconds. Effectively always 100 on Linux.
#define USER_HZ 100.0

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
    // The comm field (field 2) is wrapped in parens and may contain spaces and
    // ')', so start parsing after the final ')'. utime is field 14, stime
    // field 15, i.e. indices 11 and 12 of the whitespace-split remainder.
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

// Returns the CPU% for the interval since the last call, plus current RSS.
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
    m->file = fopen("metrics.jsonl", "w");
    if (m->file == NULL) {
        fprintf(stderr, "Failed to open metrics file\n");
        exit(1);
    }

    bucket_init(&m->latency);
    bucket_init(&m->bytes);
    bucket_init(&m->messages);
    bucket_init(&m->rss);
    bucket_init(&m->cpu);
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
    // Establish the CPU/RSS sampling baseline so the first window measures a
    // ~1 s interval rather than the whole process lifetime.
    proc_sampler_init(&proc_sampler);
}

static char *metrics_print_double(double f) {
    char *str = malloc(100 * sizeof(char));
    if (isinf(f)) {
        if (f > 0) {
            snprintf(str, 100, "inf");
        } else {
            snprintf(str, 100, "-inf");
        }
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

static long percentile_from_hist(const long* hist, size_t len, double p);

static void metrics_rollover(Metrics* m) {
    pthread_mutex_lock(&m->mutex);

    long current_window_start = m->window_start_ms;
    m->window_start_ms = current_time_ms();

    // Get current values
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

    // Get interval CPU% (for this window only) and current RSS from /proc/self.
    double cpu;
    long rss;
    proc_sampler_sample(&proc_sampler, &cpu, &rss);

    if (cpu >= 0.0) {
        bucket_add(&m->cpu, cpu);
    }

    if (rss >= 0) {
        bucket_add(&m->rss, (double)rss);
    }

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

    // Per-window latency percentiles, then reset the window histogram.
    long lat_p50 = percentile_from_hist(m->latency_pct_hist, MAX_LATENCY_MS + 2, 0.50);
    long lat_p90 = percentile_from_hist(m->latency_pct_hist, MAX_LATENCY_MS + 2, 0.90);
    long lat_p99 = percentile_from_hist(m->latency_pct_hist, MAX_LATENCY_MS + 2, 0.99);
    long lat_p999 = percentile_from_hist(m->latency_pct_hist, MAX_LATENCY_MS + 2, 0.999);
    memset(m->latency_pct_hist, 0, sizeof(m->latency_pct_hist));

    // Reset buckets
    bucket_init(&m->latency);
    bucket_init(&m->bytes);
    bucket_init(&m->messages);
    bucket_init(&m->rss);
    bucket_init(&m->cpu);

    pthread_mutex_unlock(&m->mutex);

    // Convert values to strings using helper functions
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

    char *current_window_start_str = metrics_print_long(current_window_start);
    char *window_start_ms_str = metrics_print_long(m->window_start_ms);
    char *measurement_start_ms_str = metrics_print_long(m->measurement_start_ms);
    char *measurement_end_ms_str = metrics_print_long(m->measurement_end_ms);

    // Write JSON
    fprintf(m->file,
        "{\"rss\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
        "\"cpu\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
        "\"latency\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\","
        "\"p50\":\"%s\",\"p90\":\"%s\",\"p99\":\"%s\",\"p999\":\"%s\"},"
        "\"bytes\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
        "\"messages\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
        "\"window_start_ms\":\"%s\",\"window_end_ms\":\"%s\","
        "\"measurement_start_ms\":\"%s\",\"measurement_end_ms\":\"%s\"}\n",
        rss_avg_str, rss_max_str, rss_total_str, rss_count_str,
        cpu_avg_str, cpu_max_str, cpu_total_str, cpu_count_str,
        lat_avg_str, lat_max_str, lat_total_str, lat_count_str,
        lat_p50_str, lat_p90_str, lat_p99_str, lat_p999_str,
        bytes_avg_str, bytes_max_str, bytes_total_str, bytes_count_str,
        msgs_avg_str, msgs_max_str, msgs_total_str, msgs_count_str,
        current_window_start_str, window_start_ms_str,
        measurement_start_ms_str, measurement_end_ms_str);
    fflush(m->file);

    // Free all allocated strings
    free(rss_avg_str);
    free(rss_max_str);
    free(rss_total_str);
    free(rss_count_str);

    free(cpu_avg_str);
    free(cpu_max_str);
    free(cpu_total_str);
    free(cpu_count_str);

    free(lat_avg_str);
    free(lat_max_str);
    free(lat_total_str);
    free(lat_count_str);
    free(lat_p50_str);
    free(lat_p90_str);
    free(lat_p99_str);
    free(lat_p999_str);

    free(bytes_avg_str);
    free(bytes_max_str);
    free(bytes_total_str);
    free(bytes_count_str);

    free(msgs_avg_str);
    free(msgs_max_str);
    free(msgs_total_str);
    free(msgs_count_str);

    free(current_window_start_str);
    free(window_start_ms_str);
    free(measurement_start_ms_str);
    free(measurement_end_ms_str);
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
            m->total_external_metrics++;
            m->total_cpu += m->last_cpu;
            m->total_rss += m->last_rss;
        }
    }
    return NULL;
}

static void metrics_start_collecting(Metrics* m, int interval_s) {
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

static void metrics_add_latency(Metrics* m, double latency_ms) {
    pthread_mutex_lock(&m->mutex);
    bucket_add(&m->latency, latency_ms);
    long idx = (long)latency_ms;
    if (idx < 0) idx = 0;
    if (idx > MAX_LATENCY_MS + 1) idx = MAX_LATENCY_MS + 1;
    m->latency_pct_hist[idx]++;
    pthread_mutex_unlock(&m->mutex);
}

static void metrics_add_bytes_sent(Metrics* m, long bytes) {
    pthread_mutex_lock(&m->mutex);
    bucket_add(&m->bytes, (double)bytes);
    pthread_mutex_unlock(&m->mutex);
}

static void metrics_add_messages_sent(Metrics* m, long messages) {
    pthread_mutex_lock(&m->mutex);
    bucket_add(&m->messages, (double)messages);
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

static void queue_init(Queue* q, size_t capacity) {
    q->items = malloc(sizeof(void *) * capacity);
    q->head = 0;
    q->tail = 0;
    q->capacity = capacity;
    q->cnt = 0;
    q->closed = false;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

static void queue_destroy(Queue* q) {
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
    free(q->items);
}

#define queue_full(q) (q->cnt > 0 && ((q)->tail + 1) % (q)->capacity == (q)->head)
#define queue_empty(q) (q->cnt == 0)

static bool queue_push(Queue* q, void* item) {
    pthread_mutex_lock(&q->mutex);

    if (q->closed) {
        pthread_mutex_unlock(&q->mutex);
        return false;
    }

    // The handoff queue imposes no size limit of its own: when full, grow it
    // instead of blocking. The only backpressure on in-flight messages then
    // comes from the producer client (Rust `buffer.memory` / librdkafka
    // `queue.buffering.max.*`), which already bounds how many sends can be
    // outstanding — so this queue naturally settles at the producer's buffer
    // depth rather than an arbitrary test-imposed cap.
    if (queue_full(q)) {
        size_t new_capacity = q->capacity * 2;
        void** new_items = malloc(sizeof(void*) * new_capacity);
        for (size_t i = 0; i < q->cnt; i++) {
            new_items[i] = q->items[(q->head + i) % q->capacity];
        }
        free(q->items);
        q->items = new_items;
        q->head = 0;
        q->tail = q->cnt;
        q->capacity = new_capacity;
    }

    q->items[q->tail] = item;
    bool was_empty = queue_empty(q);
    q->tail = (q->tail + 1) % q->capacity;
    q->cnt++;
    if (was_empty) {
        pthread_cond_signal(&q->not_empty);
    }
    pthread_mutex_unlock(&q->mutex);
    return true;
}

static bool queue_pop(Queue* q, void ** item, int timeout_ms) {
    struct timespec ts;
    pthread_mutex_lock(&q->mutex);
    if (!q->closed && q->cnt > 0) {
        *item = q->items[q->head];
        bool was_full = queue_full(q);
        q->head = (q->head + 1) % q->capacity;
        q->cnt--;
        if (was_full) {
            pthread_cond_signal(&q->not_full);
        }
        pthread_mutex_unlock(&q->mutex);
        return true;
    }

    if (timeout_ms >= 0) {
        struct timespec ts;
        struct timeval now;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        int64_t limit = ts.tv_sec * 1000000000LL + ts.tv_nsec + timeout_ms * 1000000LL;
        gettimeofday(&now,NULL);
        ts.tv_sec = now.tv_sec + timeout_ms / 1000;
        ts.tv_nsec = now.tv_usec * 1000 + (timeout_ms % 1000) * 1000000;
        if (ts.tv_nsec >= 1000000000) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000;
        }

        while (queue_empty(q) && !q->closed) {
            pthread_cond_timedwait(&q->not_empty, &q->mutex, &ts);
            clock_gettime(CLOCK_MONOTONIC, &ts);
            int64_t now = ts.tv_sec * 1000000000LL + ts.tv_nsec;
            if (now >= limit)
                break;
        }
    } else {
        while (queue_empty(q) && !q->closed) {
            pthread_cond_wait(&q->not_empty, &q->mutex);
        }
    }

    if (queue_empty(q)) {
        pthread_mutex_unlock(&q->mutex);
        return false;
    }

    *item = q->items[q->head];
    bool was_full = queue_full(q);
    q->head = (q->head + 1) % q->capacity;
    q->cnt--;
    if (was_full) {
        pthread_cond_signal(&q->not_full);
    }
    pthread_mutex_unlock(&q->mutex);
    return true;
}

static void queue_close(Queue* q) {
    pthread_mutex_lock(&q->mutex);
    q->closed = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}

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

// Returns the p-th percentile (0.0..=1.0) latency in ms from the histogram,
// mirroring the Rust percentile_from_hist.
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

static void record_completed_calls(ProducedMessage* pm) {
    if (!test_verify_future(pm->future)) {
        return;
    }

    long start_time = pm->start_time;
    completed_messages++;

    long current_latency = current_time_ns() - start_time;
    if (current_latency > max_latency) {
        max_latency = current_latency;
    }
    total_latency += current_latency;

    long latency_ms = current_latency / 1000000L;
    if (latency_ms < 0) latency_ms = 0;
    if (latency_ms > MAX_LATENCY_MS + 1) latency_ms = MAX_LATENCY_MS + 1;
    latency_hist[latency_ms]++;

    metrics_add_latency(&metrics, (double)(current_latency) / 1000000.0);
    metrics_add_bytes_sent(&metrics, MESSAGE_SIZE);
    metrics_add_messages_sent(&metrics, 1);
}

static void* record_completed_calls_thread(void* arg) {
    Queue* queue = (Queue*)arg;
    ProducedMessage *pmp = NULL;

    while (record_running && (NUM_MESSAGES <= 0 || completed_messages < NUM_MESSAGES)) {
        if (queue_pop(queue, (void**)&pmp, 1000)) {
            record_completed_calls(pmp);
            free(pmp);
        }
    }
    // Process remaining items
    while (queue_pop(queue, (void**)&pmp, 0)) {
        record_completed_calls(pmp);
        free(pmp);
    }

    return NULL;
}

static void *poll_rdkafka_loop(void* rk) {
    while (poll_running) {
        rd_kafka_poll(rk, 1000);
    }
    return NULL;
}

static void start_polling_rdkafka(rd_kafka_t* rk) {
    poll_running = true;
    pthread_create(&poll_thread, NULL, poll_rdkafka_loop, rk);
}

static void start_recording_completed_calls(Queue* queue) {
    record_running = true;
    pthread_create(&record_thread, NULL, record_completed_calls_thread, queue);
}

static void stop_recording_completed_calls() {
    record_running = false;
    pthread_join(record_thread, NULL);
}

static void sleep_until_rate_limit_met(long messages_sent,
        double limit_rpns) {
    long elapsed = current_time_ns() - first_message_time;
    double expected_messages = elapsed * limit_rpns;

    if (messages_sent > expected_messages) {
        double sleep_time = (messages_sent - expected_messages) / LIMIT_RPS;
        long usleep_time = (long)(sleep_time * 1000000);
        if (usleep_time > 0)
            usleep(usleep_time);
    }
}

static void run_test() {
    printf("Running C sync producer performance test ...\n");

    Message* messages = generate_message_data();
    metrics_init(&metrics);
    metrics_start_collecting(&metrics, 1);
    test_Producer_t producer;
    Queue produce_calls;

    if (CLIENT_VERSION == 3) {
        kafka_producer_ProducerProperties_t* properties =
            kafka_producer_ProducerProperties_new();
        kafka_producer_ProducerProperties_put(properties, "bootstrap.servers", BOOTSTRAP_SERVERS);
        if (sasl_enabled) {
            char buffer[512];
            snprintf(buffer, sizeof(buffer),
                "org.apache.kafka.common.security.plain.PlainLoginModule required \n\tusername=\"%s\" \n\tpassword=\"%s\";",
                SASL_USERNAME, SASL_PASSWORD);
            kafka_producer_ProducerProperties_put(properties, "security.protocol", SECURITY_PROTOCOL);
            kafka_producer_ProducerProperties_put(properties, "sasl.mechanism", SASL_MECHANISM);
            kafka_producer_ProducerProperties_put(properties, "sasl.jaas.config", buffer);
        }
        if (SSL_CA_LOCATION) {
            kafka_producer_ProducerProperties_put(properties, "ssl.truststore.location", SSL_CA_LOCATION);
            kafka_producer_ProducerProperties_put(properties, "ssl.truststore.type", "PEM");
        }

        char buffer[512];
        if (buffer_memory_bytes > 0) {
            snprintf(buffer, sizeof(buffer), "%" PRId64, buffer_memory_bytes);
            kafka_producer_ProducerProperties_put(properties, "buffer.memory", buffer);
        }
        snprintf(buffer, sizeof(buffer), "%" PRId64, batch_size_bytes);
        kafka_producer_ProducerProperties_put(properties, "batch.size", buffer);
        snprintf(buffer, sizeof(buffer), "%" PRId64, max_request_size_bytes);
        kafka_producer_ProducerProperties_put(properties, "max.request.size", buffer);
        kafka_producer_ProducerProperties_put(properties, "compression.type", COMPRESSION_TYPE);
        kafka_producer_ProducerProperties_put(properties, "linger.ms", LINGER_MS);
        kafka_producer_ProducerProperties_put(properties, "acks", "all");
        kafka_producer_ProducerProperties_put(properties, "enable.idempotence", ENABLE_IDEMPOTENCE);
        kafka_producer_ProducerProperties_put(properties, "max.in.flight.requests.per.connection", MAX_IN_FLIGHT);

        kafka_common_KafkaError_t* error = NULL;
        producer = kafka_producer_KafkaProducer_new(properties, &error);
        kafka_producer_ProducerProperties_destroy(properties);

        if (error) {
            const char* msg = kafka_common_KafkaError_message(error);
            fprintf(stderr, "Error: %s\n", msg ? msg : "Unknown error");
            kafka_common_KafkaError_destroy(error);
            producer = NULL;
        }
    } else {
        char buffer[512];
        int64_t num_messages_conf = NUM_MESSAGES > INT_MAX ? INT_MAX : NUM_MESSAGES;

        rd_kafka_conf_t* conf = rd_kafka_conf_new();
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

        if (NUM_MESSAGES > 0) {
            snprintf(buffer, sizeof(buffer), "%" PRId64, num_messages_conf);
            rd_kafka_conf_set(conf, "queue.buffering.max.messages", buffer, NULL, 0);
        }
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
        rd_kafka_conf_set(conf, "acks", "all", NULL, 0);
        rd_kafka_conf_set(conf, "enable.idempotence", ENABLE_IDEMPOTENCE, NULL, 0);
        rd_kafka_conf_set(conf, "max.in.flight.requests.per.connection", MAX_IN_FLIGHT, NULL, 0);

        rd_kafka_conf_set_dr_msg_cb(conf, test_v2_dr);

        producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0);
        if (!producer) {
            fprintf(stderr, "Failed to create librdkafka producer\n");
            rd_kafka_conf_destroy(conf);
        } else {
            start_polling_rdkafka(producer);
        }
    }

    if (!producer) {
        fprintf(stderr, "Failed to create producer\n");
        free_message_data(messages);
        return;
    }


    long messages_sent = 0;
    long checkpoint_interval = 0;
    long next_checkpoint = 0;
    double limit_rpns = 0.0;
    bool warmup_succeeded = true;

    if (LIMIT_RPS > 0) {
        checkpoint_interval = LIMIT_RPS / 10;
        limit_rpns = LIMIT_RPS / 1e9;
        next_checkpoint = checkpoint_interval;
    }

    if (WARMUP_S > 0) {
        printf("Warming up for %d seconds ...\n", WARMUP_S);
        long warmup_end_time = current_time_ns() + (long)WARMUP_S * 1000000000L;
        int i = 0;
        while (current_time_ns() < warmup_end_time) {
            Message* message = &messages[i % GENERATED_MESSAGE_COUNT];
            test_Future_t future = test_send(producer, message);
            if (future && !test_verify_future(future)) {
                warmup_succeeded = false;
                break;
            }
            usleep(100000);
        }
        if (!warmup_succeeded) {
            printf("Warmup failed due to message verification error\n");
            goto end;
        } else {
            printf("Warmup complete.\n");
            verified = 0;
        }
    }

    // Small initial capacity; queue_push grows it on demand, so the producer's
    // own buffer/queue size is the only backpressure (no test-imposed cap).
    queue_init(&produce_calls, 1024);
    start_recording_completed_calls(&produce_calls);
    first_message_time = current_time_ns();
    long current_ms = current_time_ms();
    metrics_set_measurement_start_ms(&metrics, current_ms);
    printf("Starting measured interval at %ld ms\n", current_ms);

    bool continue_sending;
    if (NUM_MESSAGES > 0) {
        continue_sending = messages_sent < NUM_MESSAGES;
    } else {
        continue_sending = !interrupted;
    }
    while (continue_sending) {
        Message* message = &messages[messages_sent % GENERATED_MESSAGE_COUNT];

        long start_time = current_time_ns();
        test_Future_t future = test_send(producer, message);
        if (!future) {
            continue;
        }

        ProducedMessage *pm = calloc(1, sizeof(ProducedMessage));
        pm->future = future;
        pm->start_time = start_time;
        queue_push(&produce_calls, pm);

        messages_sent++;
        if (LIMIT_RPS > 0) {
            if (messages_sent >= next_checkpoint) {
                sleep_until_rate_limit_met(messages_sent, limit_rpns);
                next_checkpoint += checkpoint_interval;
            }
        }
        if (messages_sent % 10000 == 0) {
                long duration = current_time_ns() - first_message_time;
                long exceeded_seconds = NUM_MESSAGES > 0 ? 10 : 1;
                if (duration > (long)((TEST_DURATION_S + exceeded_seconds) * 1000000000L)) {
                    printf("Test duration reached, %ld seconds. Interrupting...\n", duration / 1000000000L);
                    interrupted = true;
                    break;
                }
        }

        if (NUM_MESSAGES > 0) {
            continue_sending = messages_sent < NUM_MESSAGES;
        } else {
            continue_sending = !interrupted;
        }
    }

    stop_recording_completed_calls();
    queue_close(&produce_calls);
    if (messages_sent > 0 && LIMIT_RPS > 0) {
        sleep_until_rate_limit_met(messages_sent, limit_rpns);
    }
    after_ns = current_time_ns();
    long after_ms = current_time_ms();

    // Verify all messages were verified
    if (verified != completed_messages) {
        fprintf(stderr, "Verified messages %ld does not match completed messages %ld\n",
            verified, completed_messages);
    } else if (NUM_MESSAGES > 0 && completed_messages != NUM_MESSAGES) {
        fprintf(stderr, "Completed messages %ld does not match produced messages %ld\n",
            completed_messages, NUM_MESSAGES);
    } else {
        metrics_set_measurement_end_ms(&metrics, after_ns);
        long total_time_ns = after_ns - first_message_time;
        double total_time_s = total_time_ns / 1e9;
        double average_cpu, average_rss;
        double message_rate = completed_messages / total_time_s;
        metrics_external_metrics_aggregations(&metrics, &average_cpu, &average_rss);

        printf("End time: %ld ms\n", after_ms);
        printf("Duration: %.2f ms\n", (double)total_time_ns / 1e6);
        printf("Average CPU: %.2f %%\n",
            average_cpu);
        printf("Average RSS: %.2f KiB\n",
            average_rss / 1024.0);
        printf("CPU efficiency: %.2f msg/(s * 1%% CPU)\n",
            (double)message_rate / (average_cpu > 0.0 ? average_cpu : 1.0));
        printf("Memory efficiency: %.2f msg/(s * KB RSS)\n",
            (double)message_rate / (average_rss > 0.0 ? average_rss / 1024.0 : 1.0));
        printf("Average time: %.2f ms\n",
            (double)total_time_ns / completed_messages / 1e6);
        printf("Average rate msg/s: %.2f msg/s\n",
            message_rate);
        printf("Average rate MiB/s: %.2f MiB/s\n",
            (completed_messages * MESSAGE_SIZE / (1024.0 * 1024.0)) / total_time_s);
        printf("Average latency: %.2f ms\n",
            (double)total_latency / completed_messages / 1e6);
        printf("Max latency: %.2f ms\n", (double)max_latency / 1e6);

        long p99_ms = percentile_from_hist(latency_hist, MAX_LATENCY_MS + 2, 0.99);
        printf("p99 latency: %ld ms\n", p99_ms);
        // Latency budget — only asserted when set (P99_LIMIT_MS=0 disables it
        // for max-rate benchmark runs, where queueing makes per-message latency
        // moot). Mirrors the Rust producer_perf_test.
        if (P99_LIMIT_MS > 0 && p99_ms > P99_LIMIT_MS) {
            fprintf(stderr, "p99 latency %ld ms exceeds %ld ms budget\n",
                p99_ms, P99_LIMIT_MS);
            latency_budget_exceeded = true;
        }
    }

    queue_destroy(&produce_calls);
end:
    if (CLIENT_VERSION == 3) {
        kafka_common_KafkaError_t *error = NULL;
        kafka_producer_Producer_close(producer, &error);
        if (error) {
            kafka_common_KafkaError_destroy(error);
        }
        kafka_producer_Producer_destroy(producer);
    } else {
        if (poll_running) {
            poll_running = false;
            pthread_join(poll_thread, NULL);
        }
        rd_kafka_destroy(producer);
    }

    free_message_data(messages);
}

int main(int argc, char** argv) {
    srand(time(NULL));
    double last_cpu = 0.0;
    double last_rss = 0.0;

    const char* do_verify_env = getenv("DO_VERIFY");
    if (do_verify_env != NULL && strcmp(do_verify_env, "False") == 0) {
        DO_VERIFY = false;
        printf("Verification disabled\n");
    }

    const char* client_version_env = getenv("CLIENT_VERSION");
    if (client_version_env != NULL && strcmp(client_version_env, "2") == 0) {
        CLIENT_VERSION = 2;
    }

    const char* warmup_seconds_env = getenv("WARMUP_SECONDS");
    if (warmup_seconds_env != NULL) {
        WARMUP_S = atoi(warmup_seconds_env);
    }
    const char* test_duration_env = getenv("TEST_DURATION_SECONDS");
    if (test_duration_env != NULL) {
        TEST_DURATION_S = atoi(test_duration_env);
    }

    const char* p99_limit_ms_env = getenv("P99_LIMIT_MS");
    if (p99_limit_ms_env != NULL) {
        P99_LIMIT_MS = atol(p99_limit_ms_env);
    }

    const char* bootstrap_servers_env = getenv("BOOTSTRAP_SERVERS");
    if (bootstrap_servers_env != NULL) {
        BOOTSTRAP_SERVERS = bootstrap_servers_env;
    }

    SECURITY_PROTOCOL = getenv("SECURITY_PROTOCOL");
    SASL_MECHANISM = getenv("SASL_MECHANISM");
    SASL_USERNAME = getenv("SASL_USERNAME");
    SASL_PASSWORD = getenv("SASL_PASSWORD");
    SSL_CA_LOCATION = getenv("SSL_CA_LOCATION");

    const char *enable_idempotence_env = getenv("ENABLE_IDEMPOTENCE");
    if (enable_idempotence_env != NULL && strcmp(enable_idempotence_env, "True") == 0) {
        ENABLE_IDEMPOTENCE = "true";
    }

    const char *max_in_flight_env = getenv("MAX_IN_FLIGHT");
    if (max_in_flight_env != NULL) {
        MAX_IN_FLIGHT = max_in_flight_env;
    }

    const char *compression_type_env = getenv("COMPRESSION_TYPE");
    if (compression_type_env != NULL) {
        COMPRESSION_TYPE = compression_type_env;
    }

    const char *buffer_memory_env = getenv("BUFFER_MEMORY");
    if (buffer_memory_env != NULL) {
        BUFFER_MEMORY_MB = atoi(buffer_memory_env);
    }

    const char *batch_size_env = getenv("BATCH_SIZE");
    if (batch_size_env != NULL) {
        BATCH_SIZE_KB = atoi(batch_size_env);
    }

    const char *max_request_size_env = getenv("MAX_REQUEST_SIZE");
    if (max_request_size_env != NULL) {
        MAX_REQUEST_SIZE_KB = atoi(max_request_size_env);
    }

    const char *linger_ms_env = getenv("LINGER_MS");
    if (linger_ms_env != NULL) {
        LINGER_MS = linger_ms_env;
    }

    const char *key_size_env = getenv("KEY_SIZE");
    if (key_size_env != NULL) {
        KEY_SIZE = atoi(key_size_env);
    }

    const char *value_size_env = getenv("VALUE_SIZE");
    if (value_size_env != NULL) {
        VALUE_SIZE = atoi(value_size_env);
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

    if (CLIENT_VERSION == 3) {
        buffer_memory_bytes = BUFFER_MEMORY_MB > 0 ? BUFFER_MEMORY_MB * 1024L * 1024L : BUFFER_MEMORY_MB;
        batch_size_bytes = BATCH_SIZE_KB * 1024L;
        if (MAX_REQUEST_SIZE_KB > 0) {
            max_request_size_bytes = MAX_REQUEST_SIZE_KB * 1024L;
        } else {
            max_request_size_bytes = batch_size_bytes * 64L;
        }

        if (buffer_memory_bytes > -1) {
            if (buffer_memory_bytes > INT_MAX) {
                buffer_memory_bytes = INT_MAX;
            }
            if (buffer_memory_bytes < 1000000) {
                buffer_memory_bytes = 1000000;
            }
        }
    } else {
        long max_bytes = BUFFER_MEMORY_MB > 0 ? BUFFER_MEMORY_MB * 1024L * 1024L : BUFFER_MEMORY_MB;
        buffer_memory_bytes = max_bytes > INT_MAX ? INT_MAX : max_bytes;
        batch_size_bytes = BATCH_SIZE_KB < 1024 ? 1024 * 1024 : BATCH_SIZE_KB * 1024;
        if (MAX_REQUEST_SIZE_KB > 0) {
            max_request_size_bytes = MAX_REQUEST_SIZE_KB * 1024L;
        } else {
            max_request_size_bytes = batch_size_bytes * 64L;
        }
    }
    if (max_request_size_bytes > 8 * 1024 * 1024) {
        max_request_size_bytes = 8 * 1024 * 1024;
    }

    printf("Key size: %ld bytes\n", KEY_SIZE);
    printf("Value size: %ld bytes\n", VALUE_SIZE);
    printf("Bootstrap servers: %s\n", BOOTSTRAP_SERVERS);
    printf("Enable idempotence: %s\n", ENABLE_IDEMPOTENCE);
    printf("Max in flight requests: %s\n", MAX_IN_FLIGHT);
    printf("Compression type: %s\n", COMPRESSION_TYPE);
    if (buffer_memory_bytes < 0)
         printf("Buffer memory: default\n");
    else
        printf("Buffer memory: %ld MB\n", buffer_memory_bytes / (1024L * 1024L));
    printf("Batch size: %" PRId64 " KB\n", batch_size_bytes / 1024);
    printf("Max request size: %" PRId64 " KB\n", max_request_size_bytes / 1024);
    printf("Linger ms: %s\n", LINGER_MS);

    if (CLIENT_VERSION == 2) {
        test_send = test_v2_send;
        test_verify_future = test_v2_verify_future;
        printf("Using librdkafka producer (v2)\n");
    } else {
        test_send = test_v3_send;
        test_verify_future = test_v3_verify_future;
        printf("Using Rust C bindings producer (v3)\n");
    }

    const char *topic_name_env = getenv("TOPIC_NAME");
    if (topic_name_env != NULL) {
        TOPIC = topic_name_env;
    }
    printf("Using topic name: %s\n", TOPIC);

    const char *num_messages_env = getenv("NUM_MESSAGES");
    if (num_messages_env != NULL) {
        NUM_MESSAGES = atol(num_messages_env);
    }

    const char* limit_rps_env = getenv("LIMIT_RPS");
    if (limit_rps_env != NULL) {
        LIMIT_RPS = atol(limit_rps_env);
        NUM_MESSAGES = LIMIT_RPS * TEST_DURATION_S; // Run for TEST_DURATION_S seconds
        printf("Producing %ld messages at %ld msg/s\n",
            NUM_MESSAGES, LIMIT_RPS);
    } else {
        if (NUM_MESSAGES == 0)
            printf("Producing at max rate for %d seconds\n",
                TEST_DURATION_S);
        else
            printf("Producing %ld messages at max rate\n",
                NUM_MESSAGES);
    }
    TOTAL_SIZE = NUM_MESSAGES * MESSAGE_SIZE;
    TOTAL_SIZE_MB = TOTAL_SIZE / (1024.0 * 1024.0);

    run_test();

    printf("Waiting for final metrics collection...\n");
    fflush(stdout);
    sleep(10);
    metrics_external_metrics_last_values(&metrics,
        &last_cpu, &last_rss);
    printf("Final CPU: %.2f %%\n", last_cpu);
    printf("Final RSS: %.2f KiB\n", last_rss / 1024.0);
    metrics_stop_collecting(&metrics);
    printf("Done\n");

    return latency_budget_exceeded ? 1 : 0;
}
