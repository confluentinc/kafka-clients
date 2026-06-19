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
 * ---------------------------------------------------------------------------
 * FAITHFUL PORT of the librdkafka path from
 *   example-confluent-kafka-native-java @ test_consumer_benchmark_c_sync
 *   c/tests/benchmark_e2e_latency.c
 *
 * This is the "exact translation" baseline requested for the consumer perf
 * comparison: the measurement methodology is preserved verbatim so we can
 * later evaluate, change by change, what to modify. The ONLY deviation from
 * the upstream file is that the GraalVM `kafkanative` consumer arm has been
 * removed, because its headers (kafkanative.h, consumer.h, ...) do not exist
 * in this repository. `--client` is still accepted; anything other than
 * `librdkafka` errors out.
 *
 * Methodology kept AS-IS from upstream (intentionally NOT "improved" yet):
 *   - Percentiles via Welford (mean/stdev) + a decimated reservoir, NOT an
 *     exact histogram.
 *   - e2e latency clock read PER RECORD (now_ms - record.timestamp()), after
 *     verify_consumer_record() has touched the payload bytes.
 *   - Measurement stop is gated behind the message-count progress interval.
 *   - Time-based warmup (--warmup seconds).
 *   - Current RSS (task_info / /proc/self/statm) sampled ~1/s, aggregated
 *     avg/min/max per interval by a background snapshot-writer thread.
 *   - librdkafka statistics.interval.ms callback dumped to librdkafka_stats.jsonl.
 *
 * Known divergences from the repo's own librdkafka_e2e.c (the points we plan
 * to revisit): exact histogram, per-batch clock, ungated stop window. See the
 * comparison notes for the rationale of each.
 *
 * Build (matching the existing standalone harness; no Makefile):
 *   # Linux (librdkafka on the default search path):
 *   cc -O2 -std=c11 -Wall benchmark_e2e_latency.c -o benchmark_e2e_latency \
 *      -lrdkafka -lpthread -lm
 *   # macOS (Homebrew librdkafka — point at its include/lib; Mach RSS APIs
 *   # link automatically as a system framework):
 *   P=$(brew --prefix librdkafka); \
 *   cc -O2 -std=c11 -Wall -I"$P/include" -L"$P/lib" benchmark_e2e_latency.c \
 *      -o benchmark_e2e_latency -lrdkafka -lpthread -lm
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <math.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <unistd.h>
#include <pthread.h>

#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/task.h>
#endif

/* For librdkafka */
#include <librdkafka/rdkafka.h>

#define DEFAULT_TARGET_MESSAGES 500000
#define LIBRDKAFKA_BATCH_SIZE 2000

/* ============================================================================
 * Configuration
 * ============================================================================ */

typedef struct {
    char bootstrap_servers[512];
    char topic[256];
    char group_id[256];
    char client_type[32];  /* "librdkafka" (graal arm removed in this port) */
    int target_messages;
    int poll_timeout_ms;
    int warmup_seconds;
    int test_duration_seconds;
    int interval_seconds;
    char output_file[512];
    char output_dir[300];   /* Base output directory */
    /* SASL config */
    char sasl_username[256];
    char sasl_password[512];
} BenchmarkConfig;

static BenchmarkConfig config;

/* ============================================================================
 * Streaming Statistics (Welford's algorithm - O(1) per sample)
 * ============================================================================ */

typedef struct {
    int64_t count;
    double mean;
    double M2;  /* Sum of squares of differences from mean */
    double min_val;
    double max_val;
    /* Reservoir sampling for percentiles */
    double *samples;
    int sample_count;
    int sample_capacity;
    int sample_rate;
} StreamingStats;

static void stats_init(StreamingStats *stats, int initial_capacity) {
    stats->count = 0;
    stats->mean = 0.0;
    stats->M2 = 0.0;
    stats->min_val = 1e18;
    stats->max_val = -1e18;
    stats->sample_capacity = initial_capacity;
    stats->samples = (double *)malloc(sizeof(double) * initial_capacity);
    stats->sample_count = 0;
    stats->sample_rate = 1;
}

static void stats_add(StreamingStats *stats, double value) {
    stats->count++;

    /* Welford's online algorithm for mean and variance */
    double delta = value - stats->mean;
    stats->mean += delta / stats->count;
    double delta2 = value - stats->mean;
    stats->M2 += delta * delta2;

    if (value < stats->min_val) stats->min_val = value;
    if (value > stats->max_val) stats->max_val = value;

    /* Sample for percentile calculation */
    if (stats->count % stats->sample_rate == 0) {
        if (stats->sample_count < stats->sample_capacity) {
            stats->samples[stats->sample_count++] = value;
        } else {
            /* Downsample: keep every other sample */
            int new_count = 0;
            for (int i = 0; i < stats->sample_count; i += 2) {
                stats->samples[new_count++] = stats->samples[i];
            }
            stats->sample_count = new_count;
            stats->sample_rate *= 2;
            stats->samples[stats->sample_count++] = value;
        }
    }
}

static double stats_variance(StreamingStats *stats) {
    return stats->count > 1 ? stats->M2 / stats->count : 0.0;
}

static double stats_stdev(StreamingStats *stats) {
    return sqrt(stats_variance(stats));
}

static int compare_doubles(const void *a, const void *b) {
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

static double stats_percentile(StreamingStats *stats, double p) {
    if (stats->sample_count == 0) return 0.0;

    /* Sort samples */
    qsort(stats->samples, stats->sample_count, sizeof(double), compare_doubles);

    int idx = (int)(stats->sample_count * p);
    if (idx >= stats->sample_count) idx = stats->sample_count - 1;
    return stats->samples[idx];
}

static void stats_reset(StreamingStats *stats) {
    stats->count = 0;
    stats->mean = 0.0;
    stats->M2 = 0.0;
    stats->min_val = 1e18;
    stats->max_val = -1e18;
    stats->sample_count = 0;
    stats->sample_rate = 1;
}

static void stats_free(StreamingStats *stats) {
    if (stats->samples) {
        free(stats->samples);
        stats->samples = NULL;
    }
}

/* ============================================================================
 * Interval Snapshot for time-series data
 * ============================================================================ */

typedef struct {
    int interval_index;
    int64_t window_start_ms;
    int64_t window_end_ms;
    /* Messages stats */
    int64_t messages_count;
    int64_t messages_total;
    double messages_avg;
    int64_t messages_max;
    /* Bytes stats */
    int64_t bytes_total;
    double bytes_avg;
    int64_t bytes_max;
    double throughput_msg_s;
    /* Latency stats for this interval */
    double lat_min, lat_max, lat_mean;
    double lat_p50, lat_p90, lat_p95, lat_p99, lat_p999;
    /* Resource stats for this interval */
    double cpu_avg, cpu_min, cpu_max;
    double mem_avg, mem_min, mem_max;  /* in bytes */
} IntervalSnapshot;

/* ============================================================================
 * Snapshot Writer (Background Thread for JSONL output)
 * ============================================================================ */

static FILE *statistics_file = NULL;

typedef struct {
    IntervalSnapshot *snapshots;
    int snapshot_count;
    int snapshot_capacity;
    pthread_mutex_t mutex;
    pthread_t thread;
    bool running;
    FILE *jsonl_file;
    char output_path[512];
    char client_type[32];
    int64_t measurement_start_ms;
    int64_t measurement_end_ms;
} SnapshotWriter;

static char *snapshot_print_double(double f) {
    char *s = malloc(32);
    if (s) snprintf(s, 32, "%.2f", f);
    return s;
}

static char *snapshot_print_long(int64_t l) {
    char *s = malloc(32);
    if (s) {
        if (l == INT64_MIN) {
            snprintf(s, 32, "-inf");
        } else {
            snprintf(s, 32, "%lld", (long long)l);
        }
    }
    return s;
}

static void snapshot_writer_init(SnapshotWriter *sw, const char *output_dir,
                                  const char *client_type, int capacity) {
    sw->snapshot_capacity = capacity;
    sw->snapshots = (IntervalSnapshot *)malloc(sizeof(IntervalSnapshot) * capacity);
    sw->snapshot_count = 0;
    sw->running = false;
    sw->measurement_start_ms = INT64_MIN;
    sw->measurement_end_ms = INT64_MIN;
    pthread_mutex_init(&sw->mutex, NULL);

    strncpy(sw->client_type, client_type, sizeof(sw->client_type) - 1);
    snprintf(sw->output_path, sizeof(sw->output_path), "%s/metrics.jsonl", output_dir);

    sw->jsonl_file = fopen(sw->output_path, "w");
    if (!sw->jsonl_file) {
        fprintf(stderr, "Failed to open %s for writing\n", sw->output_path);
    } else {
        printf("Snapshot metrics will be written to: %s\n", sw->output_path);
    }
}

static void snapshot_writer_write_batch(SnapshotWriter *sw, IntervalSnapshot *batch, int count) {
    if (!sw->jsonl_file || count == 0) return;

    /* Get measurement times (these are shared across all snapshots in this batch) */
    char *measurement_start_ms_str = snapshot_print_long(sw->measurement_start_ms);
    char *measurement_end_ms_str = snapshot_print_long(sw->measurement_end_ms);

    for (int i = 0; i < count; i++) {
        IntervalSnapshot *s = &batch[i];

        /* Check if this window is before measurement started */
        bool before_end_measurement =
            s->window_start_ms < sw->measurement_end_ms;

        /* Convert values to strings */
        char *lat_min_str = snapshot_print_double(s->lat_min);
        char *lat_max_str = snapshot_print_double(s->lat_max);
        char *lat_mean_str = snapshot_print_double(s->lat_mean);
        char *lat_p50_str = snapshot_print_double(s->lat_p50);
        char *lat_p90_str = snapshot_print_double(s->lat_p90);
        char *lat_p95_str = snapshot_print_double(s->lat_p95);
        char *lat_p99_str = snapshot_print_double(s->lat_p99);
        char *lat_p999_str = snapshot_print_double(s->lat_p999);

        char *cpu_avg_str = snapshot_print_double(s->cpu_avg);
        char *cpu_min_str = snapshot_print_double(s->cpu_min);
        char *cpu_max_str = snapshot_print_double(s->cpu_max);

        char *mem_avg_str = snapshot_print_double(s->mem_avg);
        char *mem_min_str = snapshot_print_double(s->mem_min);
        char *mem_max_str = snapshot_print_double(s->mem_max);

        /* Messages stats */
        char *msg_avg_str = snapshot_print_double(s->messages_avg);
        char *msg_max_str = snapshot_print_long(s->messages_max);
        char *msg_total_str = snapshot_print_long(s->messages_total);
        char *msg_count_str = snapshot_print_long(s->messages_count);

        /* Bytes stats */
        char *bytes_avg_str = snapshot_print_double(s->bytes_avg);
        char *bytes_max_str = snapshot_print_long(s->bytes_max);
        char *bytes_total_str = snapshot_print_long(s->bytes_total);
        char *bytes_count_str = snapshot_print_long(s->messages_count);  /* Same as message count */

        /* Window times - output as "-inf" if before measurement started */
        char *window_start_str = snapshot_print_long(s->window_start_ms);
        char *window_end_str = snapshot_print_long(s->window_end_ms);

        /* Write JSON line - matching producer format structure */
        fprintf(sw->jsonl_file,
            "{\"rss\":{\"average\":\"%s\",\"min\":\"%s\",\"max\":\"%s\"},"
            "\"cpu\":{\"average\":\"%s\",\"min\":\"%s\",\"max\":\"%s\"},"
            "\"latency\":{\"min\":\"%s\",\"max\":\"%s\",\"average\":\"%s\","
            "\"p50\":\"%s\",\"p90\":\"%s\",\"p95\":\"%s\",\"p99\":\"%s\",\"p999\":\"%s\"},"
            "\"bytes\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
            "\"messages\":{\"average\":\"%s\",\"max\":\"%s\",\"total\":\"%s\",\"count\":\"%s\"},"
            "\"window_start_ms\":\"%s\",\"window_end_ms\":\"%s\","
            "\"measurement_start_ms\":\"%s\",\"measurement_end_ms\":\"%s\"}\n",
            mem_avg_str, mem_min_str, mem_max_str,
            cpu_avg_str, cpu_min_str, cpu_max_str,
            lat_min_str, lat_max_str, lat_mean_str,
            lat_p50_str, lat_p90_str, lat_p95_str, lat_p99_str, lat_p999_str,
            bytes_avg_str, bytes_max_str, bytes_total_str, bytes_count_str,
            msg_avg_str, msg_max_str, msg_total_str, msg_count_str,
            window_start_str, window_end_str,
            measurement_start_ms_str, before_end_measurement ? "-inf" :
                measurement_end_ms_str);

        /* Free allocated strings */
        free(lat_min_str); free(lat_max_str); free(lat_mean_str);
        free(lat_p50_str); free(lat_p90_str); free(lat_p95_str);
        free(lat_p99_str); free(lat_p999_str);
        free(cpu_avg_str); free(cpu_min_str); free(cpu_max_str);
        free(mem_avg_str); free(mem_min_str); free(mem_max_str);
        free(msg_avg_str); free(msg_max_str); free(msg_total_str); free(msg_count_str);
        free(bytes_avg_str); free(bytes_max_str); free(bytes_total_str); free(bytes_count_str);
        free(window_start_str); free(window_end_str);
    }

    free(measurement_start_ms_str);
    free(measurement_end_ms_str);

    fflush(sw->jsonl_file);
}

static void *snapshot_writer_thread_func(void *arg) {
    SnapshotWriter *sw = (SnapshotWriter *)arg;

    while (sw->running) {
        sleep(1);  /* Check every second */

        pthread_mutex_lock(&sw->mutex);

        /* Check if we've reached half capacity */
        int half_capacity = sw->snapshot_capacity / 2;
        if (sw->snapshot_count >= half_capacity) {
            int count_written = sw->snapshot_count;

            /* Write current batch to file */
            snapshot_writer_write_batch(sw, sw->snapshots, sw->snapshot_count);

            /* Reset count (reuse array) */
            sw->snapshot_count = 0;

            printf("[SNAPSHOT WRITER] Wrote %d snapshots to %s\n",
                   count_written, sw->output_path);
        }

        pthread_mutex_unlock(&sw->mutex);
    }

    return NULL;
}

static void snapshot_writer_start(SnapshotWriter *sw) {
    if (sw->running) return;
    sw->running = true;
    pthread_create(&sw->thread, NULL, snapshot_writer_thread_func, sw);
}

static void snapshot_writer_stop(SnapshotWriter *sw) {
    if (!sw->running) return;

    sw->running = false;
    pthread_join(sw->thread, NULL);

    /* Flush any remaining snapshots */
    pthread_mutex_lock(&sw->mutex);
    if (sw->snapshot_count > 0) {
        snapshot_writer_write_batch(sw, sw->snapshots, sw->snapshot_count);
        printf("[SNAPSHOT WRITER] Flushed final %d snapshots\n", sw->snapshot_count);
    }
    pthread_mutex_unlock(&sw->mutex);

    if (sw->jsonl_file) {
        fclose(sw->jsonl_file);
        sw->jsonl_file = NULL;
    }
}

static void snapshot_writer_add(SnapshotWriter *sw, IntervalSnapshot *snapshot) {
    pthread_mutex_lock(&sw->mutex);

    if (sw->snapshot_count < sw->snapshot_capacity) {
        sw->snapshots[sw->snapshot_count] = *snapshot;
        sw->snapshot_count++;
    } else {
        fprintf(stderr, "[SNAPSHOT WRITER] Warning: snapshot buffer full\n");
    }

    pthread_mutex_unlock(&sw->mutex);
}

static void snapshot_writer_free(SnapshotWriter *sw) {
    pthread_mutex_destroy(&sw->mutex);
    if (sw->snapshots) {
        free(sw->snapshots);
        sw->snapshots = NULL;
    }
}

static void snapshot_writer_set_measurement_start_ms(SnapshotWriter *sw, int64_t ms) {
    pthread_mutex_lock(&sw->mutex);
    sw->measurement_start_ms = ms;
    pthread_mutex_unlock(&sw->mutex);
}

static void snapshot_writer_set_measurement_end_ms(SnapshotWriter *sw, int64_t ms) {
    pthread_mutex_lock(&sw->mutex);
    sw->measurement_end_ms = ms;
    pthread_mutex_unlock(&sw->mutex);
}

/* ============================================================================
 * Utility Functions (forward declarations)
 * ============================================================================ */

static int64_t current_time_ms(void);
static double current_time_s(void);

/* ============================================================================
 * Resource Monitoring (CPU and Memory)
 * ============================================================================ */

typedef struct {
    double *cpu_samples;
    double *mem_samples;
    int sample_count;
    int sample_capacity;
    double last_cpu_time;
    double last_wall_time;
} ResourceMonitor;

static void resource_monitor_init(ResourceMonitor *mon, int capacity) {
    mon->cpu_samples = (double *)malloc(sizeof(double) * capacity);
    mon->mem_samples = (double *)malloc(sizeof(double) * capacity);
    mon->sample_count = 0;
    mon->sample_capacity = capacity;
    mon->last_cpu_time = 0;
    mon->last_wall_time = 0;
}

static void resource_monitor_free(ResourceMonitor *mon) {
    if (mon->cpu_samples) free(mon->cpu_samples);
    if (mon->mem_samples) free(mon->mem_samples);
    mon->cpu_samples = NULL;
    mon->mem_samples = NULL;
}

static void resource_monitor_reset(ResourceMonitor *mon) {
    mon->sample_count = 0;
}

/* Get current CPU time (user + system) in seconds */
static double get_cpu_time(void) {
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
        return (double)usage.ru_utime.tv_sec + (double)usage.ru_utime.tv_usec / 1000000.0 +
               (double)usage.ru_stime.tv_sec + (double)usage.ru_stime.tv_usec / 1000000.0;
    }
    return 0.0;
}

/* Get current RSS memory in bytes */
static int64_t get_rss_bytes(void) {
#ifdef __APPLE__
    struct task_basic_info info;
    mach_msg_type_number_t size = TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&info, &size) == KERN_SUCCESS) {
        return (int64_t)info.resident_size;
    }
    return 0;
#else
    /* Linux: read from /proc/self/statm */
    FILE *f = fopen("/proc/self/statm", "r");
    if (f) {
        long pages;
        if (fscanf(f, "%*d %ld", &pages) == 1) {
            fclose(f);
            return pages * sysconf(_SC_PAGESIZE);
        }
        fclose(f);
    }
    return 0;
#endif
}

/* Sample current resource usage */
static void resource_monitor_sample(ResourceMonitor *mon) {
    double now = current_time_s();
    double cpu_time = get_cpu_time();

    /* Calculate CPU percentage since last sample */
    double cpu_percent = 0.0;
    if (mon->last_wall_time > 0) {
        double wall_delta = now - mon->last_wall_time;
        double cpu_delta = cpu_time - mon->last_cpu_time;
        if (wall_delta > 0) {
            cpu_percent = (cpu_delta / wall_delta) * 100.0;
        }
    }
    mon->last_cpu_time = cpu_time;
    mon->last_wall_time = now;

    /* Get memory in bytes */
    double mem_bytes = (double)get_rss_bytes();

    /* Store samples */
    if (mon->sample_count < mon->sample_capacity) {
        mon->cpu_samples[mon->sample_count] = cpu_percent;
        mon->mem_samples[mon->sample_count] = mem_bytes;
        mon->sample_count++;
    }
}

/* Get resource statistics */
static void resource_monitor_stats(ResourceMonitor *mon,
                                   double *cpu_avg, double *cpu_min, double *cpu_max,
                                   double *mem_avg, double *mem_min, double *mem_max) {
    *cpu_avg = *cpu_min = *cpu_max = 0.0;
    *mem_avg = *mem_min = *mem_max = 0.0;

    if (mon->sample_count == 0) return;

    double cpu_sum = 0, mem_sum = 0;
    *cpu_min = *cpu_max = mon->cpu_samples[0];
    *mem_min = *mem_max = mon->mem_samples[0];

    for (int i = 0; i < mon->sample_count; i++) {
        cpu_sum += mon->cpu_samples[i];
        mem_sum += mon->mem_samples[i];
        if (mon->cpu_samples[i] < *cpu_min) *cpu_min = mon->cpu_samples[i];
        if (mon->cpu_samples[i] > *cpu_max) *cpu_max = mon->cpu_samples[i];
        if (mon->mem_samples[i] < *mem_min) *mem_min = mon->mem_samples[i];
        if (mon->mem_samples[i] > *mem_max) *mem_max = mon->mem_samples[i];
    }

    *cpu_avg = cpu_sum / mon->sample_count;
    *mem_avg = mem_sum / mon->sample_count;
}

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

static volatile bool running = true;

static void signal_handler(int sig) {
    (void)sig;
    running = false;
    printf("\n[SIGNAL] Received interrupt, shutting down...\n");
}

static int64_t current_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static double current_time_s(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

/* Create directory recursively (like mkdir -p) */
static int mkdir_p(const char *path) {
    char tmp[400];
    char *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    if (tmp[len - 1] == '/') {
        tmp[len - 1] = 0;
    }

    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    return mkdir(tmp, 0755);
}

/* Get current timestamp string */
static void get_timestamp_str(char *buf, size_t len) {
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(buf, len, "%Y%m%d_%H%M%S", tm_info);
}

/* ============================================================================
 * librdkafka Statistics Helpers
 * ============================================================================ */

/* Statistics callback - librdkafka calls this at statistics.interval.ms */

static void append_rdkafka_metrics(const char *json, const char *output_file_path) {
    if (!statistics_file) {
        char stats_file[512];
        const char *last_slash = strrchr(output_file_path, '/');
        if (last_slash) {
            size_t dir_len = last_slash - output_file_path;
            strncpy(stats_file, output_file_path, dir_len);
            stats_file[dir_len] = '\0';
            strcat(stats_file, "/librdkafka_stats.jsonl");
        } else {
            strcpy(stats_file, "librdkafka_stats.jsonl");
        }
        printf("Saving librdkafka stats to: %s\n", stats_file);
        statistics_file = fopen(stats_file, "w");
    }
    if (statistics_file) {
        fputs(json, statistics_file);
        fputs("\n", statistics_file);
    }
}

static int rdkafka_stats_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
    (void)rk;
    (void)json_len;
    (void)opaque;
    append_rdkafka_metrics(json, config.output_file); /* Save to file */
    return 0;  /* Return 0 so librdkafka frees the json buffer */
}

static void close_rdkafka_metrics(void) {
    if (statistics_file)
        fclose(statistics_file);
}

static void verify_consumer_record(int64_t record_timestamp_ms, int partition, int64_t offset, const void *key, size_t key_len, const void *value, size_t value_len) {
        if (record_timestamp_ms <= 0) {
            fprintf(stderr, "[LIBRDKAFKA] Error: wrong message timestamp %lld\n", (long long)record_timestamp_ms);
        }

        if (offset < 0) {
            fprintf(stderr, "[LIBRDKAFKA] Error: wrong message offset %lld\n", (long long)offset);
        }
        if (partition < 0) {
            fprintf(stderr, "[LIBRDKAFKA] Error: wrong message partition %d\n", partition);
        }
        if (key_len > 0) {
            int key_sum = 0;
            for (size_t i = 0; i < key_len; i++) {
                key_sum += ((char *)key)[i];
            }
            if (key_sum == 0) {
                fprintf(stderr, "[LIBRDKAFKA] Error: key_sum=0\n");
            }
        }
        if (value_len > 0) {
            int value_sum = 0;
            for (size_t i = 0; i < value_len; i++) {
                value_sum += ((char *)value)[i];
            }
            if (value_sum == 0) {
                fprintf(stderr, "[LIBRDKAFKA] Error: value_sum=0\n");
            }
        }
}

/* ============================================================================
 * librdkafka Consumer Benchmark
 * ============================================================================ */

static int benchmark_librdkafka(BenchmarkConfig *config, StreamingStats *stats,
                                 SnapshotWriter *writer) {
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    /* Set configuration */
    if (rd_kafka_conf_set(conf, "bootstrap.servers", config->bootstrap_servers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error: %s\n", errstr);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "group.id", config->group_id,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error: %s\n", errstr);
        return -1;
    }

    if (rd_kafka_conf_set(conf, "auto.offset.reset", "latest",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error: %s\n", errstr);
        return -1;
    }

    /* KIP-848 new consumer group protocol (aligned with the repo harness). */
    if (rd_kafka_conf_set(conf, "group.protocol", "consumer",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error: %s\n", errstr);
        return -1;
    }

    /* CRC checking OFF (aligned comparison). */
    if (rd_kafka_conf_set(conf, "check.crcs", "false",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error: %s\n", errstr);
        return -1;
    }

    /* fetch.min.bytes = 4 MiB. */
    if (rd_kafka_conf_set(conf, "fetch.min.bytes", "4194304",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error: %s\n", errstr);
        return -1;
    }

    /* fetch.message.max.bytes is librdkafka's alias for max.partition.fetch.bytes;
     * set to 4 MiB. */
    if (rd_kafka_conf_set(conf, "fetch.message.max.bytes", "4194304",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error: %s\n", errstr);
        return -1;
    }

    /* Enable statistics for metrics comparison */
    if (rd_kafka_conf_set(conf, "statistics.interval.ms", "10000",
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error: %s\n", errstr);
        return -1;
    }
    rd_kafka_conf_set_stats_cb(conf, rdkafka_stats_cb);

    /* SASL config if provided - MUST set security.protocol BEFORE sasl.mechanisms */
    if (config->sasl_username[0] != '\0') {
        if (rd_kafka_conf_set(conf, "security.protocol", "SASL_SSL", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "ERROR: Failed to set security.protocol=SASL_SSL: %s\n", errstr);
            fprintf(stderr, "This usually means librdkafka was built without OpenSSL support; "
                            "rebuild librdkafka with --enable-ssl.\n");
            return -1;
        }
        if (rd_kafka_conf_set(conf, "sasl.mechanisms", "PLAIN", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Error setting sasl.mechanisms: %s\n", errstr);
            return -1;
        }
        if (rd_kafka_conf_set(conf, "sasl.username", config->sasl_username, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Error setting sasl.username: %s\n", errstr);
            return -1;
        }
        if (rd_kafka_conf_set(conf, "sasl.password", config->sasl_password, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            fprintf(stderr, "Error setting sasl.password: %s\n", errstr);
            return -1;
        }
    }

    /* Create consumer */
    rd_kafka_t *consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!consumer) {
        fprintf(stderr, "Failed to create consumer: %s\n", errstr);
        return -1;
    }

    /* Subscribe to topic */
    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, config->topic, RD_KAFKA_PARTITION_UA);

    rd_kafka_resp_err_t err = rd_kafka_subscribe(consumer, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    if (err) {
        fprintf(stderr, "Failed to subscribe: %s\n", rd_kafka_err2str(err));
        rd_kafka_destroy(consumer);
        return -1;
    }

    printf("[LIBRDKAFKA] Subscribed to '%s', waiting for messages...\n", config->topic);

    /* Get consumer queue for batch consumption (matches Java's poll() with max.poll.records=500) */
    rd_kafka_queue_t *consumer_queue = rd_kafka_queue_get_consumer(consumer);
    if (!consumer_queue) {
        fprintf(stderr, "Failed to get consumer queue\n");
        rd_kafka_destroy(consumer);
        return -1;
    }

    /* Benchmark loop */
    int64_t messages_consumed = 0;
    bool warmup_complete = false;

    double last_report_time = 0.0;
    int64_t last_report_count = 0;

    /* Interval tracking */
    double interval_start_time = 0, consume_start_time = 0;
    int64_t interval_start_count = 0;
    int64_t interval_bytes_consumed = 0;
    int64_t interval_max_bytes = 0;
    int current_interval = 0;
    StreamingStats interval_stats;
    stats_init(&interval_stats, 10000);

    /* Resource monitoring */
    ResourceMonitor res_mon;
    resource_monitor_init(&res_mon, 10000);
    ResourceMonitor interval_res_mon;
    resource_monitor_init(&interval_res_mon, 1000);
    double last_resource_sample_time = 0.0;

    /* Calculate progress interval */
    int target_messages = (config->target_messages > 0 ?
        (config->target_messages / 1000) :
        DEFAULT_TARGET_MESSAGES);
    int total_test_duration_s = config->warmup_seconds + config->test_duration_seconds;
    int progress_interval = target_messages / 10;

    /* Batch consume buffer - matches Java's max.poll.records=2000 */
    rd_kafka_message_t *batch[LIBRDKAFKA_BATCH_SIZE];

    while (running && (config->target_messages <= 0 || messages_consumed < config->target_messages)) {
        ssize_t batch_count = rd_kafka_consume_batch_queue(
            consumer_queue, config->poll_timeout_ms, batch, LIBRDKAFKA_BATCH_SIZE);

        /* Service stats callback on main queue (stats live there, not on consumer queue) */
        rd_kafka_poll(consumer, 0);

        if (batch_count <= 0) continue;

        for (ssize_t bi = 0; bi < batch_count; bi++) {
            rd_kafka_message_t *msg = batch[bi];

            if (msg->err) {
                if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    fprintf(stderr, "[LIBRDKAFKA] Error: %s\n", rd_kafka_message_errstr(msg));
                }
                rd_kafka_message_destroy(msg);
                continue;
            }

            /* Get timestamp */
            rd_kafka_timestamp_type_t ts_type;
            int64_t record_timestamp_ms = rd_kafka_message_timestamp(msg, &ts_type);

            verify_consumer_record(
                record_timestamp_ms, msg->partition, msg->offset, msg->key, msg->key_len, msg->payload, msg->len
            );

            if (record_timestamp_ms > 0) {
                int64_t now_ms = current_time_ms();
                double latency_ms = (double)(now_ms - record_timestamp_ms);

                if (latency_ms >= 0) {
                    messages_consumed++;
                    int64_t msg_bytes = (msg->key_len ? msg->key_len : 0) + (msg->len ? msg->len : 0);

                    if (messages_consumed == 1) {
                        consume_start_time = current_time_s();
                        last_report_time = consume_start_time;
                    }

                    if (!warmup_complete) {
                        double now = current_time_s();
                        if (consume_start_time > 0 && now - consume_start_time >= config->warmup_seconds) {
                            warmup_complete = true;
                            interval_start_time = now;
                            last_resource_sample_time = now;
                            interval_start_count = messages_consumed;
                            snapshot_writer_set_measurement_start_ms(writer, current_time_ms());
                            printf("[LIBRDKAFKA] Warmup complete (%d secs), starting measurement...\n",
                                    config->warmup_seconds);
                        }
                    } else {
                        stats_add(stats, latency_ms);
                        stats_add(&interval_stats, latency_ms);
                        interval_bytes_consumed += msg_bytes;
                        if (msg_bytes > interval_max_bytes) {
                            interval_max_bytes = msg_bytes;
                        }
                    }

                    /* Progress report */
                    if (messages_consumed % progress_interval == 0) {
                        double now = current_time_s();
                        double elapsed = now - last_report_time;
                        double throughput = (messages_consumed - last_report_count) / elapsed;
                        printf("[LIBRDKAFKA] %lld msgs, avg_lat=%.2fms, throughput=%.0f/s\n",
                                (long long)messages_consumed, stats->mean, throughput);
                        last_report_time = now;
                        last_report_count = messages_consumed;
                        if (config->test_duration_seconds > 0 &&
                            now  > consume_start_time + total_test_duration_s) {
                            printf("[LIBRDKAFKA] Reached total test duration of %d seconds, stopping...\n", total_test_duration_s);
                            running = false;
                        }
                    }
                }
            }

            rd_kafka_message_destroy(msg);
        }

        /* After processing batch: sample resources & check interval boundary */
        if (warmup_complete) {
            double now = current_time_s();
            if (now - last_resource_sample_time >= 1.0) {
                resource_monitor_sample(&res_mon);
                resource_monitor_sample(&interval_res_mon);
                last_resource_sample_time = now;
            }

            /* Check interval boundary */
            if (now - interval_start_time >= config->interval_seconds) {
                IntervalSnapshot snap;
                snap.interval_index = current_interval;
                snap.window_start_ms = (int64_t)(interval_start_time * 1000LL);
                snap.window_end_ms = (int64_t)(now * 1000LL);

                /* Messages stats */
                snap.messages_count = messages_consumed - interval_start_count;
                snap.messages_total = snap.messages_count;
                snap.messages_avg = 1;  /* Each message counts as 1 */
                snap.messages_max = 1;  /* Each message counts as 1 */

                /* Bytes stats */
                snap.bytes_total = interval_bytes_consumed;
                snap.bytes_avg = snap.messages_count > 0 ?
                    (double)interval_bytes_consumed / snap.messages_count : 0;
                snap.bytes_max = interval_max_bytes;

                snap.throughput_msg_s = snap.messages_count / (now - interval_start_time);
                snap.lat_min = interval_stats.min_val;
                snap.lat_max = interval_stats.max_val;
                snap.lat_mean = interval_stats.mean;
                snap.lat_p50 = stats_percentile(&interval_stats, 0.50);
                snap.lat_p90 = stats_percentile(&interval_stats, 0.90);
                snap.lat_p95 = stats_percentile(&interval_stats, 0.95);
                snap.lat_p99 = stats_percentile(&interval_stats, 0.99);
                snap.lat_p999 = interval_stats.sample_count > 100 ?
                                stats_percentile(&interval_stats, 0.999) : 0;

                /* Resource stats for interval */
                resource_monitor_stats(&interval_res_mon,
                    &snap.cpu_avg, &snap.cpu_min, &snap.cpu_max,
                    &snap.mem_avg, &snap.mem_min, &snap.mem_max);

                /* Add to background writer */
                if (writer) {
                    snapshot_writer_add(writer, &snap);
                }

                printf("[INTERVAL %d] msgs=%lld, throughput=%.0f/s, p99=%.2fms, cpu=%.1f%%, mem=%.1fMB\n",
                    current_interval, (long long)snap.messages_count,
                    snap.throughput_msg_s, snap.lat_p99, snap.cpu_avg, snap.mem_avg / (1024.0 * 1024.0));

                stats_reset(&interval_stats);
                resource_monitor_reset(&interval_res_mon);
                interval_start_time = now;
                interval_start_count = messages_consumed;
                interval_bytes_consumed = 0;
                interval_max_bytes = 0;
                current_interval++;
            }
        }
    }

    stats_free(&interval_stats);
    resource_monitor_free(&res_mon);
    resource_monitor_free(&interval_res_mon);

    /* Final flush of stats callback - must wait >= statistics.interval.ms
     * (1000ms) so the internal timer fires one more stats event with
     * final cumulative counters. */
    rd_kafka_poll(consumer, 1200);

    /* Print consumer metrics before closing */
    double end_time = current_time_s();
    if (consume_start_time == 0) consume_start_time = end_time;
    close_rdkafka_metrics();

    rd_kafka_queue_destroy(consumer_queue);
    rd_kafka_consumer_close(consumer);
    rd_kafka_destroy(consumer);

    printf("[LIBRDKAFKA] Finished: %lld messages consumed\n", (long long)messages_consumed);
    return 0;
}

/* ============================================================================
 * Results Output
 * ============================================================================ */

static void print_results(BenchmarkConfig *config, StreamingStats *stats, double duration_s) {
    printf("\n");
    printf("================================================================================\n");
    printf("E2E Latency Results - %s\n", config->client_type);
    printf("================================================================================\n");
    printf("Topic: %s\n", config->topic);
    printf("Duration: %.2f seconds\n", duration_s);
    printf("Messages: %lld\n",
           (long long)stats->count);
    printf("\nLatency (milliseconds):\n");
    printf("  Min:    %.2f ms\n", stats->min_val);
    printf("  Max:    %.2f ms\n", stats->max_val);
    printf("  Mean:   %.2f ms\n", stats->mean);
    printf("  StDev:  %.2f ms\n", stats_stdev(stats));
    printf("  P50:    %.2f ms\n", stats_percentile(stats, 0.50));
    printf("  P90:    %.2f ms\n", stats_percentile(stats, 0.90));
    printf("  P95:    %.2f ms\n", stats_percentile(stats, 0.95));
    printf("  P99:    %.2f ms\n", stats_percentile(stats, 0.99));
    if (stats->sample_count > 1000) {
        printf("  P99.9:  %.2f ms\n", stats_percentile(stats, 0.999));
    }
    printf("================================================================================\n");
    printf("\nNote: Time-series interval data written to metrics.jsonl\n");
}

static void save_results_json(const char *filename, BenchmarkConfig *config,
                              StreamingStats *stats, double duration_s) {
    FILE *f = fopen(filename, "w");
    if (!f) {
        fprintf(stderr, "Failed to open %s for writing\n", filename);
        return;
    }

    fprintf(f, "{\n");
    fprintf(f, "  \"client_type\": \"%s\",\n", config->client_type);
    fprintf(f, "  \"topic\": \"%s\",\n", config->topic);
    fprintf(f, "  \"bootstrap_servers\": \"%s\",\n", config->bootstrap_servers);
    fprintf(f, "  \"target_messages\": %d,\n", config->target_messages);
    fprintf(f, "  \"warmup_seconds\": %d,\n", config->warmup_seconds);
    fprintf(f, "  \"total_duration_s\": %.2f,\n", duration_s);
    fprintf(f, "  \"messages_measured\": %lld,\n", (long long)stats->count);
    fprintf(f, "  \"latency_ms\": {\n");
    fprintf(f, "    \"min\": %.2f,\n", stats->min_val);
    fprintf(f, "    \"max\": %.2f,\n", stats->max_val);
    fprintf(f, "    \"mean\": %.2f,\n", stats->mean);
    fprintf(f, "    \"stdev\": %.2f,\n", stats_stdev(stats));
    fprintf(f, "    \"p50\": %.2f,\n", stats_percentile(stats, 0.50));
    fprintf(f, "    \"p90\": %.2f,\n", stats_percentile(stats, 0.90));
    fprintf(f, "    \"p95\": %.2f,\n", stats_percentile(stats, 0.95));
    fprintf(f, "    \"p99\": %.2f,\n", stats_percentile(stats, 0.99));
    fprintf(f, "    \"p999\": %.2f\n", stats->sample_count > 1000 ? stats_percentile(stats, 0.999) : 0.0);
    fprintf(f, "  }\n");
    fprintf(f, "}\n");

    fclose(f);
    printf("Results saved to: %s\n", filename);
}

/* ============================================================================
 * Main
 * ============================================================================ */

static void print_usage(const char *prog) {
    printf("Usage: %s [options]\n", prog);
    printf("\nOptions:\n");
    printf("  --client TYPE       Client type: 'librdkafka' (graal arm removed in this port)\n");
    printf("  --bootstrap SERVERS Bootstrap servers (default: localhost:9092)\n");
    printf("  --topic TOPIC       Topic name (default: test-topic)\n");
    printf("  --messages N        Target messages to consume (default: unbounded)\n");
    printf("  --poll-timeout MS   Poll timeout in ms (default: 1000)\n");
    printf("  --warmup SEC        Warmup duration in seconds (default: 120)\n");
    printf("  --test-duration SEC Test duration in seconds (default: 600)\n");
    printf("  --interval SEC      Interval for time-series (default: 1)\n");
    printf("  --sasl-user USER    SASL username\n");
    printf("  --sasl-pass PASS    SASL password\n");
    printf("  --output-dir DIR    Base output directory (default: benchmark_results/c_consumer)\n");
    printf("  --output FILE       Output JSON file (overrides auto-generated path)\n");
    printf("  --help              Show this help\n");
    printf("\nResults are saved to: <output-dir>/<timestamp>_<client>/results.json\n");
}

int main(int argc, char *argv[]) {
    config = (BenchmarkConfig){
        .bootstrap_servers = "localhost:9092",
        .topic = "test-topic",
        .group_id = "",
        .client_type = "librdkafka",
        .target_messages = -1,
        .poll_timeout_ms = 1000,
        .warmup_seconds = 120,
        .test_duration_seconds = 600,
        .interval_seconds = 1,
        .output_file = "",
        .output_dir = "",
        .sasl_username = "",
        .sasl_password = ""
    };

    /* Parse arguments */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--client") == 0 && i + 1 < argc) {
            strncpy(config.client_type, argv[++i], sizeof(config.client_type) - 1);
        } else if (strcmp(argv[i], "--bootstrap") == 0 && i + 1 < argc) {
            strncpy(config.bootstrap_servers, argv[++i], sizeof(config.bootstrap_servers) - 1);
        } else if (strcmp(argv[i], "--topic") == 0 && i + 1 < argc) {
            strncpy(config.topic, argv[++i], sizeof(config.topic) - 1);
        } else if (strcmp(argv[i], "--messages") == 0 && i + 1 < argc) {
            config.target_messages = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--poll-timeout") == 0 && i + 1 < argc) {
            config.poll_timeout_ms = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--warmup") == 0 && i + 1 < argc) {
            config.warmup_seconds = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--test-duration") == 0 && i + 1 < argc) {
            config.test_duration_seconds = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--interval") == 0 && i + 1 < argc) {
            config.interval_seconds = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--sasl-user") == 0 && i + 1 < argc) {
            strncpy(config.sasl_username, argv[++i], sizeof(config.sasl_username) - 1);
        } else if (strcmp(argv[i], "--sasl-pass") == 0 && i + 1 < argc) {
            strncpy(config.sasl_password, argv[++i], sizeof(config.sasl_password) - 1);
        } else if (strcmp(argv[i], "--output-dir") == 0 && i + 1 < argc) {
            strncpy(config.output_dir, argv[++i], sizeof(config.output_dir) - 1);
        } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
            strncpy(config.output_file, argv[++i], sizeof(config.output_file) - 1);
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }

    /* This port only carries the librdkafka arm (the GraalVM kafkanative arm
     * was removed because its headers are not present in this repository). */
    if (strcmp(config.client_type, "librdkafka") != 0) {
        fprintf(stderr,
                "ERROR: client_type '%s' is not available in this port. "
                "Only --client librdkafka is supported here "
                "(the GraalVM arm requires kafkanative headers not in this repo).\n",
                config.client_type);
        return 2;
    }

    /* Generate group ID */
    snprintf(config.group_id, sizeof(config.group_id),
             "benchmark-%s-%ld", config.client_type, (long)time(NULL));

    /* Setup output directory structure if not explicitly specified */
    char timestamp_str[32];
    get_timestamp_str(timestamp_str, sizeof(timestamp_str));

    if (config.output_dir[0] == '\0') {
        /* Default base directory - relative to where benchmark is run */
        strncpy(config.output_dir, "benchmark_results/c_consumer", sizeof(config.output_dir) - 1);
    }

    /* Create run-specific subdirectory: <base>/<timestamp>_<client>/ */
    char run_dir[400];
    snprintf(run_dir, sizeof(run_dir), "%s/%s_%s",
             config.output_dir, timestamp_str, config.client_type);

    /* Create directory structure */
    mkdir_p(run_dir);

    /* Set output file if not explicitly specified */
    if (config.output_file[0] == '\0') {
        snprintf(config.output_file, sizeof(config.output_file),
                 "%s/results.json", run_dir);
    }

    printf("Results will be saved to: %s\n", run_dir);

    /* Setup signal handler */
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    printf("\n================================================================================\n");
    printf("E2E Latency Benchmark - librdkafka Consumer (C)\n");
    printf("================================================================================\n");
    printf("Bootstrap: %s\n", config.bootstrap_servers);
    printf("Topic: %s\n", config.topic);
    printf("Target Messages: %d\n", config.target_messages);
    printf("Warmup: %d seconds\n", config.warmup_seconds);
    printf("Test Duration: %d seconds\n", config.test_duration_seconds);
    printf("Interval: %d seconds\n", config.interval_seconds);
    printf("rdkafka ver: %s\n", rd_kafka_version_str());
    printf("================================================================================\n\n");

    /* Initialize stats */
    StreamingStats stats;
    stats_init(&stats, 100000);

    /* Initialize snapshot writer for background JSONL output */
    SnapshotWriter snapshot_writer;
    snapshot_writer_init(&snapshot_writer, run_dir, config.client_type, 1000);
    snapshot_writer_start(&snapshot_writer);

    double start_time = current_time_s();
    int result = benchmark_librdkafka(&config, &stats, &snapshot_writer);

    /* Set measurement end time before flushing final snapshots */
    snapshot_writer_set_measurement_end_ms(&snapshot_writer, current_time_ms());

    /* Stop snapshot writer and flush remaining data */
    snapshot_writer_stop(&snapshot_writer);

    double duration = current_time_s() - start_time;

    if (result == 0 && stats.count > 0) {
        print_results(&config, &stats, duration);

        /* Save results JSON */
        save_results_json(config.output_file, &config, &stats, duration);

        /* Save config.json in same directory */
        char config_file[512];
        char *last_slash = strrchr(config.output_file, '/');
        if (last_slash) {
            size_t dir_len = last_slash - config.output_file;
            strncpy(config_file, config.output_file, dir_len);
            config_file[dir_len] = '\0';
            strcat(config_file, "/config.json");
        } else {
            strcpy(config_file, "config.json");
        }

        FILE *cf = fopen(config_file, "w");
        if (cf) {
            fprintf(cf, "{\n");
            fprintf(cf, "  \"client_type\": \"%s\",\n", config.client_type);
            fprintf(cf, "  \"bootstrap_servers\": \"%s\",\n", config.bootstrap_servers);
            fprintf(cf, "  \"topic\": \"%s\",\n", config.topic);
            fprintf(cf, "  \"target_messages\": %d,\n", config.target_messages);
            fprintf(cf, "  \"poll_timeout_ms\": %d,\n", config.poll_timeout_ms);
            fprintf(cf, "  \"warmup_seconds\": %d,\n", config.warmup_seconds);
            fprintf(cf, "  \"test_duration_seconds\": %d,\n", config.test_duration_seconds);
            fprintf(cf, "  \"interval_seconds\": %d\n", config.interval_seconds);
            fprintf(cf, "}\n");
            fclose(cf);
            printf("Config saved to: %s\n", config_file);
        }

        /* Save human-readable summary.txt */
        char summary_file[512];
        if (last_slash) {
            size_t dir_len = last_slash - config.output_file;
            strncpy(summary_file, config.output_file, dir_len);
            summary_file[dir_len] = '\0';
            strcat(summary_file, "/summary.txt");
        } else {
            strcpy(summary_file, "summary.txt");
        }

        FILE *sf = fopen(summary_file, "w");
        if (sf) {
            fprintf(sf, "E2E Latency Benchmark Results\n");
            fprintf(sf, "=============================\n\n");
            fprintf(sf, "Client Type: %s\n", config.client_type);
            fprintf(sf, "Topic: %s\n", config.topic);
            fprintf(sf, "Duration: %.2f seconds\n", duration);
            fprintf(sf, "Messages: %lld\n\n", (long long)stats.count);
            fprintf(sf, "Latency (milliseconds):\n");
            fprintf(sf, "  Min:    %.2f ms\n", stats.min_val);
            fprintf(sf, "  Max:    %.2f ms\n", stats.max_val);
            fprintf(sf, "  Mean:   %.2f ms\n", stats.mean);
            fprintf(sf, "  StDev:  %.2f ms\n", stats_stdev(&stats));
            fprintf(sf, "  P50:    %.2f ms\n", stats_percentile(&stats, 0.50));
            fprintf(sf, "  P90:    %.2f ms\n", stats_percentile(&stats, 0.90));
            fprintf(sf, "  P95:    %.2f ms\n", stats_percentile(&stats, 0.95));
            fprintf(sf, "  P99:    %.2f ms\n", stats_percentile(&stats, 0.99));
            if (stats.sample_count > 1000) {
                fprintf(sf, "  P99.9:  %.2f ms\n", stats_percentile(&stats, 0.999));
            }

            fprintf(sf, "\nNote: Time-series resource usage data available in metrics.jsonl\n");

            fclose(sf);
            printf("Summary saved to: %s\n", summary_file);
        }
    }

    /* Cleanup */
    stats_free(&stats);
    snapshot_writer_free(&snapshot_writer);

    return result;
}
