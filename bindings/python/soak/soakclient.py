#!/usr/bin/env python3
#
# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Soak test producer-consumer end-to-end client for long term validation of
# the Rust Kafka client through its Python bindings.
#
# Modelled on confluent-kafka-python's tests/soak/soakclient.py. The structure
# (SoakRecord, SoakClient, one producer thread + one consumer thread,
# filter_config prefix routing, per-partition high-water-mark bookkeeping,
# incr_counter/set_gauge, rusage sampling, periodic status lines) is ported
# faithfully; the client call sites are not, because this binding mirrors the
# Java API rather than librdkafka's. See README.md for the full list of
# differences.
#
# Usage:
#   soakclient.py -i <testid> -t <topic> -r <rate> -f <client-conf-file>
#                 [--variant <profile>] [--payload-size <bytes>]
#
# A unique topic should be used for each soakclient instance.

import argparse
import importlib.util
import json
import logging
import os
import resource
import signal
import socket
import sys
import threading
import time
import traceback
from collections import defaultdict

import psutil

# The bindings install *flat* top-level modules (`producer`, `consumer`); there
# is no package. Nothing in this directory may therefore be named producer.py
# or consumer.py.
from producer import KafkaProducer, ProducerRecord, RecordMetadata, KafkaError
from consumer import KafkaConsumer, TopicPartition, OffsetAndMetadata

# The metrics primitives (Bucket / LatencyBucket / MemoryBucket / CPUBucket /
# Metrics) are shared with the performance harness, which is not an installable
# package either, so its directory goes on sys.path.
_SOAK_DIR = os.path.dirname(os.path.abspath(__file__))
_PERF_DIR = os.path.normpath(os.path.join(_SOAK_DIR, os.pardir, "test", "performance"))
if _PERF_DIR not in sys.path:
    sys.path.insert(0, _PERF_DIR)

from performance_common import Bucket, LatencyBucket, Metrics  # noqa: E402


# ---------------------------------------------------------------------------
# Accepted client configuration keys.
#
# The Rust client only *warns* on an unknown configuration key
# (src/producer/producer_config.rs, src/consumer/consumer_config.rs, both end
# their from_properties() match with a `warn!("Unknown ... key")` arm), so a
# typo would silently start a soak with the default value — e.g. an
# unauthenticated PLAINTEXT connection. A multi-day run must not begin that
# way, so the soak validates its own configuration up front and refuses to
# start on an unknown key.
#
# Keep these in sync with the two Rust files named above.
# ---------------------------------------------------------------------------
PRODUCER_CONFIG_KEYS = frozenset([
    "acks",
    "batch.size",
    "bootstrap.servers",
    "buffer.memory",
    "client.id",
    "compression.type",
    "connections.max.idle.ms",
    "delivery.timeout.ms",
    "enable.idempotence",
    "linger.ms",
    "max.block.ms",
    "max.in.flight.requests.per.connection",
    "max.request.size",
    "metadata.max.age.ms",
    "metadata.max.idle.ms",
    "partitioner.adaptive.partitioning.enable",
    "partitioner.availability.timeout.ms",
    "partitioner.ignore.keys",
    "receive.buffer.bytes",
    "reconnect.backoff.max.ms",
    "reconnect.backoff.ms",
    "request.timeout.ms",
    "retries",
    "retry.backoff.max.ms",
    "retry.backoff.ms",
    "sasl.jaas.config",
    "sasl.mechanism",
    "security.protocol",
    "send.buffer.bytes",
    "transaction.timeout.ms",
    "transactional.id",
])

CONSUMER_CONFIG_KEYS = frozenset([
    "allow.auto.create.topics",
    "auto.commit.interval.ms",
    "auto.offset.reset",
    "bootstrap.servers",
    "check.crcs",
    "client.dns.lookup",
    "client.id",
    "client.rack",
    "config.providers",
    "connections.max.idle.ms",
    "default.api.timeout.ms",
    "enable.auto.commit",
    "enable.metrics.push",
    "exclude.internal.topics",
    "fetch.max.bytes",
    "fetch.max.wait.ms",
    "fetch.min.bytes",
    "group.id",
    "group.instance.id",
    "group.protocol",
    "group.remote.assignor",
    "heartbeat.interval.ms",
    "interceptor.classes",
    "internal.throw.on.fetch.stable.offset.unsupported",
    "isolation.level",
    "key.deserializer",
    "max.partition.fetch.bytes",
    "max.poll.interval.ms",
    "max.poll.records",
    "metadata.max.age.ms",
    "metadata.recovery.rebootstrap.trigger.ms",
    "metadata.recovery.strategy",
    "metric.reporters",
    "metrics.num.samples",
    "metrics.recording.level",
    "metrics.sample.window.ms",
    "partition.assignment.strategy",
    "receive.buffer.bytes",
    "reconnect.backoff.max.ms",
    "reconnect.backoff.ms",
    "request.timeout.ms",
    "retry.backoff.max.ms",
    "retry.backoff.ms",
    "sasl.jaas.config",
    "sasl.mechanism",
    "security.protocol",
    "security.providers",
    "send.buffer.bytes",
    "session.timeout.ms",
    "share.acknowledgement.mode",
    "share.acquire.mode",
    "socket.connection.setup.timeout.max.ms",
    "socket.connection.setup.timeout.ms",
    "value.deserializer",
])

# Both configs route every `ssl.*` key to apply_ssl_config_key() rather than
# listing them individually, so the validator accepts the prefix wholesale.
ACCEPTED_CONFIG_PREFIXES = ("ssl.",)

# Protocol error codes (src/common/protocol/errors.rs) used to classify the
# errors a broker roll produces. Client-side errors (Wakeup, Timeout, ...) all
# report UnknownServerError (-1), so they are classified by message instead.
COORDINATOR_ERROR_CODES = frozenset([
    14,  # CoordinatorLoadInProgress
    15,  # CoordinatorNotAvailable
    16,  # NotCoordinator
])
DISCONNECT_ERROR_CODES = frozenset([
    5,   # LeaderNotAvailable
    6,   # NotLeaderOrFollower
    7,   # RequestTimedOut
    8,   # BrokerNotAvailable
    13,  # NetworkException
])
DISCONNECT_MESSAGE_MARKERS = ("disconnect", "connection", "timed out", "timeout")

METRIC_PFX = "kafka.client.soak.rust."


class SoakRecord(object):
    """A private record type carrying its own metadata in the value payload.

    ``b"{msgid}|{send_time_ms}|{txcnt}|" + padding``

    The Python soak client stamps ``msgid`` / ``time`` / ``txcnt`` as *record
    headers*. This binding cannot: ``ProducerRecord`` accepts only
    ``{topic, value, key, partition, timestamp}``, and the underlying
    ``kafka_producer_ProducerRecord_t`` has no headers field at all, so all four
    FFI send paths pass NULL headers. Producing headers is a client-side gap
    tracked separately; the soak therefore carries the same three fields inside
    the value, which costs nothing and keeps the end-to-end latency measurement
    intact.

    Padding brings the serialized record up to the profile's target payload
    size, so ``848-hi-throughput-*`` exercises ~10 KB records at the same
    message rate.
    """

    _PAD_UNIT = b" SoakRecord nr #0"
    _PAD_SOURCE = _PAD_UNIT * 640  # 10880 bytes, enough for the 10240 B profile
    _target_size = 0

    __slots__ = ("msgid", "send_time_ms", "txcnt")

    def __init__(self, msgid, send_time_ms=None, txcnt=1):
        self.msgid = int(msgid)
        self.send_time_ms = (int(time.time() * 1000)
                             if send_time_ms is None else int(send_time_ms))
        self.txcnt = int(txcnt)

    @classmethod
    def configure_padding(cls, target_size):
        """Set the target serialized size, in bytes, for every record.

        Records whose prefix already exceeds the target are emitted unpadded.
        """
        cls._target_size = max(0, int(target_size))
        if cls._target_size > len(cls._PAD_SOURCE):
            reps = (cls._target_size // len(cls._PAD_UNIT)) + 1
            cls._PAD_SOURCE = cls._PAD_UNIT * reps

    def serialize(self):
        """Return the record as bytes, padded to the configured target size."""
        prefix = b"%d|%d|%d|" % (self.msgid, self.send_time_ms, self.txcnt)
        padding = self._target_size - len(prefix)
        if padding > 0:
            return prefix + self._PAD_SOURCE[:padding]
        return prefix

    def __str__(self):
        return "SoakRecord(msgid={}, send_time_ms={}, txcnt={})".format(
            self.msgid, self.send_time_ms, self.txcnt)

    @classmethod
    def deserialize(cls, binstr):
        """Parse a serialized record.

        Raises ``ValueError`` for any payload that is not in the format above,
        which the consumer counts as a message error rather than letting it
        drive the duplicate/gap accounting.
        """
        if binstr is None:
            raise ValueError("empty payload (None)")
        # `record.value` is a zero-copy memoryview over the fetch batch; copy
        # before parsing so nothing is retained past the poll loop.
        data = bytes(binstr)
        parts = data.split(b"|", 3)
        if len(parts) != 4:
            raise ValueError(
                "malformed payload: expected 4 '|'-separated fields, got {} "
                "in {!r}".format(len(parts), data[:64]))
        try:
            msgid = int(parts[0].decode("ascii"))
            send_time_ms = int(parts[1].decode("ascii"))
            txcnt = int(parts[2].decode("ascii"))
        except (ValueError, UnicodeDecodeError) as ex:
            raise ValueError(
                "malformed payload: non-numeric header field in {!r}: {}".format(
                    data[:64], ex))
        return cls(msgid, send_time_ms, txcnt)


class HighWaterMarks(object):
    """Per-partition high-water-mark bookkeeping.

    Ported from soakclient.py's consumer loop, which is the part of that file
    worth keeping verbatim:

    * ``offset <= hw``    -> duplicate, ``(hw + 1) - offset`` messages
    * ``offset > hw + 1`` -> **loss**, ``offset - (hw + 1)`` messages
    * the first offset seen on a partition establishes the mark and is never
      counted (Java and librdkafka both start consuming at an arbitrary
      committed position)

    The mark is set to the observed offset unconditionally — it is *not*
    advance-only. That matters: when a rebalance replays a partition from an
    older offset, the single jump-back reports exactly the number of records
    about to be redelivered, and the replayed records that follow are then
    in-order and counted once.
    """

    def __init__(self):
        self._marks = defaultdict(int)

    def observe(self, key, offset):
        """Return ``(duplicates, missed)`` implied by seeing ``offset``."""
        hw = self._marks[key]
        duplicates = 0
        missed = 0
        if hw > 0:
            if offset <= hw:
                duplicates = (hw + 1) - offset
            elif offset > hw + 1:
                missed = offset - (hw + 1)
        self._marks[key] = offset
        return duplicates, missed

    def marks(self):
        return dict(self._marks)

    def __len__(self):
        return len(self._marks)


def filter_config(conf, filter_out, strip_prefix):
    """Route a flat config dict to one client.

    Drops every key starting with one of ``filter_out`` and strips
    ``strip_prefix`` from the keys that carry it. Ported verbatim from
    soakclient.py.
    """
    len_sp = len(strip_prefix)
    out = {}
    for k, v in conf.items():
        if len([x for x in filter_out if k.startswith(x)]) > 0:
            continue
        if k.startswith(strip_prefix):
            k = k[len_sp:]
        out[k] = v
    return out


def route_shared_config(conf, own_keys, other_keys):
    """Drop unprefixed keys that belong to the *other* client.

    A single shared config file naturally carries `group.id` (consumer-only)
    alongside `linger.ms` (producer-only). The Python soak hands both to every
    client and relies on librdkafka ignoring what it doesn't want; validating
    strictly (see validate_config) would instead reject them. So a key that is
    unknown to this client but known to the other one is routed away rather
    than rejected — anything unknown to *both* still fails startup.

    Returns ``(kept_conf, routed_away_key_names)``.
    """
    routed = sorted(k for k in conf if k not in own_keys and k in other_keys)
    kept = {k: v for k, v in conf.items() if k not in routed}
    return kept, routed


def validate_config(conf, accepted, what):
    """Raise ``ValueError`` naming every key the Rust client would ignore.

    See the comment on PRODUCER_CONFIG_KEYS for why silent acceptance is not
    survivable on a multi-day run.
    """
    unknown = sorted(
        k for k in conf
        if k not in accepted and not k.startswith(ACCEPTED_CONFIG_PREFIXES))
    if unknown:
        raise ValueError(
            "unknown {} configuration key(s): {}. The Rust client only logs a "
            "warning for unrecognised keys, so this would silently run with the "
            "default value. Fix the key, or prefix it for another client "
            "(producer./consumer./admin.). Accepted {} keys: {}".format(
                what, ", ".join(unknown), what, ", ".join(sorted(accepted))))


def stringify_config(conf):
    """Coerce every value to ``str``.

    The C extension rejects non-string configuration values with a
    ``TypeError``, so an int ``linger.ms`` from a profile would abort startup.
    """
    return {k: str(v) for k, v in conf.items()}


def parse_config_file(fileobj):
    """Parse a ``key=value`` client configuration file."""
    conf = {}
    for line in fileobj:
        line = line.strip()
        if len(line) == 0 or line[0] == '#':
            continue
        i = line.find('=')
        if i <= 0:
            raise ValueError(
                "Configuration lines must be `name=value..`, not {}".format(line))
        conf[line[:i]] = line[i + 1:]
    return conf


def jaas_credentials(jaas_config):
    """Extract ``(username, password)`` from a Java JAAS login-module string.

    Only used to drive librdkafka's AdminClient (topic creation), which takes
    ``sasl.username`` / ``sasl.password`` instead. Returns ``(None, None)`` when
    the string carries neither.
    """
    def field(name):
        marker = name + "="
        idx = jaas_config.find(marker)
        if idx < 0:
            return None
        rest = jaas_config[idx + len(marker):].lstrip()
        if not rest.startswith('"'):
            return None
        end = rest.find('"', 1)
        return None if end < 0 else rest[1:end]

    return field("username"), field("password")


def librdkafka_admin_config(conf):
    """Translate a soak (Java-style) config into a librdkafka AdminClient one.

    Topic creation goes through confluent-kafka's AdminClient, whose
    configuration namespace differs: it has no ``sasl.jaas.config`` and *errors*
    on unknown keys. So only the keys it certainly understands are forwarded,
    and the JAAS credentials are unpacked into username/password.
    """
    passthrough = ("bootstrap.servers", "security.protocol", "sasl.mechanism",
                   "client.id", "ssl.ca.location", "ssl.certificate.location",
                   "ssl.key.location", "ssl.endpoint.identification.algorithm")
    out = {k: v for k, v in conf.items() if k in passthrough}
    jaas = conf.get("sasl.jaas.config")
    if jaas:
        username, password = jaas_credentials(jaas)
        if username is not None:
            out["sasl.username"] = username
        if password is not None:
            out["sasl.password"] = password
    return out


class _OtelSink(object):
    """OpenTelemetry counters/gauges, mirroring soakclient.py's instruments.

    Only constructed when ``opentelemetry`` is importable *and* an exporter is
    configured; the soak is fully functional without it (see SoakMetrics).
    """

    def __init__(self, base_tags):
        from opentelemetry import metrics as otel_metrics

        self._api = otel_metrics
        self._meter = otel_metrics.get_meter("confluent.rust.soak.tests")
        self._base_tags = dict(base_tags)
        self._counters = {}
        self._gauges = {}
        self._gauge_cbs = {}
        self._gauge_values = {}

    @staticmethod
    def available():
        """Importable *and* configured — never a hard dependency."""
        if os.environ.get("OTEL_METRICS_EXPORTER", "none").lower() in ("", "none"):
            return False
        return importlib.util.find_spec("opentelemetry.metrics") is not None

    def incr_counter(self, full_name, incrval, tags):
        merged = dict(tags)
        merged.update(self._base_tags)
        if full_name not in self._counters:
            self._counters[full_name] = self._meter.create_counter(
                full_name, description=full_name)
        self._counters[full_name].add(incrval, merged)

    def set_gauge(self, full_name, val, tags):
        merged = dict(tags)
        merged.update(self._base_tags)
        self._gauge_values.setdefault(full_name, []).append([val, merged])

        if full_name not in self._gauges:
            def cb(_):
                for value in self._gauge_values[full_name]:
                    yield self._api.Observation(value[0], value[1])
                self._gauge_values[full_name] = []

            self._gauge_cbs[full_name] = cb
            self._gauges[full_name] = self._meter.create_observable_gauge(
                callbacks=[cb], name=full_name, description=full_name)


class SoakMetrics(Metrics):
    """Counters and gauges on top of the performance harness's ``Metrics``.

    ``Metrics`` already provides the RSS/CPU buckets, the 1 ms-resolution
    latency histogram with p50/p90/p99/p999, and the daemon rollover thread
    that appends one JSON object per window; this adds the soak's named
    counters and gauges to each rolled-over record.

    The JSONL file is always written — it is the durable local record a 2-week
    run is analysed from, and it is what makes the soak runnable before the
    OTLP pipeline exists. An OTEL meter is driven *in addition* when
    ``opentelemetry`` is importable and ``OTEL_METRICS_EXPORTER`` names an
    exporter.
    """

    #: Gauges recorded through the 1 ms histogram rather than a plain bucket.
    LATENCY_GAUGES = frozenset([
        "producer.latency",
        "consumer.e2e_latency",
        "consumer.recovery_ms",
    ])

    def __init__(self, path, base_tags, prefix=METRIC_PFX):
        # Append: run.sh restarts the client repeatedly and each restart must
        # add to the series, not truncate it.
        super().__init__(path=path, mode="a")
        self._lock = threading.Lock()
        self._prefix = prefix
        self._base_tags = dict(base_tags)
        self._counters = {}
        self._counters_at_last_rollover = {}
        self._gauges = {}
        self._otel = _OtelSink(base_tags) if _OtelSink.available() else None

    @property
    def otel_enabled(self):
        return self._otel is not None

    @staticmethod
    def _key(name, tags):
        if not tags:
            return name
        return "{}{{{}}}".format(
            name, ",".join("{}={}".format(k, tags[k]) for k in sorted(tags)))

    def incr_counter(self, metric_name, incrval, tags=None):
        key = self._key(metric_name, tags)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + incrval
        if self._otel is not None:
            self._otel.incr_counter(self._prefix + metric_name, incrval, tags or {})

    def set_gauge(self, metric_name, val, tags=None):
        key = self._key(metric_name, tags)
        with self._lock:
            bucket = self._gauges.get(key)
            if bucket is None:
                bucket = (LatencyBucket() if metric_name in self.LATENCY_GAUGES
                          else Bucket())
                self._gauges[key] = bucket
            bucket.add_measurement(val)
        if self._otel is not None:
            self._otel.set_gauge(self._prefix + metric_name, val, tags or {})

    def observe_message(self, size_bytes, latency_ms):
        """Feed the inherited throughput/latency buckets (thread-safe)."""
        with self._lock:
            self.messages.add_measurement(1)
            self.bytes.add_measurement(size_bytes)
            if latency_ms is not None:
                self.latency.add_measurement(latency_ms)

    def rollover(self):
        with self._lock:
            record = super().rollover()
            counters = {}
            for key, total in self._counters.items():
                previous = self._counters_at_last_rollover.get(key, 0)
                counters[key] = {"total": total, "delta": total - previous}
            self._counters_at_last_rollover = dict(self._counters)
            gauges = {key: bucket.rollover()
                      for key, bucket in self._gauges.items()}
        record["prefix"] = self._prefix
        record["tags"] = self._base_tags
        record["counters"] = counters
        record["gauges"] = gauges
        return record

    def write_final(self):
        """Write one last window before shutting down."""
        self.write_record(self.rollover())


class SoakClient(object):
    """A Producer sending messages at the given rate and a Consumer consuming
    them, each on its own thread, printing their counters every ~10 seconds.

    Producer ``send()`` is thread-safe (the C extension takes a mutex under
    ``Py_BEGIN_ALLOW_THREADS``); the Consumer is single-owner, hence exactly one
    consumer thread and no other caller touching it.
    """

    METRIC_PFX = METRIC_PFX

    def __init__(self, args, conf):
        self.topic = args.topic
        self.testid = args.testid
        self.variant = args.variant
        self.rate = float(args.rate)
        # Messages between sample log lines, and — divided by the rate — the
        # seconds between status lines (~10 s, matching the Python soak's
        # documented behaviour).
        self.disprate = max(1, int(self.rate * 10))
        self.status_interval = self.disprate / self.rate
        self.commit_interval = float(args.commit_interval)
        self.poll_timeout = float(args.poll_timeout)
        self.stall_threshold = float(args.stall_threshold)
        self.max_send_attempts = int(args.max_send_attempts)
        self.run = True
        self.start_time = time.time()

        # Set by the signal handler; also used instead of time.sleep() so the
        # producer thread's pacing sleep aborts immediately on shutdown.
        self.stop_event = threading.Event()
        # Guards against issuing more than one consumer.wakeup() — see
        # request_stop().
        self._wakeup_sent = threading.Event()

        self.logger = self._make_logger(args.log_level)

        # Counters. Delivery callbacks fire on the C poll thread, so every
        # counter is guarded.
        self._lock = threading.Lock()
        self.producer_msgid = 0
        self.dr_cnt = 0
        self.dr_err_cnt = 0
        self.producer_error_cb_cnt = 0
        self.outstanding = 0
        self.msg_cnt = 0
        self.msg_dup_cnt = 0
        self.msg_miss_cnt = 0
        self.msg_err_cnt = 0
        self.consumer_err_cnt = 0
        self.consumer_error_cb_cnt = 0
        self.rebalance_cnt = 0
        self.disconnect_cnt = 0
        self.coordinator_move_cnt = 0
        self.last_committed = None

        self.last_rusage = None
        self.last_rusage_time = None
        self.baseline_rss_mib = None
        self.proc = psutil.Process(os.getpid())

        # A unique metrics host id so several soaks on one box stay distinct.
        hostname = os.environ.get("HOSTNAME") or socket.gethostname()
        self.hostname = "rust-{}-{}".format(hostname, self.topic)

        base_tags = {"host": self.hostname, "testid": self.testid,
                     "variant": self.variant}
        self.metrics = SoakMetrics(path=args.metrics_file, base_tags=base_tags)
        self.logger.info("SoakClient id %s (variant %s, rate %g msg/s, "
                         "payload %d B, metrics -> %s, otel %s)",
                         self.hostname, self.variant, self.rate,
                         args.payload_size, args.metrics_file,
                         "on" if self.metrics.otel_enabled else "off (JSONL only)")
        self._log_build_manifest()

        conf = dict(conf)
        if 'group.id' not in conf and 'consumer.group.id' not in conf:
            conf['group.id'] = 'soakclient-{}-{}'.format(
                self.hostname, sys.version.split(' ')[0])

        # Route and validate all three configs BEFORE anything with a
        # side effect: a rejected key must not leave a topic behind.
        aconf = filter_config(conf, ["consumer.", "producer."], "admin.")
        aconf['client.id'] = self.testid

        pconf = stringify_config(filter_config(conf, ["consumer.", "admin."], "producer."))
        pconf['client.id'] = self.testid
        pconf, routed = route_shared_config(pconf, PRODUCER_CONFIG_KEYS,
                                            CONSUMER_CONFIG_KEYS)
        if routed:
            self.logger.info("producer: not a producer key, routed to the "
                             "consumer only: %s", ", ".join(routed))
        validate_config(pconf, PRODUCER_CONFIG_KEYS, "producer")

        cconf = stringify_config(filter_config(conf, ["producer.", "admin."], "consumer."))
        cconf['client.id'] = self.testid
        cconf, routed = route_shared_config(cconf, CONSUMER_CONFIG_KEYS,
                                            PRODUCER_CONFIG_KEYS)
        if routed:
            self.logger.info("consumer: not a consumer key, routed to the "
                             "producer only: %s", ", ".join(routed))
        validate_config(cconf, CONSUMER_CONFIG_KEYS, "consumer")

        # Topic creation goes through librdkafka's AdminClient (this binding
        # ships no admin module). Create-if-absent by default: run.sh restarts
        # the client repeatedly and a restart must never discard the topic.
        self.create_topic(self.topic, librdkafka_admin_config(stringify_config(aconf)),
                          partitions=args.partitions,
                          replication_factor=args.replication_factor,
                          recreate=args.recreate_topic)

        # Both clients are constructed before either thread starts, so a
        # failure here cannot leave the producer running with no consumer.
        self.logger.info("producer: using client.id %s", pconf['client.id'])
        self.producer = KafkaProducer(pconf)

        self.logger.info("consumer: using group.id %s", cconf.get('group.id'))
        self.consumer = KafkaConsumer(cconf)

        # Counters that must appear in the metrics even while they stay at
        # zero. producer.errorcb / consumer.errorcb have no source in this
        # client (it exposes no error callback) and are emitted only so the
        # dashboards ported from the Python soak keep their series.
        for name in ("producer.drerr", "producer.errorcb", "consumer.error",
                     "consumer.msgdup", "consumer.msgerr", "consumer.missedmsg",
                     "consumer.errorcb", "consumer.rebalance",
                     "consumer.disconnect", "consumer.coordinator_move"):
            self.incr_counter(name, 0)

        # RSS baseline *after* client construction: a Python process's RSS
        # includes CPython, its GC and the C extension, so absolute RSS growth
        # is not by itself attributable to the Rust client. memory.rss.delta is
        # measured from here.
        self.baseline_rss_mib = self.proc.memory_info().rss / (1024.0 * 1024.0)
        self.logger.info("baseline RSS after client construction: %.3f MiB",
                         self.baseline_rss_mib)

        # Mark the measurement as started so the inherited CPU/RSS aggregation
        # (external_metrics_aggregations) accumulates over the whole run rather
        # than treating every window as warmup.
        self.metrics.measurement_start_ms = int(time.time() * 1000)
        self.metrics.start_collecting(interval_s=args.metrics_interval)

        self.producer_thread = threading.Thread(
            target=self.producer_thread_main, name="producer")
        self.consumer_thread = threading.Thread(
            target=self.consumer_thread_main, name="consumer")
        self.producer_thread.start()
        self.consumer_thread.start()

    # -- setup helpers ------------------------------------------------------
    @staticmethod
    def _make_logger(level):
        logger = logging.getLogger('soakclient')
        logger.setLevel(getattr(logging, level.upper(), logging.DEBUG))
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            '%(asctime)-15s %(levelname)-8s [%(threadName)s] %(message)s'))
        logger.addHandler(handler)
        return logger

    def _log_build_manifest(self):
        """Log the manifest build.sh wrote, so a 2-week run is traceable to an
        exact commit."""
        path = os.environ.get("SOAK_BUILD_MANIFEST",
                              os.path.join(_SOAK_DIR, "build-manifest.json"))
        try:
            with open(path) as fh:
                manifest = json.load(fh)
        except (OSError, ValueError) as ex:
            self.logger.warning("no build manifest at %s (%s); "
                                "this build is not traceable to a commit", path, ex)
            return
        self.logger.info("build manifest: %s", json.dumps(manifest, sort_keys=True))

    def create_topic(self, topic, aconf, partitions, replication_factor, recreate):
        """Create the topic if it doesn't already exist.

        Uses confluent-kafka's AdminClient: this binding exposes no admin API to
        Python. ``recreate`` (``--recreate-topic``) additionally deletes it
        first, which is destructive and must never be the default — run.sh
        restarts the soak in a loop.
        """
        from confluent_kafka.admin import AdminClient, NewTopic
        from confluent_kafka import KafkaException
        from confluent_kafka import KafkaError as CKafkaError

        if recreate:
            from performance_common import recreate_topic
            self.logger.warning("--recreate-topic: deleting and re-creating %s", topic)
            sasl_conf = {k: v for k, v in aconf.items() if k != "bootstrap.servers"}
            recreate_topic(aconf["bootstrap.servers"], topic,
                           sasl_conf=sasl_conf, partitions=partitions)
            return

        admin = AdminClient(aconf)
        new_topic = NewTopic(topic, num_partitions=partitions,
                             replication_factor=replication_factor)
        for _topic, fut in admin.create_topics([new_topic]).items():
            try:
                fut.result()
                self.logger.info("Created topic %s (partitions=%d, rf=%d)",
                                 _topic, partitions, replication_factor)
            except KafkaException as ex:
                if ex.args[0].code() == CKafkaError.TOPIC_ALREADY_EXISTS:
                    self.logger.info("Topic %s already exists: good", _topic)
                else:
                    raise

    # -- instrumentation ----------------------------------------------------
    def incr_counter(self, metric_name, incrval, tags=None):
        """Increment metric counter by incrval."""
        self.metrics.incr_counter(metric_name, incrval, tags)

    def set_gauge(self, metric_name, val, tags=None):
        """Set metric gauge to val."""
        self.metrics.set_gauge(metric_name, val, tags)

    # -- producer -----------------------------------------------------------
    def _record_delivery(self, metadata: RecordMetadata, latency_ms):
        """Account for one successful delivery report."""
        with self._lock:
            self.dr_cnt += 1
            dr_cnt = self.dr_cnt
        self.incr_counter("producer.drok", 1)
        self.set_gauge("producer.latency", latency_ms,
                       tags={"partition": "{}".format(metadata.partition())})
        if (dr_cnt % self.disprate) == 0:
            self.logger.debug(
                "producer: delivered message to %s [%d] at offset %d in %.1f ms",
                metadata.topic(), metadata.partition(), metadata.offset(),
                latency_ms)

    def _on_delivery(self, future, sent_at):
        """Delivery report. Runs on the C extension's poll thread."""
        try:
            try:
                metadata = future.result()
            finally:
                with self._lock:
                    self.outstanding -= 1
            self._record_delivery(metadata, (time.time() - sent_at) * 1000.0)
        except KafkaError as ex:
            with self._lock:
                self.dr_err_cnt += 1
            self.logger.warning("producer: delivery failed: %s [code %s]",
                                ex.message, ex.code)
            self.incr_counter("producer.drerr", 1)
            self.incr_counter("producer.delivery.failure", 1,
                              tags={"err": str(ex.code)})
        except Exception as ex:  # cancellation, and anything else
            with self._lock:
                self.dr_err_cnt += 1
            self.logger.warning("producer: delivery failed: %s", ex)
            self.incr_counter("producer.drerr", 1)

    def produce_record(self):
        """Produce a single record.

        ``send()`` returns a future and blocks the calling thread when the
        accumulator is full (there is no ``BufferError``/retry-on-full loop to
        port, and no ``poll()`` to serve a queue). ``txcnt`` counts *send
        attempts*: the loop only re-runs when ``send()`` itself raises a
        retriable error.
        """
        with self._lock:
            msgid = self.producer_msgid
            self.producer_msgid += 1

        txcnt = 0
        while self.run and txcnt < self.max_send_attempts:
            txcnt += 1
            record = SoakRecord(msgid, txcnt=txcnt)
            producer_record = ProducerRecord(self.topic, record.serialize())

            with self._lock:
                self.outstanding += 1
            sent_at = time.time()
            try:
                future = self.producer.send(producer_record)
            except KafkaError as ex:
                with self._lock:
                    self.outstanding -= 1
                if not ex.is_retriable or txcnt >= self.max_send_attempts:
                    self._count_send_failure(msgid, ex)
                    return
                self.logger.warning("producer: send attempt %d for msgid %d "
                                    "failed (retriable): %s", txcnt, msgid, ex.message)
                self.stop_event.wait(0.1)
                continue
            except Exception as ex:
                with self._lock:
                    self.outstanding -= 1
                self._count_send_failure(msgid, ex)
                return

            future.add_done_callback(
                lambda f, t=sent_at: self._on_delivery(f, t))
            self.incr_counter("producer.send", 1)
            return

    def _count_send_failure(self, msgid, ex):
        with self._lock:
            self.dr_err_cnt += 1
        self.logger.error("producer: giving up on msgid %d: %s", msgid, ex)
        self.incr_counter("producer.drerr", 1)

    def producer_status(self):
        """Print producer status."""
        with self._lock:
            produced, delivered = self.producer_msgid, self.dr_cnt
            failed, errcbs = self.dr_err_cnt, self.producer_error_cb_cnt
            outstanding = self.outstanding
        self.logger.info(
            "producer: %d messages produced, %d delivered, %d failed, "
            "%d error_cbs, %d outstanding",
            produced, delivered, failed, errcbs, outstanding)

    def producer_run(self):
        """Producer main loop.

        Batched pacing, ported from the Python soak's ``--perf`` path: produce
        ``max(1, rate/100)`` records then sleep off the batch's remaining time
        budget. At the soak's 80 msg/s the batch is 1 — the batching only
        matters if the rate is raised later, where a per-message sleep is below
        what the OS can honour.
        """
        batch = max(1, int(self.rate / 100))
        batch_intvl = batch / self.rate
        next_status = time.time() + self.status_interval

        while self.run:
            t_start = time.time()

            for _ in range(batch):
                if not self.run:
                    break
                self.produce_record()

            now = time.time()
            if now > next_status:
                self.producer_status()
                next_status = now + self.status_interval

            remaining_time = batch_intvl - (time.time() - t_start)
            if remaining_time > 0:
                # Event.wait() rather than sleep(): aborts on shutdown.
                self.stop_event.wait(remaining_time)

        # Wait for outstanding messages to be delivered. flush() is
        # uninterruptible in this binding, which is why main() arms a shutdown
        # watchdog before joining the threads.
        self.logger.info("producer: flushing")
        self.producer.flush()
        self.producer_status()

    def producer_thread_main(self):
        """Producer thread main function."""
        try:
            self.producer_run()
        except KeyboardInterrupt:
            self.logger.info("producer: aborted by user")
            self.abort()
        except Exception as ex:
            self.logger.fatal("producer: fatal exception: %s:\n%s",
                              ex, traceback.format_exc())
            self.abort()

    # -- consumer -----------------------------------------------------------
    def consumer_status(self):
        """Print consumer status."""
        with self._lock:
            stats = (self.msg_cnt, self.msg_dup_cnt, self.msg_miss_cnt,
                     self.msg_err_cnt, self.consumer_err_cnt,
                     self.consumer_error_cb_cnt)
        self.logger.info(
            "consumer: %d messages consumed, %d duplicates, %d missed, "
            "%d message errors, %d consumer errors, %d error_cbs", *stats)

    def _classify_error(self, where, ex):
        """Count a client error, and the roll symptoms it implies."""
        with self._lock:
            self.consumer_err_cnt += 1
        self.incr_counter("consumer.error", 1)

        code = ex.code if isinstance(ex, KafkaError) else None
        message = (ex.message if isinstance(ex, KafkaError) else str(ex)).lower()
        if code in COORDINATOR_ERROR_CODES:
            with self._lock:
                self.coordinator_move_cnt += 1
            self.incr_counter("consumer.coordinator_move", 1)
            self.logger.warning("%s: coordinator moved (code %s): %s",
                                where, code, ex)
        elif (code in DISCONNECT_ERROR_CODES
                or any(m in message for m in DISCONNECT_MESSAGE_MARKERS)):
            with self._lock:
                self.disconnect_cnt += 1
            self.incr_counter("consumer.disconnect", 1)
            self.logger.warning("%s: broker/connection error (code %s): %s",
                                where, code, ex)
        else:
            self.logger.error("%s: error (code %s): %s", where, code, ex)

    def _check_assignment(self, previous):
        """Report assignment changes — the only rebalance signal available.

        The binding bridges no ``ConsumerRebalanceListener`` callbacks (see
        consumer.py's module docstring), so a rebalance is observed after the
        fact, by polling ``assignment()``.
        """
        try:
            current = frozenset(
                (tp.topic, tp.partition) for tp in self.consumer.assignment())
        except Exception as ex:
            self.logger.warning("consumer: assignment() failed: %s", ex)
            return previous
        if current != previous:
            with self._lock:
                self.rebalance_cnt += 1
            self.incr_counter("consumer.rebalance", 1)
            self.set_gauge("consumer.assignment_size", len(current))
            self.logger.info("consumer: assignment changed: %d partition(s): %s",
                             len(current), sorted(current))
        return current

    def _consume_record(self, record, hwmarks, pending):
        """Verify and account for one record."""
        try:
            soak_record = SoakRecord.deserialize(record.value)
        except ValueError as ex:
            self.logger.info(
                "consumer: Failed to deserialize message in %s [%d] at offset "
                "%d: %s", record.topic, record.partition, record.offset, ex)
            with self._lock:
                self.msg_err_cnt += 1
            self.incr_counter("consumer.msgerr", 1)
            # Corrupt payload: don't count it as consumed and don't let it
            # drive hwmark/dup logic — a bad payload is not a gap.
            return

        with self._lock:
            self.msg_cnt += 1
            msg_cnt = self.msg_cnt
        self.incr_counter("consumer.msg", 1)

        # End-to-end latency, in milliseconds, from the payload's send time.
        latency_ms = (time.time() * 1000.0) - soak_record.send_time_ms
        self.set_gauge("consumer.e2e_latency", latency_ms,
                       tags={"partition": "{}".format(record.partition)})
        self.metrics.observe_message(record.serialized_value_size, latency_ms)

        if (msg_cnt % self.disprate) == 0:
            self.logger.info(
                "consumer: %d messages consumed: Message %s [%d] at offset %d "
                "(msgid %d, txcnt %d, latency %.1f ms)",
                msg_cnt, record.topic, record.partition, record.offset,
                soak_record.msgid, soak_record.txcnt, latency_ms)

        hwkey = "{}-{}".format(record.topic, record.partition)
        duplicates, missed = hwmarks.observe(hwkey, record.offset)
        if duplicates:
            self.logger.warning(
                "consumer: Old or duplicate message %s [%d] at offset %d: "
                "wanted a higher offset (%d duplicate(s), last committed %s)",
                record.topic, record.partition, record.offset, duplicates,
                self.last_committed)
            with self._lock:
                self.msg_dup_cnt += duplicates
            self.incr_counter("consumer.msgdup", 1)
        elif missed:
            self.logger.warning(
                "consumer: Lost messages, now at %s [%d] offset %d: "
                "%d message(s) missed (last committed %s)",
                record.topic, record.partition, record.offset, missed,
                self.last_committed)
            with self._lock:
                self.msg_miss_cnt += missed
            self.incr_counter("consumer.missedmsg", 1)

        pending[TopicPartition(record.topic, record.partition)] = \
            OffsetAndMetadata(record.offset + 1)

    @staticmethod
    def _is_wakeup(ex):
        """Whether an exception is this client's WakeupException equivalent.

        Client-side errors carry no distinct protocol code (KafkaError::Wakeup
        reports UnknownServerError, -1), so the message is the only signal.
        """
        message = ex.message if isinstance(ex, KafkaError) else str(ex)
        return "wakeup" in message.lower()

    def _commit(self, pending):
        """Commit the pending offsets synchronously.

        ``commit_async()`` takes neither offsets nor a completion callback in
        this binding, so the sync form is the only one whose failures can be
        counted — which is the whole point of the Python soak's ``on_commit``
        callback.

        A commit aborted by ``wakeup()`` is retried once rather than counted as
        a failure. ``wakeup()`` aborts exactly one blocking operation, and a
        wakeup only ever comes from this client's own shutdown path; if it lands
        between two polls it aborts a commit instead, which would otherwise lose
        the window and log a spurious error on every clean shutdown.
        """
        if not pending:
            return
        offsets = dict(pending)
        try:
            self.consumer.commit(offsets)
        except Exception as ex:
            # KafkaError is an Exception subclass; _classify_error reads .code
            # when present and falls back to the message otherwise.
            if not self._is_wakeup(ex):
                self._classify_error("consumer: offset commit failed", ex)
                return
            self.logger.info("consumer: commit aborted by wakeup; retrying once")
            try:
                self.consumer.commit(offsets)
            except Exception as retry_ex:
                self._classify_error("consumer: offset commit failed", retry_ex)
                return
        self.last_committed = {
            "{}-{}".format(tp.topic, tp.partition): oam.offset
            for tp, oam in offsets.items()
        }
        pending.clear()

    def consumer_run(self):
        """Consumer main loop."""
        self.consumer.subscribe([self.topic])

        hwmarks = HighWaterMarks()
        pending = {}
        assignment = frozenset()

        now = time.time()
        next_status = now + self.status_interval
        next_commit = now + self.commit_interval
        last_progress = now
        stalled = False

        while self.run:
            now = time.time()
            if now > next_status:
                self.consumer_status()
                next_status = now + self.status_interval

            try:
                # NOTE: poll() takes SECONDS (float) here, not milliseconds.
                records = self.consumer.poll(self.poll_timeout)
            except Exception as ex:
                if not self.run:
                    break  # wakeup() from the signal handler
                self._classify_error("consumer: poll", ex)
                self.stop_event.wait(0.5)
                continue

            assignment = self._check_assignment(assignment)

            if len(records):
                if stalled:
                    recovery_ms = (time.time() - last_progress) * 1000.0
                    self.set_gauge("consumer.recovery_ms", recovery_ms)
                    self.logger.warning(
                        "consumer: recovered after %.1f ms without records",
                        recovery_ms)
                    stalled = False
                last_progress = time.time()

                for record in records:
                    self._consume_record(record, hwmarks, pending)
            elif not stalled and (time.time() - last_progress) > self.stall_threshold:
                stalled = True
                self.logger.warning(
                    "consumer: no records for %.1f s (assignment: %d partitions)",
                    time.time() - last_progress, len(assignment))

            if time.time() > next_commit:
                self._commit(pending)
                next_commit = time.time() + self.commit_interval

        # Best-effort final commit so a restart does not replay this window.
        self._commit(pending)

    def consumer_thread_main(self):
        """Consumer thread main function."""
        try:
            self.consumer_run()
        except KeyboardInterrupt:
            self.logger.info("consumer: aborted by user")
            self.abort()
        except Exception as ex:
            self.logger.fatal("consumer: fatal exception: %s\n%s",
                              ex, traceback.format_exc())
            self.abort()
        finally:
            try:
                self.consumer.close()
            except Exception as ex:
                self.logger.warning("consumer: close failed: %s", ex)
            self.consumer_status()

    # -- lifecycle ----------------------------------------------------------
    def abort(self):
        """Stop both loops (from a thread that hit a fatal error)."""
        self.run = False
        self.stop_event.set()

    def request_stop(self):
        """Stop both loops and unblock the consumer's in-flight poll.

        Safe to call from a signal handler: ``wakeup()`` is a non-blocking FFI
        call. The producer has no ``wakeup()``, which is why ``stop_event``
        exists — its pacing sleep waits on that event rather than sleeping.

        The ``wakeup()`` is issued at most once per process. Each call arms the
        token again and therefore aborts one more blocking operation, and
        shutdown routinely delivers two signals (a Ctrl-C reaching the process
        group plus run.sh's own SIGTERM). Java's ``wakeup()`` is a flag and is
        idempotent while one is pending; this restores that.
        """
        self.run = False
        self.stop_event.set()
        if self._wakeup_sent.is_set():
            return
        self._wakeup_sent.set()
        try:
            self.consumer.wakeup()
        except Exception:
            pass

    def terminate(self):
        """Terminate producer and consumer."""
        self.logger.info("Terminating (ran for %.0fs)", time.time() - self.start_time)
        self.request_stop()

        self.producer_thread.join()
        self.consumer_thread.join()

        try:
            self.producer.close()
        except Exception as ex:
            self.logger.warning("producer: close failed: %s", ex)

        # Final resource usage and metrics window.
        self.get_rusage()
        self.metrics.measurement_end_ms = int(time.time() * 1000)
        self.metrics.stop_collecting()
        self.metrics.write_final()
        self.metrics.close()
        self.final_report()

    def final_report(self):
        """One-line verdict: only gaps are a hard failure."""
        with self._lock:
            produced, delivered = self.producer_msgid, self.dr_cnt
            consumed, dups, missed = self.msg_cnt, self.msg_dup_cnt, self.msg_miss_cnt
            errors = self.dr_err_cnt + self.msg_err_cnt + self.consumer_err_cnt
        self.logger.info(
            "SUMMARY variant=%s testid=%s topic=%s produced=%d delivered=%d "
            "consumed=%d duplicates=%d missed=%d errors=%d rebalances=%d "
            "disconnects=%d coordinator_moves=%d verdict=%s",
            self.variant, self.testid, self.topic, produced, delivered,
            consumed, dups, missed, errors, self.rebalance_cnt,
            self.disconnect_cnt, self.coordinator_move_cnt,
            "FAIL (message loss)" if missed else "PASS")

    # -- resource usage -----------------------------------------------------
    def calc_rusage_deltas(self, curr, prev, elapsed):
        """Calculate deltas between previous and current resource usage."""
        user_cpu = ((curr.ru_utime - prev.ru_utime) / elapsed) * 100.0
        self.set_gauge("cpu.user", user_cpu)

        sys_cpu = ((curr.ru_stime - prev.ru_stime) / elapsed) * 100.0
        self.set_gauge("cpu.system", sys_cpu)

        # ru_maxrss is KiB on Linux and bytes on macOS; the Python soak assumes
        # KiB, and the soak runs on Linux.
        max_rss = curr.ru_maxrss / 1024.0
        self.set_gauge("memory.rss.max", max_rss)

        self.logger.info("User CPU: %.1f%%, System CPU: %.1f%%, MaxRSS %.3f MiB",
                         user_cpu, sys_cpu, max_rss)

    def get_rusage(self):
        """Get resource usage and calculate CPU load, etc."""
        ru = resource.getrusage(resource.RUSAGE_SELF)
        now = time.time()

        if self.last_rusage is not None:
            self.calc_rusage_deltas(ru, self.last_rusage, now - self.last_rusage_time)

        self.last_rusage = ru
        self.last_rusage_time = now

        rss = float(self.proc.memory_info().rss) / (1024.0 * 1024.0)
        self.set_gauge("memory.rss", rss)
        if self.baseline_rss_mib is not None:
            # Growth since the client was constructed: separates Rust/
            # extension-side growth from the interpreter's own footprint.
            self.set_gauge("memory.rss.delta", rss - self.baseline_rss_mib)

        with self._lock:
            outstanding = self.outstanding
        self.set_gauge("producer.outq", outstanding)


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description='Kafka Rust client soak test')
    parser.add_argument('-i', dest='testid', type=str, required=True,
                        help='Test id')
    parser.add_argument('-b', dest='brokers', type=str, default=None,
                        help='Bootstrap servers')
    parser.add_argument('-t', dest='topic', type=str, required=True,
                        help='Topic to use')
    parser.add_argument('-r', dest='rate', type=float, default=80,
                        help='Message produce rate per second (default: 80)')
    parser.add_argument('-f', dest='conffile', type=argparse.FileType('r'),
                        help='Configuration file (configprop=value format)')
    parser.add_argument('--variant', dest='variant', type=str,
                        default=os.environ.get('SOAK_VARIANT', 'unspecified'),
                        help='Profile name, emitted as the "variant" metric tag')
    parser.add_argument('--payload-size', dest='payload_size', type=int, default=50,
                        help='Target serialized record size in bytes (default: 50). '
                             'Replaces the Python soak\'s --perf flag; the high '
                             'throughput profiles use 10240.')
    parser.add_argument('--partitions', dest='partitions', type=int, default=2,
                        help='Partitions to create the topic with (default: 2)')
    parser.add_argument('--replication-factor', dest='replication_factor', type=int,
                        default=-1,
                        help='Replication factor for topic creation '
                             '(default: -1, broker default)')
    parser.add_argument('--recreate-topic', dest='recreate_topic',
                        action='store_true', default=False,
                        help='Delete and re-create the topic at startup. '
                             'Destructive: never use with run.sh, which restarts '
                             'the client in a loop.')
    parser.add_argument('--metrics-file', dest='metrics_file', type=str, default=None,
                        help='JSONL metrics output path (default: '
                             'soak-metrics-<variant>-<testid>.jsonl)')
    parser.add_argument('--metrics-interval', dest='metrics_interval', type=float,
                        default=10.0,
                        help='Seconds between metrics windows (default: 10)')
    parser.add_argument('--commit-interval', dest='commit_interval', type=float,
                        default=5.0,
                        help='Seconds between synchronous offset commits (default: 5)')
    parser.add_argument('--poll-timeout', dest='poll_timeout', type=float, default=1.0,
                        help='Consumer poll timeout in SECONDS (default: 1.0)')
    parser.add_argument('--stall-threshold', dest='stall_threshold', type=float,
                        default=10.0,
                        help='Seconds without records before reporting a stall, '
                             'and from which consumer.recovery_ms is measured '
                             '(default: 10)')
    parser.add_argument('--max-send-attempts', dest='max_send_attempts', type=int,
                        default=10,
                        help='Attempts before abandoning a record whose send() '
                             'raises a retriable error (default: 10)')
    parser.add_argument('--runtime-seconds', dest='runtime_seconds', type=float,
                        default=0.0,
                        help='Exit after this many seconds (default: 0, run forever)')
    parser.add_argument('--shutdown-timeout', dest='shutdown_timeout', type=float,
                        default=60.0,
                        help='Hard-exit if shutdown takes longer than this, in '
                             'seconds (default: 60). flush()/close() are '
                             'uninterruptible in this binding.')
    parser.add_argument('--log-level', dest='log_level', type=str, default='INFO',
                        help='DEBUG, INFO, WARNING, ERROR (default: INFO)')
    return parser


def main(argv=None):
    args = build_arg_parser().parse_args(argv)

    if args.rate <= 0:
        raise SystemExit("-r must be greater than zero")
    if args.metrics_file is None:
        args.metrics_file = "soak-metrics-{}-{}.jsonl".format(args.variant, args.testid)

    SoakRecord.configure_padding(args.payload_size)

    conf = {}
    if args.conffile is not None:
        conf = parse_config_file(args.conffile)
        args.conffile.close()

    if args.brokers is not None:
        # Overwrite any brokers from the configuration file.
        conf['bootstrap.servers'] = args.brokers

    try:
        soak = SoakClient(args, conf)
    except ValueError as ex:
        # Configuration rejected at startup (an unknown key, or a malformed
        # config line). A traceback adds nothing here.
        print("soakclient: configuration error: {}".format(ex), file=sys.stderr)
        return 2

    shutdown_started = threading.Event()
    exited = threading.Event()

    def watchdog():
        """Hard-exit if shutdown wedges.

        The producer has no ``wakeup()``: a ``send()`` parked on backpressure,
        ``flush()`` and ``close()`` are all uninterruptible. Four soaks share
        one box, so a wedged shutdown must not need a human.
        """
        shutdown_started.wait()
        if not exited.wait(args.shutdown_timeout):
            os.write(sys.stderr.fileno(),
                     b"Shutdown watchdog expired, hard-exiting\n")
            os._exit(2)

    threading.Thread(target=watchdog, name="watchdog", daemon=True).start()

    def signal_handler(signum, frame):
        # print() in a signal handler can raise "reentrant call"; write(2)
        # cannot.
        os.write(sys.stdout.fileno(),
                 b"Termination signal received, shutting down...\n")
        shutdown_started.set()
        soak.request_stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    deadline = (soak.start_time + args.runtime_seconds
                if args.runtime_seconds > 0 else None)
    try:
        while soak.run:
            wait_for = 10.0
            if deadline is not None:
                wait_for = min(wait_for, deadline - time.time())
                if wait_for <= 0:
                    soak.logger.info("Runtime limit of %.0fs reached",
                                     args.runtime_seconds)
                    break
            soak.stop_event.wait(wait_for)
            soak.get_rusage()
        else:
            soak.logger.info("Soak client aborted")
    except KeyboardInterrupt:
        soak.logger.info("Interrupted by user")
    except Exception as ex:
        soak.logger.error("Fatal exception %s\n%s", ex, traceback.format_exc())

    shutdown_started.set()
    soak.terminate()
    exited.set()
    return 1 if soak.msg_miss_cnt else 0


if __name__ == '__main__':
    sys.exit(main())
