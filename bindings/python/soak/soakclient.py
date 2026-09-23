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
#
# Exit codes (a contract with run.sh, which keys its restart policy off them):
#   0  clean shutdown
#   1  message loss detected — the headline failure this soak exists to catch
#   2  fatal: config rejected, bindings missing, authentication failed.
#      Restarting cannot fix it, so the supervisor must NOT loop.
#   3  transient startup failure (broker unreachable) — worth retrying
#   4  consumer wedged: poll() failed past its bound. A restart re-authenticates
#      and re-joins the group, so the supervisor should retry (bounded).

import argparse
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
import tracemalloc
import types
from collections import defaultdict

import psutil

# The metric primitives live in soak_metrics.py, this directory's own copy of the
# performance harness's helpers. Copied rather than imported so a two-week run
# cannot be broken by a perf-test refactor, and so the soak's needs (append mode,
# a promptly-stoppable collector) do not distort code the perf tests depend on.
# Read that file's header before changing the JSONL record schema.
from soak_metrics import Bucket, LatencyBucket, Metrics

# Process RSS immediately after imports, before any client exists. The plan asks
# for two baselines — this one and a second after client construction — because a
# Python process's RSS is CPython + its GC + the C extension + Rust, and the
# difference between the two is the only startup-time split available.
RSS_AFTER_IMPORTS_MIB = psutil.Process(os.getpid()).memory_info().rss / (1024.0 * 1024.0)


# The bindings are imported lazily, by _bindings() below — NOT at module scope.
# `_confluentkafka` is a compiled C extension that only builds on Linux
# (`_confluentkafka.c` includes <threads.h>, C11 threads, which macOS does not
# ship), so a module-scope import would make this file unimportable on a Mac and
# take the pure-logic unit tests (payload parsing, duplicate/gap accounting,
# config validation) down with it. Those tests need no Kafka client, and must be
# runnable wherever development happens.

_SOAK_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# Accepted client configuration keys.
#
# The Rust client only *warns* on an unknown configuration key
# (src/producer/producer_config.rs, src/consumer/consumer_config.rs, both end
# their new() match with a `warn!("Unknown ... key")` arm), so a
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

# Identity of the thing being soaked: the Rust client driven through its Python
# bindings, as distinct from a future native-Rust soak (which would use `rust`)
# and from the librdkafka soak (`python`).
#
# The requested spelling was `rust(python)`, and it is NOT used verbatim, on
# purpose. Prometheus metric names must match `[a-zA-Z_:][a-zA-Z0-9_:]*`, and the
# OTLP->Prometheus translation replaces every invalid character with `_`. So
# `kafka.client.soak.rust(python).producer.send` would arrive as
# `kafka_client_soak_rust_python__producer_send` — note the DOUBLE underscore
# left by the two parentheses, which is easy to typo in a dashboard query and
# impossible to guess. `rust_python` survives the translation unchanged.
#
# One constant, used for both the metric prefix and the host tag, so changing the
# token later is a one-line edit.
SOAK_CLIENT_TOKEN = "rust_python"

METRIC_PFX = "kafka.client.soak.{}.".format(SOAK_CLIENT_TOKEN)

# ---------------------------------------------------------------------------
# Exit codes. run.sh keys its restart policy off these, so they are a contract:
# only EXIT_FATAL means "restarting will never help".
# ---------------------------------------------------------------------------
EXIT_OK = 0
EXIT_MESSAGE_LOSS = 1        # ran fine, but detected a gap — the headline failure
EXIT_FATAL = 2               # config rejected, bindings missing, auth failed
EXIT_TRANSIENT_STARTUP = 3   # broker unreachable at startup — worth retrying
EXIT_CONSUMER_WEDGED = 4     # poll() failed past its bound — a restart re-joins

#: Consecutive non-retriable poll failures before the consumer is declared
#: wedged. Deliberately >1: every *client-side* error (Timeout, Wakeup,
#: IllegalState) reports UnknownServerError, which
#: `Errors::is_retriable()` (src/common/protocol/errors.rs:393) excludes, so one
#: non-retriable poll error is a routine timeout during a broker roll — exactly
#: what a run against the rolled cluster must survive — not a permanent failure.
NON_RETRIABLE_POLL_FAILURE_LIMIT = 3


class FatalStartupError(RuntimeError):
    """A startup failure a restart cannot fix (bad credentials, no authorization).

    Mapped to EXIT_FATAL so the supervisor stops instead of crash-looping.
    """


class TransientStartupError(RuntimeError):
    """A startup failure that may clear on its own (broker unreachable).

    Mapped to EXIT_TRANSIENT_STARTUP so the supervisor retries, bounded by its
    own consecutive-rapid-failure limit.
    """


_BINDINGS = None


def _bindings():
    """Import the Rust client's Python bindings, once, on first use.

    Returns a namespace with the types the soak constructs: ``KafkaProducer``,
    ``KafkaConsumer``, ``ProducerRecord``, ``TopicPartition``,
    ``OffsetAndMetadata``, plus ``KafkaError`` / ``RecordMetadata`` for callers
    that want the types themselves.

    The bindings install *flat* top-level modules (`producer`, `consumer`);
    there is no package, so nothing in this directory may be named producer.py
    or consumer.py.

    Raises ``RuntimeError`` with an actionable message rather than letting a bare
    ``ModuleNotFoundError`` escape — the overwhelmingly likely cause is that
    build.sh has not been run, or that this is not Linux.
    """
    global _BINDINGS
    if _BINDINGS is None:
        try:
            import admin as _admin
            import consumer as _consumer
            import producer as _producer
        except ImportError as ex:
            raise RuntimeError(
                "the Rust client's Python bindings are not importable ({}). Run "
                "bindings/python/soak/build.sh, or activate the venv it created. "
                "Note the bindings only build on Linux: _confluentkafka.c "
                "includes <threads.h> (C11 threads), which macOS does not "
                "ship.".format(ex)) from ex
        _BINDINGS = types.SimpleNamespace(
            KafkaProducer=_producer.KafkaProducer,
            ProducerRecord=_producer.ProducerRecord,
            RecordMetadata=_producer.RecordMetadata,
            KafkaError=_producer.KafkaError,
            KafkaConsumer=_consumer.KafkaConsumer,
            TopicPartition=_consumer.TopicPartition,
            OffsetAndMetadata=_consumer.OffsetAndMetadata,
            AdminClient=_admin.AdminClient,
            NewTopic=_admin.NewTopic,
        )
    return _BINDINGS


def error_message(ex):
    """The message of a client error, without needing its type.

    ``KafkaError`` exposes ``message`` as a property; anything else falls back to
    ``str(ex)``. Read by duck typing so the error-classification helpers stay
    importable — and unit-testable — without the bindings.
    """
    message = getattr(ex, "message", None)
    return message if isinstance(message, str) else str(ex)


def error_code(ex):
    """The protocol error code of a client error, or ``None``.

    ``KafkaError.code`` is an ``int`` property. Duck-typed for the same reason as
    :func:`error_message`.
    """
    code = getattr(ex, "code", None)
    return code if isinstance(code, int) and not isinstance(code, bool) else None


def error_is_retriable(ex):
    """Whether a client error advertises itself as retriable."""
    return getattr(ex, "is_retriable", False) is True


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


def jaas_field(jaas_config, name):
    """Extract one field's value from a Java JAAS login-module string.

    Accepts the spacing and quoting variants a JAAS string legally carries:
    ``username="k"``, ``username = "k"``, ``username='k'`` and bare
    ``username=k``. Returns ``None`` only when the field is genuinely absent.

    This mirrors the Rust parser (``SaslConfig::parse_jaas_option``) byte for
    byte so the client and this validator never disagree: the key is recognized
    only at an **option start** (beginning of string or after whitespace), quoted
    regions are skipped wholesale **honoring backslash escapes**, and the value
    may be double-quoted, single-quoted, or bare. A ``name=`` sequence that lives
    inside another option's quoted value is therefore never mistaken for the
    option (e.g. ``password="username=x" username="right"`` resolves ``username``
    to ``right``, not ``x``) — a regex ``search`` did make that mistake and let a
    missing-username config pass the fast-fail. The returned value is the raw
    inner content between the quotes (escapes are honored for boundary detection
    but not expanded), matching the Rust parser.

    A hand-rolled scanner (rather than a regex) is required precisely because a
    regex cannot skip escaped quotes / balanced quoting the way the Rust lexer
    does; the two must agree so the hard failure in
    :func:`check_admin_credentials` is reliable.
    """
    s = jaas_config
    n = len(s)

    def skip_quoted(i):
        # Advance past a quoted region opening at s[i], honoring '\' escapes.
        quote = s[i]
        i += 1
        while i < n:
            if s[i] == '\\':
                i += 2
                continue
            if s[i] == quote:
                return i + 1
            i += 1
        return i

    i = 0
    while i < n:
        c = s[i]
        if c == '"' or c == "'":
            i = skip_quoted(i)
            continue
        at_option_start = i == 0 or s[i - 1].isspace()
        if at_option_start and s.startswith(name, i):
            j = i + len(name)
            while j < n and s[j].isspace():
                j += 1
            if j < n and s[j] == '=':
                j += 1
                while j < n and s[j].isspace():
                    j += 1
                if j >= n:
                    return None
                quote = s[j]
                if quote == '"' or quote == "'":
                    start = j + 1
                    k = start
                    while k < n:
                        if s[k] == '\\':
                            k += 2
                            continue
                        if s[k] == quote:
                            return s[start:k]
                        k += 1
                    return None  # unterminated quote — malformed
                # Bare value: read until whitespace or ';'.
                k = j
                while k < n and not s[k].isspace() and s[k] != ';':
                    k += 1
                return s[j:k]
        i += 1
    return None


def jaas_credentials(jaas_config):
    """Extract ``(username, password)`` from a Java JAAS login-module string.

    Either element is ``None`` when that field is absent. Used only to *validate*
    the soak's admin credentials at startup (:func:`check_admin_credentials`) —
    the Rust admin client takes ``sasl.jaas.config`` verbatim and parses it
    itself, so these values are never forwarded anywhere.
    """
    return jaas_field(jaas_config, "username"), jaas_field(jaas_config, "password")


def check_admin_credentials(conf):
    """Fail fast at startup when SASL is configured but credentials cannot be
    recovered from ``sasl.jaas.config``.

    The Rust admin client parses ``sasl.jaas.config`` itself, so this does NOT
    translate the config — it only *validates* that a username and password are
    present. Silently proceeding with no usable credentials is the one outcome
    worth refusing: it turns a typo into an authentication error from the broker
    minutes later, or — with an unauthenticated listener — into a soak that runs
    for two weeks against the wrong thing. PLAINTEXT (no SASL) requires no
    credentials and passes untouched.

    A second, symmetric misconfiguration is refused just as fast: SASL
    credentials or a mechanism are configured, but ``security.protocol`` is not a
    SASL protocol (does not contain ``"SASL"``). The Rust client defaults to
    PLAINTEXT, so it would connect *unauthenticated* while the operator believes
    SASL is in force — exactly the "runs for two weeks against the wrong thing"
    failure, in the opposite direction. Naming it at startup beats discovering it
    from an unauthenticated listener later.

    Raises :class:`FatalStartupError` (mapped to ``EXIT_FATAL``: a restart cannot
    fix a bad credential), naming the missing piece.
    """
    protocol = conf.get("security.protocol", "")
    protocol_is_sasl = "SASL" in protocol.upper()
    mechanism = conf.get("sasl.mechanism", "")
    jaas = conf.get("sasl.jaas.config")
    username, password = jaas_credentials(jaas) if jaas else (None, None)
    sasl_creds_present = username is not None or password is not None
    sasl_configured = bool(mechanism) or sasl_creds_present

    # SASL is configured, but the protocol would not actually use it: the client
    # connects as PLAINTEXT (the Rust default) — silently unauthenticated. A
    # restart cannot fix a protocol mismatch, so refuse before the run begins.
    if sasl_configured and not protocol_is_sasl:
        raise FatalStartupError(
            "sasl.mechanism={!r} / sasl.jaas.config {}, but security.protocol={!r} "
            "is not a SASL protocol, so the client would connect WITHOUT SASL "
            "(PLAINTEXT, the Rust default) — silently unauthenticated. Set "
            "security.protocol to a SASL protocol (e.g. SASL_SSL or "
            "SASL_PLAINTEXT). Restarting will not fix this.".format(
                mechanism,
                "has credentials" if sasl_creds_present else "has no credentials",
                protocol))

    sasl_expected = protocol_is_sasl or bool(mechanism)
    if not sasl_expected:
        return

    if username is not None and password is not None:
        return

    missing = [
        name
        for name, value in (("username", username), ("password", password))
        if value is None
    ]
    raise FatalStartupError(
        "security.protocol={!r} / sasl.mechanism={!r} require credentials, but "
        "sasl.jaas.config {}: could not extract {}. Expected Java JAAS form: "
        "sasl.jaas.config=org.apache.kafka.common.security.plain."
        "PlainLoginModule required username=\"KEY\" password=\"SECRET\"; "
        "Restarting will not fix this.".format(
            conf.get("security.protocol", ""), conf.get("sasl.mechanism", ""),
            "is not set" if not jaas else "is set but unparseable",
            " and ".join(missing)))


class LastValueGauges(object):
    """The most recent observation per (metric, tag-set), retained.

    The reference soak buffers gauge observations in a list and *clears* it in
    the observable-gauge callback. That works only for gauges written on every
    interval: an event-driven gauge is exported once and then, on the next
    collection, the callback yields nothing — and a series that yields nothing
    is not reported as "unchanged", it **disappears from the backend entirely**.

    Observed against a real collector: `consumer.assignment_size` was absent
    from the export despite the assignment having changed at startup. That and
    `consumer.recovery_ms` are precisely the two metrics a run against the
    rolled cluster exists to produce.

    So the last value is retained and re-yielded on every subsequent collection,
    which is ordinary gauge semantics. Retention is **per tag-set**, not per
    metric name: `consumer.e2e_latency{partition=0}` and `{partition=1}` are
    distinct series and must not overwrite each other.

    Bounded: keys are (metric, tag-set) where tags are partitions, error codes
    and the fixed base tags — the same bounded cardinality as the JSONL
    counters. This also removes the previous unbounded-growth failure mode,
    where a misconfigured exporter that never collected let the buffer list grow
    at ~160 appends/s inside the process being watched for leaks.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._latest = {}

    @staticmethod
    def _key(tags):
        return tuple(sorted(tags.items()))

    def record(self, name, value, tags):
        with self._lock:
            self._latest.setdefault(name, {})[self._key(tags)] = (value, dict(tags))

    def snapshot(self, name):
        """`[(value, tags), ...]` — every retained series for this metric."""
        with self._lock:
            return list(self._latest.get(name, {}).values())

    def series_count(self, name):
        with self._lock:
            return len(self._latest.get(name, {}))


class _OtelSink(object):
    """OpenTelemetry counters/gauges, mirroring soakclient.py's instruments.

    Built only by :meth:`create`, which returns ``None`` — loudly — unless a
    real, exporting SDK pipeline could be established. Getting that wrong is
    worse than having no telemetry at all: the previous version called
    ``get_meter(...)`` with no ``MeterProvider`` installed, which returns a
    **no-op meter** that silently discards every measurement, while the startup
    line still said "otel on". Confirmed against a real collector: 30+ minutes
    with ``OTEL_METRICS_EXPORTER=otlp``, counters flat, no errors logged.

    This class is thread-safe. ``SoakMetrics`` calls into it from four threads —
    the producer, the consumer, the C extension's delivery-report thread and the
    main thread's rusage sampling — and the SDK collects from a fifth. One lock
    covers instrument creation (a check-then-set that could otherwise register a
    duplicate instrument, which the SDK drops, silently losing a series); the
    gauge store has its own.
    """

    #: Meter/instrumentation-scope name, as it appears in the export.
    SCOPE = "confluent.kafka.soak.rust"

    def __init__(self, meter_provider, base_tags, owns_provider):
        from opentelemetry import metrics as otel_metrics

        self._api = otel_metrics
        self._provider = meter_provider
        self._owns_provider = owns_provider
        self._meter = meter_provider.get_meter(self.SCOPE)
        self._base_tags = dict(base_tags)
        self._lock = threading.Lock()
        self._counters = {}
        self._gauges = {}
        self._gauge_cbs = {}
        self._gauge_values = LastValueGauges()

    # -- construction -------------------------------------------------------
    @staticmethod
    def _requested_exporters():
        """`OTEL_METRICS_EXPORTER` as a list; empty when telemetry is off."""
        raw = os.environ.get("OTEL_METRICS_EXPORTER", "").strip()
        if not raw or raw.lower() == "none":
            return []
        return [name.strip().lower() for name in raw.split(",") if name.strip()]

    @staticmethod
    def _existing_real_provider():
        """An already-installed *SDK* MeterProvider, or None.

        `opentelemetry-instrument` installs one before this process's code runs;
        double-installing ours on top would double-report every measurement. The
        isinstance check against the SDK class is the only reliable test — the
        API's own default is a proxy/no-op provider that looks the same.
        """
        try:
            from opentelemetry import metrics as otel_metrics
            from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
        except ImportError:
            return None
        provider = otel_metrics.get_meter_provider()
        return provider if isinstance(provider, SdkMeterProvider) else None

    @classmethod
    def _build_exporter(cls, name):
        """One metric exporter by `OTEL_METRICS_EXPORTER` name, or raise."""
        if name == "console":
            from opentelemetry.sdk.metrics.export import ConsoleMetricExporter
            return ConsoleMetricExporter()
        if name != "otlp":
            raise ValueError(
                "unsupported OTEL_METRICS_EXPORTER {!r} (supported: otlp, "
                "console, none)".format(name))

        # Protocol per the spec's env vars; the metrics-specific one wins.
        protocol = (os.environ.get("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
                    or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL")
                    or "grpc").strip().lower()
        errors = []
        # Try the requested protocol first, then the other: which exporter
        # package is installed varies, and failing over is better than a soak
        # with no telemetry.
        order = ([("http/protobuf", "http"), ("grpc", "grpc")]
                 if protocol.startswith("http")
                 else [("grpc", "grpc"), ("http/protobuf", "http")])
        for label, kind in order:
            try:
                if kind == "grpc":
                    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter \
                        import OTLPMetricExporter
                else:
                    from opentelemetry.exporter.otlp.proto.http.metric_exporter \
                        import OTLPMetricExporter
                # The exporter reads OTEL_EXPORTER_OTLP_ENDPOINT / _HEADERS /
                # _CERTIFICATE / _INSECURE itself; do not second-guess it.
                return OTLPMetricExporter()
            except Exception as ex:  # ImportError, or a bad endpoint/cert
                errors.append("{}: {}".format(label, ex))
        raise RuntimeError(
            "no usable OTLP metric exporter (tried {}). Install "
            "opentelemetry-exporter-otlp.".format("; ".join(errors)))

    @classmethod
    def create(cls, base_tags, logger):
        """Return a working sink, or None with the reason logged.

        Never raises: telemetry must not be able to stop the soak.
        """
        requested = cls._requested_exporters()
        if not requested:
            logger.info("telemetry: OTEL_METRICS_EXPORTER is unset or 'none'; "
                        "metrics go to the JSONL file only")
            return None

        existing = cls._existing_real_provider()
        if existing is not None:
            logger.info("telemetry: reusing the MeterProvider already installed "
                        "in this process (%s) — not installing a second one",
                        type(existing).__name__)
            try:
                return cls(existing, base_tags, owns_provider=False)
            except Exception as ex:
                logger.warning("telemetry: DISABLED — could not attach to the "
                               "existing MeterProvider: %s. Metrics go to the "
                               "JSONL file only.", ex)
                return None

        try:
            from opentelemetry import metrics as otel_metrics
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.sdk.resources import Resource
        except ImportError as ex:
            logger.warning(
                "telemetry: DISABLED — OTEL_METRICS_EXPORTER=%s was requested but "
                "the OpenTelemetry SDK is not installed (%s). Install "
                "opentelemetry-sdk and opentelemetry-exporter-otlp. Metrics go to "
                "the JSONL file only.", ",".join(requested), ex)
            return None

        try:
            readers = []
            for name in requested:
                exporter = cls._build_exporter(name)
                # PeriodicExportingMetricReader honours OTEL_METRIC_EXPORT_INTERVAL
                # itself; passing it explicitly keeps the value in the log.
                interval_ms = int(os.environ.get("OTEL_METRIC_EXPORT_INTERVAL",
                                                 "60000"))
                readers.append(PeriodicExportingMetricReader(
                    exporter, export_interval_millis=interval_ms))

            # Resource.create() merges OTEL_RESOURCE_ATTRIBUTES and
            # OTEL_SERVICE_NAME; only default service.name if unset.
            attributes = {}
            if not os.environ.get("OTEL_SERVICE_NAME"):
                attributes["service.name"] = "kafka-client-soak-rust"
            resource = Resource.create(attributes)

            provider = MeterProvider(metric_readers=readers, resource=resource)
            otel_metrics.set_meter_provider(provider)
            sink = cls(provider, base_tags, owns_provider=True)
        except Exception as ex:
            logger.warning(
                "telemetry: DISABLED — could not build the %s exporter pipeline: "
                "%s. Metrics go to the JSONL file only.",
                ",".join(requested), ex)
            return None

        logger.info(
            "telemetry: OTLP pipeline installed (exporters=%s, interval=%sms, "
            "endpoint=%s, scope=%s)", ",".join(requested),
            os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "60000"),
            os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
            or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "<sdk default>"),
            cls.SCOPE)
        return sink

    #: Cap on the final flush. An unreachable collector otherwise retries for
    #: the SDK's default 10 s per call and stretches every shutdown — measured
    #: at ~19 s against a dead endpoint, which eats into the shutdown watchdog.
    SHUTDOWN_TIMEOUT_MS = 5000

    def shutdown(self):
        """Flush and stop the pipeline, if this sink installed it.

        Never raises and never blocks indefinitely: telemetry must not be able
        to wedge the soak's shutdown.
        """
        if not self._owns_provider:
            return
        for step, action in (("force_flush", self._provider.force_flush),
                             ("shutdown", self._provider.shutdown)):
            try:
                action(timeout_millis=self.SHUTDOWN_TIMEOUT_MS)
            except Exception as ex:
                # Best-effort: a collector that is down at shutdown must not
                # turn a clean exit into a hang or a traceback.
                logging.getLogger('soakclient').debug(
                    "telemetry: %s during shutdown: %s", step, ex)

    # -- instruments --------------------------------------------------------
    def incr_counter(self, full_name, incrval, tags):
        merged = dict(tags)
        merged.update(self._base_tags)
        with self._lock:
            counter = self._counters.get(full_name)
            if counter is None:
                counter = self._meter.create_counter(
                    full_name, description=full_name)
                self._counters[full_name] = counter
        counter.add(incrval, merged)

    def set_gauge(self, full_name, val, tags):
        merged = dict(tags)
        merged.update(self._base_tags)
        self._gauge_values.record(full_name, val, merged)

        with self._lock:
            if full_name in self._gauges:
                return

            def cb(_options, _name=full_name):
                # Snapshot under the store's lock, then yield outside it: a
                # generator holding a lock across yields would keep it for as
                # long as the SDK takes to consume.
                for value, tags_ in self._gauge_values.snapshot(_name):
                    yield self._api.Observation(value, tags_)

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
    run is analysed from, and it is what makes the soak runnable whether or not
    the OTLP pipeline is up. An OTEL pipeline is driven *in addition* when
    ``OTEL_METRICS_EXPORTER`` requests one and it could actually be built; see
    :meth:`_OtelSink.create`, which logs loudly and falls back to JSONL rather
    than silently exporting into a no-op meter.
    """

    #: Gauges recorded through the 1 ms histogram rather than a plain bucket.
    LATENCY_GAUGES = frozenset([
        "producer.latency",
        "consumer.e2e_latency",
        "consumer.recovery_ms",
    ])

    #: Gauges EXPORTED in seconds while still being RECORDED in milliseconds.
    #:
    #: The reference soak reports both of these as a float in seconds, so
    #: dashboards built against `kafka.client.soak.python.*` read ours on the
    #: same scale. The conversion has to happen on the export path and nowhere
    #: else: `LatencyBucket` buckets are 1 ms wide, so handing it seconds sends
    #: every sample to `int(0.016) == 0` and reports p50/p90/p99/p999 as zero —
    #: silently destroying the JSONL, which is the artifact a two-week run is
    #: actually analysed from.
    #:
    #: `consumer.recovery_ms` is deliberately absent: its name asserts
    #: milliseconds and it has no reference-soak counterpart to match.
    SECONDS_ON_EXPORT = frozenset([
        "producer.latency",
        "consumer.e2e_latency",
    ])

    def __init__(self, path, base_tags, logger, prefix=METRIC_PFX):
        # Append: run.sh restarts the client repeatedly and each restart must
        # add to the series, not truncate it.
        super().__init__(path=path, mode="a")
        self._lock = threading.Lock()
        self._prefix = prefix
        self._base_tags = dict(base_tags)
        self._counters = {}
        self._counters_at_last_rollover = {}
        self._gauges = {}
        # Returns None (having logged why) unless a real exporting pipeline was
        # established, so `otel_enabled` cannot claim telemetry that is not
        # happening.
        self._otel = _OtelSink.create(base_tags, logger)

    @property
    def otel_enabled(self):
        return self._otel is not None

    def close(self):
        """Flush and stop the OTLP pipeline, then close the JSONL file."""
        if self._otel is not None:
            self._otel.shutdown()
        super().close()

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
            exported = (val / 1000.0 if metric_name in self.SECONDS_ON_EXPORT
                        else val)
            self._otel.set_gauge(self._prefix + metric_name, exported, tags or {})

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
        self.max_poll_failures = int(args.max_poll_failures)
        self.run = True
        self.start_time = time.time()

        # Set by the signal handler; also used instead of time.sleep() so the
        # producer thread's pacing sleep aborts immediately on shutdown.
        self.stop_event = threading.Event()
        # Guards against issuing more than one consumer.wakeup() — see
        # request_stop().
        self._wakeup_sent = threading.Event()

        self.logger = self._make_logger(args.log_level)

        # tracemalloc separates Python-side allocation from the C extension's and
        # Rust's, which plain RSS cannot: RSS is CPython + GC + extension + Rust
        # in one number. Frame depth 1 keeps the overhead to a per-allocation
        # bookkeeping entry with no traceback capture, which is all the
        # `memory.tracemalloc` gauge needs. Started before the clients exist so
        # their allocations are counted.
        self.tracemalloc_enabled = not args.no_tracemalloc
        if self.tracemalloc_enabled:
            tracemalloc.start(1)

        # Resolve the bindings HERE — before the topic is created and long
        # before any thread starts — so a missing/unbuilt binding fails at
        # startup with _bindings()' actionable message instead of surfacing on
        # the first produce. The classes are then held as attributes so the
        # per-record paths never re-enter the accessor.
        bindings = _bindings()
        self._ProducerRecord = bindings.ProducerRecord
        self._TopicPartition = bindings.TopicPartition
        self._OffsetAndMetadata = bindings.OffsetAndMetadata

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
        #: Set when a loop gives up (see consumer_run's poll bound). Reported in
        #: the SUMMARY line and turned into a non-zero exit code by main().
        self.fatal_reason = None

        self.last_rusage = None
        self.last_rusage_time = None
        self.baseline_rss_mib = None
        self.proc = psutil.Process(os.getpid())

        # A unique metrics host id so several soaks on one box stay distinct.
        # Same token as METRIC_PFX — see SOAK_CLIENT_TOKEN.
        hostname = os.environ.get("HOSTNAME") or socket.gethostname()
        self.hostname = "{}-{}-{}".format(SOAK_CLIENT_TOKEN, hostname, self.topic)

        base_tags = {"host": self.hostname, "testid": self.testid,
                     "variant": self.variant}
        # SoakMetrics logs precisely what happened to the telemetry pipeline —
        # installed, reused, or disabled with the reason — so this line reports
        # the outcome rather than an intention.
        self.metrics = SoakMetrics(path=args.metrics_file, base_tags=base_tags,
                                   logger=self.logger)
        self.logger.info("SoakClient id %s (variant %s, rate %g msg/s, "
                         "payload %d B, metrics -> %s, otel %s)",
                         self.hostname, self.variant, self.rate,
                         args.payload_size, args.metrics_file,
                         "exporting" if self.metrics.otel_enabled
                         else "not exporting (JSONL only)")
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

        # Topic creation goes through this repo's Rust-backed AdminClient
        # (bindings/python/admin.py), which takes the same Java-style config
        # namespace as the producer/consumer — no librdkafka translation. The
        # security.protocol fix on this branch makes it work against SASL/SSL
        # clusters (Confluent Cloud). Create-if-absent by default: run.sh
        # restarts the client repeatedly and a restart must never discard the
        # topic.
        admin_conf = stringify_config(aconf)
        # Fail fast — before the long run begins — if SASL is configured but no
        # credentials can be recovered from sasl.jaas.config, so a typo surfaces
        # here rather than as a broker auth error minutes later (or a two-week
        # soak against the wrong thing). PLAINTEXT passes untouched.
        check_admin_credentials(admin_conf)
        if admin_conf.get("sasl.mechanism"):
            # Log the mechanism — never the secret — so a SASL misconfiguration
            # is visible in the first lines of the log rather than as an
            # authorization error later. The Rust admin parses sasl.jaas.config
            # itself, so there is no username/password to unpack here.
            self.logger.info("admin: SASL %s (security.protocol=%s)",
                             admin_conf.get("sasl.mechanism"),
                             admin_conf.get("security.protocol", "?"))
        self.create_topic(self.topic, admin_conf,
                          partitions=args.partitions,
                          replication_factor=args.replication_factor,
                          recreate=args.recreate_topic)

        # Both clients are constructed before either thread starts, so a
        # failure here cannot leave the producer running with no consumer.
        self.logger.info("producer: using client.id %s", pconf['client.id'])
        self.producer = bindings.KafkaProducer(pconf)

        self.logger.info("consumer: using group.id %s", cconf.get('group.id'))
        self.consumer = bindings.KafkaConsumer(cconf)

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
        # measured from here; the difference between the two baselines is what
        # the client itself costs at startup.
        self.baseline_rss_mib = self.proc.memory_info().rss / (1024.0 * 1024.0)
        self.logger.info(
            "baseline RSS: %.3f MiB after imports, %.3f MiB after client "
            "construction (client cost %.3f MiB)",
            RSS_AFTER_IMPORTS_MIB, self.baseline_rss_mib,
            self.baseline_rss_mib - RSS_AFTER_IMPORTS_MIB)

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
        # build.sh warns at build time; repeat it here, because this log is what
        # someone reads in two weeks when the manifest file is long gone.
        if manifest.get("traceable") is False or manifest.get("git_sha") in (
                None, "", "unknown"):
            self.logger.warning(
                "BUILD IS NOT TRACEABLE TO A COMMIT: the manifest carries no git "
                "sha (source_root=%s, label=%r). Rebuild with "
                "build.sh --src <dir> --sha <commit> if this run's results need "
                "to be attributed to code.",
                manifest.get("source_root", "?"), manifest.get("build_label", ""))

    def create_topic(self, topic, aconf, partitions, replication_factor, recreate):
        """Create the topic if it doesn't already exist.

        Uses this repo's Rust-backed AdminClient (bindings/python/admin.py),
        which accepts the same Java-style config namespace as the
        producer/consumer (bootstrap.servers, security.protocol, sasl.mechanism,
        sasl.jaas.config, ssl.*). ``recreate`` (``--recreate-topic``)
        additionally deletes it first, which is destructive and must never be
        the default — run.sh restarts the soak in a loop.
        """
        bindings = _bindings()
        KafkaError = bindings.KafkaError

        if recreate:
            from soak_metrics import recreate_topic
            self.logger.warning("--recreate-topic: deleting and re-creating %s", topic)
            recreate_topic(aconf, topic, partitions=partitions)
            return

        # `Errors` wire codes (src/common/protocol/errors.rs). The admin binding
        # returns ``create_topics([...])`` -> ``{name: TopicMetadataAndConfig |
        # KafkaError}`` (each value is already the per-key result or a
        # ``KafkaError`` whose ``.code`` is the wire code), and the whole call
        # itself raises a ``KafkaError`` on a call-level failure.
        # Authentication / authorization failures will never clear by retrying;
        # everything else here (broker unreachable, metadata timeout) might.
        TOPIC_ALREADY_EXISTS = 36
        auth_codes = {
            29,   # TOPIC_AUTHORIZATION_FAILED
            30,   # GROUP_AUTHORIZATION_FAILED
            31,   # CLUSTER_AUTHORIZATION_FAILED
            33,   # UNSUPPORTED_SASL_MECHANISM
            34,   # ILLEGAL_SASL_STATE
            58,   # SASL_AUTHENTICATION_FAILED
        }

        admin = bindings.AdminClient(dict(aconf))
        try:
            new_topic = bindings.NewTopic(topic, num_partitions=partitions,
                                          replication_factor=replication_factor)

            def _handle_create_error(_topic, ex):
                code = ex.code
                if code == TOPIC_ALREADY_EXISTS:
                    self.logger.info("Topic %s already exists: good", _topic)
                elif code in auth_codes:
                    raise FatalStartupError(
                        "authentication/authorization failed creating topic {!r}: {}. "
                        "Check sasl.jaas.config (username/password) and the API "
                        "key's ACLs. Restarting will not fix this.".format(
                            _topic, ex.message)) from ex
                else:
                    raise TransientStartupError(
                        "could not create or verify topic {!r}: {}. If the cluster "
                        "is reachable this may clear on retry.".format(
                            _topic, ex.message)) from ex

            try:
                results = admin.create_topics([new_topic])
            except KafkaError as ex:
                # The old admin interface raises a whole-call KafkaError from
                # the call itself; classify it the same way as a per-key error.
                _handle_create_error(topic, ex)
            else:
                for _topic, result in results.items():
                    if isinstance(result, KafkaError):
                        _handle_create_error(_topic, result)
                    else:
                        self.logger.info("Created topic %s (partitions=%d, rf=%d)",
                                         _topic, partitions, replication_factor)
        finally:
            admin.close()

    # -- instrumentation ----------------------------------------------------
    def incr_counter(self, metric_name, incrval, tags=None):
        """Increment metric counter by incrval."""
        self.metrics.incr_counter(metric_name, incrval, tags)

    def set_gauge(self, metric_name, val, tags=None):
        """Set metric gauge to val."""
        self.metrics.set_gauge(metric_name, val, tags)

    # -- producer -----------------------------------------------------------
    def _record_delivery(self, metadata, latency_ms):
        """Account for one successful delivery report.

        ``metadata`` is a ``RecordMetadata``, whose accessors are *methods*
        (``offset()``, ``topic()``, ``partition()``, ``timestamp()``) — unlike
        ``ConsumerRecord``'s, which are properties.
        """
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
        except Exception as ex:
            # A failed send raises KafkaError through the future; a cancelled
            # one raises CancelledError. Both are counted the same way, and the
            # error's code/message are read by duck typing (error_code /
            # error_message) rather than by isinstance, so this file needs no
            # module-scope KafkaError.
            with self._lock:
                self.dr_err_cnt += 1
            code = error_code(ex)
            self.logger.warning("producer: delivery failed: %s [code %s]",
                                error_message(ex), code)
            self.incr_counter("producer.drerr", 1)
            self.incr_counter("producer.delivery.failure", 1,
                              tags={"err": str(code)})

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
            producer_record = self._ProducerRecord(self.topic, record.serialize())

            with self._lock:
                self.outstanding += 1
            sent_at = time.time()
            try:
                future = self.producer.send(producer_record)
            except Exception as ex:
                with self._lock:
                    self.outstanding -= 1
                if (not error_is_retriable(ex)
                        or txcnt >= self.max_send_attempts):
                    self._count_send_failure(msgid, ex)
                    return
                self.logger.warning("producer: send attempt %d for msgid %d "
                                    "failed (retriable): %s",
                                    txcnt, msgid, error_message(ex))
                self.stop_event.wait(0.1)
                continue

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

        code = error_code(ex)
        message = error_message(ex).lower()
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

        # End-to-end latency from the payload's send time.
        #
        # Recorded in MILLISECONDS and exported in SECONDS. `set_gauge` applies
        # that conversion itself for everything in SoakMetrics.SECONDS_ON_EXPORT,
        # because the value passed here also feeds a 1 ms-resolution
        # LatencyBucket: dividing at this call site sends every sample to
        # `int(0.016) == 0` and reports p50/p90/p99/p999 as zero.
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
            self.incr_counter("consumer.msgdup", duplicates)
        elif missed:
            self.logger.warning(
                "consumer: Lost messages, now at %s [%d] offset %d: "
                "%d message(s) missed (last committed %s)",
                record.topic, record.partition, record.offset, missed,
                self.last_committed)
            with self._lock:
                self.msg_miss_cnt += missed
            self.incr_counter("consumer.missedmsg", missed)

        pending[self._TopicPartition(record.topic, record.partition)] = \
            self._OffsetAndMetadata(record.offset + 1)

    @staticmethod
    def _is_wakeup(ex):
        """Whether an exception is this client's WakeupException equivalent.

        Client-side errors carry no distinct protocol code (KafkaError::Wakeup
        reports UnknownServerError, -1), so the message is the only signal.
        """
        return "wakeup" in error_message(ex).lower()

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
            # KafkaError is an Exception subclass; _classify_error reads its
            # code/message by duck typing (error_code / error_message).
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

    def _poll_failure_is_terminal(self, ex, consecutive):
        """Whether a run of consecutive ``poll()`` failures should end the run.

        Unbounded retrying is the worst outcome for an unattended soak: the
        process stays alive, the producer keeps producing, nothing is consumed,
        and the SUMMARY line that adjudicates message loss is never reached. So
        the storm is bounded and escalates to ``abort()``, which lets
        ``terminate()`` print the verdict and exits non-zero; run.sh then
        restarts (re-authenticating and re-joining the group), and its own
        rapid-failure bound catches a permanent condition.

        Two tiers, because ``is_retriable`` cannot be trusted as a
        never-going-to-work signal here: every *client-side* error — Timeout,
        Wakeup, IllegalState — reports ``UnknownServerError``, which
        ``Errors::is_retriable()`` excludes. A single non-retriable poll error is
        therefore routine during a broker roll, so the non-retriable tier is a
        small count rather than one.
        """
        retriable = error_is_retriable(ex)
        limit = (self.max_poll_failures if retriable
                 else min(NON_RETRIABLE_POLL_FAILURE_LIMIT, self.max_poll_failures))
        if consecutive < limit:
            return False
        self.fatal_reason = (
            "consumer poll failed {} consecutive times ({}retriable), last error: "
            "{}".format(consecutive, "" if retriable else "non-", error_message(ex)))
        self.logger.fatal("consumer: %s — aborting so the run is restarted rather "
                          "than silently consuming nothing", self.fatal_reason)
        self.abort()
        return True

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
        poll_failures = 0

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
                poll_failures += 1
                if self._poll_failure_is_terminal(ex, poll_failures):
                    break
                self.stop_event.wait(0.5)
                continue

            poll_failures = 0
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
        if missed:
            verdict = "FAIL (message loss)"
        elif self.fatal_reason:
            verdict = "ABORTED ({})".format(self.fatal_reason)
        else:
            verdict = "PASS"
        self.logger.info(
            "SUMMARY variant=%s testid=%s topic=%s produced=%d delivered=%d "
            "consumed=%d duplicates=%d missed=%d errors=%d rebalances=%d "
            "disconnects=%d coordinator_moves=%d verdict=%s",
            self.variant, self.testid, self.topic, produced, delivered,
            consumed, dups, missed, errors, self.rebalance_cnt,
            self.disconnect_cnt, self.coordinator_move_cnt, verdict)

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

        # Re-emitted every window even though they never change: a gauge sampled
        # once reports average=0 in every later window (an empty bucket averages
        # to 0), which reads as "the baseline is 0 MiB" rather than "no sample
        # here". Two constants per 10 s is cheaper than that ambiguity, and it
        # lets a dashboard compute rss - baseline in any window.
        self.set_gauge("memory.rss.baseline_imports", RSS_AFTER_IMPORTS_MIB)
        if self.baseline_rss_mib is not None:
            self.set_gauge("memory.rss.baseline_constructed", self.baseline_rss_mib)

        if self.baseline_rss_mib is not None:
            # Growth since the client was constructed: separates Rust/
            # extension-side growth from the interpreter's own footprint.
            self.set_gauge("memory.rss.delta", rss - self.baseline_rss_mib)

        if self.tracemalloc_enabled:
            # Python-side heap only. Read against memory.rss: RSS climbing while
            # this stays flat points at the C extension or Rust; both climbing
            # together points at Python. That split is the soak's headline
            # question, and RSS alone cannot answer it.
            traced, traced_peak = tracemalloc.get_traced_memory()
            self.set_gauge("memory.tracemalloc", traced / (1024.0 * 1024.0))
            self.set_gauge("memory.tracemalloc.peak",
                           traced_peak / (1024.0 * 1024.0))

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
                        help='Variant label, emitted as the "variant" metric tag')
    parser.add_argument('--payload-size', dest='payload_size', type=int, default=50,
                        help='Target serialized record size in bytes (default: 50). '
                             'Replaces the Python soak\'s --perf flag; HI=true '
                             'in run.sh sets 10240.')
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
                        default=5.0,
                        help='Seconds without records before reporting a stall, '
                             'and from which consumer.recovery_ms is measured '
                             '(default: 5). Was 10, applied as a per-variant '
                             'override for the rolled cluster; a 5s stall is '
                             'worth flagging anywhere, so it is now the uniform '
                             'default.')
    parser.add_argument('--max-send-attempts', dest='max_send_attempts', type=int,
                        default=10,
                        help='Attempts before abandoning a record whose send() '
                             'raises a retriable error (default: 10)')
    parser.add_argument('--max-poll-failures', dest='max_poll_failures', type=int,
                        default=20,
                        help='Consecutive consumer poll() failures before the run '
                             'is aborted so the supervisor restarts it (default: '
                             '20, i.e. ~10s of failures). Non-retriable errors '
                             'abort after {} instead.'.format(
                                 NON_RETRIABLE_POLL_FAILURE_LIMIT))
    parser.add_argument('--no-tracemalloc', dest='no_tracemalloc',
                        action='store_true', default=False,
                        help='Disable tracemalloc sampling. On by default: it is '
                             'the only way to separate Python-side heap growth '
                             'from the C extension\'s and Rust\'s, which plain RSS '
                             'cannot.')
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


def _shutdown_watchdog(shutdown_started, exited, timeout_seconds):
    """Hard-exit if shutdown wedges.

    The producer has no ``wakeup()``: a ``send()`` parked on backpressure,
    ``flush()`` and ``close()`` are all uninterruptible. Four soaks share
    one box, so a wedged shutdown must not need a human.

    Exits ``EXIT_CONSUMER_WEDGED``, not ``EXIT_FATAL``: a shutdown that
    wedges on backpressure during a broker roll is transient, not a
    permanent failure, and run.sh only restarts non-fatal codes.
    """
    shutdown_started.wait()
    if not exited.wait(timeout_seconds):
        os.write(sys.stderr.fileno(),
                 b"Shutdown watchdog expired, hard-exiting\n")
        os._exit(EXIT_CONSUMER_WEDGED)


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

    # Startup failures are classified so the supervisor can tell "will never
    # work" from "try again": an unhandled traceback here would exit 1 and be
    # restarted forever. FatalStartupError / TransientStartupError subclass
    # RuntimeError, so they must be caught before it.
    try:
        soak = SoakClient(args, conf)
    except ValueError as ex:
        # Configuration rejected at startup (an unknown key, a malformed config
        # line, unusable SASL credentials). A traceback adds nothing here.
        print("soakclient: configuration error: {}".format(ex), file=sys.stderr)
        return EXIT_FATAL
    except FatalStartupError as ex:
        print("soakclient: fatal startup error: {}".format(ex), file=sys.stderr)
        return EXIT_FATAL
    except TransientStartupError as ex:
        print("soakclient: transient startup error: {}".format(ex), file=sys.stderr)
        return EXIT_TRANSIENT_STARTUP
    except RuntimeError as ex:
        # Startup precondition failed — in practice the bindings not being
        # importable (see _bindings()). Raised before the topic is created and
        # before any thread starts, so there is nothing to unwind.
        print("soakclient: startup error: {}".format(ex), file=sys.stderr)
        return EXIT_FATAL
    except Exception as ex:
        # Unclassified: keep the traceback, since this is the case nobody has
        # diagnosed yet, but exit "transient" so the supervisor retries a few
        # times under its rapid-failure bound rather than stopping dead on
        # something that might be a flapping broker.
        print("soakclient: unexpected startup failure: {}\n{}".format(
            ex, traceback.format_exc()), file=sys.stderr)
        return EXIT_TRANSIENT_STARTUP

    shutdown_started = threading.Event()
    exited = threading.Event()

    threading.Thread(
        target=_shutdown_watchdog,
        args=(shutdown_started, exited, args.shutdown_timeout),
        name="watchdog", daemon=True).start()

    def signal_handler(signum, frame):
        # print() in a signal handler can raise "reentrant call"; write(2)
        # cannot.
        os.write(sys.stdout.fileno(),
                 b"Termination signal received, shutting down...\n")
        shutdown_started.set()
        soak.request_stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Initial resource-usage sample, as the reference does before its loop
    # (soakclient.py:954): without it the first metrics window carries no
    # memory/CPU gauges at all, because the sampler and the window roll on the
    # same cadence.
    soak.get_rusage()

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

    # Message loss outranks everything else — it is the result the soak exists to
    # report. A wedged loop exits distinctly so the supervisor can restart it and
    # a human can see why in one line.
    if soak.msg_miss_cnt:
        return EXIT_MESSAGE_LOSS
    if soak.fatal_reason:
        return EXIT_CONSUMER_WEDGED
    return EXIT_OK


if __name__ == '__main__':
    sys.exit(main())
