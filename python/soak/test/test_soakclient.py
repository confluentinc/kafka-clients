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

"""Unit tests for the soak client's pure logic.

Covers the parts a two-week run depends on being right and that no broker can
verify for us: the payload round-trip, the duplicate/gap accounting, and the
startup configuration validation that stands in for the client's silent
acceptance of unknown keys.

None of this needs the Rust client's Python bindings, and this file must stay
runnable where they cannot even be built — they are a C extension that requires
Linux (`_confluentkafka.c` includes <threads.h>). `soakclient` imports them
lazily for exactly that reason; `test_module_imports_without_bindings` guards it.
"""

import io
import logging
import os
import re
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from soakclient import (
    CONSUMER_CONFIG_KEYS,
    EXIT_CONSUMER_WEDGED,
    EXIT_FATAL,
    EXIT_MESSAGE_LOSS,
    EXIT_OK,
    EXIT_TRANSIENT_STARTUP,
    NON_RETRIABLE_POLL_FAILURE_LIMIT,
    PRODUCER_CONFIG_KEYS,
    FatalStartupError,
    HighWaterMarks,
    LastValueGauges,
    SoakClient,
    SoakMetrics,
    SoakRecord,
    check_admin_credentials,
    error_code,
    error_is_retriable,
    error_message,
    filter_config,
    jaas_credentials,
    parse_config_file,
    route_shared_config,
    stringify_config,
    validate_config,
)


# ---------------------------------------------------------------------------
# SoakRecord
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_padding():
    """SoakRecord's target size is class state; keep tests independent."""
    SoakRecord.configure_padding(0)
    yield
    SoakRecord.configure_padding(0)


def test_serialize_deserialize_round_trip():
    original = SoakRecord(42, send_time_ms=1765432100123, txcnt=3)
    parsed = SoakRecord.deserialize(original.serialize())
    assert parsed.msgid == 42
    assert parsed.send_time_ms == 1765432100123
    assert parsed.txcnt == 3


def test_serialize_defaults_send_time_to_now():
    record = SoakRecord(0)
    assert record.send_time_ms > 1_700_000_000_000
    assert record.txcnt == 1


def test_round_trip_survives_padding():
    SoakRecord.configure_padding(10240)
    payload = SoakRecord(7, send_time_ms=1765432100123, txcnt=1).serialize()
    assert len(payload) == 10240
    parsed = SoakRecord.deserialize(payload)
    assert (parsed.msgid, parsed.send_time_ms, parsed.txcnt) == (7, 1765432100123, 1)


def test_padding_target_is_exact_across_msgid_widths():
    SoakRecord.configure_padding(50)
    for msgid in (0, 9, 10, 999999, 12345678901234):
        assert len(SoakRecord(msgid, send_time_ms=1765432100123).serialize()) == 50


def test_no_padding_when_prefix_exceeds_target():
    SoakRecord.configure_padding(4)
    payload = SoakRecord(123456, send_time_ms=1765432100123, txcnt=1).serialize()
    assert payload == b"123456|1765432100123|1|"


def test_padding_grows_source_buffer_beyond_default():
    SoakRecord.configure_padding(100000)
    assert len(SoakRecord(1, send_time_ms=1765432100123).serialize()) == 100000


def test_deserialize_accepts_memoryview():
    # `record.value` is a zero-copy memoryview over the fetch batch.
    payload = SoakRecord(5, send_time_ms=1765432100123).serialize()
    parsed = SoakRecord.deserialize(memoryview(payload))
    assert parsed.msgid == 5


@pytest.mark.parametrize("payload", [
    b"",                        # empty
    b"not-a-soak-record",       # no separators at all
    b"1|2",                     # too few fields
    b"1|2|3",                   # missing the trailing separator
    b"x|2|3|pad",               # non-numeric msgid
    b"1|y|3|pad",               # non-numeric send time
    b"1|2|z|pad",               # non-numeric txcnt
    b"\xff\xfe|2|3|pad",        # non-ascii msgid
    b"|2|3|pad",                # empty msgid
])
def test_deserialize_rejects_malformed_payload(payload):
    with pytest.raises(ValueError):
        SoakRecord.deserialize(payload)


def test_deserialize_rejects_none():
    with pytest.raises(ValueError):
        SoakRecord.deserialize(None)


# ---------------------------------------------------------------------------
# High-water-mark accounting
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("name,marks,offset,expected", [
    # (description, prior observations, next offset, (duplicates, missed))
    ("first message at offset 0", [], 0, (0, 0)),
    # The first offset seen only establishes the mark: consumption starts at an
    # arbitrary committed position, which is not a gap.
    ("first message mid-partition", [], 5, (0, 0)),
    ("in-order", [5], 6, (0, 0)),
    ("in-order after a long run", [1, 2, 3, 4], 5, (0, 0)),
    ("duplicate of the mark itself", [10], 10, (1, 0)),
    ("replay of three", [10], 8, (3, 0)),
    ("gap of one", [10], 12, (0, 1)),
    ("gap of four", [10], 15, (0, 4)),
])
def test_hwmark_accounting(name, marks, offset, expected):
    hwmarks = HighWaterMarks()
    for mark in marks:
        hwmarks.observe("t-0", mark)
    assert hwmarks.observe("t-0", offset) == expected, name


@pytest.mark.parametrize("name,marks,offset", [
    ("a real duplicate of offset 0 is missed", [0], 0),
    ("a real gap after offset 0 is missed", [0], 7),
])
def test_hwmark_offset_zero_blind_spot_is_a_known_limitation(name, marks, offset):
    """Documents a defect inherited from the reference — it does NOT bless it.

    `_marks` is a `defaultdict(int)`, so "never seen" and "last seen at offset 0"
    are the same state, and the `if hw > 0` guard therefore skips the check for
    exactly one transition per partition. A duplicate of offset 0, or a gap
    immediately after it, is not counted.

    This is a faithful port of soakclient.py:324 and is kept for fidelity; the
    blast radius is ~2 records, at the start of the first run against a fresh
    topic only (every later restart begins at a committed non-zero offset). The
    correct fix is a `None`/-1 sentinel, deferred as Issue 10 in
    COMMENTS.DONE.0.md.

    If someone changes the sentinel, this test SHOULD fail — that is the point of
    its name.
    """
    hwmarks = HighWaterMarks()
    for mark in marks:
        hwmarks.observe("t-0", mark)
    assert hwmarks.observe("t-0", offset) == (0, 0), name


def test_hwmark_is_per_partition():
    hwmarks = HighWaterMarks()
    hwmarks.observe("t-0", 100)
    hwmarks.observe("t-1", 5)
    # A low offset on a different partition is not a duplicate.
    assert hwmarks.observe("t-1", 6) == (0, 0)
    assert hwmarks.observe("t-0", 101) == (0, 0)
    assert len(hwmarks) == 2


def test_hwmark_replay_counts_each_record_once():
    """A rebalance replay must report the replayed records exactly once.

    The mark is set unconditionally (not advance-only): the single jump back
    accounts for the whole replay, and the records that follow are in order.
    """
    hwmarks = HighWaterMarks()
    for offset in range(0, 201):
        hwmarks.observe("t-0", offset)

    duplicates, missed = hwmarks.observe("t-0", 100)
    assert (duplicates, missed) == (101, 0)

    total_extra = 0
    for offset in range(101, 201):
        dup, miss = hwmarks.observe("t-0", offset)
        total_extra += dup + miss
    assert total_extra == 0

    assert hwmarks.observe("t-0", 201) == (0, 0)


def test_hwmark_gap_then_recovery():
    hwmarks = HighWaterMarks()
    hwmarks.observe("t-0", 1)
    assert hwmarks.observe("t-0", 10) == (0, 8)
    assert hwmarks.observe("t-0", 11) == (0, 0)


def _bare_soak_client():
    """A `SoakClient` with none of `__init__`'s bindings/network setup --
    just the attributes `_consume_record` touches. Lets the counter-accounting
    logic be exercised without a real consumer, producer, or config.
    """
    client = object.__new__(SoakClient)
    client.logger = logging.getLogger("test_soakclient")
    client._lock = threading.Lock()
    client.disprate = 10 ** 9  # never hit the periodic info-log branch
    client.last_committed = None
    client.msg_err_cnt = 0
    client.msg_cnt = 0
    client.msg_dup_cnt = 0
    client.msg_miss_cnt = 0
    client.metrics = MagicMock()
    client.incr_counter = MagicMock()
    client.set_gauge = MagicMock()
    client._TopicPartition = lambda topic, partition: (topic, partition)
    client._OffsetAndMetadata = lambda offset: offset
    return client


def _fetched_record(offset, msgid=1):
    soak_record = SoakRecord(msgid=msgid, send_time_ms=int(time.time() * 1000))
    value = soak_record.serialize()
    return SimpleNamespace(topic="t", partition=0, offset=offset, value=value,
                           serialized_value_size=len(value))


def test_consume_record_reports_the_actual_duplicate_count():
    """`incr_counter("consumer.msgdup", ...)` must receive the real duplicate
    count, not a flat 1 -- the SUMMARY log line and `msg_dup_cnt` already
    report the real count, so a dashboard built on the OTEL/JSONL counter
    alone would otherwise read as far fewer duplicates than actually occurred.
    """
    client = _bare_soak_client()
    hwmarks = HighWaterMarks()
    pending = {}

    client._consume_record(_fetched_record(10), hwmarks, pending)
    # Replay of three: high-water mark is 10 (wants 11), offset 8 is 3 behind.
    client._consume_record(_fetched_record(8), hwmarks, pending)

    assert client.msg_dup_cnt == 3
    client.incr_counter.assert_any_call("consumer.msgdup", 3)


def test_consume_record_reports_the_actual_missed_count():
    """Same defect class as the duplicate counter, for `consumer.missedmsg`."""
    client = _bare_soak_client()
    hwmarks = HighWaterMarks()
    pending = {}

    client._consume_record(_fetched_record(1), hwmarks, pending)
    # Gap of eight: high-water mark is 1 (wants 2), offset 10 skips 8 records.
    client._consume_record(_fetched_record(10), hwmarks, pending)

    assert client.msg_miss_cnt == 8
    client.incr_counter.assert_any_call("consumer.missedmsg", 8)


# ---------------------------------------------------------------------------
# Configuration handling
# ---------------------------------------------------------------------------
def test_validate_config_accepts_known_keys():
    conf = {
        "bootstrap.servers": "localhost:9092",
        "linger.ms": "5",
        "compression.type": "lz4",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.jaas.config": "org.apache...PlainLoginModule required;",
    }
    validate_config(conf, PRODUCER_CONFIG_KEYS, "producer")


def test_validate_config_accepts_ssl_prefix():
    # Both Rust configs route every `ssl.*` key to apply_ssl_config_key().
    validate_config({"ssl.truststore.location": "/x"}, PRODUCER_CONFIG_KEYS, "producer")
    validate_config({"ssl.keystore.password": "x"}, CONSUMER_CONFIG_KEYS, "consumer")


def test_validate_config_rejects_unknown_key_and_names_it():
    with pytest.raises(ValueError) as exc:
        validate_config({"sasl.username": "u"}, PRODUCER_CONFIG_KEYS, "producer")
    message = str(exc.value)
    assert "sasl.username" in message
    assert "producer" in message


def test_validate_config_rejects_every_unknown_key():
    with pytest.raises(ValueError) as exc:
        validate_config({"bootstrap.servers": "x", "sasl.password": "p",
                         "lingerr.ms": "5"},
                        PRODUCER_CONFIG_KEYS, "producer")
    message = str(exc.value)
    assert "sasl.password" in message
    assert "lingerr.ms" in message
    assert "bootstrap.servers" not in message.split("Accepted")[0]


def test_validate_config_rejects_consumer_typo():
    with pytest.raises(ValueError) as exc:
        validate_config({"group.protocoll": "consumer"},
                        CONSUMER_CONFIG_KEYS, "consumer")
    assert "group.protocoll" in str(exc.value)


def test_group_protocol_is_an_accepted_consumer_key():
    # Every profile must set group.protocol=consumer: the client defaults to
    # `classic` and construction fails for it (KIP-848 only).
    assert "group.protocol" in CONSUMER_CONFIG_KEYS


def test_route_shared_config_moves_other_clients_keys():
    conf = {"bootstrap.servers": "x", "group.id": "g", "linger.ms": "5"}
    kept, routed = route_shared_config(conf, PRODUCER_CONFIG_KEYS,
                                       CONSUMER_CONFIG_KEYS)
    assert routed == ["group.id"]
    assert kept == {"bootstrap.servers": "x", "linger.ms": "5"}
    validate_config(kept, PRODUCER_CONFIG_KEYS, "producer")


def test_route_shared_config_keeps_keys_unknown_to_both():
    kept, routed = route_shared_config({"nonsense.key": "1"},
                                       PRODUCER_CONFIG_KEYS, CONSUMER_CONFIG_KEYS)
    assert routed == []
    assert kept == {"nonsense.key": "1"}
    with pytest.raises(ValueError):
        validate_config(kept, PRODUCER_CONFIG_KEYS, "producer")


def test_filter_config_strips_prefix_and_drops_others():
    conf = {
        "bootstrap.servers": "x",
        "producer.linger.ms": "5",
        "consumer.group.id": "g",
        "admin.client.id": "a",
    }
    pconf = filter_config(conf, ["consumer.", "admin."], "producer.")
    assert pconf == {"bootstrap.servers": "x", "linger.ms": "5"}

    cconf = filter_config(conf, ["producer.", "admin."], "consumer.")
    assert cconf == {"bootstrap.servers": "x", "group.id": "g"}


def test_stringify_config_coerces_every_value():
    # The C extension raises TypeError on a non-string configuration value.
    conf = stringify_config({"linger.ms": 5, "enable.idempotence": True,
                             "client.id": "soak"})
    assert conf == {"linger.ms": "5", "enable.idempotence": "True",
                    "client.id": "soak"}
    assert all(isinstance(v, str) for v in conf.values())


def test_parse_config_file_skips_comments_and_blanks():
    text = ("# a comment\n"
            "\n"
            "bootstrap.servers=host:9092\n"
            "sasl.jaas.config=org.apache.kafka.common.security.plain."
            "PlainLoginModule required username=\"u\" password=\"p=q\";\n")
    conf = parse_config_file(io.StringIO(text))
    assert conf["bootstrap.servers"] == "host:9092"
    # Values may contain '=' — only the first one separates.
    assert conf["sasl.jaas.config"].endswith('password="p=q";')


def test_parse_config_file_rejects_a_line_without_a_separator():
    with pytest.raises(ValueError):
        parse_config_file(io.StringIO("bootstrap.servers\n"))


# ---------------------------------------------------------------------------
# JAAS credential parsing + startup credential check
# ---------------------------------------------------------------------------
def test_jaas_credentials_extraction():
    jaas = ("org.apache.kafka.common.security.plain.PlainLoginModule required \n\t"
            'username="API_KEY" \n\tpassword="API_SECRET";')
    assert jaas_credentials(jaas) == ("API_KEY", "API_SECRET")


@pytest.mark.parametrize("jaas", [
    # The spacing and quoting variants a JAAS string legally carries. The
    # original hand-rolled scanner accepted only the first of these and returned
    # (None, None) for the rest, which produced an admin client with no
    # credentials at all.
    'PlainLoginModule required username="k" password="s";',
    'PlainLoginModule required username = "k" password = "s";',
    "PlainLoginModule required username='k' password='s';",
    'PlainLoginModule required username=k password=s;',
    'PlainLoginModule required\n\tusername="k"\n\tpassword="s";',
    'PlainLoginModule required password="s" username="k";',
    'PlainLoginModule required serviceName="kafka" username="k" password="s";',
])
def test_jaas_credentials_tolerates_spacing_and_quoting(jaas):
    assert jaas_credentials(jaas) == ("k", "s")


def test_jaas_credentials_does_not_match_a_longer_field_name():
    # `serviceName=` must not satisfy a search for `name=`, and a dotted or
    # prefixed key must not satisfy `username=`.
    jaas = 'PlainLoginModule required myusername="wrong" username="right" password="s";'
    assert jaas_credentials(jaas)[0] == "right"


def test_jaas_credentials_does_not_match_a_key_inside_a_quoted_value():
    # A `username=` sequence embedded in another option's quoted value must not be
    # picked up (a regex `search` did pick it up). The real `username` option must
    # win, matching the Rust parser, so the fast-fail cannot be fooled into
    # passing a config whose real username is absent.
    jaas = 'PlainLoginModule required password="username=x" username="right";'
    assert jaas_credentials(jaas) == ("right", "username=x")


def test_jaas_credentials_handles_escaped_quote_in_value():
    # An escaped quote inside a value must not terminate it or misalign later
    # options; `username` after it still resolves. Value is returned raw (escapes
    # not expanded), matching the Rust parser.
    jaas = 'PlainLoginModule required password="a\\"b" username="right";'
    assert jaas_credentials(jaas) == ("right", 'a\\"b')


def test_jaas_credentials_absent():
    assert jaas_credentials("org.apache...PlainLoginModule required;") == (None, None)


def test_jaas_credentials_preserves_special_characters_in_the_secret():
    # Confluent Cloud secrets contain '+', '/' and '='.
    jaas = 'PlainLoginModule required username="K/EY+1" password="a+b/c=d==";'
    assert jaas_credentials(jaas) == ("K/EY+1", "a+b/c=d==")


def test_check_admin_credentials_refuses_sasl_without_credentials():
    """A SASL config whose credentials cannot be recovered must fail loudly at
    startup — the one outcome worth refusing turns a typo into an opaque broker
    auth error minutes later, or a two-week soak against the wrong thing."""
    with pytest.raises(FatalStartupError) as exc:
        check_admin_credentials({
            "bootstrap.servers": "host:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            # `sasl.jaas.config` is absent, so no credentials can be recovered.
        })
    message = str(exc.value)
    assert "sasl.jaas.config" in message
    assert "username and password" in message


def test_check_admin_credentials_refuses_a_half_parsed_jaas():
    with pytest.raises(FatalStartupError) as exc:
        check_admin_credentials({
            "bootstrap.servers": "host:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": 'PlainLoginModule required username="k";',
        })
    assert "password" in str(exc.value)


def test_check_admin_credentials_triggers_on_sasl_mechanism_alone():
    # SASL implied by sasl.mechanism even when security.protocol is unset.
    with pytest.raises(FatalStartupError):
        check_admin_credentials({
            "bootstrap.servers": "host:9092",
            "sasl.mechanism": "PLAIN",
        })


def test_check_admin_credentials_refuses_sasl_mechanism_with_plaintext_protocol():
    """SASL is configured (mechanism set) but security.protocol=PLAINTEXT, so the
    client would connect unauthenticated — refuse, even though credentials happen
    to be present, because the protocol would not use them."""
    with pytest.raises(FatalStartupError) as exc:
        check_admin_credentials({
            "bootstrap.servers": "host:9092",
            "security.protocol": "PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": ('org.apache.kafka.common.security.plain.'
                                 'PlainLoginModule required username="u" '
                                 'password="p";'),
        })
    message = str(exc.value)
    assert "not a SASL protocol" in message
    assert "WITHOUT SASL" in message


def test_check_admin_credentials_refuses_sasl_mechanism_without_protocol():
    """sasl.mechanism set but security.protocol absent: the Rust default is
    PLAINTEXT, so this too would connect unauthenticated and must be refused."""
    with pytest.raises(FatalStartupError) as exc:
        check_admin_credentials({
            "bootstrap.servers": "host:9092",
            "sasl.mechanism": "PLAIN",
        })
    assert "not a SASL protocol" in str(exc.value)


def test_check_admin_credentials_refuses_jaas_creds_with_plaintext_protocol():
    """Credentials via sasl.jaas.config but security.protocol=PLAINTEXT (no
    mechanism): still a SASL-configured-but-PLAINTEXT mismatch to refuse."""
    with pytest.raises(FatalStartupError) as exc:
        check_admin_credentials({
            "bootstrap.servers": "host:9092",
            "security.protocol": "PLAINTEXT",
            "sasl.jaas.config": ('org.apache.kafka.common.security.plain.'
                                 'PlainLoginModule required username="u" '
                                 'password="p";'),
        })
    assert "not a SASL protocol" in str(exc.value)


def test_check_admin_credentials_passes_with_valid_jaas():
    # A jaas with an extractable user+pass is accepted (no exception).
    check_admin_credentials({
        "bootstrap.servers": "host:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.jaas.config": ('org.apache.kafka.common.security.plain.'
                             'PlainLoginModule required username="u" '
                             'password="p";'),
    })


def test_check_admin_credentials_allows_plaintext_without_credentials():
    # No SASL configured: absent credentials are correct, not an error.
    check_admin_credentials({"bootstrap.servers": "host:9092"})


# ---------------------------------------------------------------------------
# Binding decoupling and duck-typed error inspection
# ---------------------------------------------------------------------------
def test_module_imports_without_bindings():
    """`import soakclient` must not require the (Linux-only) bindings.

    The bindings are a compiled C extension; on macOS they cannot be built at
    all. If this file's import of `soakclient` pulled them in, every test here
    would fail to *collect* on a dev machine. Assert the decoupling directly
    rather than trusting that the import above happened to work.
    """
    import soakclient

    assert soakclient.__name__ == "soakclient"
    for module in ("producer", "consumer", "_confluentkafka"):
        assert not hasattr(soakclient, module), \
            f"soakclient must not bind {module} at module scope"


def test_bindings_accessor_reports_a_missing_binding_clearly(monkeypatch):
    """A missing binding must produce an actionable error, not a bare ImportError.

    The absence is simulated rather than inferred from the environment, so the
    failure path is asserted on Linux (where the bindings *are* installed) too.
    A ``None`` entry in ``sys.modules`` makes ``import x`` raise ``ImportError``;
    ``monkeypatch`` undoes both that and the ``_BINDINGS`` cache reset.
    """
    import soakclient

    monkeypatch.setitem(sys.modules, "producer", None)
    monkeypatch.setitem(sys.modules, "consumer", None)
    monkeypatch.setattr(soakclient, "_BINDINGS", None)

    with pytest.raises(RuntimeError) as exc:
        soakclient._bindings()
    message = str(exc.value)
    assert "build.sh" in message
    assert "threads.h" in message


def test_bindings_accessor_caches(monkeypatch):
    """The accessor must resolve once, not on every record.

    `produce_record` / `_consume_record` run per message, so a repeated import
    lookup there would be on the hot path; SoakClient.__init__ resolves the
    classes into attributes and this cache backs that.
    """
    import soakclient

    sentinel = object()
    monkeypatch.setattr(soakclient, "_BINDINGS", sentinel)
    assert soakclient._bindings() is sentinel


class _FakeKafkaError(Exception):
    """Shaped like the binding's KafkaError: code/message/is_retriable are
    properties, not methods."""

    def __init__(self, code, message, is_retriable=False):
        super().__init__(message)
        self._code = code
        self._message = message
        self._is_retriable = is_retriable

    @property
    def code(self):
        return self._code

    @property
    def message(self):
        return self._message

    @property
    def is_retriable(self):
        return self._is_retriable


def test_error_message_prefers_the_message_property():
    assert error_message(_FakeKafkaError(16, "not the coordinator")) == \
        "not the coordinator"


def test_error_message_falls_back_to_str():
    assert error_message(RuntimeError("plain failure")) == "plain failure"


def test_error_code_reads_the_code_property():
    assert error_code(_FakeKafkaError(16, "x")) == 16


@pytest.mark.parametrize("ex", [
    RuntimeError("no code here"),
    ValueError("also none"),
])
def test_error_code_is_none_without_a_code(ex):
    assert error_code(ex) is None


def test_error_code_ignores_a_non_int_code():
    class Weird(Exception):
        code = "16"

    assert error_code(Weird()) is None


def test_error_code_ignores_a_bool_code():
    # bool is a subclass of int; True must not read as error code 1.
    class Weird(Exception):
        code = True

    assert error_code(Weird()) is None


@pytest.mark.parametrize("ex,expected", [
    (_FakeKafkaError(7, "timed out", is_retriable=True), True),
    (_FakeKafkaError(29, "auth failed", is_retriable=False), False),
    (RuntimeError("no such attribute"), False),
])
def test_error_is_retriable(ex, expected):
    assert error_is_retriable(ex) is expected


@pytest.mark.parametrize("message,expected", [
    ("WakeupTrigger fired", True),
    ("wakeup", True),
    ("Timed out waiting for the coordinator", False),
    ("This is not the correct coordinator.", False),
])
def test_is_wakeup_classification(message, expected):
    # KafkaError::Wakeup reports UnknownServerError (-1) like every other
    # client-side error, so the message is the only signal; the commit path at
    # shutdown relies on this to retry rather than report a failure.
    assert SoakClient._is_wakeup(RuntimeError(message)) is expected
    # Same verdict when the message arrives via the property, as it does from
    # the real KafkaError.
    assert SoakClient._is_wakeup(_FakeKafkaError(-1, message)) is expected


# ---------------------------------------------------------------------------
# Poll-failure bound: the escalation that stops an unattended soak from
# consuming nothing for two weeks while looking alive.
# ---------------------------------------------------------------------------
class _StubClient:
    """Just enough of SoakClient to exercise the decision in isolation.

    `_poll_failure_is_terminal` is a pure decision over (error, count, limit); a
    real SoakClient would need a broker. Called as an unbound method with this as
    `self`.
    """

    def __init__(self, max_poll_failures=20):
        self.max_poll_failures = max_poll_failures
        self.fatal_reason = None
        self.aborted = False
        self.logger = logging.getLogger("test-soak-stub")

    def abort(self):
        self.aborted = True


def _terminal(stub, ex, consecutive):
    return SoakClient._poll_failure_is_terminal(stub, ex, consecutive)


@pytest.mark.parametrize("consecutive", [1, 5, 19])
def test_retriable_poll_failures_below_the_bound_keep_going(consecutive):
    stub = _StubClient(max_poll_failures=20)
    ex = _FakeKafkaError(13, "NetworkException", is_retriable=True)
    assert _terminal(stub, ex, consecutive) is False
    assert stub.aborted is False
    assert stub.fatal_reason is None


def test_retriable_poll_failures_at_the_bound_terminate():
    stub = _StubClient(max_poll_failures=20)
    ex = _FakeKafkaError(13, "NetworkException", is_retriable=True)
    assert _terminal(stub, ex, 20) is True
    assert stub.aborted is True
    assert "20 consecutive" in stub.fatal_reason
    assert "non-retriable" not in stub.fatal_reason


@pytest.mark.parametrize("consecutive", range(1, NON_RETRIABLE_POLL_FAILURE_LIMIT))
def test_a_single_non_retriable_poll_failure_is_not_fatal(consecutive):
    """The critical regression guard for the rolling profiles.

    Every *client-side* error — Timeout, Wakeup, IllegalState — reports
    UnknownServerError, which `Errors::is_retriable()` excludes. A routine poll
    timeout during a broker roll therefore looks non-retriable, and escalating on
    the first one would kill the soak precisely when it is supposed to be proving
    it survives.
    """
    stub = _StubClient()
    ex = RuntimeError("Timeout waiting for the coordinator")
    assert _terminal(stub, ex, consecutive) is False
    assert stub.aborted is False


def test_repeated_non_retriable_poll_failures_terminate_sooner():
    stub = _StubClient(max_poll_failures=20)
    ex = _FakeKafkaError(29, "TopicAuthorizationFailed", is_retriable=False)
    assert _terminal(stub, ex, NON_RETRIABLE_POLL_FAILURE_LIMIT) is True
    assert stub.aborted is True
    assert "non-retriable" in stub.fatal_reason
    assert "TopicAuthorizationFailed" in stub.fatal_reason


def test_the_non_retriable_tier_never_exceeds_the_configured_bound():
    # --max-poll-failures 2 must not be *raised* to 3 by the non-retriable tier.
    stub = _StubClient(max_poll_failures=2)
    ex = _FakeKafkaError(29, "auth", is_retriable=False)
    assert _terminal(stub, ex, 2) is True


# ---------------------------------------------------------------------------
# Telemetry: last-value gauge retention, and honest availability reporting
# ---------------------------------------------------------------------------
def test_last_value_gauge_retains_across_collections():
    """An event-driven gauge must keep being reported after its last update.

    The regression this pins: the previous callback yielded the buffered values
    then cleared the buffer, so the *second* collection yielded nothing and the
    series vanished from the backend rather than holding its last value.
    Observed against a real collector as `consumer.assignment_size` missing
    entirely.
    """
    gauges = LastValueGauges()
    gauges.record("consumer.assignment_size", 2, {"host": "h"})

    first = gauges.snapshot("consumer.assignment_size")
    assert first == [(2, {"host": "h"})]

    # Several more collections with no further updates: still reported.
    for _ in range(5):
        assert gauges.snapshot("consumer.assignment_size") == [(2, {"host": "h"})]


def test_last_value_gauge_overwrites_within_a_tag_set():
    gauges = LastValueGauges()
    for value in (1, 2, 3):
        gauges.record("g", value, {"host": "h"})
    assert gauges.snapshot("g") == [(3, {"host": "h"})]
    assert gauges.series_count("g") == 1


def test_last_value_gauge_retains_per_tag_set():
    """Retention is per tag-set: partitions are distinct series."""
    gauges = LastValueGauges()
    gauges.record("consumer.e2e_latency", 10.0, {"partition": "0"})
    gauges.record("consumer.e2e_latency", 20.0, {"partition": "1"})
    gauges.record("consumer.e2e_latency", 11.0, {"partition": "0"})

    assert gauges.series_count("consumer.e2e_latency") == 2
    assert sorted(gauges.snapshot("consumer.e2e_latency"),
                  key=lambda item: item[1]["partition"]) == [
        (11.0, {"partition": "0"}),
        (20.0, {"partition": "1"}),
    ]


def test_last_value_gauge_keeps_metrics_separate():
    gauges = LastValueGauges()
    gauges.record("a", 1, {})
    gauges.record("b", 2, {})
    assert gauges.snapshot("a") == [(1, {})]
    assert gauges.snapshot("b") == [(2, {})]
    assert gauges.snapshot("never-recorded") == []


def test_last_value_gauge_snapshot_is_a_copy():
    # The SDK collects on its own thread; mutating a returned snapshot, or
    # recording during iteration, must not corrupt the store.
    gauges = LastValueGauges()
    gauges.record("g", 1, {"k": "v"})
    snapshot = gauges.snapshot("g")
    snapshot.clear()
    assert gauges.snapshot("g") == [(1, {"k": "v"})]


def test_last_value_gauge_is_thread_safe():
    """Concurrent recorders must not lose or corrupt series.

    Four threads write in production (producer, consumer, the C extension's
    delivery-report thread, the main thread's rusage sampling) while the SDK
    collects from a fifth.
    """
    import threading as _threading

    gauges = LastValueGauges()
    errors = []

    def writer(partition):
        try:
            for i in range(500):
                gauges.record("g", i, {"partition": str(partition)})
        except Exception as ex:  # pragma: no cover - only on a real race
            errors.append(ex)

    def collector():
        try:
            for _ in range(500):
                gauges.snapshot("g")
        except Exception as ex:  # pragma: no cover
            errors.append(ex)

    threads = [_threading.Thread(target=writer, args=(p,)) for p in range(4)]
    threads.append(_threading.Thread(target=collector))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert gauges.series_count("g") == 4
    assert sorted(v for v, _ in gauges.snapshot("g")) == [499, 499, 499, 499]


def test_otel_sink_disabled_when_exporter_not_requested(monkeypatch, caplog):
    """No OTEL_METRICS_EXPORTER means no telemetry — and it must say so."""
    import soakclient

    monkeypatch.delenv("OTEL_METRICS_EXPORTER", raising=False)
    logger = logging.getLogger("test-otel-off")
    with caplog.at_level(logging.INFO, logger="test-otel-off"):
        assert soakclient._OtelSink.create({"host": "h"}, logger) is None
    assert "JSONL" in caplog.text


def test_otel_sink_disabled_and_loud_when_sdk_missing(monkeypatch, caplog):
    """Requested but unbuildable must be a WARNING, never a silent no-op.

    This is the finding: `get_meter()` with no provider returns a no-op meter
    that discards everything, while the client logged "otel on".
    """
    import soakclient

    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "otlp")
    # Simulate the SDK not being installed, deterministically on both platforms.
    for module in ("opentelemetry", "opentelemetry.metrics",
                   "opentelemetry.sdk", "opentelemetry.sdk.metrics"):
        monkeypatch.setitem(sys.modules, module, None)

    logger = logging.getLogger("test-otel-missing")
    with caplog.at_level(logging.WARNING, logger="test-otel-missing"):
        assert soakclient._OtelSink.create({"host": "h"}, logger) is None
    assert "DISABLED" in caplog.text
    assert "opentelemetry-sdk" in caplog.text


@pytest.mark.parametrize("value,expected", [
    ("", []),
    ("none", []),
    ("NONE", []),
    ("otlp", ["otlp"]),
    ("OTLP", ["otlp"]),
    ("otlp,console", ["otlp", "console"]),
    (" otlp , console ", ["otlp", "console"]),
])
def test_requested_exporters_parsing(monkeypatch, value, expected):
    import soakclient

    monkeypatch.setenv("OTEL_METRICS_EXPORTER", value)
    assert soakclient._OtelSink._requested_exporters() == expected


def test_requested_exporters_when_unset(monkeypatch):
    import soakclient

    monkeypatch.delenv("OTEL_METRICS_EXPORTER", raising=False)
    assert soakclient._OtelSink._requested_exporters() == []


# ---------------------------------------------------------------------------
# Exit-code contract shared with run.sh
# ---------------------------------------------------------------------------
def test_exit_codes_are_distinct():
    codes = [EXIT_OK, EXIT_MESSAGE_LOSS, EXIT_FATAL, EXIT_TRANSIENT_STARTUP,
             EXIT_CONSUMER_WEDGED]
    assert len(set(codes)) == len(codes)
    assert EXIT_OK == 0


def test_shutdown_watchdog_hard_exits_with_consumer_wedged_not_fatal(monkeypatch):
    """A wedged shutdown (backpressure during a broker roll) is transient:
    run.sh must see EXIT_CONSUMER_WEDGED, not EXIT_FATAL, or a real broker
    roll gets treated as permanent and the soak never comes back on its own.
    """
    import threading

    import soakclient

    exit_codes = []
    monkeypatch.setattr(soakclient.os, "_exit", lambda code: exit_codes.append(code))

    shutdown_started = threading.Event()
    exited = threading.Event()
    shutdown_started.set()  # shutdown already underway
    # exited is deliberately never set: the shutdown is wedged.
    soakclient._shutdown_watchdog(shutdown_started, exited, timeout_seconds=0.05)

    assert exit_codes == [EXIT_CONSUMER_WEDGED]
    assert EXIT_FATAL not in exit_codes


def test_shutdown_watchdog_does_not_fire_when_shutdown_completes_in_time(monkeypatch):
    import threading

    import soakclient

    exit_codes = []
    monkeypatch.setattr(soakclient.os, "_exit", lambda code: exit_codes.append(code))

    shutdown_started = threading.Event()
    exited = threading.Event()
    shutdown_started.set()
    exited.set()  # shutdown finished well within the timeout
    soakclient._shutdown_watchdog(shutdown_started, exited, timeout_seconds=5.0)

    assert exit_codes == []


def test_run_sh_agrees_on_the_fatal_exit_code():
    """run.sh keys "never restart" off this exact number; drift would silently
    restore the crash loop."""
    run_sh = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "run.sh")
    with open(run_sh) as fh:
        source = fh.read()
    match = re.search(r'^EXIT_FATAL=(\d+)$', source, re.MULTILINE)
    assert match is not None, "run.sh no longer defines EXIT_FATAL"
    assert int(match.group(1)) == EXIT_FATAL


def test_latency_gauges_record_ms_and_export_seconds(tmp_path):
    """`set_gauge` records milliseconds and exports seconds, in one place.

    Guards the SECONDS_ON_EXPORT contract: callers hand `set_gauge`
    milliseconds and it converts on the way out, so the 1 ms-wide
    `LatencyBucket` keeps usable percentiles while dashboards still read
    seconds.

    It does NOT guard the call sites. A caller that divides by 1000 itself
    reaches `set_gauge` with seconds, lands every sample in `int(0.016) == 0`
    and reports p50/p90/p99/p999 as zero — which is exactly what happened
    before this contract existed. Catching that needs a constructed
    SoakClient, so the defence there is the comment at the call site.
    """
    exported = []

    class _CapturingSink(object):
        def set_gauge(self, name, val, tags):
            exported.append((name, val))

        def incr_counter(self, name, incrval, tags):
            pass

        def shutdown(self):
            pass

    metrics = SoakMetrics(path=str(tmp_path / "m.jsonl"),
                          base_tags={}, logger=logging.getLogger("t"))
    metrics._otel = _CapturingSink()
    try:
        # 17 ms, the order of magnitude a healthy soak actually reports.
        metrics.set_gauge("producer.latency", 17.0, tags={"partition": "0"})
        metrics.set_gauge("consumer.e2e_latency", 17.0, tags={"partition": "0"})
        # Not in SECONDS_ON_EXPORT: its name asserts milliseconds.
        metrics.set_gauge("consumer.recovery_ms", 17.0)

        by_name = dict(exported)
        assert by_name[metrics._prefix + "producer.latency"] == 0.017
        assert by_name[metrics._prefix + "consumer.e2e_latency"] == 0.017
        assert by_name[metrics._prefix + "consumer.recovery_ms"] == 17.0

        # The histogram kept milliseconds, so the percentiles are non-zero.
        record = metrics.rollover()
        gauges = record.get("gauges", record)
        for key, bucket in gauges.items():
            if "latency" in key and isinstance(bucket, dict) and "p99" in bucket:
                assert float(bucket["p99"]) > 0, \
                    "{} p99 collapsed to zero: histogram was fed seconds".format(key)
    finally:
        metrics.close()
