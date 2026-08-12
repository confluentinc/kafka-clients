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
    HighWaterMarks,
    SoakClient,
    SoakRecord,
    error_code,
    error_is_retriable,
    error_message,
    filter_config,
    jaas_credentials,
    librdkafka_admin_config,
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


def test_jaas_credentials_absent():
    assert jaas_credentials("org.apache...PlainLoginModule required;") == (None, None)


def test_jaas_credentials_preserves_special_characters_in_the_secret():
    # Confluent Cloud secrets contain '+', '/' and '='.
    jaas = 'PlainLoginModule required username="K/EY+1" password="a+b/c=d==";'
    assert jaas_credentials(jaas) == ("K/EY+1", "a+b/c=d==")


def test_librdkafka_admin_config_refuses_sasl_without_credentials():
    """A SASL config whose credentials cannot be recovered must fail loudly.

    Forwarding no credentials is the one outcome worth refusing: it turns a typo
    into an opaque broker-side authentication error minutes later.
    """
    with pytest.raises(ValueError) as exc:
        librdkafka_admin_config({
            "bootstrap.servers": "host:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            # `sasl.username`/`sasl.password` are not keys of this client, so a
            # config written that way carries no usable credentials.
        })
    message = str(exc.value)
    assert "sasl.jaas.config" in message
    assert "username and password" in message


def test_librdkafka_admin_config_refuses_a_half_parsed_jaas():
    with pytest.raises(ValueError) as exc:
        librdkafka_admin_config({
            "bootstrap.servers": "host:9092",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": 'PlainLoginModule required username="k";',
        })
    assert "password" in str(exc.value)


def test_librdkafka_admin_config_allows_plaintext_without_credentials():
    # No SASL configured: absent credentials are correct, not an error.
    assert librdkafka_admin_config({"bootstrap.servers": "host:9092"}) == {
        "bootstrap.servers": "host:9092"}


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
# Exit-code contract shared with run.sh
# ---------------------------------------------------------------------------
def test_exit_codes_are_distinct():
    codes = [EXIT_OK, EXIT_MESSAGE_LOSS, EXIT_FATAL, EXIT_TRANSIENT_STARTUP,
             EXIT_CONSUMER_WEDGED]
    assert len(set(codes)) == len(codes)
    assert EXIT_OK == 0


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


def test_librdkafka_admin_config_translates_jaas():
    conf = {
        "bootstrap.servers": "host:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.jaas.config": ('org.apache.kafka.common.security.plain.'
                             'PlainLoginModule required username="u" '
                             'password="p";'),
        "linger.ms": "5",
    }
    admin = librdkafka_admin_config(conf)
    assert admin == {
        "bootstrap.servers": "host:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "u",
        "sasl.password": "p",
    }
    # librdkafka errors on unknown keys, so Java-only keys must not leak.
    assert "sasl.jaas.config" not in admin
    assert "linger.ms" not in admin
