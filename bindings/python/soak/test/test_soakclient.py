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
"""

import io

import pytest

from soakclient import (
    CONSUMER_CONFIG_KEYS,
    PRODUCER_CONFIG_KEYS,
    HighWaterMarks,
    SoakClient,
    SoakRecord,
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
    # hw stays 0 until a non-zero offset is seen, mirroring the Python soak's
    # `if hw > 0` guard.
    ("repeat of offset 0 is not counted", [0], 0, (0, 0)),
])
def test_hwmark_accounting(name, marks, offset, expected):
    hwmarks = HighWaterMarks()
    for mark in marks:
        hwmarks.observe("t-0", mark)
    assert hwmarks.observe("t-0", offset) == expected, name


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


def test_jaas_credentials_absent():
    assert jaas_credentials("org.apache...PlainLoginModule required;") == (None, None)


@pytest.mark.parametrize("message,expected", [
    ("WakeupTrigger fired", True),
    ("wakeup", True),
    ("Timed out waiting for the coordinator", False),
    ("This is not the correct coordinator.", False),
])
def test_is_wakeup_classification(message, expected):
    # KafkaError::Wakeup reports UnknownServerError (-1) like every other
    # client-side error, so the message is the only signal; the final commit at
    # shutdown relies on this to retry rather than report a failure.
    assert SoakClient._is_wakeup(RuntimeError(message)) is expected


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
