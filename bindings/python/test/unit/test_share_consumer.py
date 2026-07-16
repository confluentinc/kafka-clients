# Copyright 2025 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test suite for the Python KIP-932 share consumer (MockShareConsumer-driven)."""

import gc

import pytest
from producer import KafkaError
from share_consumer import (
    AcknowledgeType,
    MockShareConsumer,
    TopicIdPartition,
    _make_kafka_error,
    _to_share_ack_offsets,
    _to_share_commit_map,
)

POLL_TIMEOUT = 1.0


def _seed(c, topic="t", partition=0, records=None):
    """Subscribe one topic and enqueue records at offsets 0, 1, 2, ..."""
    c.subscribe([topic])
    for i, (k, v) in enumerate(records or []):
        c.add_record(topic, partition, i, k, v)
    return topic


# -- lifecycle ---------------------------------------------------------------

def test_create_and_close():
    c = MockShareConsumer()
    c.close()
    assert c.closed


def test_close_idempotent():
    c = MockShareConsumer()
    c.close()
    c.close()  # no error


def test_context_manager():
    with MockShareConsumer() as c:
        assert not c.closed
    assert c.closed


def test_poll_after_close_raises():
    c = MockShareConsumer()
    c.close()
    with pytest.raises(RuntimeError) as exc:
        c.poll(POLL_TIMEOUT)
    assert "closed" in str(exc.value)


# -- subscribe / subscription ------------------------------------------------

def test_subscribe_and_subscription():
    with MockShareConsumer() as c:
        c.subscribe(["a", "b"])
        assert c.subscription() == {"a", "b"}


def test_unsubscribe_clears_subscription():
    with MockShareConsumer() as c:
        c.subscribe(["a"])
        assert c.subscription() == {"a"}
        c.unsubscribe()
        assert c.subscription() == set()


def test_add_record_to_unsubscribed_topic_raises():
    with MockShareConsumer() as c:
        with pytest.raises(KafkaError) as exc:
            c.add_record("not-subscribed", 0, 0, b"k", b"v")
        # Error message content is part of the behavioral contract.
        assert "not subscribed" in str(exc.value)


# -- poll --------------------------------------------------------------------

def test_poll_empty_when_no_records():
    with MockShareConsumer() as c:
        c.subscribe(["t"])
        recs = c.poll(POLL_TIMEOUT)
        assert len(recs) == 0
        assert recs.is_empty()
        assert list(recs) == []


def test_poll_key_and_value():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k1", b"v1")])
        recs = c.poll(POLL_TIMEOUT)
        assert len(recs) == 1
        assert not recs.is_empty()
        (r,) = list(recs)
        assert r.topic == "t"
        assert r.partition == 0
        assert r.offset == 0
        assert bytes(r.key) == b"k1"
        assert bytes(r.value) == b"v1"


def test_poll_value_only():
    with MockShareConsumer() as c:
        _seed(c, records=[(None, b"v")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        assert r.key is None
        assert bytes(r.value) == b"v"


def test_poll_multiple_records_offsets():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k0", b"v0"), (b"k1", b"v1"), (b"k2", b"v2")])
        recs = list(c.poll(POLL_TIMEOUT))
        assert [r.offset for r in recs] == [0, 1, 2]
        assert [bytes(r.value) for r in recs] == [b"v0", b"v1", b"v2"]


def test_value_and_key_are_memoryview():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"value")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        assert isinstance(r.value, memoryview)
        assert isinstance(r.key, memoryview)


def test_delivery_count_absent_for_mock():
    # The mock does not report a delivery count; the getter surfaces None.
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"v")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        assert r.delivery_count is None


def test_memoryview_zero_copy_lifetime():
    """A memoryview must keep the underlying batch alive after the
    ConsumerRecords / ConsumerRecord that produced it are dropped."""
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"the-value")])
        recs = c.poll(POLL_TIMEOUT)
        rec = next(iter(recs))
        mv = rec.value
        assert bytes(mv) == b"the-value"
        # Drop every Python-visible owner except the memoryview itself.
        del rec
        del recs
        gc.collect()
        # The exporter still holds the batch alive: bytes remain valid.
        assert bytes(mv) == b"the-value"


# -- acknowledge -------------------------------------------------------------

def test_acknowledge_type_values_match_abi():
    assert int(AcknowledgeType.ACCEPT) == 1
    assert int(AcknowledgeType.RELEASE) == 2
    assert int(AcknowledgeType.REJECT) == 3
    assert int(AcknowledgeType.RENEW) == 4


def test_acknowledge_record_accept():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"v")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        c.acknowledge(r)  # ACCEPT, no error


def test_acknowledge_record_with_type():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"v")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        c.acknowledge(r, AcknowledgeType.RELEASE)  # no error


def test_acknowledge_by_offset():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"v")])
        list(c.poll(POLL_TIMEOUT))
        c.acknowledge("t", 0, 0, AcknowledgeType.REJECT)  # no error


# -- commit ------------------------------------------------------------------

def test_commit_sync_returns_dict():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"v")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        c.acknowledge(r)
        result = c.commit_sync()
        # The mock reports no per-partition outcomes; the shape is still a dict.
        assert isinstance(result, dict)
        assert result == {}


def test_commit_sync_timeout_returns_dict():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"v")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        c.acknowledge(r)
        assert c.commit_sync_timeout(POLL_TIMEOUT) == {}


def test_commit_async_no_error():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"v")])
        (r,) = list(c.poll(POLL_TIMEOUT))
        c.acknowledge(r)
        c.commit_async()  # fire-and-forget, no error


# -- acquisition lock timeout ------------------------------------------------

def test_acquisition_lock_timeout_absent_for_mock():
    with MockShareConsumer() as c:
        c.subscribe(["t"])
        assert c.acquisition_lock_timeout_ms() is None


# -- wakeup ------------------------------------------------------------------
#
# The mock share consumer's poll returns immediately and does not honor wakeup
# (it never blocks), so here we only assert wakeup is safe and non-disruptive.
# Blocking-interruption behavior is covered against a live broker.

def test_wakeup_is_safe_and_nondisruptive():
    with MockShareConsumer() as c:
        _seed(c, records=[(b"k", b"v")])
        c.wakeup()
        recs = c.poll(POLL_TIMEOUT)
        assert len(recs) == 1
        assert c.subscription() == {"t"}


def test_wakeup_after_close_is_noop():
    c = MockShareConsumer()
    c.close()
    c.wakeup()  # no error


# -- registered ack-commit callback ------------------------------------------
#
# The mock never fires the ack-commit callback end-to-end (its setter is a
# no-op on the Rust side, matching the C-FFI mock). So the register / replace /
# clear lifecycle is tested over the boundary, and the marshaling the callback
# performs is tested by driving the wrapper's bridge closure directly with the
# raw structures the C trampoline would hand it.

def test_set_and_clear_ack_commit_callback():
    with MockShareConsumer() as c:
        assert c._ack_commit_bridge is None
        c.set_acknowledgement_commit_callback(lambda offs, err: None)
        assert c._ack_commit_bridge is not None
        c.set_acknowledgement_commit_callback(None)
        assert c._ack_commit_bridge is None


def test_replace_ack_commit_callback():
    with MockShareConsumer() as c:
        c.set_acknowledgement_commit_callback(lambda offs, err: None)
        first = c._ack_commit_bridge
        c.set_acknowledgement_commit_callback(lambda offs, err: None)
        assert c._ack_commit_bridge is not None
        assert c._ack_commit_bridge is not first


def test_set_ack_commit_callback_rejects_non_callable():
    with MockShareConsumer() as c:
        with pytest.raises(TypeError):
            c.set_acknowledgement_commit_callback(123)


def test_close_clears_registered_ack_commit_callback():
    c = MockShareConsumer()
    c.set_acknowledgement_commit_callback(lambda offs, err: None)
    assert c._ack_commit_bridge is not None
    c.close()
    # close() drops the persistent callback so the extension's INCREF is
    # released rather than leaked.
    assert c._ack_commit_bridge is None


def test_ack_commit_bridge_marshals_to_value_types():
    received = []
    with MockShareConsumer() as c:
        c.set_acknowledgement_commit_callback(
            lambda offs, err: received.append((offs, err)))
        bridge = c._ack_commit_bridge
        assert bridge is not None
        topic_id = b"\x11" * 16
        # Success: offsets present, no error.
        bridge({("t", topic_id, 0): {5, 6}}, None)
        # Failure: error fields present (code, message, retriable, fatal).
        bridge({}, (42, "boom", True, False))

    (offs0, err0), (offs1, err1) = received
    (tip,) = list(offs0)
    assert isinstance(tip, TopicIdPartition)
    assert tip.topic == "t"
    assert tip.partition == 0
    assert tip.topic_id == topic_id
    assert offs0[tip] == {5, 6}
    assert err0 is None

    assert offs1 == {}
    assert isinstance(err1, KafkaError)
    assert err1.code == 42
    assert "boom" in str(err1)
    assert err1.is_retriable is True
    assert err1.is_fatal is False


# -- value-type conversions --------------------------------------------------

def test_topic_id_partition_hash_and_eq():
    a = TopicIdPartition("t", b"\x00" * 16, 0)
    b = TopicIdPartition("t", b"\x00" * 16, 0)
    d = TopicIdPartition("t", b"\x00" * 16, 1)
    assert a == b
    assert a != d
    assert hash(a) == hash(b)
    assert {a, b, d} == {a, d}  # a and b collapse


def test_to_share_ack_offsets_conversion():
    raw = {("t", b"\x01" * 16, 3): {7, 8, 9}}
    out = _to_share_ack_offsets(raw)
    (tip,) = list(out)
    assert isinstance(tip, TopicIdPartition)
    assert tip.topic == "t" and tip.partition == 3
    assert out[tip] == {7, 8, 9}


def test_to_share_commit_map_conversion():
    raw = {
        ("ok", b"\x02" * 16, 0): None,
        ("bad", b"\x03" * 16, 1): (11, "nope", False, True),
    }
    out = _to_share_commit_map(raw)
    by_topic = {tip.topic: (tip, err) for tip, err in out.items()}
    assert by_topic["ok"][1] is None
    err = by_topic["bad"][1]
    assert isinstance(err, KafkaError)
    assert err.code == 11
    assert str(err) == "nope"
    assert err.is_retriable is False
    assert err.is_fatal is True


def test_make_kafka_error_fields():
    e = _make_kafka_error(7, "explosion", True, False)
    assert isinstance(e, KafkaError)
    assert e.code == 7
    assert str(e) == "explosion"
    assert e.message == "explosion"
    assert e.is_retriable is True
    assert e.is_fatal is False
