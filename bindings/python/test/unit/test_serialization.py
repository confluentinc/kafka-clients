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

"""Unit tests for ``confluent_kafka.common.serialization`` (P3).

Translates Java's ``SerializationTest``
(``kafka/clients/src/test/java/org/apache/kafka/common/serialization/SerializationTest.java``),
one serde family at a time, plus byte-level vectors against Java's encoders.

Java tests translated:

- ``allSerdesShouldRoundtripInput`` — per-type round-trip (string / short-as-int
  / int / long / float / double / bytes / uuid); ``ByteBuffer`` / ``Bytes`` rows
  become the ``bytes`` / ``memoryview`` serdes.
- ``allSerdesShouldSupportNull`` — every serde maps ``None`` to ``None``.
- ``stringSerdeShouldSupportDifferentEncodings`` — UTF-8 / UTF-16 round-trip.
- ``stringSerdeConfigureThrowsOnUnknownEncoding`` — ``configure`` with an unknown
  charset raises ``SerializationError`` (Java's ``SerializationException``).
- ``floatDeserializerShouldThrow…OnZero/TooFew/TooManyBytes`` +
  ``booleanDeserializerShouldThrowOnEmptyInput`` — size checks.
- ``floatSerdeShouldPreserveNaNValues`` — canonical-NaN round-trip (Python-float
  limitation on raw signaling-NaN payloads — see C17).
- ``testBooleanSerializer`` / ``testBooleanDeserializer`` (``@ParameterizedTest``)
  — parametrized true/false byte vectors.
- ``stringDeserializerSupportByteBuffer`` — the ``memoryview`` input path.

Java tests skipped (subject not in the v1 built-in roster — D6):

- All ``listSerde…`` cases — ``List`` serde is "not in v1" (D6 built-ins table;
  needs an inner-serde story first). No ``list_serializer`` factory exists.
- ``testSerializeVoid`` / ``testDeserializeVoid`` /
  ``voidDeserializerShouldThrowOnNotNullValues`` — ``Void`` serde is "not in v1"
  (``bytes_*`` + None-passthrough covers it).
- ``testSerdeFromUnknown`` / ``testSerdeFromNotNull`` /
  ``allSerdesShouldRoundtripInput``'s ``Serde``/``Serdes`` plumbing — the
  ``Serde`` bundle + ``Serdes`` catalog are Streams-only machinery, offered
  nowhere on this surface (D6 "Serde<T> / Serdes finding"). The factories are
  tested directly instead.

Additional byte-vector assertions (DoD #3: wire-level, not only round-trip)
check each encoder against Java's known output for representative values.
"""

from __future__ import annotations

import struct

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common.errors import SerializationError
from confluent_kafka.common.serialization import (
    bool_deserializer,
    bool_serializer,
    bytes_deserializer,
    bytes_serializer,
    float_deserializer,
    float_serializer,
    int_deserializer,
    int_serializer,
    json_deserializer,
    json_serializer,
    memoryview_deserializer,
    string_deserializer,
    string_serializer,
    uuid_deserializer,
    uuid_serializer,
)
from confluent_kafka.common.serialization.string_deserializer import StringDeserializer
from confluent_kafka.common.serialization.string_serializer import StringSerializer
from confluent_kafka.common.uuid import Uuid

TOPIC = "testTopic"


def mv(data: bytes) -> memoryview:
    """Deserializers take a ``memoryview`` on the receive path; wrap test bytes."""
    return memoryview(data)


# ---------------------------------------------------------------------------
# allSerdesShouldRoundtripInput / allSerdesShouldSupportNull
# ---------------------------------------------------------------------------


class TestRoundtrip:
    def test_string_roundtrip(self) -> None:
        for value in (None, "my string"):
            ser = string_serializer()
            deser = string_deserializer()
            out = ser(TOPIC, value)
            got = deser(TOPIC, None if out is None else mv(out))
            assert got == value

    @pytest.mark.parametrize("value", [None, 423412424, -41243432])
    def test_int_roundtrip(self, value: int | None) -> None:
        out = int_serializer()(TOPIC, value)
        got = int_deserializer()(TOPIC, None if out is None else mv(out))
        assert got == value

    @pytest.mark.parametrize("value", [None, 32767, -32768])
    def test_short_range_via_int_size2_is_not_offered(self, value: int | None) -> None:
        # Java's Short serde has no spec factory (int_serializer allows size 4|8
        # only) — the Short row of Java's testData has no home here (C16). The
        # size-2 factory call is rejected.
        with pytest.raises(IllegalArgumentError):
            int_serializer(size=2)

    @pytest.mark.parametrize("value", [None, 922337203685477580, -922337203685477581])
    def test_long_roundtrip(self, value: int | None) -> None:
        out = int_serializer(size=8)(TOPIC, value)
        got = int_deserializer(size=8)(TOPIC, None if out is None else mv(out))
        assert got == value

    @pytest.mark.parametrize("value", [None, 5678567.12312, -5678567.12341])
    def test_double_roundtrip(self, value: float | None) -> None:
        out = float_serializer()(TOPIC, value)
        got = float_deserializer()(TOPIC, None if out is None else mv(out))
        assert got == value

    @pytest.mark.parametrize("value", [None, 1.5, -2.25, 256.0])
    def test_float_roundtrip(self, value: float | None) -> None:
        out = float_serializer(size=4)(TOPIC, value)
        got = float_deserializer(size=4)(TOPIC, None if out is None else mv(out))
        assert got == value

    def test_bytes_roundtrip(self) -> None:
        for value in (None, b"my string"):
            out = bytes_serializer()(TOPIC, value)
            got = bytes_deserializer()(TOPIC, None if out is None else mv(out))
            assert got == value

    def test_memoryview_roundtrip(self) -> None:
        for value in (None, b"my string"):
            out = bytes_serializer()(TOPIC, value)
            got = memoryview_deserializer()(TOPIC, None if out is None else mv(out))
            assert (got is None and value is None) or bytes(got) == value

    def test_uuid_roundtrip(self) -> None:
        for value in (None, Uuid.random_uuid()):
            out = uuid_serializer()(TOPIC, value)
            got = uuid_deserializer()(TOPIC, None if out is None else mv(out))
            assert got == value

    def test_all_serializers_support_null(self) -> None:
        serializers = [
            string_serializer(),
            int_serializer(),
            int_serializer(size=8),
            float_serializer(),
            float_serializer(size=4),
            bool_serializer(),
            bytes_serializer(),
            uuid_serializer(),
            json_serializer(),
        ]
        for ser in serializers:
            assert ser(TOPIC, None) is None

    def test_all_deserializers_support_null(self) -> None:
        deserializers = [
            string_deserializer(),
            int_deserializer(),
            int_deserializer(size=8),
            float_deserializer(),
            float_deserializer(size=4),
            bool_deserializer(),
            bytes_deserializer(),
            memoryview_deserializer(),
            uuid_deserializer(),
            json_deserializer(),
        ]
        for deser in deserializers:
            assert deser(TOPIC, None) is None


# ---------------------------------------------------------------------------
# Byte-level vectors against Java's encoders (DoD #3)
# ---------------------------------------------------------------------------


class TestByteVectors:
    def test_int_big_endian_signed(self) -> None:
        # Java IntegerSerializer emits big-endian 4-byte two's complement.
        assert int_serializer()(TOPIC, 1) == b"\x00\x00\x00\x01"
        assert int_serializer()(TOPIC, -1) == b"\xff\xff\xff\xff"
        assert int_serializer()(TOPIC, 423412424) == struct.pack(">i", 423412424)

    def test_long_big_endian_signed(self) -> None:
        assert int_serializer(size=8)(TOPIC, 1) == b"\x00\x00\x00\x00\x00\x00\x00\x01"
        assert int_serializer(size=8)(TOPIC, -1) == b"\xff" * 8
        assert int_serializer(size=8)(TOPIC, 922337203685477580) == struct.pack(
            ">q", 922337203685477580
        )

    def test_float_big_endian_ieee754(self) -> None:
        assert float_serializer(size=4)(TOPIC, 1.5) == struct.pack(">f", 1.5)
        # Java FloatSerializer uses floatToRawIntBits -> big-endian int bits.
        assert float_serializer(size=4)(TOPIC, 1.5) == b"\x3f\xc0\x00\x00"

    def test_double_big_endian_ieee754(self) -> None:
        assert float_serializer()(TOPIC, 1.5) == struct.pack(">d", 1.5)
        assert float_serializer()(TOPIC, 1.5) == b"\x3f\xf8\x00\x00\x00\x00\x00\x00"

    def test_bool_bytes(self) -> None:
        assert bool_serializer()(TOPIC, True) == b"\x01"
        assert bool_serializer()(TOPIC, False) == b"\x00"

    def test_string_utf8_bytes(self) -> None:
        assert string_serializer()(TOPIC, "my string") == b"my string"


# ---------------------------------------------------------------------------
# String encodings (stringSerdeShouldSupportDifferentEncodings, configure)
# ---------------------------------------------------------------------------


class TestStringEncoding:
    @pytest.mark.parametrize("encoding", ["utf_8", "utf_16", "UTF-8", "UTF-16"])
    def test_roundtrip_with_encoding(self, encoding: str) -> None:
        text = "my string"
        ser = string_serializer(encoding=encoding)
        deser = string_deserializer(encoding=encoding)
        out = ser(TOPIC, text)
        assert out is not None
        assert deser(TOPIC, mv(out)) == text

    def test_configure_precedence_key_over_generic(self) -> None:
        ser = StringSerializer()
        ser.configure(
            {"serializer.encoding": "utf_16", "key.serializer.encoding": "ascii"},
            True,
        )
        assert ser(TOPIC, "x") == b"x"  # ascii

    def test_configure_falls_back_to_generic(self) -> None:
        deser = StringDeserializer()
        deser.configure({"deserializer.encoding": "ascii"}, False)
        assert deser(TOPIC, mv(b"x")) == "x"

    def test_configure_throws_on_unknown_encoding(self) -> None:
        # Java: stringSerdeConfigureThrowsOnUnknownEncoding — the bad charset is
        # rejected at configure() time, not on the first serialize.
        ser = StringSerializer()
        with pytest.raises(SerializationError):
            ser.configure({"key.serializer.encoding": "encoding-does-not-exist"}, True)
        deser = StringDeserializer()
        with pytest.raises(SerializationError):
            deser.configure(
                {"key.deserializer.encoding": "encoding-does-not-exist"}, True
            )


# ---------------------------------------------------------------------------
# Size / value checks (float, boolean)
# ---------------------------------------------------------------------------


class TestSizeChecks:
    @pytest.mark.parametrize("length", [0, 3, 5])
    def test_float_deserializer_wrong_size(self, length: int) -> None:
        # floatDeserializerShouldThrow…OnZero/TooFew/TooManyBytes.
        with pytest.raises(SerializationError) as exc:
            float_deserializer(size=4)(TOPIC, mv(b"\x00" * length))
        assert str(exc.value) == "Size of data received by Deserializer is not 4"

    def test_int_deserializer_wrong_size(self) -> None:
        with pytest.raises(SerializationError) as exc:
            int_deserializer()(TOPIC, mv(b"\x00"))
        assert (
            str(exc.value) == "Size of data received by IntegerDeserializer is not 4"
        )

    def test_long_deserializer_wrong_size(self) -> None:
        with pytest.raises(SerializationError) as exc:
            int_deserializer(size=8)(TOPIC, mv(b"\x00"))
        assert str(exc.value) == "Size of data received by LongDeserializer is not 8"

    def test_boolean_deserializer_empty(self) -> None:
        # booleanDeserializerShouldThrowOnEmptyInput.
        with pytest.raises(SerializationError) as exc:
            bool_deserializer()(TOPIC, mv(b""))
        assert str(exc.value) == "Size of data received by BooleanDeserializer is not 1"

    def test_boolean_deserializer_unexpected_byte(self) -> None:
        with pytest.raises(SerializationError) as exc:
            bool_deserializer()(TOPIC, mv(b"\x05"))
        assert str(exc.value) == "Unexpected byte received by BooleanDeserializer: 5"

    def test_boolean_deserializer_unexpected_byte_signed(self) -> None:
        # Java's byte is signed; 0xFF prints as -1.
        with pytest.raises(SerializationError) as exc:
            bool_deserializer()(TOPIC, mv(b"\xff"))
        assert str(exc.value) == "Unexpected byte received by BooleanDeserializer: -1"


# ---------------------------------------------------------------------------
# Boolean serializer/deserializer (@ParameterizedTest booleans)
# ---------------------------------------------------------------------------


class TestBoolean:
    @pytest.mark.parametrize("value", [True, False])
    def test_boolean_serializer(self, value: bool) -> None:
        expected = bytes([1 if value else 0])
        assert bool_serializer()(TOPIC, value) == expected

    @pytest.mark.parametrize("value", [True, False])
    def test_boolean_deserializer(self, value: bool) -> None:
        data = bytes([1 if value else 0])
        assert bool_deserializer()(TOPIC, mv(data)) is value


# ---------------------------------------------------------------------------
# Float NaN handling (floatSerdeShouldPreserveNaNValues, adapted — see C17)
# ---------------------------------------------------------------------------


class TestFloatNaN:
    def test_float_nan_roundtrips_canonically(self) -> None:
        # Java preserves the raw signaling-NaN bit pattern (floatToRawIntBits);
        # a Python float cannot carry that payload, so we assert a canonical-NaN
        # round-trip (still a NaN, bits preserved through our own encoder) — C17.
        nan = float("nan")
        out = float_serializer(size=4)(TOPIC, nan)
        assert out is not None
        got = float_deserializer(size=4)(TOPIC, mv(out))
        assert got != got  # NaN != NaN
        # The bit pattern our serializer emitted round-trips exactly.
        assert float_serializer(size=4)(TOPIC, got) == out

    def test_double_nan_canonicalized_like_java(self) -> None:
        # Java's DoubleSerializer uses doubleToLongBits, which canonicalizes
        # every NaN to 0x7ff8000000000000 (unlike the raw doubleToRawLongBits) —
        # F2 / C20. A non-canonical double NaN is representable in a Python float
        # (it IS a C double), so we build one from raw bits and assert the
        # serializer emits Java's canonical bytes, NOT the raw pack.
        non_canonical = struct.unpack(">d", b"\x7f\xf0\x00\x00\x00\x00\x00\x01")[0]
        assert non_canonical != non_canonical  # it is a NaN
        # struct.pack(">d") (raw) would leak the non-canonical payload:
        assert struct.pack(">d", non_canonical) == b"\x7f\xf0\x00\x00\x00\x00\x00\x01"
        # Our serializer canonicalizes it to match Java doubleToLongBits:
        out = float_serializer(size=8)(TOPIC, non_canonical)
        assert out == b"\x7f\xf8\x00\x00\x00\x00\x00\x00"
        # A plain (already-canonical) NaN also emits the canonical bytes.
        assert float_serializer(size=8)(TOPIC, float("nan")) == (
            b"\x7f\xf8\x00\x00\x00\x00\x00\x00"
        )


# ---------------------------------------------------------------------------
# memoryview input path (stringDeserializerSupportByteBuffer)
# ---------------------------------------------------------------------------


class TestMemoryviewInput:
    def test_string_deserializer_from_memoryview(self) -> None:
        data = "Hello, ByteBuffer!"
        out = string_serializer()(TOPIC, data)
        assert out is not None
        assert string_deserializer()(TOPIC, mv(out)) == data


# ---------------------------------------------------------------------------
# UUID serde (byte form + parse error)
# ---------------------------------------------------------------------------


class TestUuid:
    def test_uuid_serializes_string_form(self) -> None:
        u = Uuid.random_uuid()
        # Java UUIDSerializer serializes data.toString().getBytes(encoding);
        # our Uuid's string form is its base64 encoding (C15).
        assert uuid_serializer()(TOPIC, u) == str(u).encode("utf_8")

    def test_uuid_deserializer_parse_error(self) -> None:
        with pytest.raises(SerializationError) as exc:
            # A string too long to be a base64 UUID -> Uuid.from_string raises
            # IllegalArgumentError -> wrapped as "Error parsing data into UUID".
            uuid_deserializer()(TOPIC, mv(b"x" * 40))
        assert str(exc.value) == "Error parsing data into UUID"


# ---------------------------------------------------------------------------
# JSON serde (Python-natural, no Java counterpart)
# ---------------------------------------------------------------------------


class TestJson:
    def test_json_roundtrip(self) -> None:
        value = {"a": 1, "b": ["x", "y"], "c": None}
        out = json_serializer()(TOPIC, value)
        assert out is not None
        assert json_deserializer()(TOPIC, mv(out)) == value

    def test_json_serialize_error(self) -> None:
        with pytest.raises(SerializationError):
            json_serializer()(TOPIC, object())

    def test_json_deserialize_error(self) -> None:
        with pytest.raises(SerializationError):
            json_deserializer()(TOPIC, mv(b"not json"))


# ---------------------------------------------------------------------------
# Factory size validation
# ---------------------------------------------------------------------------


class TestFactorySizes:
    @pytest.mark.parametrize("size", [0, 1, 2, 3, 16])
    def test_int_factory_rejects_bad_size(self, size: int) -> None:
        with pytest.raises(IllegalArgumentError):
            int_serializer(size=size)
        with pytest.raises(IllegalArgumentError):
            int_deserializer(size=size)

    @pytest.mark.parametrize("size", [0, 1, 2, 3, 16])
    def test_float_factory_rejects_bad_size(self, size: int) -> None:
        with pytest.raises(IllegalArgumentError):
            float_serializer(size=size)
        with pytest.raises(IllegalArgumentError):
            float_deserializer(size=size)

    def test_factory_arguments_are_keyword_only(self) -> None:
        with pytest.raises(TypeError):
            int_serializer(4)  # type: ignore[misc]
        with pytest.raises(TypeError):
            string_serializer("utf_8")  # type: ignore[misc]
