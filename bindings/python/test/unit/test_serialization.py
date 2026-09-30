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

"""Tests of ``confluent_kafka.common.serialization``.

Java's ``SerializationTest`` translated, with Java's test data, plus byte
vectors of each built-in against Java's encoding and Java's messages.

Translated: ``allSerdesShouldRoundtripInput`` (String, Integer, Long, Float,
Double, byte[], ByteBuffer, UUID rows; each through the record path's
``memoryview`` as Java's through both ``deserialize`` overloads),
``allSerdesShouldSupportNull``, ``stringSerdeShouldSupportDifferentEncodings``,
``stringSerdeConfigureThrowsOnUnknownEncoding``,
``floatDeserializerShouldThrowSerializationExceptionOnZeroBytes`` /
``OnTooFewBytes`` / ``OnTooManyBytes``, ``floatSerdeShouldPreserveNaNValues``,
``stringDeserializerSupportByteBuffer``, ``testBooleanSerializer`` /
``testBooleanDeserializer`` (``@ParameterizedTest`` over both booleans),
``booleanDeserializerShouldThrowOnEmptyInput``.

Skipped, their subject having no built-in (CLAUDE.md, Python Binding
Conventions, Serialization: "No others"): the ``Short`` and ``Bytes`` rows of
the round trip, every ``listSerde…`` test, ``testSerializeVoid`` /
``testDeserializeVoid`` / ``voidDeserializerShouldThrowOnNotNullValues``, and
``testSerdeFromUnknown`` / ``testSerdeFromNotNull`` (``Serdes``).
"""

from __future__ import annotations

import struct
import uuid

import pytest

from confluent_kafka import IllegalArgumentError
from confluent_kafka.common.errors import SerializationError
from confluent_kafka.common.serialization import (
    Closeable,
    Configurable,
    Deserializer,
    SerdeBase,
    Serializer,
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

TOPIC = "testTopic"


def _configured(serde: object, configs: dict[str, object], is_key: bool) -> None:
    assert isinstance(serde, Configurable)
    serde.configure(configs, is_key)


def _roundtrip(ser: Serializer[object], deser: Deserializer[object], value: object) -> None:
    serialized = ser(TOPIC, value)
    got = deser(TOPIC, None if serialized is None else memoryview(serialized))
    if isinstance(got, memoryview):
        got = bytes(got)
    assert got == value, "Should get the original value after serialization and deserialization"


# --------------------------------------------------------------------------- #
# allSerdesShouldRoundtripInput / allSerdesShouldSupportNull
# --------------------------------------------------------------------------- #

_FLOAT32 = struct.unpack(">f", struct.pack(">f", 5678567.12312))[0]
_NEG_FLOAT32 = struct.unpack(">f", struct.pack(">f", -5678567.12341))[0]

_TEST_DATA: list[tuple[str, Serializer[object], Deserializer[object], list[object]]] = [
    ("String", string_serializer(), string_deserializer(), [None, "my string"]),
    ("Integer", int_serializer(), int_deserializer(), [None, 423412424, -41243432]),
    ("Long", int_serializer(size=8), int_deserializer(size=8),
     [None, 922337203685477580, -922337203685477581]),
    ("Float", float_serializer(size=4), float_deserializer(size=4),
     [None, _FLOAT32, _NEG_FLOAT32]),
    ("Double", float_serializer(), float_deserializer(), [None, 5678567.12312, -5678567.12341]),
    ("byte[]", bytes_serializer(), bytes_deserializer(), [None, b"my string"]),
    ("ByteBuffer", bytes_serializer(), memoryview_deserializer(), [None, b"my string"]),
    ("UUID", uuid_serializer(), uuid_deserializer(), [None, uuid.uuid4()]),
]


@pytest.mark.parametrize("name,ser,deser,values", _TEST_DATA, ids=[t[0] for t in _TEST_DATA])
def test_all_serdes_should_roundtrip_input(name: str, ser: Serializer[object],
                                           deser: Deserializer[object],
                                           values: list[object]) -> None:
    for value in values:
        _roundtrip(ser, deser, value)


@pytest.mark.parametrize("name,ser,deser,values", _TEST_DATA, ids=[t[0] for t in _TEST_DATA])
def test_all_serdes_should_support_null(name: str, ser: Serializer[object],
                                        deser: Deserializer[object],
                                        values: list[object]) -> None:
    assert ser(TOPIC, None) is None, f"Should support null in {name} serialization"
    assert deser(TOPIC, None) is None, f"Should support null in {name} deserialization"
    assert deser(TOPIC, None, ()) is None


def test_bool_and_json_support_null() -> None:
    for ser in (bool_serializer(), json_serializer()):
        assert ser(TOPIC, None) is None
    for deser in (bool_deserializer(), json_deserializer()):
        assert deser(TOPIC, None) is None


# --------------------------------------------------------------------------- #
# Byte vectors (Java's encoders)
# --------------------------------------------------------------------------- #


def test_integer_and_long_bytes() -> None:
    assert int_serializer()(TOPIC, 423412424) == bytes.fromhex("193cc2c8")
    assert int_serializer()(TOPIC, -41243432) == bytes.fromhex("fd8aacd8")
    assert int_serializer()(TOPIC, -1) == b"\xff\xff\xff\xff"
    assert int_serializer(size=8)(TOPIC, 922337203685477580) == bytes.fromhex("0ccccccccccccccc")
    assert int_serializer(size=8)(TOPIC, -922337203685477581) == bytes.fromhex("f333333333333333")
    assert int_deserializer()(TOPIC, memoryview(bytes.fromhex("fd8aacd8"))) == -41243432
    assert int_deserializer(size=8)(TOPIC, memoryview(b"\xff" * 8)) == -1


def test_float_and_double_bytes() -> None:
    assert float_serializer(size=4)(TOPIC, 5678567.12312) == bytes.fromhex("4aad4bce")
    assert float_serializer(size=4)(TOPIC, -5678567.12341) == bytes.fromhex("caad4bce")
    assert float_serializer()(TOPIC, 5678567.12312) == bytes.fromhex("4155a979c7e132b5")
    assert float_serializer()(TOPIC, -5678567.12341) == bytes.fromhex("c155a979c7e5f30e")
    assert float_serializer(size=4)(TOPIC, 1.5) == b"\x3f\xc0\x00\x00"
    assert float_serializer()(TOPIC, 1.5) == b"\x3f\xf8\x00\x00\x00\x00\x00\x00"
    # (float) narrowing: beyond the 32-bit range is an infinity.
    assert float_serializer(size=4)(TOPIC, 1e39) == b"\x7f\x80\x00\x00"
    assert float_serializer(size=4)(TOPIC, -1e39) == b"\xff\x80\x00\x00"


def test_double_serializer_writes_nan_as_double_to_long_bits() -> None:
    non_canonical = struct.unpack(">d", b"\x7f\xf0\x00\x00\x00\x00\x00\x01")[0]
    assert non_canonical != non_canonical
    assert float_serializer()(TOPIC, non_canonical) == b"\x7f\xf8\x00\x00\x00\x00\x00\x00"
    assert float_serializer()(TOPIC, float("nan")) == b"\x7f\xf8\x00\x00\x00\x00\x00\x00"
    # A plain NaN narrows to Java's quiet float NaN.
    assert float_serializer(size=4)(TOPIC, float("nan")) == b"\x7f\xc0\x00\x00"


def test_bool_and_string_bytes() -> None:
    assert bool_serializer()(TOPIC, True) == b"\x01"
    assert bool_serializer()(TOPIC, False) == b"\x00"
    assert string_serializer()(TOPIC, "my string") == b"my string"
    assert string_serializer()(TOPIC, "é€") == b"\xc3\xa9\xe2\x82\xac"
    # getBytes replaces what the charset cannot encode; new String replaces
    # malformed input with U+FFFD.
    assert string_serializer()(TOPIC, "a\ud800b") == b"a?b"
    assert string_serializer(encoding="ascii")(TOPIC, "é") == b"?"
    assert string_deserializer()(TOPIC, memoryview(b"a\xffb")) == "a�b"


def test_string_utf16_and_utf32_follow_java_byte_order() -> None:
    # Java's UTF-16 writes a big-endian byte-order mark (none for ""); its
    # UTF-32 is big-endian with no mark.
    assert string_serializer(encoding="UTF-16")(TOPIC, "my string") == (
        b"\xfe\xff" + bytes.fromhex("006d007900200073007400720069006e0067"))
    assert string_serializer(encoding="UTF-16")(TOPIC, "") == b""
    assert string_serializer(encoding="UTF-32")(TOPIC, "a") == b"\x00\x00\x00a"
    utf16 = string_deserializer(encoding="UTF-16")
    assert utf16(TOPIC, memoryview(b"\x00a")) == "a"  # big-endian without a mark
    assert utf16(TOPIC, memoryview(b"\xff\xfea\x00")) == "a"
    assert utf16(TOPIC, memoryview(b"\xfe\xff\x00a")) == "a"
    utf32 = string_deserializer(encoding="UTF-32")
    assert utf32(TOPIC, memoryview(b"\x00\x00\x00a")) == "a"
    assert utf32(TOPIC, memoryview(b"\xff\xfe\x00\x00a\x00\x00\x00")) == "a"


def test_uuid_bytes_are_the_dashed_form() -> None:
    value = uuid.UUID("0f14d0ab-9605-4a62-a9e4-5ed26688389b")
    assert uuid_serializer()(TOPIC, value) == b"0f14d0ab-9605-4a62-a9e4-5ed26688389b"


def test_uuid_serializer_writes_text_not_the_binary_value() -> None:
    # Java's UUIDSerializer writes UUID.toString() in the configured encoding:
    # the 36-character dashed text, never the 16-byte binary value.
    value = uuid.UUID("0f14d0ab-9605-4a62-a9e4-5ed26688389b")
    data = uuid_serializer()(TOPIC, value)
    assert data is not None and len(data) == 36 and data != value.bytes
    utf16 = uuid_serializer()
    _configured(utf16, {"value.serializer.encoding": "UTF-16BE"}, False)
    assert utf16(TOPIC, value) == str(value).encode("utf-16-be")


# --------------------------------------------------------------------------- #
# stringSerdeShouldSupportDifferentEncodings / ConfigureThrowsOnUnknownEncoding
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("encoding", ["UTF-8", "UTF-16"])
def test_string_serde_should_support_different_encodings(encoding: str) -> None:
    serializer = string_serializer()
    _configured(serializer, {"key.serializer.encoding": encoding}, True)
    deserializer = string_deserializer()
    _configured(deserializer, {"key.deserializer.encoding": encoding}, True)
    out = serializer(TOPIC, "my string")
    assert out is not None
    assert deserializer(TOPIC, memoryview(out)) == "my string", (
        f"Should get the original string after serialization and deserialization with "
        f"encoding {encoding}")
    # The factory argument is the constructor route to the same encodings.
    out = string_serializer(encoding=encoding)(TOPIC, "my string")
    assert out is not None
    assert string_deserializer(encoding=encoding)(TOPIC, memoryview(out)) == "my string"


def test_string_serde_configure_throws_on_unknown_encoding() -> None:
    encoding = "encoding-does-not-exist"
    with pytest.raises(SerializationError) as exc:
        _configured(string_serializer(), {"key.serializer.encoding": encoding}, True)
    assert str(exc.value) == "Unsupported encoding encoding-does-not-exist"
    assert isinstance(exc.value.__cause__, LookupError)
    with pytest.raises(SerializationError) as exc:
        _configured(string_deserializer(), {"key.deserializer.encoding": encoding}, True)
    assert str(exc.value) == "Unsupported encoding encoding-does-not-exist"
    with pytest.raises(SerializationError) as exc:
        string_serializer(encoding=encoding)
    assert str(exc.value) == "Unsupported encoding encoding-does-not-exist"


def test_string_configure_precedence() -> None:
    # The key/value property takes precedence over serializer.encoding.
    serializer = string_serializer()
    _configured(serializer, {"serializer.encoding": "UTF-16", "key.serializer.encoding": "ascii"},
                True)
    assert serializer(TOPIC, "x") == b"x"
    serializer = string_serializer()
    _configured(serializer, {"serializer.encoding": "UTF-16", "key.serializer.encoding": "ascii"},
                False)
    assert serializer(TOPIC, "x") == b"\xfe\xff\x00x"
    deserializer = string_deserializer()
    _configured(deserializer, {"deserializer.encoding": "ascii"}, False)
    assert deserializer(TOPIC, memoryview(b"x")) == "x"


# --------------------------------------------------------------------------- #
# Size checks
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("length", [0, 3, 5])
def test_float_deserializer_should_throw_serialization_exception_on_wrong_size(length: int) -> None:
    # floatDeserializerShouldThrowSerializationExceptionOnZeroBytes / OnTooFewBytes /
    # OnTooManyBytes.
    with pytest.raises(SerializationError) as exc:
        float_deserializer(size=4)(TOPIC, memoryview(bytes(length)))
    assert str(exc.value) == "Size of data received by Deserializer is not 4"


def test_numeric_deserializer_messages() -> None:
    cases = [
        (float_deserializer(), "Size of data received by DoubleDeserializer is not 8"),
        (int_deserializer(), "Size of data received by IntegerDeserializer is not 4"),
        (int_deserializer(size=8), "Size of data received by LongDeserializer is not 8"),
    ]
    for deser, message in cases:
        with pytest.raises(SerializationError) as exc:
            deser(TOPIC, memoryview(b"\x00"))
        assert str(exc.value) == message


def test_boolean_deserializer_should_throw_on_empty_input() -> None:
    with pytest.raises(SerializationError) as exc:
        bool_deserializer()(TOPIC, memoryview(b""))
    assert str(exc.value) == "Size of data received by BooleanDeserializer is not 1"


def test_boolean_deserializer_unexpected_byte() -> None:
    with pytest.raises(SerializationError) as exc:
        bool_deserializer()(TOPIC, memoryview(b"\x05"))
    assert str(exc.value) == "Unexpected byte received by BooleanDeserializer: 5"
    with pytest.raises(SerializationError) as exc:
        bool_deserializer()(TOPIC, memoryview(b"\xff"))
    assert str(exc.value) == "Unexpected byte received by BooleanDeserializer: -1"


# --------------------------------------------------------------------------- #
# testBooleanSerializer / testBooleanDeserializer (@ParameterizedTest)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("data_to_serialize", [True, False])
def test_boolean_serializer(data_to_serialize: bool) -> None:
    assert bool_serializer()(TOPIC, data_to_serialize) == bytes([1 if data_to_serialize else 0])


@pytest.mark.parametrize("data_to_deserialize", [True, False])
def test_boolean_deserializer(data_to_deserialize: bool) -> None:
    data = memoryview(bytes([1 if data_to_deserialize else 0]))
    assert bool_deserializer()(TOPIC, data) is data_to_deserialize


# --------------------------------------------------------------------------- #
# floatSerdeShouldPreserveNaNValues
# --------------------------------------------------------------------------- #


def test_float_serde_should_preserve_nan_values() -> None:
    serializer, deserializer = float_serializer(size=4), float_deserializer(size=4)
    for some_nan_as_int_bits in (0x7F800001, 0x7F800002, 0xFFC00123):
        raw = some_nan_as_int_bits.to_bytes(4, "big")
        # Java builds the NaN with Float.intBitsToFloat; here it is read.
        some_nan = deserializer(TOPIC, memoryview(raw))
        assert some_nan is not None and some_nan != some_nan
        roundtrip = deserializer(TOPIC, memoryview(serializer(TOPIC, some_nan) or b""))
        assert roundtrip is not None
        # Because of NaN semantics we must assert based on the raw int bits.
        assert serializer(TOPIC, roundtrip) == raw


# --------------------------------------------------------------------------- #
# stringDeserializerSupportByteBuffer
# --------------------------------------------------------------------------- #


def test_string_deserializer_support_byte_buffer() -> None:
    data = "Hello, ByteBuffer!"
    serialized = string_serializer()(TOPIC, data)
    assert serialized is not None
    buffer = bytearray(len(serialized) * 2)
    buffer[: len(serialized)] = serialized
    # A view of part of a larger buffer, as a fetched record's is.
    assert string_deserializer()(TOPIC, memoryview(buffer)[: len(serialized)]) == data


# --------------------------------------------------------------------------- #
# UUID (UUIDDeserializer / java.util.UUID.fromString)
# --------------------------------------------------------------------------- #


def test_uuid_deserializer_parses_as_java() -> None:
    deserializer = uuid_deserializer()
    value = uuid.UUID("0f14d0ab-9605-4a62-a9e4-5ed26688389b")
    assert deserializer(TOPIC, memoryview(b"0F14D0AB-9605-4A62-A9E4-5ED26688389B")) == value
    # Each group is masked to its width, as UUID.fromString does.
    assert deserializer(TOPIC, memoryview(b"1-1-1-1-1")) == uuid.UUID(
        "00000001-0001-0001-0001-000000000001")
    for bad in (b"not a uuid", b"0f14d0ab96054a62a9e45ed26688389b", b"1-1-1-1-1-1",
                b"x" * 40, b"g-1-1-1-1", b"1--1-1-1"):
        with pytest.raises(SerializationError) as exc:
            deserializer(TOPIC, memoryview(bad))
        assert str(exc.value) == "Error parsing data into UUID"
        assert isinstance(exc.value.__cause__, IllegalArgumentError)
    with pytest.raises(SerializationError) as exc:
        deserializer(TOPIC, memoryview(b"x" * 40))
    assert str(exc.value.__cause__) == "UUID string too large"


def test_uuid_deserializer_rejects_the_binary_value() -> None:
    # Java's UUIDDeserializer decodes the bytes as text (a malformed byte becomes
    # U+FFFD) and parses it with UUID.fromString, so the 16-byte binary value is
    # not a UUID record; the serializer's own output reads back.
    value = uuid.UUID("0f14d0ab-9605-4a62-a9e4-5ed26688389b")
    deserializer = uuid_deserializer()
    with pytest.raises(SerializationError) as exc:
        deserializer(TOPIC, memoryview(value.bytes))
    assert str(exc.value) == "Error parsing data into UUID"
    assert isinstance(exc.value.__cause__, IllegalArgumentError)
    data = uuid_serializer()(TOPIC, value)
    assert data is not None
    assert deserializer(TOPIC, memoryview(data)) == value


def test_uuid_unsupported_encoding() -> None:
    serializer = uuid_serializer()
    _configured(serializer, {"value.serializer.encoding": "no-such-charset"}, False)
    with pytest.raises(SerializationError) as exc:
        serializer(TOPIC, uuid.uuid4())
    assert str(exc.value) == (
        "Error when serializing UUID to byte[] due to unsupported encoding no-such-charset")
    assert exc.value.__cause__ is None
    deserializer = uuid_deserializer()
    _configured(deserializer, {"deserializer.encoding": "no-such-charset"}, True)
    with pytest.raises(SerializationError) as exc:
        deserializer(TOPIC, memoryview(b"1-1-1-1-1"))
    assert str(exc.value) == (
        "Error when deserializing ByteBuffer to UUID due to unsupported encoding "
        "no-such-charset")


# --------------------------------------------------------------------------- #
# JSON (no Java counterpart)
# --------------------------------------------------------------------------- #


def test_json_serde() -> None:
    value = {"a": 1, "b": ["x", "y"], "c": None}
    out = json_serializer()(TOPIC, value)
    assert out == b'{"a": 1, "b": ["x", "y"], "c": null}'
    assert json_deserializer()(TOPIC, memoryview(out)) == value
    with pytest.raises(SerializationError) as exc:
        json_serializer()(TOPIC, object())
    assert str(exc.value) == "Error serializing value to JSON"
    with pytest.raises(SerializationError) as exc:
        json_deserializer()(TOPIC, memoryview(b"not json"))
    assert str(exc.value) == "Error deserializing JSON value"


# --------------------------------------------------------------------------- #
# Factories and protocols
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("size", [0, 1, 2, 3, 16])
def test_factories_reject_other_sizes(size: int) -> None:
    for factory, sizes in ((int_serializer, "4 or 8"), (int_deserializer, "4 or 8"),
                           (float_serializer, "8 or 4"), (float_deserializer, "8 or 4")):
        with pytest.raises(IllegalArgumentError) as exc:
            factory(size=size)
        assert str(exc.value) == f"{factory.__name__}() size must be {sizes}; got {size}"


def test_factory_arguments_are_keyword_only() -> None:
    with pytest.raises(TypeError):
        int_serializer(4)  # type: ignore[misc]
    with pytest.raises(TypeError):
        string_serializer("utf_8")  # type: ignore[misc]
    with pytest.raises(TypeError):
        float_deserializer(8)  # type: ignore[misc]


def test_bytes_deserializer_copies_and_memoryview_deserializer_views() -> None:
    buffer = bytearray(b"abc")
    copied = bytes_deserializer()(TOPIC, memoryview(buffer))
    viewed = memoryview_deserializer()(TOPIC, memoryview(buffer))
    buffer[0] = ord("X")
    assert copied == b"abc"
    assert viewed is not None and viewed.tobytes() == b"Xbc"


def test_lifecycle_protocols() -> None:
    base = SerdeBase()
    assert isinstance(base, Configurable) and isinstance(base, Closeable)
    base.configure({}, True)
    base.close()

    def bare(topic: str, value: object, headers: object = None) -> bytes | None:
        return None

    assert not isinstance(bare, Configurable)
    assert not isinstance(bare, Closeable)
    import confluent_kafka.common.serialization as serialization

    # One file per public class (CLAUDE.md, Modules).
    assert [c.__module__ for c in (Serializer, Deserializer, Configurable, Closeable, SerdeBase)] == [
        "confluent_kafka.common.serialization." + m
        for m in ("serializer", "deserializer", "configurable", "closeable", "serde_base")]

    assert serialization.__all__ == [
        "Serializer", "Deserializer", "Configurable", "Closeable", "SerdeBase",
        "bytes_serializer", "bytes_deserializer", "memoryview_deserializer",
        "string_serializer", "string_deserializer", "int_serializer", "int_deserializer",
        "float_serializer", "float_deserializer", "bool_serializer", "bool_deserializer",
        "uuid_serializer", "uuid_deserializer", "json_serializer", "json_deserializer"]
