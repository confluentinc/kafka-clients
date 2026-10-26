# Copyright 2026 Confluent Inc.
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

"""``ConfigEntry``: Java's ``org.apache.kafka.clients.admin.ConfigEntry``.

Java's two constructors ``(name, value)`` and ``(name, value, source,
isSensitive, isReadOnly, synonyms, type, documentation)`` are one keyword-only
constructor matched by ``java_forms`` (CLAUDE.md, Python Binding Conventions,
Signatures): ``(name, value)`` passes ``UNKNOWN``, ``false``, ``false``, an
empty list, ``UNKNOWN`` and ``null`` to the longer one. ``ConfigType`` and
``ConfigSource`` carry no integer field, so they are ``enum.Enum``\\ s valued by
the constant's name. ``ConfigSynonym``'s constructor is package-private, so
``ConfigEntry.ConfigSynonym()`` raises ``TypeError`` and the client builds it
through ``_new``.
"""

from __future__ import annotations

from collections.abc import Sequence
from enum import Enum
from typing import Any, overload

from confluent_kafka._args import MISSING, Form, java_forms
from confluent_kafka._java import java_str

__all__ = ["ConfigEntry"]

# Java's (name, value) passes ConfigSource.UNKNOWN and ConfigType.UNKNOWN to the
# longer constructor; the nested enums do not exist yet when the decorator
# runs, so the Form names them through these private markers.
_UNKNOWN_SOURCE: Any = object()
_UNKNOWN_TYPE: Any = object()


class ConfigEntry:
    """A class representing a configuration entry containing name, value and
    additional metadata.

    Java: ``org.apache.kafka.clients.admin.ConfigEntry``.
    """

    class ConfigType(Enum):
        """Data type of configuration entry.

        Java: ``org.apache.kafka.clients.admin.ConfigEntry.ConfigType``.
        """

        UNKNOWN = "UNKNOWN"
        BOOLEAN = "BOOLEAN"
        STRING = "STRING"
        INT = "INT"
        SHORT = "SHORT"
        LONG = "LONG"
        DOUBLE = "DOUBLE"
        LIST = "LIST"
        CLASS = "CLASS"
        PASSWORD = "PASSWORD"

        def __str__(self) -> str:
            return self.name

    class ConfigSource(Enum):
        """Source of configuration entries.

        Java: ``org.apache.kafka.clients.admin.ConfigEntry.ConfigSource``.
        """

        #: dynamic topic config that is configured for a specific topic
        DYNAMIC_TOPIC_CONFIG = "DYNAMIC_TOPIC_CONFIG"
        #: dynamic broker logger config that is configured for a specific broker
        DYNAMIC_BROKER_LOGGER_CONFIG = "DYNAMIC_BROKER_LOGGER_CONFIG"
        #: dynamic broker config that is configured for a specific broker
        DYNAMIC_BROKER_CONFIG = "DYNAMIC_BROKER_CONFIG"
        #: dynamic broker config that is configured as default for all brokers in the cluster
        DYNAMIC_DEFAULT_BROKER_CONFIG = "DYNAMIC_DEFAULT_BROKER_CONFIG"
        #: dynamic client metrics subscription config that is configured for all clients
        DYNAMIC_CLIENT_METRICS_CONFIG = "DYNAMIC_CLIENT_METRICS_CONFIG"
        #: dynamic group config that is configured for a specific group
        DYNAMIC_GROUP_CONFIG = "DYNAMIC_GROUP_CONFIG"
        #: static broker config provided as broker properties at start up
        #: (e.g. server.properties file)
        STATIC_BROKER_CONFIG = "STATIC_BROKER_CONFIG"
        #: built-in default configuration for configs that have a default value
        DEFAULT_CONFIG = "DEFAULT_CONFIG"
        #: source unknown e.g. in the ConfigEntry used for alter requests where
        #: source is not set
        UNKNOWN = "UNKNOWN"

        def __str__(self) -> str:
            return self.name

    class ConfigSynonym:
        """Class representing a configuration synonym of a ``ConfigEntry``.

        Java: ``org.apache.kafka.clients.admin.ConfigEntry.ConfigSynonym``.
        """

        __slots__ = ("_name", "_value", "_source")

        _name: str
        _value: str | None
        _source: ConfigEntry.ConfigSource

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise TypeError(
                "ConfigEntry.ConfigSynonym cannot be constructed directly; use "
                "ConfigEntry.synonyms()")

        @classmethod
        def _new(cls, *, name: str, value: str | None,
                 source: ConfigEntry.ConfigSource) -> ConfigEntry.ConfigSynonym:
            """Java's package-private ``ConfigSynonym(String name, String value,
            ConfigSource source)``: the configuration name (this may be
            different from the name of the associated ``ConfigEntry``), value
            and source."""
            synonym = cls.__new__(cls)
            synonym._name = name
            synonym._value = value
            synonym._source = source
            return synonym

        def name(self) -> str:
            """Returns the name of this configuration."""
            return self._name

        def value(self) -> str | None:
            """Returns the value of this configuration, which may be ``None`` if
            the configuration is sensitive."""
            return self._value

        def source(self) -> ConfigEntry.ConfigSource:
            """Returns the source of this configuration."""
            return self._source

        def __eq__(self, other: object) -> bool:
            if self is other:
                return True
            if type(other) is not type(self):
                return NotImplemented
            assert isinstance(other, ConfigEntry.ConfigSynonym)
            return (self._name == other._name and self._value == other._value
                    and self._source is other._source)

        def __hash__(self) -> int:
            return hash((self._name, self._value, self._source))

        def __str__(self) -> str:
            return ("ConfigSynonym(name=" + java_str(self._name) + ", value="
                    + java_str(self._value) + ", source=" + java_str(self._source) + ")")

    __slots__ = ("_name", "_value", "_source", "_is_sensitive", "_is_read_only", "_synonyms",
                 "_type", "_documentation")

    @overload
    def __init__(self, *, name: str, value: str | None) -> None: ...
    @overload
    def __init__(self, *, name: str, value: str | None,
                 source: ConfigEntry.ConfigSource, is_sensitive: bool,
                 is_read_only: bool, synonyms: Sequence[ConfigEntry.ConfigSynonym],
                 type: ConfigEntry.ConfigType | None,  # noqa: A002 - Java's name
                 documentation: str | None) -> None: ...

    @java_forms(Form("name", "value"),
                Form("name", "value", "source", "is_sensitive", "is_read_only", "synonyms",
                     "type", "documentation",
                     defaults={"source": _UNKNOWN_SOURCE, "is_sensitive": False,
                               "is_read_only": False, "synonyms": (), "type": _UNKNOWN_TYPE,
                               "documentation": None}))
    def __init__(self, *, name: str, value: str | None,
                 source: ConfigEntry.ConfigSource = MISSING, is_sensitive: bool = MISSING,
                 is_read_only: bool = MISSING,
                 synonyms: Sequence[ConfigEntry.ConfigSynonym] = MISSING,
                 type: ConfigEntry.ConfigType | None = MISSING,  # noqa: A002 - Java's name
                 documentation: str | None = MISSING) -> None:
        """Create a configuration entry with the provided values.

        ``name``: the non-null config name; ``value``: the config value or
        ``None``; ``source``: the source of this config entry; ``is_sensitive``:
        whether the config value is sensitive, the broker never returns the
        value if it is sensitive; ``is_read_only``: whether the config is
        read-only and cannot be updated; ``synonyms``: synonym configs in order
        of precedence; ``type``: the config data type, ``None`` for Java's null
        type, which ``KafkaAdminClient`` gives the entries of a ``createTopics``
        result.
        """
        if source is _UNKNOWN_SOURCE:
            source = ConfigEntry.ConfigSource.UNKNOWN
        if type is _UNKNOWN_TYPE:
            type = ConfigEntry.ConfigType.UNKNOWN
        if name is None:
            raise TypeError("name must not be None")
        self._name = name
        self._value = value
        self._source = source
        self._is_sensitive = is_sensitive
        self._is_read_only = is_read_only
        self._synonyms: tuple[ConfigEntry.ConfigSynonym, ...] | None = (
            None if synonyms is None else tuple(synonyms))
        self._type = type
        self._documentation = documentation

    def name(self) -> str:
        """Return the config name."""
        return self._name

    def value(self) -> str | None:
        """Return the value or ``None``. ``None`` is returned if the config is
        unset or if ``is_sensitive`` is true."""
        return self._value

    def source(self) -> ConfigEntry.ConfigSource:
        """Return the source of this configuration entry."""
        return self._source

    def is_default(self) -> bool:
        """Return whether the config value is the default or if it's been
        explicitly set."""
        return self._source is ConfigEntry.ConfigSource.DEFAULT_CONFIG

    def is_sensitive(self) -> bool:
        """Return whether the config value is sensitive. The value is always
        set to ``None`` by the broker if the config value is sensitive."""
        return self._is_sensitive

    def is_read_only(self) -> bool:
        """Return whether the config is read-only and cannot be updated."""
        return self._is_read_only

    def synonyms(self) -> list[ConfigEntry.ConfigSynonym]:
        """Returns all config values that may be used as the value of this
        config along with their source, in the order of precedence. The list
        starts with the value returned in this ``ConfigEntry``. The list is
        empty if synonyms were not requested using
        ``DescribeConfigsOptions.include_synonyms``."""
        synonyms = self._synonyms
        # Java stores a null it is given; a list is returned whenever the
        # client builds the entry.
        return None if synonyms is None else list(synonyms)  # type: ignore[return-value]

    def type(self) -> ConfigEntry.ConfigType | None:  # noqa: A003 - Java's name
        """Return the config data type, or ``None`` for the entries of a
        ``createTopics`` result, which Java builds with a null type."""
        return self._type

    def documentation(self) -> str | None:
        """Return the config documentation."""
        return self._documentation

    def __eq__(self, other: object) -> bool:
        if self is other:
            return True
        if type(other) is not type(self):
            return NotImplemented
        assert isinstance(other, ConfigEntry)
        return (self._name == other._name
                and self._value == other._value
                and self._is_sensitive == other._is_sensitive
                and self._is_read_only == other._is_read_only
                and self._source == other._source
                and self._synonyms == other._synonyms
                and self._type == other._type
                and self._documentation == other._documentation)

    def __hash__(self) -> int:
        return hash((self._name, self._value, self._is_sensitive, self._is_read_only,
                     self._source, self._synonyms, self._type, self._documentation))

    def __str__(self) -> str:
        """Redacts a sensitive value. WARNING, the user should be responsible
        to set the correct ``is_sensitive`` field for each config entry."""
        return ("ConfigEntry(name=" + self._name
                + ", value=" + ("Redacted" if self._is_sensitive else java_str(self._value))
                + ", source=" + java_str(self._source)
                + ", isSensitive=" + java_str(self._is_sensitive)
                + ", isReadOnly=" + java_str(self._is_read_only)
                + ", synonyms=" + java_str(self._synonyms)
                + ", type=" + java_str(self._type)
                + ", documentation=" + java_str(self._documentation) + ")")
