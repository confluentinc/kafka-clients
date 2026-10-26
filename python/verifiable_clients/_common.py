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

"""What ``verifiable_producer`` and ``verifiable_consumer`` share.

Java gives each tool its own copy of ``printJson`` and the shutdown hook, and
the argument parser and ``loadProps`` come from argparse4j and
``org.apache.kafka.common.utils.Utils``; here they are written once.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from types import FrameType
from typing import Any, Callable

__all__ = [
    "OUTPUT_LOCK",
    "ArgumentParser",
    "ArgumentParserError",
    "ShutdownHook",
    "configure_logging",
    "event",
    "load_props",
    "now_millis",
    "parse_properties",
    "print_json",
]

# Java's `synchronized (System.out)`: one JSON object per line, never interleaved,
# from the main thread, the shutdown hook and the producer's completion thread.
# Re-entrant because the producer's callback prints while holding it.
OUTPUT_LOCK = threading.RLock()


def now_millis() -> int:
    """Java's ``System.currentTimeMillis()``."""
    return time.time_ns() // 1_000_000


def event(name: str, **fields: Any) -> dict[str, Any]:
    """An event as Jackson writes it: Java's ``@JsonPropertyOrder({"timestamp",
    "name"})`` puts ``timestamp`` then ``name`` first, and the event's own
    properties follow in declaration order."""
    return {"timestamp": now_millis(), "name": name, **fields}


def print_json(data: dict[str, Any]) -> None:
    """Java's ``printJson``: one compact JSON object per line on stdout, or a
    diagnostic if the data cannot be written as JSON.

    stdout is flushed after each line: the system tests read it through a pipe,
    where Python would otherwise buffer whole blocks and the harness would see
    events late or, after a ``SIGKILL``, never.

    A failed write is dropped, as Java's ``PrintStream`` only sets its error
    flag: a closed pipe must not end the loop or skip the shutdown sequence.
    """
    try:
        line = json.dumps(data, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as e:
        line = f"Bad data can't be written as json: {e}"
    with OUTPUT_LOCK:
        try:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
        except OSError:
            _discard_stdout()


def _discard_stdout() -> None:
    """Point stdout at the null device after a failed write, so the next
    writes, and the flush at interpreter exit, do not fail again on the same
    broken pipe (the course Python's documentation gives for ``SIGPIPE``)."""
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        os.close(devnull)
    except (OSError, ValueError):
        pass


def configure_logging() -> None:
    """Send the tool's and the client's log records to stderr, as the Java
    tools' log4j configuration keeps logs off stdout, in log4j's tools layout.
    ``VERIFIABLE_CLIENT_LOG_LEVEL`` sets the level (``WARNING`` by default)."""
    logging.basicConfig(
        stream=sys.stderr,
        level=os.environ.get("VERIFIABLE_CLIENT_LOG_LEVEL", "WARNING").upper(),
        format="[%(asctime)s] %(levelname)s %(message)s (%(name)s)",
    )


class ArgumentParserError(Exception):
    """argparse4j's ``ArgumentParserException``: a command line the parser
    rejects, or one ``createFromArgs`` rejects after parsing."""


class ArgumentParser(argparse.ArgumentParser):
    """The argparse4j parser the Java tools use (argparse4j is a port of
    Python's ``argparse``). An error raises ``ArgumentParserError`` instead of
    exiting, as argparse4j throws to ``main``, which prints it and exits 1."""

    def __init__(self, prog: str, description: str) -> None:
        # Java's `.defaultHelp(true)`: the help shows each default.
        super().__init__(prog=prog, description=description, allow_abbrev=False,
                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    def error(self, message: str) -> Any:
        raise ArgumentParserError(message)


def load_props(filename: str) -> dict[str, str]:
    """Java's ``Utils.loadProps``: read a properties file. An unreadable file
    raises ``ArgumentParserError`` with the I/O error's message, as
    ``createFromArgs`` rethrows the ``IOException``."""
    try:
        # java.util.Properties.load(InputStream) reads ISO 8859-1.
        with open(filename, encoding="latin-1") as file:
            contents = file.read()
    except OSError as e:
        raise ArgumentParserError(str(e)) from e
    return parse_properties(contents)


# The whitespace java.util.Properties skips: space, tab and form feed.
_PROPERTIES_WHITESPACE = " \t\f"


def parse_properties(contents: str) -> dict[str, str]:
    """``java.util.Properties.load``: ``key=value``, ``key:value`` and
    ``key value`` lines, ``#`` and ``!`` comment lines, blank lines, a line
    ending in an odd number of backslashes continued on the next one (whose
    leading whitespace is dropped), and backslash escapes in keys and values.
    Kafka's system tests write multi-line values this way: ``sasl.jaas.config``
    for a SASL_PLAINTEXT client spans the JAAS file's lines. A value is also
    trimmed, as Java's ``ConfigDef`` trims what it reads. A later line for a
    key replaces an earlier one. A malformed ``\\uXXXX`` escape raises
    ``ValueError``, as Java's ``IllegalArgumentException`` ends its ``main``."""
    props: dict[str, str] = {}
    for line in _logical_lines(contents):
        key, value = _split_property(line)
        props[key] = value.strip(_PROPERTIES_WHITESPACE)
    return props


def _logical_lines(contents: str) -> list[str]:
    """``Properties.LineReader``: the logical lines, comments and blank lines
    left out, each continued line joined to the next without its leading
    whitespace. A comment character starts a comment only at the start of a
    natural line, never on a continuation."""
    lines: list[str] = []
    current: str | None = None
    for natural in contents.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = natural.lstrip(_PROPERTIES_WHITESPACE)
        if current is None and (not line or line[0] in "#!"):
            continue
        backslashes = len(line) - len(line.rstrip("\\"))
        continued = backslashes % 2 == 1
        part = line[:-1] if continued else line
        current = part if current is None else current + part
        if not continued:
            lines.append(current)
            current = None
    if current is not None:
        lines.append(current)
    return lines


def _split_property(line: str) -> tuple[str, str]:
    """``Properties.load0``: the key runs up to the first unescaped ``=``,
    ``:`` or whitespace; whitespace and one ``=`` or ``:`` separator are then
    skipped, and the rest is the value."""
    key_end = len(line)
    value_start = len(line)
    has_separator = False
    preceding_backslash = False
    for i, c in enumerate(line):
        if not preceding_backslash and c in "=:":
            key_end, value_start, has_separator = i, i + 1, True
            break
        if not preceding_backslash and c in _PROPERTIES_WHITESPACE:
            key_end, value_start = i, i + 1
            break
        preceding_backslash = c == "\\" and not preceding_backslash
    while value_start < len(line):
        c = line[value_start]
        if c not in _PROPERTIES_WHITESPACE:
            if has_separator or c not in "=:":
                break
            has_separator = True
        value_start += 1
    return _unescape(line[:key_end]), _unescape(line[value_start:])


def _unescape(text: str) -> str:
    """``Properties.loadConvert``: ``\\t``, ``\\n``, ``\\r``, ``\\f`` and
    ``\\uXXXX`` escapes, and any other escaped character as itself."""
    if "\\" not in text:
        return text
    out: list[str] = []
    i = 0
    while i < len(text):
        c = text[i]
        i += 1
        if c != "\\" or i == len(text):
            out.append(c)
            continue
        c = text[i]
        i += 1
        if c == "u":
            digits = text[i:i + 4]
            if len(digits) < 4 or any(d not in "0123456789abcdefABCDEF" for d in digits):
                raise ValueError("Malformed \\uxxxx encoding.")
            out.append(chr(int(digits, 16)))
            i += 4
        else:
            out.append({"t": "\t", "r": "\r", "n": "\n", "f": "\f"}.get(c, c))
    return "".join(out)


class ShutdownHook:
    """Java's ``Runtime.addShutdownHook(new Thread(hook, name))``: the JVM runs
    a shutdown hook on its own thread on ``SIGINT`` and ``SIGTERM``, the signal
    the system tests send for a clean shutdown.

    The signal handler only sets an event; ``hook`` runs on the hook's own
    thread, at the first signal, once. A handler that printed could run in the
    middle of the main thread's own write to stdout, which Python rejects as a
    re-entrant call. Replacing the ``SIGINT`` handler also keeps
    ``KeyboardInterrupt`` from cutting the tool short, as Ctrl+C only runs the
    hook in Java.
    """

    def __init__(self, hook: Callable[[], None], name: str) -> None:
        self._requested = threading.Event()
        self._thread = threading.Thread(target=self._run, args=(hook,), name=name, daemon=True)
        self._thread.start()
        signal.signal(signal.SIGINT, self._on_signal)
        signal.signal(signal.SIGTERM, self._on_signal)

    def _on_signal(self, signum: int, frame: FrameType | None) -> None:
        self._requested.set()

    def _run(self, hook: Callable[[], None]) -> None:
        self._requested.wait()
        hook()

    def finish(self) -> None:
        """Called by the main thread as it ends: ignore any later signal, and
        wait for a hook already started, as the JVM waits for its shutdown
        hooks before it exits. Signal handlers run on the main thread, so once
        the handlers are replaced no hook can start."""
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        if self._requested.is_set():
            self._thread.join()
