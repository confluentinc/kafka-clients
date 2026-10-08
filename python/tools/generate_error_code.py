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

"""Generates ``_error_code.py`` from ``kafka_common_ErrorCode_e``.

``rust/src/ffi/common.rs``'s ``kafka_common_ErrorCode_e`` is the one place the
error codes are declared. The C extension sees them through the
cbindgen-generated header; the pure-Python modules cannot include that header,
so they get a generated copy of the values. Copies are what drift, so
``test/static/test_error_code_generated.py`` re-renders the file and fails on
any difference.

Usage::

    python tools/generate_error_code.py
"""

import re
import sys
from pathlib import Path

_PYTHON_DIR = Path(__file__).resolve().parent.parent
ERROR_CODE_SOURCE = _PYTHON_DIR.parent / "rust" / "src" / "ffi" / "common.rs"
ERROR_CODE_PY = _PYTHON_DIR / "_error_code.py"

_HEADER = '''\
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

"""Error-code constants -- GENERATED, DO NOT EDIT.

Generated from kafka_common_ErrorCode_e in rust/src/ffi/common.rs by
`python tools/generate_error_code.py`, and checked for staleness by
test/static/test_error_code_generated.py.

Private plumbing, not public API: KafkaError exposes `code`, `message` and
`is_retriable`, and neither producer.py nor consumer.py re-exports this module.
The users are the gRPC test servers, which stamp the real code on their own
synthetic errors, and the unit tests, which compare a code instead of matching
message text.

Values are the FFI error codes: Java's wire codes at Java's own values, plus
negatives for the classes only the client raises. They are injective over the
error classes, so the code alone identifies the class.
"""

'''

_ENUMERATOR = re.compile(r"^kafka_common_ErrorCode_e_(\w+) = (-?\d+),?$")


def parse_error_codes(source=None):
    """Returns ``(name, value)`` for every enumerator of
    ``kafka_common_ErrorCode_e``, in declaration order."""
    if source is None:
        source = ERROR_CODE_SOURCE.read_text(encoding="utf-8")
    _, found, body = source.partition("pub enum kafka_common_ErrorCode_e {")
    if not found:
        raise ValueError(
            f"{ERROR_CODE_SOURCE}: kafka_common_ErrorCode_e not found")
    # The enum is the only item declared before the next top-level `}`.
    body = body.split("\n}", 1)[0]

    codes = []
    for line in body.splitlines():
        m = _ENUMERATOR.match(line.strip())
        if m:
            codes.append((m.group(1), int(m.group(2))))
    if not codes:
        raise ValueError(
            f"{ERROR_CODE_SOURCE}: kafka_common_ErrorCode_e has no enumerators")
    return codes


def render(codes=None):
    """Returns the full text of ``_error_code.py``."""
    if codes is None:
        codes = parse_error_codes()
    return _HEADER + "".join(f"{name} = {value}\n" for name, value in codes)


def main():
    codes = parse_error_codes()
    ERROR_CODE_PY.write_text(render(codes), encoding="utf-8")
    print(f"Wrote {len(codes)} constants to {ERROR_CODE_PY}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
