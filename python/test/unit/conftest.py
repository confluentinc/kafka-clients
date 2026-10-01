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

"""Fixtures shared by the unit test modules."""

import sys

import pytest


@pytest.fixture
def two_gib_bytes():
    """A zero-filled ``bytes`` of exactly 2 GiB (``2**31`` bytes): one byte more
    than the ``int32_t`` lengths of the C API can carry.

    ``bytes(n)`` is allocated with ``calloc``, so its pages are mapped lazily and
    the buffer costs almost no resident memory. It needs a 64-bit interpreter,
    and a test using it is skipped rather than failed where it cannot be
    allocated.
    """
    if sys.maxsize <= 2**32:
        pytest.skip("a 2 GiB buffer needs a 64-bit Python")
    try:
        return bytes(2**31)
    except MemoryError:
        pytest.skip("could not allocate a 2 GiB buffer")
