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

from io import StringIO

from translation_agent import streaming


def test_run_with_prefix_captures_output_and_returns_rc():
    sink = StringIO()
    rc, captured = streaming.run_with_prefix(
        ["sh", "-c", "echo line1; echo line2; echo err >&2"],
        pr_number=42,
        out_stream=sink,
    )
    assert rc == 0
    # Both stdout and stderr are merged into captured.
    assert "line1" in captured
    assert "line2" in captured
    assert "err" in captured
    # Prefix appears at least once for the end-of-stream flush.
    assert "From agent #42" in sink.getvalue()


def test_run_with_prefix_propagates_nonzero_exit():
    sink = StringIO()
    rc, _ = streaming.run_with_prefix(
        ["sh", "-c", "echo hi; exit 7"],
        pr_number=1,
        out_stream=sink,
    )
    assert rc == 7


def test_run_with_prefix_flushes_every_100_lines():
    sink = StringIO()
    rc, captured = streaming.run_with_prefix(
        # Print 250 lines so we get 2 mid-stream flushes plus an end flush.
        ["sh", "-c", "for i in $(seq 1 250); do echo \"line $i\"; done"],
        pr_number=99,
        out_stream=sink,
    )
    assert rc == 0
    # Three prefix occurrences expected (100 + 100 + 50).
    assert sink.getvalue().count(">>>>> From agent #99") == 3
    # Captured has all 250 lines.
    assert captured.count("\n") == 250


def test_run_with_prefix_handles_empty_output():
    sink = StringIO()
    rc, captured = streaming.run_with_prefix(
        ["true"], pr_number=5, out_stream=sink,
    )
    assert rc == 0
    assert captured == ""
    # No prefix when there's nothing to flush.
    assert sink.getvalue() == ""
