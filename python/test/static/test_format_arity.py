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

"""Static format-arity gate over ``_confluentkafka.c``, and the scanner's own
tests: a gate is only worth as much as the parser behind it.

Needs neither the Rust library nor the C extension to be built.
"""

from tools.format_arity import DEFAULT_SOURCE, count_sites, scan


def details(src):
    return [f"{f.line}:{f.detail}" for f in scan(src)]


def test_confluentkafka_c_has_no_arity_mismatches():
    source = DEFAULT_SOURCE.read_text(encoding="utf-8")
    assert sum(count_sites(source)) > 0, "no call sites found: wrong file?"
    findings = [f"{DEFAULT_SOURCE.name}:{f.line}: {f.function}: {f.detail}"
                for f in scan(source)]
    assert not findings, "\n".join(findings)


def test_a_matching_build_value_call_is_clean():
    assert scan('x = Py_BuildValue("(sON)", a, b, c);') == []


def test_one_unit_short_is_reported_with_both_counts():
    # The shape of the real defect fixed in 761da3b2: ten units, eleven
    # arguments.
    src = 'x = Py_BuildValue("(sONsssNNNN)", a, b, c, d, e, f, g, h, i, j, k);'
    found = details(src)
    assert len(found) == 1, found
    assert ("needs 10 format unit(s), so the call takes 11 argument(s), "
            "but 12 were passed") in found[0], found


def test_one_unit_too_many_is_reported():
    found = details('Py_BuildValue("(ss)", a);')
    assert len(found) == 1, found
    assert "needs 2 format unit(s)" in found[0], found


def test_two_argument_units_are_counted_as_two():
    assert scan('Py_BuildValue("(s#O&)", buf, len, conv, obj);') == []
    assert len(details('Py_BuildValue("(s#)", buf);')) == 1


def test_nested_structure_characters_consume_nothing():
    assert scan('Py_BuildValue("{s:i,s:i}", a, b, c, d);') == []
    assert scan('Py_BuildValue("[(si)(si)]", a, b, c, d);') == []


def test_empty_format_takes_no_arguments():
    assert scan('Py_BuildValue("");') == []
    assert len(details('Py_BuildValue("", x);')) == 1


def test_nested_calls_and_commas_inside_them_do_not_split_arguments():
    src = 'Py_BuildValue("(sN)", topic, make_tuple(a, b, c));'
    assert scan(src) == [], details(src)
    # Braced initializer and subscript, likewise.
    assert scan('Py_BuildValue("(ii)", arr[i, j], (int){1, 2});') == []


def test_a_comma_inside_a_string_or_char_literal_does_not_split():
    assert scan('Py_BuildValue("(ss)", "a,b", "c,d");') == []
    assert scan("Py_BuildValue(\"(ci)\", ',', n);") == []


def test_adjacent_string_literals_are_concatenated():
    assert scan('Py_BuildValue("(s" "s)", a, b);') == []
    assert len(details('Py_BuildValue("(s" "s)", a);')) == 1


def test_parse_tuple_counts_its_two_fixed_arguments():
    assert scan('PyArg_ParseTuple(args, "sO", &a, &b);') == []
    found = details('PyArg_ParseTuple(args, "sO", &a);')
    assert len(found) == 1, found
    assert "takes 4 argument(s), but 3 were passed" in found[0], found


def test_parse_tuple_and_keywords_counts_its_four_fixed_arguments():
    assert scan(
        'PyArg_ParseTupleAndKeywords(args, kwargs, "sO", kwlist, &a, &b);') == []
    assert len(details(
        'PyArg_ParseTupleAndKeywords(args, kwargs, "sO", kwlist, &a);')) == 1


def test_parse_only_grammar_is_honoured():
    # `|` and `$` consume nothing; `:name` terminates the format.
    assert scan('PyArg_ParseTuple(args, "s|i:fn", &a, &b);') == []
    assert scan(
        'PyArg_ParseTupleAndKeywords(args, kw, "s|$i", kwlist, &a, &b);') == []
    # `O!` and `O&` take two pointers, `s*` one, `es#` three.
    assert scan('PyArg_ParseTuple(args, "O!O&s*", &t, &o, &c, &o2, &buf);') == []
    assert scan('PyArg_ParseTuple(args, "es#", &enc, &buf, &len);') == []


def test_the_terminator_is_not_a_terminator_for_py_build_value():
    # `:` is a dict separator in a build format, not an end marker.
    assert scan('Py_BuildValue("{s:s}", k, v);') == []


def test_calls_inside_comments_and_strings_are_ignored():
    assert scan('// Py_BuildValue("(ss)", a);\n') == []
    assert scan('/* Py_BuildValue("(ss)", a);\n   more */\n') == []
    assert scan('const char *doc = "Py_BuildValue(\\"(ss)\\", a)";') == []


def test_a_longer_identifier_is_not_a_match():
    assert scan('my_Py_BuildValue("(ss)", a);') == []
    assert scan('Py_BuildValueX("(ss)", a);') == []


def test_parse_tuple_is_not_double_counted_as_the_keywords_variant():
    # `PyArg_ParseTuple` is a strict prefix of `PyArg_ParseTupleAndKeywords`;
    # a wrong boundary check would either scan the keywords call twice or
    # charge it the wrong fixed count.
    src = 'PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &a);'
    assert scan(src) == [], details(src)
    assert count_sites(src) == (0, 0, 1)


def test_a_non_literal_format_is_reported_rather_than_skipped():
    found = details("Py_BuildValue(fmt, a, b);")
    assert len(found) == 1, found
    assert "not a string literal" in found[0], found


def test_an_unknown_format_unit_is_reported():
    found = details('Py_BuildValue("(sQ)", a, b);')
    assert len(found) == 1, found
    assert "unknown format unit `Q`" in found[0], found


def test_line_numbers_survive_comment_stripping_and_wrapping():
    src = ('/* a\n   multi-line\n   comment */\n'
           'Py_BuildValue(\n    "(ss)",\n    a);\n')
    found = scan(src)
    assert len(found) == 1
    assert found[0].line == 4


def test_site_counting_matches_the_scanned_calls():
    src = '''
        Py_BuildValue("(s)", a);
        PyObject_CallFunction(cb, "K", n);
        PyArg_ParseTuple(args, "s", &a);
        PyArg_ParseTupleAndKeywords(args, kw, "s", kwlist, &a);
    '''
    assert count_sites(src) == (1, 1, 2)
    assert scan(src) == []


def test_a_matching_call_function_is_clean_and_a_short_one_is_reported():
    # Two fixed arguments (the callable and the format), then one per unit.
    assert scan('PyObject_CallFunction(cb, "KK", a, b);') == []
    assert scan('PyObject_CallFunction(cb, "LisL", a, b, c, d);') == []
    found = details('PyObject_CallFunction(cb, "KK", a);')
    assert len(found) == 1, found
    assert ("needs 2 format unit(s), so the call takes 4 argument(s), "
            "but 3 were passed") in found[0], found


def test_call_function_uses_the_build_value_grammar_not_the_parse_one():
    # `|` is parse-only: under the build grammar it is an unknown unit, so a
    # `PyObject_CallFunction` misclassified as a parse call would go
    # unreported here.
    found = details('PyObject_CallFunction(cb, "s|i", a, b);')
    assert len(found) == 1, found
    assert "unknown format unit `|`" in found[0], found


def test_call_function_obj_args_is_not_scanned_as_call_function():
    # `PyObject_CallFunction` is a strict prefix of
    # `PyObject_CallFunctionObjArgs`, which is NULL-terminated and takes no
    # format at all -- scanning it would report its first argument as a
    # non-literal format.
    src = "PyObject_CallFunctionObjArgs(cb, a, b, NULL);"
    assert scan(src) == [], details(src)
    assert count_sites(src) == (0, 0, 0)


def test_an_unbalanced_call_is_reported_not_silently_dropped():
    found = details('Py_BuildValue("(ss)", a, b\n')
    assert len(found) == 1, found
    assert "unterminated" in found[0], found
