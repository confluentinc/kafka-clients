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

"""Static gate: ``_confluentkafka.c`` calls the C API only through the
``_err`` twins of ``confluent_kafka_bindings.h``, and the scanner's own tests.

Needs neither the Rust library nor the C extension to be built.
"""

from tools.ffi_twins import count_names, scan
from tools.format_arity import DEFAULT_SOURCE


def names(src):
    return [f.name for f in scan(src)]


def test_confluentkafka_c_uses_only_err_twins():
    source = DEFAULT_SOURCE.read_text(encoding="utf-8")
    assert count_names(source) > 0, "no kafka_* names found: wrong file?"
    assert "#include <confluent_kafka_bindings.h>" in source
    findings = [f"{DEFAULT_SOURCE.name}:{f.line}: {f.name}" for f in scan(source)]
    assert not findings, "\n".join(findings)


def test_a_twin_call_is_clean():
    assert scan("if (ffi_raise(kafka_common_Uuid_new_err(1, 2, &u))) return NULL;") == []


def test_a_plain_call_is_reported_with_its_line():
    found = scan("int a;\nkafka_common_Uuid_t* u = kafka_common_Uuid_new(1, 2);\n")
    assert [(f.line, f.name) for f in found] == [(2, "kafka_common_Uuid_new")]


def test_a_plain_function_pointer_is_reported():
    # Compiles under a cast even with only the bindings header included.
    assert names("obj_list_init(&l, n, (handle_destroy_fn)kafka_admin_FeatureUpdate_destroy);") == [
        "kafka_admin_FeatureUpdate_destroy"]


def test_types_and_enumerators_are_clean():
    src = ("kafka_common_Error_t* e; kafka_common_Error_e code = kafka_common_Error_LOCAL_TIMEOUT;\n"
           "case kafka_admin_ConfigEntry_ConfigType_BOOLEAN: break;")
    assert scan(src) == []


def test_a_constant_case_name_that_is_called_is_reported():
    assert names("kafka_common_Error_WAKEUP();") == ["kafka_common_Error_WAKEUP"]


def test_a_name_in_a_comment_or_a_literal_is_not_scanned():
    src = ('// kafka_common_Uuid_new(1, 2)\n/* kafka_List_size(l) */\n'
           'PyErr_SetString(PyExc_TypeError, "kafka_List_get failed");')
    assert scan(src) == []


def test_a_function_the_file_defines_is_clean():
    src = ("static PyObject* kafka_principal_to_py(const void* p) { return NULL; }\n"
           "x = kafka_principal_to_py(p);")
    assert scan(src) == []


def test_a_pasted_name_is_judged_by_its_last_piece():
    assert scan("x = kafka_admin_##Rpc##Options_new_err(&var);") == []
    assert scan("kafka_admin_##Rpc##Options_t* var = NULL;") == []
    assert names("kafka_admin_##Rpc##Options_new();") == ["kafka_admin_##…Options_new"]


def test_the_c_users_header_is_reported():
    assert names("#include <confluent_kafka.h>\n") == ["confluent_kafka.h"]
    assert scan("#include <confluent_kafka_bindings.h>\n") == []
