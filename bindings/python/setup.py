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

"""Build configuration for the Confluent Kafka Rust Python bindings."""

from setuptools import setup, Extension
import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
include_dir = os.path.join(project_root, 'target', 'include')
lib_dir = os.environ.get('CONFLUENT_KAFKA_LIB_DIR',
                         os.path.join(project_root, 'target', 'release'))

# MSVC has no rpath (runtime_library_dirs) and rejects the gcc-style -std flag;
# on Windows the linked DLL is found via delvewheel vendoring it into the wheel.
if sys.platform == 'win32':
    extra_compile_args = []
    runtime_library_dirs = []
else:
    extra_compile_args = ['-std=c99']
    runtime_library_dirs = [lib_dir]

ext = Extension(
    '_confluentkafka',
    sources=['_confluentkafka.c', 'tinycthread.c'],
    include_dirs=[include_dir],
    library_dirs=[lib_dir],
    libraries=['confluent_kafka'],
    extra_compile_args=extra_compile_args,
    runtime_library_dirs=runtime_library_dirs,
)

setup(ext_modules=[ext])
