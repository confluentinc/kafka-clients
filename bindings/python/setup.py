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

sources = ['_confluentkafka.c']
include_dirs = [include_dir]
if sys.platform == 'darwin':
    # Apple's platform libc doesn't ship C11 <threads.h>; _confluentkafka.c
    # falls back to the vendored tinycthread shim there (see
    # third_party/tinycthread/README.md). Linux keeps using glibc's native
    # <threads.h>, so tinycthread.c is only built on macOS.
    tinycthread_dir = os.path.join('third_party', 'tinycthread')
    sources.append(os.path.join(tinycthread_dir, 'tinycthread.c'))
    include_dirs.append(tinycthread_dir)

ext = Extension(
    '_confluentkafka',
    sources=sources,
    include_dirs=include_dirs,
    library_dirs=[lib_dir],
    libraries=['confluent_kafka'],
    extra_compile_args=['-std=c99'],
    runtime_library_dirs=[lib_dir],
)

setup(ext_modules=[ext])
