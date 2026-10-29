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

# The Node-API addon over libconfluent_kafka's C FFI. It includes only
# confluent_kafka_bindings.h and links the library dynamically, with an rpath
# to its directory, as the Python extension does. The library is a
# prerequisite (`make -C rust build-all-features`); CONFLUENT_KAFKA_LIB_DIR and
# CONFLUENT_KAFKA_INCLUDE_DIR override where it and the header are found.
{
  "variables": {
    "ck_lib_dir": "<!(node -p \"require('path').resolve(process.env.CONFLUENT_KAFKA_LIB_DIR || '../rust/target/release')\")",
    "ck_include_dir": "<!(node -p \"require('path').resolve(process.env.CONFLUENT_KAFKA_INCLUDE_DIR || '../rust/target/include')\")"
  },
  "targets": [
    {
      "target_name": "confluent_kafka_js",
      "sources": [
        "src/addon.c"
      ],
      "include_dirs": [
        "<(ck_include_dir)"
      ],
      "defines": [
        "NAPI_VERSION=8"
      ],
      "cflags_c": [
        "-std=c11",
        "-Wall",
        "-Wextra",
        "-Werror"
      ],
      "libraries": [
        "-L<(ck_lib_dir)",
        "-lconfluent_kafka"
      ],
      "conditions": [
        [
          "OS=='mac'",
          {
            "xcode_settings": {
              "GCC_C_LANGUAGE_STANDARD": "c11",
              "WARNING_CFLAGS": [
                "-Wall",
                "-Wextra",
                "-Werror"
              ],
              "OTHER_LDFLAGS": [
                "-Wl,-rpath,<(ck_lib_dir)"
              ],
              "MACOSX_DEPLOYMENT_TARGET": "11.0"
            }
          }
        ],
        [
          "OS=='linux'",
          {
            "ldflags": [
              "-Wl,-rpath,<(ck_lib_dir)"
            ]
          }
        ]
      ]
    }
  ]
}
