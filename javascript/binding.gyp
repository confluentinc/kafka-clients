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
# confluent_kafka_bindings.h. The library is a prerequisite (`make -C rust
# build-all-features`, or `make -C rust build`); CONFLUENT_KAFKA_LIB_DIR and
# CONFLUENT_KAFKA_INCLUDE_DIR override where it and the header are found.
#
# CONFLUENT_KAFKA_LINK chooses how the addon links the library:
#   dynamic (the default) links libconfluent_kafka.{so,dylib}, with an rpath to
#     its directory, as the Python extension does: the development build, the
#     tests and the gRPC test image.
#   static links libconfluent_kafka.a into the addon, so the addon is the one
#     file an npm platform package carries (.semaphore/build-javascript-native.sh).
#     The linker drops the code the addon does not reach, and the addon exports
#     only the two Node-API entry points: addon.exports.map on Linux, the
#     -exported_symbol flags on macOS. The system libraries are those the Rust
#     core needs besides libc: libpthread, libdl and libm on Linux (as
#     c/CMakeLists.txt links them), libiconv on macOS.
{
  "variables": {
    "ck_lib_dir": "<!(node -p \"require('path').resolve(process.env.CONFLUENT_KAFKA_LIB_DIR || '../rust/target/release')\")",
    "ck_include_dir": "<!(node -p \"require('path').resolve(process.env.CONFLUENT_KAFKA_INCLUDE_DIR || '../rust/target/include')\")",
    "ck_link%": "<!(node -p \"const l = process.env.CONFLUENT_KAFKA_LINK || 'dynamic'; if (l !== 'dynamic' && l !== 'static') throw new Error('CONFLUENT_KAFKA_LINK is ' + JSON.stringify(l) + ', not dynamic or static'); l\")",
    "ck_exports_map": "<!(node -p \"require('path').resolve('addon.exports.map')\")"
  },
  "targets": [
    {
      "target_name": "confluent-kafka-javascript",
      "sources": [
        "src/addon.c",
        "src/callback_queue.c",
        "src/error.c",
        "src/producer.c",
        "src/record.c"
      ],
      "include_dirs": [
        "<(ck_include_dir)",
        "src"
      ],
      "actions": [
        {
          # An error's errorName: each kafka_common_Error_e value's enumerator name,
          # read from the header this addon is built against.
          "action_name": "error_names",
          "inputs": [
            "scripts/gen_error_names.js",
            "scripts/header.js",
            "<(ck_include_dir)/confluent_kafka_bindings.h"
          ],
          "outputs": [
            "<(INTERMEDIATE_DIR)/error_names.c"
          ],
          "action": [
            "node",
            "scripts/gen_error_names.js",
            "<(ck_include_dir)/confluent_kafka_bindings.h",
            "<(INTERMEDIATE_DIR)/error_names.c"
          ],
          "process_outputs_as_sources": 1
        }
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
              "MACOSX_DEPLOYMENT_TARGET": "11.0"
            }
          }
        ],
        [
          "ck_link=='dynamic'",
          {
            "libraries": [
              "-L<(ck_lib_dir)",
              "-lconfluent_kafka"
            ],
            "conditions": [
              [
                "OS=='mac'",
                {
                  "xcode_settings": {
                    "OTHER_LDFLAGS": [
                      "-Wl,-rpath,<(ck_lib_dir)"
                    ]
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
        ],
        [
          "ck_link=='static'",
          {
            "conditions": [
              [
                "OS=='mac'",
                {
                  "libraries": [
                    "<(ck_lib_dir)/libconfluent_kafka.a",
                    "-liconv"
                  ],
                  "xcode_settings": {
                    "OTHER_LDFLAGS": [
                      "-Wl,-dead_strip",
                      "-Wl,-exported_symbol,_napi_register_module_v1",
                      "-Wl,-exported_symbol,_node_api_module_get_api_version_v1"
                    ]
                  }
                }
              ],
              [
                "OS=='linux'",
                {
                  "libraries": [
                    "<(ck_lib_dir)/libconfluent_kafka.a",
                    "-lpthread",
                    "-ldl",
                    "-lm"
                  ],
                  "ldflags": [
                    "-Wl,--gc-sections",
                    "-Wl,--as-needed",
                    "-Wl,--version-script=<(ck_exports_map)"
                  ]
                }
              ]
            ]
          }
        ]
      ]
    }
  ]
}
