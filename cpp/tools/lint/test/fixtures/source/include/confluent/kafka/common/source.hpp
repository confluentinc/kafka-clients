// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#pragma once

// Stands for a source of the library: the check reads function bodies, and the
// fixture runner compiles headers.

#include <cstdint>

extern "C" int32_t kafka_widget_poll_cb();
extern "C" int32_t kafka_widget_poll_err();

namespace confluent::kafka::common {
inline int32_t calls_cb() { return kafka_widget_poll_cb(); }  // EXPECT: confluent-kafka-source is a _cb function
inline int32_t calls_err() { return kafka_widget_poll_err(); }
}  // namespace confluent::kafka::common
