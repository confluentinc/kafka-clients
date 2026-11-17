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

#include <cstdint>
#include <exception>
#include <string>
#include <string_view>

extern "C" int32_t kafka_widget_poll_cb();

namespace confluent::kafka::common {
class Widget {  // EXPECT: confluent-kafka-java-members has no method unsupported translating Java's org.apache.kafka.common.Widget#unsupported(int,int)
public:
    static constexpr std::string_view KIND = "widget";
    static const Widget& EMPTY();
    explicit Widget(std::string_view name);
    void failure(std::exception_ptr failure);
    [[nodiscard]] std::string name() const;
    [[nodiscard]] int32_t count() const { return kafka_widget_poll_cb(); }  // EXPECT: confluent-kafka-java-members is a _cb function
    static Widget parse(std::string_view text);
    [[nodiscard]] int32_t extra() const;  // EXPECT: confluent-kafka-java-members translates nothing of Java's org.apache.kafka.common.Widget
    int32_t visible = 0;  // EXPECT: confluent-kafka-java-members is a public data member
};
inline void stray_function() {}  // EXPECT: confluent-kafka-java-members translates no Java member
}  // namespace confluent::kafka::common
