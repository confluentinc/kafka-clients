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

#include <compare>
#include <cstdint>

#include <confluent/kafka/common/producer.hpp>

namespace confluent::kafka::common {
class Good : public Producer<int32_t, int32_t> {
public:
    void send(const int32_t& key) override;
};
}  // namespace confluent::kafka::common
