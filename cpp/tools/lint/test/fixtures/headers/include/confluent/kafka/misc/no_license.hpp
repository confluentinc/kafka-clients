// Fixture header.

#pragma once

namespace confluent::kafka::misc {
inline int no_license() { return 0; }
// EXPECT@1: confluent-kafka-header lacks the Apache 2.0 header
}  // namespace confluent::kafka::misc
