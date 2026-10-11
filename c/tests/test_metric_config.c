// Copyright 2025 Confluent Inc.
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

// `org.apache.kafka.common.metrics.MetricConfig` through the C FFI (CLAUDE.md
// §4): a setter Java makes throw on a bad argument returns the error instead
// of being `void`.

#include <confluent_kafka.h>
#include <stddef.h>
#include <stdint.h>

#include "unity.h"

void setUp(void) {}
void tearDown(void) {}

// `MetricConfig.samples(int)` throws `IllegalArgumentException("The number of
// samples must be at least 1.")` below 1 and leaves the field as it was.
static void test_set_samples_below_one_returns_javas_error(void) {
    kafka_common_metrics_MetricConfig_t *config = kafka_common_metrics_MetricConfig_new();
    TEST_ASSERT_NULL(kafka_common_metrics_MetricConfig_set_samples(config, 5));
    TEST_ASSERT_EQUAL_INT32(5, kafka_common_metrics_MetricConfig_samples(config));

    const int32_t rejected[] = {0, -1, INT32_MIN};
    for (size_t i = 0; i < sizeof(rejected) / sizeof(rejected[0]); i++) {
        kafka_common_Error_t *error = kafka_common_metrics_MetricConfig_set_samples(config, rejected[i]);
        TEST_ASSERT_NOT_NULL(error);
        TEST_ASSERT_EQUAL_INT(kafka_common_ErrorCode_LOCAL_ILLEGAL_ARGUMENT, kafka_common_Error_code(error));
        TEST_ASSERT_TRUE(kafka_common_Error_is_local_illegal_argument_error(error));
        TEST_ASSERT_EQUAL_STRING("The number of samples must be at least 1.", kafka_common_Error_message(error));
        kafka_common_Error_destroy(error);
        TEST_ASSERT_EQUAL_INT32(5, kafka_common_metrics_MetricConfig_samples(config));
    }
    kafka_common_metrics_MetricConfig_destroy(config);
}

int main(void) {
    UNITY_BEGIN();
    RUN_TEST(test_set_samples_below_one_returns_javas_error);
    return UNITY_END();
}
