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

// The typing test of a Duration parameter against a real Temporal.Duration
// type, the @js-temporal/polyfill's: compiled by test/typing/typing.test.js
// beside usage.ts, under an ES2022 lib (the polyfill's declarations use Intl
// types an ES2020 lib lacks). The client types a Duration by its structure, so
// the built-in Temporal of Node 26 and any polyfill's are accepted.

import { Temporal } from '@js-temporal/polyfill';
import { type CloseArgs, KafkaProducer } from '@confluentinc/kafka-javascript/producer';

async function close(producer: KafkaProducer): Promise<void> {
  const options: CloseArgs = { timeout: Temporal.Duration.from({ seconds: 5 }) };
  await producer.close(options);
  await producer.close({ timeout: Temporal.Duration.from({ milliseconds: 250 }) });
  await producer.close(Temporal.Duration.from({ minutes: 1 }));
  // @ts-expect-error another Temporal type is not a Duration
  await producer.close({ timeout: Temporal.Instant.fromEpochMilliseconds(0) });
}
void close;
