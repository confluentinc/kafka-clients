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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// the Java sources (the generated modules of lib/consumer/);
// `cargo xtask check-generated` checks it is up to date.

'use strict';

// `lib/consumer`: the generated modules of lib/consumer/.

const { CommitFailedError } = require('./commit_failed_error');
const { InvalidOffsetError } = require('./invalid_offset_error');
const { LogTruncationError } = require('./log_truncation_error');
const { NoOffsetForPartitionError } = require('./no_offset_for_partition_error');
const { OffsetOutOfRangeError } = require('./offset_out_of_range_error');
const { RetriableCommitFailedError } = require('./retriable_commit_failed_error');

module.exports = {
  CommitFailedError,
  InvalidOffsetError,
  LogTruncationError,
  NoOffsetForPartitionError,
  OffsetOutOfRangeError,
  RetriableCommitFailedError,
};
