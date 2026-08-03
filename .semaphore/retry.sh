#!/bin/sh
#
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
#
# Retry a command up to 5 times with a 5s pause, for transient external failures.
# Usage: sh .semaphore/retry.sh <command> [args...]

set -u

n=1
while [ "$n" -le 5 ]; do
    if "$@"; then exit 0; fi
    [ "$n" -eq 5 ] && break
    echo "retry: attempt $n/5 of '$*' failed; sleeping 5s" >&2
    n=$((n + 1))
    sleep 5
done
echo "retry: '$*' failed after 5 attempts" >&2
exit 1
