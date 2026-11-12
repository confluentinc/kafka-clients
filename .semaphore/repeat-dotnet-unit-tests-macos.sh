#!/bin/bash
# Temporary, for the macOS .NET hang investigation only: runs the .NET unit tests as
# `make test-dotnet-macos-docker` does, then runs them again, without the build, until one run hangs
# or <seconds> have passed since the script started. A hang is a TestTimeout bound of at least 5 s
# that expired: TestTimeout's NativeStacks and ManagedDump then write their files to
# CONFLUENT_KAFKA_DOTNET_HANG_DIR, and the script stops at once so the job uploads them. A run that
# fails without a hang does not stop the repeats; the script fails at the end if any did.
#
# Usage: repeat-dotnet-unit-tests-macos.sh <seconds>
set -u

budget=${1:?usage: $0 <seconds>}
hang_dir=${CONFLUENT_KAFKA_DOTNET_HANG_DIR:?CONFLUENT_KAFKA_DOTNET_HANG_DIR is not set}
filter='FullyQualifiedName!~Confluent.Kafka.IntegrationTests'
end=$(($(date +%s) + budget))
failed=0

hung() {
  local file
  for file in "$hang_dir"/dump-*.txt "$hang_dir"/sample-*.txt; do
    if [ -e "$file" ]; then
      return 0
    fi
  done
  return 1
}

# The traces of a run that did not hang are not needed; dropping them keeps the uploaded folder small.
drop_traces() {
  rm -f "$hang_dir"/rust-trace-*.log "$hang_dir"/dotnet-trace-*.log
}

stop_hung() {
  echo "=== $1 hung; its files are in $hang_dir ==="
  ls -la "$hang_dir"
  exit 1
}

echo "=== run 1: make test-dotnet-macos-docker ==="
if ! make test-dotnet-macos-docker; then
  if hung; then
    stop_hung "run 1"
  fi
  if [ ! -f dotnet/tests/Confluent.Kafka.UnitTests/bin/Release/net10.0/Confluent.Kafka.UnitTests.dll ]; then
    echo "=== run 1 failed before the unit tests were built; nothing to repeat ==="
    exit 1
  fi
  echo "=== run 1 failed without a hang ==="
  failed=1
fi
drop_traces

run=1
while [ "$(date +%s)" -lt "$end" ]; do
  run=$((run + 1))
  for tfm in net8.0 net10.0; do
    echo "=== run $run, $tfm ==="
    if ! dotnet test -c Release -f "$tfm" dotnet/Confluent.Kafka.sln --no-build --filter "$filter"; then
      if hung; then
        stop_hung "run $run ($tfm)"
      fi
      echo "=== run $run failed on $tfm without a hang ==="
      failed=$((failed + 1))
    fi
    drop_traces
  done
done

echo "=== $run runs, $failed failed without a hang, none hung ==="
[ "$failed" -eq 0 ]
