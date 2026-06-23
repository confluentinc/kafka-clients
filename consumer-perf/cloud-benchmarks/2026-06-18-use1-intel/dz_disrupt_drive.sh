#!/usr/bin/env bash
# Broker-disruption recovery test: REJECT-reset the Confluent Cloud LB IP for 6s,
# 4 times during the measurement window, identically for Rust and librdkafka.
# Measures how fast each client reconnects + resumes fetching after the cut.
set +e
BROKER_IP=3.227.135.119
RULE="OUTPUT -d $BROKER_IP -p tcp -j REJECT --reject-with tcp-reset"
flush_rule(){ while sudo -n iptables -C $RULE 2>/dev/null; do sudo -n iptables -D $RULE; done; }
trap 'flush_rule' EXIT
BS=pkc-devcozvdmy.us-east-1.aws.devel.cpdev.cloud:9092
PROPS=$HOME/cc.properties; KB=$HOME/kafka/bin; T=cmp-bench-200p
BIN=$HOME/cc/target/release/consumer-perf; CE=$HOME/cc/consumer-perf/compare/librdkafka_e2e
MSGSZ=2048; RATE=150000; DUR=300; WARMUP=600000; HZ=$(getconf CLK_TCK)
OFFSETS="110 175 240 305"   # seconds after consumer launch (T0)
log(){ echo "[$(date -u +%T)] $*"; }
psample(){ echo "t,cpu_pct,rss_mb" > "$2"; local prev pw cur nw rss dw cpu
  prev=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); pw=$(date +%s.%N)
  while kill -0 "$1" 2>/dev/null; do sleep 10; [ -r /proc/$1/stat ] || break
    cur=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); nw=$(date +%s.%N)
    rss=$(awk '/VmRSS/{printf "%.1f",$2/1024}' /proc/$1/status 2>/dev/null)
    dw=$(echo "$nw - $pw" | bc -l)
    cpu=$(awk -v c="$cur" -v p="$prev" -v hz="$HZ" -v d="$dw" 'BEGIN{if(d>0)printf "%.1f",((c-p)/hz)/d*100; else print 0}')
    echo "$(date -u +%T),$cpu,$rss" >> "$2"; prev=$cur; pw=$nw; done; }
start_prod(){ local n=4 per nrec; per=$((RATE/n)); nrec=$((per*(DUR+150)))
  for i in $(seq 1 $n); do nohup $KB/kafka-producer-perf-test.sh --topic $T --num-records $nrec \
    --record-size $MSGSZ --throughput $per --producer.config $PROPS --producer-props acks=1 \
    > $HOME/bench/dz_prod_$i.log 2>&1 & done; }
# disruption scheduler: T0 = consumer launch epoch (arg1), client label (arg2)
disrupt(){ local T0=$1 lbl=$2 dlog=$HOME/bench/dz_${lbl}_disrupt.log; : > "$dlog"
  for off in $OFFSETS; do local tgt=$((T0+off))
    while [ "$(date +%s)" -lt "$tgt" ]; do sleep 1; done
    echo "$(date -u +%T) ON  t0+${off}s" >> "$dlog"; sudo -n iptables -A $RULE
    sleep 6
    sudo -n iptables -D $RULE; echo "$(date -u +%T) OFF t0+$((off+6))s" >> "$dlog"
  done; }
stopall(){ pkill -f ProducerPerformance 2>/dev/null; pkill -f "release/consumer-perf" 2>/dev/null; pkill -f librdkafka_e2e 2>/dev/null; flush_rule; sleep 3; }
RUSTARGS="--bootstrap $BS --topic $T --client-config $PROPS --kafka-bin $KB --no-produce --no-create-topic --duration $DUR --warmup-messages $WARMUP --interval 10 --max-poll-records 500 --fetch-min-bytes 1 --max-partition-fetch-bytes 1048576 --join-timeout 90"

stopall

# ---- RUST ----
log "RUST-DISRUPT start"
T0=$(date +%s)
nohup $BIN $RUSTARGS --group-id dz-rustc-$(date +%s) > $HOME/bench/dz_rust.log 2>&1 &
CPID=$!; sleep 45; start_prod; psample $CPID $HOME/bench/dz_rust_cpu.csv & SP=$!
disrupt "$T0" rust & DC=$!
wait $CPID; kill $SP $DC 2>/dev/null; flush_rule; stopall

# ---- LIBRDKAFKA ----
log "LIBRDKAFKA-DISRUPT start"
T0=$(date +%s)
nohup $CE --bootstrap $BS --topic $T --protocol consumer --group-id dz-rdk-$(date +%s) \
  --duration $DUR --warmup $WARMUP --interval 10 --poll-timeout-ms 500 --single-poll \
  --fetch-min-bytes 1 --fetch-message-max-bytes 1048576 --conf check.crcs=false \
  --conf security.protocol=SASL_SSL --conf sasl.mechanisms=PLAIN \
  --conf sasl.username=WTZRYTIWPPBOVA7S --conf sasl.password=cfltj2cpq+mcOnvjHqkOIz+KUtxyqcSnjYdGRhefpVJe9qYl1MAeN7Ee7t7UCM7Q \
  --no-produce --results-dir $HOME/bench/dz_rdk > $HOME/bench/dz_rdk.log 2>&1 &
CPID=$!; sleep 45; start_prod; psample $CPID $HOME/bench/dz_rdk_cpu.csv & SP=$!
disrupt "$T0" rdk & DC=$!
wait $CPID; kill $SP $DC 2>/dev/null; flush_rule; stopall

log "DZ DONE"; touch $HOME/bench/DZ_DONE
