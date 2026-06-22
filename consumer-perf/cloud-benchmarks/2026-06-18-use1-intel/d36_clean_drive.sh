#!/usr/bin/env bash
# 36p default/latency config, 150k msg/s (~300MB/s) via 4 producers, 5-min measure.
# Clean comparison: Rust (no diag) then librdkafka, back-to-back. Consumer starts
# before producers (settles to live edge on empty), then 4 producers drive 150k.
set +e
BS=pkc-devcozvdmy.us-east-1.aws.devel.cpdev.cloud:9092
PROPS=$HOME/cc.properties; KB=$HOME/kafka/bin; T=cmp-bench-36p
BIN=$HOME/cc/target/release/consumer-perf; CE=$HOME/cc/consumer-perf/compare/librdkafka_e2e
MSGSZ=2048; RATE=150000; DUR=300; WARMUP=600000; HZ=$(getconf CLK_TCK)
log(){ echo "[$(date -u +%H:%M:%S)] $*"; }
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
    > $HOME/bench/d36_prod_$i.log 2>&1 & done; }
stopall(){ pkill -f ProducerPerformance 2>/dev/null; pkill -f "release/consumer-perf" 2>/dev/null; pkill -f librdkafka_e2e 2>/dev/null; sleep 3; }
RUSTARGS="--bootstrap $BS --topic $T --client-config $PROPS --kafka-bin $KB --no-produce --no-create-topic --duration $DUR --warmup-messages $WARMUP --interval 30 --max-poll-records 500 --fetch-min-bytes 1 --max-partition-fetch-bytes 1048576 --join-timeout 90"

stopall

log "RUST-CLEAN start (36p,2KB,150k,default,${DUR}s)"
nohup $BIN $RUSTARGS --group-id d36-rustc-$(date +%s) > $HOME/bench/d36_rust_clean.log 2>&1 &
CPID=$!; sleep 45; start_prod; psample $CPID $HOME/bench/d36_rust_clean_cpu.csv & SP=$!
wait $CPID; kill $SP 2>/dev/null; stopall

log "LIBRDKAFKA start"
nohup $CE --bootstrap $BS --topic $T --protocol consumer --group-id d36-rdk-$(date +%s) \
  --duration $DUR --warmup $WARMUP --interval 30 --poll-timeout-ms 500 --single-poll \
  --fetch-min-bytes 1 --fetch-message-max-bytes 1048576 --conf check.crcs=false \
  --conf security.protocol=SASL_SSL --conf sasl.mechanisms=PLAIN \
  --conf sasl.username=WTZRYTIWPPBOVA7S --conf sasl.password=cfltj2cpq+mcOnvjHqkOIz+KUtxyqcSnjYdGRhefpVJe9qYl1MAeN7Ee7t7UCM7Q \
  --no-produce --results-dir $HOME/bench/d36_rdk > $HOME/bench/d36_rdk.log 2>&1 &
CPID=$!; sleep 45; start_prod; psample $CPID $HOME/bench/d36_rdk_cpu.csv & SP=$!
wait $CPID; kill $SP 2>/dev/null; stopall

log "D2-CLEAN DONE"; touch $HOME/bench/D36_CLEAN_DONE
