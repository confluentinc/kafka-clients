#!/usr/bin/env bash
# 10p @ 5 MB/s (2500 msg/s, 2KB), ARM, Rust(with-metrics binary) + librdkafka(default, no stats),
# 5-min each, default config, ~24s warmup, /proc CPU. Sequential (shared topic).
set +e
BS=pkc-devcozvdmy.us-east-1.aws.devel.cpdev.cloud:9092
PROPS=$HOME/bench/cc_bench.properties; KB=$HOME/kafka/bin
BIN=$HOME/cc/target/release/consumer-perf; CE=$HOME/bench/librdkafka_e2e
APIKEY=$(grep -oP 'username="\K[^"]+' "$PROPS"); APISECRET=$(grep -oP 'password="\K[^"]+' "$PROPS")
HZ=$(getconf CLK_TCK); OUT=$HOME/bench/lowrate10; mkdir -p "$OUT"; MASTER=$OUT/master.log
export LD_LIBRARY_PATH=/usr/local/lib
T=cmp-bench-10p; RATE=2500; DUR=300; WARMUP=60000
log(){ echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$MASTER"; }
stopall(){ pkill -f ProducerPerformance 2>/dev/null; pkill -9 -x consumer-perf 2>/dev/null; pkill -f librdkafka_e2e 2>/dev/null; sleep 4; }
trap 'stopall' EXIT
psample(){ echo "t,cpu_pct,rss_mb" > "$2"; local prev pw cur nw rss dw cpu
  prev=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); pw=$(date +%s.%N)
  while kill -0 "$1" 2>/dev/null; do sleep 10; [ -r /proc/$1/stat ] || break
    cur=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); nw=$(date +%s.%N)
    rss=$(awk '/VmRSS/{printf "%.1f",$2/1024}' /proc/$1/status 2>/dev/null)
    dw=$(echo "$nw - $pw" | bc -l)
    cpu=$(awk -v c="$cur" -v p="$prev" -v hz="$HZ" -v d="$dw" 'BEGIN{if(d>0)printf "%.1f",((c-p)/hz)/d*100; else print 0}')
    echo "$(date -u +%T),$cpu,$rss" >> "$2"; prev=$cur; pw=$nw; done; }
start_prod(){ local nrec=$((RATE*(WARMUP/RATE+DUR+100)))
  nohup $KB/kafka-producer-perf-test.sh --topic $T --num-records $nrec --record-size 2048 \
    --throughput $RATE --producer.config "$PROPS" --producer-props acks=1 > "$OUT/prod.log" 2>&1 & }

rm -f $OUT/LOWRATE10_DONE; stopall
log "LOWRATE10 START (10p, ${RATE} msg/s ~5MB/s, 5-min each)"

# Rust (self-produce)
log "START rust (with-metrics, self-produce)"
nohup $BIN --bootstrap $BS --topic $T --client-config $PROPS --kafka-bin $KB --no-create-topic \
  --throughput $RATE --message-size 2048 --duration $DUR --warmup-messages $WARMUP --offset-reset latest \
  --interval 30 --results-dir $OUT/rust_res > $OUT/rust.log 2>&1 &
CPID=$!; psample $CPID $OUT/rust_cpu.csv & SP=$!
wait $CPID; kill $SP 2>/dev/null; stopall; log "END rust"

# librdkafka (external producer, default config / no stats)
log "START librdkafka (default, ext producer)"
nohup $CE --bootstrap $BS --topic $T --protocol consumer --group-id lr10-$(date +%s) \
  --duration $DUR --warmup $WARMUP --interval 30 --poll-timeout-ms 500 --single-poll \
  --fetch-min-bytes 1 --conf check.crcs=false --conf security.protocol=SASL_SSL --conf sasl.mechanisms=PLAIN \
  --conf sasl.username=$APIKEY --conf sasl.password=$APISECRET --no-produce --results-dir $OUT/rdk_res > $OUT/rdk.log 2>&1 &
CPID=$!; sleep 45; start_prod; psample $CPID $OUT/rdk_cpu.csv & SP=$!
wait $CPID; kill $SP 2>/dev/null; stopall; log "END librdkafka"

log "LOWRATE10 DONE"; touch $OUT/LOWRATE10_DONE
