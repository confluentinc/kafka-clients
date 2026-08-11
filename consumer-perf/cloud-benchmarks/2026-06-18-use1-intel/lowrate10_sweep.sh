#!/usr/bin/env bash
# 10p sweep @ 10/20/50 MB/s (5000/10000/25000 msg/s, 2KB), ARM, Rust(with-metrics) + librdkafka(default).
# 5-min each, default config, ~30s warmup, /proc CPU. Sequential (shared cmp-bench-10p topic).
set +e
BS=pkc-devcozvdmy.us-east-1.aws.devel.cpdev.cloud:9092
PROPS=$HOME/bench/cc_bench.properties; KB=$HOME/kafka/bin
BIN=$HOME/cc/target/release/consumer-perf; CE=$HOME/bench/librdkafka_e2e
APIKEY=$(grep -oP 'username="\K[^"]+' "$PROPS"); APISECRET=$(grep -oP 'password="\K[^"]+' "$PROPS")
HZ=$(getconf CLK_TCK); OUT=$HOME/bench/lowrate10_sweep; mkdir -p "$OUT"; MASTER=$OUT/master.log
export LD_LIBRARY_PATH=/usr/local/lib
T=cmp-bench-10p; DUR=300
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
start_prod(){ local rate=$1 nrec=$2
  nohup $KB/kafka-producer-perf-test.sh --topic $T --num-records $nrec --record-size 2048 \
    --throughput $rate --producer.config "$PROPS" --producer-props acks=1 > "$OUT/prod_${rate}.log" 2>&1 & }
run_rust(){ local rate=$1 mbs=$2 CPID SP; local warmup=$((rate*30))
  log "START rust ${mbs}MBs (${rate} msg/s, self-produce)"
  nohup $BIN --bootstrap $BS --topic $T --client-config $PROPS --kafka-bin $KB --no-create-topic \
    --throughput $rate --message-size 2048 --duration $DUR --warmup-messages $warmup --offset-reset latest \
    --interval 30 --results-dir $OUT/rust_${mbs}_res > $OUT/rust_${mbs}.log 2>&1 &
  CPID=$!; psample $CPID $OUT/rust_${mbs}_cpu.csv & SP=$!
  wait $CPID; kill $SP 2>/dev/null; stopall; log "END rust ${mbs}MBs"; }
run_rdk(){ local rate=$1 mbs=$2 CPID SP; local warmup=$((rate*30)); local nrec=$((rate*430))
  log "START rdk ${mbs}MBs (${rate} msg/s, ext producer)"
  nohup $CE --bootstrap $BS --topic $T --protocol consumer --group-id lr10-${mbs}-$(date +%s) \
    --duration $DUR --warmup $warmup --interval 30 --poll-timeout-ms 500 --single-poll \
    --fetch-min-bytes 1 --conf check.crcs=false --conf security.protocol=SASL_SSL --conf sasl.mechanisms=PLAIN \
    --conf sasl.username=$APIKEY --conf sasl.password=$APISECRET --no-produce --results-dir $OUT/rdk_${mbs}_res > $OUT/rdk_${mbs}.log 2>&1 &
  CPID=$!; sleep 45; start_prod $rate $nrec; psample $CPID $OUT/rdk_${mbs}_cpu.csv & SP=$!
  wait $CPID; kill $SP 2>/dev/null; stopall; log "END rdk ${mbs}MBs"; }

rm -f $OUT/SWEEP_DONE; stopall
log "LOWRATE10-SWEEP START (10p @ 10/20/50 MB/s)"
run_rust 5000  10; run_rdk 5000  10
run_rust 10000 20; run_rdk 10000 20
run_rust 25000 50; run_rdk 25000 50
log "SWEEP DONE"; touch $OUT/SWEEP_DONE
