#!/usr/bin/env bash
# ARM metrics-comparison batch: Rust 36p (with-metrics, self-produce) + librdkafka 200p
# (statistics.interval.ms=5000). 30-min each, default config, 4-min warmup, /proc CPU.
set +e
BS=pkc-devcozvdmy.us-east-1.aws.devel.cpdev.cloud:9092
PROPS=$HOME/bench/cc_bench.properties; KB=$HOME/kafka/bin
BIN=$HOME/cc/target/release/consumer-perf; CE=$HOME/bench/librdkafka_e2e_stats
APIKEY=$(grep -oP 'username="\K[^"]+' "$PROPS"); APISECRET=$(grep -oP 'password="\K[^"]+' "$PROPS")
HZ=$(getconf CLK_TCK); OUT=$HOME/bench/mbatch_arm; mkdir -p "$OUT"; MASTER=$OUT/master.log
export LD_LIBRARY_PATH=/usr/local/lib
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
start_prod(){ nohup $KB/kafka-producer-perf-test.sh --topic "$1" --num-records "$3" --record-size 2048 \
  --throughput "$2" --producer.config "$PROPS" --producer-props acks=1 > "$OUT/prod_$1.log" 2>&1 & }
run_rust(){ local T=$1 dur=$2 warmup=$3 tag=$4 CPID SP
  log "START $tag (rust WITH-METRICS self-produce, $T, ${dur}s)"
  nohup $BIN --bootstrap $BS --topic $T --client-config $PROPS --kafka-bin $KB --no-create-topic \
    --throughput 150000 --message-size 2048 --duration $dur --warmup-messages $warmup --offset-reset latest \
    --interval 30 --results-dir $OUT/${tag}_res > $OUT/${tag}.log 2>&1 &
  CPID=$!; psample $CPID $OUT/${tag}_cpu.csv & SP=$!
  wait $CPID; kill $SP 2>/dev/null; stopall; log "END $tag"; }
run_rdk(){ local T=$1 dur=$2 warmup=$3 tag=$4 CPID SP nrec
  nrec=$((150000*(warmup/150000+dur+200)))
  log "START $tag (librdkafka statistics.interval.ms=5000, $T, ${dur}s, ext producer)"
  nohup $CE --bootstrap $BS --topic $T --protocol consumer --group-id ${tag}-$(date +%s) \
    --duration $dur --warmup $warmup --interval 30 --poll-timeout-ms 500 --single-poll \
    --fetch-min-bytes 1 --fetch-message-max-bytes 1048576 --conf check.crcs=false \
    --conf statistics.interval.ms=5000 \
    --conf security.protocol=SASL_SSL --conf sasl.mechanisms=PLAIN \
    --conf sasl.username=$APIKEY --conf sasl.password=$APISECRET --no-produce \
    --results-dir $OUT/${tag}_res > $OUT/${tag}.log 2>&1 &
  CPID=$!; sleep 45; start_prod $T 150000 $nrec; psample $CPID $OUT/${tag}_cpu.csv & SP=$!
  wait $CPID; kill $SP 2>/dev/null; stopall; log "END $tag"; }

rm -f $OUT/MBATCH_ARM_DONE; stopall
log "MBATCH-ARM START"
run_rust cmp-bench-36p  1800 36000000 R-36p-metrics
run_rdk  cmp-bench-200p 1800 36000000 RDK-200p-stats
log "MBATCH-ARM DONE"; touch $OUT/MBATCH_ARM_DONE
