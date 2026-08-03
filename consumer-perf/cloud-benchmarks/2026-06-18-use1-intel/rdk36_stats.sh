#!/usr/bin/env bash
# librdkafka 36p WITH statistics.interval.ms=5000, 30-min, 300MB/s, ext producer, /proc CPU.
# Pairs with the librdkafka 36p no-stats baseline (arm-batch AA-36p rdk = 84% CPU).
set +e
BS=pkc-devcozvdmy.us-east-1.aws.devel.cpdev.cloud:9092
PROPS=$HOME/bench/cc_bench.properties; KB=$HOME/kafka/bin; CE=$HOME/bench/librdkafka_e2e_stats
APIKEY=$(grep -oP 'username="\K[^"]+' "$PROPS"); APISECRET=$(grep -oP 'password="\K[^"]+' "$PROPS")
HZ=$(getconf CLK_TCK); OUT=$HOME/bench/rdk36_stats; mkdir -p "$OUT"
export LD_LIBRARY_PATH=/usr/local/lib
T=cmp-bench-36p; dur=1800; warmup=36000000; nrec=$((150000*(warmup/150000+dur+200)))
stopall(){ pkill -f ProducerPerformance 2>/dev/null; pkill -f librdkafka_e2e 2>/dev/null; sleep 4; }
trap 'stopall' EXIT
psample(){ echo "t,cpu_pct,rss_mb" > "$2"; local prev pw cur nw rss dw cpu
  prev=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); pw=$(date +%s.%N)
  while kill -0 "$1" 2>/dev/null; do sleep 10; [ -r /proc/$1/stat ] || break
    cur=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); nw=$(date +%s.%N)
    rss=$(awk '/VmRSS/{printf "%.1f",$2/1024}' /proc/$1/status 2>/dev/null)
    dw=$(echo "$nw - $pw" | bc -l)
    cpu=$(awk -v c="$cur" -v p="$prev" -v hz="$HZ" -v d="$dw" 'BEGIN{if(d>0)printf "%.1f",((c-p)/hz)/d*100; else print 0}')
    echo "$(date -u +%T),$cpu,$rss" >> "$2"; prev=$cur; pw=$nw; done; }
rm -f "$OUT/RDK36_DONE"; stopall
nohup $CE --bootstrap $BS --topic $T --protocol consumer --group-id rdk36-$(date +%s) \
  --duration $dur --warmup $warmup --interval 30 --poll-timeout-ms 500 --single-poll \
  --fetch-min-bytes 1 --fetch-message-max-bytes 1048576 --conf check.crcs=false \
  --conf statistics.interval.ms=5000 --conf security.protocol=SASL_SSL --conf sasl.mechanisms=PLAIN \
  --conf sasl.username=$APIKEY --conf sasl.password=$APISECRET --no-produce --results-dir "$OUT/res" > "$OUT/rdk36.log" 2>&1 &
CPID=$!; sleep 45
nohup $KB/kafka-producer-perf-test.sh --topic $T --num-records $nrec --record-size 2048 \
  --throughput 150000 --producer.config "$PROPS" --producer-props acks=1 > "$OUT/prod.log" 2>&1 &
psample $CPID "$OUT/cpu.csv" & SP=$!
wait $CPID; kill $SP 2>/dev/null; stopall
echo DONE > "$OUT/RDK36_DONE"
