#!/usr/bin/env bash
# Autonomous 3-way (Rust / librdkafka / Java) e2e-latency batch on Intel, USE1.
# Phase A: 30-min default-config runs, 200p then 36p, 150k msg/s (~300 MB/s).
# Phase B: 15-min runs, 36p, rates 100k/50k/25k msg/s (~200/100/50 MB/s).
# All: default config (fetch.min.bytes=1, max.partition.fetch=1MiB, max.poll=500),
# 2 KB msgs, 4-min warmup per client, consumer settles to live edge then producers.
set +e
BS=pkc-devcozvdmy.us-east-1.aws.devel.cpdev.cloud:9092
PROPS=$HOME/cc.properties; KB=$HOME/kafka/bin
BIN=$HOME/cc/target/release/consumer-perf; CE=$HOME/cc/consumer-perf/compare/librdkafka_e2e
APIKEY=$(grep -oP 'username="\K[^"]+' "$PROPS"); APISECRET=$(grep -oP 'password="\K[^"]+' "$PROPS")
HZ=$(getconf CLK_TCK); OUT=$HOME/bench/batch; mkdir -p "$OUT"
MASTER=$OUT/master.log
log(){ echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$MASTER"; }
stopall(){ pkill -f ProducerPerformance 2>/dev/null; pkill -f "release/consumer-perf" 2>/dev/null; pkill -f librdkafka_e2e 2>/dev/null; pkill -f JavaE2E 2>/dev/null; sleep 4; }
trap 'stopall' EXIT
psample(){ echo "t,cpu_pct,rss_mb" > "$2"; local prev pw cur nw rss dw cpu
  prev=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); pw=$(date +%s.%N)
  while kill -0 "$1" 2>/dev/null; do sleep 10; [ -r /proc/$1/stat ] || break
    cur=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); nw=$(date +%s.%N)
    rss=$(awk '/VmRSS/{printf "%.1f",$2/1024}' /proc/$1/status 2>/dev/null)
    dw=$(echo "$nw - $pw" | bc -l)
    cpu=$(awk -v c="$cur" -v p="$prev" -v hz="$HZ" -v d="$dw" 'BEGIN{if(d>0)printf "%.1f",((c-p)/hz)/d*100; else print 0}')
    echo "$(date -u +%T),$cpu,$rss" >> "$2"; prev=$cur; pw=$nw; done; }
start_prod(){ local T=$1 rate=$2 n=$3 nrec=$4 per=$((rate/$3))
  for i in $(seq 1 "$n"); do nohup $KB/kafka-producer-perf-test.sh --topic "$T" --num-records "$nrec" \
    --record-size 2048 --throughput "$per" --producer.config "$PROPS" --producer-props acks=1 \
    > "$OUT/prod_${T}_$i.log" 2>&1 & done; }
# run_one: client topic rate dur warmup nprod tag
run_one(){ local client=$1 T=$2 rate=$3 dur=$4 warmup=$5 nprod=$6 tag=$7
  local wtime=$((warmup/rate)) prodsecs per nrec base gid CPID SP
  prodsecs=$((wtime+dur+150)); per=$((rate/nprod)); nrec=$((per*prodsecs))
  base=$OUT/${tag}_${client}; gid="${tag}-${client}-$(date +%s)"
  log "START $tag client=$client topic=$T rate=$rate dur=${dur}s warmup=${warmup}(~${wtime}s) nprod=$nprod"
  case $client in
    rust) nohup $BIN --bootstrap $BS --topic $T --client-config $PROPS --kafka-bin $KB --no-produce --no-create-topic \
            --duration $dur --warmup-messages $warmup --interval 30 --max-poll-records 500 \
            --fetch-min-bytes 1 --max-partition-fetch-bytes 1048576 --join-timeout 90 --group-id $gid > ${base}.log 2>&1 & ;;
    rdk)  nohup $CE --bootstrap $BS --topic $T --protocol consumer --group-id $gid \
            --duration $dur --warmup $warmup --interval 30 --poll-timeout-ms 500 --single-poll \
            --fetch-min-bytes 1 --fetch-message-max-bytes 1048576 --conf check.crcs=false \
            --conf security.protocol=SASL_SSL --conf sasl.mechanisms=PLAIN \
            --conf sasl.username=$APIKEY --conf sasl.password=$APISECRET \
            --no-produce --results-dir ${base}_res > ${base}.log 2>&1 & ;;
    java) nohup java -cp "$HOME/bench:$HOME/kafka/libs/*" JavaE2E --bootstrap $BS --topic $T \
            --group-protocol consumer --client-config $PROPS --group-id $gid \
            --duration $dur --warmup $warmup --interval 30 --max-poll-records 500 > ${base}.log 2>&1 & ;;
  esac
  CPID=$!; sleep 45; start_prod $T $rate $nprod $nrec; psample $CPID ${base}_cpu.csv & SP=$!
  wait $CPID; kill $SP 2>/dev/null; stopall
  log "END   $tag client=$client"; }

rm -f $OUT/BATCH_DONE; stopall
log "BATCH START"

# ---- Phase A: 30-min default config, 200p then 36p, 150k/s ----
for cl in rust rdk java; do run_one $cl cmp-bench-200p 150000 1800 36000000 4 A-200p; done
for cl in rust rdk java; do run_one $cl cmp-bench-36p  150000 1800 36000000 4 A-36p;  done

# ---- Phase B: 15-min, 36p, 200/100/50 MB/s (100k/50k/25k msg/s) ----
for cl in rust rdk java; do run_one $cl cmp-bench-36p 100000 900 24000000 3 B-200mbs; done
for cl in rust rdk java; do run_one $cl cmp-bench-36p  50000 900 12000000 2 B-100mbs; done
for cl in rust rdk java; do run_one $cl cmp-bench-36p  25000 900  6000000 1 B-50mbs;  done

log "BATCH DONE"; touch $OUT/BATCH_DONE
