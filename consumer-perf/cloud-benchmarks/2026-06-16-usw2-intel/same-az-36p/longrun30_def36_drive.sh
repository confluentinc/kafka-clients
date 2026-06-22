#!/usr/bin/env bash
# 30-min steady-state e2e per client, DEFAULT/latency config, on a 36-PARTITION topic.
# Same config as the 200p workload2 latency-tuned run:
#   fetch.min.bytes=1, default max.partition.fetch.bytes (1MiB), max.poll.records=500,
#   librdkafka --single-poll, crc OFF (aligned), 2KB msgs, ~300 MB/s (150k msg/s).
# Difference vs workload2: topic = cmp-bench-36p (36 partitions), and a ~4-min warmup
#   (36,000,000 records at 150k msg/s) for ALL three clients before the 1800s window.
# Consumer always started BEFORE the producer. Rust binary includes the TLS-reset fix (d5b0e50).
source ~/bench/env.sh
cd ~/bench
T2=cmp-bench-36p; MSGSZ=2048; RATE=150000; DUR=1800; WARMUP=36000000; HZ=$(getconf CLK_TCK)
log(){ echo "[$(date -u +%H:%M:%S)] $*"; }
psample(){ echo "t,cpu_pct,rss_mb" > "$2"; local prev pw cur nw rss dw cpu
  prev=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); pw=$(date +%s.%N)
  while kill -0 "$1" 2>/dev/null; do sleep 10; [ -r /proc/$1/stat ] || break
    cur=$(awk '{print $14+$15}' /proc/$1/stat 2>/dev/null); nw=$(date +%s.%N)
    rss=$(awk '/VmRSS/{printf "%.1f",$2/1024}' /proc/$1/status 2>/dev/null)
    dw=$(echo "$nw - $pw"|bc -l)
    cpu=$(awk -v c="$cur" -v p="$prev" -v hz="$HZ" -v d="$dw" 'BEGIN{if(d>0)printf "%.1f",((c-p)/hz)/d*100; else print 0}')
    echo "$(date -u +%T),$cpu,$rss" >> "$2"; prev=$cur; pw=$nw; done; }
# 4 producers * 37.5k/s = 150k msg/s aggregate (2KB => ~307 MB/s). DUR+540 outlives the 4-min warmup.
start_prod(){ local n=4 per nrec; per=$(( RATE / n )); nrec=$(( per * (DUR + 540) ))
  for i in $(seq 1 $n); do nohup $KB/kafka-producer-perf-test.sh --topic $T2 --num-records $nrec \
    --record-size $MSGSZ --throughput $per --producer.config $PROPS --producer-props acks=1 \
    > ~/bench/lr36d_prod_$i.log 2>&1 & done; }
stopall(){ pkill -f ProducerPerformance 2>/dev/null; pkill -f "release/consumer-perf" 2>/dev/null; pkill -f librdkafka_e2e 2>/dev/null; pkill -f JavaE2E 2>/dev/null; }
stopall; sleep 2

# ---------- RUST (fetch.min=1, max.poll.records=500) ----------
log "RUST starting (36p,2KB,~300MB/s,DEFAULT: fetch.min=1,maxpoll=500,warmup=${WARMUP}(~4min),dur=${DUR}s)"
nohup ./../cc/target/release/consumer-perf --bootstrap $BS --topic $T2 --client-config ~/bench/cc_rust.properties \
  --duration $DUR --warmup-messages $WARMUP --interval 30 --max-poll-records 500 \
  --fetch-min-bytes 1 --max-partition-fetch-bytes 1048576 --no-produce \
  --group-id lr36d-rust-$(date +%s) > ~/bench/lr36d_rust.log 2>&1 &
CPID=$!; sleep 45; start_prod; psample $CPID ~/bench/lr36d_rust_cpu.csv & SP=$!
wait $CPID; kill $SP 2>/dev/null; stopall; sleep 8

# ---------- LIBRDKAFKA (fetch.min=1, --single-poll) ----------
log "LIBRDKAFKA starting (36p,2KB,~300MB/s,DEFAULT: fetch.min=1,single-poll,warmup=${WARMUP}(~4min),dur=${DUR}s)"
nohup ./librdkafka_e2e --bootstrap $BS --topic $T2 --protocol consumer --group-id lr36d-repo-$(date +%s) \
  --duration $DUR --warmup $WARMUP --interval 30 --poll-timeout-ms 500 --single-poll \
  --fetch-min-bytes 1 --fetch-message-max-bytes 1048576 --conf check.crcs=false \
  --conf security.protocol=SASL_SSL --conf sasl.mechanisms=PLAIN \
  --conf sasl.username=$APIKEY --conf sasl.password=$APISECRET \
  --no-produce --results-dir ~/bench/lr36d_repo > ~/bench/lr36d_repo.log 2>&1 &
CPID=$!; sleep 45; start_prod
wait $CPID; stopall; sleep 8

# ---------- JAVA (fetch.min=1 compiled in, max.poll.records=500) ----------
log "JAVA starting (36p,2KB,~300MB/s,DEFAULT: fetch.min=1,maxpoll=500,warmup=${WARMUP}(~4min),dur=${DUR}s)"
nohup java -cp "$HOME/bench:$HOME/kafka/libs/*" JavaE2E --bootstrap $BS --topic $T2 \
  --group-protocol consumer --client-config ~/bench/cc_rust.properties \
  --duration $DUR --warmup $WARMUP --interval 30 --max-poll-records 500 \
  --group-id lr36d-java-$(date +%s) > ~/bench/lr36d_java.log 2>&1 &
CPID=$!; sleep 45; start_prod; psample $CPID ~/bench/lr36d_java_cpu.csv & SP=$!
wait $CPID; kill $SP 2>/dev/null; stopall

log "LONGRUN30-DEF36 DONE"; touch ~/bench/LR36D_DONE
