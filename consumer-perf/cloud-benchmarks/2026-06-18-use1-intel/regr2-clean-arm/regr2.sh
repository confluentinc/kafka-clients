#!/usr/bin/env bash
# Clean regression confirmation: 36p, 150k (~300MB/s), default, self-produce, 1 producer,
# 4-min warmup (exclude startup), 5-min measure, + /proc CPU/RSS sampling of the consumer.
set +e
BS=pkc-devcozvdmy.us-east-1.aws.devel.cpdev.cloud:9092
PROPS=$HOME/bench/cc_bench.properties; KB=$HOME/kafka/bin; BIN=$HOME/cc/target/release/consumer-perf
HZ=$(getconf CLK_TCK); OUT=$HOME/bench/regr2; mkdir -p "$OUT"
rm -f "$OUT/REGR2_DONE"
nohup $BIN --bootstrap $BS --topic cmp-bench-36p --client-config $PROPS --kafka-bin $KB --no-create-topic \
  --throughput 150000 --message-size 2048 --duration 300 --warmup-messages 36000000 --offset-reset latest \
  --interval 30 --results-dir "$OUT/res" > "$OUT/regr2.log" 2>&1 &
CPID=$!
echo "t,cpu_pct,rss_mb" > "$OUT/cpu.csv"
prev=$(awk '{print $14+$15}' /proc/$CPID/stat 2>/dev/null); pw=$(date +%s.%N)
while kill -0 $CPID 2>/dev/null; do sleep 10; [ -r /proc/$CPID/stat ] || break
  cur=$(awk '{print $14+$15}' /proc/$CPID/stat 2>/dev/null); nw=$(date +%s.%N)
  rss=$(awk '/VmRSS/{printf "%.1f",$2/1024}' /proc/$CPID/status 2>/dev/null)
  dw=$(echo "$nw - $pw" | bc -l)
  cpu=$(awk -v c="$cur" -v p="$prev" -v hz="$HZ" -v d="$dw" 'BEGIN{if(d>0)printf "%.1f",((c-p)/hz)/d*100; else print 0}')
  echo "$(date -u +%T),$cpu,$rss" >> "$OUT/cpu.csv"; prev=$cur; pw=$nw
done
echo DONE > "$OUT/REGR2_DONE"
