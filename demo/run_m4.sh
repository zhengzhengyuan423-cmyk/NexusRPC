#!/usr/bin/env bash
# registry + 2 provider + 1 consumer 四进程；kill -9 一个 provider，consumer 1s 级切流
set -u
BIN="$(cd "$(dirname "$0")/.." && pwd)/build"
cd "$BIN"
pkill -f 'registry_server_main|provider_main|consumer_main' 2>/dev/null
sleep 0.3

./registry_server_main 19890 3 > reg.log 2>&1 &
REG=$!
sleep 0.5
./provider_main 127.0.0.1 19890 19891 > p1.log 2>&1 &
P1=$!
./provider_main 127.0.0.1 19890 19892 > p2.log 2>&1 &
P2=$!
sleep 1 # 让 provider 先完成首次心跳：consumer 的 List 快照才有内容
./consumer_main 127.0.0.1 19890 > cons.log 2>&1 &
C=$!

sleep 3
echo "== kill -9 provider1 ($P1) =="
kill -9 "$P1"
sleep 3
kill "$C" "$P2" "$REG" 2>/dev/null # TERM 收摊：正常 return 才刷得掉 cout 缓冲；kill -9 留给 M4 主角
wait 2>/dev/null

echo "== consumer log =="
tail -n 24 cons.log
