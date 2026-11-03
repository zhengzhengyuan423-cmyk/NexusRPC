#!/usr/bin/env bash
# 跨进程 Add/Sub，同步/future/回调三链路
set -u
BIN="$(cd "$(dirname "$0")/.." && pwd)/build"
PORT=${PORT:-19890}

"$BIN/add_sub_server" "$PORT" 1 2 &
SRV=$!
trap 'kill $SRV 2>/dev/null' EXIT

for i in $(seq 1 50); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; then exec 3<&- ; exec 3>&-; break; fi
    sleep 0.1
done

"$BIN/add_sub_client" 127.0.0.1 "$PORT"
RC=$?
kill $SRV 2>/dev/null
wait $SRV 2>/dev/null
exit $RC
