#!/bin/sh
set -eu

emberd=${EMBERD:-./bin/emberd}
redis_cli=${REDIS_CLI:-redis-cli}
work=$(mktemp -d)
pid=

cleanup() {
    if [ -n "$pid" ]; then
        kill -TERM "$pid" 2>/dev/null || :
        wait "$pid" || :
    fi
    rm -rf "$work"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$emberd" -addr 127.0.0.1:0 -dir "$work/db" >"$work/emberd.log" 2>&1 &
pid=$!
port=
for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    port=$(sed -n 's/.*"address":"127\.0\.0\.1:\([0-9][0-9]*\)".*/\1/p' "$work/emberd.log")
    if [ -n "$port" ]; then
        break
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if [ -z "$port" ]; then
    cat "$work/emberd.log" >&2
    exit 1
fi

redis() {
    "$redis_cli" -h 127.0.0.1 -p "$port" --raw "$@"
}

[ "$(redis PING)" = PONG ]
[ "$(redis SET ember:smoke 41)" = OK ]
[ "$(redis INCR ember:smoke)" = 42 ]
[ "$(redis GET ember:smoke)" = 42 ]
kill -TERM "$pid"
wait "$pid"
pid=
printf '%s\n' 'Ember RESP2 smoke test passed'
