#!/usr/bin/env bash
# F17 完成门:24h 性能长跑 (ROADMAP「F17 完成门」)
# - 固定版本快照 (二进制 + 配置复制到独立目录，运行期不重建)
# - 合成稳定负载:self-http 自检 + supervise sleep + tcp 探测
# - 每 60s 采样：时间戳 / %cpu(生命周期均值=总 CPU/uptime)/ RSS / /ready 状态
# - 24h 后 SIGTERM 退出，记录退出码;判定：
#   1) 生命周期 CPU 均值 < 2%(单核)
#   2) RSS 稳态增长 (第 2 小时起 → 结束)< 20MB(排除启动分配)
#   3) /ready 失败采样 = 0
#   4) 退出码 = 0(SIGTERM 优雅)
# 用法：docs/deploy/soak-24h.sh [秒数，默认 86400]

set -uo pipefail

BIN_SRC="${BIN_SRC:-./target/release/monitor-agent}"
SECONDS_TOTAL="${1:-86400}"
PORT=19199
# 教训 (第一次长跑被 /var/tmp 系统清理器抹掉):默认必须用持久目录
ROOT="${SOAK_DIR:-$HOME/.local/state/sw-soak-24h}"
STAMP="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="$ROOT/$STAMP"
LOG="$RUN_DIR/soak.log"
VERDICT="$RUN_DIR/verdict.txt"
INTERVAL=60

mkdir -p "$RUN_DIR"
cp "$BIN_SRC" "$RUN_DIR/monitor-agent"
chmod +x "$RUN_DIR/monitor-agent"

cat > "$RUN_DIR/config.toml" <<EOF
[agent]
data_dir = "$RUN_DIR/data"

[control]
listen = "127.0.0.1:$PORT"

[[check]]
name = "self"
kind = "http"
url = "http://127.0.0.1:$PORT/health"
interval = "5s"
timeout = "2s"

[[check]]
name = "idle-tcp"
kind = "tcp"
addr = "127.0.0.1:$((PORT + 1))"
interval = "5s"
timeout = "2s"

[[service]]
name = "sleeper"
command = "/bin/sleep"
args = ["172800"]
[service.restart]
startup_grace = "2s"
min_uptime = "2s"
EOF

"$RUN_DIR/monitor-agent" --check-config "$RUN_DIR/config.toml" >/dev/null || {
  echo "config check failed" | tee "$VERDICT"; exit 2; }

echo "soak start $(date '+%F %T') bin=$RUN_DIR/monitor-agent dur=${SECONDS_TOTAL}s" >> "$LOG"
"$RUN_DIR/monitor-agent" --config "$RUN_DIR/config.toml" --log-json >> "$RUN_DIR/agent.jsonl" 2>&1 &
AGENT=$!

# 等 ready
READY_AT=""
for _ in $(seq 1 100); do
  if curl -sf "http://127.0.0.1:$PORT/ready" >/dev/null 2>&1; then
    READY_AT=$(date +%s); break
  fi
  sleep 0.2
done
[[ -n "$READY_AT" ]] || { echo "FAIL: agent never ready" | tee "$VERDICT"; kill $AGENT; exit 2; }
echo "ready at epoch $READY_AT" >> "$LOG"

END=$(( $(date +%s) + SECONDS_TOTAL ))
N=0
READY_FAILS=0
while (( $(date +%s) < END )); do
  N=$((N + 1))
  CPU=$(ps -o %cpu= -p "$AGENT" 2>/dev/null | tr -d ' ' || echo "-")
  RSS=$(ps -o rss= -p "$AGENT" 2>/dev/null | tr -d ' ' || echo "-")
  if curl -sf "http://127.0.0.1:$PORT/ready" >/dev/null 2>&1; then RF=0; else RF=1; READY_FAILS=$((READY_FAILS+1)); fi
  echo "$(date '+%F %T') sample=$N cpu=$CPU rss_kb=$RSS ready_ok=$RF" >> "$LOG"
  sleep "$INTERVAL"
done

kill -TERM "$AGENT" 2>/dev/null
for _ in $(seq 1 100); do kill -0 "$AGENT" 2>/dev/null || break; sleep 0.1; done
wait "$AGENT" 2>/dev/null
EXIT_CODE=$?
echo "agent exit code: $EXIT_CODE" >> "$LOG"
echo "ready_failures: $READY_FAILS" >> "$LOG"

# ---- 判定 ----
# CPU:排除首个采样 (含启动尖峰) 后的生命周期均值
AVG_CPU=$(grep -v "sample=1 " "$LOG" | awk -F'cpu=' '/sample=/{split($2,a," "); s+=a[1]; n++} END {if(n) printf "%.2f", s/n; else print "-"}')
# RSS 稳态：第 120 采样 (约 2h) 之后
RSS_BASE=$(awk -F'rss_kb=' '/sample=120/{split($2,a," "); print a[1]; exit}' "$LOG")
RSS_LAST=$(awk -F'rss_kb=' '/sample=/{split($2,a," "); v=a[1]} END {print v}' "$LOG")
GROWTH=$(( ${RSS_LAST:-0} - ${RSS_BASE:-0} ))

VERDICT_LINE=0
awk -v c="$AVG_CPU" 'BEGIN { exit !(c != "-" && c < 2.0) }' || { echo "FAIL: avg cpu $AVG_CPU >= 2%" >> "$VERDICT"; VERDICT_LINE=1; }
(( ${RSS_BASE:-0} > 0 )) || { echo "SKIP: 样本不足 120,RSS 判定跳过 (短跑？)" >> "$VERDICT"; }
(( GROWTH < 20480 )) || { echo "FAIL: rss growth ${GROWTH}KB >= 20MB" >> "$VERDICT"; VERDICT_LINE=1; }
(( READY_FAILS == 0 )) || { echo "FAIL: ready failures $READY_FAILS" >> "$VERDICT"; VERDICT_LINE=1; }
(( EXIT_CODE == 0 || EXIT_CODE == 130 )) || { echo "FAIL: exit code $EXIT_CODE" >> "$VERDICT"; VERDICT_LINE=1; }

{
  echo "== verdict =="
  echo "avg_cpu(排除首采样)=$AVG_CPU%"
  echo "rss_growth(稳态)= ${GROWTH}KB"
  echo "ready_failures=$READY_FAILS"
  echo "exit_code=$EXIT_CODE"
  (( VERDICT_LINE == 0 )) && echo "RESULT: PASS" || echo "RESULT: FAIL"
} >> "$VERDICT"
cat "$VERDICT"
exit "$VERDICT_LINE"
