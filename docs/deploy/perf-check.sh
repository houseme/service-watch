#!/usr/bin/env bash
# F17 性能预算验证脚本(ROADMAP v0.2)— macOS/Linux
# 度量 agent 在合成负载下的 CPU(阈值:< 2% 单核)与 RSS 变化(泄漏指示),
# 输出 PASS/FAIL 结论。24h 长跑仍需人工执行(本脚本是快速回归门)。
#
# 用法: docs/deploy/perf-check.sh [二进制路径] [持续秒数,默认 60]
# 依赖: bash、ps、curl、awk;合成目标和探测均使用本地地址

set -euo pipefail

BIN="${1:-./target/release/monitor-agent}"
DURATION="${2:-60}"
PORT_BASE=19190
SMOKE_DIR="$(mktemp -d /tmp/sw-perf.XXXXXX)"
AGENT_PID=''
export LC_ALL=C

cleanup() {
  local result=$?
  trap - EXIT
  if [[ -n "$AGENT_PID" ]]; then
    kill -TERM "$AGENT_PID" 2>/dev/null || true
    for ((attempt = 0; attempt < 20; attempt++)); do
      kill -0 "$AGENT_PID" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$AGENT_PID" 2>/dev/null || true
    wait "$AGENT_PID" 2>/dev/null || true
  fi
  if [[ -r "$SMOKE_DIR/sleeper.pid" ]]; then
    local target_pid target_command
    target_pid=$(cat "$SMOKE_DIR/sleeper.pid")
    if [[ "$target_pid" =~ ^[0-9]+$ ]]; then
      target_command=$(ps -o args= -p "$target_pid" 2>/dev/null || true)
      case "$target_command" in
        *"$SMOKE_DIR/sleeper.sh"*)
          kill -TERM -- "-$target_pid" 2>/dev/null || true
          sleep 0.1
          kill -KILL -- "-$target_pid" 2>/dev/null || true
          ;;
      esac
    fi
  fi
  if (( result == 0 )); then
    rm -rf "$SMOKE_DIR"
  else
    echo "保留本次验证日志: $SMOKE_DIR/agent.log" >&2
  fi
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

[[ "$DURATION" =~ ^[0-9]+$ ]] && (( DURATION >= 10 )) || {
  echo '持续秒数必须是至少 10 的整数' >&2
  exit 2
}
[[ -x "$BIN" ]] || { echo "二进制不可执行: $BIN(cargo build --release -p monitor-agent)"; exit 2; }

# pid 文件和唯一脚本路径用于只清理本次派生进程组。
cat > "$SMOKE_DIR/sleeper.sh" <<EOF
#!/bin/sh
echo \$\$ > "$SMOKE_DIR/sleeper.pid"
sleep 86400 &
wait
EOF

# ---- 合成配置:1 个 http 自检 + 1 个 supervise + 1 个 tcp 探测(稳定负载)----
cat > "$SMOKE_DIR/config.toml" <<EOF
[agent]
data_dir = "$SMOKE_DIR/data"
[control]
listen = "127.0.0.1:$PORT_BASE"
[[check]]
name = "self"
kind = "http"
url = "http://127.0.0.1:$PORT_BASE/health"
interval = "5s"
timeout = "2s"
[[check]]
name = "idle-tcp"
kind = "tcp"
addr = "127.0.0.1:$((PORT_BASE + 1))"
interval = "5s"
timeout = "2s"
[[service]]
name = "sleeper"
command = "/bin/sh"
args = ["$SMOKE_DIR/sleeper.sh"]
[service.restart]
startup_grace = "2s"
min_uptime = "2s"
EOF

"$BIN" --check-config "$SMOKE_DIR/config.toml" >/dev/null

echo "== 启动 agent(负载:${DURATION}s)=="
"$BIN" --config "$SMOKE_DIR/config.toml" > "$SMOKE_DIR/agent.log" 2>&1 &
AGENT_PID=$!

# 等待 ready(就绪后才开始度量,排除启动期)
READY=0
for ((attempt = 0; attempt < 50; attempt++)); do
  kill -0 "$AGENT_PID" 2>/dev/null || { echo 'agent 提前退出' >&2; exit 2; }
  if curl -sf --max-time 1 "http://127.0.0.1:$PORT_BASE/ready" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 0.2
done
(( READY == 1 )) || { echo 'agent 未在 10s 内就绪' >&2; exit 2; }

CPU_SAMPLES=()
RSS_SAMPLES=()
INTERVAL=5
for ((elapsed = 0; elapsed < DURATION; elapsed += INTERVAL)); do
  # ps %cpu 使用平台 ps 的累计/衰减均值语义,此处记录其采样均值与 RSS 趋势。
  # 短时尖峰和 24h 稳态泄漏需要长跑与进一步分析。
  CPU_SAMPLES+=("$(ps -o %cpu= -p "$AGENT_PID" | tr -d ' ')")
  RSS_SAMPLES+=("$(ps -o rss= -p "$AGENT_PID" | tr -d ' ')")
  REMAINING=$((DURATION - elapsed))
  if (( REMAINING < INTERVAL )); then
    sleep "$REMAINING"
  else
    sleep "$INTERVAL"
  fi
done

CPU_AVG=$(printf '%s\n' "${CPU_SAMPLES[@]}" | awk '{s+=$1; n++} END {printf "%.2f", s/n}')
RSS_FIRST="${RSS_SAMPLES[0]}"; RSS_LAST="${RSS_SAMPLES[${#RSS_SAMPLES[@]}-1]}"
RSS_MAX=$(printf '%s\n' "${RSS_SAMPLES[@]}" | sort -n | tail -1)
RSS_GROWTH=$(( RSS_LAST - RSS_FIRST ))

echo "== 结果 =="
echo "CPU(生命周期均值): ${CPU_AVG}%   阈值: < 2%"
echo "RSS: 首 ${RSS_FIRST}KB → 末 ${RSS_LAST}KB(增长 ${RSS_GROWTH}KB,峰值 ${RSS_MAX}KB)"
echo "采样序列: ${CPU_SAMPLES[*]}"

VERDICT=0
awk -v c="$CPU_AVG" 'BEGIN { exit !(c < 2.0) }' || { echo "FAIL: CPU 超预算"; VERDICT=1; }
# RSS 增长 > 20MB 视为疑似泄漏(合成负载下应近零)
(( RSS_GROWTH < 20480 )) || { echo "FAIL: RSS 增长异常"; VERDICT=1; }

if (( VERDICT == 0 )); then
  echo "PASS: 性能预算内(F17 快速门;24h 长跑另测)"
else
  echo "agent 日志: $SMOKE_DIR/agent.log"
fi
exit "$VERDICT"
