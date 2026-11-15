# service-watch launchd 部署(macOS)

> plist:[com.houseme.service-watch.plist](com.houseme.service-watch.plist),
> 安装目标 `/Library/LaunchDaemons/`(系统级 LaunchDaemon 域)。
> 总览/升级/卸载见 [../README.md](../README.md),日常处置见 [../../RUNBOOK.md](../../RUNBOOK.md)。

## 1. 安装

```bash
# 0) 构建与目录(产物 target/release/monitor-agent / monitorctl)
sudo install -m 0755 target/release/monitor-agent /usr/local/bin/
sudo install -m 0755 target/release/monitorctl    /usr/local/bin/
sudo mkdir -p /etc/service-watch /var/lib/service-watch /var/log/service-watch
sudo install -m 0644 config/config.example.toml /etc/service-watch/config.toml
# $ sudoedit /etc/service-watch/config.toml
# 确认 [agent].data_dir = "/var/lib/service-watch"

# 1) 配置与采集自检
/usr/local/bin/monitor-agent --check-config /etc/service-watch/config.toml
/usr/local/bin/monitor-agent --dump-snapshot | head

# 2) 放置 plist —— LaunchDaemon 域要求 root:wheel 且 0644,否则 launchd 拒载
sudo cp docs/deploy/launchd/com.houseme.service-watch.plist /Library/LaunchDaemons/
sudo chown root:wheel /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo chmod 644 /Library/LaunchDaemons/com.houseme.service-watch.plist

# 3) 加载(两种方式按 macOS 版本二选一)
sudo launchctl bootstrap system /Library/LaunchDaemons/com.houseme.service-watch.plist
#   旧版(macOS ≤ 10.10,bootstrap 不可用时)等价命令:
# sudo launchctl load /Library/LaunchDaemons/com.houseme.service-watch.plist
```

`bootstrap`/`load` 后因 `RunAtLoad=true`(且 KeepAlive 隐含 RunAtLoad,见 plist
头注释)job 会立即启动一次。

## 2. 验证

```bash
# (a) launchd 视角:state = running + PID;last exit status 观察退出码轨迹
sudo launchctl print system/com.houseme.service-watch
sudo launchctl list | grep service-watch        # 第二列 = 上次退出码,第三列 = PID

# (b) 控制平面
monitorctl status
curl -fsS http://127.0.0.1:9170/health
curl -fsS http://127.0.0.1:9170/ready           # {"ready":true}

# (c) 日志
tail -f /var/log/service-watch/agent.log
tail -f /var/log/service-watch/agent.err.log    # tracing 的 ERROR/WARN 也走 stdout;
                                                # err.log 通常应为空,有内容优先看它
```

常用控制命令(语义对照见 systemd/README §4,行为一致):

```bash
sudo launchctl kickstart system/com.houseme.service-watch            # 启动(已在跑则无操作)
sudo launchctl kickstart -k system/com.houseme.service-watch         # 杀掉并重启(SIGTERM→优雅停机退出 130→kickstart 立即再启)
sudo launchctl kill SIGTERM system/com.houseme.service-watch         # 仅发 SIGTERM(优雅停机退出 130;job 仍加载,KeepAlive 在节流后拉起)
sudo launchctl bootout system/com.houseme.service-watch              # 卸载并停止(对应 systemctl stop;job 已卸载,不会拉起)
```

## 3. 卸载

```bash
sudo launchctl bootout system/com.houseme.service-watch
# 旧版: sudo launchctl unload /Library/LaunchDaemons/com.houseme.service-watch.plist
sudo rm /Library/LaunchDaemons/com.houseme.service-watch.plist
# 数据/配置/日志确认后删除(见 ../README.md §4 的保留建议)
sudo rm -rf /etc/service-watch /var/lib/service-watch /var/log/service-watch
```

bootout 停止 = SIGTERM → agent 优雅停机,退出码 **130**(DESIGN §9.3 信号停机;
job 已被卸载,KeepAlive 不适用,不会被拉起);被监管子进程保留(AbandonProcessGroup
+ 独立进程组),处置方式见 RUNBOOK。

## 4. 日志位置与轮转

| 流向 | 位置 | 说明 |
|---|---|---|
| tracing 输出 | `/var/log/service-watch/agent.log` | StandardOutPath;`--log-json` 时逐行 JSON |
| 进程 stderr | `/var/log/service-watch/agent.err.log` | 正常应为空;非空优先排查 |
| 事件持久化 | `/var/lib/service-watch/journal.jsonl` | 非日志,append-only 事件源(Fatal/重启/incident),**不要与日志混轮转** |

launchd 代持这两个日志文件的句柄(O_APPEND)。轮转注意:**newsyslog 的默认
rename 式轮转对"进程持有 fd 持续 append"的文件无效**——rename 后进程仍写旧
inode,新文件永远为空。见 RUNBOOK §6.3:macOS 推荐用 newsyslog 片段管理时先
理解其局限,或直接采用"copy + truncate"式轮转(对本项目的 O_APPEND 写者安全,
有极小竞态窗口,RUNBOOK 中有说明与脚本)。

## 5. App Nap / 休眠注意

- **App Nap 不影响本部署**:App Nap 只作用于用户 GUI 会话中的应用;本 job 安装在
  `/Library/LaunchDaemons/`(system 域,headless,不属于任何 Aqua 会话),不在
  App Nap 的管辖范围,无需 `NSProcessInfo` 活动声明或 `ProcessType` 调整。
- **合盖/系统休眠会暂停 agent**:macOS 笔记本进入 sleep 后全部用户态进程(包括
  LaunchDaemon)被冻结,唤醒后继续。agent 对时钟跳变有显式处理(DESIGN §7.1:
  休眠唤醒后首轮探测丢弃并记 degraded),不会把休眠时长误判为服务故障;但休眠
  期间的检查与告警是真空期,依赖 deadman_url 心跳的外部监控会收到超时——按
  "主机失联"而非"agent 故障"处理。
- 服务器用途建议禁用系统休眠(接电源场景):

```bash
sudo pmset -c sleep 0 disksleep 0        # -c = 接电源;笔记本用户态可先 caffeinate 临时验证
pmset -g                                 # 核对当前策略
```

- 若确实需要"合盖继续跑",除 pmset 外可评估 `caffeinate -s` 包装(仅接电源有效),
  或改用 `Disabled`+定时 `kickstart` 的调度模式——但对监控 agent 通常直接禁休眠
  更简单可靠。

## 6. 常见故障表

| 现象 | 诊断 | 处置 |
|---|---|---|
| `bootstrap` 报 `Input/output error` 或 plist 未加载 | plist 权限不是 root:wheel 0644,或 XML 语法错误 | `plutil -lint com.houseme.service-watch.plist`;核对权限 |
| `launchctl print` 显示 state = not running,last exit = 1 | 配置错误(退出码 1) | `agent.err.log` / `agent.log` 看 `config error:`;`--check-config` 修好后 `kickstart` |
| job 以 5s 节流反复重启 | ThrottleInterval 生效中;持续非零退出 | 见上;`sudo launchctl list` 第二列持续非 0 即持续失败 |
| 9170 被占,bind 失败退出码 2 | 旧实例或其他进程占用 | `lsof -nP -iTCP:9170 -sTCP:LISTEN`;处置后 `kickstart` |
| `monitorctl` unreachable(退出码 4) | agent 未运行 / `[control].enabled=false` / 地址不符 | `curl http://127.0.0.1:9170/health` 定位 |
| 重启机器后 agent 没起来 | FileVault 启用且未解锁磁盘前 LaunchDaemon 会被延迟 | 属预期(系统级 daemon 在解锁后加载);确认 `sudo launchctl print system/…` |
| 日志文件轮转后不再增长 | newsyslog rename 式轮转对持有 fd 的写者无效 | 见本 README §4 与 RUNBOOK §6.3(copy+truncate 方案) |
| 唤醒后出现 `degraded` / 首轮探测丢弃 | DESIGN §7.1 显式设计(防休眠时长误判) | 无需处理;持续出现则检查 `[collect].interval` 与系统时钟源 |
