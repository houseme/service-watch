# service-watch systemd 部署(Linux)

> unit 文件:[service-watch.service](service-watch.service),安装目标
> `/etc/systemd/system/service-watch.service`。总览/升级/卸载见
> [../README.md](../README.md),日常处置见 [../../RUNBOOK.md](../../RUNBOOK.md)。

## 1. 安装步骤

### 1.1 创建运行账户与目录

```bash
# 专用系统账户:无登录 shell,home 即 data_dir(便于属主管理)
sudo useradd --system --user-group \
             --home-dir /var/lib/service-watch \
             --shell /usr/sbin/nologin svcwatch      # RHEL/Fedora 为 /sbin/nologin

# 目录布局约定
sudo mkdir -p /etc/service-watch /var/lib/service-watch

# data_dir 所有权:journal.jsonl 由 agent 以 svcwatch 身份 append 写入,
# 属主必须是 svcwatch(见 deploy 总览 FAQ §5.2 —— 关键写入失败会触发实例故障停机)。
sudo chown svcwatch:svcwatch /var/lib/service-watch
sudo chmod 750 /var/lib/service-watch

# 配置目录:svcwatch 只需读;属主 root 便于配置管理
sudo chown root:root /etc/service-watch
sudo chmod 755 /etc/service-watch
sudo install -m 0640 -o root -g svcwatch \
     config/config.example.toml /etc/service-watch/config.toml
# $ sudoedit /etc/service-watch/config.toml   # 按需修改:webhook、check、service 等
# 确认 [agent].data_dir = "/var/lib/service-watch"
```

### 1.2 安装二进制与 unit

```bash
sudo install -m 0755 target/release/monitor-agent /usr/local/bin/
sudo install -m 0755 target/release/monitorctl    /usr/local/bin/

# 先离线校验(unit 的 ExecStartPre 也会再校验一次,但提前做避免起停噪音)
monitor-agent --check-config /etc/service-watch/config.toml

sudo install -m 0644 docs/deploy/systemd/service-watch.service \
     /etc/systemd/system/service-watch.service
sudo systemctl daemon-reload
sudo systemctl enable --now service-watch
```

`enable --now` = 设置开机自启并立即启动。若 `enable` 后 start 失败,
`systemctl status service-watch` 与 `journalctl -u service-watch -n 50` 给出原因
(最常见:配置校验失败、data_dir 属主不对、9170 被占)。

## 2. 验证

```bash
# (a) systemd 视角:active (running),无 restart 计数增长
systemctl status service-watch

# (b) 控制平面视角:全 target 状态摘要
monitorctl status

# (c) HTTP 端点(DESIGN §9.2):
curl -fsS http://127.0.0.1:9170/health    # 浅存活,200 空体
curl -fsS http://127.0.0.1:9170/ready     # {"ready":true} = 配置加载成功 + 调度循环已启动
curl -fsS http://127.0.0.1:9170/status | jq '.targets[] | {name, mode, health, recovery}'
curl -fsS http://127.0.0.1:9170/metrics                    # JSON 指标
curl -fsS 'http://127.0.0.1:9170/metrics?format=prometheus' # Prometheus 文本格式

# (d) 配置错误反例(应看到 ExecStartPre 校验失败,单元起不来 —— 这是对的):
#   $ sudo systemctl start service-watch; journalctl -u service-watch -n 5
```

判定就绪的权威信号是 **/ready 为 200**(降级采集不影响 ready;温度缺失等走
`Degradation` 路径,属正常降级而非不就绪)。

## 3. 日志

```bash
journalctl -u service-watch -f          # 跟随
journalctl -u service-watch --since "10 min ago"
journalctl -u service-watch -p warning  # 仅 warning 及以上
journalctl -u service-watch -o json-pretty | jq '.MESSAGE | fromjson'  # 需 --log-json 时
```

- 需要 JSON 结构化日志:在 unit 的 `ExecStart` 追加 `--log-json` 后
  `daemon-reload + restart`(或改 config `[log].json = true`)。
- 等级由 `Environment=RUST_LOG=info` 控制;临时排查用 drop-in,不要改 unit 本体:

```bash
sudo systemctl edit service-watch
#   [Service]
#   Environment=RUST_LOG=debug
sudo systemctl restart service-watch
```

## 4. SIGTERM 语义(systemctl stop / restart 时发生什么)

DESIGN §8.7(v0.1 权威语义):

1. `systemctl stop` 向**主进程**(KillMode=process,仅 agent 本体)发送 SIGTERM;
2. agent 优雅停机:停探测 → flush journal → 退出码 **130**(DESIGN §9.3 信号
   停机;unit 以 `SuccessExitStatus=130` 记为干净退出,单元状态不会变 failed);
3. **被监管子进程不被停止、不被 SIGKILL** —— systemd 因 `KillMode=process` 不做
   cgroup 收尾,Unix 侧子进程又运行在自身进程组(DESIGN §8.4 `process_group(0)`),
   双保险之下孤儿进程存活;
4. agent 重启后重放 Fatal、预算和进程身份，确认旧身份存活后只读接管，确认消失后按预算恢复。身份未知时保持等待，旧版 journal 或尚无身份记录的实例需运维检查。

因此:

- `systemctl restart` ≠ "重启被监管服务"。要重启某个被监管目标用
  `monitorctl restart <target>`(Fatal 态需先 `monitorctl reset <target>`)。
- 想"连孩子一起停"需人工:`systemctl stop service-watch` 后自行结束目标进程,
  或者临时改用 `KillMode=control-group`(改完记得改回来,那不是默认推荐形态)。

## 5. 常见故障表

| 现象 | 诊断 | 处置 |
|---|---|---|
| `systemctl start` 报 `Failed with result 'exit-code'`,journal 有 `config error:` | 配置校验失败(退出码 1) | 按 journal 提示修配置;`monitor-agent --check-config` 通过后 `systemctl reset-failed service-watch && systemctl start service-watch` |
| 单元进入 `failed`,journal 显示 Start limit hit | StartLimitBurst 熔断(10 次/300s),通常是持续配置错误或依赖端口被占 | 见 deploy 总览 FAQ §5.1/§5.4;修复后 `reset-failed` 再 start |
| agent 运行但 `monitorctl` 报 agent unreachable(退出码 4) | `[control].enabled=false`、端口/地址不一致、agent 未启动 | `curl http://127.0.0.1:9170/health` 定位;核对 `--url` 与 `[control].listen` |
| journalctl 反复出现 `journal: 写入失败` | data_dir 权限/磁盘满 | `chown svcwatch:svcwatch /var/lib/service-watch`;`df -h`;检查 journal 失败计数与退出码，修复后重启 |
| 端口占用:`Address already in use`(bind 失败,退出码 2) | 9170 被其他进程或旧实例占用 | `ss -ltnp 'sport = :9170'` / `lsof -iTCP:9170`;释放或改 `[control].listen` |
| restart 循环但每次都能起来又退出 | 多为 `[[service]]` 目标 crash loop 耗尽 budget → Fatal | `monitorctl status` 看 Fatal;journal 查 `Restart.fast_exit`;按 RUNBOOK §1.3 处置 |
| 升级后 Fatal target 一直不拉起 | Fatal 是终态闩锁,跨重启保留,跳过冷启动 spawn | 设计行为;修根因后 `monitorctl reset <target>` |
| SELinux enforcing 下无法执行 /usr/local/bin/monitor-agent | 非标准路径 label | `restorecon -v /usr/local/bin/monitor-agent`;或改放 `/usr/bin`(packaged 路径) |
| `Network is unreachable` 类探测失败风暴 | network-online 未真正等齐(如仅 systemd-networkd 无 wait-online) | 确认 `NetworkManager-wait-online.service` / `systemd-networkd-wait-online.service` 存在并被本单元 Wants 链路拉起;或接受 agent 内部重试(agent 设计上不依赖 systemd 硬等网络) |
