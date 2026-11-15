#Requires -RunAsAdministrator
<#
.SYNOPSIS
    将 service-watch monitor-agent 注册为 Windows 服务(SCM 管理)。

.DESCRIPTION
    使用 New-Service 的结构化参数创建服务,sc.exe 配置失败恢复动作。
    ImagePath 保留带引号的绝对路径,账户密码通过 PSCredential 传递。

    停止语义(DESIGN.md §8.6 / ROADMAP F15):
    binPath 以 --service-run 运行 SCM 感知形态 —— 要求二进制以
    `--features windows-service` 构建(标准构建下 agent 拒绝服务模式,
    打印说明并以退出码 1 结束)。`sc stop` = SERVICE_CONTROL_STOP →
    优雅停机(采集线程停 + alert 分发排空 + journal flush)。
    被监管子进程保留(Job Object 默认不带 KILL_ON_JOB_CLOSE,观察者外置)。
    强杀可能丢失尚未确认落盘的 journal 队列和在途记录。

.PARAMETER InstallDir
    monitor-agent.exe 所在目录。默认 C:\service-watch。

.PARAMETER ConfigPath
    配置文件路径。默认 C:\service-watch\config.toml。

.PARAMETER DataDir
    [agent].data_dir(journal.jsonl 目录)。默认 C:\service-watch\data。
    必须与 config.toml 中的 data_dir 一致。

.PARAMETER ServiceName
    Windows 服务名。默认 service-watch。

.PARAMETER ServiceAccount
    服务运行账户。默认 LocalSystem;也可用 .\svcwatch 或 DOMAIN\svcwatch
    (此时需要 -ServiceAccountPassword,且脚本会授予其对 DataDir 的完全控制 ACL)。

.PARAMETER ServiceAccountPassword
    专用账户的密码(仅 -ServiceAccount 非 LocalSystem/LocalService/NetworkService
    时必填)。

.PARAMETER NoStart
    只注册不启动(供 Ansible/脚本编排先行统一拉起)。

.EXAMPLE
    .\install-service.ps1
    .\install-service.ps1 -InstallDir 'D:\apps\service-watch' -ConfigPath 'D:\apps\service-watch\config.toml' -DataDir 'D:\data\service-watch'
#>
[CmdletBinding()]
param(
    [string]$InstallDir   = 'C:\service-watch',
    [string]$ConfigPath   = 'C:\service-watch\config.toml',
    [string]$DataDir      = 'C:\service-watch\data',
    [string]$ServiceName  = 'service-watch',
    [string]$ServiceAccount = 'LocalSystem',
    [string]$ServiceAccountPassword = '',
    [switch]$NoStart
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScExe = Join-Path $env:SystemRoot 'System32\sc.exe'
$IcaclsExe = Join-Path $env:SystemRoot 'System32\icacls.exe'

function Assert-LastExit([string]$What) {
    if ($LASTEXITCODE -ne 0) {
        throw "$What failed: sc/icacls exit code $LASTEXITCODE"
    }
}

Write-Host "== service-watch Windows 服务安装 ==" -ForegroundColor Cyan

# ---------------------------------------------------------------------------
# 1. 前置校验(管理员权限已由 #Requires -RunAsAdministrator 保证)
# ---------------------------------------------------------------------------

# 二进制必须存在且为绝对路径 —— binPath 里的路径会被写进注册表 ImagePath,
# 相对路径会被 SCM 按 System32 解析,是经典部署事故。
if (-not ([System.IO.Path]::IsPathRooted($InstallDir)))  { throw "-InstallDir 必须是绝对路径: $InstallDir" }
if (-not ([System.IO.Path]::IsPathRooted($ConfigPath)))  { throw "-ConfigPath 必须是绝对路径: $ConfigPath" }
if (-not ([System.IO.Path]::IsPathRooted($DataDir)))     { throw "-DataDir 必须是绝对路径: $DataDir" }

$ExePath = Join-Path $InstallDir 'monitor-agent.exe'
if (-not (Test-Path -LiteralPath $ExePath -PathType Leaf)) {
    throw "未找到二进制: $ExePath (先 cargo build --release -p monitor-agent -p monitorctl 并拷贝产物)"
}
if (-not (Test-Path -LiteralPath $ConfigPath -PathType Leaf)) {
    throw "未找到配置: $ConfigPath (先放置 config.toml;模板见仓库 config/config.example.toml)"
}

# 安装前离线校验配置(退出码 0=合法,1=配置错误,DESIGN §9.3)
& $ExePath --check-config $ConfigPath
if ($LASTEXITCODE -ne 0) {
    throw "配置校验失败(--check-config 退出码 $LASTEXITCODE);修复后重试"
}

# data_dir 与 config 中的 [agent].data_dir 需一致 —— 这里只确保目录存在;
# 真正的一致性由 --check-config 与运行期报错兜底( journal 写失败会留 ERROR 日志)。
if (-not (Test-Path -LiteralPath $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
}

# ---------------------------------------------------------------------------
# 2. 专用账户的目录授权(LocalSystem 默认拥有本机全部本地路径权限,无需 ACL)
# ---------------------------------------------------------------------------
$BuiltinAccounts = @('LocalSystem', 'NT AUTHORITY\LocalSystem', 'LocalService', 'NetworkService')
if ($BuiltinAccounts -notcontains $ServiceAccount) {
    if ([string]::IsNullOrEmpty($ServiceAccountPassword)) {
        throw "专用账户 $ServiceAccount 需要 -ServiceAccountPassword"
    }
    Write-Host "授予 ${ServiceAccount} 对 $DataDir 的完全控制 ..."
    # (OI)(CI) = 对象继承+容器继承;F = full。账户需已有"作为服务登录"权利
    # (域环境一般由组策略授予;本机账户可用 secpol.msc 手工确认)。
    & $IcaclsExe $DataDir /grant "${ServiceAccount}:(OI)(CI)F" | Out-Null
    Assert-LastExit 'icacls (grant DataDir)'
}

# ---------------------------------------------------------------------------
# 3. 已存在检查(幂等性交还给运维:重装 = 先 delete 再 install)
# ---------------------------------------------------------------------------
$existing = Get-Service | Where-Object { $_.Name -eq $ServiceName }
if ($null -ne $existing) {
    throw "服务 $ServiceName 已存在(state=$($existing.Status))。重装请先: " +
          "Stop-Service $ServiceName -Force; sc.exe delete $ServiceName"
}

# ---------------------------------------------------------------------------
# 4. 创建服务(结构化参数保留 ImagePath 与账户凭据)
# ---------------------------------------------------------------------------
# New-Service 直接调用服务管理接口,路径中的空格、&、% 和账户密码均保持字面值。
# 文档 https://learn.microsoft.com/powershell/module/microsoft.powershell.management/new-service
foreach ($Value in @($ExePath, $ConfigPath, $DataDir, $ServiceName)) {
    if ($Value.Contains('"') -or $Value -match '[\x00-\x1f]') {
        throw '路径和服务名必须可编码为 Windows 命令行参数'
    }
}
$LogPath = Join-Path $DataDir 'agent.log'
$binPathValue = '"{0}" --config "{1}" --service-run --service-name "{2}" --log-file "{3}"' -f `
    $ExePath, $ConfigPath, $ServiceName, $LogPath
$ServiceParameters = @{
    Name = $ServiceName
    BinaryPathName = $binPathValue
    StartupType = 'Automatic'
    Description = 'service-watch monitoring agent (supervise/alert; see docs/RUNBOOK.md)'
}
if ($ServiceAccount -notin @('LocalSystem', 'NT AUTHORITY\LocalSystem')) {
    $AccountName = switch ($ServiceAccount) {
        'LocalService' { 'NT AUTHORITY\LocalService' }
        'NetworkService' { 'NT AUTHORITY\NetworkService' }
        default { $ServiceAccount }
    }
    $Password = if ([string]::IsNullOrEmpty($ServiceAccountPassword)) {
        New-Object System.Security.SecureString
    } else {
        ConvertTo-SecureString $ServiceAccountPassword -AsPlainText -Force
    }
    $ServiceParameters.Credential = [System.Management.Automation.PSCredential]::new($AccountName, $Password)
}
New-Service @ServiceParameters | Out-Null

# 失败恢复动作:24h(86400s)无失败则清零计数;第 1/2/后续次失败均在 5s 后重启。
# 语义提醒:sc stop(受控停止)不计入失败动作;仅异常退出(崩溃/非零退出)触发。
& $ScExe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/5000
Assert-LastExit 'sc.exe failure'
# service.rs 以 Stopped + 非零错误码报告受控故障,需开启非 crash failure 动作。
& $ScExe failureflag $ServiceName 1
Assert-LastExit 'sc.exe failureflag'

& $ScExe description $ServiceName 'service-watch monitoring agent (supervise/alert; see docs/RUNBOOK.md)'
Assert-LastExit 'sc.exe description'

# ---------------------------------------------------------------------------
# 5. 防火墙 —— 默认不建规则,理由如下(如需放开再取消注释)
# ---------------------------------------------------------------------------
# 控制平面默认仅监听 127.0.0.1:9170(config [control].listen,DESIGN §4.1:
# "仅回环;改非回环需自行承担风险")。回环流量不经过 Windows 防火墙的入站
# 过滤策略,因此无需任何入站规则 —— 现在建规则只会在未来把 listen 改到
# 非回环时给出虚假安全感。
#
# <# 将来 [control].listen 改为非回环时的参考(注意 v0.1 控制平面无鉴权):
#    New-NetFirewallRule -DisplayName 'service-watch control plane (9170)' `
#        -Direction Inbound -Action Allow -Protocol TCP -LocalPort 9170 `
#        -RemoteAddress 127.0.0.1 -Profile Any
#    (RemoteAddress 127.0.0.1 仅作声明;真正放开到子网时请同时收紧
#     RemoteAddress 为可信网段,并评估无鉴权风险)
# #>

# ---------------------------------------------------------------------------
# 6. 启动与验证
# ---------------------------------------------------------------------------
if ($NoStart) {
    Write-Host "已注册但未启动(-NoStart)。手动启动: Start-Service $ServiceName"
}
else {
    Start-Service -Name $ServiceName
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        Start-Sleep -Seconds 1
        try {
            $r = Invoke-RestMethod -Uri 'http://127.0.0.1:9170/ready' -TimeoutSec 2
            if ($r.ready) { $ready = $true; break }
        } catch { }
    }
    if ($ready) {
        Write-Host '/ready = true,agent 已就绪' -ForegroundColor Green
    }
    else {
        Write-Warning "/ready 30s 内未就绪 —— 用 sc.exe query $ServiceName 与 Get-WinEvent 排查;配置错误会在启动后立刻以退出码 1 结束并触发 failure 动作"
    }
}

& $ScExe qc $ServiceName
Write-Host ''
Write-Host '后续:' -ForegroundColor Cyan
Write-Host "  状态   : sc.exe query $ServiceName ; monitorctl status"
Write-Host "  事件   : Get-WinEvent -FilterHashtable @{LogName='System'; ProviderName='Service Control Manager'} -MaxEvents 10"
Write-Host "  数据   : $DataDir\journal.jsonl (incident/restart/fatal/reset/audit)"
Write-Host '  文档   : docs/deploy/windows/README.md 与 docs/RUNBOOK.md'
