//! 探测执行(DESIGN.md §7.1)。所有探测被 `tokio::time::timeout` 包裹,超时 = 失败。

use crate::config::{CheckCfg, CheckKindCfg};
use crate::model::snapshot::ProcessSnapshot;

/// 单次探测结果。
#[derive(Debug, Clone)]
pub enum ProbeResult {
    /// 观测能力暂不可用,本轮不推进健康状态机。
    Unavailable {
        reason: String,
    },
    Ok {
        observed: Option<String>,
    },
    Fail {
        reason: String,
        observed: Option<String>,
    },
}

impl ProbeResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok { .. })
    }
    pub fn reason(&self) -> &str {
        match self {
            Self::Ok { .. } => "",
            Self::Fail { reason, .. } | Self::Unavailable { reason } => reason,
        }
    }
    pub fn observed(&self) -> Option<&str> {
        match self {
            Self::Ok { observed, .. } | Self::Fail { observed, .. } => observed.as_deref(),
            Self::Unavailable { .. } => None,
        }
    }
}

/// 探测上下文:HTTP 客户端(http probe)与目标进程最新快照(process probe)。
pub struct ProbeCtx<'a> {
    #[cfg(feature = "probe-http")]
    pub http: Option<&'a reqwest::Client>,
    /// process 检查目标的最新快照;`None` = 快照缺失(视为失败,PID 复用防护)。
    pub process_snapshot: Option<&'a ProcessSnapshot>,
    /// 期望匹配的进程身份(可选;用于校验快照属于该目标)。
    pub expected_identity: Option<&'a crate::model::ids::ProcessIdentity>,
}

/// 执行一次探测。**必须**在调用方以 `tokio::time::timeout(check.timeout, ...)` 包裹。
pub async fn execute(check: &CheckCfg, ctx: ProbeCtx<'_>) -> ProbeResult {
    match &check.kind {
        CheckKindCfg::Tcp { addr } => {
            match tokio::time::timeout(check.timeout, tokio::net::TcpStream::connect(*addr)).await {
                Ok(Ok(_)) => ProbeResult::Ok {
                    observed: Some(format!("connect {addr} ok")),
                },
                Ok(Err(e)) => ProbeResult::Fail {
                    reason: format!("connect {addr}: {e}"),
                    observed: None,
                },
                Err(_) => ProbeResult::Fail {
                    reason: format!("connect {addr}: timeout"),
                    observed: None,
                },
            }
        }
        CheckKindCfg::Http {
            url,
            expected_status,
        } => {
            #[cfg(feature = "probe-http")]
            {
                run_http(check, url, *expected_status, ctx).await
            }
            #[cfg(not(feature = "probe-http"))]
            {
                let _ = (url, expected_status, ctx);
                ProbeResult::Fail {
                    reason: "probe-http feature disabled".into(),
                    observed: None,
                }
            }
        }
        CheckKindCfg::Process {
            process,
            cpu_threshold,
            mem_threshold_mb,
        } => run_process_check(process, *cpu_threshold, *mem_threshold_mb, ctx),
    }
}

#[cfg(feature = "probe-http")]
async fn run_http(
    check: &CheckCfg,
    url: &url::Url,
    expected: crate::config::StatusRange,
    ctx: ProbeCtx<'_>,
) -> ProbeResult {
    let Some(client) = ctx.http else {
        return ProbeResult::Fail {
            reason: "http client unavailable".into(),
            observed: None,
        };
    };
    let fut = async {
        match client.get(url.clone()).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if expected.0 <= status && status <= expected.1 {
                    ProbeResult::Ok {
                        observed: Some(format!("status {status}")),
                    }
                } else {
                    ProbeResult::Fail {
                        reason: format!(
                            "status {status} outside {}",
                            expected.serialize_as_string()
                        ),
                        observed: Some(format!("status {status}")),
                    }
                }
            }
            Err(e) => ProbeResult::Fail {
                reason: format!("request: {e}"),
                observed: None,
            },
        }
    };
    match tokio::time::timeout(check.timeout, fut).await {
        Ok(r) => r,
        Err(_) => ProbeResult::Fail {
            reason: format!("http {url}: timeout"),
            observed: None,
        },
    }
}

fn run_process_check(
    process: &str,
    cpu_threshold: Option<f32>,
    mem_threshold_mb: Option<u64>,
    ctx: ProbeCtx<'_>,
) -> ProbeResult {
    let _ = process; // 名称解析由 eval 完成,这里只消费已解析身份
    let Some(snap) = ctx.process_snapshot else {
        return ProbeResult::Fail {
            reason: "process not found in latest snapshot".into(),
            observed: None,
        };
    };
    if let Some(expected) = ctx.expected_identity
        && snap.identity != *expected
    {
        return ProbeResult::Fail {
            reason: format!("identity mismatch (pid reuse?): {}", snap.identity),
            observed: None,
        };
    }
    if cpu_threshold.is_some() && snap.cpu_usage_percent.is_none() {
        return ProbeResult::Unavailable {
            reason: "process CPU measurement unavailable".into(),
        };
    }
    if mem_threshold_mb.is_some() && snap.memory_bytes.is_none() {
        return ProbeResult::Unavailable {
            reason: "process memory measurement unavailable".into(),
        };
    }
    if let Some(th) = cpu_threshold
        && let Some(cpu) = snap.cpu_usage_percent
        && cpu > th
    {
        return ProbeResult::Fail {
            reason: format!("cpu {cpu:.1}% > {th:.1}%"),
            observed: Some(format!("cpu {cpu:.1}%")),
        };
    }
    if let Some(th_mb) = mem_threshold_mb
        && let Some(mem) = snap.memory_bytes
    {
        let mb = mem / 1024 / 1024;
        if mb > th_mb {
            return ProbeResult::Fail {
                reason: format!("mem {mb}MB > {th_mb}MB"),
                observed: Some(format!("mem {mb}MB")),
            };
        }
    }
    ProbeResult::Ok {
        observed: Some("within thresholds".into()),
    }
}
