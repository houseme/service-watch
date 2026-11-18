//! Webhook 通知器 (feature `notify-webhook`)。Slack 兼容与通用 JSON 双格式。
//! 格式枚举统一定义在 [`crate::config::WebhookFormat`](配置层的单一事实源)。
//!
//! F29:HMAC-SHA256 签名 (可选)。密钥配置后每次投递附
//! `X-Service-Watch-Signature: sha256=<hex 小写>` 头，签名对象为**实际发送的
//! body 字节**(slack/json 两格式与 spool 重放同走 [`crate::alert::Notifier::notify`]
//! 路径，天然覆盖)。签名随 `notify-webhook` feature 门控，无独立降级路径;
//! 密钥与签名值都不进日志/审计。
//! F34:签名实现 (头名/`signature_hex`) 已下沉 [`super::signature`] 共享层
//! (门控放宽为 http-client，指标推送复用同一口径) —— 本文件只引用，行为零变化。
//!
//! F37:自定义投递头。构造期接收 `BTreeMap<String, String>`（全局 `[alert] headers`
//! 或路由合并表 —— 合并语义「路由级同名键覆盖全局」在 runtime 装配侧解析），
//! notify() 时**全部**叠加到请求。语义与边界：
//! - 自定义头叠加在**签名头之外** —— 两类头独立共存：签名头 (F29) 总是附加，
//!   不因同名自定义头而丢弃；若自定义了保留头名 `X-Service-Watch-Signature`，
//!   reqwest 侧 append 会产生两个同名头（多数接收端取第一个或拒绝）——
//!   **不要自定义保留头**（doc 级约定，不做代码强拒）；
//! - 构造期一次性把名/值转换为 `HeaderName`/`HeaderValue`；注入面校验的权威
//!   在 config 层（RFC 7230 token、值非空且无 CR/LF/控制字符），此处只兜底
//!   库调用方直构 —— 非法项 WARN 丢弃（日志只含头名；头值可能是敏感 token，
//!   永不落日志，与 F29 密钥纪律同口径）；
//! - slack/json 两格式与 spool 重放同走 [`crate::alert::Notifier::notify`]
//!   路径，自定义头天然全覆盖。

use async_trait::async_trait;
use std::time::Duration;

use crate::config::WebhookFormat;
use crate::error::Result;
use crate::model::alert::AlertEvent;

pub struct WebhookNotifier {
    url: url::Url,
    format: WebhookFormat,
    timeout: Duration,
    client: reqwest::Client,
    /// F29:可选 HMAC-SHA256 签名密钥 (路由级覆盖全局的解析在 runtime 装配侧)。
    secret: Option<String>,
    /// F37:自定义投递头（构造期已转换为合法 header 名/值；非法项已 WARN 丢弃，
    /// 值不落任何日志）。
    headers: Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>,
}

impl WebhookNotifier {
    pub fn new(
        url: url::Url,
        format: WebhookFormat,
        timeout: Duration,
        secret: Option<String>,
        headers: std::collections::BTreeMap<String, String>,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            // 禁用重定向 (评审 P2):reqwest 默认跟随 10 跳，301/303 会把告警
            // POST 降级为 GET 发往新目标 —— 告警可能"成功"投到错误端点。
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| crate::error::CoreError::Alert(format!("http client build: {e}")))?;
        // F37:自定义头构造期一次性转换（notify 热路径零校验开销）。产品路径
        // 的非法头已被 config.validate 拒绝，这里只兜底库直构：非法名（非
        // RFC 7230 token）/非法值（含 CR/LF 等控制字节）WARN 丢弃 —— WARN
        // 只带头名，头值可能是敏感 token，永不落日志（F29 密钥纪律同口径）。
        let headers = headers
            .iter()
            .filter_map(|(name, value)| {
                match (
                    reqwest::header::HeaderName::from_bytes(name.as_bytes()),
                    reqwest::header::HeaderValue::from_str(value),
                ) {
                    (Ok(n), Ok(v)) => Some((n, v)),
                    _ => {
                        tracing::warn!(header = %name, "webhook custom header invalid; dropped");
                        None
                    }
                }
            })
            .collect();
        let notifier = Self {
            url,
            format,
            timeout,
            client,
            secret,
            headers,
        };
        #[cfg(not(feature = "notify-webhook"))]
        if notifier.secret.is_some() {
            // 库调用方直构面 (runtime 产品路径在 notify-webhook 关闭时根本不会
            // 走到这里，该 WARN 只提示库形态下的组合错配);读取本字段同时避免
            // feature 关闭组合下的 dead_code 警告。
            tracing::warn!(
                "webhook secret configured but notify-webhook feature disabled; requests sent unsigned"
            );
        }
        Ok(notifier)
    }

    fn render(&self, event: &AlertEvent) -> serde_json::Value {
        match self.format {
            WebhookFormat::Slack => serde_json::json!({
                "text": format!(
                    "[{:?}] {:?}/{:?}: {} (x{})",
                    event.severity, event.target, event.check, event.reason, event.occurrences
                )
            }),
            WebhookFormat::Json => serde_json::to_value(event)
                .unwrap_or_else(|_| serde_json::json!({"error": "serialize"})),
        }
    }

    /// F29:待附签名头 (`sha256=<hex 小写>`);密钥未配置 → None，请求不带
    /// 签名头。F34:委托共享实现 ([`super::signature::signature_header`]),
    /// 本地只保留方法形态供既有单测/调用点零变化。
    #[cfg(feature = "notify-webhook")]
    fn signature_header(&self, body: &[u8]) -> Option<(&'static str, String)> {
        super::signature::signature_header(self.secret.as_deref(), body)
    }
}

#[async_trait]
impl crate::alert::Notifier for WebhookNotifier {
    async fn notify(&self, event: &AlertEvent) -> Result<()> {
        let body = self.render(event);
        // F29:先序列化出**最终发送字节** —— 签名与请求体用同一份字节 (等价于
        // reqwest `.json()` 内部的 serde_json::to_vec),接收方按收到的原始 body
        // 重算 HMAC 验签必然一致。
        let body_bytes = serde_json::to_vec(&body)?;
        let mut request = self
            .client
            .post(self.url.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body_bytes.clone())
            .timeout(self.timeout);
        // F37:自定义头全部叠加（在签名头之外；同名冲突时 reqwest 侧 append，
        // 两类头共存 —— 签名头总是附加，不因同名自定义头而丢弃；保留头警示见
        // 模块 doc）。头值只进请求头，不进任何 tracing 字段。
        for (name, value) in &self.headers {
            request = request.header(name.clone(), value.clone());
        }
        // 签名值与密钥都只进请求头，不进任何 tracing 字段;签名随 notify-webhook
        // feature 门控，关闭组合下本段整体不参与编译 (request 无需 mut)。
        #[cfg(feature = "notify-webhook")]
        let request = if let Some((name, value)) = self.signature_header(&body_bytes) {
            request.header(name, value)
        } else {
            request
        };
        let resp = request.send().await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(crate::error::CoreError::Alert(format!(
                "webhook status {status}"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::alert::{Category, Severity};
    use std::time::SystemTime;

    fn sample() -> AlertEvent {
        AlertEvent {
            incident: 42,
            category: Category::Alert,
            severity: Severity::Critical,
            target: "web-1".into(),
            check: "api".into(),
            reason: "connection refused".into(),
            observed: None,
            first_seen: SystemTime::UNIX_EPOCH,
            last_seen: SystemTime::UNIX_EPOCH,
            occurrences: 3,
        }
    }

    fn notifier(format: WebhookFormat) -> WebhookNotifier {
        WebhookNotifier::new(
            url::Url::parse("http://127.0.0.1:1/hook").unwrap(),
            format,
            Duration::from_millis(200),
            None,
            std::collections::BTreeMap::new(),
        )
        .unwrap()
    }

    // ---- F37:自定义投递头（构造期转换 / 非法项丢弃；线上叠加语义由
    // tests/alert_routing.rs 的双 stub 集成测试覆盖）----

    /// 合法头保留并按 HeaderName 小写归一；非法名（非 token）/非法值（CR/LF）
    /// 构造期 WARN 丢弃，不 panic、不误伤其余头。空值是合法 HTTP 头值
    /// (HeaderValue::from_str("") = Ok) —— notifier 兜底只拦 HTTP 非法项，
    /// 「值不得为空」是 config 层的注入面策略 (产品配置到不了这里)。
    #[test]
    fn custom_headers_collected_and_invalid_dropped() {
        let mut headers = std::collections::BTreeMap::new();
        headers.insert("X-Auth-Token".to_string(), "route-secret".to_string());
        headers.insert("Bad Header".to_string(), "v".to_string());
        headers.insert("X-Inj".to_string(), "line1\r\nline2".to_string());
        headers.insert("X-Empty".to_string(), String::new());
        let n = WebhookNotifier::new(
            url::Url::parse("http://127.0.0.1:1/hook").unwrap(),
            WebhookFormat::Json,
            Duration::from_millis(200),
            None,
            headers,
        )
        .unwrap();
        let kept: Vec<&str> = n.headers.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            kept,
            vec!["x-auth-token", "x-empty"],
            "非法名/值丢弃;合法 token 头保留 (HeaderName 小写归一，含合法空值)"
        );
        assert_eq!(
            n.headers[0].1,
            reqwest::header::HeaderValue::from_static("route-secret")
        );
    }

    /// Slack 格式:{"text": 一行摘要},含 target/check/reason/occurrences。
    #[test]
    fn slack_payload_is_text_object() {
        let body = notifier(WebhookFormat::Slack).render(&sample());
        assert_eq!(
            body["text"].as_str().unwrap(),
            "[Critical] \"web-1\"/\"api\": connection refused (x3)"
        );
    }

    /// JSON 格式:AlertEvent 原样序列化 (字段完整)。
    #[test]
    fn json_payload_is_full_event() {
        let body = notifier(WebhookFormat::Json).render(&sample());
        assert_eq!(body["incident"].as_u64(), Some(42));
        assert_eq!(body["category"].as_str(), Some("alert"));
        assert_eq!(body["severity"].as_str(), Some("critical"));
        assert_eq!(body["occurrences"].as_u64(), Some(3));
        assert_eq!(body["reason"].as_str(), Some("connection refused"));
    }

    // ---- F29:HMAC-SHA256 签名 (F34 起实现下沉 super::signature,RFC 4231
    // 向量测试随共享 helper;此处经委托方法锁定 webhook 侧口径不变) ----

    /// 密钥未配置 → 不产生签名头 (裸投递);配置后 → 头名与 `sha256=` 前缀。
    #[cfg(feature = "notify-webhook")]
    #[test]
    fn signature_header_absent_without_secret() {
        let unsigned = notifier(WebhookFormat::Json);
        assert!(
            unsigned.signature_header(b"body").is_none(),
            "None 密钥不加签名头"
        );
        let signed = WebhookNotifier::new(
            url::Url::parse("http://127.0.0.1:1/hook").unwrap(),
            WebhookFormat::Json,
            Duration::from_millis(200),
            Some("Jefe".into()),
            std::collections::BTreeMap::new(),
        )
        .unwrap();
        let (name, value) = signed
            .signature_header(b"what do ya want for nothing?")
            .unwrap();
        assert_eq!(name, "X-Service-Watch-Signature");
        assert_eq!(
            value,
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
}
