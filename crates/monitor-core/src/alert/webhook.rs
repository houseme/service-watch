//! Webhook 通知器(feature `notify-webhook`)。Slack 兼容与通用 JSON 双格式。
//! 格式枚举统一定义在 [`crate::config::WebhookFormat`](配置层的单一事实源)。

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
}

impl WebhookNotifier {
    pub fn new(url: url::Url, format: WebhookFormat, timeout: Duration) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            // 禁用重定向(评审 P2):reqwest 默认跟随 10 跳,301/303 会把告警
            // POST 降级为 GET 发往新目标 —— 告警可能"成功"投到错误端点。
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| crate::error::CoreError::Alert(format!("http client build: {e}")))?;
        Ok(Self {
            url,
            format,
            timeout,
            client,
        })
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
}

#[async_trait]
impl crate::alert::Notifier for WebhookNotifier {
    async fn notify(&self, event: &AlertEvent) -> Result<()> {
        let body = self.render(event);
        let resp = self
            .client
            .post(self.url.clone())
            .json(&body)
            .timeout(self.timeout)
            .send()
            .await?;
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
        )
        .unwrap()
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

    /// JSON 格式:AlertEvent 原样序列化(字段完整)。
    #[test]
    fn json_payload_is_full_event() {
        let body = notifier(WebhookFormat::Json).render(&sample());
        assert_eq!(body["incident"].as_u64(), Some(42));
        assert_eq!(body["category"].as_str(), Some("alert"));
        assert_eq!(body["severity"].as_str(), Some("critical"));
        assert_eq!(body["occurrences"].as_u64(), Some(3));
        assert_eq!(body["reason"].as_str(), Some("connection refused"));
    }
}
