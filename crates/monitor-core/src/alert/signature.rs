//! F34:HMAC-SHA256 签名共享层 —— webhook (F29) 与指标周期推送 (F34) 复用
//! 同一实现，签名口径/头名完全一致：
//! - 头名 `X-Service-Watch-Signature`,值 `sha256=<hex 小写>`;
//! - 签名对象 = **实际发送的 body 字节**(调用方先 `serde_json::to_vec` 出
//!   最终字节再签名发送，接收方按收到的原始 body 重算 HMAC 即可验签);
//! - 密钥未配置 → 无签名头 (裸投递，行为与既有版本一致);密钥值与签名值
//!   都不进任何日志/审计 (返回值只进请求头)。
//!
//! 门控 `http-client`:两消费方 ([`crate::alert::webhook`] /
//! [`crate::alert::metrics_push`]) 同在该门控内;hmac/sha2 纯 Rust 无 C 依赖，
//! 不破坏 DESIGN §6.5 交叉编译线 (notify-webhook ⊃ http-client，既有 webhook
//! 组合无回归)。

/// F29/F34:签名头名。
pub const SIGNATURE_HEADER: &str = "X-Service-Watch-Signature";

/// F29/F34:待附签名头 (`sha256=<hex 小写>`);密钥未配置 → None，请求不带
/// 签名头。
pub fn signature_header(secret: Option<&str>, body: &[u8]) -> Option<(&'static str, String)> {
    secret.map(|secret| (SIGNATURE_HEADER, signature_hex(secret, body)))
}

/// F29/F34:HMAC-SHA256 → `sha256=<hex 小写>`(纯函数，便于单测对照 RFC 4231 向量)。
/// HMAC 接受任意长度密钥 (超块长先哈希再填充),`new_from_slice` 对 HMAC 不会失败。
pub fn signature_hex(secret: &str, body: &[u8]) -> String {
    // hmac 0.13 (digest 0.11):`new_from_slice` 随 `KeyInit` trait 提供，
    // 不再经 `Mac` supertrait 可达 —— 显式导入 (2026-11-18 依赖核对随 bump)。
    use hmac::{KeyInit, Mac};
    use std::fmt::Write as _;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts any key length");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest.iter() {
        let _ = write!(hex, "{byte:02x}");
    }
    format!("sha256={hex}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 4231 Test Case 2 已知向量 (key="Jefe",data="what do ya want for
    /// nothing?";hex 对照 RFC 4231 §4.3 原文，2026-11-18 核实)。
    #[test]
    fn hmac_sha256_matches_rfc4231_test_case_2() {
        assert_eq!(
            signature_hex("Jefe", b"what do ya want for nothing?"),
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    /// 密钥未配置 → 不产生签名头 (裸投递);配置后 → 头名与 `sha256=` 前缀
    /// (F29 webhook 与 F34 指标推送共享同一口径)。
    #[test]
    fn signature_header_absent_without_secret() {
        assert!(
            signature_header(None, b"body").is_none(),
            "None 密钥不加签名头"
        );
        let (name, value) =
            signature_header(Some("Jefe"), b"what do ya want for nothing?").unwrap();
        assert_eq!(name, "X-Service-Watch-Signature");
        assert_eq!(
            value,
            "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }
}
