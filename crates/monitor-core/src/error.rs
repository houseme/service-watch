//! 统一错误类型(DESIGN.md §5)。

use thiserror::Error;

pub type Result<T, E = CoreError> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("config error: {0}")]
    Config(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("collector error: {0}")]
    Collect(String),

    #[error("alert error: {0}")]
    Alert(String),

    #[error("recover error: {0}")]
    Recover(String),

    #[error("control error: {0}")]
    Control(String),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("http client error: {0}")]
    #[cfg(feature = "http-client")]
    Http(#[from] reqwest::Error),
}
