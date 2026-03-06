use reqwest::StatusCode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CrawlError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("url parse error: {0}")]
    Url(#[from] url::ParseError),

    #[error("blocked or rate-limited: status={0}")]
    Blocked(StatusCode),

    #[error("requires javascript rendering or authenticated access (missing required fields)")]
    RequiresJsOrBlocked,

    #[error("parse error: {0}")]
    Parse(String),

    #[error("webdriver error: {0}")]
    WebDriver(String),
}

impl CrawlError {
    pub fn should_fallback_to_plan_b(&self) -> bool {
        matches!(self, CrawlError::Blocked(_) | CrawlError::RequiresJsOrBlocked)
    }
}
