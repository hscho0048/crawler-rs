use std::path::Path;

use tracing::{info, warn};
use url::Url;

use crate::{
    csv_out::ensure_out_dir,
    errors::CrawlError,
    plan_a::PlanAHttpCrawler,
    plan_b::webdriver_smoke_test,
};

pub struct TestOptions {
    pub url: Url,
    pub out_dir: String,
    pub webdriver_url: Option<String>,
}

pub async fn run_smoke_test(opt: TestOptions) -> Result<(), CrawlError> {
    let out_dir = Path::new(&opt.out_dir);
    ensure_out_dir(out_dir).map_err(|e| CrawlError::Parse(format!("failed to create out dir: {e}")))?;
    info!(out_dir = %out_dir.display(), "output directory ok");

    if let Some(wd) = opt.webdriver_url.as_deref() {
        info!(webdriver = wd, "checking webdriver connectivity");
        webdriver_smoke_test(wd).await?;
    }

    info!(url = %opt.url, "Plan A single-page fetch test");
    let crawler = PlanAHttpCrawler::new()?;
    match crawler.crawl_one(opt.url.clone()).await {
        Ok(post) => {
            info!(title = %post.title, written_at = %post.written_at, comments = post.comments.len(), "Plan A parse ok");
        }
        Err(e) if e.should_fallback_to_plan_b() => {
            warn!("Plan A indicates fallback likely needed: {e}");
        }
        Err(e) => {
            warn!("Plan A failed: {e}");
        }
    }

    Ok(())
}
