use std::sync::Arc;

use tracing::{info, warn};
use url::Url;

use crate::{
    errors::CrawlError,
    merge::merge_posts,
    models::PostData,
    plan_a::{crawl_many_plan_a, PlanAHttpCrawler},
    plan_b::crawl_plan_b_parallel,
};

pub struct CrawlOptions {
    pub max_in_flight: usize,
    pub webdriver_url: Option<String>,
    pub plan_b_pages: usize,
}

#[allow(dead_code)]
pub async fn crawl_all(urls: Vec<Url>, opt: CrawlOptions) -> Result<Vec<PostData>, CrawlError> {
    let crawler = Arc::new(PlanAHttpCrawler::new()?);

    info!(total = urls.len(), max_in_flight = opt.max_in_flight, "starting Plan A");
    let results = crawl_many_plan_a(crawler, urls, opt.max_in_flight).await;

    let mut ok = Vec::new();
    let mut fallback = Vec::new();
    let mut errors = 0usize;

    for r in results {
        match r {
            Ok(p) => ok.push(p),
            Err(e) if e.should_fallback_to_plan_b() => {
                warn!("Plan A fallback triggered: {e}");
                // We don't have URL inside CrawlError; URL is in task input.
                // So we re-run with Plan B later based on URL list kept by caller.
                // In this skeleton, Plan A returns error without URL. To keep it simple,
                // treat all failures as needing Plan B when enabled.
                fallback.push(e);
            }
            Err(e) => {
                warn!("Plan A failed (no fallback): {e}");
                errors += 1;
            }
        }
    }

    // NOTE: because results don't carry URLs on errors, we can only fall back if the caller supplies
    // the subset of URLs needing fallback. For correctness, we provide a helper in main that keeps the URL list.
    // This function assumes fallback URLs will be handled by main.

    info!(ok = ok.len(), errors, fallback = fallback.len(), "Plan A finished");

    Ok(merge_posts(ok))
}

/// A more practical crawl that keeps error URLs and performs Plan B if configured.
pub async fn crawl_all_with_fallback(
    urls: Vec<Url>,
    opt: CrawlOptions,
) -> Result<Vec<PostData>, CrawlError> {
    let crawler = Arc::new(PlanAHttpCrawler::new()?);

    info!(total = urls.len(), max_in_flight = opt.max_in_flight, "starting Plan A");

    // Keep the URL alongside the task to preserve which ones fail.
    // (We do it here, not in plan_a::crawl_many_plan_a, to keep plan_a module small.)
    let mut joinset = tokio::task::JoinSet::new();
    let sem = Arc::new(tokio::sync::Semaphore::new(opt.max_in_flight.max(1)));

    for url in urls {
        let crawler = crawler.clone();
        let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
        joinset.spawn(async move {
            let _permit = permit;
            let res = crawler.crawl_one(url.clone()).await;
            (url, res)
        });
    }

    let mut ok = Vec::new();
    let mut fallback_urls = Vec::new();
    let mut errors = 0usize;

    while let Some(res) = joinset.join_next().await {
        match res {
            Ok((_url, Ok(p))) => ok.push(p),
            Ok((url, Err(e))) if e.should_fallback_to_plan_b() => {
                warn!(%url, "Plan A fallback: {e}");
                fallback_urls.push(url);
            }
            Ok((url, Err(e))) => {
                warn!(%url, "Plan A failed: {e}");
                errors += 1;
            }
            Err(e) => {
                warn!("Join error: {e}");
                errors += 1;
            }
        }
    }

    info!(ok = ok.len(), errors, fallback = fallback_urls.len(), "Plan A finished");

    let mut all = ok;

    if !fallback_urls.is_empty() {
        if let (Some(webdriver_url), pages) = (opt.webdriver_url.as_deref(), opt.plan_b_pages) {
            if pages > 0 {
                info!(pages, "starting Plan B fallback");
                let b_results = crawl_plan_b_parallel(webdriver_url, fallback_urls, pages, std::sync::Arc::new(vec![])).await;
                for r in b_results {
                    match r {
                        Ok(p) => all.push(p),
                        Err(e) => warn!("Plan B failed: {e}"),
                    }
                }
            } else {
                warn!("Plan B was configured but --plan-b-pages=0, skipping fallback");
            }
        } else {
            warn!("Fallback URLs exist but no --webdriver was provided; skipping Plan B");
        }
    }

    Ok(merge_posts(all))
}
