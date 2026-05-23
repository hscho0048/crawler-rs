use std::collections::{HashSet, VecDeque};
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use csv::WriterBuilder;
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thirtyfour::prelude::*;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::plan_e::build_driver;

const REVIEW_API_URL: &str = "https://www.coupang.com/vp/product/reviews";
const REVIEW_PAGE_SIZE: usize = 5;

#[derive(Debug, Clone)]
pub struct PlanOConfig {
    pub product_urls: Vec<String>,
    pub start_page: usize,
    pub max_pages: usize,
    pub workers: usize,
    pub output: String,
    pub cookie: Option<String>,
    pub cookie_file: Option<String>,
    pub page_delay_ms: u64,
    pub browser_fetch: bool,
    pub webdriver_url: String,
    pub headless: bool,
}

impl Default for PlanOConfig {
    fn default() -> Self {
        Self {
            product_urls: Vec::new(),
            start_page: 1,
            max_pages: 10,
            workers: 3,
            output: "out/coupang_reviews.csv".into(),
            cookie: None,
            cookie_file: None,
            page_delay_ms: 1000,
            browser_fetch: false,
            webdriver_url: "http://localhost:4444".into(),
            headless: false,
        }
    }
}

#[derive(Debug, Clone)]
struct PageTask {
    product_url: String,
    product_id: String,
    page: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CoupangReviewRow {
    pub product_url: String,
    pub product_id: String,
    pub page: usize,
    pub idx_in_page: usize,
    pub product_title: String,
    pub product_option: String,
    pub author: String,
    pub rating: String,
    pub date: String,
    pub helpful_count: String,
    pub headline: String,
    pub review_body: String,
    pub survey_answer: String,
    pub raw_text: String,
}

pub async fn run(cfg: PlanOConfig) -> Result<()> {
    if cfg.product_urls.is_empty() {
        return Err(anyhow!("수집할 쿠팡 상품 URL이 없습니다. --url 또는 --input을 지정하세요."));
    }
    if cfg.max_pages == 0 {
        return Err(anyhow!("--max-pages는 1 이상이어야 합니다."));
    }

    if let Some(parent) = Path::new(&cfg.output).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("output directory create failed: {}", parent.display()))?;
        }
    }

    let tasks = build_page_tasks(&cfg.product_urls, cfg.start_page, cfg.max_pages)?;
    let cookie_header = load_cookie_header(cfg.cookie.as_deref(), cfg.cookie_file.as_deref())?;
    info!(
        products = cfg.product_urls.len(),
        pages = tasks.len(),
        workers = cfg.workers,
        "coupang review api crawl start"
    );

    let rows = if cfg.browser_fetch {
        scrape_pages_parallel_browser(&cfg, tasks, cookie_header).await?
    } else {
        scrape_pages_parallel(&cfg, tasks, cookie_header).await?
    };
    write_reviews_csv(Path::new(&cfg.output), &rows)?;
    info!(rows = rows.len(), output = %cfg.output, "coupang review csv saved");
    Ok(())
}

fn build_page_tasks(product_urls: &[String], start_page: usize, max_pages: usize) -> Result<Vec<PageTask>> {
    let start_page = start_page.max(1);
    let mut tasks = Vec::new();
    for url in product_urls {
        let product_id = product_id_from_url(url)?;
        for offset in 0..max_pages {
            tasks.push(PageTask {
                product_url: url.clone(),
                product_id: product_id.clone(),
                page: start_page + offset,
            });
        }
    }
    Ok(tasks)
}

fn product_id_from_url(url: &str) -> Result<String> {
    let re = Regex::new(r"/products/(\d+)").unwrap();
    re.captures(url)
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        .ok_or_else(|| anyhow!("쿠팡 상품 URL에서 productId를 찾지 못했습니다: {url}"))
}

fn load_cookie_header(cookie: Option<&str>, cookie_file: Option<&str>) -> Result<Option<String>> {
    if let Some(cookie) = cookie.and_then(normalize_cookie_header) {
        return Ok(Some(cookie));
    }

    let Some(cookie_file) = cookie_file else {
        return Ok(None);
    };
    let text = fs::read_to_string(cookie_file)
        .with_context(|| format!("cookie file read failed: {cookie_file}"))?;
    Ok(normalize_cookie_header(&text))
}

fn normalize_cookie_header(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }

    if text.starts_with('[') {
        if let Ok(values) = serde_json::from_str::<Vec<Value>>(text) {
            let joined = values
                .into_iter()
                .filter_map(|value| {
                    let name = value.get("name")?.as_str()?.trim();
                    let cookie_value = value.get("value")?.as_str()?.trim();
                    if name.is_empty() {
                        return None;
                    }
                    Some(format!("{name}={cookie_value}"))
                })
                .collect::<Vec<_>>()
                .join("; ");
            return (!joined.is_empty()).then_some(joined);
        }
    }

    let text = text
        .strip_prefix("Cookie:")
        .or_else(|| text.strip_prefix("cookie:"))
        .unwrap_or(text)
        .trim();
    Some(
        text.lines()
            .map(|line| line.trim().trim_end_matches(';').trim())
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join("; "),
    )
}

fn cookie_pairs(cookie_header: &str) -> Vec<(String, String)> {
    cookie_header
        .split(';')
        .filter_map(|part| {
            let part = part.trim();
            let (name, value) = part.split_once('=')?;
            let name = name.trim();
            if name.is_empty() {
                return None;
            }
            Some((name.to_string(), value.trim().to_string()))
        })
        .collect()
}

async fn scrape_pages_parallel(
    cfg: &PlanOConfig,
    tasks: Vec<PageTask>,
    cookie_header: Option<String>,
) -> Result<Vec<CoupangReviewRow>> {
    let queue = Arc::new(Mutex::new(VecDeque::from(tasks)));
    let client = Arc::new(build_client()?);
    let cookie_header = Arc::new(cookie_header);
    let mut joinset = JoinSet::new();
    let workers = cfg.workers.max(1);
    let page_delay_ms = cfg.page_delay_ms;

    for worker_id in 0..workers {
        let queue = queue.clone();
        let client = client.clone();
        let cookie_header = cookie_header.clone();

        joinset.spawn(async move {
            let mut rows = Vec::new();
            loop {
                let task = queue.lock().await.pop_front();
                let Some(task) = task else { break };

                match fetch_review_page(&client, &task, cookie_header.as_deref()).await {
                    Ok(mut page_rows) => {
                        info!(
                            worker = worker_id,
                            page = task.page,
                            rows = page_rows.len(),
                            "coupang api page done"
                        );
                        rows.append(&mut page_rows);
                    }
                    Err(err) => {
                        warn!(
                            worker = worker_id,
                            page = task.page,
                            url = %task.product_url,
                            "coupang api page failed: {err:#}"
                        );
                    }
                }

                sleep(Duration::from_millis(page_delay_ms)).await;
            }
            rows
        });
    }

    let mut rows = Vec::new();
    while let Some(result) = joinset.join_next().await {
        match result {
            Ok(mut worker_rows) => rows.append(&mut worker_rows),
            Err(err) => warn!("coupang worker join failed: {err}"),
        }
    }

    rows.sort_by(|a, b| {
        a.product_url
            .cmp(&b.product_url)
            .then(a.page.cmp(&b.page))
            .then(a.idx_in_page.cmp(&b.idx_in_page))
    });
    Ok(dedupe_rows(rows))
}

async fn scrape_pages_parallel_browser(
    cfg: &PlanOConfig,
    tasks: Vec<PageTask>,
    cookie_header: Option<String>,
) -> Result<Vec<CoupangReviewRow>> {
    let queue = Arc::new(Mutex::new(VecDeque::from(tasks)));
    let cookie_header = Arc::new(cookie_header);
    let mut joinset = JoinSet::new();
    let workers = cfg.workers.max(1);
    let page_delay_ms = cfg.page_delay_ms;

    for worker_id in 0..workers {
        let queue = queue.clone();
        let cookie_header = cookie_header.clone();
        let wd = cfg.webdriver_url.clone();
        let headless = cfg.headless;

        joinset.spawn(async move {
            let profile_dir = coupang_profile_base().join(format!("browser_fetch_worker_{worker_id}"));
            let driver = match build_driver(&wd, headless, Some(&profile_dir)).await {
                Ok(driver) => driver,
                Err(err) => {
                    warn!("coupang browser worker {worker_id} webdriver open failed: {err:#}");
                    return Vec::new();
                }
            };

            if let Err(err) = prepare_coupang_browser(&driver, cookie_header.as_deref()).await {
                warn!("coupang browser worker {worker_id} prepare failed: {err:#}");
                let _ = driver.quit().await;
                return Vec::new();
            }

            let mut rows = Vec::new();
            loop {
                let task = queue.lock().await.pop_front();
                let Some(task) = task else { break };

                match fetch_review_page_with_browser(&driver, &task).await {
                    Ok(mut page_rows) => {
                        info!(
                            worker = worker_id,
                            page = task.page,
                            rows = page_rows.len(),
                            "coupang browser page done"
                        );
                        rows.append(&mut page_rows);
                    }
                    Err(err) => {
                        warn!(
                            worker = worker_id,
                            page = task.page,
                            url = %task.product_url,
                            "coupang browser page failed: {err:#}"
                        );
                    }
                }

                sleep(Duration::from_millis(page_delay_ms)).await;
            }

            let _ = driver.quit().await;
            rows
        });
    }

    let mut rows = Vec::new();
    while let Some(result) = joinset.join_next().await {
        match result {
            Ok(mut worker_rows) => rows.append(&mut worker_rows),
            Err(err) => warn!("coupang browser worker join failed: {err}"),
        }
    }

    rows.sort_by(|a, b| {
        a.product_url
            .cmp(&b.product_url)
            .then(a.page.cmp(&b.page))
            .then(a.idx_in_page.cmp(&b.idx_in_page))
    });
    Ok(dedupe_rows(rows))
}

fn coupang_profile_base() -> PathBuf {
    std::env::var_os("COUPANG_CHROME_PROFILE_BASE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join("target")
                .join("coupang_chrome_profiles")
        })
}

async fn prepare_coupang_browser(driver: &WebDriver, cookie_header: Option<&str>) -> Result<()> {
    driver.goto("https://www.coupang.com/").await?;
    sleep(Duration::from_millis(1200)).await;

    if let Some(cookie_header) = cookie_header {
        for (name, value) in cookie_pairs(cookie_header) {
            let mut cookie = thirtyfour::cookie::Cookie::new(&name, &value);
            cookie.set_domain(".coupang.com");
            cookie.set_path("/");
            let _ = driver.add_cookie(cookie).await;
        }
        driver.goto("https://www.coupang.com/").await?;
        sleep(Duration::from_millis(1200)).await;
    }

    Ok(())
}

async fn fetch_review_page_with_browser(driver: &WebDriver, task: &PageTask) -> Result<Vec<CoupangReviewRow>> {
    let value = driver
        .execute(
            r#"
            const [productId, page, size, referer] = arguments;
            const params = new URLSearchParams({
                productId,
                page: String(page),
                size: String(size),
                sortBy: 'ORDER_SCORE_ASC',
                ratings: '',
                q: '',
                viRoleCode: '2',
                ratingSummary: 'true'
            });
            const res = await fetch('/vp/product/reviews?' + params.toString(), {
                method: 'GET',
                credentials: 'include',
                headers: {
                    'accept': '*/*',
                    'accept-language': 'ko,en;q=0.9,en-US;q=0.8',
                    'x-coupang-accept-language': 'ko-KR'
                }
            });
            const text = await res.text();
            return JSON.stringify({ status: res.status, ok: res.ok, body: text });
            "#,
            vec![
                json!(task.product_id),
                json!(task.page),
                json!(REVIEW_PAGE_SIZE),
                json!(task.product_url),
            ],
        )
        .await?;

    let text = value.json().as_str().unwrap_or("{}");
    let parsed: Value = serde_json::from_str(text).unwrap_or_else(|_| json!({}));
    let status = parsed["status"].as_u64().unwrap_or(0);
    let html = parsed["body"].as_str().unwrap_or("").to_string();
    if status < 200 || status >= 300 {
        return Err(anyhow!(
            "browser review api status={status}, body={}",
            html.chars().take(160).collect::<String>()
        ));
    }
    if html.contains("Access Denied") || html.contains("errors.edgesuite.net") {
        return Err(anyhow!("browser review api access denied"));
    }

    Ok(parse_review_html(&html, task))
}

fn build_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
        .build()
        .context("coupang reqwest client build failed")
}

async fn fetch_review_page(
    client: &reqwest::Client,
    task: &PageTask,
    cookie_header: Option<&str>,
) -> Result<Vec<CoupangReviewRow>> {
    let params = [
        ("productId", task.product_id.as_str()),
        ("page", &task.page.to_string()),
        ("size", &REVIEW_PAGE_SIZE.to_string()),
        ("sortBy", "ORDER_SCORE_ASC"),
        ("ratings", ""),
        ("q", ""),
        ("viRoleCode", "2"),
        ("ratingSummary", "true"),
    ];

    let mut request = client
        .get(REVIEW_API_URL)
        .header("accept", "*/*")
        .header("accept-language", "ko,en;q=0.9,en-US;q=0.8")
        .header("priority", "u=1, i")
        .header("sec-ch-ua", r#""Chromium";v="131", "Not_A Brand";v="24""#)
        .header("sec-ch-ua-mobile", "?0")
        .header("sec-ch-ua-platform", r#""Windows""#)
        .header("sec-fetch-dest", "empty")
        .header("sec-fetch-mode", "cors")
        .header("sec-fetch-site", "same-origin")
        .header("x-coupang-accept-language", "ko-KR")
        .header("referer", &task.product_url)
        .query(&params);

    if let Some(cookie_header) = cookie_header.filter(|cookie| !cookie.trim().is_empty()) {
        request = request.header("cookie", cookie_header);
    }

    let response = request
        .send()
        .await
        .with_context(|| format!("review api request failed: page {}", task.page))?;

    let status = response.status();
    let html = response.text().await?;
    if !status.is_success() {
        return Err(anyhow!("review api status={status}, body={}", html.chars().take(160).collect::<String>()));
    }
    if html.contains("Access Denied") || html.contains("errors.edgesuite.net") {
        return Err(anyhow!("review api access denied"));
    }

    Ok(parse_review_html(&html, task))
}

fn parse_review_html(html: &str, task: &PageTask) -> Vec<CoupangReviewRow> {
    let document = Html::parse_fragment(html);
    let article_selector = selector("article.sdp-review__article__list");
    let title = text_first(&document, "h1.prod-buy-header__title").unwrap_or_default();

    document
        .select(&article_selector)
        .enumerate()
        .map(|(idx, article)| parse_review_article(article, task, idx + 1, &title))
        .collect()
}

fn parse_review_article(
    article: ElementRef<'_>,
    task: &PageTask,
    idx_in_page: usize,
    product_title: &str,
) -> CoupangReviewRow {
    let raw_text = normalize_text(&article.text().collect::<Vec<_>>().join(" "));
    CoupangReviewRow {
        product_url: task.product_url.clone(),
        product_id: task.product_id.clone(),
        page: task.page,
        idx_in_page,
        product_title: product_title.to_string(),
        product_option: text_in(&article, "div.sdp-review__article__list__info__product-info__name")
            .unwrap_or_default(),
        author: text_in(&article, "span.sdp-review__article__list__info__user__name").unwrap_or_default(),
        rating: rating_in(&article).unwrap_or_default(),
        date: text_in(&article, "div.sdp-review__article__list__info__product-info__reg-date")
            .or_else(|| first_date(&raw_text))
            .unwrap_or_default(),
        helpful_count: helpful_count(&raw_text).unwrap_or_default(),
        headline: text_in(&article, "div.sdp-review__article__list__headline").unwrap_or_default(),
        review_body: text_in(&article, "div.sdp-review__article__list__review > div").unwrap_or_default(),
        survey_answer: text_in(&article, "span.sdp-review__article__list__survey__row__answer")
            .unwrap_or_default(),
        raw_text,
    }
}

fn selector(css: &str) -> Selector {
    Selector::parse(css).unwrap()
}

fn text_first(document: &Html, css: &str) -> Option<String> {
    let selector = selector(css);
    document.select(&selector).next().map(|el| normalize_text(&el.text().collect::<Vec<_>>().join(" "))).filter(|s| !s.is_empty())
}

fn text_in(element: &ElementRef<'_>, css: &str) -> Option<String> {
    let selector = selector(css);
    element.select(&selector).next().map(|el| normalize_text(&el.text().collect::<Vec<_>>().join(" "))).filter(|s| !s.is_empty())
}

fn rating_in(element: &ElementRef<'_>) -> Option<String> {
    let selector = selector("div.sdp-review__article__list__info__product-info__star-orange");
    element
        .select(&selector)
        .next()
        .and_then(|el| el.value().attr("data-rating"))
        .map(str::to_string)
}

fn normalize_text(text: &str) -> String {
    Regex::new(r"\s+").unwrap().replace_all(text, " ").trim().to_string()
}

fn helpful_count(text: &str) -> Option<String> {
    let patterns = [
        r"(\d[\d,]*)\s*명에게\s*도움",
        r"(\d[\d,]*)\s*명이\s*도움",
        r"도움(?:이)?\s*(?:돼요|되었어요|되었습니다)?\s*[\(\[]?\s*(\d[\d,]*)",
    ];
    for pattern in patterns {
        let re = Regex::new(pattern).ok()?;
        if let Some(caps) = re.captures(text) {
            return caps
                .get(1)
                .map(|m| m.as_str().replace(',', "").trim().to_string());
        }
    }
    None
}

fn first_date(text: &str) -> Option<String> {
    let re = Regex::new(r"20\d{2}[.\-/]\s*\d{1,2}[.\-/]\s*\d{1,2}").ok()?;
    re.find(text).map(|m| normalize_text(m.as_str()))
}

fn dedupe_rows(rows: Vec<CoupangReviewRow>) -> Vec<CoupangReviewRow> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for row in rows {
        let key = format!("{}|{}|{}|{}", row.product_id, row.page, row.author, row.raw_text);
        if seen.insert(key) {
            out.push(row);
        }
    }
    out
}

fn write_reviews_csv(path: &Path, rows: &[CoupangReviewRow]) -> Result<()> {
    let mut file = File::create(path)
        .with_context(|| format!("csv create failed: {}", path.display()))?;
    file.write_all(b"\xef\xbb\xbf")?;
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(file);
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_product_id_from_product_url() {
        let url = "https://www.coupang.com/vp/products/1524451385?vendorItemId=70606707327";

        assert_eq!(product_id_from_url(url).unwrap(), "1524451385");
    }

    #[test]
    fn builds_page_task_per_product_page() {
        let urls = vec!["https://www.coupang.com/vp/products/1".to_string()];

        let tasks = build_page_tasks(&urls, 3, 4).unwrap();

        assert_eq!(tasks.len(), 4);
        assert_eq!(tasks[0].page, 3);
        assert_eq!(tasks[3].page, 6);
        assert_eq!(tasks[2].product_id, "1");
    }

    #[test]
    fn extracts_helpful_count_patterns() {
        assert_eq!(helpful_count("12명에게 도움이 되었습니다").as_deref(), Some("12"));
        assert_eq!(helpful_count("도움이 돼요 1,234").as_deref(), Some("1234"));
    }

    #[test]
    fn normalizes_cookie_header_text() {
        let cookie = normalize_cookie_header("Cookie: a=1;\nb=2").unwrap();

        assert_eq!(cookie, "a=1; b=2");
    }

    #[test]
    fn normalizes_json_cookie_array() {
        let cookie = normalize_cookie_header(
            r#"[{"name":"a","value":"1"},{"name":"b","value":"2"}]"#,
        )
        .unwrap();

        assert_eq!(cookie, "a=1; b=2");
    }

    #[test]
    fn parses_cookie_pairs() {
        let pairs = cookie_pairs("a=1; b=two=2; empty=");

        assert_eq!(pairs[0], ("a".to_string(), "1".to_string()));
        assert_eq!(pairs[1], ("b".to_string(), "two=2".to_string()));
        assert_eq!(pairs[2], ("empty".to_string(), "".to_string()));
    }

    #[test]
    fn parses_legacy_review_html() {
        let html = r#"
            <article class="sdp-review__article__list">
              <span class="sdp-review__article__list__info__user__name">홍길동</span>
              <div class="sdp-review__article__list__info__product-info__star-orange" data-rating="5"></div>
              <div class="sdp-review__article__list__info__product-info__reg-date">2026.05.23</div>
              <div class="sdp-review__article__list__info__product-info__name">옵션 A</div>
              <div class="sdp-review__article__list__headline">좋아요</div>
              <div class="sdp-review__article__list__review"><div>리뷰 본문입니다.</div></div>
              <span class="sdp-review__article__list__survey__row__answer">만족</span>
              <div>12명에게 도움이 되었습니다</div>
            </article>
        "#;
        let task = PageTask {
            product_url: "https://www.coupang.com/vp/products/1".to_string(),
            product_id: "1".to_string(),
            page: 2,
        };

        let rows = parse_review_html(html, &task);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].author, "홍길동");
        assert_eq!(rows[0].rating, "5");
        assert_eq!(rows[0].helpful_count, "12");
        assert_eq!(rows[0].review_body, "리뷰 본문입니다.");
    }
}
