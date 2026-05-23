use anyhow::{anyhow, Context, Result};
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashSet, VecDeque};
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thirtyfour::prelude::*;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;

const WAIT_TIMEOUT_SECS: u64 = 12;

#[derive(Debug, Clone)]
pub struct PlanNConfig {
    pub search_url: String,
    pub max_scrolls: usize,
    pub max_posts: usize,
    pub workers: usize,
    pub webdriver_url: String,
    pub out_dir: String,
    pub headless: bool,
    pub comment_page_limit: usize,
}

impl Default for PlanNConfig {
    fn default() -> Self {
        Self {
            search_url: String::new(),
            max_scrolls: 30,
            max_posts: 0,
            workers: 3,
            webdriver_url: "http://localhost:4444".to_string(),
            out_dir: "out".to_string(),
            headless: false,
            comment_page_limit: 50,
        }
    }
}

#[derive(Debug, Clone)]
struct SearchCandidate {
    source_url: String,
    search_title: Option<String>,
}

#[derive(Debug, Clone)]
struct SearchApiRequest {
    url: Url,
    x_prs_query_info: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct PostOutputRow {
    source_url: String,
    final_url: String,
    source_type: String,
    title: String,
    date: String,
    body: String,
    comments: String,
    comment_count: usize,
    status: String,
    error: String,
}

#[derive(Debug, Clone, Serialize)]
struct CommentOutputRow {
    post_url: String,
    source_type: String,
    comment_index: usize,
    author: String,
    date: String,
    body: String,
}

#[derive(Debug, Clone, Default)]
struct DetailData {
    source_url: String,
    final_url: String,
    source_type: SourceType,
    title: String,
    date: String,
    body: String,
    comments: Vec<CommentData>,
    error: Option<String>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
enum SourceType {
    NaverBlog,
    Tistory,
    #[default]
    Other,
}

impl SourceType {
    fn as_str(&self) -> &'static str {
        match self {
            SourceType::NaverBlog => "naver_blog",
            SourceType::Tistory => "tistory",
            SourceType::Other => "other",
        }
    }
}

#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
struct CommentData {
    author: String,
    date: String,
    body: String,
}

pub async fn run(cfg: PlanNConfig) -> Result<()> {
    if cfg.search_url.trim().is_empty() {
        return Err(anyhow!("--url is required"));
    }

    tokio::fs::create_dir_all(&cfg.out_dir)
        .await
        .with_context(|| format!("output directory create failed: {}", cfg.out_dir))?;

    info!("naver search collector open");
    let list_driver = make_driver(&cfg.webdriver_url, cfg.headless).await?;
    list_driver.goto(&cfg.search_url).await?;
    wait_for_body(&list_driver).await?;
    sleep(Duration::from_millis(1200)).await;

    let candidates = collect_search_candidates(&list_driver, &cfg).await?;
    let _ = list_driver.quit().await;

    if candidates.is_empty() {
        warn!("no supported search urls collected");
        return Ok(());
    }

    info!("search urls collected: {}", candidates.len());
    let details = scrape_details_parallel(&cfg, candidates).await?;
    let (post_rows, comment_rows) = build_output_rows(details);

    let out_dir = Path::new(&cfg.out_dir);
    write_csv(&out_dir.join("naver_search_posts.csv"), &post_rows)?;
    write_csv(&out_dir.join("naver_search_comments.csv"), &comment_rows)?;

    info!(
        posts = post_rows.len(),
        comments = comment_rows.len(),
        "naver search csv saved"
    );
    Ok(())
}

async fn collect_search_candidates(driver: &WebDriver, cfg: &PlanNConfig) -> Result<Vec<SearchCandidate>> {
    let initial_html = driver.source().await?;
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for candidate in parse_search_candidates(&initial_html) {
        if seen.insert(candidate.source_url.clone()) {
            out.push(candidate);
            if cfg.max_posts > 0 && out.len() >= cfg.max_posts {
                return Ok(out);
            }
        }
    }

    if let Some(api_request) = extract_search_api_request(&initial_html, &cfg.search_url) {
        let before_api = out.len();
        match collect_search_candidates_from_api(&api_request, cfg, &mut seen, &mut out).await {
            Ok(()) if out.len() > before_api => {
                info!("search api collected {} additional urls", out.len() - before_api);
                return Ok(out);
            }
            Ok(()) => warn!("search api returned no additional supported urls; fallback to DOM scroll"),
            Err(e) => warn!("search api failed; fallback to DOM scroll: {e:#}"),
        }
    } else {
        warn!("search api endpoint not found; fallback to DOM scroll");
    }

    collect_search_candidates_from_dom_scroll(driver, cfg, seen, out).await
}

async fn collect_search_candidates_from_dom_scroll(
    driver: &WebDriver,
    cfg: &PlanNConfig,
    mut seen: HashSet<String>,
    mut out: Vec<SearchCandidate>,
) -> Result<Vec<SearchCandidate>> {
    let mut last_count = 0usize;
    let mut stable_rounds = 0usize;
    let mut last_height = 0i64;

    for round in 0..=cfg.max_scrolls {
        let html = driver.source().await?;
        for candidate in parse_search_candidates(&html) {
            if seen.insert(candidate.source_url.clone()) {
                out.push(candidate);
                if cfg.max_posts > 0 && out.len() >= cfg.max_posts {
                    return Ok(out);
                }
            }
        }

        if round == cfg.max_scrolls {
            break;
        }

        let height = script_i64(driver, "return document.body.scrollHeight || document.documentElement.scrollHeight || 0;").await;
        driver
            .execute("window.scrollTo(0, document.body.scrollHeight);", Vec::new())
            .await?;
        sleep(Duration::from_millis(1100)).await;

        if out.len() == last_count && height == last_height {
            stable_rounds += 1;
        } else {
            stable_rounds = 0;
            last_count = out.len();
            last_height = height;
        }

        if stable_rounds >= 3 {
            break;
        }
    }

    Ok(out)
}

async fn collect_search_candidates_from_api(
    api_request: &SearchApiRequest,
    cfg: &PlanNConfig,
    seen: &mut HashSet<String>,
    out: &mut Vec<SearchCandidate>,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        )
        .build()?;

    let base_page = query_usize(&api_request.url, "page").unwrap_or(2);
    let base_start = query_usize(&api_request.url, "start").unwrap_or(24);
    let base_prank = query_usize(&api_request.url, "prank").unwrap_or(base_start.saturating_sub(1));
    let page_size = 30usize;
    let mut stable_rounds = 0usize;

    for idx in 0..cfg.max_scrolls.max(1) {
        let mut url = api_request.url.clone();
        if idx > 0 {
            set_query_param(&mut url, "page", &(base_page + idx).to_string());
            set_query_param(&mut url, "start", &(base_start + idx * page_size).to_string());
            set_query_param(&mut url, "prank", &(base_prank + idx * page_size).to_string());
        }

        let mut req = client
            .get(url.clone())
            .header("accept", "application/json, text/javascript, */*; q=0.01")
            .header("referer", &cfg.search_url);
        if let Some(header) = &api_request.x_prs_query_info {
            req = req.header("X-Prs-Query-Info", header);
        }

        let text = req
            .send()
            .await
            .with_context(|| format!("search api request failed: {url}"))?
            .error_for_status()
            .with_context(|| format!("search api bad status: {url}"))?
            .text()
            .await?;

        let rows = parse_search_api_response_candidates(&text);
        let before = out.len();
        for candidate in rows {
            if seen.insert(candidate.source_url.clone()) {
                out.push(candidate);
                if cfg.max_posts > 0 && out.len() >= cfg.max_posts {
                    return Ok(());
                }
            }
        }

        if out.len() == before {
            stable_rounds += 1;
            if stable_rounds >= 2 {
                break;
            }
        } else {
            stable_rounds = 0;
        }
    }

    Ok(())
}

async fn scrape_details_parallel(cfg: &PlanNConfig, candidates: Vec<SearchCandidate>) -> Result<Vec<DetailData>> {
    let worker_count = cfg.workers.max(1).min(candidates.len().max(1));
    let queue: Arc<Mutex<VecDeque<SearchCandidate>>> =
        Arc::new(Mutex::new(VecDeque::from(candidates)));
    let mut joinset: JoinSet<Vec<DetailData>> = JoinSet::new();

    for worker_id in 0..worker_count {
        let cfg = cfg.clone();
        let queue = queue.clone();

        joinset.spawn(async move {
            sleep(Duration::from_millis(worker_id as u64 * 500)).await;
            let driver = match make_driver(&cfg.webdriver_url, cfg.headless).await {
                Ok(driver) => driver,
                Err(e) => {
                    warn!("worker {worker_id} driver create failed: {e:#}");
                    return Vec::new();
                }
            };

            let mut rows = Vec::new();
            loop {
                let candidate = {
                    let mut queue = queue.lock().await;
                    queue.pop_front()
                };
                let Some(candidate) = candidate else {
                    break;
                };

                match scrape_one_detail(&driver, &candidate, &cfg).await {
                    Ok(row) => {
                        info!("worker {worker_id} done: {}", row.final_url);
                        rows.push(row);
                    }
                    Err(e) => {
                        warn!("worker {worker_id} failed: {} | {e:#}", candidate.source_url);
                        rows.push(DetailData {
                            source_url: candidate.source_url.clone(),
                            final_url: candidate.source_url,
                            title: candidate.search_title.unwrap_or_default(),
                            error: Some(format!("{e:#}")),
                            ..Default::default()
                        });
                    }
                }
            }

            let _ = driver.quit().await;
            rows
        });
    }

    let mut out = Vec::new();
    while let Some(res) = joinset.join_next().await {
        match res {
            Ok(rows) => out.extend(rows),
            Err(e) => warn!("worker join failed: {e}"),
        }
    }
    Ok(out)
}

async fn scrape_one_detail(
    driver: &WebDriver,
    candidate: &SearchCandidate,
    cfg: &PlanNConfig,
) -> Result<DetailData> {
    driver.goto(&candidate.source_url).await?;
    wait_for_body(driver).await?;
    sleep(Duration::from_millis(900)).await;

    let final_url = driver.current_url().await?.to_string();
    let source_type = classify_url(&final_url).or_else(|| classify_url(&candidate.source_url));

    match source_type.unwrap_or_default() {
        SourceType::NaverBlog => scrape_naver_blog(driver, candidate, cfg).await,
        SourceType::Tistory => scrape_tistory(driver, candidate, cfg).await,
        SourceType::Other => scrape_generic(driver, candidate).await,
    }
}

async fn scrape_naver_blog(
    driver: &WebDriver,
    candidate: &SearchCandidate,
    cfg: &PlanNConfig,
) -> Result<DetailData> {
    let final_url = driver.current_url().await?.to_string();
    let _ = switch_mainframe_if_exists(driver).await;
    sleep(Duration::from_millis(500)).await;

    scroll_until_stable(driver, 250, 6).await?;
    let source = driver.source().await?;
    let mut detail = {
        let document = Html::parse_document(&source);
        parse_naver_detail(&document, candidate, &final_url)
    };

    let opened = open_naver_comments(driver).await.unwrap_or(false);
    if opened {
        detail.comments = collect_naver_comments(driver, cfg.comment_page_limit)
            .await
            .unwrap_or_default();
    }
    Ok(detail)
}

async fn scrape_tistory(
    driver: &WebDriver,
    candidate: &SearchCandidate,
    cfg: &PlanNConfig,
) -> Result<DetailData> {
    let final_url = driver.current_url().await?.to_string();
    scroll_until_stable(driver, 300, 5).await?;
    click_tistory_more_comments(driver, cfg.comment_page_limit).await?;
    let source = driver.source().await?;
    let document = Html::parse_document(&source);
    Ok(parse_tistory_detail(&document, candidate, &final_url))
}

async fn scrape_generic(driver: &WebDriver, candidate: &SearchCandidate) -> Result<DetailData> {
    let final_url = driver.current_url().await?.to_string();
    let source = driver.source().await?;
    let document = Html::parse_document(&source);
    let (title, date, body) = parse_generic_detail(&document, candidate.search_title.as_deref().unwrap_or_default());
    Ok(DetailData {
        source_url: candidate.source_url.clone(),
        final_url,
        source_type: SourceType::Other,
        title,
        date,
        body,
        comments: Vec::new(),
        error: None,
    })
}

fn parse_search_candidates(html: &str) -> Vec<SearchCandidate> {
    let document = Html::parse_document(html);
    let selectors = [
        "a._sp_each_url[href]",
        "a.title_link[href]",
        "a.total_tit[href]",
        "a.api_txt_lines[href]",
        "a[data-heatmap-target='.nblg'][href]",
        "a[href*='blog.naver.com/']",
        "a[href*='m.blog.naver.com/']",
        "a[href*='tistory.com/']",
    ];

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for css in selectors {
        for a in document.select(&sel(css)) {
            let Some(raw_href) = a.value().attr("href") else {
                continue;
            };
            let Some(url) = normalize_candidate_url(raw_href) else {
                continue;
            };
            if !is_supported_url(&url) || !seen.insert(url.clone()) {
                continue;
            }

            let title = text_of(Some(a));
            out.push(SearchCandidate {
                source_url: url,
                search_title: if title.is_empty() { None } else { Some(title) },
            });
        }
    }
    out
}

fn extract_search_api_request(html: &str, search_url: &str) -> Option<SearchApiRequest> {
    let url_re = Regex::new(r#"url:"([^"]*/p/review/50/search\.naver\?[^"]+)""#).ok()?;
    let raw_url = url_re
        .captures(html)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().replace("\\/", "/").replace("&amp;", "&"))?;

    let url = Url::parse(&raw_url)
        .or_else(|_| Url::parse(search_url).and_then(|base| base.join(&raw_url)))
        .ok()?;

    let header_re = Regex::new(r#""X-Prs-Query-Info":"([^"]+)""#).ok()?;
    let x_prs_query_info = header_re
        .captures(html)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string());

    Some(SearchApiRequest { url, x_prs_query_info })
}

fn parse_search_api_response_candidates(text: &str) -> Vec<SearchCandidate> {
    if let Ok(value) = serde_json::from_str::<Value>(text) {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        collect_candidates_from_json(&value, &mut seen, &mut out);
        return out;
    }

    parse_search_candidates(text)
}

fn collect_candidates_from_json(value: &Value, seen: &mut HashSet<String>, out: &mut Vec<SearchCandidate>) {
    match value {
        Value::String(s) => {
            for candidate in parse_search_candidates(s) {
                if seen.insert(candidate.source_url.clone()) {
                    out.push(candidate);
                }
            }
            if let Some(url) = normalize_candidate_url(s) {
                if is_supported_url(&url) && seen.insert(url.clone()) {
                    out.push(SearchCandidate { source_url: url, search_title: None });
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_candidates_from_json(item, seen, out);
            }
        }
        Value::Object(map) => {
            let title = map
                .get("title")
                .or_else(|| map.get("name"))
                .and_then(Value::as_str)
                .map(clean_text)
                .filter(|v| !v.is_empty());

            for key in ["url", "link", "href", "postUrl", "blogUrl"] {
                if let Some(raw_url) = map.get(key).and_then(Value::as_str) {
                    if let Some(url) = normalize_candidate_url(raw_url) {
                        if is_supported_url(&url) && seen.insert(url.clone()) {
                            out.push(SearchCandidate {
                                source_url: url,
                                search_title: title.clone(),
                            });
                        }
                    }
                }
            }

            for item in map.values() {
                collect_candidates_from_json(item, seen, out);
            }
        }
        _ => {}
    }
}

fn normalize_candidate_url(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() || value.starts_with("javascript:") || value.starts_with('#') {
        return None;
    }

    let parsed = Url::parse(value).ok()?;
    if is_supported_url(parsed.as_str()) {
        return Some(parsed.to_string());
    }

    for key in ["url", "u", "target"] {
        if let Some((_, encoded)) = parsed.query_pairs().find(|(k, _)| k == key) {
            let decoded = encoded.to_string();
            if Url::parse(&decoded).ok().is_some_and(|u| is_supported_url(u.as_str())) {
                return Some(decoded);
            }
        }
    }

    None
}

fn query_usize(url: &Url, key: &str) -> Option<usize> {
    url.query_pairs()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| v.parse().ok())
}

fn set_query_param(url: &mut Url, key: &str, value: &str) {
    let mut pairs = url
        .query_pairs()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<Vec<_>>();

    let mut replaced = false;
    for (k, v) in &mut pairs {
        if k == key {
            *v = value.to_string();
            replaced = true;
        }
    }
    if !replaced {
        pairs.push((key.to_string(), value.to_string()));
    }

    let mut query = url.query_pairs_mut();
    query.clear();
    for (k, v) in pairs {
        query.append_pair(&k, &v);
    }
}

fn is_supported_url(value: &str) -> bool {
    classify_url(value).is_some()
}

fn classify_url(value: &str) -> Option<SourceType> {
    let parsed = Url::parse(value).ok()?;
    let host = parsed.host_str()?.to_ascii_lowercase();
    if host == "blog.naver.com" || host == "m.blog.naver.com" {
        return Some(SourceType::NaverBlog);
    }
    if host == "tistory.com" || host.ends_with(".tistory.com") {
        return Some(SourceType::Tistory);
    }
    None
}

fn parse_naver_detail(document: &Html, candidate: &SearchCandidate, final_url: &str) -> DetailData {
    DetailData {
        source_url: candidate.source_url.clone(),
        final_url: final_url.to_string(),
        source_type: SourceType::NaverBlog,
        title: first_text(
            document,
            &[
                ".se-title-text",
                ".se-title-text span",
                ".pcol1.itemSubjectBoldfont",
                ".htitle .pcol1",
                "h3.se_textarea",
                "meta[property='og:title']",
                "title",
            ],
        )
        .or_else(|| candidate.search_title.clone())
        .unwrap_or_default(),
        date: first_text(
            document,
            &[
                ".se_publishDate",
                ".blog2_container .date",
                ".postdate",
                ".date",
                "meta[property='article:published_time']",
                "meta[name='date']",
            ],
        )
        .unwrap_or_default(),
        body: first_text(
            document,
            &[
                "div.se-main-container",
                "#postViewArea",
                ".post-view",
                ".post_ct",
                "#post-view",
                "article",
            ],
        )
        .unwrap_or_default(),
        comments: parse_naver_comments(document),
        error: None,
    }
}

fn parse_tistory_detail(document: &Html, candidate: &SearchCandidate, final_url: &str) -> DetailData {
    DetailData {
        source_url: candidate.source_url.clone(),
        final_url: final_url.to_string(),
        source_type: SourceType::Tistory,
        title: first_text(
            document,
            &["meta[property='og:title']", "h1", ".article_title", ".title_post", "title"],
        )
        .or_else(|| candidate.search_title.clone())
        .unwrap_or_default(),
        date: first_text(
            document,
            &[
                ".date",
                ".tt_date",
                "time",
                "meta[property='article:published_time']",
                "meta[name='date']",
            ],
        )
        .unwrap_or_default(),
        body: first_text(
            document,
            &[
                "#article-view",
                ".entry-content",
                ".tt_article_useless_p_margin",
                ".contents_style",
                ".article_view",
                ".post-content",
                "article",
            ],
        )
        .unwrap_or_default(),
        comments: parse_tistory_comments(document),
        error: None,
    }
}

fn parse_generic_detail(document: &Html, fallback_title: &str) -> (String, String, String) {
    let title = first_text(document, &["meta[property='og:title']", "h1", "title"])
        .unwrap_or_else(|| fallback_title.to_string());
    let date = first_text(document, &["meta[property='article:published_time']", "time", ".date"])
        .unwrap_or_default();
    let body = first_text(document, &["article", "main", "body"]).unwrap_or_default();
    (title, date, body)
}

fn parse_tistory_comments(document: &Html) -> Vec<CommentData> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for li in document.select(&sel(".tt-list-reply li.tt-item-reply, li.tt-item-reply")) {
        let comment = CommentData {
            author: first_text_from(&li, &[".tt-link-user", ".name", ".author"]).unwrap_or_default(),
            date: first_text_from(&li, &[".tt_date", "time", ".date"]).unwrap_or_default(),
            body: first_text_from(&li, &[".tt_desc", ".desc", ".comment-content"]).unwrap_or_default(),
        };
        if !comment.body.is_empty() && seen.insert(comment.clone()) {
            out.push(comment);
        }
    }

    out
}

fn parse_naver_comments(document: &Html) -> Vec<CommentData> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for li in document.select(&sel("li.u_cbox_comment")) {
        let comment = CommentData {
            author: first_text_from(&li, &[".u_cbox_nick", ".u_cbox_name"]).unwrap_or_default(),
            date: first_text_from(&li, &[".u_cbox_date"]).unwrap_or_default(),
            body: first_text_from(
                &li,
                &[
                    ".u_cbox_text_wrap .u_cbox_contents",
                    ".u_cbox_contents",
                    ".u_cbox_secret_contents",
                    ".u_cbox_delete_contents",
                ],
            )
            .unwrap_or_default(),
        };
        if !comment.body.is_empty() && seen.insert(comment.clone()) {
            out.push(comment);
        }
    }

    out
}

fn build_output_rows(details: Vec<DetailData>) -> (Vec<PostOutputRow>, Vec<CommentOutputRow>) {
    let mut posts = Vec::new();
    let mut comments = Vec::new();

    for detail in details {
        let source_type = detail.source_type.as_str().to_string();
        let joined_comments = detail
            .comments
            .iter()
            .map(|comment| comment.body.as_str())
            .collect::<Vec<_>>()
            .join(" | ");

        for (idx, comment) in detail.comments.iter().enumerate() {
            comments.push(CommentOutputRow {
                post_url: detail.final_url.clone(),
                source_type: source_type.clone(),
                comment_index: idx + 1,
                author: comment.author.clone(),
                date: comment.date.clone(),
                body: comment.body.clone(),
            });
        }

        posts.push(PostOutputRow {
            source_url: detail.source_url,
            final_url: detail.final_url,
            source_type,
            title: detail.title,
            date: detail.date,
            body: detail.body,
            comments: joined_comments,
            comment_count: detail.comments.len(),
            status: if detail.error.is_some() { "error" } else { "ok" }.to_string(),
            error: detail.error.unwrap_or_default(),
        });
    }

    (posts, comments)
}

async fn make_driver(webdriver_url: &str, headless: bool) -> Result<WebDriver> {
    let mut caps = DesiredCapabilities::chrome();
    if headless {
        caps.add_arg("--headless=new")?;
    }
    caps.add_arg("--start-maximized")?;
    caps.add_arg("--disable-blink-features=AutomationControlled")?;
    caps.add_arg("--disable-infobars")?;
    caps.add_arg("--disable-dev-shm-usage")?;
    caps.add_arg("--no-sandbox")?;
    caps.add_arg("--lang=ko-KR")?;

    let driver = WebDriver::new(webdriver_url, caps).await?;
    let _ = driver.set_page_load_timeout(Duration::from_secs(35)).await;
    let _ = driver.set_implicit_wait_timeout(Duration::from_millis(0)).await;
    Ok(driver)
}

async fn wait_for_body(driver: &WebDriver) -> Result<()> {
    driver
        .query(By::Css("body"))
        .wait(Duration::from_secs(20), Duration::from_millis(250))
        .first()
        .await
        .context("body load failed")?;
    Ok(())
}

async fn switch_mainframe_if_exists(driver: &WebDriver) -> Result<bool> {
    let _ = driver.enter_default_frame().await;
    if let Ok(frame) = driver.find(By::Css("iframe#mainFrame")).await {
        frame.enter_frame().await?;
        return Ok(true);
    }
    Ok(false)
}

async fn open_naver_comments(driver: &WebDriver) -> Result<bool> {
    let clicked = script_bool(
        driver,
        r#"
        return (function() {
            const nodes = Array.from(document.querySelectorAll(
                "span.btn_arr, a.btn_comment._cmtList, a[href*='Comment'], button, a"
            ));
            for (const node of nodes) {
                const text = (node.innerText || node.textContent || "").trim();
                const html = node.innerHTML || "";
                const cls = node.className || "";
                const isTarget =
                    cls.toString().includes("btn_arr") ||
                    text.includes("\uB313\uAE00") ||
                    html.includes("\uC774 \uAE00\uC5D0 \uB313\uAE00 \uB2E8 \uBE14\uB85C\uAC70 \uC5F4\uACE0 \uB2EB\uAE30");
                if (!isTarget) continue;
                const target = node.closest("a,button") || node;
                target.scrollIntoView({block: "center"});
                target.click();
                return true;
            }
            return false;
        })();
        "#,
    )
    .await;

    if !clicked {
        return Ok(false);
    }

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(WAIT_TIMEOUT_SECS) {
        let html = driver.source().await.unwrap_or_default();
        if html.contains("u_cbox") || html.contains("naverComment") {
            sleep(Duration::from_millis(500)).await;
            return Ok(true);
        }
        sleep(Duration::from_millis(250)).await;
    }
    Ok(false)
}

async fn collect_naver_comments(driver: &WebDriver, page_limit: usize) -> Result<Vec<CommentData>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let limit = page_limit.max(1);

    for page_index in 0..limit {
        sleep(Duration::from_millis(350)).await;
        let comments = {
            let source = driver.source().await.unwrap_or_default();
            let document = Html::parse_document(&source);
            parse_naver_comments(&document)
        };
        for comment in comments {
            if seen.insert(comment.clone()) {
                out.push(comment);
            }
        }

        if page_index + 1 >= limit {
            break;
        }
        if !click_next_naver_comment_page(driver).await? {
            break;
        }
    }

    Ok(out)
}

async fn click_next_naver_comment_page(driver: &WebDriver) -> Result<bool> {
    let before = first_comment_signature(driver).await;
    let clicked = script_bool(
        driver,
        r##"
        return (function() {
            const active = document.querySelector(
                ".u_cbox_paginate strong.u_cbox_page[data-param], .u_cbox_paginate .u_cbox_page_on[data-param]"
            );
            const current = active ? parseInt(active.getAttribute("data-param") || active.textContent || "0", 10) : 0;
            const candidates = Array.from(document.querySelectorAll(
                ".u_cbox_paginate a.u_cbox_page[data-param], .u_cbox_next:not(.u_cbox_next_end), .commentbox_pagination ._naverCommentNext"
            ));
            for (const node of candidates) {
                const text = (node.innerText || node.textContent || "").trim();
                const cls = node.className || "";
                const page = parseInt(node.getAttribute("data-param") || "0", 10);
                const disabled =
                    cls.toString().includes("dimmed") ||
                    node.getAttribute("aria-disabled") === "true" ||
                    node.disabled;
                if (disabled) continue;
                if (page && current && page <= current) continue;
                if (node.tagName === "A" && node.href && node.href.includes("#") && !text) continue;
                node.scrollIntoView({block: "center"});
                node.click();
                return true;
            }
            return false;
        })();
        "##,
    )
    .await;

    if !clicked {
        return Ok(false);
    }

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(WAIT_TIMEOUT_SECS) {
        let now = first_comment_signature(driver).await;
        if !now.is_empty() && now != before {
            return Ok(true);
        }
        sleep(Duration::from_millis(250)).await;
    }

    Ok(false)
}

async fn first_comment_signature(driver: &WebDriver) -> String {
    if let Ok(first) = driver.find(By::Css("li.u_cbox_comment")).await {
        if let Ok(Some(data_info)) = first.attr("data-info").await {
            return data_info;
        }
        if let Ok(text) = first.text().await {
            return text.chars().take(160).collect();
        }
    }
    String::new()
}

async fn click_tistory_more_comments(driver: &WebDriver, limit: usize) -> Result<()> {
    let max_clicks = limit.max(1);
    for _ in 0..max_clicks {
        let before_count = count_elements(driver, "li.tt-item-reply").await;
        let Some(button) = first_visible_button(driver, "button.tt_btn_prev_more").await else {
            break;
        };
        let _ = button.scroll_into_view().await;
        sleep(Duration::from_millis(150)).await;
        if button.click().await.is_err() {
            break;
        }

        let start = Instant::now();
        loop {
            sleep(Duration::from_millis(300)).await;
            let after_count = count_elements(driver, "li.tt-item-reply").await;
            if after_count > before_count || start.elapsed() > Duration::from_secs(4) {
                break;
            }
        }
    }
    Ok(())
}

async fn first_visible_button(driver: &WebDriver, selector: &str) -> Option<WebElement> {
    let buttons = driver.find_all(By::Css(selector)).await.ok()?;
    for button in buttons {
        let displayed = button.is_displayed().await.unwrap_or(false);
        let enabled = button.is_enabled().await.unwrap_or(false);
        if displayed && enabled {
            return Some(button);
        }
    }
    None
}

async fn count_elements(driver: &WebDriver, selector: &str) -> usize {
    driver
        .find_all(By::Css(selector))
        .await
        .map(|items| items.len())
        .unwrap_or(0)
}

async fn scroll_until_stable(driver: &WebDriver, pause_ms: u64, max_scrolls: usize) -> Result<()> {
    let mut last_height = 0i64;
    let mut stable_rounds = 0usize;

    for _ in 0..max_scrolls {
        driver
            .execute("window.scrollTo(0, document.body.scrollHeight);", Vec::new())
            .await?;
        sleep(Duration::from_millis(pause_ms)).await;
        let height = script_i64(driver, "return document.body.scrollHeight || document.documentElement.scrollHeight || 0;").await;
        if height == last_height {
            stable_rounds += 1;
            if stable_rounds >= 2 {
                break;
            }
        } else {
            stable_rounds = 0;
            last_height = height;
        }
    }

    Ok(())
}

async fn script_bool(driver: &WebDriver, script: &str) -> bool {
    driver
        .execute(script, Vec::new())
        .await
        .ok()
        .and_then(|ret| ret.json().as_bool())
        .unwrap_or(false)
}

async fn script_i64(driver: &WebDriver, script: &str) -> i64 {
    driver
        .execute(script, Vec::new())
        .await
        .ok()
        .and_then(|ret| ret.json().as_i64())
        .unwrap_or(0)
}

fn first_text(document: &Html, selectors: &[&str]) -> Option<String> {
    for css in selectors {
        let selector = sel(css);
        if let Some(node) = document.select(&selector).next() {
            let value = text_or_content(node);
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

fn first_text_from(root: &ElementRef<'_>, selectors: &[&str]) -> Option<String> {
    for css in selectors {
        let selector = sel(css);
        if let Some(node) = root.select(&selector).next() {
            let value = text_or_content(node);
            if !value.is_empty() {
                return Some(value);
            }
        }
    }
    None
}

fn text_or_content(node: ElementRef<'_>) -> String {
    if node.value().name() == "meta" {
        return node
            .value()
            .attr("content")
            .map(clean_text)
            .unwrap_or_default();
    }
    text_of(Some(node))
}

fn text_of(node: Option<ElementRef<'_>>) -> String {
    node.map(|n| clean_text(&n.text().collect::<Vec<_>>().join(" ")))
        .unwrap_or_default()
}

fn clean_text(text: &str) -> String {
    let mut value = text
        .replace('\u{200b}', " ")
        .replace('\u{00a0}', " ")
        .replace('\r', "\n");
    let ws_re = Regex::new(r"[ \t]+").unwrap();
    let nl_re = Regex::new(r"\n{3,}").unwrap();
    value = ws_re.replace_all(&value, " ").to_string();
    value = nl_re.replace_all(&value, "\n\n").to_string();
    value.trim().to_string()
}

fn sel(selector: &str) -> Selector {
    Selector::parse(selector).unwrap()
}

fn write_csv<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    let file = std::fs::File::create(path)?;
    let mut buf = std::io::BufWriter::new(file);
    buf.write_all(b"\xef\xbb\xbf")?;
    let mut wtr = csv::WriterBuilder::new().has_headers(true).from_writer(buf);
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_search_result_urls() {
        let html = r#"
            <a class="_sp_each_url" href="https://blog.naver.com/abc/123">Naver title</a>
            <a class="title_link" href="https://sample.tistory.com/15">Tistory title</a>
            <a class="title_link" href="https://example.com/not-supported">Other</a>
        "#;

        let rows = parse_search_candidates(html);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].source_url, "https://blog.naver.com/abc/123");
        assert_eq!(rows[0].search_title.as_deref(), Some("Naver title"));
        assert_eq!(rows[1].source_url, "https://sample.tistory.com/15");
    }

    #[test]
    fn extracts_search_api_request_from_page_html() {
        let html = r#"
            <script>
              new XLoad({url:"https://s.search.naver.com/p/review/50/search.naver?page=2&start=24&prank=23",headers:{"X-Prs-Query-Info":"token-value"}})
            </script>
        "#;

        let request = extract_search_api_request(html, "https://search.naver.com/search.naver?query=test")
            .expect("api request");

        assert_eq!(request.url.as_str(), "https://s.search.naver.com/p/review/50/search.naver?page=2&start=24&prank=23");
        assert_eq!(request.x_prs_query_info.as_deref(), Some("token-value"));
    }

    #[test]
    fn parses_search_api_json_with_html_payload() {
        let json = serde_json::json!({
            "html": "<a class=\"_sp_each_url\" href=\"https://blog.naver.com/abc/123\">JSON title</a>",
            "items": [{"url": "https://sample.tistory.com/15", "title": "Tistory"}]
        })
        .to_string();

        let rows = parse_search_api_response_candidates(&json);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].source_url, "https://blog.naver.com/abc/123");
        assert_eq!(rows[1].source_url, "https://sample.tistory.com/15");
        assert_eq!(rows[1].search_title.as_deref(), Some("Tistory"));
    }

    #[test]
    fn parses_tistory_detail_and_comments() {
        let html = r#"
            <html>
              <head><meta property="og:title" content="Tistory post"></head>
              <body>
                <span class="date">2024. 2. 26. 07:52</span>
                <div class="entry-content" id="article-view">Body text</div>
                <ul class="tt-list-reply">
                  <li class="tt-item-reply">
                    <a class="tt-link-user">Author A</a>
                    <p class="tt_desc">First comment</p>
                    <span class="tt_date">2024. 2. 26. 15:42</span>
                  </li>
                  <li class="tt-item-reply">
                    <a class="tt-link-user">Author B</a>
                    <p class="tt_desc">Second comment</p>
                    <span class="tt_date">2024. 2. 26. 16:29</span>
                  </li>
                </ul>
              </body>
            </html>
        "#;
        let document = Html::parse_document(html);
        let candidate = SearchCandidate {
            source_url: "https://sample.tistory.com/15".to_string(),
            search_title: None,
        };

        let detail = parse_tistory_detail(&document, &candidate, &candidate.source_url);

        assert_eq!(detail.title, "Tistory post");
        assert_eq!(detail.date, "2024. 2. 26. 07:52");
        assert_eq!(detail.body, "Body text");
        assert_eq!(detail.comments.len(), 2);
        assert_eq!(detail.comments[0].body, "First comment");
    }

    #[test]
    fn parses_naver_comment_items() {
        let html = r#"
            <div id="naverComment_1" class="u_cbox">
              <ul class="u_cbox_list">
                <li class="u_cbox_comment" data-info="commentNo:'1'">
                  <div class="u_cbox_comment_box">
                    <span class="u_cbox_nick">Nick</span>
                    <span class="u_cbox_contents">Hello</span>
                    <span class="u_cbox_date">2026.05.22</span>
                  </div>
                </li>
              </ul>
            </div>
        "#;
        let document = Html::parse_document(html);
        let comments = parse_naver_comments(&document);

        assert_eq!(comments.len(), 1);
        assert_eq!(comments[0].author, "Nick");
        assert_eq!(comments[0].body, "Hello");
    }

    #[test]
    fn joins_comments_with_pipe_for_post_csv() {
        let detail = DetailData {
            source_url: "https://blog.naver.com/a/1".to_string(),
            final_url: "https://blog.naver.com/a/1".to_string(),
            source_type: SourceType::NaverBlog,
            comments: vec![
                CommentData { body: "one".to_string(), ..Default::default() },
                CommentData { body: "two".to_string(), ..Default::default() },
            ],
            ..Default::default()
        };

        let (posts, comments) = build_output_rows(vec![detail]);

        assert_eq!(posts[0].comments, "one | two");
        assert_eq!(comments.len(), 2);
    }
}
