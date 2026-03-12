/// Plan H — 네이버 블로그 검색 크롤러
///
/// 흐름:
///   1단계) 단일 드라이버로 검색 결과 스크롤 → SearchRow 목록 수집
///   2단계) Worker Pool — 각 워커가 드라이버를 1개씩 열고
///           본문 + 댓글을 병렬 수집
use std::collections::{HashSet, VecDeque};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use rand::Rng;
use regex::Regex;
use scraper::{ElementRef, Html, Selector};
use serde::Serialize;
use thirtyfour::prelude::*;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;
use urlencoding::encode;

const WAIT_TIMEOUT_SECS: u64 = 12;

// ─────────────────────────────────────────────────────────────────
// 퍼블릭 설정 구조체
// ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PlanHConfig {
    pub query: String,
    pub start_date: String,
    pub end_date: String,
    pub max_posts: usize,
    pub headless: bool,
    pub webdriver_url: String,
    pub output_dir: PathBuf,
    pub workers: usize,
    pub search_scroll_pause: f64,
    pub search_max_scrolls: usize,
    pub detail_scroll_pause: f64,
    pub detail_max_scrolls: usize,
}

impl Default for PlanHConfig {
    fn default() -> Self {
        Self {
            query: String::new(),
            start_date: String::new(),
            end_date: String::new(),
            max_posts: 30,
            headless: true,
            webdriver_url: "http://localhost:9515".into(),
            output_dir: PathBuf::from("output_naver_blog"),
            workers: 3,
            search_scroll_pause: 0.7,
            search_max_scrolls: 30,
            detail_scroll_pause: 0.3,
            detail_max_scrolls: 8,
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// 내부 데이터 구조
// ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
struct SearchRow {
    search_title: String,
    url: String,
    search_date: String,
    #[allow(dead_code)] search_preview: String,
    #[allow(dead_code)] blog_name: String,
    #[allow(dead_code)] blog_home: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct PostRow {
    #[serde(rename = "제목")]
    title: String,
    #[serde(rename = "url")]
    url: String,
    #[serde(rename = "날짜")]
    search_date: String,
    #[serde(rename = "본문")]
    body: String,
    #[serde(rename = "댓글")]
    comments_json: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct CommentRow {
    post_url: String,
    comment_id: String,
    parent_comment_id: String,
    reply_level: i64,
    author_name: String,
    author_url: String,
    content: String,
    created_at: String,
    display_date: String,
    like_count: i64,
    is_deleted: bool,
    is_secret: bool,
    is_blog_owner: bool,
    has_sticker: bool,
    sticker_url: String,
}

// ─────────────────────────────────────────────────────────────────
// 퍼블릭 진입점
// ─────────────────────────────────────────────────────────────────

pub async fn run(config: PlanHConfig) -> Result<()> {
    if config.query.trim().is_empty() {
        return Err(anyhow!("query가 비어 있습니다."));
    }
    if config.start_date.trim().is_empty() || config.end_date.trim().is_empty() {
        return Err(anyhow!("start_date / end_date 를 지정하세요."));
    }

    tokio::fs::create_dir_all(&config.output_dir).await?;

    // ── 1단계: 단일 드라이버로 검색 결과 수집 ─────────────────────
    info!("🔍 [1단계] 검색 결과 수집: \"{}\" ({} ~ {})",
        config.query, config.start_date, config.end_date);

    let search_url = build_search_url(&config.query, &config.start_date, &config.end_date);
    let list_driver = init_driver(&config).await?;
    list_driver.goto(&search_url).await?;
    safe_sleep(2.0, 0.5).await;

    let total_cards = scroll_search_results(
        &list_driver, config.search_scroll_pause, config.search_max_scrolls,
    ).await?;
    info!("  검색 카드 {}개 로딩 완료", total_cards);

    let html = list_driver.source().await?;
    let _ = list_driver.quit().await;

    let mut search_rows = parse_search_cards(&html);
    if config.max_posts > 0 && search_rows.len() > config.max_posts {
        search_rows.truncate(config.max_posts);
    }
    info!("  상세 수집 대상: {}개", search_rows.len());

    if search_rows.is_empty() {
        warn!("검색 결과가 없습니다.");
        return Ok(());
    }

    // ── 2단계: Worker Pool — 게시글 병렬 수집 ────────────────────
    let workers = config.workers.max(1);
    info!("⚡ [2단계] 게시글 {}개 병렬 수집 (워커: {})", search_rows.len(), workers);

    let total = search_rows.len();
    let queue: Arc<Mutex<VecDeque<SearchRow>>> =
        Arc::new(Mutex::new(VecDeque::from(search_rows)));
    let done = Arc::new(AtomicUsize::new(0));

    let mut joinset: JoinSet<(Vec<PostRow>, Vec<CommentRow>)> = JoinSet::new();

    for worker_id in 0..workers {
        let queue   = queue.clone();
        let done    = done.clone();
        let config  = config.clone();

        joinset.spawn(async move {
            let driver = match init_driver(&config).await {
                Ok(d) => d,
                Err(e) => {
                    warn!("워커 {worker_id} 드라이버 오류: {e}");
                    return (vec![], vec![]);
                }
            };
            info!("워커 {worker_id} 준비");

            let mut posts    = Vec::new();
            let mut comments = Vec::new();

            loop {
                let row = queue.lock().await.pop_front();
                let Some(row) = row else { break };

                let n = done.fetch_add(1, Ordering::Relaxed) + 1;

                match crawl_single_post(&driver, &row.url, &row, &config).await {
                    Ok((post_row, post_comments)) => {
                        info!("[{n}/{total}] 워커{worker_id} 완료: {}", post_row.title);
                        posts.push(post_row);
                        comments.extend(post_comments);
                    }
                    Err(e) => {
                        warn!("[{n}/{total}] 워커{worker_id} 실패 ({}): {e}", row.url);
                        posts.push(PostRow {
                            title:         row.search_title.clone(),
                            url:           row.url.clone(),
                            search_date:   row.search_date.clone(),
                            comments_json: "[]".into(),
                            ..Default::default()
                        });
                    }
                }
            }

            let _ = driver.quit().await;
            info!("워커 {worker_id} 종료");
            (posts, comments)
        });
    }

    let mut all_posts:    Vec<PostRow>    = Vec::new();
    let mut all_comments: Vec<CommentRow> = Vec::new();

    while let Some(res) = joinset.join_next().await {
        match res {
            Ok((posts, comments)) => {
                all_posts.extend(posts);
                all_comments.extend(comments);
            }
            Err(e) => warn!("join error: {e}"),
        }
    }

    info!("수집 완료: 게시글 {}개, 댓글 {}개", all_posts.len(), all_comments.len());

    // ── CSV / JSON 저장 ───────────────────────────────────────────
    let base_name = format!(
        "{}_{}_{}",
        slugify_filename(&config.query),
        normalize_date_str(&config.start_date),
        normalize_date_str(&config.end_date),
    );

    let posts_csv    = config.output_dir.join(format!("{}_posts.csv",    base_name));
    let comments_csv = config.output_dir.join(format!("{}_comments.csv", base_name));

    write_csv(&posts_csv,    &all_posts)?;
    write_csv(&comments_csv, &all_comments)?;

    info!("🎉 저장 완료 → {}", config.output_dir.display());
    Ok(())
}

// ─────────────────────────────────────────────────────────────────
// 헬퍼 — 공통 유틸
// ─────────────────────────────────────────────────────────────────

fn sel(s: &str) -> Selector {
    Selector::parse(s).unwrap()
}

fn clean_text(text: &str) -> String {
    let mut s = text
        .replace('\u{200b}', " ")
        .replace('\u{00a0}', " ")
        .replace('\r', "\n");
    let ws_re = Regex::new(r"[ \t]+").unwrap();
    let nl_re = Regex::new(r"\n{3,}").unwrap();
    s = ws_re.replace_all(&s, " ").to_string();
    s = nl_re.replace_all(&s, "\n\n").to_string();
    s.trim().to_string()
}

fn normalize_date_str(date_str: &str) -> String {
    date_str.replace('-', "").trim().to_string()
}

fn build_search_url(query: &str, start_date: &str, end_date: &str) -> String {
    let q     = encode(query);
    let start = normalize_date_str(start_date);
    let end   = normalize_date_str(end_date);
    let nso   = format!("so:r,p:from{}to{}", start, end);
    format!(
        "https://search.naver.com/search.naver?ssc=tab.blog.all&query={}&sm=tab_opt&nso={}",
        q, encode(&nso)
    )
}

fn slugify_filename(text: &str) -> String {
    let re  = Regex::new(r"[^0-9A-Za-z가-힣_\-]+").unwrap();
    let re2 = Regex::new(r"_+").unwrap();
    let s   = re.replace_all(text, "_").to_string();
    let s   = re2.replace_all(&s, "_").to_string();
    let s   = s.trim_matches('_').to_string();
    if s.is_empty() { "naver_blog".into() } else { s.chars().take(80).collect() }
}

fn extract_int(text: &str) -> i64 {
    let re     = Regex::new(r"\d+").unwrap();
    let joined = re.find_iter(&text.replace(',', ""))
        .map(|m| m.as_str())
        .collect::<Vec<_>>()
        .join("");
    joined.parse::<i64>().unwrap_or(0)
}

fn is_blog_post_url(url: &str) -> bool {
    if url.is_empty() { return false; }
    let parsed = match Url::parse(url) { Ok(v) => v, Err(_) => return false };
    let host = parsed.host_str().unwrap_or_default();
    if host != "blog.naver.com" && host != "m.blog.naver.com" { return false; }
    parsed.path_segments()
        .map(|it| it.filter(|s| !s.is_empty()).count())
        .unwrap_or(0) >= 2
}

fn text_of(node: Option<ElementRef<'_>>) -> String {
    node.map(|n| clean_text(&n.text().collect::<Vec<_>>().join(" ")))
        .unwrap_or_default()
}

fn attr_of(node: Option<ElementRef<'_>>, attr: &str) -> String {
    node.and_then(|n| n.value().attr(attr))
        .map(|v| v.trim().to_string())
        .unwrap_or_default()
}

fn has_class(el: &ElementRef<'_>, class_name: &str) -> bool {
    el.value()
        .attr("class")
        .map(|v| v.split_whitespace().any(|c| c == class_name))
        .unwrap_or(false)
}

async fn safe_sleep(base: f64, jitter: f64) {
    let millis = ((base + rand::thread_rng().gen_range(0.0..=jitter)) * 1000.0) as u64;
    sleep(Duration::from_millis(millis)).await;
}

// ─────────────────────────────────────────────────────────────────
// 헬퍼 — 드라이버
// ─────────────────────────────────────────────────────────────────

async fn init_driver(config: &PlanHConfig) -> Result<WebDriver> {
    let mut caps = DesiredCapabilities::chrome();
    if config.headless {
        caps.add_arg("--headless=new")?;
    }
    caps.add_arg("--start-maximized")?;
    caps.add_arg("--disable-blink-features=AutomationControlled")?;
    caps.add_arg("--disable-infobars")?;
    caps.add_arg("--disable-dev-shm-usage")?;
    caps.add_arg("--no-sandbox")?;
    caps.add_arg("--lang=ko-KR")?;

    let driver = WebDriver::new(&config.webdriver_url, caps).await?;
    let _ = driver.set_page_load_timeout(Duration::from_secs(30)).await;
    let _ = driver.set_implicit_wait_timeout(Duration::from_millis(0)).await; // 명시적 폴링 사용
    Ok(driver)
}

async fn safe_click(element: &WebElement) -> Result<()> {
    let _ = element.scroll_into_view().await;
    sleep(Duration::from_millis(150)).await;
    element.click().await?;
    Ok(())
}

async fn find_first(driver: &WebDriver, selector: &str, timeout_secs: u64) -> Option<WebElement> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(timeout_secs) {
        if let Ok(el) = driver.find(By::Css(selector)).await {
            return Some(el);
        }
        sleep(Duration::from_millis(150)).await;
    }
    None
}

async fn wait_for_any(driver: &WebDriver, selector: &str, timeout_secs: u64) -> bool {
    find_first(driver, selector, timeout_secs).await.is_some()
}

// ─────────────────────────────────────────────────────────────────
// 헬퍼 — 검색 단계
// ─────────────────────────────────────────────────────────────────

async fn scroll_search_results(driver: &WebDriver, pause: f64, max_scrolls: usize) -> Result<usize> {
    let mut last_count    = 0usize;
    let mut stable_rounds = 0usize;

    for _ in 0..max_scrolls {
        let current_count = driver
            .find_all(By::Css("div[data-template-id='ugcItem']"))
            .await.map(|v| v.len()).unwrap_or(0);

        let _ = driver.execute("window.scrollTo(0, document.body.scrollHeight);", Vec::new()).await;
        safe_sleep(pause, 0.2).await;

        if current_count == last_count {
            stable_rounds += 1;
        } else {
            stable_rounds = 0;
            last_count    = current_count;
        }
        if stable_rounds >= 2 { break; }
    }
    Ok(last_count)
}

/// 페이지 높이가 더 이상 늘어나지 않을 때까지 스크롤 (최대 max_scrolls회)
async fn scroll_until_stable(driver: &WebDriver, pause_ms: u64, max_scrolls: usize) -> Result<()> {
    let mut last_height: i64 = 0;
    let mut stable = 0usize;
    for _ in 0..max_scrolls {
        let _ = driver.execute(
            "window.scrollTo(0, document.body.scrollHeight);", Vec::new(),
        ).await;
        sleep(Duration::from_millis(pause_ms)).await;
        let height = driver.execute("return document.body.scrollHeight;", Vec::new())
            .await.ok().and_then(|v| v.json().as_i64()).unwrap_or(0);
        if height == last_height {
            stable += 1;
            if stable >= 2 { break; }
        } else {
            stable      = 0;
            last_height = height;
        }
    }
    Ok(())
}

fn parse_search_cards(html: &str) -> Vec<SearchRow> {
    let document      = Html::parse_document(html);
    let item_sel      = sel("div[data-template-id='ugcItem']");
    let title_sel     = sel("a[data-heatmap-target='.nblg']");
    let a_sel         = sel("a[href]");
    let preview_sel1  = sel("a.fds-ugc-ellipsis2 span");
    let preview_sel2  = sel(".fds-ugc-ellipsis2");
    let date_sel      = sel(".sds-comps-profile-info-subtext");
    let blog_name_sel = sel(".sds-comps-profile-info-title-text");
    let blog_url_sel  = sel("a[data-heatmap-target='articleSourceJSX_title']");

    let mut out  = Vec::new();
    let mut seen = HashSet::new();

    for item in document.select(&item_sel) {
        let mut title_node = item.select(&title_sel).next();

        if title_node.is_none() {
            for a in item.select(&a_sel) {
                let href = attr_of(Some(a), "href");
                let text = text_of(Some(a));
                if is_blog_post_url(&href) && !text.is_empty() {
                    title_node = Some(a);
                    break;
                }
            }
        }

        let Some(title_node) = title_node else { continue };
        let url   = attr_of(Some(title_node), "href");
        let title = text_of(Some(title_node));

        if !is_blog_post_url(&url) || seen.contains(&url) { continue; }

        let preview = item.select(&preview_sel1).next()
            .map(|n| text_of(Some(n))).filter(|v| !v.is_empty())
            .or_else(|| item.select(&preview_sel2).next().map(|n| text_of(Some(n))))
            .unwrap_or_default();

        out.push(SearchRow {
            search_title: title,
            url: url.clone(),
            search_date:    text_of(item.select(&date_sel).next()),
            search_preview: preview,
            blog_name:      text_of(item.select(&blog_name_sel).next()),
            blog_home:      attr_of(item.select(&blog_url_sel).next(), "href"),
        });
        seen.insert(url);
    }
    out
}

// ─────────────────────────────────────────────────────────────────
// 헬퍼 — 게시글 상세
// ─────────────────────────────────────────────────────────────────

async fn switch_mainframe_if_exists(driver: &WebDriver) -> Result<bool> {
    let _ = driver.enter_default_frame().await;
    if let Ok(frame) = driver.find(By::Css("iframe#mainFrame")).await {
        frame.enter_frame().await?;
        return Ok(true);
    }
    Ok(false)
}

async fn load_detail_page(driver: &WebDriver, url: &str, config: &PlanHConfig) -> Result<()> {
    driver.goto(url).await?;
    safe_sleep(0.8, 0.2).await;
    let switched = switch_mainframe_if_exists(driver).await?;
    if switched { safe_sleep(0.4, 0.1).await; }
    let pause_ms = (config.detail_scroll_pause * 1000.0) as u64;
    scroll_until_stable(driver, pause_ms, config.detail_max_scrolls).await?;
    Ok(())
}

fn extract_detail_title(document: &Html, fallback: &str) -> String {
    let selectors = [
        "meta[property='og:title']",
        ".se-title-text span",
        ".pcol1 .title",
        "title",
    ];
    for css in selectors {
        let selector = sel(css);
        if let Some(node) = document.select(&selector).next() {
            let value = if node.value().name() == "meta" {
                attr_of(Some(node), "content")
            } else {
                text_of(Some(node))
            };
            if !value.is_empty() { return value; }
        }
    }
    fallback.to_string()
}

fn extract_body_text(document: &Html) -> String {
    let container_sel = sel("div.se-main-container");
    let text_p_sel    = sel("p.se-text-paragraph");
    let og_title_sel  = sel(".se-oglink-title");
    let og_link_sel   = sel("a.se-oglink-info[href], a.se-oglink-thumbnail[href]");
    let caption_sel   = sel(".se-caption");

    let Some(container) = document.select(&container_sel).next() else {
        return String::new();
    };

    let mut lines = Vec::new();
    for child in container.children() {
        let Some(child) = ElementRef::wrap(child) else { continue };
        if !has_class(&child, "se-component") { continue; }

        if has_class(&child, "se-text") {
            for p in child.select(&text_p_sel) {
                let text = text_of(Some(p));
                if !text.is_empty() { lines.push(text); }
            }
        } else if has_class(&child, "se-oglink") {
            let title = text_of(child.select(&og_title_sel).next());
            let href  = attr_of(child.select(&og_link_sel).next(), "href");
            if !title.is_empty() { lines.push(format!("[링크카드] {}", title)); }
            if !href.is_empty()  { lines.push(format!("[링크] {}", href)); }
        } else if has_class(&child, "se-image") || has_class(&child, "se-imageGroup") {
            let caption = text_of(child.select(&caption_sel).next());
            if !caption.is_empty() { lines.push(format!("[이미지설명] {}", caption)); }
        }
    }
    lines.join("\n").trim().to_string()
}

fn extract_like_count(document: &Html) -> i64 {
    let selector = sel(".blog_like_area .u_likeit_text._count.num");
    document.select(&selector).next()
        .map(|n| extract_int(&text_of(Some(n)))).unwrap_or(0)
}

fn extract_comment_count(document: &Html) -> i64 {
    for css in ["#commentCount", "._commentCount"] {
        if let Some(node) = document.select(&sel(css)).next() {
            return extract_int(&text_of(Some(node)));
        }
    }
    0
}

// ─────────────────────────────────────────────────────────────────
// 헬퍼 — 댓글
// ─────────────────────────────────────────────────────────────────

async fn open_comment_panel(driver: &WebDriver) -> Result<bool> {
    let Some(btn) = find_first(driver, "a.btn_comment._cmtList", WAIT_TIMEOUT_SECS).await else {
        return Ok(false);
    };
    safe_click(&btn).await?;
    if !wait_for_any(driver, "div[id^='naverComment_'][id$='_ct']", WAIT_TIMEOUT_SECS).await {
        return Ok(false);
    }
    if !wait_for_any(
        driver,
        "div[id^='naverComment_'].u_cbox ul.u_cbox_list > li.u_cbox_comment",
        WAIT_TIMEOUT_SECS,
    ).await { return Ok(false); }
    safe_sleep(0.4, 0.1).await;
    Ok(true)
}

async fn ensure_comment_sort_oldest(driver: &WebDriver) -> Result<bool> {
    if let Ok(selected) = driver.find_all(By::Css(".u_cbox_sort_option_on .u_cbox_sort_label")).await {
        if let Some(first) = selected.first() {
            let text = clean_text(&first.text().await.unwrap_or_default());
            if text.contains("과거순") { return Ok(true); }
        }
    }
    if let Ok(candidates) = driver.find_all(By::Css(".u_cbox_sort_option_wrap a.u_cbox_select")).await {
        for btn in candidates {
            let label = clean_text(&btn.text().await.unwrap_or_default());
            if label.contains("과거순") {
                safe_click(&btn).await?;
                safe_sleep(0.4, 0.1).await;
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn get_comment_page_state(driver: &WebDriver) -> (i64, i64) {
    let mut current: Option<i64> = None;
    let mut last:    Option<i64> = None;

    if let Ok(el) = driver.find(By::Css(".commentbox_pagination ._currentPageNo")).await {
        current = el.text().await.ok().and_then(|t| t.trim().parse().ok());
    }
    if let Ok(el) = driver.find(By::Css(".commentbox_pagination ._lastPageNo")).await {
        last = el.text().await.ok().and_then(|t| t.trim().parse().ok());
    }
    if let Ok(el) = driver.find(By::Css(".u_cbox_paginate strong.u_cbox_page[data-param]")).await {
        current = el.attr("data-param").await.ok().flatten()
            .and_then(|v| v.parse().ok()).or(current);
    }
    if let Ok(pages) = driver.find_all(By::Css(".u_cbox_paginate .u_cbox_page[data-param]")).await {
        let mut nums = Vec::new();
        for el in pages {
            if let Ok(Some(v)) = el.attr("data-param").await {
                if let Ok(n) = v.parse::<i64>() { nums.push(n); }
            }
        }
        if let Some(max_v) = nums.into_iter().max() {
            last = Some(last.unwrap_or(max_v).max(max_v));
        }
    }
    (current.unwrap_or(1), last.unwrap_or(1))
}

async fn get_first_comment_signature(driver: &WebDriver) -> String {
    if let Ok(first) = driver.find(By::Css(
        "div[id^='naverComment_'].u_cbox ul.u_cbox_list > li.u_cbox_comment",
    )).await {
        if let Ok(Some(info)) = first.attr("data-info").await { return info; }
        if let Ok(text) = first.text().await { return text.chars().take(100).collect(); }
    }
    String::new()
}

async fn click_comment_page(driver: &WebDriver, target_page: i64) -> Result<bool> {
    let (current_page, last_page) = get_comment_page_state(driver).await;
    if target_page < 1 || target_page > last_page { return Ok(false); }
    if current_page == target_page { return Ok(true); }

    let before_signature = get_first_comment_signature(driver).await;

    let selectors = vec![
        format!(".u_cbox_paginate a.u_cbox_page[data-param='{}']",         target_page),
        format!(".u_cbox_paginate a.u_cbox_pre_end[data-param='{}']",      target_page),
        format!(".u_cbox_paginate a.u_cbox_next_end[data-param='{}']",     target_page),
    ];

    let mut target: Option<WebElement> = None;
    for selector in &selectors {
        if let Ok(btns) = driver.find_all(By::Css(selector)).await {
            if let Some(btn) = btns.into_iter().next() { target = Some(btn); break; }
        }
    }

    if target.is_none() && target_page == current_page + 1 {
        if let Ok(btns) = driver.find_all(By::Css(".commentbox_pagination ._naverCommentNext")).await {
            for btn in btns {
                let classes = btn.attr("class").await.ok().flatten().unwrap_or_default();
                if !classes.contains("dimmed") { target = Some(btn); break; }
            }
        }
    }
    if target.is_none() && target_page == current_page - 1 {
        if let Ok(btns) = driver.find_all(By::Css(".commentbox_pagination ._naverCommentPrev")).await {
            for btn in btns {
                let classes = btn.attr("class").await.ok().flatten().unwrap_or_default();
                if !classes.contains("dimmed") { target = Some(btn); break; }
            }
        }
    }

    let Some(target) = target else { return Ok(false); };
    safe_click(&target).await?;

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(WAIT_TIMEOUT_SECS) {
        let (now_page, _)     = get_comment_page_state(driver).await;
        let now_signature     = get_first_comment_signature(driver).await;
        if now_page == target_page || now_signature != before_signature { break; }
        sleep(Duration::from_millis(200)).await;
    }
    safe_sleep(0.5, 0.1).await;
    Ok(true)
}

async fn go_to_first_comment_page(driver: &WebDriver) -> Result<bool> {
    let (current_page, last_page) = get_comment_page_state(driver).await;
    if last_page <= 1 || current_page == 1 { return Ok(true); }
    click_comment_page(driver, 1).await
}

fn direct_child_by_class<'a>(tag: &ElementRef<'a>, tag_name: &str, class_name: &str) -> Option<ElementRef<'a>> {
    for child in tag.children() {
        let Some(child) = ElementRef::wrap(child) else { continue };
        if child.value().name() == tag_name
            && child.value().attr("class")
                .map(|v| v.split_whitespace().any(|c| c == class_name))
                .unwrap_or(false)
        {
            return Some(child);
        }
    }
    None
}

fn extract_comment_id(data_info: &str) -> String {
    Regex::new(r"commentNo:'([^']+)'").unwrap()
        .captures(data_info).and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string()).unwrap_or_default()
}

fn extract_parent_comment_id(data_info: &str) -> String {
    Regex::new(r"parentCommentNo:'([^']+)'").unwrap()
        .captures(data_info).and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string()).unwrap_or_default()
}

fn extract_reply_level(data_info: &str) -> i64 {
    Regex::new(r"replyLevel:(\d+)").unwrap()
        .captures(data_info).and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok()).unwrap_or(1)
}

fn parse_comment_box(comment_box: &ElementRef<'_>, li: &ElementRef<'_>, post_url: &str) -> CommentRow {
    let data_info = li.value().attr("data-info").unwrap_or_default();
    let is_deleted = has_class(comment_box, "u_cbox_type_delete");
    let is_secret  = has_class(comment_box, "u_cbox_type_secret");

    let content = if is_deleted {
        text_of(comment_box.select(&sel(".u_cbox_delete_contents")).next())
    } else if is_secret {
        text_of(comment_box.select(&sel(".u_cbox_secret_contents")).next())
    } else {
        text_of(comment_box.select(&sel(".u_cbox_text_wrap .u_cbox_contents")).next())
    };

    let date_node    = comment_box.select(&sel(".u_cbox_date")).next();
    let sticker_node = comment_box.select(&sel(".u_cbox_sticker_wrap img")).next();

    CommentRow {
        post_url: post_url.to_string(),
        comment_id:        extract_comment_id(data_info),
        parent_comment_id: extract_parent_comment_id(data_info),
        reply_level:       extract_reply_level(data_info),
        author_name:       text_of(comment_box.select(&sel(".u_cbox_nick")).next()),
        author_url:        attr_of(comment_box.select(&sel("a.u_cbox_name[href]")).next(), "href"),
        content,
        created_at:        attr_of(date_node, "data-value"),
        display_date:      text_of(date_node),
        like_count:        comment_box.select(&sel(".u_cbox_cnt_recomm")).next()
                               .map(|n| extract_int(&text_of(Some(n)))).unwrap_or(0),
        is_deleted,
        is_secret,
        is_blog_owner: comment_box.select(&sel(".u_cbox_ico_editor")).next().is_some(),
        has_sticker:   sticker_node.is_some(),
        sticker_url:   attr_of(sticker_node, "src"),
    }
}

fn parse_comment_tree(li: &ElementRef<'_>, post_url: &str) -> Vec<CommentRow> {
    let mut rows = Vec::new();
    if let Some(cb) = direct_child_by_class(li, "div", "u_cbox_comment_box") {
        rows.push(parse_comment_box(&cb, li, post_url));
    }
    if let Some(reply_area) = direct_child_by_class(li, "div", "u_cbox_reply_area") {
        if let Some(reply_ul) = reply_area.select(&sel("ul.u_cbox_list")).next() {
            for reply_li in reply_ul.select(&sel("li.u_cbox_comment")) {
                if let Some(rb) = direct_child_by_class(&reply_li, "div", "u_cbox_comment_box") {
                    rows.push(parse_comment_box(&rb, &reply_li, post_url));
                }
            }
        }
    }
    rows
}

fn parse_comment_page_source(page_source: &str, post_url: &str) -> Vec<CommentRow> {
    let document = Html::parse_document(page_source);
    let Some(root)   = document.select(&sel("div[id^='naverComment_'].u_cbox")).next() else { return vec![]; };
    let Some(top_ul) = root.select(&sel("ul.u_cbox_list")).next() else { return vec![]; };
    let mut rows = Vec::new();
    for li in top_ul.select(&sel("li.u_cbox_comment")) {
        rows.extend(parse_comment_tree(&li, post_url));
    }
    rows
}

async fn collect_all_comments(driver: &WebDriver, post_url: &str) -> Result<Vec<CommentRow>> {
    let _ = ensure_comment_sort_oldest(driver).await;
    let _ = go_to_first_comment_page(driver).await;

    let (_, last_page) = get_comment_page_state(driver).await;
    let mut all_rows = Vec::new();
    let mut seen     = HashSet::new();

    for target_page in 1..=last_page {
        if target_page > 1 {
            if !click_comment_page(driver, target_page).await? { break; }
        }
        safe_sleep(0.4, 0.1).await;
        let page_source = driver.source().await.unwrap_or_default();
        for row in parse_comment_page_source(&page_source, post_url) {
            let key = format!("{}|{}|{}", row.comment_id, row.reply_level, row.created_at);
            if seen.insert(key) { all_rows.push(row); }
        }
    }
    Ok(all_rows)
}

async fn crawl_single_post(
    driver: &WebDriver,
    url: &str,
    search_row: &SearchRow,
    config: &PlanHConfig,
) -> Result<(PostRow, Vec<CommentRow>)> {
    load_detail_page(driver, url, config).await?;
    let page_source = driver.source().await?;

    // Html은 Send가 아니므로 블록 스코프로 await 전에 확실히 해제
    let (title, body, _like_count, comment_count) = {
        let document = Html::parse_document(&page_source);
        (
            extract_detail_title(&document, &search_row.search_title),
            extract_body_text(&document),
            extract_like_count(&document),
            extract_comment_count(&document),
        )
    };

    let mut comments = Vec::new();
    if comment_count > 0 {
        if open_comment_panel(driver).await? {
            comments = collect_all_comments(driver, url).await.unwrap_or_default();
        }
    }

    let post_row = PostRow {
        title,
        url: url.to_string(),
        search_date: search_row.search_date.clone(),
        body,
        comments_json: serde_json::to_string(&comments)?,
    };
    Ok((post_row, comments))
}

// ─────────────────────────────────────────────────────────────────
// CSV / JSON 저장 (UTF-8 BOM)
// ─────────────────────────────────────────────────────────────────

fn write_csv<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    let file = std::fs::File::create(path)?;
    let mut buf = std::io::BufWriter::new(file);
    buf.write_all(b"\xef\xbb\xbf")?;
    let mut wtr = csv::WriterBuilder::new().has_headers(true).from_writer(buf);
    for row in rows { wtr.serialize(row)?; }
    wtr.flush()?;
    Ok(())
}

