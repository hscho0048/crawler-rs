use anyhow::{anyhow, Context, Result};
use csv::Writer;
use scraper::{ElementRef, Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashSet, VecDeque};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thirtyfour::prelude::*;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;

const BASE_URL: &str = "https://www.itdasocial.kr";

#[derive(Debug, Clone)]
pub struct PlanMConfig {
    pub start_page: usize,
    pub max_pages: usize,
    pub max_posts: usize,
    pub workers: usize,
    pub webdriver_url: String,
    pub out_dir: String,
    pub headless_workers: bool,
    pub profile_dir: String,
}

impl Default for PlanMConfig {
    fn default() -> Self {
        Self {
            start_page: 1,
            max_pages: 43,
            max_posts: 0,
            workers: 3,
            webdriver_url: "http://localhost:4444".to_string(),
            out_dir: "out".to_string(),
            headless_workers: true,
            profile_dir: "target/itda_login_profile".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct PostCandidate {
    list_page: usize,
    url: String,
    channel: Option<String>,
    title: Option<String>,
    comment_count: Option<i64>,
    likes: Option<i64>,
    views: Option<i64>,
}

#[derive(Debug, Clone)]
struct BrowserState {
    cookies: Vec<Cookie>,
    local_storage: Vec<[String; 2]>,
    session_storage: Vec<[String; 2]>,
}

#[derive(Debug, Default, Deserialize)]
struct StorageSnapshot {
    #[serde(rename = "localStorage")]
    local_storage: Vec<[String; 2]>,
    #[serde(rename = "sessionStorage")]
    session_storage: Vec<[String; 2]>,
}

#[derive(Debug, Clone)]
struct CommentDetail {
    author: Option<String>,
    date: Option<String>,
    likes: Option<i64>,
    body: Option<String>,
}

#[derive(Debug, Clone)]
struct PostDetail {
    list_page: usize,
    url: String,
    channel: Option<String>,
    title: Option<String>,
    author: Option<String>,
    date: Option<String>,
    body: Option<String>,
    tags: Vec<String>,
    views: Option<i64>,
    likes: Option<i64>,
    comment_count: Option<i64>,
    comments: Vec<CommentDetail>,
}

#[derive(Debug, Clone, Serialize)]
struct OutputRow {
    list_page: usize,
    post_url: String,
    channel: Option<String>,
    title: Option<String>,
    author: Option<String>,
    date: Option<String>,
    body: Option<String>,
    tags: String,
    views: Option<i64>,
    likes: Option<i64>,
    comment_count: Option<i64>,
    comment_index: Option<usize>,
    comment_author: Option<String>,
    comment_date: Option<String>,
    comment_likes: Option<String>,
    comment_body: Option<String>,
}

pub async fn run(cfg: PlanMConfig) -> Result<()> {
    tokio::fs::create_dir_all(&cfg.out_dir)
        .await
        .with_context(|| format!("output directory create failed: {}", cfg.out_dir))?;

    info!("itda login/list driver open");
    let profile_dir = login_profile_dir(&cfg.profile_dir);
    let list_driver = make_driver(&cfg.webdriver_url, false, Some(&profile_dir)).await?;
    let first_list_url = list_url(cfg.start_page);
    list_driver.goto(&first_list_url).await?;

    wait_for_enter(
        "Chrome window에서 로그인 완료 후 Enter를 누르세요. 이후 community?page=1로 다시 이동합니다: ",
    )?;

    list_driver.goto(&first_list_url).await?;
    sleep(Duration::from_secs(2)).await;

    let candidates = collect_post_candidates(&list_driver, &cfg).await?;
    info!("post urls collected: {}", candidates.len());

    let state = extract_browser_state(&list_driver)
        .await
        .context("login browser state extraction failed")?;
    let _ = list_driver.quit().await;

    if candidates.is_empty() {
        warn!("no post urls collected");
        return Ok(());
    }

    let details = scrape_details_parallel(&cfg, candidates, Arc::new(state)).await?;
    let rows = build_rows(&details);
    let csv_path = Path::new(&cfg.out_dir).join("itda_community.csv");
    write_csv(&csv_path, &rows)?;

    info!("itda community csv saved: {}", csv_path.display());
    Ok(())
}

fn login_profile_dir(configured: &str) -> PathBuf {
    if configured == PlanMConfig::default().profile_dir {
        return PathBuf::from("target")
            .join(format!("itda_login_profile_{}", std::process::id()));
    }
    PathBuf::from(configured)
}

fn list_url(page: usize) -> String {
    format!("{BASE_URL}/community?page={page}")
}

async fn collect_post_candidates(driver: &WebDriver, cfg: &PlanMConfig) -> Result<Vec<PostCandidate>> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let last_page = cfg.start_page.saturating_add(cfg.max_pages.saturating_sub(1));

    for page in cfg.start_page..=last_page {
        let page_url = list_url(page);
        info!("collect list page: {page_url}");
        driver.goto(&page_url).await?;
        driver
            .query(By::Css("body"))
            .wait(Duration::from_secs(20), Duration::from_millis(300))
            .first()
            .await
            .context("list page body load failed")?;
        sleep(Duration::from_millis(1200)).await;

        let source = driver.source().await?;
        let page_items = parse_list_page(&source, &page_url, page)?;
        if page_items.is_empty() {
            warn!("no post rows found on page {page}");
            break;
        }

        for item in page_items {
            if seen.insert(item.url.clone()) {
                out.push(item);
                if cfg.max_posts > 0 && out.len() >= cfg.max_posts {
                    return Ok(out);
                }
            }
        }
    }

    Ok(out)
}

async fn scrape_details_parallel(
    cfg: &PlanMConfig,
    candidates: Vec<PostCandidate>,
    state: Arc<BrowserState>,
) -> Result<Vec<PostDetail>> {
    let worker_count = cfg.workers.max(1).min(candidates.len().max(1));
    let queue: Arc<Mutex<VecDeque<PostCandidate>>> =
        Arc::new(Mutex::new(VecDeque::from(candidates)));
    let mut joinset: JoinSet<Vec<PostDetail>> = JoinSet::new();

    for worker_id in 0..worker_count {
        let cfg = cfg.clone();
        let queue = queue.clone();
        let state = state.clone();

        joinset.spawn(async move {
            sleep(Duration::from_millis(worker_id as u64 * 700)).await;

            let driver = match make_driver(&cfg.webdriver_url, cfg.headless_workers, None).await {
                Ok(driver) => driver,
                Err(e) => {
                    warn!("worker {worker_id} driver create failed: {e:#}");
                    return Vec::new();
                }
            };

            if let Err(e) = inject_browser_state(&driver, &state).await {
                warn!("worker {worker_id} browser state injection failed: {e:#}");
                let _ = driver.quit().await;
                return Vec::new();
            }
            info!("worker {worker_id} ready");

            let mut rows = Vec::new();
            loop {
                let candidate = {
                    let mut queue = queue.lock().await;
                    queue.pop_front()
                };
                let Some(candidate) = candidate else {
                    break;
                };

                match scrape_one_detail(&driver, &candidate).await {
                    Ok(detail) => {
                        info!("worker {worker_id} done: {}", candidate.url);
                        rows.push(detail);
                    }
                    Err(e) => warn!("worker {worker_id} failed: {} | {e:#}", candidate.url),
                }
            }

            let _ = driver.quit().await;
            rows
        });
    }

    let mut all = Vec::new();
    while let Some(result) = joinset.join_next().await {
        match result {
            Ok(batch) => all.extend(batch),
            Err(e) => warn!("worker join failed: {e}"),
        }
    }
    Ok(all)
}

async fn scrape_one_detail(driver: &WebDriver, candidate: &PostCandidate) -> Result<PostDetail> {
    driver.goto(&candidate.url).await?;
    driver
        .query(By::Css("article.cl-detail"))
        .wait(Duration::from_secs(20), Duration::from_millis(300))
        .first()
        .await
        .context("detail article load failed")?;
    sleep(Duration::from_millis(700)).await;

    let source = driver.source().await?;
    let mut detail = parse_detail_page(&source, &candidate.url, candidate.list_page)?;

    if detail.channel.is_none() {
        detail.channel = candidate.channel.clone();
    }
    if detail.title.is_none() {
        detail.title = candidate.title.clone();
    }
    if detail.comment_count.is_none() {
        detail.comment_count = candidate.comment_count;
    }
    if detail.likes.is_none() {
        detail.likes = candidate.likes;
    }
    if detail.views.is_none() {
        detail.views = candidate.views;
    }

    Ok(detail)
}

async fn make_driver(
    webdriver_url: &str,
    headless: bool,
    profile_dir: Option<&Path>,
) -> Result<WebDriver> {
    let mut caps = DesiredCapabilities::chrome();
    caps.add_arg("--window-size=1440,2200")?;
    caps.add_arg("--lang=ko-KR")?;
    caps.add_arg("--disable-blink-features=AutomationControlled")?;
    caps.add_arg("--disable-gpu")?;
    caps.add_arg("--no-sandbox")?;
    caps.add_arg("--disable-dev-shm-usage")?;
    caps.add_experimental_option("excludeSwitches", vec!["enable-automation"])?;
    caps.add_experimental_option("useAutomationExtension", false)?;
    if headless {
        caps.add_arg("--headless=new")?;
    }
    if let Some(profile_dir) = profile_dir {
        std::fs::create_dir_all(profile_dir)
            .with_context(|| format!("profile directory create failed: {}", profile_dir.display()))?;
        let abs_profile = std::fs::canonicalize(profile_dir)
            .unwrap_or_else(|_| profile_dir.to_path_buf());
        caps.add_arg(&format!("--user-data-dir={}", abs_profile.display()))?;
    }

    let driver = WebDriver::new(webdriver_url, caps).await?;
    let _ = driver.set_page_load_timeout(Duration::from_secs(35)).await;
    let _ = driver.set_implicit_wait_timeout(Duration::from_millis(0)).await;
    let _ = driver
        .execute(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});",
            vec![],
        )
        .await;
    Ok(driver)
}

async fn extract_browser_state(driver: &WebDriver) -> Result<BrowserState> {
    let cookies = driver.get_all_cookies().await?;
    let storage_value = driver
        .execute(
            r#"
            const dump = (storage) => {
                const rows = [];
                for (let i = 0; i < storage.length; i++) {
                    const key = storage.key(i);
                    rows.push([key, storage.getItem(key)]);
                }
                return rows;
            };
            return {
                localStorage: dump(window.localStorage),
                sessionStorage: dump(window.sessionStorage),
            };
            "#,
            vec![],
        )
        .await?;
    let storage: StorageSnapshot = serde_json::from_value(storage_value.json().clone())?;

    Ok(BrowserState {
        cookies,
        local_storage: storage.local_storage,
        session_storage: storage.session_storage,
    })
}

async fn inject_browser_state(driver: &WebDriver, state: &BrowserState) -> Result<()> {
    driver.goto(&list_url(1)).await?;
    sleep(Duration::from_millis(700)).await;

    for cookie in &state.cookies {
        let _ = driver.add_cookie(cookie.clone()).await;
    }

    driver
        .execute(
            r#"
            const localRows = arguments[0] || [];
            const sessionRows = arguments[1] || [];
            for (const [key, value] of localRows) {
                if (key) window.localStorage.setItem(key, value || '');
            }
            for (const [key, value] of sessionRows) {
                if (key) window.sessionStorage.setItem(key, value || '');
            }
            return true;
            "#,
            vec![json!(state.local_storage), json!(state.session_storage)],
        )
        .await?;

    driver.goto(&list_url(1)).await?;
    sleep(Duration::from_millis(900)).await;
    Ok(())
}

fn parse_list_page(html: &str, page_url: &str, page_no: usize) -> Result<Vec<PostCandidate>> {
    let base = Url::parse(page_url)?;
    let doc = Html::parse_document(html);
    let row_sel = selector("a.cl-post-row");
    let mut out = Vec::new();

    for row in doc.select(&row_sel) {
        let Some(href) = row.value().attr("href") else {
            continue;
        };
        let Ok(url) = base.join(href) else {
            continue;
        };

        out.push(PostCandidate {
            list_page: page_no,
            url: url.to_string(),
            channel: first_text(&row, ".cl-post-row__channel-badge"),
            title: first_text(&row, ".cl-post-row__title"),
            comment_count: first_text(&row, ".cl-post-row__comment-count")
                .and_then(|text| first_number(&text)),
            likes: first_text(&row, ".cl-post-row__likes").and_then(|text| first_number(&text)),
            views: first_text(&row, ".cl-post-row__views-icon").and_then(|text| first_number(&text)),
        });
    }

    Ok(out)
}

fn parse_detail_page(html: &str, post_url: &str, list_page: usize) -> Result<PostDetail> {
    let doc = Html::parse_document(html);
    let article_sel = selector("article.cl-detail");
    let article = doc
        .select(&article_sel)
        .next()
        .ok_or_else(|| anyhow!("detail article not found"))?;

    let meta_numbers = article
        .select(&selector(".cl-detail__meta-left .cl-detail__meta-icon"))
        .filter_map(|el| first_number(&element_text(&el)))
        .collect::<Vec<_>>();

    let comments = article
        .select(&selector("section.cl-comments article.cl-comment"))
        .map(parse_comment)
        .collect::<Vec<_>>();

    Ok(PostDetail {
        list_page,
        url: post_url.to_string(),
        channel: first_text(&article, ".cl-detail__channel"),
        title: first_text(&article, ".cl-detail__title"),
        author: first_text(&article, ".cl-detail__author-name"),
        date: first_text(&article, ".cl-detail__author-date"),
        body: first_text(&article, ".cl-detail__body"),
        tags: all_text(&article, ".cl-detail__tag"),
        views: meta_numbers.get(0).copied(),
        likes: meta_numbers.get(1).copied(),
        comment_count: meta_numbers.get(2).copied(),
        comments,
    })
}

fn parse_comment(comment: ElementRef<'_>) -> CommentDetail {
    let action_spans = comment
        .select(&selector(".cl-comment__actions > span"))
        .collect::<Vec<_>>();
    let date = action_spans
        .iter()
        .find(|span| {
            !span
                .value()
                .attr("class")
                .unwrap_or_default()
                .contains("cl-comment__likes")
        })
        .map(element_text)
        .filter(|text| !text.is_empty());

    CommentDetail {
        author: first_text(&comment, ".cl-comment__author"),
        date,
        likes: first_text(&comment, ".cl-comment__likes").and_then(|text| first_number(&text)),
        body: first_text(&comment, ".cl-comment__body"),
    }
}

fn build_rows(details: &[PostDetail]) -> Vec<OutputRow> {
    let mut rows = Vec::new();
    for detail in details {
        rows.push(output_row(detail));
    }
    rows
}

fn output_row(detail: &PostDetail) -> OutputRow {
    let comment_author = join_comment_values(detail.comments.iter().filter_map(|c| c.author.as_deref()));
    let comment_date = join_comment_values(detail.comments.iter().filter_map(|c| c.date.as_deref()));
    let comment_likes = join_comment_values(
        detail
            .comments
            .iter()
            .filter_map(|c| c.likes.map(|likes| likes.to_string()))
            .collect::<Vec<_>>()
            .iter()
            .map(String::as_str),
    );
    let comment_body = join_comment_values(detail.comments.iter().filter_map(|c| c.body.as_deref()));

    OutputRow {
        list_page: detail.list_page,
        post_url: detail.url.clone(),
        channel: detail.channel.clone(),
        title: detail.title.clone(),
        author: detail.author.clone(),
        date: detail.date.clone(),
        body: detail.body.clone(),
        tags: detail.tags.join(" "),
        views: detail.views,
        likes: detail.likes,
        comment_count: detail.comment_count,
        comment_index: None,
        comment_author,
        comment_date,
        comment_likes,
        comment_body,
    }
}

fn join_comment_values<'a>(values: impl Iterator<Item = &'a str>) -> Option<String> {
    let joined = values
        .map(normalize_text)
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(" | ");
    if joined.is_empty() {
        None
    } else {
        Some(joined)
    }
}

fn write_csv(path: &Path, rows: &[OutputRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut writer = Writer::from_path(path)?;
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}

fn wait_for_enter(prompt: &str) -> Result<()> {
    print!("{prompt}");
    io::stdout().flush()?;
    let mut buf = String::new();
    io::stdin().read_line(&mut buf)?;
    Ok(())
}

fn selector(css: &str) -> Selector {
    Selector::parse(css).unwrap()
}

fn first_text(scope: &ElementRef<'_>, css: &str) -> Option<String> {
    scope
        .select(&selector(css))
        .next()
        .map(|el| element_text(&el))
        .filter(|text| !text.is_empty())
}

fn all_text(scope: &ElementRef<'_>, css: &str) -> Vec<String> {
    scope
        .select(&selector(css))
        .map(|el| element_text(&el))
        .filter(|text| !text.is_empty())
        .collect()
}

fn element_text(el: &ElementRef<'_>) -> String {
    normalize_text(&el.text().collect::<Vec<_>>().join(" "))
}

fn normalize_text(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn first_number(text: &str) -> Option<i64> {
    let digits = text
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_list_post_rows() {
        let html = r#"
        <a class="cl-post-row cl-post-row--revealed" href="/community/region/d9ab">
          <span class="cl-post-row__channel-badge">지역이야기</span>
          <span class="cl-post-row__title">안녕하세요</span>
          <span class="cl-post-row__comment-count">댓글 2</span>
          <span class="cl-post-row__author">무무</span>
          <span class="cl-post-row__likes">1</span>
          <span class="cl-post-row__views-icon">3</span>
          <span class="cl-post-row__date">3시간 전</span>
          <span class="cl-post-row__preview">본문 미리보기</span>
        </a>
        "#;

        let rows = parse_list_page(html, "https://www.itdasocial.kr/community?page=1", 1).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].url, "https://www.itdasocial.kr/community/region/d9ab");
        assert_eq!(rows[0].title.as_deref(), Some("안녕하세요"));
        assert_eq!(rows[0].comment_count, Some(2));
    }

    #[test]
    fn parses_detail_with_all_comments() {
        let html = r#"
        <article class="cl-detail">
          <a class="cl-detail__channel">지역이야기</a>
          <h1 class="cl-detail__title">제목</h1>
          <div class="cl-detail__meta-left">
            <span class="cl-detail__meta-icon">조회 7</span>
            <span class="cl-detail__meta-icon cl-detail__meta-icon--likes">좋아요 2</span>
            <span class="cl-detail__meta-icon cl-detail__meta-icon--comments">댓글 2</span>
          </div>
          <span class="cl-detail__author-name">작성자</span>
          <span class="cl-detail__author-date">2026-05-22 08:05</span>
          <span class="cl-detail__tag">#첫인사</span>
          <div class="cl-detail__body">본문 내용</div>
          <section class="cl-comments">
            <article class="cl-comment">
              <span class="cl-comment__author">댓글1</span>
              <p class="cl-comment__body">반가워요</p>
              <div class="cl-comment__actions">
                <span class="cl-comment__likes">1</span>
                <span>4시간 전</span>
              </div>
            </article>
            <article class="cl-comment">
              <span class="cl-comment__author">댓글2</span>
              <p class="cl-comment__body">환영해요</p>
              <div class="cl-comment__actions">
                <span class="cl-comment__likes">0</span>
                <span>3시간 전</span>
              </div>
            </article>
          </section>
        </article>
        "#;

        let detail = parse_detail_page(html, "https://www.itdasocial.kr/community/region/d9ab", 1).unwrap();
        let rows = build_rows(&[detail]);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].comment_author.as_deref(), Some("댓글1 | 댓글2"));
        assert_eq!(rows[0].comment_likes.as_deref(), Some("1 | 0"));
        assert_eq!(rows[0].comment_body.as_deref(), Some("반가워요 | 환영해요"));
    }
}
