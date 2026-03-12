/// Plan I — Threads.com 키워드 크롤러
///
/// 흐름:
///   1단계) 단일 드라이버(창 표시) — 로그인 대기 → 검색 스크롤 → URL 목록 수집 → 쿠키 추출
///   2단계) Worker Pool — 쿠키 주입된 N개 드라이버가 게시글 상세·댓글 병렬 수집
use std::collections::{HashSet, VecDeque};
use std::io::{self, Write as _};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{anyhow, Result};
use regex::Regex;
use rust_xlsxwriter::{Workbook, XlsxError};
use serde::Serialize;
use thirtyfour::prelude::*;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};
use urlencoding::encode;

// ─────────────────────────────────────────────────────────────────
// 셀렉터 상수
// ─────────────────────────────────────────────────────────────────

const POST_CONTAINER_SELECTOR:     &str = "div.x1n2onr6.x1ypdohk.x1f9n5g.x17dsfyh";
const POST_TEXT_BLOCK_SELECTOR:    &str = "div.x1a6qonq";
const COMMENT_TEXT_BLOCK_SELECTOR: &str = "div.x1a6qonq";
const POST_LINK_SELECTOR:          &str = "a[href*=\"/post/\"]";

// ─────────────────────────────────────────────────────────────────
// 퍼블릭 설정 구조체
// ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PlanIConfig {
    pub keyword: String,
    pub max_posts: usize,
    pub workers: usize,
    pub webdriver_url: String,
    pub out_dir: String,
    /// 검색 결과 스크롤 최대 횟수
    pub search_max_rounds: usize,
    /// 검색 스크롤 간격 (초)
    pub search_pause_secs: u64,
    /// 댓글 스크롤 최대 횟수
    pub comment_scroll_rounds: usize,
    /// 댓글 스크롤 간격 (초)
    pub comment_pause_secs: u64,
}

impl Default for PlanIConfig {
    fn default() -> Self {
        Self {
            keyword: String::new(),
            max_posts: 30,
            workers: 3,
            webdriver_url: "http://localhost:9515".into(),
            out_dir: "out".into(),
            search_max_rounds: 60,
            search_pause_secs: 2,
            comment_scroll_rounds: 10,
            comment_pause_secs: 1,
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// 내부 데이터 구조
// ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct SearchPostCandidate {
    url: String,
    text: String,
}

#[derive(Debug, Clone)]
struct PostDetail {
    url: String,
    author: Option<String>,
    date: Option<String>,
    post_text: Option<String>,
    likes: Option<i64>,
    replies: Option<i64>,
    reposts: Option<i64>,
    comments: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct OutputRow {
    keyword: String,
    url: String,
    author: Option<String>,
    date: Option<String>,
    post_text: Option<String>,
    likes: Option<i64>,
    replies: Option<i64>,
    reposts: Option<i64>,
    comment_text: Option<String>,
}

// ─────────────────────────────────────────────────────────────────
// 퍼블릭 진입점
// ─────────────────────────────────────────────────────────────────

pub async fn run(config: PlanIConfig) -> Result<()> {
    if config.keyword.trim().is_empty() {
        return Err(anyhow!("keyword가 비어 있습니다."));
    }

    tokio::fs::create_dir_all(&config.out_dir).await?;

    // ── 1단계: 단일 드라이버로 로그인 + 검색 결과 수집 ───────────
    info!("🔍 [1단계] Threads 검색: \"{}\"", config.keyword);
    info!("  브라우저가 열리면 Threads에 로그인한 뒤 Enter를 누르세요.");

    let list_driver = make_driver(false, &config.webdriver_url).await?;
    list_driver.goto("https://www.threads.com/").await?;

    // 로그인 대기
    wait_for_enter("로그인 완료 후 Enter: ")?;

    let search_url = format!("https://www.threads.com/search?q={}", encode(&config.keyword));
    list_driver.goto(&search_url).await?;
    sleep(Duration::from_secs(4)).await;
    info!("  검색 URL: {}", search_url);

    let candidates = infinite_collect_posts(
        &list_driver,
        config.max_posts,
        config.search_max_rounds,
        config.search_pause_secs,
    ).await?;
    info!("  수집된 게시글 후보: {}개", candidates.len());

    // 쿠키 추출
    let cookies = Arc::new(
        list_driver.get_all_cookies().await
            .map_err(|e| anyhow!("쿠키 추출 실패: {e}"))?
    );
    let _ = list_driver.quit().await;

    if candidates.is_empty() {
        warn!("검색 결과가 없습니다.");
        return Ok(());
    }

    // ── 2단계: Worker Pool — 게시글 상세 병렬 수집 ───────────────
    let workers = config.workers.max(1);
    info!("⚡ [2단계] {}개 게시글 병렬 수집 (워커: {})", candidates.len(), workers);

    let total = candidates.len();
    let queue: Arc<Mutex<VecDeque<SearchPostCandidate>>> =
        Arc::new(Mutex::new(VecDeque::from(candidates)));
    let done = Arc::new(AtomicUsize::new(0));

    let mut joinset: JoinSet<Vec<PostDetail>> = JoinSet::new();

    for worker_id in 0..workers {
        let queue   = queue.clone();
        let done    = done.clone();
        let config  = config.clone();
        let cookies = cookies.clone();

        joinset.spawn(async move {
            let driver = match make_driver(true, &config.webdriver_url).await {
                Ok(d) => d,
                Err(e) => { warn!("워커 {worker_id} 드라이버 오류: {e}"); return vec![]; }
            };

            // 쿠키 주입
            if let Err(e) = inject_cookies(&driver, &cookies).await {
                warn!("워커 {worker_id} 쿠키 주입 실패: {e}");
                let _ = driver.quit().await;
                return vec![];
            }
            info!("워커 {worker_id} 준비 완료");

            let mut results = Vec::new();

            loop {
                let candidate = queue.lock().await.pop_front();
                let Some(candidate) = candidate else { break };

                let n = done.fetch_add(1, Ordering::Relaxed) + 1;

                match scrape_post(&driver, &candidate, &config).await {
                    Ok(detail) => {
                        info!("[{n}/{total}] 워커{worker_id} 완료: {}", candidate.url);
                        results.push(detail);
                    }
                    Err(e) => {
                        warn!("[{n}/{total}] 워커{worker_id} 실패 ({}): {e}", candidate.url);
                    }
                }
            }

            let _ = driver.quit().await;
            info!("워커 {worker_id} 종료");
            results
        });
    }

    let mut all_results: Vec<PostDetail> = Vec::new();
    while let Some(res) = joinset.join_next().await {
        match res {
            Ok(batch) => all_results.extend(batch),
            Err(e)    => warn!("join error: {e}"),
        }
    }

    info!("수집 완료: 게시글 {}개", all_results.len());

    // ── 저장 ─────────────────────────────────────────────────────
    let rows        = build_rows(&config.keyword, &all_results);
    let safe_kw     = sanitize_filename(&config.keyword);
    let out         = Path::new(&config.out_dir);
    let csv_path    = out.join(format!("threads_{}.csv",  safe_kw));
    let xlsx_path   = out.join(format!("threads_{}.xlsx", safe_kw));

    write_csv(&csv_path, &rows)
        .map_err(|e| anyhow!("CSV 저장 실패: {e}"))?;
    write_xlsx(&xlsx_path, &rows)
        .map_err(|e| anyhow!("XLSX 저장 실패: {e}"))?;

    info!("🎉 저장 완료");
    info!("  {}", csv_path.display());
    info!("  {}", xlsx_path.display());
    Ok(())
}

// ─────────────────────────────────────────────────────────────────
// 헬퍼 — 드라이버
// ─────────────────────────────────────────────────────────────────

async fn make_driver(headless: bool, webdriver_url: &str) -> Result<WebDriver> {
    let mut caps = DesiredCapabilities::chrome();
    if headless {
        caps.add_arg("--headless=new")?;
    }
    caps.add_arg("--start-maximized")?;
    caps.add_arg("--disable-blink-features=AutomationControlled")?;
    caps.add_arg("--disable-gpu")?;
    caps.add_arg("--no-sandbox")?;
    caps.add_arg("--disable-dev-shm-usage")?;

    let driver = WebDriver::new(webdriver_url, caps).await?;
    let _ = driver.set_page_load_timeout(Duration::from_secs(30)).await;
    let _ = driver.set_implicit_wait_timeout(Duration::from_millis(0)).await;

    let _ = driver.execute(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});",
        vec![],
    ).await;

    Ok(driver)
}

async fn inject_cookies(driver: &WebDriver, cookies: &[Cookie]) -> Result<()> {
    driver.goto("https://www.threads.com/").await?;
    sleep(Duration::from_secs(2)).await;
    for cookie in cookies {
        let _ = driver.add_cookie(cookie.clone()).await;
    }
    driver.refresh().await?;
    sleep(Duration::from_secs(3)).await;
    Ok(())
}

fn wait_for_enter(prompt: &str) -> Result<()> {
    print!("{}", prompt);
    io::stdout().flush()?;
    let mut buf = String::new();
    io::stdin().read_line(&mut buf)?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────
// 헬퍼 — 검색 단계
// ─────────────────────────────────────────────────────────────────

async fn get_scroll_height(driver: &WebDriver) -> i64 {
    driver.execute("return document.body.scrollHeight;", vec![])
        .await.ok().and_then(|v| v.json().as_i64()).unwrap_or(0)
}

async fn get_post_text_from_container(container: &WebElement) -> String {
    let blocks = match container.find_all(By::Css(POST_TEXT_BLOCK_SELECTOR)).await {
        Ok(v) => v,
        Err(_) => return String::new(),
    };

    let mut texts = Vec::new();
    for block in blocks {
        let spans = match block.find_all(By::Css("span")).await {
            Ok(v) => v,
            Err(_) => continue,
        };
        let mut parts = Vec::new();
        for sp in spans {
            let t = match sp.text().await {
                Ok(v) => normalize_text(&v),
                Err(_) => continue,
            };
            if !t.is_empty() { parts.push(t); }
        }
        let chunk = parts.join("\n").trim().to_string();
        if !chunk.is_empty() { texts.push(chunk); }
    }

    let mut dedup = Vec::new();
    let mut seen  = HashSet::new();
    for t in texts {
        if seen.insert(t.clone()) { dedup.push(t); }
    }
    dedup.join("\n").trim().to_string()
}

async fn get_post_url_from_container(container: &WebElement) -> Option<String> {
    let a = container.find(By::Css(POST_LINK_SELECTOR)).await.ok()?;
    a.attr("href").await.ok().flatten()
}

async fn get_search_post_candidates(driver: &WebDriver) -> Vec<SearchPostCandidate> {
    let containers = match driver.find_all(By::Css(POST_CONTAINER_SELECTOR)).await {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let mut candidates = Vec::new();
    let mut seen_url   = HashSet::new();

    for c in containers {
        let text = get_post_text_from_container(&c).await;
        if text.len() < 8 { continue; }

        let raw_url = match get_post_url_from_container(&c).await {
            Some(v) if v.contains("/post/") => v,
            _ => continue,
        };
        // href가 상대 경로(/@user/post/id)인 경우 절대 URL로 변환
        let url = if raw_url.starts_with("http") {
            raw_url
        } else {
            format!("https://www.threads.com{}", raw_url)
        };
        if !seen_url.insert(url.clone()) { continue; }

        candidates.push(SearchPostCandidate { url, text });
    }
    candidates
}

async fn infinite_collect_posts(
    driver: &WebDriver,
    target_count: usize,
    max_rounds: usize,
    pause_secs: u64,
) -> Result<Vec<SearchPostCandidate>> {
    let mut collected  = Vec::new();
    let mut seen       = HashSet::new();
    let mut stagnant   = 0usize;
    let mut last_height = get_scroll_height(driver).await;

    for i in 0..max_rounds {
        for item in get_search_post_candidates(driver).await {
            if seen.insert(item.url.clone()) {
                collected.push(item);
                if collected.len() >= target_count {
                    info!("  목표 달성: {}개", collected.len());
                    return Ok(collected);
                }
            }
        }

        info!("  스크롤 {}/{} | 누적 {}개", i + 1, max_rounds, collected.len());
        let _ = driver.execute("window.scrollTo(0, document.body.scrollHeight);", vec![]).await;
        sleep(Duration::from_secs(pause_secs)).await;

        let new_height = get_scroll_height(driver).await;
        if new_height == last_height {
            stagnant += 1;
            if stagnant >= 3 {
                info!("  새 게시글 없음 — 조기 종료");
                break;
            }
        } else {
            stagnant    = 0;
            last_height = new_height;
        }
    }
    Ok(collected)
}

// ─────────────────────────────────────────────────────────────────
// 헬퍼 — 게시글 상세
// ─────────────────────────────────────────────────────────────────

async fn scrape_post(
    driver: &WebDriver,
    candidate: &SearchPostCandidate,
    config: &PlanIConfig,
) -> Result<PostDetail> {
    driver.goto(&candidate.url).await?;
    sleep(Duration::from_secs(3)).await;

    let detail = extract_post_detail(driver, Some(&candidate.text), config).await?;
    Ok(detail)
}

async fn extract_post_detail(
    driver: &WebDriver,
    source_text: Option<&str>,
    config: &PlanIConfig,
) -> Result<PostDetail> {
    let url = driver.current_url().await?.to_string();

    let author = async {
        let el = driver.find(By::Css("a[href^=\"/@\"] span")).await.ok()?;
        let t  = normalize_text(&el.text().await.ok()?);
        if t.is_empty() { None } else { Some(t) }
    }.await;

    let date = async {
        let el = driver.find(By::Css("time")).await.ok()?;
        if let Some(v) = el.attr("datetime").await.ok().flatten() {
            let v = normalize_text(&v);
            if !v.is_empty() { return Some(v); }
        }
        let v = normalize_text(&el.text().await.ok()?);
        if v.is_empty() { None } else { Some(v) }
    }.await;

    let post_text = {
        let from_dom = async {
            let blocks = driver.find_all(By::Css(COMMENT_TEXT_BLOCK_SELECTOR)).await.ok()?;
            let t = normalize_text(&blocks.first()?.text().await.ok()?);
            if t.is_empty() { None } else { Some(t) }
        }.await;
        from_dom.or_else(|| source_text.map(|s| normalize_text(s)).filter(|s| !s.is_empty()))
    };

    let (likes, replies, reposts) = extract_stats(driver).await;

    // 댓글 스크롤
    comment_scroll(driver, config.comment_scroll_rounds, config.comment_pause_secs).await;

    let comments = extract_comments(driver, post_text.as_deref()).await;

    Ok(PostDetail { url, author, date, post_text, likes, replies, reposts, comments })
}

async fn extract_stats(driver: &WebDriver) -> (Option<i64>, Option<i64>, Option<i64>) {
    let btns = driver.find_all(By::Css("div.x4vbgl9 div[role=\"button\"]"))
        .await.unwrap_or_default();

    let mut nums = Vec::new();
    for btn in btns {
        if let Ok(text) = btn.text().await {
            nums.push(extract_first_number(&normalize_text(&text)));
        }
    }

    (
        nums.first().copied().flatten(),
        nums.get(1).copied().flatten(),
        nums.get(2).copied().flatten(),
    )
}

async fn comment_scroll(driver: &WebDriver, rounds: usize, pause_secs: u64) {
    let mut last_height = get_scroll_height(driver).await;
    let mut stagnant    = 0usize;

    for _ in 0..rounds {
        let _ = driver.execute("window.scrollTo(0, document.body.scrollHeight);", vec![]).await;
        sleep(Duration::from_secs(pause_secs)).await;

        let new_height = get_scroll_height(driver).await;
        if new_height == last_height {
            stagnant += 1;
            if stagnant >= 3 { break; }
        } else {
            stagnant    = 0;
            last_height = new_height;
        }
    }
}

async fn extract_comments(driver: &WebDriver, post_text: Option<&str>) -> Vec<String> {
    let blocks = driver.find_all(By::Css(COMMENT_TEXT_BLOCK_SELECTOR))
        .await.unwrap_or_default();

    let normalized_post = post_text.map(normalize_text);
    let mut comments    = Vec::new();

    for block in blocks {
        let txt = match block.text().await {
            Ok(v) => normalize_text(&v),
            Err(_) => continue,
        };
        if txt.len() < 2 { continue; }
        if normalized_post.as_deref() == Some(txt.as_str()) { continue; }
        comments.push(txt);
    }

    let mut dedup = Vec::new();
    let mut seen  = HashSet::new();
    for c in comments {
        if seen.insert(c.clone()) { dedup.push(c); }
    }
    dedup
}

// ─────────────────────────────────────────────────────────────────
// 헬퍼 — 유틸
// ─────────────────────────────────────────────────────────────────

fn normalize_text(s: &str) -> String {
    let nbsp = s.replace('\u{00a0}', " ");
    let re   = Regex::new(r"\s+").unwrap();
    re.replace_all(&nbsp, " ").trim().to_string()
}

fn extract_first_number(text: &str) -> Option<i64> {
    Regex::new(r"(\d+)").unwrap()
        .captures(text)?.get(1)?
        .as_str().parse().ok()
}

fn sanitize_filename(input: &str) -> String {
    Regex::new(r"[^0-9A-Za-z가-힣_\-]+").unwrap()
        .replace_all(input, "_").to_string()
}

fn build_rows(keyword: &str, results: &[PostDetail]) -> Vec<OutputRow> {
    let mut rows = Vec::new();
    for item in results {
        if item.comments.is_empty() {
            rows.push(OutputRow {
                keyword: keyword.to_string(),
                url: item.url.clone(),
                author: item.author.clone(),
                date: item.date.clone(),
                post_text: item.post_text.clone(),
                likes: item.likes,
                replies: item.replies,
                reposts: item.reposts,
                comment_text: None,
            });
        } else {
            for c in &item.comments {
                rows.push(OutputRow {
                    keyword: keyword.to_string(),
                    url: item.url.clone(),
                    author: item.author.clone(),
                    date: item.date.clone(),
                    post_text: item.post_text.clone(),
                    likes: item.likes,
                    replies: item.replies,
                    reposts: item.reposts,
                    comment_text: Some(c.clone()),
                });
            }
        }
    }
    rows
}

// ─────────────────────────────────────────────────────────────────
// CSV / XLSX 저장
// ─────────────────────────────────────────────────────────────────

fn write_csv(path: &Path, rows: &[OutputRow]) -> Result<()> {
    use std::io::Write as _;
    let file = std::fs::File::create(path)?;
    let mut buf = std::io::BufWriter::new(file);
    buf.write_all(b"\xef\xbb\xbf")?; // UTF-8 BOM
    let mut wtr = csv::WriterBuilder::new().has_headers(true).from_writer(buf);
    for row in rows { wtr.serialize(row)?; }
    wtr.flush()?;
    Ok(())
}

fn write_xlsx(path: &Path, rows: &[OutputRow]) -> Result<(), XlsxError> {
    let mut workbook  = Workbook::new();
    let worksheet     = workbook.add_worksheet();
    let headers       = ["keyword","url","author","date","post_text","likes","replies","reposts","comment_text"];

    for (col, h) in headers.iter().enumerate() {
        worksheet.write_string(0, col as u16, *h)?;
    }
    for (idx, row) in rows.iter().enumerate() {
        let r = (idx + 1) as u32;
        worksheet.write_string(r, 0, &row.keyword)?;
        worksheet.write_string(r, 1, &row.url)?;
        worksheet.write_string(r, 2, row.author.as_deref().unwrap_or(""))?;
        worksheet.write_string(r, 3, row.date.as_deref().unwrap_or(""))?;
        worksheet.write_string(r, 4, row.post_text.as_deref().unwrap_or(""))?;
        if let Some(v) = row.likes    { worksheet.write_number(r, 5, v as f64)?; }
        if let Some(v) = row.replies  { worksheet.write_number(r, 6, v as f64)?; }
        if let Some(v) = row.reposts  { worksheet.write_number(r, 7, v as f64)?; }
        worksheet.write_string(r, 8, row.comment_text.as_deref().unwrap_or(""))?;
    }
    workbook.save(path)?;
    Ok(())
}
