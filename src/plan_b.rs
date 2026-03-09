use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{info, warn};
use url::Url;

use thirtyfour::cookie::Cookie;
use thirtyfour::prelude::*;
use thirtyfour::ChromeCapabilities;

use crate::{
    errors::CrawlError,
    models::{BodyImage, Comment, PostData, Source},
};

// ─────────────────────────────────────────────────────────────────
// 쿠키 (네이버 로그인 세션)
// ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct CookieEntry {
    pub name: String,
    pub value: String,
}

/// JSON 파일에서 쿠키 목록을 로드합니다.
/// 형식: [{"name": "NID_AUT", "value": "..."}, ...]
pub fn load_cookies(path: &Path) -> Result<Vec<CookieEntry>, CrawlError> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| CrawlError::Parse(format!("쿠키 파일 읽기 실패: {e}")))?;
    serde_json::from_str::<Vec<CookieEntry>>(&raw)
        .map_err(|e| CrawlError::Parse(format!("쿠키 JSON 파싱 실패: {e}")))
}

/// WebDriver 세션에 쿠키를 주입합니다.
/// 반드시 cafe.naver.com 로 이동한 뒤에 호출해야 합니다.
async fn inject_cookies(driver: &WebDriver, cookies: &[CookieEntry]) {
    for c in cookies {
        let mut cookie = Cookie::new(c.name.as_str(), c.value.as_str());
        cookie.set_domain(".naver.com");
        cookie.set_path("/");
        if let Err(e) = driver.add_cookie(cookie).await {
            warn!(name = %c.name, "쿠키 주입 실패: {e}");
        }
    }
    info!(count = cookies.len(), "쿠키 주입 완료");
}

/// Chrome 세션을 생성하고 쿠키를 한 번만 주입합니다.
async fn create_session_with_cookies(
    webdriver_url: &str,
    cookies: &[CookieEntry],
) -> Result<WebDriver, CrawlError> {
    let driver = open_driver(webdriver_url).await?;
    if !cookies.is_empty() {
        if driver.goto("https://cafe.naver.com").await.is_ok() {
            inject_cookies(&driver, cookies).await;
        }
    }
    Ok(driver)
}

// ─────────────────────────────────────────────────────────────────
// 내부 구조체
// ─────────────────────────────────────────────────────────────────

/// 리스트 페이지에서 수집한 게시글 기본 정보
struct ArticleRef {
    url: Url,
    title: String,
    date: String,
}

// ─────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────

/// 리스트 페이지 크롤:
/// 1) URL 파라미터로 페이지 순회 (?page=N&size=50)
/// 2) 게시글 링크 수집
/// 3) workers 개 세션(Worker Pool)으로 게시글 스크랩 + 진행률 출력
///    - 세션은 시작 시 N개 생성, 쿠키 N번만 주입
///    - 이후 세션을 재사용하며 계속 크롤
pub async fn crawl_plan_b_from_list(
    webdriver_url: &str,
    list_url: Url,
    max_posts: usize,
    workers: usize,
    cookies: Arc<Vec<CookieEntry>>,
) -> Vec<Result<PostData, CrawlError>> {
    let max_posts = max_posts.max(1);
    let workers = workers.max(1);
    info!(url = %list_url, max_posts, workers, "리스트 크롤 시작");

    // ── 1단계: 링크 수집 (전용 세션 1개) ─────────────────────────
    let list_driver = match create_session_with_cookies(webdriver_url, &cookies).await {
        Ok(d) => d,
        Err(e) => return vec![Err(e)],
    };
    let refs = collect_article_refs_by_url(&list_driver, &list_url, max_posts).await;
    let _ = list_driver.quit().await;

    let total = refs.len();
    info!(total, "게시글 링크 수집 완료 → Worker Pool 스크랩 시작");

    if refs.is_empty() {
        return vec![Err(CrawlError::Parse(
            "게시글 링크를 찾지 못했습니다. CSS 선택자를 확인하세요.".into(),
        ))];
    }

    // ── 2단계: Worker Pool 스크랩 ─────────────────────────────────
    scrape_with_pool(webdriver_url, refs, workers, &cookies).await
}

/// 이미 알고 있는 URL 목록을 Worker Pool로 병렬 스크랩
pub async fn crawl_plan_b_parallel(
    webdriver_url: &str,
    urls: Vec<Url>,
    workers: usize,
    cookies: Arc<Vec<CookieEntry>>,
) -> Vec<Result<PostData, CrawlError>> {
    info!(n = urls.len(), workers, "Plan B Worker Pool 병렬 스크랩");
    let articles = urls
        .into_iter()
        .map(|url| ArticleRef { url, title: String::new(), date: String::new() })
        .collect();
    scrape_with_pool(webdriver_url, articles, workers, &cookies).await
}

/// WebDriver 연결 확인
pub async fn webdriver_smoke_test(webdriver_url: &str) -> Result<(), CrawlError> {
    let driver = open_driver(webdriver_url).await?;
    driver
        .goto("data:text/html,<title>ok</title>")
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    let title = driver.title().await.map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    info!(%title, "webdriver smoke test ok");
    driver.quit().await.map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────
// Worker Pool 핵심 로직
// ─────────────────────────────────────────────────────────────────

/// workers 개의 Chrome 세션을 미리 생성(쿠키 1회 주입)하고,
/// 공유 큐에서 게시글을 꺼내 순차 처리하는 Worker Pool.
async fn scrape_with_pool(
    webdriver_url: &str,
    articles: Vec<ArticleRef>,
    workers: usize,
    cookies: &Arc<Vec<CookieEntry>>,
) -> Vec<Result<PostData, CrawlError>> {
    let total = articles.len();
    let queue: Arc<Mutex<VecDeque<ArticleRef>>> =
        Arc::new(Mutex::new(VecDeque::from(articles)));
    let done = Arc::new(AtomicUsize::new(0));
    let mut joinset: JoinSet<Vec<Result<PostData, CrawlError>>> = JoinSet::new();

    for worker_id in 0..workers {
        let wd = webdriver_url.to_string();
        let queue = queue.clone();
        let cookies = cookies.clone();
        let done = done.clone();

        joinset.spawn(async move {
            // 세션 생성 + 쿠키 주입 (이 워커 수명 동안 1회)
            let driver = match create_session_with_cookies(&wd, &cookies).await {
                Ok(d) => d,
                Err(e) => {
                    warn!("워커 {worker_id} 세션 생성 실패: {e}");
                    return vec![];
                }
            };
            info!("워커 {worker_id} 준비 완료 (쿠키 주입 완료)");

            let mut results = Vec::new();

            loop {
                // 큐에서 게시글 1개 가져옴 (락은 pop 직후 즉시 해제)
                let article = queue.lock().await.pop_front();
                let Some(article) = article else { break };

                let result = scrape_with_driver(&driver, article).await;
                let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                match &result {
                    Ok(p)  => info!("[{n}/{total}] 워커{worker_id} 완료: {}", p.title),
                    Err(e) => warn!("[{n}/{total}] 워커{worker_id} 실패: {e}"),
                }
                results.push(result);
            }

            let _ = driver.quit().await;
            info!("워커 {worker_id} 종료");
            results
        });
    }

    let mut out = Vec::new();
    while let Some(res) = joinset.join_next().await {
        match res {
            Ok(r)  => out.extend(r),
            Err(e) => out.push(Err(CrawlError::Parse(format!("join error: {e}")))),
        }
    }
    out
}

// ─────────────────────────────────────────────────────────────────
// URL 기반 페이지 순회
// ─────────────────────────────────────────────────────────────────

/// ?page=N&size=50 파라미터로 페이지를 순회하며 링크 수집
async fn collect_article_refs_by_url(
    driver: &WebDriver,
    base_url: &Url,
    max: usize,
) -> Vec<ArticleRef> {
    let mut all: Vec<ArticleRef> = Vec::new();
    let mut page = 1u32;

    loop {
        let page_url = build_page_url(base_url, page);
        info!("리스트 페이지 {page} 수집 중 → {page_url}");

        if let Err(e) = driver.goto(page_url.as_str()).await {
            warn!("페이지 {page} 이동 실패: {e}");
            break;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        let rows = scrape_page_rows(driver, &page_url).await;
        let count = rows.len();
        info!("페이지 {page}: {count}개 발견 (누적 {})", all.len() + count);

        if count == 0 {
            info!("페이지 {page}: 게시글 없음 → 수집 종료");
            break;
        }

        for row in rows {
            all.push(row);
            if all.len() >= max {
                info!("목표 {max}개 달성 → 수집 종료");
                return all;
            }
        }

        page += 1;
    }

    all
}

/// URL에 page=N, size=50 파라미터를 설정 (기존 page/size는 덮어씀)
fn build_page_url(base: &Url, page: u32) -> Url {
    let mut url = base.clone();
    let existing: Vec<(String, String)> = url
        .query_pairs()
        .filter(|(k, _)| k != "page" && k != "size")
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();

    let mut params = existing;
    params.push(("page".to_string(), page.to_string()));
    params.push(("size".to_string(), "50".to_string()));

    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    url.set_query(Some(&query));
    url
}

/// 현재 페이지 테이블에서 일반글 row 파싱 (공지 제외)
async fn scrape_page_rows(driver: &WebDriver, base_url: &Url) -> Vec<ArticleRef> {
    let rows = driver
        .find_all(By::Css("table.article-table > tbody > tr:not(.board-notice)"))
        .await
        .unwrap_or_default();

    let mut articles = Vec::new();
    for row in &rows {
        if let Some(a) = parse_article_row(row, base_url).await {
            articles.push(a);
        }
    }
    articles
}

/// 단일 row에서 URL / 제목 / 날짜 추출
async fn parse_article_row(row: &WebElement, base_url: &Url) -> Option<ArticleRef> {
    // 제목 링크 (a.article)
    let link_elem = row.find(By::Css("a.article")).await.ok()?;
    let href = link_elem.attr("href").await.ok()??;

    let url = if href.starts_with("http") {
        Url::parse(&href).ok()?
    } else {
        base_url.join(&href).ok()?
    };

    // 제목 텍스트 (head 접두사 포함)
    let title = link_elem.text().await.unwrap_or_default().trim().to_string();

    // 작성일 (td.type_date)
    let date = if let Ok(date_elem) = row.find(By::Css("td.type_date")).await {
        date_elem.text().await.unwrap_or_default().trim().to_string()
    } else {
        String::new()
    };

    Some(ArticleRef { url, title, date })
}


// ─────────────────────────────────────────────────────────────────
// 게시글 페이지 스크랩 (세션 재사용 버전 — quit 없음)
// ─────────────────────────────────────────────────────────────────

/// 기존 세션(driver)을 받아 게시글 1개를 스크랩합니다.
/// 세션 생성/종료는 호출자(Worker Pool)가 담당합니다.
async fn scrape_with_driver(
    driver: &WebDriver,
    article: ArticleRef,
) -> Result<PostData, CrawlError> {
    // 이전 게시글에서 iframe에 들어가 있을 수 있으므로 최상위 프레임으로 복귀
    let _ = driver.enter_default_frame().await;

    if let Err(e) = driver.goto(article.url.as_str()).await {
        return Err(CrawlError::WebDriver(e.to_string()));
    }
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Naver Cafe는 본문/댓글이 모두 cafe_main iframe 안에 있음
    // JS로 iframe index를 동적으로 찾아 전환 (thirtyfour 0.35 = u16 index만 지원)
    if let Ok(val) = driver
        .execute(
            "const frames = Array.from(document.querySelectorAll('iframe')); \
             return frames.findIndex(f => f.id === 'cafe_main');",
            vec![],
        )
        .await
    {
        if let Some(idx) = val.json().as_i64().filter(|&i| i >= 0) {
            if driver.enter_frame(idx as u16).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    // 제목: 리스트 페이지 값 우선, 없으면 포스트 페이지에서
    let title = if !article.title.is_empty() {
        article.title.clone()
    } else {
        let t = find_text(driver, &[
            "h3.title_text",
            ".ArticleTitle h3",
            ".article_viewer .title_text",
            ".ArticleContentBox .title_text",
            "meta[property='og:title']",
            "title",
        ])
        .await;
        // 위 셀렉터 전부 실패 시 본문 첫 span으로 백업
        if t.as_deref().map(|s| s.is_empty()).unwrap_or(true) {
            find_text(driver, &[".se-main-container .se-text-paragraph span"])
                .await
                .unwrap_or_default()
        } else {
            t.unwrap_or_default()
        }
    };

    let author = find_text(driver, &[".profile_area .profile_info .nickname"])
        .await
        .unwrap_or_default();

    let author_level = find_text(driver, &[".profile_area .profile_info .nick_level"])
        .await
        .unwrap_or_default();

    // 날짜: 포스트 페이지 우선 (더 정확), 없으면 리스트 페이지 날짜
    // (.comment_info_date 는 댓글 날짜이므로 제외)
    let written_at = find_text(driver, &[
        "span.date",
        ".article_info .date",
        ".WriterInfo .date",
        ".ArticleContentBox .date",
        "meta[property='article:published_time']",
    ])
    .await
    .unwrap_or(article.date);

    let views = find_text(driver, &[".profile_area .article_info .count"])
        .await
        .unwrap_or_default();

    let body = collect_body_text(driver).await;
    let body_images = collect_body_images(driver).await;
    let comments = collect_comments(driver).await;

    if title.trim().is_empty() && body.trim().is_empty() {
        return Err(CrawlError::RequiresJsOrBlocked);
    }

    Ok(PostData {
        source: Source::NaverCafe,
        url: article.url,
        title,
        author,
        author_level,
        written_at,
        views,
        body,
        body_images,
        comments,
    })
}

// ─────────────────────────────────────────────────────────────────
// 본문 텍스트 / 이미지
// ─────────────────────────────────────────────────────────────────

async fn collect_body_text(driver: &WebDriver) -> String {
    let body_selectors = [
        ".se-main-container",
        ".article_viewer .content.CafeViewer",
        ".ContentRenderer",
        ".ArticleContentBox .article_viewer",
        ".article_container .content",
    ];

    for css in &body_selectors {
        let Ok(container) = driver.find(By::Css(*css)).await else { continue };

        // Smart Editor(SE) 구조면 span 단위로 추출해 줄바꿈 보존
        let paras = container
            .find_all(By::Css(".se-text-paragraph span"))
            .await
            .unwrap_or_default();
        if !paras.is_empty() {
            let mut lines = Vec::new();
            for p in &paras {
                if let Ok(t) = p.text().await {
                    let s = t.trim().to_string();
                    if !s.is_empty() {
                        lines.push(s);
                    }
                }
            }
            let result = lines.join("\n");
            if !result.is_empty() {
                return result;
            }
        }

        // 일반 구조: 컨테이너 전체 텍스트
        if let Ok(t) = container.text().await {
            let s = t.trim().to_string();
            if !s.is_empty() {
                return s;
            }
        }
    }

    String::new()
}

async fn collect_body_images(driver: &WebDriver) -> Vec<BodyImage> {
    let img_elems = driver
        .find_all(By::Css(
            ".article_viewer .se-module-image img.se-image-resource",
        ))
        .await
        .unwrap_or_default();
    let mut display_srcs: Vec<String> = Vec::new();
    for img in &img_elems {
        if let Ok(Some(src)) = img.attr("src").await {
            display_srcs.push(src);
        }
    }

    let link_elems = driver
        .find_all(By::Css(
            ".article_viewer .se-module-image a.__se_image_link",
        ))
        .await
        .unwrap_or_default();

    let mut images: Vec<BodyImage> = Vec::new();
    for (i, elem) in link_elems.iter().enumerate() {
        let src = display_srcs.get(i).cloned().unwrap_or_default();
        let mut img = BodyImage { src, ..Default::default() };

        if let Ok(Some(data)) = elem.attr("data-linkdata").await {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&data) {
                img.original_src = json.get("src").and_then(|v| v.as_str()).map(String::from);
                img.original_width = json.get("originalWidth").and_then(|v| v.as_u64()).map(|n| n as u32);
                img.original_height = json.get("originalHeight").and_then(|v| v.as_u64()).map(|n| n as u32);
                img.file_size = json.get("fileSize").and_then(|v| v.as_u64());
            }
        }
        images.push(img);
    }

    if images.is_empty() {
        for src in display_srcs {
            images.push(BodyImage { src, ..Default::default() });
        }
    }
    images
}

// ─────────────────────────────────────────────────────────────────
// 댓글
// ─────────────────────────────────────────────────────────────────

async fn collect_comments(driver: &WebDriver) -> Vec<Comment> {
    // ── 1) 페이지 하단까지 스크롤 → IntersectionObserver 트리거 ──────────
    for _ in 0..3 {
        let _ = driver
            .execute("window.scrollTo(0, document.body.scrollHeight);", vec![])
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // ── 2) CommentBox 명시적 스크롤 ──────────────────────────────────────
    let _ = driver
        .execute(
            "const el = document.querySelector('.CommentBox, #comment, .comment_area'); \
             if (el) el.scrollIntoView({block: 'center'});",
            vec![],
        )
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── 3) li.CommentItem 이 나타날 때까지 최대 10초 폴링 ───────────────
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let found = driver
            .find_all(By::Css("ul.comment_list li.CommentItem"))
            .await
            .map(|v| !v.is_empty())
            .unwrap_or(false);
        if found || std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // ── 6) "더보기" 반복 클릭 ────────────────────────────────────────────
    loop {
        match driver
            .find(By::Css(".btn_more_comment, .CommentMore button, .btn_more"))
            .await
        {
            Ok(btn) if btn.is_displayed().await.unwrap_or(false) => {
                if btn.click().await.is_err() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(800)).await;
            }
            _ => break,
        }
    }

    let items = driver
        .find_all(By::Css("ul.comment_list li.CommentItem"))
        .await
        .unwrap_or_default();

    let mut comments = Vec::new();
    for item in &items {
        let comment_id = item.attr("id").await.ok().flatten().unwrap_or_default();
        let class = item.attr("class").await.ok().flatten().unwrap_or_default();
        let is_reply = class.contains("CommentItem--reply");

        let author = elem_text(item, ".comment_nickname").await;
        let date = elem_text(item, ".comment_info_date").await.unwrap_or_default();
        let content = elem_text(item, ".comment_text_box .text_comment").await.unwrap_or_default();
        let content = normalize_ws(&content);

        if content.is_empty() {
            continue;
        }

        let author_avatar = elem_attr(item, "a.comment_thumb img", "src").await;
        let author_level_icon =
            elem_attr(item, ".comment_nick_box .LevelIcon.icon_level", "style")
                .await
                .and_then(|s| extract_bg_url(&s));

        comments.push(Comment {
            comment_id,
            is_reply,
            author,
            author_level_icon,
            author_avatar,
            date,
            content,
        });
    }
    comments
}

fn extract_bg_url(style: &str) -> Option<String> {
    let start = style.find("url(")? + 4;
    let rest = &style[start..];
    let end = rest.find(')')?;
    let url = rest[..end].trim_matches(|c| c == '"' || c == '\'').to_string();
    if url.is_empty() { None } else { Some(url) }
}

// ─────────────────────────────────────────────────────────────────
// DOM 유틸
// ─────────────────────────────────────────────────────────────────

async fn find_text(driver: &WebDriver, selectors: &[&str]) -> Option<String> {
    for css in selectors {
        if css.starts_with("meta[") {
            if let Ok(elem) = driver.find(By::Css(*css)).await {
                if let Ok(Some(v)) = elem.attr("content").await {
                    let s = v.trim().to_string();
                    if !s.is_empty() {
                        return Some(s);
                    }
                }
            }
            continue;
        }
        if *css == "title" {
            if let Ok(t) = driver.title().await {
                let s = t.trim().to_string();
                if !s.is_empty() {
                    return Some(s);
                }
            }
            continue;
        }
        if let Ok(elem) = driver.find(By::Css(*css)).await {
            if let Ok(t) = elem.text().await {
                let s = normalize_ws(&t);
                if !s.is_empty() {
                    return Some(s);
                }
            }
        }
    }
    None
}

async fn elem_text(parent: &WebElement, css: &str) -> Option<String> {
    let child = parent.find(By::Css(css)).await.ok()?;
    let t = child.text().await.ok()?;
    let s = t.trim().to_string();
    if s.is_empty() { None } else { Some(s) }
}

async fn elem_attr(parent: &WebElement, css: &str, attr: &str) -> Option<String> {
    let child = parent.find(By::Css(css)).await.ok()?;
    child.attr(attr).await.ok().flatten().filter(|s| !s.is_empty())
}

fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

// ─────────────────────────────────────────────────────────────────
// Chrome 설정
// ─────────────────────────────────────────────────────────────────

async fn open_driver(webdriver_url: &str) -> Result<WebDriver, CrawlError> {
    let caps = chrome_caps()?;
    let driver = WebDriver::new(webdriver_url, caps)
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    driver
        .set_page_load_timeout(Duration::from_secs(30))
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    driver
        .set_implicit_wait_timeout(Duration::from_secs(5))
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    Ok(driver)
}

fn chrome_caps() -> Result<ChromeCapabilities, CrawlError> {
    let mut caps = ChromeCapabilities::new();
    caps.set_headless().map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.set_no_sandbox().map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.set_disable_gpu().map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.set_disable_dev_shm_usage().map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    // headless 뷰포트 크기 명시 → IntersectionObserver가 올바르게 동작하도록
    caps.add_arg("--window-size=1920,1080")
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    // 자동화 감지 우회
    caps.add_arg("--disable-blink-features=AutomationControlled")
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    Ok(caps)
}
