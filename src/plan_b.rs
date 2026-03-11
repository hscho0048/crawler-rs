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

        // 게시글 테이블이 렌더링될 때까지 최대 5초 폴링 (고정 2초 sleep 대체)
        {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                let ready = driver
                    .execute(
                        "return document.querySelector('table.article-table') !== null;",
                        vec![],
                    )
                    .await
                    .ok()
                    .and_then(|v| v.json().as_bool())
                    .unwrap_or(false);
                if ready || std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

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

/// 현재 페이지 테이블에서 일반글 row 파싱 (공지 제외) — JS 단일 호출로 일괄 추출
async fn scrape_page_rows(driver: &WebDriver, base_url: &Url) -> Vec<ArticleRef> {
    let script = r#"
        const rows = document.querySelectorAll('table.article-table > tbody > tr:not(.board-notice)');
        return Array.from(rows).map(row => {
            const link = row.querySelector('a.article');
            const date = row.querySelector('td.type_date');
            return {
                href:  link ? (link.getAttribute('href') || '') : '',
                title: link ? (link.textContent || '').trim() : '',
                date:  date ? (date.textContent || '').trim() : '',
            };
        }).filter(r => r.href);
    "#;

    let val = match driver.execute(script, vec![]).await {
        Ok(v)  => v,
        Err(e) => { warn!("JS row 추출 실패: {e}"); return vec![]; }
    };

    let arr = match val.json().as_array() {
        Some(a) => a.clone(),
        None    => return vec![],
    };

    let mut articles = Vec::new();
    for item in arr {
        let href  = item.get("href").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let title = item.get("title").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let date  = item.get("date").and_then(|v| v.as_str()).unwrap_or("").to_string();

        if href.is_empty() { continue; }
        let url = if href.starts_with("http") {
            match Url::parse(&href) { Ok(u) => u, Err(_) => continue }
        } else {
            match base_url.join(&href) { Ok(u) => u, Err(_) => continue }
        };
        articles.push(ArticleRef { url, title, date });
    }
    articles
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

    // cafe_main iframe이 DOM에 나타날 때까지 최대 5초 폴링 (고정 2초 sleep 대체)
    let iframe_idx: Option<u16> = {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(val) = driver
                .execute(
                    "const frames = Array.from(document.querySelectorAll('iframe')); \
                     return frames.findIndex(f => f.id === 'cafe_main');",
                    vec![],
                )
                .await
            {
                if let Some(idx) = val.json().as_i64().filter(|&i| i >= 0) {
                    break Some(idx as u16);
                }
            }
            if std::time::Instant::now() >= deadline {
                break None;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };

    // Naver Cafe는 본문/댓글이 모두 cafe_main iframe 안에 있음
    if let Some(idx) = iframe_idx {
        let _ = driver.enter_frame(idx).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
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
    // 여러 셀렉터 + SE span 추출을 단일 JS 호출로 처리
    let js = r#"
    (() => {
        const selectors = [
            '.se-main-container',
            '.article_viewer .content.CafeViewer',
            '.ContentRenderer',
            '.ArticleContentBox .article_viewer',
            '.article_container .content',
        ];
        for (const sel of selectors) {
            const el = document.querySelector(sel);
            if (!el) continue;
            const spans = Array.from(el.querySelectorAll('.se-text-paragraph span'));
            if (spans.length) {
                const t = spans.map(s => s.textContent.trim()).filter(Boolean).join('\n');
                if (t) return t;
            }
            const t = (el.innerText || el.textContent || '').trim();
            if (t) return t;
        }
        return '';
    })()
    "#;

    match driver.execute(js, vec![]).await {
        Ok(v) => v.json().as_str().unwrap_or("").trim().to_string(),
        Err(_) => String::new(),
    }
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
    // 스크롤 + 댓글 영역 노출
    let _ = driver.execute("window.scrollTo(0, document.body.scrollHeight);", vec![]).await;
    let _ = driver.execute(
        "const el = document.querySelector('.CommentBox, #comment, .comment_area'); \
         if (el) el.scrollIntoView({block: 'center'});",
        vec![],
    ).await;

    // CommentItem이 나타날 때까지 polling (최대 8초, 300ms 간격)
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    loop {
        let found = driver
            .execute("return !!document.querySelector('ul.comment_list li.CommentItem');", vec![])
            .await
            .ok()
            .and_then(|v| v.json().as_bool())
            .unwrap_or(false);
        if found || std::time::Instant::now() >= deadline { break; }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // "더보기" 반복 클릭 (클릭 후 DOM 변화 감지, 최대 500ms 대기)
    loop {
        match driver.find(By::Css(".btn_more_comment, .CommentMore button, .btn_more")).await {
            Ok(btn) if btn.is_displayed().await.unwrap_or(false) => {
                let before: i64 = driver
                    .execute("return document.querySelectorAll('ul.comment_list li.CommentItem').length;", vec![])
                    .await.ok().and_then(|v| v.json().as_i64()).unwrap_or(0);
                if btn.click().await.is_err() { break; }
                let wait_dl = std::time::Instant::now() + Duration::from_millis(500);
                loop {
                    let after: i64 = driver
                        .execute("return document.querySelectorAll('ul.comment_list li.CommentItem').length;", vec![])
                        .await.ok().and_then(|v| v.json().as_i64()).unwrap_or(0);
                    if after > before || std::time::Instant::now() >= wait_dl { break; }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            _ => break,
        }
    }

    // 댓글 전체를 단일 JS 호출로 추출
    #[derive(serde::Deserialize)]
    struct RawComment {
        comment_id: String,
        is_reply: bool,
        author: Option<String>,
        date: String,
        content: String,
    }

    let js = r#"
    Array.from(document.querySelectorAll('ul.comment_list li.CommentItem')).map(item => {
        const comment_id = item.getAttribute('id') || '';
        const is_reply = (item.getAttribute('class') || '').includes('CommentItem--reply');
        const author = item.querySelector('.comment_nickname')?.textContent?.trim() || null;
        const date = item.querySelector('.comment_info_date')?.textContent?.trim() || '';
        const raw = item.querySelector('.comment_text_box .text_comment')?.textContent || '';
        const content = raw.replace(/\s+/g, ' ').trim();
        return { comment_id, is_reply, author, date, content };
    }).filter(x => x.content)
    "#;

    let raw: Vec<RawComment> = match driver.execute(js, vec![]).await {
        Ok(v) => serde_json::from_value(v.json().clone()).unwrap_or_default(),
        Err(_) => return vec![],
    };

    raw.into_iter().map(|c| Comment {
        comment_id: c.comment_id,
        is_reply: c.is_reply,
        author: c.author,
        author_level_icon: None,
        author_avatar: None,
        date: c.date,
        content: c.content,
    }).collect()
}

// ─────────────────────────────────────────────────────────────────
// DOM 유틸
// ─────────────────────────────────────────────────────────────────

async fn find_text(driver: &WebDriver, selectors: &[&str]) -> Option<String> {
    // 셀렉터 목록을 단일 JS 호출로 처리 (WebDriver 왕복 절감)
    let sels_json = serde_json::to_string(selectors).unwrap_or_else(|_| "[]".to_string());
    let js = format!(
        r#"
        (() => {{
            const sels = {sels};
            for (const sel of sels) {{
                if (sel.startsWith('meta[')) {{
                    const v = document.querySelector(sel)?.getAttribute('content')?.trim();
                    if (v) return v;
                }} else if (sel === 'title') {{
                    const t = document.title?.trim();
                    if (t) return t;
                }} else {{
                    const el = document.querySelector(sel);
                    const t = (el?.innerText || el?.textContent || '').replace(/\s+/g, ' ').trim();
                    if (t) return t;
                }}
            }}
            return null;
        }})()
        "#,
        sels = sels_json
    );

    match driver.execute(&js, vec![]).await {
        Ok(v) => {
            let s = v.json().as_str()?.trim().to_string();
            if s.is_empty() { None } else { Some(s) }
        }
        Err(_) => None,
    }
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
        .set_implicit_wait_timeout(Duration::from_millis(0))
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
