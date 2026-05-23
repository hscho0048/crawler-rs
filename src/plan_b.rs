use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use reqwest::header::{self, HeaderMap, HeaderValue};
use scraper::Html;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{info, warn};
use url::Url;

use thirtyfour::cookie::{Cookie, SameSite};
use thirtyfour::prelude::*;
use thirtyfour::{Capabilities, ChromeCapabilities, DesiredCapabilities};

use crate::{
    errors::CrawlError,
    models::{BodyImage, Comment, PostData, Source},
};

const CAFE_LIST_READY_TIMEOUT_SECS: u64 = 180;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserKind {
    Chrome,
    Firefox,
}

impl BrowserKind {
    pub fn parse(value: &str) -> Result<Self, CrawlError> {
        match value.to_ascii_lowercase().as_str() {
            "chrome" => Ok(Self::Chrome),
            "firefox" | "gecko" | "geckodriver" => Ok(Self::Firefox),
            other => Err(CrawlError::Parse(format!(
                "unsupported browser '{other}'. use chrome or firefox"
            ))),
        }
    }
}

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
        // SameSite=None + Secure: 서브도메인 간 cross-origin fetch에서도 쿠키 전달
        cookie.set_same_site(SameSite::None);
        cookie.set_secure(true);
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
        // cafe.naver.com 에 쿠키 주입
        if driver.goto("https://cafe.naver.com").await.is_ok() {
            inject_cookies(&driver, cookies).await;
        }
        // article.cafe.naver.com 에도 주입 (게시글 본문 API 호출 도메인)
        if driver.goto("https://article.cafe.naver.com").await.is_ok() {
            inject_cookies(&driver, cookies).await;
        }
    }
    Ok(driver)
}

// ─────────────────────────────────────────────────────────────────
// Naver Cafe API — reqwest 직접 호출 (브라우저 쿠키 정책 우회)
// ─────────────────────────────────────────────────────────────────

/// 쿠키 목록 → "key=val; key2=val2" HTTP 헤더 문자열
fn build_cookie_header(cookies: &[CookieEntry]) -> String {
    cookies
        .iter()
        .map(|c| format!("{}={}", c.name, c.value))
        .collect::<Vec<_>>()
        .join("; ")
}

/// SPA URL 또는 ArticleRead.nhn URL에서 (cafeId, articleId) 추출
fn extract_cafe_article_ids(url: &Url) -> Option<(String, String)> {
    // SPA: /ca-fe/cafes/{cafeId}/articles/{articleId}
    let segs: Vec<&str> = url.path().split('/').filter(|s| !s.is_empty()).collect();
    if segs.len() >= 5
        && (segs[0] == "ca-fe" || segs[0] == "f-e")
        && segs[1] == "cafes"
        && segs[3] == "articles"
    {
        return Some((segs[2].to_string(), segs[4].to_string()));
    }
    // ArticleRead.nhn: ?clubid=...&articleid=...
    let params: std::collections::HashMap<_, _> = url.query_pairs().collect();
    if let (Some(club), Some(art)) = (params.get("clubid"), params.get("articleid")) {
        return Some((club.to_string(), art.to_string()));
    }
    None
}

/// HTML 문자열에서 텍스트만 추출 (scraper 사용)
fn html_to_text(html: &str) -> String {
    Html::parse_fragment(html)
        .root_element()
        .text()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Unix milliseconds → "YYYY-MM-DD HH:MM:SS" (KST = UTC+9)
fn ms_to_kst_str(ms: i64) -> String {
    let secs = ms / 1000 + 9 * 3600;
    let sec = secs % 60;
    let min = (secs / 60) % 60;
    let hour = (secs / 3600) % 24;
    let mut days = secs / 86400;
    let mut year = 1970i32;
    loop {
        let dy = if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 366 } else { 365 };
        if days < dy { break; }
        days -= dy;
        year += 1;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let mlen: [i64; 12] = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut month = 1i32;
    for dl in mlen {
        if days < dl { break; }
        days -= dl;
        month += 1;
    }
    let day = days + 1;
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{min:02}:{sec:02}")
}

// ── API 응답 구조체 ────────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArticleApiResp {
    result: Option<ArticleApiResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArticleApiResult {
    article: Option<ArticleApiArticle>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ArticleApiArticle {
    subject: Option<String>,
    content_html: Option<String>,
    writer: Option<ArticleApiWriter>,
    write_date: Option<i64>,
    read_count: Option<u64>,
}

#[derive(Deserialize)]
struct ArticleApiWriter {
    nick: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommentApiResp {
    result: Option<CommentApiResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommentApiResult {
    comment: Option<CommentApiData>,
    // gw/v4 가 다른 필드명을 쓸 경우 대비
    comments: Option<CommentApiData>,
    comment_list: Option<CommentApiData>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommentApiData {
    items: Option<Vec<CommentApiItem>>,
    total_count: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommentApiItem {
    id: Option<u64>,
    content: Option<String>,
    writer: Option<CommentApiWriter>,
    update_date: Option<i64>,
    #[serde(default)]
    is_reply: bool,
}

#[derive(Deserialize)]
struct CommentApiWriter {
    nick: Option<String>,
}

/// 게시글 API 호출 → ArticleApiArticle
async fn fetch_article_api(
    client: &reqwest::Client,
    cafe_id: &str,
    article_id: &str,
) -> Option<ArticleApiArticle> {
    let url = format!(
        "https://article.cafe.naver.com/gw/v4/cafes/{cafe_id}/articles/{article_id}\
         ?query=&useCafeId=true&requestFrom=A"
    );
    let referer = format!("https://cafe.naver.com/ca-fe/cafes/{cafe_id}/articles/{article_id}");
    let resp = client
        .get(&url)
        .header("Origin",  "https://cafe.naver.com")
        .header("Referer", &referer)
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        warn!("article API {} → cafe={cafe_id} art={article_id}", resp.status());
        return None;
    }
    let data: ArticleApiResp = resp.json().await.ok()?;
    data.result?.article
}

/// 댓글 API 호출 → Vec<Comment> (페이지네이션 지원)
async fn fetch_comments_api(
    client: &reqwest::Client,
    cafe_id: &str,
    article_id: &str,
) -> Vec<Comment> {
    let mut all: Vec<Comment> = Vec::new();
    let mut page = 1u32;

    loop {
        let url = format!(
            "https://article.cafe.naver.com/gw/v4/cafes/{cafe_id}/articles/{article_id}/comments\
             ?page={page}&perPage=100&requestFrom=A"
        );
        let referer = format!("https://cafe.naver.com/ca-fe/cafes/{cafe_id}/articles/{article_id}");
        let resp = match client
            .get(&url)
            .header("Origin",  "https://cafe.naver.com")
            .header("Referer", &referer)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => { warn!("comments API 실패: {e}"); break; }
        };

        if !resp.status().is_success() {
            warn!("comments API {} page={page}", resp.status());
            break;
        }

        let data: CommentApiResp = match resp.json().await {
            Ok(d) => d,
            Err(e) => { warn!("comments JSON 파싱 실패: {e}"); break; }
        };

        let comment_data = match data.result.and_then(|r| r.comment.or(r.comments).or(r.comment_list)) {
            Some(d) => d,
            None => break,
        };

        let items = comment_data.items.unwrap_or_default();
        let total = comment_data.total_count.unwrap_or(0);

        if items.is_empty() { break; }

        for item in items {
            all.push(Comment {
                comment_id: item.id.map(|n| n.to_string()).unwrap_or_default(),
                is_reply: item.is_reply,
                author: item.writer.and_then(|w| w.nick),
                author_level_icon: None,
                author_avatar: None,
                date: item.update_date.map(ms_to_kst_str).unwrap_or_default(),
                content: item.content.unwrap_or_default(),
            });
        }

        if all.len() as u64 >= total { break; }
        page += 1;
    }

    all
}

// ── SSR HTML 파싱 ─────────────────────────────────────────────────

struct SsrArticle {
    title:      Option<String>,
    body:       Option<String>,
    author:     Option<String>,
    written_at: Option<String>,
    views:      Option<String>,
}

/// /f-e/ 또는 /ca-fe/ 페이지에서 SSR HTML을 GET 후 __NEXT_DATA__ JSON을 파싱
/// Next.js SSR이 서버 쪽에서 Naver 내부 인증으로 본문을 포함시켜 반환함
async fn fetch_article_ssr(client: &reqwest::Client, url: &Url) -> Option<SsrArticle> {
    // 쿼리 파라미터(page/menuid/…) 제거 후 순수 경로만 요청
    let mut clean = url.clone();
    clean.set_query(None);

    let resp = client
        .get(clean.as_str())
        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        .header("Accept-Language", "ko-KR,ko;q=0.9")
        .send()
        .await
        .ok()?;

    if !resp.status().is_success() {
        warn!("SSR HTTP {} → {clean}", resp.status());
        return None;
    }

    let html = resp.text().await.ok()?;

    // 1) __NEXT_DATA__ JSON embed 추출
    if let Some(art) = parse_next_data(&html) {
        return Some(art);
    }

    // 2) 정적 HTML DOM 파싱 (폴백)
    parse_html_dom(&html)
}

/// <script id="__NEXT_DATA__" type="application/json">…</script> 에서 게시글 데이터 추출
fn parse_next_data(html: &str) -> Option<SsrArticle> {
    let marker = r#"id="__NEXT_DATA__""#;
    let start  = html.find(marker)?;
    let after  = &html[start..];
    let gt     = after.find('>')?;
    let content = &after[gt + 1..];
    let end    = content.find("</script>")?;

    let json: serde_json::Value = serde_json::from_str(&content[..end]).ok()?;

    // Naver Cafe Next.js 구조 여러 경로를 시도
    let article = [
        "/props/pageProps/article",
        "/props/pageProps/initialState/article",
        "/props/pageProps/articleData/article",
        "/props/pageProps/cafearticle",
    ]
    .iter()
    .find_map(|ptr| json.pointer(ptr));

    let article = article?;

    let title = article["subject"].as_str().filter(|s| !s.is_empty()).map(String::from);
    let body  = article["content"]
        .as_str()
        .map(|h| if h.contains('<') { html_to_text(h) } else { h.to_string() })
        .filter(|s| !s.is_empty());
    let author = article
        .pointer("/writer/nick")
        .or_else(|| article.pointer("/memberNickName"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let written_at = article["writeDate"]
        .as_i64()
        .map(ms_to_kst_str)
        .or_else(|| article["writeDateText"].as_str().map(String::from));
    let views = article["readCount"]
        .as_u64()
        .map(|n| n.to_string())
        .or_else(|| article["viewCount"].as_u64().map(|n| n.to_string()));

    if title.is_none() && body.is_none() {
        warn!("__NEXT_DATA__ 발견했지만 article 필드 없음 (구조 불일치)");
        return None;
    }
    info!("SSR __NEXT_DATA__ 파싱 성공: title={:?}", title);
    Some(SsrArticle { title, body, author, written_at, views })
}

/// 정적 HTML에서 DOM 셀렉터로 본문 텍스트 추출 (SSR HTML에 대한 폴백)
fn parse_html_dom(html: &str) -> Option<SsrArticle> {
    use scraper::Selector;
    let doc = Html::parse_document(html);

    let body = [
        ".se-main-container",
        ".article_viewer",
        ".ContentRenderer",
        "#postViewArea",
        ".write_div",
    ]
    .iter()
    .find_map(|sel| {
        let s = Selector::parse(sel).ok()?;
        let el = doc.select(&s).next()?;
        let t = el.text().collect::<Vec<_>>().concat();
        if t.trim().is_empty() { None } else { Some(t.trim().to_string()) }
    });

    body.map(|b| SsrArticle { title: None, body: Some(b), author: None, written_at: None, views: None })
}

/// 쿠키 포함 reqwest Client 빌드
fn build_http_client(cookie_header: &str) -> reqwest::Client {
    let mut headers = HeaderMap::new();
    if !cookie_header.is_empty() {
        if let Ok(v) = HeaderValue::from_str(cookie_header) {
            headers.insert(header::COOKIE, v);
        }
    }
    headers.insert(
        header::USER_AGENT,
        HeaderValue::from_static(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
             AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        ),
    );
    headers.insert(
        header::ACCEPT,
        HeaderValue::from_static("application/json"),
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .unwrap_or_default()
}

// ─────────────────────────────────────────────────────────────────
// 내부 구조체
// ─────────────────────────────────────────────────────────────────

/// 리스트 페이지에서 수집한 게시글 기본 정보
pub struct ArticleRef {
    pub url: Url,
    pub title: String,
    pub date: String,
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

    // reqwest 클라이언트는 Arc로 공유 (내부적으로 connection pool 재사용)
    let cookie_header = build_cookie_header(cookies);
    let http_client = Arc::new(build_http_client(&cookie_header));

    for worker_id in 0..workers {
        let wd = webdriver_url.to_string();
        let queue = queue.clone();
        let cookies = cookies.clone();
        let done = done.clone();
        let http_client = http_client.clone();

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

                let result = scrape_with_driver(&driver, &http_client, article).await;
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
pub async fn collect_article_refs_by_url(
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
            let deadline =
                std::time::Instant::now() + Duration::from_secs(CAFE_LIST_READY_TIMEOUT_SECS);
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
pub async fn scrape_page_rows(driver: &WebDriver, base_url: &Url) -> Vec<ArticleRef> {
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

/// SPA 형식 URL → 구형 ArticleRead.nhn 형식으로 변환
/// /f-e/cafes/{clubid}/articles/{articleid}   → ArticleRead.nhn?clubid=...&articleid=...
/// /ca-fe/cafes/{clubid}/articles/{articleid} → ArticleRead.nhn?clubid=...&articleid=...
fn to_article_read_url(url: &Url) -> Option<Url> {
    if url.host_str() != Some("cafe.naver.com") {
        return None;
    }
    let segs: Vec<&str> = url.path().split('/').filter(|s| !s.is_empty()).collect();
    if segs.len() >= 5
        && (segs[0] == "ca-fe" || segs[0] == "f-e")
        && segs[1] == "cafes"
        && segs[3] == "articles"
        && segs[2].chars().all(|c| c.is_ascii_digit())
        && segs[4].chars().all(|c| c.is_ascii_digit())
    {
        let new_url = format!(
            "https://cafe.naver.com/ArticleRead.nhn?clubid={}&articleid={}",
            segs[2], segs[4]
        );
        return Url::parse(&new_url).ok();
    }
    None
}

/// 기존 세션(driver)을 받아 게시글 1개를 스크랩합니다.
/// 1) reqwest로 Naver Cafe API 직접 호출 시도 (쿠키를 HTTP 헤더로 전송 → 브라우저 정책 우회)
/// 2) API 실패 시 WebDriver DOM 방식으로 폴백
/// 세션 생성/종료는 호출자(Worker Pool)가 담당합니다.
pub async fn scrape_with_driver(
    driver: &WebDriver,
    client: &reqwest::Client,
    article: ArticleRef,
) -> Result<PostData, CrawlError> {
    // ── 1) SSR HTML 파싱 (Next.js __NEXT_DATA__ 포함) ───────────────
    if let Some(ssr) = fetch_article_ssr(client, &article.url).await {
        let body = ssr.body.unwrap_or_default();
        // 본문이 실제로 있을 때만 early return — 없으면 API/DOM 폴백으로 진행
        if !body.is_empty() {
            let title = ssr.title
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| article.title.clone());

            let comments = if let Some((cafe_id, article_id)) = extract_cafe_article_ids(&article.url) {
                let c = fetch_comments_api(client, &cafe_id, &article_id).await;
                if !c.is_empty() { c } else { vec![] }
            } else { vec![] };

            return Ok(PostData {
                source: Source::NaverCafe,
                url: article.url,
                title,
                author: ssr.author.unwrap_or_default(),
                author_level: String::new(),
                written_at: ssr.written_at.unwrap_or_else(|| article.date.clone()),
                views: ssr.views.unwrap_or_default(),
                body,
                body_images: vec![],
                comments,
            });
        }
        warn!("SSR 파싱 성공했지만 본문 없음 → API/DOM 폴백: {}", article.url);
    }

    // ── 2) gw/v4 JSON API ────────────────────────────────────────
    if let Some((cafe_id, article_id)) = extract_cafe_article_ids(&article.url) {
        if let Some(api_art) = fetch_article_api(client, &cafe_id, &article_id).await {
            let body = api_art.content_html.as_deref().map(html_to_text).unwrap_or_default();
            if !body.is_empty() {
                let title = api_art.subject
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| article.title.clone());
                let written_at = api_art.write_date
                    .map(ms_to_kst_str)
                    .unwrap_or_else(|| article.date.clone());
                let comments = fetch_comments_api(client, &cafe_id, &article_id).await;

                info!("JSON API 성공: {} (댓글 {}개)", title, comments.len());
                return Ok(PostData {
                    source: Source::NaverCafe,
                    url: article.url,
                    title,
                    author: api_art.writer.and_then(|w| w.nick).unwrap_or_default(),
                    author_level: String::new(),
                    written_at,
                    views: api_art.read_count.map(|n| n.to_string()).unwrap_or_default(),
                    body,
                    body_images: vec![],
                    comments,
                });
            }
            warn!("JSON API 본문 없음 → WebDriver DOM 폴백: {}", article.url);
        } else {
            warn!("JSON API 실패 → WebDriver DOM 폴백: {}", article.url);
        }
    }

    // ── WebDriver DOM 폴백 ────────────────────────────────────────
    // 이전 게시글에서 iframe에 들어가 있을 수 있으므로 최상위 프레임으로 복귀
    let _ = driver.enter_default_frame().await;

    // SPA URL → 구형 ArticleRead.nhn 으로 변환 (Vue SPA 빈 렌더링 회피)
    let navigate_url = to_article_read_url(&article.url)
        .unwrap_or_else(|| article.url.clone());

    if let Err(e) = driver.goto(navigate_url.as_str()).await {
        return Err(CrawlError::WebDriver(e.to_string()));
    }

    // cafe_main iframe이 DOM에 나타날 때까지 최대 5초 폴링
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
            if std::time::Instant::now() >= deadline { break None; }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };

    if let Some(idx) = iframe_idx {
        let _ = driver.enter_frame(idx).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // 본문 텍스트가 렌더링될 때까지 대기 (최대 10초)
    {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let ready = driver
                .execute(
                    "const spans = document.querySelectorAll('.se-text-paragraph span, .se-module-text span'); \
                     if (Array.from(spans).some(s => s.textContent.trim().length > 0)) return true; \
                     const containers = ['.se-main-container', '.article_viewer .content.CafeViewer', \
                                         '.ArticleContentBox .content_wrapper', '.ContentRenderer']; \
                     for (const sel of containers) { \
                         const el = document.querySelector(sel); \
                         if (el && (el.innerText || el.textContent || '').trim().length > 10) return true; \
                     } \
                     return false;",
                    vec![],
                )
                .await.ok().and_then(|v| v.json().as_bool()).unwrap_or(false);
            if ready || std::time::Instant::now() >= deadline { break; }
            tokio::time::sleep(Duration::from_millis(300)).await;
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
    let js = r#"
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
    "#;

    match driver.execute(js, vec![]).await {
        Ok(v) => {
            let s = v.json().as_str().unwrap_or("").trim().to_string();
            if s.is_empty() {
                warn!("collect_body_text: JS 실행됐지만 빈 문자열 반환");
            }
            s
        }
        Err(e) => { warn!("collect_body_text JS 실패: {e}"); String::new() }
    }
}

async fn collect_body_images(driver: &WebDriver) -> Vec<BodyImage> {
    let img_elems = driver
        .find_all(By::Css(
            ".se-module-image img.se-image-resource",
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
            ".se-module-image a.__se_image_link",
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
    return Array.from(document.querySelectorAll('ul.comment_list li.CommentItem')).map(item => {
        const comment_id = item.getAttribute('id') || '';
        const is_reply = (item.getAttribute('class') || '').includes('CommentItem--reply');
        const author = item.querySelector('.comment_nickname')?.textContent?.trim() || null;
        const date = item.querySelector('.comment_info_date')?.textContent?.trim() || '';
        const raw = item.querySelector('.comment_text_box .text_comment')?.textContent || '';
        const content = raw.replace(/\s+/g, ' ').trim();
        return { comment_id, is_reply, author, date, content };
    }).filter(x => x.content);
    "#;

    let raw: Vec<RawComment> = match driver.execute(js, vec![]).await {
        Ok(v) => {
            match serde_json::from_value(v.json().clone()) {
                Ok(r) => r,
                Err(e) => { warn!("collect_comments JSON 파싱 실패: {e}"); vec![] }
            }
        }
        Err(e) => { warn!("collect_comments JS 실패: {e}"); return vec![]; }
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

pub async fn open_driver(webdriver_url: &str) -> Result<WebDriver, CrawlError> {
    open_driver_with_browser(webdriver_url, BrowserKind::Chrome).await
}

pub async fn open_driver_with_browser(
    webdriver_url: &str,
    browser: BrowserKind,
) -> Result<WebDriver, CrawlError> {
    let caps: Capabilities = match browser {
        BrowserKind::Chrome => chrome_caps()?.into(),
        BrowserKind::Firefox => firefox_caps()?.into(),
    };
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

fn firefox_caps() -> Result<thirtyfour::FirefoxCapabilities, CrawlError> {
    let mut caps = DesiredCapabilities::firefox();
    caps.set_headless()
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.add_arg("--width=1920")
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.add_arg("--height=1080")
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    Ok(caps)
}

fn chrome_caps() -> Result<ChromeCapabilities, CrawlError> {
    let mut caps = ChromeCapabilities::new();
    // --headless=new: 구형 --headless 대체 (더 real-browser에 가깝게 동작)
    caps.add_arg("--headless=new").map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.set_no_sandbox().map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.set_disable_gpu().map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.set_disable_dev_shm_usage().map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.add_arg("--window-size=1920,1080")
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.add_arg("--disable-blink-features=AutomationControlled")
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.add_arg("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.add_experimental_option("excludeSwitches", vec!["enable-automation"])
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    caps.add_experimental_option("useAutomationExtension", false)
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    Ok(caps)
}
