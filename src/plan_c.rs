// ─── plan_c.rs ──────────────────────────────────────────────────────────────
// CDP 기반 무한 스크롤 크롤러 (chromiumoxide)
//
// plan_b(thirtyfour/WebDriver)와 동일한 구조로 구현
// 차이점:
//   - ChromeDriver 서버 불필요 → Chrome에 CDP로 직접 연결
//   - navigator.webdriver 패치를 Page.addScriptToEvaluateOnNewDocument로 처리
//   - 브라우저 1개 + 페이지 N개로 병렬 처리
// ─────────────────────────────────────────────────────────────────────────────

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::cdp::browser_protocol::network::{CookieParam, SetCookiesParams};
use chromiumoxide::cdp::browser_protocol::page::AddScriptToEvaluateOnNewDocumentParams;
use futures::StreamExt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};
use url::Url;

use crate::{
    errors::CrawlError,
    models::{Comment, PostData, Source},
    plan_b::CookieEntry,
};

// ─────────────────────────────────────────────────────────────────
// 설정
// ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ScrollConfig {
    // ── 리스트 페이지 ──────────────────────────────────────────
    pub card_selector: String,
    pub card_link_selector: String,

    // ── 스크롤 동작 ────────────────────────────────────────────
    pub scroll_pause_ms: u64,
    pub bottom_wait_ms: u64,

    // ── 상세 페이지 ────────────────────────────────────────────
    pub detail_title_selector: String,
    pub detail_author_selector: String,
    pub detail_date_selector: String,
    pub detail_body_span_selector: String,
    pub detail_views_selector: String,

    // ── 댓글 ───────────────────────────────────────────────────
    pub detail_comment_block_selector: String,
    pub detail_comment_reply_block_selector: String,
    pub detail_comment_author_selector: String,
    pub detail_comment_text_selector: String,
    pub detail_comment_date_selector: String,
    pub detail_comment_more_selector: String,
    pub detail_comment_pager_selector: String,
}

impl ScrollConfig {
    /// 오늘의집(ohouse.se) 기본값
    pub fn ohouse() -> Self {
        Self {
            card_selector: "article.css-71vdks".to_string(),
            card_link_selector: "a".to_string(),

            scroll_pause_ms: 1_500,
            bottom_wait_ms: 3_000,

            detail_title_selector: "h1".to_string(),
            detail_author_selector: "span.css-1qc0xwe".to_string(),
            detail_date_selector: "span.css-1uy8oy1".to_string(),
            detail_body_span_selector: "span.eey1b4o0".to_string(),
            detail_views_selector: "dd.css-1mbd19c".to_string(),

            // 댓글 관련
            detail_comment_block_selector: "div.css-14q17dg".to_string(),       // 일반 댓글
            detail_comment_reply_block_selector: "div.css-1lc7nmc".to_string(), // 대댓글
            detail_comment_author_selector: "span.e1uf5e1l4".to_string(),
            detail_comment_text_selector: "div.css-l4zhm".to_string(),          // span 말고 컨테이너 전체
            detail_comment_date_selector: "div.css-izoyq9".to_string(),
            detail_comment_more_selector: "button.css-zyciez".to_string(),      // "이전 댓글 n개 더보기"
            detail_comment_pager_selector: "div.css-1vrzskb".to_string(),       // 댓글 페이지네이션
        }
    }
}

// ─────────────────────────────────────────────────────────────────
// 내부 구조체
// ─────────────────────────────────────────────────────────────────

struct ArticleRef {
    url: Url,
}

#[derive(Debug, serde::Deserialize)]
struct RawCommentForJs {
    author: String,
    content: String,
    date: String,
    is_reply: bool,
}

// ─────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────

pub async fn crawl_plan_c_scroll(
    list_url: Url,
    max_posts: usize,
    workers: usize,
    config: Arc<ScrollConfig>,
    cookies: Arc<Vec<CookieEntry>>,
) -> Vec<Result<PostData, CrawlError>> {
    let max_posts = max_posts.max(1);
    let workers = workers.max(1);

    info!(url = %list_url, max_posts, workers, "Plan C (CDP 무한 스크롤) 시작");

    let browser = match launch_browser().await {
        Ok(b) => Arc::new(b),
        Err(e) => return vec![Err(e)],
    };

    // 1단계: 스크롤로 링크 수집
    let refs = collect_refs_by_scroll(&browser, &list_url, max_posts, &config, &cookies).await;
    let total = refs.len();
    info!(total, "링크 수집 완료 → 병렬 상세 스크랩 시작");

    if refs.is_empty() {
        return vec![Err(CrawlError::Parse(
            "게시글 링크를 찾지 못했습니다. card_selector를 확인하세요.".into(),
        ))];
    }

    // 2단계: 병렬 상세 스크랩
    let sem = Arc::new(Semaphore::new(workers));
    let done = Arc::new(AtomicUsize::new(0));
    let mut joinset = JoinSet::new();

    for article_ref in refs {
        let browser = browser.clone();
        let sem = sem.clone();
        let done = done.clone();
        let config = config.clone();
        let cookies = cookies.clone();

        joinset.spawn(async move {
            let _permit = sem.acquire_owned().await;
            let result = scrape_detail(&browser, article_ref, &config, &cookies).await;
            let n = done.fetch_add(1, Ordering::Relaxed) + 1;

            match &result {
                Ok(p) => info!("[{n}/{total}] 완료: {}", p.title),
                Err(e) => warn!("[{n}/{total}] 실패: {e}"),
            }

            result
        });
    }

    let mut out = Vec::new();
    while let Some(res) = joinset.join_next().await {
        match res {
            Ok(r) => out.push(r),
            Err(e) => out.push(Err(CrawlError::Parse(format!("join error: {e}")))),
        }
    }

    out
}

// ─────────────────────────────────────────────────────────────────
// 스크롤 기반 링크 수집
// ─────────────────────────────────────────────────────────────────

async fn collect_refs_by_scroll(
    browser: &Browser,
    list_url: &Url,
    max: usize,
    config: &ScrollConfig,
    cookies: &[CookieEntry],
) -> Vec<ArticleRef> {
    let page = match open_page(browser, list_url.as_str(), cookies).await {
        Ok(p) => p,
        Err(e) => {
            warn!("리스트 페이지 열기 실패: {e}");
            return vec![];
        }
    };

    // 카드 셀렉터가 나타날 때까지 polling (최대 8초)
    {
        let sel = &config.card_selector;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
        loop {
            let ready = page
                .evaluate(format!("!!document.querySelector({:?})", sel).as_str())
                .await
                .ok()
                .and_then(|v| v.into_value::<bool>().ok())
                .unwrap_or(false);
            if ready || tokio::time::Instant::now() >= deadline { break; }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    let href_js = format!(
        "Array.from(document.querySelectorAll('{} {}')).map(a => a.href).filter(Boolean)",
        esc(&config.card_selector),
        esc(&config.card_link_selector),
    );

    let mut seen: HashSet<String> = HashSet::new();
    let mut refs: Vec<ArticleRef> = Vec::new();
    let mut scroll_count = 1u32;

    loop {
        let hrefs: Vec<String> = page
            .evaluate(href_js.as_str())
            .await
            .ok()
            .and_then(|v| v.into_value().ok())
            .unwrap_or_default();

        for href in hrefs {
            if seen.contains(&href) {
                continue;
            }
            seen.insert(href.clone());

            if let Ok(url) = Url::parse(&href) {
                refs.push(ArticleRef { url });
            }

            if refs.len() >= max {
                info!("목표 {max}개 달성 → 수집 종료");
                let _ = page.close().await;
                return refs;
            }
        }

        info!("현재 {}개 링크 수집 중... (스크롤 {scroll_count}회)", refs.len());

        let before_y = eval_f64(&page, "window.scrollY").await;
        let _ = page.evaluate("window.scrollBy(0, window.innerHeight - 100)").await;
        tokio::time::sleep(Duration::from_millis(config.scroll_pause_ms)).await;
        let after_y = eval_f64(&page, "window.scrollY").await;

        if (before_y - after_y).abs() < 1.0 {
            info!("바닥 도달 의심. 추가 대기 중...");
            tokio::time::sleep(Duration::from_millis(config.bottom_wait_ms)).await;
            let after_wait_y = eval_f64(&page, "window.scrollY").await;

            if (before_y - after_wait_y).abs() < 1.0 {
                info!("페이지 맨 아래 도달");
                break;
            }
        }

        scroll_count += 1;
    }

    let _ = page.close().await;
    refs
}

// ─────────────────────────────────────────────────────────────────
// 상세 페이지 스크랩
// ─────────────────────────────────────────────────────────────────

async fn scrape_detail(
    browser: &Browser,
    article: ArticleRef,
    config: &ScrollConfig,
    cookies: &[CookieEntry],
) -> Result<PostData, CrawlError> {
    let page = open_page(browser, article.url.as_str(), cookies).await?;

    // 타이틀 요소가 나타날 때까지 polling (최대 5초)
    {
        let sel = &config.detail_title_selector;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let ready = page
                .evaluate(format!("!!document.querySelector({:?})", sel).as_str())
                .await
                .ok()
                .and_then(|v| v.into_value::<bool>().ok())
                .unwrap_or(false);
            if ready || tokio::time::Instant::now() >= deadline { break; }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    // 댓글 영역 로드 유도
    let _ = page.evaluate("window.scrollTo(0, document.body.scrollHeight)").await;
    // 댓글 블록이 나타날 때까지 polling (최대 3초)
    {
        let sel = &config.detail_comment_block_selector;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let ready = page
                .evaluate(format!("!!document.querySelector({:?})", sel).as_str())
                .await
                .ok()
                .and_then(|v| v.into_value::<bool>().ok())
                .unwrap_or(false);
            if ready || tokio::time::Instant::now() >= deadline { break; }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    let title = js_text_one(&page, &config.detail_title_selector)
        .await
        .unwrap_or_else(|| "제목 없음".to_string());

    let author = js_text_one(&page, &config.detail_author_selector)
        .await
        .unwrap_or_else(|| "작성자 없음".to_string());

    let written_at = js_text_one(&page, &config.detail_date_selector)
        .await
        .unwrap_or_default();

    let body = js_text_all(&page, &config.detail_body_span_selector, "\n").await;

    // 보통 dd 목록 중 조회수가 두 번째
    let views = js_text_nth(&page, &config.detail_views_selector, 1)
        .await
        .unwrap_or_default();

    let comments = collect_comments(&page, config).await;

    let final_url = page
        .evaluate("window.location.href")
        .await
        .ok()
        .and_then(|v| v.into_value::<String>().ok())
        .and_then(|u| Url::parse(&u).ok())
        .unwrap_or(article.url);

    let _ = page.close().await;

    if title == "제목 없음" && body.is_empty() {
        return Err(CrawlError::RequiresJsOrBlocked);
    }

    Ok(PostData {
        source: Source::Unknown,
        url: final_url,
        title,
        author,
        author_level: String::new(),
        written_at,
        views,
        body,
        body_images: vec![],
        comments,
    })
}

// ─────────────────────────────────────────────────────────────────
// 댓글 수집
// ─────────────────────────────────────────────────────────────────

async fn collect_comments(page: &chromiumoxide::Page, config: &ScrollConfig) -> Vec<Comment> {
    let mut out = Vec::new();
    let mut seen = HashSet::<String>::new();

    let mut expected_page = 1usize;
    let mut guard = 0usize;

    loop {
        let _ = page.evaluate("window.scrollTo(0, document.body.scrollHeight)").await;
        tokio::time::sleep(Duration::from_millis(400)).await;

        // 숨겨진 "이전 댓글 n개 더보기" 전부 펼치기
        expand_hidden_reply_comments(page, &config.detail_comment_more_selector).await;
        tokio::time::sleep(Duration::from_millis(300)).await;

        let raw_comments = extract_comments_from_current_page(page, config).await;

        for c in raw_comments {
            let key = format!("{}\u{1F}|{}\u{1F}|{}", c.author, c.date, c.content);
            if !seen.insert(key) {
                continue;
            }

            out.push(Comment {
                comment_id: String::new(),
                is_reply: c.is_reply,
                author: Some(c.author),
                author_level_icon: None,
                author_avatar: None,
                date: c.date,
                content: c.content,
            });
        }

        guard += 1;
        if guard > 100 {
            warn!("댓글 페이지 순회 guard 도달. 종료");
            break;
        }

        expected_page += 1;

        let moved = goto_next_comment_page(
            page,
            &config.detail_comment_pager_selector,
            expected_page,
        ).await;

        if !moved {
            break;
        }

        tokio::time::sleep(Duration::from_millis(600)).await;
    }

    out
}

async fn extract_comments_from_current_page(
    page: &chromiumoxide::Page,
    config: &ScrollConfig,
) -> Vec<RawCommentForJs> {
    let js = format!(
        r#"
(() => {{
    const topSel    = '{top_sel}';
    const replySel  = '{reply_sel}';
    const authorSel = '{author_sel}';
    const textSel   = '{text_sel}';
    const dateSel   = '{date_sel}';

    const blocks = Array.from(
        document.querySelectorAll(`${{topSel}}, ${{replySel}}`)
    );

    return blocks.map(block => {{
        const author = block.querySelector(authorSel)?.innerText?.trim() || '익명';

        const textRoot = block.querySelector(textSel);
        const content = ((textRoot?.innerText || textRoot?.textContent || ''))
            .replace(/\s+/g, ' ')
            .trim();

        const date = block.querySelector(dateSel)?.innerText?.trim() || '';
        const is_reply = block.matches(replySel);

        return {{ author, content, date, is_reply }};
    }}).filter(x => x.content);
}})()
"#,
        top_sel = esc(&config.detail_comment_block_selector),
        reply_sel = esc(&config.detail_comment_reply_block_selector),
        author_sel = esc(&config.detail_comment_author_selector),
        text_sel = esc(&config.detail_comment_text_selector),
        date_sel = esc(&config.detail_comment_date_selector),
    );

    page.evaluate(js.as_str())
        .await
        .ok()
        .and_then(|v| v.into_value::<Vec<RawCommentForJs>>().ok())
        .unwrap_or_default()
}

async fn expand_hidden_reply_comments(
    page: &chromiumoxide::Page,
    more_selector: &str,
) {
    let mut guard = 0usize;

    loop {
        let js = format!(
            r#"
(() => {{
    const btns = Array.from(document.querySelectorAll('{sel}'));
    const target = btns.find(btn => {{
        const t = (btn.innerText || '').trim();
        return !btn.disabled && /이전 댓글|답글|더보기/.test(t);
    }});

    if (!target) return false;
    target.click();
    return true;
}})()
"#,
            sel = esc(more_selector),
        );

        let clicked = page
            .evaluate(js.as_str())
            .await
            .ok()
            .and_then(|v| v.into_value::<bool>().ok())
            .unwrap_or(false);

        if !clicked {
            break;
        }

        guard += 1;
        if guard > 50 {
            warn!("숨김 댓글 펼치기 guard 도달");
            break;
        }

        tokio::time::sleep(Duration::from_millis(350)).await;
    }
}

async fn goto_next_comment_page(
    page: &chromiumoxide::Page,
    pager_selector: &str,
    target_page: usize,
) -> bool {
    let js = format!(
        r#"
(() => {{
    const pager = document.querySelector('{pager}');
    if (!pager) return false;

    const buttons = Array.from(pager.querySelectorAll('button'));
    if (!buttons.length) return false;

    const targetText = String({target});

    // 숫자 버튼에서 target page 찾기
    const pageBtn = buttons.find(btn => {{
        const t = (btn.innerText || '').trim();
        return t === targetText && !btn.disabled;
    }});

    if (pageBtn) {{
        pageBtn.click();
        return true;
    }}

    // target 숫자가 안 보이면 마지막 버튼(오른쪽 화살표) 클릭 시도
    const nextBtn = buttons[buttons.length - 1];
    if (nextBtn && !nextBtn.disabled) {{
        nextBtn.click();
        return true;
    }}

    return false;
}})()
"#,
        pager = esc(pager_selector),
        target = target_page,
    );

    page.evaluate(js.as_str())
        .await
        .ok()
        .and_then(|v| v.into_value::<bool>().ok())
        .unwrap_or(false)
}

// ─────────────────────────────────────────────────────────────────
// JS 헬퍼
// ─────────────────────────────────────────────────────────────────

async fn js_text_one(page: &chromiumoxide::Page, selector: &str) -> Option<String> {
    let js = format!(
        "document.querySelector({:?})?.innerText?.trim() || null",
        selector
    );

    page.evaluate(js.as_str())
        .await
        .ok()
        .and_then(|v| v.into_value::<Option<String>>().ok())
        .flatten()
        .filter(|s| !s.is_empty())
}

async fn js_text_nth(page: &chromiumoxide::Page, selector: &str, n: usize) -> Option<String> {
    let js = format!(
        "document.querySelectorAll({sel:?})[{n}]?.innerText?.trim() || null",
        sel = selector
    );

    page.evaluate(js.as_str())
        .await
        .ok()
        .and_then(|v| v.into_value::<Option<String>>().ok())
        .flatten()
        .filter(|s| !s.is_empty())
}

async fn js_text_all(page: &chromiumoxide::Page, selector: &str, sep: &str) -> String {
    let js = format!(
        "Array.from(document.querySelectorAll({:?})).map(el => el.innerText.trim()).filter(Boolean).join({:?})",
        selector, sep
    );

    page.evaluate(js.as_str())
        .await
        .ok()
        .and_then(|v| v.into_value::<String>().ok())
        .unwrap_or_default()
}

async fn eval_f64(page: &chromiumoxide::Page, js: &str) -> f64 {
    page.evaluate(js)
        .await
        .ok()
        .and_then(|v| v.into_value::<f64>().ok())
        .unwrap_or(0.0)
}

/// CSS selector 안 작은따옴표 escape
fn esc(s: &str) -> String {
    s.replace('\'', "\\'")
}

// ─────────────────────────────────────────────────────────────────
// 브라우저 / 페이지
// ─────────────────────────────────────────────────────────────────

async fn launch_browser() -> Result<Browser, CrawlError> {
    let unique_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    let user_data = std::env::temp_dir().join(format!("chromiumoxide-crawl-{}", unique_id));
    std::fs::create_dir_all(&user_data)
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;

    let config = BrowserConfig::builder()
        .with_head()
        .arg("--headless=new")
        .arg("--no-sandbox")
        .arg("--disable-gpu")
        .arg("--disable-dev-shm-usage")
        .arg("--disable-extensions")
        .arg("--no-first-run")
        .arg("--no-default-browser-check")
        .arg("--window-size=1920,1080")
        .arg("--disable-blink-features=AutomationControlled")
        .arg(format!("--user-data-dir={}", user_data.display()))
        .arg(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
             AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        .build()
        .map_err(CrawlError::WebDriver)?;

    let (browser, mut handler) = Browser::launch(config)
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;

    let cleanup_dir = user_data.clone();
    tokio::spawn(async move {
        while let Some(_) = handler.next().await {}
        let _ = std::fs::remove_dir_all(&cleanup_dir);
    });

    Ok(browser)
}

/// 새 페이지를 열고:
/// 1) navigator.webdriver 패치
/// 2) 쿠키 주입
/// 3) URL 이동
async fn open_page(
    browser: &Browser,
    url: &str,
    cookies: &[CookieEntry],
) -> Result<chromiumoxide::Page, CrawlError> {
    let page = browser
        .new_page("about:blank")
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;

    page.execute(AddScriptToEvaluateOnNewDocumentParams::new(
        "Object.defineProperty(navigator, 'webdriver', { get: () => undefined })",
    ))
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;

    if !cookies.is_empty() {
        let params: Vec<CookieParam> = cookies
            .iter()
            .map(|c| {
                let mut p = CookieParam::new(c.name.clone(), c.value.clone());
                p.url = Some(url.to_string());
                p
            })
            .collect();

        page.execute(SetCookiesParams::new(params))
            .await
            .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    }

    page.goto(url)
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;

    page.wait_for_navigation()
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;

    Ok(page)
}