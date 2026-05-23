/// Plan F — 미가입 네이버 카페 크롤러
///
/// plan_b 로직을 그대로 재사용하되, 게시글 본문 접근 직전에
/// 네이버 검색을 경유해 접근 가능한 URL로 교체하는 것만 추가한다.
///
/// 흐름:
///   1단계) plan_b의 collect_article_refs_by_url 로 목록·URL 수집
///   2단계) Worker Pool —
///            제목으로 네이버 검색 → 원본 게시글 번호로 결과 URL 매칭
///            → 매칭된 URL로 ArticleRef 교체 → plan_b의 scrape_with_driver 호출
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{info, warn};
use url::Url;

use crate::{
    csv_out::{write_comments_csv, write_posts_csv},
    errors::CrawlError,
    models::PostData,
    plan_b::{ArticleRef, collect_article_refs_by_url, open_driver, scrape_with_driver},
};

const PLAN_F_PAGE_LOAD_TIMEOUT_SECS: u64 = 120;

// ─────────────────────────────────────────────────────────────────
// 퍼블릭 진입점
// ─────────────────────────────────────────────────────────────────

pub async fn run(
    webdriver_url: &str,
    cafe_url: &str,
    max_posts: usize,
    workers: usize,
    out_dir: &str,
) -> Result<(), CrawlError> {
    info!("🚀 [Plan F] 미가입 네이버 카페 크롤링 시작");
    info!(" - 카페 URL : {}", cafe_url);
    info!(" - 목표 게시글: {}", max_posts);
    info!(" - 워커 수  : {}", workers);

    tokio::fs::create_dir_all(out_dir).await
        .map_err(|e| CrawlError::Parse(format!("출력 디렉토리 생성 실패: {e}")))?;

    let list_url = Url::parse(cafe_url)
        .map_err(|e| CrawlError::Parse(format!("URL 파싱 실패: {e}")))?;

    // ── 1단계: plan_b 그대로 목록 수집 ───────────────────────────
    info!("🔍 [1단계] 게시글 목록 수집 중...");
    let list_driver = open_plan_f_driver(webdriver_url).await?;
    let refs = collect_article_refs_by_url(&list_driver, &list_url, max_posts).await;
    let _ = list_driver.quit().await;
    info!("  게시글 {}개 수집 완료", refs.len());

    if refs.is_empty() {
        return Err(CrawlError::Parse(
            "게시글 목록을 찾지 못했습니다. URL을 확인하세요.".into(),
        ));
    }

    // ── 2단계: Worker Pool — 검색 경유 후 plan_b 스크랩 ──────────
    info!("⚡ [2단계] {}개 게시글 검색 경유 수집 (워커: {})", refs.len(), workers);
    let total = refs.len();
    let queue: Arc<Mutex<VecDeque<ArticleRef>>> =
        Arc::new(Mutex::new(VecDeque::from(refs)));
    let done  = Arc::new(AtomicUsize::new(0));
    let mut joinset: JoinSet<Vec<Result<PostData, CrawlError>>> = JoinSet::new();

    for worker_id in 0..workers.max(1) {
        let wd    = webdriver_url.to_string();
        let queue = queue.clone();
        let done  = done.clone();

        joinset.spawn(async move {
            let driver = match open_plan_f_driver(&wd).await {
                Ok(d) => d,
                Err(e) => { warn!("워커 {worker_id} 드라이버 실패: {e}"); return vec![]; }
            };
            let client = reqwest::Client::new();
            info!("워커 {worker_id} 준비");
            let mut results = Vec::new();

            loop {
                let article = queue.lock().await.pop_front();
                let Some(article) = article else { break };

                let n = done.fetch_add(1, Ordering::Relaxed) + 1;

                // 검색 경유 URL 교체 후 plan_b 스크랩
                let result = via_search_then_scrape(&driver, &client, article).await;
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

    let mut posts: Vec<PostData> = Vec::new();
    let mut failed = 0usize;
    while let Some(res) = joinset.join_next().await {
        match res {
            Ok(batch) => {
                for r in batch {
                    match r {
                        Ok(p)  => posts.push(p),
                        Err(e) => { warn!("실패: {e}"); failed += 1; }
                    }
                }
            }
            Err(e) => { warn!("join error: {e}"); failed += 1; }
        }
    }

    info!("수집 완료: 성공 {} / 실패 {}", posts.len(), failed);

    let out_path = std::path::Path::new(out_dir);
    write_posts_csv(out_path, &posts)
        .map_err(|e| CrawlError::Parse(format!("posts.csv 저장 실패: {e}")))?;
    write_comments_csv(out_path, &posts)
        .map_err(|e| CrawlError::Parse(format!("comments.csv 저장 실패: {e}")))?;

    info!("🎉 완료! → {}", out_dir);
    Ok(())
}

// ─────────────────────────────────────────────────────────────────
// 핵심: 검색 경유 URL 취득 → plan_b 스크랩
// ─────────────────────────────────────────────────────────────────

/// 1) 제목으로 네이버 카페 검색
/// 2) 원본 게시글 번호(URL 마지막 숫자)로 검색 결과 URL 매칭
/// 3) cru 속성(JWT 포함 URL)으로 ArticleRef를 교체한 뒤 plan_b의 scrape_with_driver 호출
async fn via_search_then_scrape(
    driver: &thirtyfour::WebDriver,
    client: &reqwest::Client,
    article: ArticleRef,
) -> Result<PostData, CrawlError> {
    // 원본 URL에서 게시글 번호 추출 (마지막 경로 세그먼트의 숫자)
    let article_id = article.url.path_segments()
        .and_then(|s| s.last())
        .filter(|s| s.chars().all(|c| c.is_ascii_digit()))
        .map(|s| s.to_string());

    // 네이버 카페 탭 검색
    let encoded    = url_encode(&article.title);
    let search_url = format!(
        "https://search.naver.com/search.naver?ssc=tab.cafe.all&sm=tab_jum&query={}",
        encoded
    );
    driver.goto(&search_url).await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;

    // 카페 검색 결과(div.title_area a.title_link)가 나올 때까지 대기 (최대 15초)
    {
        let dl = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let ready = driver
                .execute(
                    "return document.querySelectorAll('div.title_area a.title_link').length > 0;",
                    vec![],
                )
                .await.ok().and_then(|v| v.json().as_bool()).unwrap_or(false);
            if ready || std::time::Instant::now() >= dl { break; }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    }

    // 진단: 링크 수 및 첫 번째 링크 텍스트 확인
    {
        let diag = driver.execute(r#"
            const links = Array.from(document.querySelectorAll('div.title_area a.title_link'));
            return JSON.stringify({
                count: links.length,
                first_text: links[0] ? links[0].innerText.trim() : null,
                first_href: links[0] ? links[0].getAttribute('href') : null,
                page_title: document.title,
            });
        "#, vec![]).await.ok().and_then(|v| v.json().as_str().map(|s| s.to_string()));
        info!("  [진단] {}", diag.as_deref().unwrap_or("js 실패"));
    }

    // 검색 결과에서 매칭 링크의 href(JWT URL) 추출
    // 1순위: 제목 텍스트 일치 / 2순위: article_id 포함 / 3순위: 첫 번째 title_link
    let find_js = format!(r#"
        const title = '{}';
        const id    = '{}';
        const links = Array.from(document.querySelectorAll('div.title_area a.title_link'));
        if (links.length === 0) return null;

        let matched = links.find(a => a.innerText.trim() === title);

        if (!matched && id) {{
            matched = links.find(a => {{
                const h = a.getAttribute('href') || '';
                return h.includes('/' + id + '?') || h.includes('/' + id + '&') || h.endsWith('/' + id);
            }});
        }}

        if (!matched) matched = links[0];
        return matched ? (matched.getAttribute('href') || null) : null;
    "#,
        article.title.replace('\'', "\\'"),
        article_id.as_deref().unwrap_or("")
    );

    let href = driver.execute(&find_js, vec![]).await
        .ok()
        .and_then(|v| v.json().as_str().map(|s| s.to_string()))
        .filter(|s| !s.is_empty() && s != "null");

    let Some(href) = href else {
        return Err(CrawlError::Parse(
            format!("검색 결과에서 링크를 찾지 못했습니다: {}", article.title)
        ));
    };

    let post_url = Url::parse(&href)
        .map_err(|e| CrawlError::Parse(format!("URL 파싱 실패: {e}")))?;

    info!("  [검색→카페] {}", post_url);

    let accessible_ref = ArticleRef {
        url:   post_url,
        title: article.title,
        date:  article.date,
    };

    scrape_with_driver(driver, client, accessible_ref).await
}

fn url_encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

async fn open_plan_f_driver(webdriver_url: &str) -> Result<thirtyfour::WebDriver, CrawlError> {
    let driver = open_driver(webdriver_url).await?;
    driver
        .set_page_load_timeout(Duration::from_secs(PLAN_F_PAGE_LOAD_TIMEOUT_SECS))
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    Ok(driver)
}
