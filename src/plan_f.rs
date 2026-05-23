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
use std::collections::{HashSet, VecDeque};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Local;
use csv::Writer;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{info, warn};
use url::Url;

use crate::{
    errors::CrawlError,
    models::PostData,
    plan_b::{
        collect_article_refs_by_url, open_driver_with_browser, scrape_page_rows,
        scrape_with_driver, ArticleRef, BrowserKind,
    },
};

const PLAN_F_PAGE_LOAD_TIMEOUT_SECS: u64 = 180;
const PLAN_F_LIST_READY_TIMEOUT_SECS: u64 = 180;

// ─────────────────────────────────────────────────────────────────
// 퍼블릭 진입점
// ─────────────────────────────────────────────────────────────────

pub async fn run(
    webdriver_urls: &[String],
    cafe_url: Option<&str>,
    url_csv: Option<&str>,
    max_posts: usize,
    list_workers: usize,
    max_pages: usize,
    workers: usize,
    out_dir: &str,
    from_row: usize,
    to_row: usize,
    url_only: bool,
    browser: &str,
) -> Result<(), CrawlError> {
    let browser = BrowserKind::parse(browser)?;
    let webdriver_urls = normalize_webdriver_urls(webdriver_urls)?;
    info!("🚀 [Plan F] 미가입 네이버 카페 크롤링 시작");
    if let Some(url_csv) = url_csv {
        info!(" - URL CSV : {}", url_csv);
    } else if let Some(cafe_url) = cafe_url {
        info!(" - 카페 URL : {}", cafe_url);
    }
    info!(" - 목표 게시글: {}", max_posts);
    info!(" - 워커 수  : {}", workers);

    tokio::fs::create_dir_all(out_dir)
        .await
        .map_err(|e| CrawlError::Parse(format!("출력 디렉토리 생성 실패: {e}")))?;

    let from_row = from_row.max(1);
    if to_row > 0 && to_row < from_row {
        return Err(CrawlError::Parse(format!(
            "to-row({to_row}) must be 0 or greater than/equal to from-row({from_row})"
        )));
    }

    let out_path = Path::new(out_dir);
    let refs = if let Some(url_csv) = url_csv {
        let refs = read_article_refs_csv(Path::new(url_csv))?;
        info!("  URL CSV에서 게시글 {}개 로드 완료", refs.len());
        refs
    } else {
        let cafe_url = cafe_url.ok_or_else(|| {
            CrawlError::Parse("--url 또는 --url-csv 중 하나는 필요합니다.".into())
        })?;
        let list_url =
            Url::parse(cafe_url).map_err(|e| CrawlError::Parse(format!("URL 파싱 실패: {e}")))?;

        // ── 1단계: plan_b 그대로 목록 수집 ───────────────────────────
        info!("🔍 [1단계] 게시글 목록 수집 중...");
        let collect_limit = if to_row > 0 {
            max_posts.max(to_row)
        } else {
            max_posts
        };

        let refs = if list_workers.max(1) > 1 || max_pages > 0 {
            collect_article_refs_parallel(
                &webdriver_urls,
                &list_url,
                collect_limit,
                list_workers,
                max_pages,
                browser,
            )
            .await?
        } else {
            let list_driver = open_plan_f_driver(webdriver_endpoint(&webdriver_urls, 0), browser).await?;
            let refs = collect_article_refs_by_url(&list_driver, &list_url, collect_limit).await;
            let _ = list_driver.quit().await;
            refs
        };
        info!("  게시글 {}개 수집 완료", refs.len());
        refs
    };

    if refs.is_empty() {
        return Err(CrawlError::Parse(
            "게시글 목록을 찾지 못했습니다. URL을 확인하세요.".into(),
        ));
    }

    // ── 2단계: Worker Pool — 검색 경유 후 plan_b 스크랩 ──────────
    info!(
        "⚡ [2단계] {}개 게시글 검색 경유 수집 (워커: {})",
        refs.len(),
        workers
    );
    let suffix = output_suffix(from_row, to_row, refs.len());
    let urls_csv = out_path.join(format!("{suffix}_urls.csv"));
    write_article_refs_csv(&urls_csv, &refs)
        .map_err(|e| CrawlError::Parse(format!("urls csv save failed: {e}")))?;

    let Some(refs) = detail_refs_after_url_save(refs, from_row, to_row, url_only)? else {
        info!("URL CSV only mode complete: {}", urls_csv.display());
        return Ok(());
    };

    let total = refs.len();
    let queue: Arc<Mutex<VecDeque<ArticleRef>>> = Arc::new(Mutex::new(VecDeque::from(refs)));
    let done = Arc::new(AtomicUsize::new(0));
    let mut joinset: JoinSet<Vec<Result<PostData, CrawlError>>> = JoinSet::new();

    let detail_workers = effective_worker_count(workers, browser, webdriver_urls.len());
    for worker_id in 0..detail_workers {
        let wd = webdriver_endpoint(&webdriver_urls, worker_id).to_string();
        let queue = queue.clone();
        let done = done.clone();

        joinset.spawn(async move {
            let driver = match open_plan_f_driver(&wd, browser).await {
                Ok(d) => d,
                Err(e) => {
                    warn!("워커 {worker_id} 드라이버 실패: {e}");
                    return vec![];
                }
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
                    Ok(p) => info!("[{n}/{total}] 워커{worker_id} 완료: {}", p.title),
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
                        Ok(p) => posts.push(p),
                        Err(e) => {
                            warn!("실패: {e}");
                            failed += 1;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("join error: {e}");
                failed += 1;
            }
        }
    }

    info!("수집 완료: 성공 {} / 실패 {}", posts.len(), failed);

    let posts_csv = out_path.join(format!("{suffix}_results.csv"));
    let comments_csv = out_path.join(format!("{suffix}_comments.csv"));
    write_posts_csv_named(&posts_csv, &posts)
        .map_err(|e| CrawlError::Parse(format!("posts.csv 저장 실패: {e}")))?;
    write_comments_csv_named(&comments_csv, &posts)
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
fn normalize_webdriver_urls(webdriver_urls: &[String]) -> Result<Vec<String>, CrawlError> {
    let urls = webdriver_urls
        .iter()
        .map(|url| url.trim())
        .filter(|url| !url.is_empty())
        .map(|url| url.to_string())
        .collect::<Vec<_>>();
    if urls.is_empty() {
        return Err(CrawlError::Parse(
            "at least one --webdriver endpoint is required".to_string(),
        ));
    }
    Ok(urls)
}

fn webdriver_endpoint(webdriver_urls: &[String], worker_id: usize) -> &str {
    webdriver_urls[worker_id % webdriver_urls.len()].as_str()
}

fn effective_worker_count(requested: usize, browser: BrowserKind, webdriver_count: usize) -> usize {
    let requested = requested.max(1);
    if browser == BrowserKind::Firefox {
        requested.min(webdriver_count.max(1))
    } else {
        requested
    }
}

async fn collect_article_refs_parallel(
    webdriver_urls: &[String],
    list_url: &Url,
    max_posts: usize,
    list_workers: usize,
    max_pages: usize,
    browser: BrowserKind,
) -> Result<Vec<ArticleRef>, CrawlError> {
    let page_urls = page_urls_for_collection(list_url, max_posts, max_pages);
    if page_urls.is_empty() {
        return Ok(vec![]);
    }

    info!(
        "list page collection start: pages={}, workers={}",
        page_urls.len(),
        effective_worker_count(list_workers, browser, webdriver_urls.len())
    );

    let queue = Arc::new(Mutex::new(page_urls));
    let mut joinset: JoinSet<Vec<(usize, Vec<ArticleRef>)>> = JoinSet::new();

    let list_workers = effective_worker_count(list_workers, browser, webdriver_urls.len());
    for worker_id in 0..list_workers {
        let wd = webdriver_endpoint(webdriver_urls, worker_id).to_string();
        let queue = queue.clone();

        joinset.spawn(async move {
            let driver = match open_plan_f_driver(&wd, browser).await {
                Ok(d) => d,
                Err(e) => {
                    warn!("list worker {worker_id} driver open failed: {e}");
                    return vec![];
                }
            };
            let mut collected = Vec::new();

            loop {
                let job = queue.lock().await.pop_front();
                let Some((page_no, page_url)) = job else {
                    break;
                };

                info!("list worker {worker_id} page {page_no} collect: {page_url}");
                let rows = collect_article_refs_page(&driver, &page_url).await;
                info!(
                    "list worker {worker_id} page {page_no} rows={}",
                    rows.len()
                );
                collected.push((page_no, rows));
            }

            let _ = driver.quit().await;
            collected
        });
    }

    let mut pages = Vec::new();
    while let Some(result) = joinset.join_next().await {
        match result {
            Ok(batch) => pages.extend(batch),
            Err(e) => warn!("list worker join error: {e}"),
        }
    }

    pages.sort_by_key(|(page_no, _)| *page_no);
    let mut seen = HashSet::new();
    let mut refs = Vec::new();
    for (_, rows) in pages {
        for article in rows {
            let key = article.url.as_str().to_string();
            if !seen.insert(key) {
                continue;
            }
            refs.push(article);
            if refs.len() >= max_posts.max(1) {
                return Ok(refs);
            }
        }
    }

    Ok(refs)
}

async fn collect_article_refs_page(
    driver: &thirtyfour::WebDriver,
    page_url: &Url,
) -> Vec<ArticleRef> {
    if let Err(e) = driver.goto(page_url.as_str()).await {
        warn!("list page move failed: {e}");
        return vec![];
    }

    let deadline =
        std::time::Instant::now() + Duration::from_secs(PLAN_F_LIST_READY_TIMEOUT_SECS);
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

    scrape_page_rows(driver, page_url).await
}

fn page_urls_for_collection(
    list_url: &Url,
    max_posts: usize,
    max_pages: usize,
) -> VecDeque<(usize, Url)> {
    let start_page = query_usize(list_url, "page").unwrap_or(1).max(1);
    let page_size = query_usize(list_url, "size").unwrap_or(50).max(1);
    let page_count = if max_pages > 0 {
        max_pages
    } else {
        (max_posts.max(1) + page_size - 1) / page_size
    };

    (0..page_count)
        .map(|idx| {
            let page_no = start_page + idx;
            (page_no, list_page_url(list_url, page_no, page_size))
        })
        .collect()
}

fn query_usize(url: &Url, name: &str) -> Option<usize> {
    url.query_pairs()
        .find_map(|(key, value)| (key == name).then(|| value.parse::<usize>().ok()).flatten())
}

fn list_page_url(base: &Url, page: usize, size: usize) -> Url {
    let mut url = base.clone();
    let mut pairs = url
        .query_pairs()
        .filter(|(key, _)| key != "page" && key != "size")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    pairs.push(("page".to_string(), page.to_string()));
    pairs.push(("size".to_string(), size.to_string()));

    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (key, value) in pairs {
            query.append_pair(&key, &value);
        }
    }
    url
}

async fn via_search_then_scrape(
    driver: &thirtyfour::WebDriver,
    client: &reqwest::Client,
    article: ArticleRef,
) -> Result<PostData, CrawlError> {
    // 원본 URL에서 게시글 번호 추출 (마지막 경로 세그먼트의 숫자)
    let article_id = article
        .url
        .path_segments()
        .and_then(|s| s.last())
        .filter(|s| s.chars().all(|c| c.is_ascii_digit()))
        .map(|s| s.to_string());

    // 네이버 카페 탭 검색
    let encoded = url_encode(&article.title);
    let search_url = format!(
        "https://search.naver.com/search.naver?ssc=tab.cafe.all&sm=tab_jum&query={}",
        encoded
    );
    driver
        .goto(&search_url)
        .await
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
                .await
                .ok()
                .and_then(|v| v.json().as_bool())
                .unwrap_or(false);
            if ready || std::time::Instant::now() >= dl {
                break;
            }
            tokio::time::sleep(Duration::from_millis(400)).await;
        }
    }

    // 진단: 링크 수 및 첫 번째 링크 텍스트 확인
    {
        let diag = driver
            .execute(
                r#"
            const links = Array.from(document.querySelectorAll('div.title_area a.title_link'));
            return JSON.stringify({
                count: links.length,
                first_text: links[0] ? links[0].innerText.trim() : null,
                first_href: links[0] ? links[0].getAttribute('href') : null,
                page_title: document.title,
            });
        "#,
                vec![],
            )
            .await
            .ok()
            .and_then(|v| v.json().as_str().map(|s| s.to_string()));
        info!("  [진단] {}", diag.as_deref().unwrap_or("js 실패"));
    }

    // 검색 결과에서 매칭 링크의 href(JWT URL) 추출
    // 1순위: 제목 텍스트 일치 / 2순위: article_id 포함 / 3순위: 첫 번째 title_link
    let find_js = format!(
        r#"
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

    let href = driver
        .execute(&find_js, vec![])
        .await
        .ok()
        .and_then(|v| v.json().as_str().map(|s| s.to_string()))
        .filter(|s| !s.is_empty() && s != "null");

    let Some(href) = href else {
        return Err(CrawlError::Parse(format!(
            "검색 결과에서 링크를 찾지 못했습니다: {}",
            article.title
        )));
    };

    let post_url =
        Url::parse(&href).map_err(|e| CrawlError::Parse(format!("URL 파싱 실패: {e}")))?;

    info!("  [검색→카페] {}", post_url);

    let accessible_ref = ArticleRef {
        url: post_url,
        title: article.title,
        date: article.date,
    };

    scrape_with_driver(driver, client, accessible_ref).await
}

fn select_article_refs_by_range(
    refs: Vec<ArticleRef>,
    from_row: usize,
    to_row: usize,
) -> Vec<ArticleRef> {
    let start = from_row.max(1);
    refs.into_iter()
        .enumerate()
        .filter_map(|(idx, article)| {
            let row = idx + 1;
            if row < start {
                return None;
            }
            if to_row > 0 && row > to_row {
                return None;
            }
            Some(article)
        })
        .collect()
}

fn detail_refs_after_url_save(
    refs: Vec<ArticleRef>,
    from_row: usize,
    to_row: usize,
    url_only: bool,
) -> Result<Option<Vec<ArticleRef>>, CrawlError> {
    if url_only {
        return Ok(None);
    }

    let refs = select_article_refs_by_range(refs, from_row, to_row);
    if refs.is_empty() {
        return Err(CrawlError::Parse(format!(
            "selected row range is empty: from-row={from_row}, to-row={to_row}"
        )));
    }

    Ok(Some(refs))
}

fn output_suffix(from_row: usize, to_row: usize, collected_rows: usize) -> String {
    let end_row = if to_row == 0 {
        collected_rows
    } else {
        to_row.min(collected_rows)
    };
    let stamp = Local::now().format("%Y%m%d_%H%M%S");
    format!("cafe_open_rows_{from_row:03}-{end_row:03}_{stamp}")
}

fn writer_with_bom(path: &Path) -> Result<Writer<fs::File>, csv::Error> {
    let mut file = fs::File::create(path)?;
    file.write_all(b"\xEF\xBB\xBF")?;
    Ok(Writer::from_writer(file))
}

fn write_article_refs_csv(path: &Path, refs: &[ArticleRef]) -> Result<PathBuf, csv::Error> {
    let mut w = writer_with_bom(path)?;
    w.write_record(["row", "title", "url", "date"])?;
    for (idx, article) in refs.iter().enumerate() {
        w.write_record([
            &(idx + 1).to_string(),
            &article.title,
            article.url.as_str(),
            &article.date,
        ])?;
    }
    w.flush()?;
    Ok(path.to_path_buf())
}

fn read_article_refs_csv(path: &Path) -> Result<Vec<ArticleRef>, CrawlError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| CrawlError::Parse(format!("url csv read failed: {e}")))?;

    let headers = reader
        .headers()
        .map_err(|e| CrawlError::Parse(format!("url csv header read failed: {e}")))?
        .clone();
    let find_header = |name: &str| {
        headers.iter().position(|header| {
            header
                .trim_start_matches('\u{feff}')
                .eq_ignore_ascii_case(name)
        })
    };
    let title_idx = find_header("title").unwrap_or(1);
    let url_idx = find_header("url").unwrap_or(2);
    let date_idx = find_header("date").unwrap_or(3);

    let mut refs = Vec::new();
    for (idx, record) in reader.records().enumerate() {
        let record = record
            .map_err(|e| CrawlError::Parse(format!("url csv row {} read failed: {e}", idx + 2)))?;
        let url_text = record.get(url_idx).unwrap_or("").trim();
        if url_text.is_empty() {
            continue;
        }
        let url = Url::parse(url_text)
            .map_err(|e| CrawlError::Parse(format!("url csv row {} invalid url: {e}", idx + 2)))?;
        refs.push(ArticleRef {
            url,
            title: record.get(title_idx).unwrap_or("").to_string(),
            date: record.get(date_idx).unwrap_or("").to_string(),
        });
    }

    Ok(refs)
}

fn write_posts_csv_named(path: &Path, posts: &[PostData]) -> Result<PathBuf, csv::Error> {
    let mut w = writer_with_bom(path)?;
    w.write_record(["title", "url", "date", "body", "comments"])?;

    for post in posts {
        let comments_joined = post
            .comments
            .iter()
            .map(|comment| {
                let author = comment.author.as_deref().unwrap_or("");
                format!("[{}] {}", author, comment.content)
            })
            .collect::<Vec<_>>()
            .join(" | ");

        w.write_record([
            &post.title,
            post.url.as_str(),
            &post.written_at,
            &post.body,
            &comments_joined,
        ])?;
    }
    w.flush()?;
    Ok(path.to_path_buf())
}

fn write_comments_csv_named(path: &Path, posts: &[PostData]) -> Result<PathBuf, csv::Error> {
    let mut w = writer_with_bom(path)?;
    w.write_record([
        "post_url",
        "comment_id",
        "is_reply",
        "author",
        "date",
        "content",
    ])?;

    for post in posts {
        for comment in &post.comments {
            w.write_record([
                post.url.as_str(),
                &comment.comment_id,
                if comment.is_reply { "true" } else { "false" },
                comment.author.as_deref().unwrap_or(""),
                &comment.date,
                &comment.content,
            ])?;
        }
    }
    w.flush()?;
    Ok(path.to_path_buf())
}

fn url_encode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

async fn open_plan_f_driver(
    webdriver_url: &str,
    browser: BrowserKind,
) -> Result<thirtyfour::WebDriver, CrawlError> {
    let driver = open_driver_with_browser(webdriver_url, browser).await?;
    driver
        .set_page_load_timeout(Duration::from_secs(PLAN_F_PAGE_LOAD_TIMEOUT_SECS))
        .await
        .map_err(|e| CrawlError::WebDriver(e.to_string()))?;
    Ok(driver)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn article(row: usize) -> ArticleRef {
        ArticleRef {
            url: Url::parse(&format!("https://cafe.naver.com/test/{row}")).unwrap(),
            title: format!("title {row}"),
            date: format!("date {row}"),
        }
    }

    #[test]
    fn selects_one_based_row_range_inclusively() {
        let refs = (1..=5).map(article).collect::<Vec<_>>();

        let selected = select_article_refs_by_range(refs, 2, 4);

        assert_eq!(selected.len(), 3);
        assert_eq!(selected[0].title, "title 2");
        assert_eq!(selected[2].title, "title 4");
    }

    #[test]
    fn to_row_zero_selects_to_end() {
        let refs = (1..=3).map(article).collect::<Vec<_>>();

        let selected = select_article_refs_by_range(refs, 2, 0);

        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].title, "title 2");
        assert_eq!(selected[1].title, "title 3");
    }

    #[test]
    fn reads_saved_url_csv() {
        let refs = (1..=2).map(article).collect::<Vec<_>>();
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("plan_f_urls_{}_{}.csv", std::process::id(), stamp));

        write_article_refs_csv(&path, &refs).unwrap();
        let loaded = read_article_refs_csv(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].title, "title 1");
        assert_eq!(loaded[1].url.as_str(), "https://cafe.naver.com/test/2");
    }

    #[test]
    fn url_only_skips_detail_selection() {
        let refs = (1..=3).map(article).collect::<Vec<_>>();

        let selected = detail_refs_after_url_save(refs, 1, 0, true).unwrap();

        assert!(selected.is_none());
    }

    #[test]
    fn builds_query_page_urls_from_fe_menu_url() {
        let url = Url::parse(
            "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50",
        )
        .unwrap();

        let pages = page_urls_for_collection(&url, 120, 0).into_iter().collect::<Vec<_>>();

        assert_eq!(pages.len(), 3);
        assert_eq!(pages[0].0, 1);
        assert_eq!(pages[1].0, 2);
        assert_eq!(
            pages[1].1.as_str(),
            "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=2&size=50"
        );
    }

    #[test]
    fn max_pages_overrides_inferred_list_pages() {
        let url = Url::parse(
            "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=7&size=50",
        )
        .unwrap();

        let pages = page_urls_for_collection(&url, 10, 2).into_iter().collect::<Vec<_>>();

        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].0, 7);
        assert!(pages[1].1.as_str().contains("page=8"));
    }
}
