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
use regex::Regex;
use serde::Deserialize;
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
const NAVER_CAFE_MAX_LIST_PAGE: usize = 1000;

#[derive(Debug, Clone)]
pub struct MenuCommandsConfig {
    pub url: String,
    pub webdriver_urls: Vec<String>,
    pub browser: String,
    pub max_posts: usize,
    pub max_pages: usize,
    pub list_workers: usize,
    pub size: usize,
    pub out_dir: String,
    pub output: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CafeMenu {
    club_id: String,
    menu_id: String,
    title: String,
}

#[derive(Debug, Deserialize)]
struct RawMenuLink {
    href: String,
    text: String,
}

pub async fn generate_menu_commands(cfg: MenuCommandsConfig) -> Result<PathBuf, CrawlError> {
    let browser = BrowserKind::parse(&cfg.browser)?;
    let webdriver_urls = normalize_webdriver_urls(&cfg.webdriver_urls)?;
    tokio::fs::create_dir_all(&cfg.out_dir)
        .await
        .map_err(|e| CrawlError::Parse(format!("output dir create failed: {e}")))?;

    let driver = open_plan_f_driver(webdriver_endpoint(&webdriver_urls, 0), browser).await?;
    let menus = collect_cafe_menus(&driver, &cfg.url).await;
    let _ = driver.quit().await;

    if menus.is_empty() {
        return Err(CrawlError::Parse(
            "cafe menu links were not found in sidebar".to_string(),
        ));
    }

    let script = build_menu_commands_script(&menus, &cfg, &webdriver_urls)?;
    let output = cfg.output.map(PathBuf::from).unwrap_or_else(|| {
        Path::new(&cfg.out_dir).join(format!(
            "cafe_menu_commands_{}.ps1",
            Local::now().format("%Y%m%d_%H%M%S")
        ))
    });
    if let Some(parent) = output.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| CrawlError::Parse(format!("output parent create failed: {e}")))?;
    }
    tokio::fs::write(&output, script)
        .await
        .map_err(|e| CrawlError::Parse(format!("menu command script save failed: {e}")))?;

    info!(
        "cafe menu command script saved menus={} output={}",
        menus.len(),
        output.display()
    );
    Ok(output)
}

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
            let list_driver =
                open_plan_f_driver(webdriver_endpoint(&webdriver_urls, 0), browser).await?;
            let refs = collect_article_refs_by_url(&list_driver, &list_url, collect_limit).await;
            let _ = list_driver.quit().await;
            refs
        };
        info!("  게시글 {}개 수집 완료", refs.len());
        refs
    };

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

    if refs.is_empty() {
        if url_only {
            info!(
                "URL CSV only mode complete with empty list: {}",
                urls_csv.display()
            );
            return Ok(());
        }
        return Err(CrawlError::Parse(
            "게시글 목록을 찾지 못했습니다. URL을 확인하세요.".into(),
        ));
    }

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

async fn collect_cafe_menus(driver: &thirtyfour::WebDriver, start_url: &str) -> Vec<CafeMenu> {
    for candidate_url in cafe_menu_seed_urls(start_url) {
        if let Err(e) = driver.goto(candidate_url.as_str()).await {
            warn!("cafe menu page move failed: {e} url={candidate_url}");
            continue;
        }

        wait_for_cafe_menu_links(driver).await;
        expand_cafe_sidebar_groups(driver).await;

        let links = collect_raw_menu_links(driver).await;
        let menus = parse_cafe_menus(candidate_url.as_str(), links);
        if !menus.is_empty() {
            info!("cafe menu links collected menus={} url={candidate_url}", menus.len());
            return menus;
        }

        let current_url = driver.current_url().await.ok();
        let title = driver.title().await.unwrap_or_default();
        warn!(
            "cafe menu links not found url={} current_url={} title={}",
            candidate_url,
            current_url
                .as_ref()
                .map(|u| u.as_str())
                .unwrap_or("-"),
            title
        );
    }

    vec![]
}

fn cafe_menu_seed_urls(start_url: &str) -> Vec<Url> {
    let Some(base) = Url::parse(start_url).ok() else {
        return vec![];
    };
    let mut seen = HashSet::new();
    let mut urls = Vec::new();
    let mut push = |url: Url| {
        if seen.insert(url.as_str().to_string()) {
            urls.push(url);
        }
    };

    let mut normalized = base.clone();
    let has_page = normalized.query_pairs().any(|(key, _)| key == "page");
    if normalized.path().contains("/menus/") && !has_page {
        append_or_replace_query(&mut normalized, "viewType", "L");
        append_or_replace_query(&mut normalized, "page", "1");
        append_or_replace_query(&mut normalized, "size", "50");
    }
    push(normalized);
    push(base.clone());

    if let Some(club_id) = cafe_id_from_url(&base) {
        for path_prefix in ["f-e", "ca-fe"] {
            if let Ok(mut url) = Url::parse(&format!(
                "https://cafe.naver.com/{path_prefix}/cafes/{club_id}/menus/0"
            )) {
                append_or_replace_query(&mut url, "viewType", "L");
                append_or_replace_query(&mut url, "page", "1");
                append_or_replace_query(&mut url, "size", "50");
                push(url);
            }
        }
    }

    urls
}

fn cafe_id_from_url(url: &Url) -> Option<String> {
    let segments = url
        .path_segments()?
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    for window in segments.windows(2) {
        if matches!(window, ["cafes", cafe_id] if cafe_id.chars().all(|c| c.is_ascii_digit())) {
            return Some(window[1].to_string());
        }
    }

    url.query_pairs().find_map(|(key, value)| {
        (key == "clubid" || key == "search.clubid" || key == "cafeId").then(|| value.into_owned())
    })
}

fn append_or_replace_query(url: &mut Url, name: &str, value: &str) {
    let mut pairs = url
        .query_pairs()
        .filter(|(key, _)| key != name)
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    pairs.push((name.to_string(), value.to_string()));

    url.set_query(None);
    {
        let mut query = url.query_pairs_mut();
        for (key, value) in pairs {
            query.append_pair(&key, &value);
        }
    }
}

async fn wait_for_cafe_menu_links(driver: &thirtyfour::WebDriver) {
    let deadline = std::time::Instant::now() + Duration::from_secs(PLAN_F_LIST_READY_TIMEOUT_SECS);
    loop {
        let ready = driver
            .execute(
                r#"
                return document.querySelectorAll(
                    'a[href*="/menus/"], a[href*="search.menuid="], aside a[href]'
                ).length > 0;
                "#,
                vec![],
            )
            .await
            .ok()
            .and_then(|v| v.json().as_bool())
            .unwrap_or(false);
        if ready || std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

async fn expand_cafe_sidebar_groups(driver: &thirtyfour::WebDriver) {
    for _ in 0..3 {
        let clicked = driver
            .execute(
                r#"
                const buttons = Array.from(document.querySelectorAll('button[class*="Sidebar_btn_group"], aside button'));
                let clicked = 0;
                for (const button of buttons) {
                    const expanded = button.getAttribute('aria-expanded');
                    const text = (button.innerText || button.textContent || '').trim();
                    if (expanded === 'false' || (expanded === null && /그룹펴기|펼치기/.test(text))) {
                        button.click();
                        clicked += 1;
                    }
                }
                return clicked;
                "#,
                vec![],
            )
            .await
            .ok()
            .and_then(|v| v.json().as_u64())
            .unwrap_or(0);
        if clicked == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn collect_raw_menu_links(driver: &thirtyfour::WebDriver) -> Vec<RawMenuLink> {
    let Some(json) = driver
        .execute(
            r#"
            const anchors = Array.from(document.querySelectorAll('a[href]'));
            const menuPattern = /\/(?:f-e|ca-fe)\/cafes\/\d+\/menus\/\d+|search\.menuid=|ArticleList\.nhn/;
            const links = anchors
                .filter(a => menuPattern.test(a.getAttribute('href') || ''))
                .map(a => ({
                    href: a.getAttribute('href') || '',
                    text: (a.innerText || a.textContent || '').replace(/\s+/g, ' ').trim(),
                }));
            return JSON.stringify(links);
            "#,
            vec![],
        )
        .await
        .ok()
        .and_then(|v| v.json().as_str().map(|s| s.to_string()))
    else {
        return vec![];
    };

    serde_json::from_str::<Vec<RawMenuLink>>(&json).unwrap_or_default()
}

fn parse_cafe_menus(base_url: &str, links: Vec<RawMenuLink>) -> Vec<CafeMenu> {
    let menu_path_re =
        Regex::new(r"/(?:f-e|ca-fe)/cafes/(\d+)/menus/(\d+)").expect("valid menu regex");
    let base = Url::parse(base_url).ok();
    let mut seen = HashSet::new();
    let mut menus = Vec::new();

    for link in links {
        let title = clean_menu_title(&link.text);
        let absolute = if let Ok(url) = Url::parse(&link.href) {
            Some(url)
        } else {
            base.as_ref().and_then(|base| base.join(&link.href).ok())
        };

        let parsed = if let Some(caps) = menu_path_re.captures(&link.href) {
            Some((caps[1].to_string(), caps[2].to_string()))
        } else if let Some(url) = absolute {
            let club_id = url
                .query_pairs()
                .find_map(|(k, v)| (k == "search.clubid").then(|| v.into_owned()));
            let menu_id = url
                .query_pairs()
                .find_map(|(k, v)| (k == "search.menuid").then(|| v.into_owned()));
            club_id.zip(menu_id)
        } else {
            None
        };

        let Some((club_id, menu_id)) = parsed else {
            continue;
        };
        if !seen.insert((club_id.clone(), menu_id.clone())) {
            continue;
        }
        menus.push(CafeMenu {
            club_id,
            menu_id,
            title,
        });
    }

    menus
}

fn clean_menu_title(text: &str) -> String {
    let mut title = text
        .replace("\u{c0c8} \u{ac8c}\u{c2dc}\u{ae00}", "")
        .replace("\u{adf8}\u{b8f9}\u{d3b4}\u{ae30}", "")
        .replace('\u{feff}', "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if title.is_empty() {
        title = "untitled".to_string();
    }
    title
}

fn build_menu_commands_script(
    menus: &[CafeMenu],
    cfg: &MenuCommandsConfig,
    webdriver_urls: &[String],
) -> Result<String, CrawlError> {
    let mut script = String::new();
    script.push_str("# Generated by naver_crawler_engine cafe-menu-commands\n");
    script.push_str("$ErrorActionPreference = \"Stop\"\n\n");
    script.push_str(&format!(
        "$RootOutDir = {}\n",
        ps_single_quote(&cfg.out_dir)
    ));
    script.push_str("$RunStamp = Get-Date -Format \"yyyyMMdd_HHmmss\"\n");
    script.push_str("$RunOutDir = Join-Path $RootOutDir (\"cafe_menu_url_parts_\" + $RunStamp)\n");
    script.push_str(
        "$FinalCsv = Join-Path $RootOutDir (\"cafe_menu_urls_deduped_\" + $RunStamp + \".csv\")\n",
    );
    script.push_str("New-Item -ItemType Directory -Force -Path $RootOutDir | Out-Null\n");
    script.push_str("New-Item -ItemType Directory -Force -Path $RunOutDir | Out-Null\n\n");
    script.push_str("function Invoke-CafeOpenCommand {\n");
    script.push_str("    param([scriptblock]$Command, [string]$MenuName)\n");
    script.push_str("    & $Command\n");
    script.push_str("    if ($LASTEXITCODE -ne 0) { throw \"cafe-open failed: $MenuName\" }\n");
    script.push_str("}\n\n");

    let browser = BrowserKind::parse(&cfg.browser)?;
    for menu in menus {
        script.push_str(&format!(
            "# {} (club {}, menu {})\n",
            menu.title, menu.club_id, menu.menu_id
        ));
        let url = format!(
            "https://cafe.naver.com/f-e/cafes/{}/menus/{}?page=1&size={}",
            menu.club_id,
            menu.menu_id,
            cfg.size.max(1)
        );
        script.push_str(&format!(
            "Invoke-CafeOpenCommand -MenuName {} -Command {{\n",
            ps_single_quote(&safe_menu_name(menu))
        ));
        script.push_str("    cargo run --bin naver_crawler_engine -- cafe-open");
        script.push_str(&format!(" --url {}", ps_quote(&url)));
        if cfg.max_pages > 0 {
            script.push_str(&format!(" --max-pages {}", cfg.max_pages));
        } else {
            script.push_str(&format!(" --max-posts {}", cfg.max_posts.max(1)));
        }
        script.push_str(&format!(" --list-workers {}", cfg.list_workers.max(1)));
        script.push_str(" --url-only");
        if browser != BrowserKind::Chrome {
            script.push_str(&format!(" --browser {}", cfg.browser));
        }
        for webdriver_url in webdriver_urls {
            script.push_str(&format!(" --webdriver {}", ps_quote(webdriver_url)));
        }
        script.push_str(" --out-dir $RunOutDir");
        script.push_str("\n}\n\n");
    }

    script.push_str("$Rows = New-Object 'System.Collections.Generic.List[object]'\n");
    script.push_str("$Seen = @{}\n");
    script.push_str("Get-ChildItem -Path $RunOutDir -Filter '*_urls.csv' | Sort-Object Name | ForEach-Object {\n");
    script.push_str("    Import-Csv -Path $_.FullName | ForEach-Object {\n");
    script.push_str("        $UrlValue = $_.url\n");
    script.push_str("        if (![string]::IsNullOrWhiteSpace($UrlValue) -and !$Seen.ContainsKey($UrlValue)) {\n");
    script.push_str("            $Seen[$UrlValue] = $true\n");
    script.push_str("            $Rows.Add([pscustomobject]@{\n");
    script.push_str("                row = ($Rows.Count + 1)\n");
    script.push_str("                title = $_.title\n");
    script.push_str("                url = $UrlValue\n");
    script.push_str("                date = $_.date\n");
    script.push_str("            }) | Out-Null\n");
    script.push_str("        }\n");
    script.push_str("    }\n");
    script.push_str("}\n");
    script.push_str("if ($Rows.Count -gt 0) {\n");
    script.push_str("    $Rows | Export-Csv -Path $FinalCsv -NoTypeInformation -Encoding UTF8\n");
    script.push_str("} else {\n");
    script.push_str("    'row,title,url,date' | Set-Content -Path $FinalCsv -Encoding UTF8\n");
    script.push_str("}\n");
    script.push_str("Write-Host (\"Final URL CSV: {0} ({1} rows)\" -f $FinalCsv, $Rows.Count)\n");

    Ok(script)
}

fn ps_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "`\""))
}

fn ps_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn safe_menu_name(menu: &CafeMenu) -> String {
    format!("club{}_menu{}", menu.club_id, menu.menu_id)
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
    let stop_at_page = Arc::new(AtomicUsize::new(usize::MAX));
    let mut joinset: JoinSet<Vec<(usize, Vec<ArticleRef>)>> = JoinSet::new();

    let list_workers = effective_worker_count(list_workers, browser, webdriver_urls.len());
    for worker_id in 0..list_workers {
        let wd = webdriver_endpoint(webdriver_urls, worker_id).to_string();
        let queue = queue.clone();
        let stop_at_page = stop_at_page.clone();

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
                let stop_at = stop_at_page.load(Ordering::Relaxed);
                if should_skip_page_after_empty_stop(page_no, stop_at) {
                    info!(
                        "list worker {worker_id} page {page_no} skip: empty page already found at {stop_at}"
                    );
                    break;
                }

                info!("list worker {worker_id} page {page_no} collect: {page_url}");
                let rows = collect_article_refs_page(&driver, &page_url).await;
                info!("list worker {worker_id} page {page_no} rows={}", rows.len());
                if rows.is_empty() {
                    if register_empty_page_stop(&stop_at_page, page_no) {
                        info!(
                            "list worker {worker_id} page {page_no} empty: stop remaining list pages"
                        );
                    }
                    collected.push((page_no, rows));
                    break;
                }
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

fn should_skip_page_after_empty_stop(page_no: usize, stop_at_page: usize) -> bool {
    page_no >= stop_at_page
}

fn register_empty_page_stop(stop_at_page: &AtomicUsize, page_no: usize) -> bool {
    page_no < stop_at_page.fetch_min(page_no, Ordering::Relaxed)
}

async fn collect_article_refs_page(
    driver: &thirtyfour::WebDriver,
    page_url: &Url,
) -> Vec<ArticleRef> {
    if let Err(e) = driver.goto(page_url.as_str()).await {
        warn!("list page move failed: {e}");
        return vec![];
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(PLAN_F_LIST_READY_TIMEOUT_SECS);
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
    let requested_page_count = if max_pages > 0 {
        max_pages
    } else {
        (max_posts.max(1) + page_size - 1) / page_size
    };
    let page_count = if start_page > NAVER_CAFE_MAX_LIST_PAGE {
        0
    } else {
        requested_page_count.min(NAVER_CAFE_MAX_LIST_PAGE - start_page + 1)
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
    fn writes_empty_url_csv() {
        let path = std::env::temp_dir().join(format!(
            "crawler_rs_empty_refs_{}.csv",
            std::process::id()
        ));

        write_article_refs_csv(&path, &[]).unwrap();
        let loaded = read_article_refs_csv(&path).unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(loaded.is_empty());
    }

    #[test]
    fn builds_query_page_urls_from_fe_menu_url() {
        let url = Url::parse(
            "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1&size=50",
        )
        .unwrap();

        let pages = page_urls_for_collection(&url, 120, 0)
            .into_iter()
            .collect::<Vec<_>>();

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

        let pages = page_urls_for_collection(&url, 10, 2)
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].0, 7);
        assert!(pages[1].1.as_str().contains("page=8"));
    }

    #[test]
    fn page_urls_stop_at_naver_cafe_page_limit() {
        let url = Url::parse(
            "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=995&size=50",
        )
        .unwrap();

        let pages = page_urls_for_collection(&url, 1000, 10)
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(pages.len(), 6);
        assert_eq!(pages[0].0, 995);
        assert_eq!(pages[5].0, 1000);
    }

    #[test]
    fn page_urls_are_empty_after_naver_cafe_page_limit() {
        let url = Url::parse(
            "https://cafe.naver.com/f-e/cafes/17902534/menus/0?viewType=L&page=1001&size=50",
        )
        .unwrap();

        let pages = page_urls_for_collection(&url, 1000, 10);

        assert!(pages.is_empty());
    }

    #[test]
    fn empty_page_stop_skips_later_pages() {
        let stop_at = AtomicUsize::new(usize::MAX);

        assert!(register_empty_page_stop(&stop_at, 65));
        assert!(!register_empty_page_stop(&stop_at, 80));
        assert!(!should_skip_page_after_empty_stop(64, stop_at.load(Ordering::Relaxed)));
        assert!(should_skip_page_after_empty_stop(65, stop_at.load(Ordering::Relaxed)));
        assert!(should_skip_page_after_empty_stop(1000, stop_at.load(Ordering::Relaxed)));
    }

    #[test]
    fn parses_sidebar_menu_links_and_memo_links() {
        let links = vec![
            RawMenuLink {
                href: "/f-e/cafes/17902534/menus/314".to_string(),
                text: "발달장애 Q&A 새 게시글".to_string(),
            },
            RawMenuLink {
                href: "https://cafe.naver.com/MemoList.nhn?search.clubid=17902534&search.menuid=350&viewType=pc"
                    .to_string(),
                text: "새멤버환영해요".to_string(),
            },
            RawMenuLink {
                href: "/f-e/cafes/17902534/menus/314".to_string(),
                text: "duplicate".to_string(),
            },
        ];

        let menus = parse_cafe_menus("https://cafe.naver.com/f-e/cafes/17902534/menus/0", links);

        assert_eq!(menus.len(), 2);
        assert_eq!(menus[0].menu_id, "314");
        assert_eq!(menus[0].title, "발달장애 Q&A");
        assert_eq!(menus[1].menu_id, "350");
    }

    #[test]
    fn builds_menu_command_script_with_page_query() {
        let menus = vec![CafeMenu {
            club_id: "17902534".to_string(),
            menu_id: "314".to_string(),
            title: "발달장애 Q&A".to_string(),
        }];
        let cfg = MenuCommandsConfig {
            url: "https://cafe.naver.com/f-e/cafes/17902534/menus/0".to_string(),
            webdriver_urls: vec!["http://localhost:4444".to_string()],
            browser: "chrome".to_string(),
            max_posts: 50000,
            max_pages: 0,
            list_workers: 10,
            size: 50,
            out_dir: "out".to_string(),
            output: None,
        };

        let script =
            build_menu_commands_script(&menus, &cfg, &["http://localhost:4444".to_string()])
                .unwrap();

        assert!(
            script.contains("https://cafe.naver.com/f-e/cafes/17902534/menus/314?page=1&size=50")
        );
        assert!(script.contains("--max-posts 50000"));
        assert!(script.contains("--list-workers 10"));
        assert!(script.contains("--url-only"));
        assert!(script.contains("--out-dir $RunOutDir"));
        assert!(script.contains("cafe_menu_urls_deduped_"));
        assert!(script.contains("Export-Csv -Path $FinalCsv"));
        assert!(script.contains("Invoke-CafeOpenCommand -MenuName 'club17902534_menu314'"));
    }
}
