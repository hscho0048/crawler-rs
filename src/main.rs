mod csv_out;
mod engine;
mod errors;
mod input;
mod merge;
mod models;
mod plan_a;
mod plan_b;
mod plan_c;
mod plan_d;
mod plan_e;
mod plan_g;
mod plan_h;
mod plan_i;
mod plan_j;
mod plan_k;
mod plan_m;
mod plan_n;
mod plan_o;
mod test_mode;
mod plan_f;

use std::path::Path;

use clap::{Parser, Subcommand};
use tracing::{info, warn};
use tracing_subscriber::{fmt, EnvFilter};
use url::Url;

use std::sync::Arc;

use crate::{
    csv_out::{ensure_out_dir, write_comments_csv, write_posts_csv},
    engine::{crawl_all_with_fallback, CrawlOptions},
    errors::CrawlError,
    input::read_urls_from_file,
    plan_b::{crawl_plan_b_from_list, load_cookies, CookieEntry},
    plan_c::{crawl_plan_c_scroll, ScrollConfig},
    plan_e::{run_plan_e_parallel, write_csv as write_reviews_csv},
    test_mode::{run_smoke_test, TestOptions},
};

#[derive(Debug, Parser)]
#[command(name = "naver_crawler_engine")]
#[command(about = "Plan A (HTTP) + Plan B (WebDriver) crawler skeleton with CSV output", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 무한 스크롤 피드형 사이트 크롤링 (오늘의 집 등) — ChromeDriver 불필요
    Scroll {
        /// 피드 목록 URL
        #[arg(long)]
        url: String,

        /// 수집할 최대 게시글 수
        #[arg(long, default_value_t = 20)]
        max_posts: usize,

        /// 동시 탭(페이지) 수
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// 결과 저장 디렉토리
        #[arg(long, default_value = "out")]
        out_dir: String,

        /// 로그인 쿠키 파일 (JSON 배열)
        #[arg(long)]
        cookie_file: Option<String>,

        /// 게시글 카드 CSS 셀렉터 (기본: ohouse 값)
        #[arg(long, default_value = "article.css-71vdks")]
        card_selector: String,

        /// 카드 내 링크 셀렉터 (기본: a)
        #[arg(long, default_value = "a")]
        link_selector: String,

        /// 스크롤 후 대기 시간 ms
        #[arg(long, default_value_t = 1500)]
        scroll_pause: u64,
    },

    /// Quick preflight check (recommended before crawl)
    Test {
        /// A single URL to smoke-test
        #[arg(long)]
        url: String,

        /// Output directory (created if missing)
        #[arg(long, default_value = "out")]
        out_dir: String,

        /// Optional webdriver endpoint (e.g. http://localhost:4444)
        #[arg(long)]
        webdriver: Option<String>,
    },

    /// 가입 네이버 카페 크롤링 — 로그인 쿠키 + ChromeDriver 필요 (Plan B)
    Cafe {
        /// 카페 게시판 URL (예: https://cafe.naver.com/cafename/board)
        #[arg(long)]
        url: String,

        /// 수집할 최대 게시글 수
        #[arg(long, default_value_t = 20)]
        max_posts: usize,

        /// 동시에 열 Chrome 세션 수 (병렬 처리)
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// WebDriver 엔드포인트 (예: http://localhost:4444)
        #[arg(long)]
        webdriver: String,

        /// 결과 저장 디렉토리
        #[arg(long, default_value = "out")]
        out_dir: String,

        /// 네이버 로그인 쿠키 파일 (JSON 배열, 예: cookies.json)
        /// 형식: [{"name": "NID_AUT", "value": "..."}, ...]
        #[arg(long)]
        cookie_file: Option<String>,
    },

    /// DC인사이드 등 페이지네이션 게시판 크롤링 (CDP, ChromeDriver 불필요)
    Scrape {
        /// 게시판 목록 URL (예: https://gall.dcinside.com/board/lists/?id=toeic)
        #[arg(long)]
        url: String,

        /// 수집할 최대 게시글 수
        #[arg(long, default_value_t = 100)]
        max_posts: usize,

        /// 병렬 워커 수 (디시는 IP 차단이 심하므로 2~3 권장)
        #[arg(long, default_value_t = 2)]
        workers: usize,

        /// 결과 저장 디렉토리
        #[arg(long, default_value = "out")]
        out_dir: String,
    },

    /// 스마트스토어 상품 리뷰 병렬 수집 (ChromeDriver 필요)
    Smartstore {
        /// 상품 URL (반복 사용 가능, 예: --url "https://smartstore.naver.com/...")
        #[arg(long)]
        url: Vec<String>,

        /// 상품 URL 목록 파일 (한 줄에 URL 하나)
        #[arg(long)]
        input: Option<String>,

        /// 병렬 Chrome 세션 수
        #[arg(long, default_value_t = 2)]
        workers: usize,

        /// WebDriver 엔드포인트 (예: http://localhost:4444)
        #[arg(long)]
        webdriver: String,

        /// 결과 CSV 저장 경로
        #[arg(long, default_value = "out/smartstore_reviews.csv")]
        output: String,

        /// 헤드리스 모드 (화면 없이 실행)
        #[arg(long, default_value_t = false)]
        headless: bool,
    },

    /// Threads.com 키워드 크롤링 — 로그인 후 검색·댓글 병렬 수집 (Plan I)
    /// Coupang product review crawler (parallel by review page, Plan O)
    #[command(about = "Coupang product review crawler (parallel by review page, Plan O)")]
    Coupang {
        /// Product URL (repeatable)
        #[arg(long)]
        url: Vec<String>,

        /// Product URL list file, one URL per line
        #[arg(long)]
        input: Option<String>,

        /// First review page to crawl
        #[arg(long, default_value_t = 1)]
        start_page: usize,

        /// Number of review pages to crawl per product
        #[arg(long, default_value_t = 10)]
        max_pages: usize,

        /// Parallel Chrome sessions
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// WebDriver endpoint for --browser-fetch mode
        #[arg(long, default_value = "http://localhost:4444")]
        webdriver: String,

        /// Headless Chrome mode for --browser-fetch mode
        #[arg(long, default_value_t = false)]
        headless: bool,

        /// Output CSV path
        #[arg(long, default_value = "out/coupang_reviews.csv")]
        output: String,

        /// Cookie header copied from an allowed browser session
        #[arg(long)]
        cookie: Option<String>,

        /// File containing a raw Cookie header or a JSON cookie array
        #[arg(long)]
        cookie_file: Option<String>,

        /// Delay between page requests per worker
        #[arg(long, default_value_t = 1000)]
        page_delay_ms: u64,

        /// Use Chrome to call the review API with browser cookies
        #[arg(long)]
        browser_fetch: bool,
    },

    ItdaCommunity {
        #[arg(long, default_value_t = 1)]
        start_page: usize,

        #[arg(long, default_value_t = 43)]
        max_pages: usize,

        #[arg(long, default_value_t = 0)]
        max_posts: usize,

        #[arg(long, default_value_t = 3)]
        workers: usize,

        #[arg(long, default_value = "http://localhost:4444")]
        webdriver: String,

        #[arg(long, default_value = "out")]
        out_dir: String,

        #[arg(long, default_value_t = true)]
        headless: bool,

        #[arg(long, default_value = "target/itda_login_profile")]
        profile_dir: String,
    },

    /// Naver search result crawler for Naver Blog and Tistory URLs (Plan N)
    NaverSearch {
        /// Full Naver search URL
        #[arg(long)]
        url: String,

        /// Max search API pages / DOM scroll rounds
        #[arg(long, default_value_t = 30)]
        max_scrolls: usize,

        /// Max detail URLs to crawl (0 = no limit)
        #[arg(long, default_value_t = 0)]
        max_posts: usize,

        /// Parallel Chrome sessions for detail pages
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// WebDriver endpoint
        #[arg(long, default_value = "http://localhost:4444")]
        webdriver: String,

        /// Output directory
        #[arg(long, default_value = "out")]
        out_dir: String,

        /// Headless Chrome mode
        #[arg(long, default_value_t = false)]
        headless: bool,

        /// Max clicks/pages for loading comments
        #[arg(long, default_value_t = 50)]
        comment_page_limit: usize,
    },

    Threads {
        /// 검색 키워드
        #[arg(long)]
        keyword: String,

        /// 수집할 최대 게시글 수
        #[arg(long, default_value_t = 30)]
        max_posts: usize,

        /// 병렬 워커 수 (Chrome 세션 수)
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// WebDriver 엔드포인트
        #[arg(long, default_value = "http://localhost:9515")]
        webdriver: String,

        /// 결과 저장 디렉토리
        #[arg(long, default_value = "out")]
        out_dir: String,

        /// 댓글 스크롤 최대 횟수
        #[arg(long, default_value_t = 10)]
        comment_scroll_rounds: usize,

        /// 댓글 스크롤 간격 (초)
        #[arg(long, default_value_t = 1)]
        comment_pause_secs: u64,
    },

    /// 네이버 블로그 검색 크롤링 — 검색어+기간으로 수집 (Plan H)
    BlogSearch {
        /// 검색 키워드
        #[arg(long)]
        query: String,

        /// 검색 시작일 (YYYY-MM-DD)
        #[arg(long)]
        start_date: String,

        /// 검색 종료일 (YYYY-MM-DD)
        #[arg(long)]
        end_date: String,

        /// 수집할 최대 게시글 수
        #[arg(long, default_value_t = 30)]
        max_posts: usize,

        /// 병렬 워커 수 (Chrome 세션 수)
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// WebDriver 엔드포인트
        #[arg(long, default_value = "http://localhost:9515")]
        webdriver: String,

        /// 헤드리스 모드 (기본 활성화, 비활성화하려면 --headless false)
        #[arg(long, default_value_t = true)]
        headless: bool,

        /// 결과 저장 디렉토리
        #[arg(long, default_value = "out")]
        out_dir: String,

        /// 검색 결과 스크롤 횟수
        #[arg(long, default_value_t = 30)]
        search_max_scrolls: usize,

        /// 게시글 상세 스크롤 횟수
        #[arg(long, default_value_t = 12)]
        detail_max_scrolls: usize,
    },

    /// 미가입 네이버 카페 크롤링 — 네이버 검색 경유 (Plan F)
    CafeOpen {
        /// 카페 게시판 URL (예: https://cafe.naver.com/cafename/board)
        #[arg(long)]
        url: Option<String>,

        /// URL CSV saved by a previous cafe-open run
        #[arg(long)]
        url_csv: Option<String>,

        /// Parallel Chrome sessions for collecting list pages
        #[arg(long = "list-workers", default_value_t = 1)]
        list_workers: usize,

        /// Number of list pages to collect by changing the page query (0 = infer from max-posts and size)
        #[arg(long = "max-pages", default_value_t = 0)]
        max_pages: usize,

        /// 수집할 최대 게시글 수
        #[arg(long, default_value_t = 20)]
        max_posts: usize,

        /// 동시에 열 Chrome 세션 수 (병렬 처리)
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// WebDriver endpoint. Repeat for a GeckoDriver pool.
        #[arg(long, required = true)]
        webdriver: Vec<String>,

        /// 결과 저장 디렉토리
        #[arg(long, default_value = "out")]
        out_dir: String,

        /// WebDriver browser: chrome or firefox
        #[arg(long, default_value = "chrome")]
        browser: String,

        /// URL list row to start detail crawling from (1-based)
        #[arg(long = "from-row", default_value = "1")]
        from_row: usize,

        /// URL list row to stop detail crawling at (0 = last collected row)
        #[arg(long = "to-row", default_value = "0")]
        to_row: usize,

        /// Save only the collected URL CSV and skip detail crawling
        #[arg(long)]
        url_only: bool,
    },

    /// Generate cafe-open commands from a Naver Cafe sidebar menu list
    CafeMenuCommands {
        /// Cafe page URL that contains the sidebar
        #[arg(long)]
        url: String,

        /// WebDriver endpoint for opening the cafe page. Repeat to generate pooled commands.
        #[arg(long, required = true)]
        webdriver: Vec<String>,

        /// WebDriver browser: chrome or firefox
        #[arg(long, default_value = "chrome")]
        browser: String,

        /// Max posts to put in each generated cafe-open command
        #[arg(long, default_value_t = 50000)]
        max_posts: usize,

        /// Max pages to put in each generated cafe-open command (0 = use max-posts)
        #[arg(long = "max-pages", default_value_t = 0)]
        max_pages: usize,

        /// List workers to put in each generated cafe-open command
        #[arg(long = "list-workers", default_value_t = 10)]
        list_workers: usize,

        /// Page size query value to put in generated menu URLs
        #[arg(long, default_value_t = 50)]
        size: usize,

        /// Output directory for generated script and crawler outputs
        #[arg(long, default_value = "out")]
        out_dir: String,

        /// Output ps1 file path. Defaults to out/cafe_menu_commands_YYYYMMDD_HHMMSS.ps1
        #[arg(long)]
        output: Option<String>,
    },

    /// Reddit 서브레딧 크롤링 (공개 JSON API, ChromeDriver 불필요)
    Reddit {
        /// 서브레딧 이름 (예: minimalism). 생략 시 전체 Reddit에서 검색
        #[arg(long)]
        subreddit: Option<String>,

        /// 정렬 방식 (new | hot | top | rising)
        #[arg(long, default_value = "new")]
        sort: String,

        /// 페이지당 게시글 수 (최대 100)
        #[arg(long, default_value_t = 100)]
        limit: usize,

        /// 최대 페이지 수
        #[arg(long, default_value_t = 3)]
        max_pages: usize,

        /// 게시글당 최대 댓글 수
        #[arg(long, default_value_t = 200)]
        max_comments: usize,

        /// 서브레딧 내 검색어 (Reddit 검색 API 사용, 없으면 전체 수집)
        #[arg(long)]
        search_query: Option<String>,

        /// 키워드 필터 (수집 후 제목+본문 필터링, 반복 사용 가능)
        #[arg(long)]
        keyword: Vec<String>,

        /// 병렬 댓글 수집 워커 수
        #[arg(long, default_value_t = 5)]
        workers: usize,

        /// Reddit User-Agent (형식: "platform:appid:v1.0 (by /u/username)")
        #[arg(long, default_value = "rust:reddit-crawler:v1.0 (by /u/anonymous)")]
        user_agent: String,

        /// 페이지 사이 딜레이 (ms)
        #[arg(long, default_value_t = 2000)]
        page_delay_ms: u64,

        /// 결과 저장 디렉토리
        #[arg(long, default_value = "out")]
        out_dir: String,
    },

    /// Amazon 상품 리뷰 병렬 수집 (ChromeDriver 필요, Plan J)
    Amazon {
        /// 상품 리뷰 URL (반복 사용 가능, 예: --url "https://amazon.com/..." --url "https://amazon.com/...")
        #[arg(long)]
        url: Vec<String>,

        /// 상품 URL 목록 파일 (한 줄에 URL 하나)
        #[arg(long)]
        input: Option<String>,

        /// 상품당 수집할 최대 리뷰 수
        #[arg(long, default_value_t = 100)]
        max_reviews: usize,

        /// 병렬 워커(Chrome 세션) 수
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// ChromeDriver 엔드포인트
        #[arg(long, default_value = "http://localhost:9515")]
        webdriver: String,

        /// 헤드리스 모드 (--cookie-file과 함께 사용 시 완전 헤드리스)
        #[arg(long, default_value_t = false)]
        headless: bool,

        /// 쿠키 파일 경로 (첫 실행 후 amazon_output/cookies.json에 자동 저장됨)
        #[arg(long)]
        cookie_file: Option<String>,

        /// Read More 자동 클릭 비활성화
        #[arg(long, default_value_t = false)]
        no_read_more: bool,

        /// 결과 저장 디렉토리
        #[arg(long, default_value = "amazon_output")]
        out_dir: String,
    },

    /// Goodreads 리뷰 병렬 수집 — 무한 스크롤 + Show more reviews (Plan K)
    Goodreads {
        /// 리뷰 페이지 URL (반복 사용 가능)
        #[arg(long)]
        url: Vec<String>,

        /// URL 목록 파일 (한 줄에 URL 하나)
        #[arg(long)]
        input: Option<String>,

        /// 병렬 워커(Chrome 세션) 수
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// ChromeDriver 엔드포인트
        #[arg(long, default_value = "http://localhost:9515")]
        webdriver: String,

        /// 헤드리스 모드 (--cookie-file과 함께 사용)
        #[arg(long, default_value_t = false)]
        headless: bool,

        /// 쿠키 파일 경로 (첫 실행 후 goodreads_output/cookies.json에 자동 저장)
        #[arg(long)]
        cookie_file: Option<String>,

        /// Chrome 프로필 디렉토리 (로그인 세션 유지 — 로그인 단계에서만 사용)
        #[arg(long, default_value = "goodreads_profile")]
        profile_dir: String,

        /// 책당 수집할 최대 리뷰 수 (0 = 무제한)
        #[arg(long, default_value_t = 0)]
        max_reviews: usize,

        /// 새 리뷰 없을 때 종료까지 허용 라운드 수
        #[arg(long, default_value_t = 5)]
        max_idle_rounds: usize,

        /// 결과 저장 디렉토리
        #[arg(long, default_value = "goodreads_output")]
        out_dir: String,
    },

    /// Crawl URLs (file and/or repeated --url)
    Crawl {
        /// Input file path (one URL per line)
        #[arg(long)]
        input: Option<String>,

        /// Add a URL directly (can be repeated)
        #[arg(long)]
        url: Vec<String>,

        /// Max concurrent in-flight Plan A requests
        #[arg(long, default_value_t = 200)]
        max_in_flight: usize,

        /// Optional webdriver endpoint for Plan B fallback
        #[arg(long)]
        webdriver: Option<String>,

        /// Plan B pages to process (opens the same number of parallel sessions)
        #[arg(long, default_value_t = 0)]
        plan_b_pages: usize,

        /// Output directory (created if missing)
        #[arg(long, default_value = "out")]
        out_dir: String,
    },
}

fn main() -> Result<(), CrawlError> {
    let handle = std::thread::Builder::new()
        .name("crawler-main".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| CrawlError::Parse(format!("tokio runtime create failed: {e}")))?;
            runtime.block_on(async_main())
        })
        .map_err(|e| CrawlError::Parse(format!("main thread spawn failed: {e}")))?;

    handle
        .join()
        .map_err(|_| CrawlError::Parse("main thread panicked".to_string()))?
}

async fn async_main() -> Result<(), CrawlError> {
    // Logging
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                // chromiumoxide::conn / handler 의 WS 역직렬화 에러는
                // Chrome CDP 프로토콜 버전 차이로 발생하는 비치명적 노이즈이므로 억제
                EnvFilter::new("info,chromiumoxide::conn=off,chromiumoxide::handler=off")
            }),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Cafe { url, max_posts, workers, webdriver, out_dir, cookie_file } => {
            let url = Url::parse(&url)?;
            let out_dir_path = Path::new(&out_dir);
            ensure_out_dir(out_dir_path)
                .map_err(|e| CrawlError::Parse(format!("출력 디렉토리 생성 실패: {e}")))?;

            let cookies: Arc<Vec<CookieEntry>> = Arc::new(match cookie_file {
                Some(ref p) => {
                    let c = load_cookies(Path::new(p))?;
                    info!(count = c.len(), file = %p, "쿠키 파일 로드");
                    c
                }
                None => vec![],
            });

            info!(%url, max_posts, workers, "리스트 크롤 시작");
            let results = crawl_plan_b_from_list(&webdriver, url, max_posts, workers, cookies).await;

            let mut posts = Vec::new();
            let mut failed = 0usize;
            for r in results {
                match r {
                    Ok(p) => posts.push(p),
                    Err(e) => {
                        warn!("게시글 실패: {e}");
                        failed += 1;
                    }
                }
            }

            info!(ok = posts.len(), failed, "수집 완료");

            write_posts_csv(out_dir_path, &posts)
                .map_err(|e| CrawlError::Parse(format!("csv 저장 오류: {e}")))?;
            write_comments_csv(out_dir_path, &posts)
                .map_err(|e| CrawlError::Parse(format!("csv 저장 오류: {e}")))?;

            info!(out_dir, "CSV 저장 완료");
        }

        Commands::Scroll {
            url, max_posts, workers, out_dir, cookie_file,
            card_selector, link_selector, scroll_pause,
        } => {
            let url = Url::parse(&url)?;
            let out_dir_path = Path::new(&out_dir);
            ensure_out_dir(out_dir_path)
                .map_err(|e| CrawlError::Parse(format!("출력 디렉토리 생성 실패: {e}")))?;

            let cookies: Arc<Vec<CookieEntry>> = Arc::new(match cookie_file {
                Some(ref p) => {
                    let c = load_cookies(Path::new(p))?;
                    info!(count = c.len(), file = %p, "쿠키 파일 로드");
                    c
                }
                None => vec![],
            });

            // ohouse 기본값을 베이스로, CLI에서 받은 셀렉터로 override
            let config = Arc::new(ScrollConfig {
                card_selector,
                card_link_selector: link_selector,
                scroll_pause_ms: scroll_pause,
                ..ScrollConfig::ohouse()
            });

            info!(%url, max_posts, workers, "스크롤 크롤 시작");
            let results = crawl_plan_c_scroll(url, max_posts, workers, config, cookies).await;

            let mut posts = Vec::new();
            let mut failed = 0usize;
            for r in results {
                match r {
                    Ok(p) => posts.push(p),
                    Err(e) => {
                        warn!("게시글 실패: {e}");
                        failed += 1;
                    }
                }
            }

            info!(ok = posts.len(), failed, "수집 완료");
            write_posts_csv(out_dir_path, &posts)
                .map_err(|e| CrawlError::Parse(format!("csv 저장 오류: {e}")))?;
            write_comments_csv(out_dir_path, &posts)
                .map_err(|e| CrawlError::Parse(format!("csv 저장 오류: {e}")))?;
            info!(out_dir, "CSV 저장 완료");
        }

        Commands::Smartstore { url, input, workers, webdriver, output, headless } => {
            let mut urls: Vec<String> = url;
            if let Some(path) = input {
                let parsed = read_urls_from_file(Path::new(&path))?;
                urls.extend(parsed.into_iter().map(|u| u.to_string()));
            }
            if urls.is_empty() {
                return Err(CrawlError::Parse(
                    "URL이 없습니다. --url 또는 --input을 지정하세요.".to_string(),
                ));
            }

            // 출력 디렉토리 생성
            if let Some(parent) = Path::new(&output).parent() {
                if !parent.as_os_str().is_empty() {
                    ensure_out_dir(parent)
                        .map_err(|e| CrawlError::Parse(format!("출력 디렉토리 생성 실패: {e}")))?;
                }
            }

            info!(total = urls.len(), workers, headless, "Plan E 스마트스토어 리뷰 수집 시작");
            let rows = run_plan_e_parallel(&webdriver, urls, workers, headless)
                .await
                .map_err(|e| CrawlError::Parse(format!("Plan E 오류: {e}")))?;

            info!(reviews = rows.len(), output, "CSV 저장");
            write_reviews_csv(&output, &rows)
                .map_err(|e| CrawlError::Parse(format!("CSV 저장 실패: {e}")))?;

            info!(output, "완료");
        }

        Commands::Coupang {
            url,
            input,
            start_page,
            max_pages,
            workers,
            webdriver,
            headless,
            output,
            cookie,
            cookie_file,
            page_delay_ms,
            browser_fetch,
        } => {
            let mut urls: Vec<String> = url;
            if let Some(path) = input {
                let lines = std::fs::read_to_string(&path)
                    .map_err(|e| CrawlError::Parse(format!("file read failed {path}: {e}")))?;
                for line in lines.lines() {
                    let line = line.trim();
                    if !line.is_empty() && !line.starts_with('#') {
                        urls.push(line.to_string());
                    }
                }
            }
            if urls.is_empty() {
                return Err(CrawlError::Parse(
                    "URL이 없습니다. --url 또는 --input을 지정하세요.".to_string(),
                ));
            }

            plan_o::run(plan_o::PlanOConfig {
                product_urls: urls,
                start_page,
                max_pages,
                workers,
                output,
                cookie,
                cookie_file,
                page_delay_ms,
                browser_fetch,
                webdriver_url: webdriver,
                headless,
            })
            .await
            .map_err(|e| CrawlError::Parse(format!("coupang error: {e}")))?;
        }

        Commands::ItdaCommunity {
            start_page,
            max_pages,
            max_posts,
            workers,
            webdriver,
            out_dir,
            headless,
            profile_dir,
        } => {
            plan_m::run(plan_m::PlanMConfig {
                start_page,
                max_pages,
                max_posts,
                workers,
                webdriver_url: webdriver,
                out_dir,
                headless_workers: headless,
                profile_dir,
            })
            .await
            .map_err(|e| CrawlError::Parse(format!("itda-community error: {e}")))?;
        }

        Commands::NaverSearch {
            url,
            max_scrolls,
            max_posts,
            workers,
            webdriver,
            out_dir,
            headless,
            comment_page_limit,
        } => {
            plan_n::run(plan_n::PlanNConfig {
                search_url: url,
                max_scrolls,
                max_posts,
                workers,
                webdriver_url: webdriver,
                out_dir,
                headless,
                comment_page_limit,
            })
            .await
            .map_err(|e| CrawlError::Parse(format!("naver-search error: {e}")))?;
        }

        Commands::Threads {
            keyword, max_posts, workers, webdriver, out_dir,
            comment_scroll_rounds, comment_pause_secs,
        } => {
            plan_i::run(plan_i::PlanIConfig {
                keyword,
                max_posts,
                workers,
                webdriver_url: webdriver,
                out_dir,
                comment_scroll_rounds,
                comment_pause_secs,
                ..plan_i::PlanIConfig::default()
            })
            .await
            .map_err(|e| CrawlError::Parse(format!("threads 오류: {e}")))?;
        }

        Commands::BlogSearch {
            query, start_date, end_date, max_posts, workers,
            webdriver, headless, out_dir,
            search_max_scrolls, detail_max_scrolls,
        } => {
            plan_h::run(plan_h::PlanHConfig {
                query,
                start_date,
                end_date,
                max_posts,
                workers,
                webdriver_url: webdriver,
                headless,
                output_dir: std::path::PathBuf::from(out_dir),
                search_max_scrolls,
                detail_max_scrolls,
                ..plan_h::PlanHConfig::default()
            })
            .await
            .map_err(|e| CrawlError::Parse(format!("blog-search 오류: {e}")))?;
        }

        Commands::CafeOpen { url, url_csv, max_posts, list_workers, max_pages, workers, webdriver, out_dir, browser, from_row, to_row, url_only } => {
            plan_f::run(
                &webdriver,
                url.as_deref(),
                url_csv.as_deref(),
                max_posts,
                list_workers,
                max_pages,
                workers,
                &out_dir,
                from_row,
                to_row,
                url_only,
                &browser,
            )
                .await
                .map_err(|e| CrawlError::Parse(format!("cafe-open 오류: {e}")))?;
        }

        Commands::CafeMenuCommands {
            url,
            webdriver,
            browser,
            max_posts,
            max_pages,
            list_workers,
            size,
            out_dir,
            output,
        } => {
            let output = plan_f::generate_menu_commands(plan_f::MenuCommandsConfig {
                url,
                webdriver_urls: webdriver,
                browser,
                max_posts,
                max_pages,
                list_workers,
                size,
                out_dir,
                output,
            })
            .await
            .map_err(|e| CrawlError::Parse(format!("cafe-menu-commands error: {e}")))?;
            info!("cafe menu commands saved: {}", output.display());
        }

        Commands::Reddit { subreddit, sort, limit, max_pages, max_comments, search_query, keyword, workers, user_agent, page_delay_ms, out_dir } => {
            plan_g::run(plan_g::RedditConfig {
                subreddit,
                sort,
                limit,
                max_pages,
                max_comments,
                keywords: keyword,
                search_query,
                workers,
                user_agent,
                page_delay_ms,
                out_dir,
            })
            .await
            .map_err(|e| CrawlError::Parse(format!("reddit 오류: {e}")))?;
        }

        Commands::Amazon { url, input, max_reviews, workers, webdriver, headless, cookie_file, no_read_more, out_dir } => {
            let mut urls: Vec<String> = url;
            if let Some(path) = input {
                let lines = std::fs::read_to_string(&path)
                    .map_err(|e| CrawlError::Parse(format!("파일 읽기 실패 {path}: {e}")))?;
                for line in lines.lines() {
                    let line = line.trim();
                    if !line.is_empty() && !line.starts_with('#') {
                        urls.push(line.to_string());
                    }
                }
            }
            if urls.is_empty() {
                return Err(CrawlError::Parse(
                    "URL이 없습니다. --url 또는 --input을 지정하세요.".to_string(),
                ));
            }
            info!(total = urls.len(), "Amazon 수집 대상 상품 수");
            plan_j::run(plan_j::PlanJConfig {
                review_urls: urls,
                max_reviews,
                workers,
                webdriver_url: webdriver,
                headless,
                cookie_file,
                click_read_more: !no_read_more,
                out_dir,
            })
            .await
            .map_err(|e| CrawlError::Parse(format!("amazon 오류: {e}")))?;
        }

        Commands::Goodreads { url, input, workers, webdriver, headless, cookie_file, profile_dir, max_reviews, max_idle_rounds, out_dir } => {
            let mut urls: Vec<String> = url;
            if let Some(path) = input {
                let lines = std::fs::read_to_string(&path)
                    .map_err(|e| CrawlError::Parse(format!("파일 읽기 실패 {path}: {e}")))?;
                for line in lines.lines() {
                    let line = line.trim();
                    if !line.is_empty() && !line.starts_with('#') {
                        urls.push(line.to_string());
                    }
                }
            }
            if urls.is_empty() {
                return Err(CrawlError::Parse(
                    "URL이 없습니다. --url 또는 --input을 지정하세요.".to_string(),
                ));
            }
            plan_k::run(plan_k::PlanKConfig {
                target_urls: urls,
                workers,
                webdriver_url: webdriver,
                headless,
                cookie_file,
                profile_dir,
                max_reviews,
                max_idle_rounds,
                out_dir,
            })
            .await
            .map_err(|e| CrawlError::Parse(format!("goodreads 오류: {e}")))?;
        }

        Commands::Scrape { url, max_posts, workers, out_dir } => {
            plan_d::run(&url, max_posts, workers, &out_dir)
                .await
                .map_err(|e| CrawlError::Parse(format!("scrape 오류: {e}")))?;
        }

        Commands::Test { url, out_dir, webdriver } => {
            let url = Url::parse(&url)?;
            run_smoke_test(TestOptions { url, out_dir, webdriver_url: webdriver }).await?;
        }
        Commands::Crawl {
            input,
            url,
            max_in_flight,
            webdriver,
            plan_b_pages,
            out_dir,
        } => {
            let mut urls = Vec::new();
            if let Some(input_path) = input {
                urls.extend(read_urls_from_file(Path::new(&input_path))?);
            }
            for u in url {
                urls.push(Url::parse(&u)?);
            }

            if urls.is_empty() {
                return Err(CrawlError::Parse(
                    "no URLs provided. Use --input urls.txt and/or --url ...".to_string(),
                ));
            }

            let out_dir_path = Path::new(&out_dir);
            ensure_out_dir(out_dir_path)
                .map_err(|e| CrawlError::Parse(format!("failed to create out dir: {e}")))?;

            info!(count = urls.len(), "starting crawl");

            let posts = crawl_all_with_fallback(
                urls,
                CrawlOptions {
                    max_in_flight,
                    webdriver_url: webdriver,
                    plan_b_pages,
                },
            )
            .await?;

            // CSV exports
            write_posts_csv(out_dir_path, &posts)
                .map_err(|e| CrawlError::Parse(format!("csv write error: {e}")))?;
            write_comments_csv(out_dir_path, &posts)
                .map_err(|e| CrawlError::Parse(format!("csv write error: {e}")))?;

            info!(posts = posts.len(), "done");
        }
    }

    Ok(())
}
