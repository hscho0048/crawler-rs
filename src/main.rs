mod csv_out;
mod engine;
mod errors;
mod input;
mod merge;
mod models;
mod plan_a;
mod plan_b;
mod plan_c;
mod test_mode;
mod plan_d;

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

    /// 게시판 리스트 페이지에서 게시글을 자동 수집 (제목 클릭 → 본문/날짜/댓글)
    List {
        /// 게시판 리스트 페이지 URL (예: 네이버 카페 게시판 URL)
        #[arg(long)]
        url: String,

        /// 수집할 최대 게시글 수
        #[arg(long, default_value_t = 20)]
        max_posts: usize,

        /// 동시에 열 Chrome 세션 수 (병렬 처리)
        #[arg(long, default_value_t = 3)]
        workers: usize,

        /// WebDriver 엔드포인트 (필수, 예: http://localhost:4444)
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

#[tokio::main]
async fn main() -> Result<(), CrawlError> {
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
        Commands::List { url, max_posts, workers, webdriver, out_dir, cookie_file } => {
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
