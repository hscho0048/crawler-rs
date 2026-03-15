use anyhow::{bail, Context, Result};
use csv::WriterBuilder;
use dotenvy::dotenv;
use scraper::{ElementRef, Html, Selector};
use serde::Deserialize;
use std::collections::{HashSet, VecDeque};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thirtyfour::cookie::Cookie;
use thirtyfour::prelude::*;
use thirtyfour::ChromeCapabilities;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use rand::Rng;
use tokio::time::sleep;

const DEFAULT_INPUT_FILE: &str = "keywords.txt";
const DEFAULT_OUTPUT_DIR: &str = ".";
const DEFAULT_WEBDRIVER_URL: &str = "http://localhost:4444";
const DEFAULT_IMPLICIT_WAIT_SECS: u64 = 2;
const DEFAULT_PAGE_LOAD_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_POSTS_PER_TAG: usize = 100;
const DEFAULT_ACCEPT_LANGUAGE: &str = "en-US,en;q=0.9";
const DEFAULT_BROWSER_LOCALE: &str = "en-US";
const DEFAULT_WINDOW_WIDTH: u32 = 1440;
const DEFAULT_WINDOW_HEIGHT: u32 = 2000;
const DEFAULT_WORKERS: usize = 1;
const POLL_MS: u64 = 250;

#[derive(Debug, Default)]
struct CliArgs {
    config_path: Option<PathBuf>,
    input_path: Option<PathBuf>,
    output_dir: Option<PathBuf>,
    max_posts: Option<usize>,
    max_comments: Option<usize>,
    min_comment_len: Option<usize>,
    workers: Option<usize>,
    urls_only: bool,
    from_urls: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct JsonConfig {
    username: Option<String>,
    password: Option<String>,
    manual_login: Option<bool>,
    webdriver_url: Option<String>,
    chrome_binary: Option<String>,
    headless: Option<bool>,
    input_file: Option<String>,
    output_dir: Option<String>,
    implicit_wait_secs: Option<u64>,
    page_load_timeout_secs: Option<u64>,
    max_posts_per_tag: Option<usize>,
    max_comments_per_post: Option<usize>,
    min_comment_len: Option<usize>,
    workers: Option<usize>,
    accept_language: Option<String>,
    browser_locale: Option<String>,
    proxy_url: Option<String>,
    user_agent: Option<String>,
    window_width: Option<u32>,
    window_height: Option<u32>,
    block_images: Option<bool>,
    disable_webrtc: Option<bool>,
}

#[derive(Debug, Clone)]
struct AppConfig {
    username: Option<String>,
    password: Option<String>,
    manual_login: bool,
    webdriver_url: String,
    chrome_binary: Option<PathBuf>,
    headless: bool,
    input_file: PathBuf,
    output_dir: PathBuf,
    implicit_wait_secs: u64,
    page_load_timeout_secs: u64,
    max_posts_per_tag: usize,
    max_comments_per_post: usize,
    min_comment_len: usize,
    workers: usize,
    accept_language: String,
    browser_locale: String,
    proxy_url: Option<String>,
    user_agent: Option<String>,
    window_width: u32,
    window_height: u32,
    block_images: bool,
    disable_webrtc: bool,
    urls_only: bool,
    from_urls: bool,
}

#[derive(Debug, Clone)]
struct TagJob {
    label: String,
    keyword: String,
}

#[derive(Debug, Clone)]
struct CommentRow {
    author: String,
    text: String,
    datetime: String,
    likes: String,
}

#[derive(Debug, Clone)]
struct ExtractedPost {
    post_url: String,
    date_text: String,
    author: Option<String>,
    article: String,
    hashtags: String,
    favorites: i64,
    comments: Vec<CommentRow>,
}

#[derive(Debug, Clone)]
enum Locator {
    Css(String),
    XPath(String),
}

impl Locator {
    fn css(value: impl Into<String>) -> Self {
        Self::Css(value.into())
    }

    fn xpath(value: impl Into<String>) -> Self {
        Self::XPath(value.into())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Arc::new(load_app_config()?);
    fs::create_dir_all(&cfg.output_dir)
        .with_context(|| format!("failed to create output dir: {}", cfg.output_dir.display()))?;

    let _driver_handles = restart_drivers(&cfg.webdriver_url, cfg.workers).await?;

    // 로그인 후 쿠키 추출 (--from-urls 모드에서는 생략)
    let cookies = if !cfg.from_urls {
        let driver = build_driver(&cfg, &cfg.webdriver_url).await?;
        let result = login(&driver, &cfg).await;
        let cookies = driver.get_all_cookies().await.context("쿠키 추출 실패")?;
        let _ = driver.quit().await;
        sleep(Duration::from_secs(2)).await;
        result?;
        eprintln!("[INFO] 로그인 완료, 쿠키 {}개 추출", cookies.len());
        Arc::new(cookies)
    } else {
        Arc::new(vec![])
    };

    // 잡 큐 구성
    let jobs = read_jobs(&cfg.input_file)?;
    if jobs.is_empty() {
        eprintln!("[INFO] 키워드가 없습니다.");
        return Ok(());
    }
    eprintln!("[INFO] 키워드 {}개, 워커 {}개로 병렬 처리 시작", jobs.len(), cfg.workers);
    let queue: Arc<Mutex<VecDeque<TagJob>>> = Arc::new(Mutex::new(VecDeque::from(jobs)));

    // 3단계: 워커 풀
    let mut joinset: JoinSet<()> = JoinSet::new();
    let n_workers = cfg.workers;

    for worker_id in 0..n_workers {
        let cfg = cfg.clone();
        let queue = queue.clone();
        let cookies = cookies.clone();
        let worker_url = assign_worker_url(&cfg.webdriver_url, worker_id);

        joinset.spawn(async move {
            async fn make_driver(
                cfg: &AppConfig,
                url: &str,
                cookies: &[Cookie],
                worker_id: usize,
            ) -> Option<WebDriver> {
                let driver = match build_driver(cfg, url).await {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("[ERROR] 워커 {worker_id}: 드라이버 초기화 실패: {e:#}");
                        return None;
                    }
                };
                if !cfg.from_urls && !cookies.is_empty() {
                    if let Err(e) = inject_cookies(&driver, cookies).await {
                        eprintln!("[WARN] 워커 {worker_id}: 쿠키 주입 실패: {e:#}");
                    }
                }
                Some(driver)
            }

            let Some(mut driver) = make_driver(&cfg, &worker_url, &cookies, worker_id).await else {
                return;
            };

            loop {
                let job = queue.lock().await.pop_front();
                let Some(job) = job else { break };

                eprintln!("[INFO] 워커 {worker_id}: {} ({}) 처리 중", job.label, job.keyword);
                match process_job(&driver, &cfg, &job).await {
                    Ok(_) => {}
                    Err(e) if is_session_dead(&e) => {
                        eprintln!("[WARN] 워커 {worker_id}: 세션 만료, 드라이버 재생성 후 재시도");
                        match make_driver(&cfg, &worker_url, &cookies, worker_id).await {
                            None => return, // 재생성 실패 → 워커 종료
                            Some(new_driver) => {
                                // 죽은 세션은 drop만 (quit 호출 불필요)
                                let _ = std::mem::replace(&mut driver, new_driver);
                                // 재시도
                                if let Err(e) = process_job(&driver, &cfg, &job).await {
                                    eprintln!("[WARN] 워커 {worker_id}: {} ({}) 재시도 실패: {e:#}", job.label, job.keyword);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[WARN] 워커 {worker_id}: {} ({}) 실패: {e:#}", job.label, job.keyword);
                    }
                }
            }

            let _ = driver.quit().await;
        });
    }

    while joinset.join_next().await.is_some() {}
    Ok(())
}

fn is_session_dead(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}");
    msg.contains("invalid session id")
        || msg.contains("session not created")
        || msg.contains("no such session")
        || msg.contains("session is already started")
}


async fn login(driver: &WebDriver, cfg: &AppConfig) -> Result<()> {
    driver.goto("https://www.instagram.com/accounts/login/").await?;

    if cfg.manual_login {
        println!("브라우저에서 직접 로그인한 뒤 Enter를 누르십시오.");
        let mut line = String::new();
        io::stdin().read_line(&mut line)?;
        return Ok(());
    }

    let username = cfg.username.as_deref().context("missing username")?;
    let password = cfg.password.as_deref().context("missing password")?;

    let username_input = wait_for_first(
        driver,
        &[
            Locator::css("input[name='email']"),
            Locator::css("input[name='username']"),
        ],
        30,
    ).await?;
    click_element(&username_input).await.ok();
    sleep(Duration::from_millis(300)).await;
    username_input.clear().await.ok();
    username_input.send_keys(username).await?;

    let password_input = wait_for_first(
        driver,
        &[
            Locator::css("input[name='pass']"),
            Locator::css("input[name='password']"),
        ],
        30,
    ).await?;
    click_element(&password_input).await.ok();
    sleep(Duration::from_millis(300)).await;
    password_input.clear().await.ok();
    password_input.send_keys(password).await?;

    let submit = wait_for_first(
        driver,
        &[
            Locator::css("button[type='submit']"),
            Locator::xpath("//button[@type='submit']"),
        ],
        15,
    ).await?;
    click_element(&submit).await?;

    sleep(Duration::from_secs(5)).await;
    let current_url = driver.current_url().await.map(|u| u.to_string()).unwrap_or_default();
    if current_url.contains("challenge") || current_url.contains("two_factor") {
        bail!("Instagram 2FA/챌린지 감지. MANUAL_LOGIN=true로 재실행하세요.");
    }
    Ok(())
}

async fn inject_cookies(driver: &WebDriver, cookies: &[Cookie]) -> Result<()> {
    driver.goto("https://www.instagram.com/").await?;
    sleep(Duration::from_millis(1000)).await;
    for cookie in cookies {
        driver.add_cookie(cookie.clone()).await.ok();
    }
    driver.goto("https://www.instagram.com/").await?;
    sleep(Duration::from_millis(1000)).await;
    Ok(())
}

fn load_app_config() -> Result<AppConfig> {
    let cli = parse_args()?;
    dotenv().ok();

    let json_path = cli
        .config_path
        .clone()
        .or_else(|| env_string("IG_CONFIG_JSON").map(PathBuf::from));

    let json_cfg = if let Some(path) = json_path {
        load_json_config(&path)?
    } else {
        JsonConfig::default()
    };

    let manual_login = env_bool("MANUAL_LOGIN")
        .or(json_cfg.manual_login)
        .unwrap_or(false);

    let username = env_string("IG_USERNAME").or_else(|| nonempty(json_cfg.username.clone()));
    let password = env_string("IG_PASSWORD").or_else(|| nonempty(json_cfg.password.clone()));

    if !manual_login && (username.is_none() || password.is_none()) {
        bail!("missing IG_USERNAME / IG_PASSWORD (or username/password in json). Or set MANUAL_LOGIN=true");
    }

    let webdriver_url = env_string("WEBDRIVER_URL")
        .or_else(|| nonempty(json_cfg.webdriver_url.clone()))
        .unwrap_or_else(|| DEFAULT_WEBDRIVER_URL.to_string());

    let chrome_binary = env_string("CHROME_BINARY")
        .or_else(|| nonempty(json_cfg.chrome_binary.clone()))
        .map(PathBuf::from);

    let headless = env_bool("HEADLESS").or(json_cfg.headless).unwrap_or(false);

    let input_file = cli
        .input_path
        .clone()
        .or_else(|| env_string("INPUT_FILE").map(PathBuf::from))
        .or_else(|| nonempty(json_cfg.input_file.clone()).map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_INPUT_FILE));

    let output_dir = cli
        .output_dir
        .clone()
        .or_else(|| env_string("OUTPUT_DIR").map(PathBuf::from))
        .or_else(|| nonempty(json_cfg.output_dir.clone()).map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_DIR));

    let implicit_wait_secs = env_u64("IMPLICIT_WAIT_SECS")
        .or(json_cfg.implicit_wait_secs)
        .unwrap_or(DEFAULT_IMPLICIT_WAIT_SECS);

    let page_load_timeout_secs = env_u64("PAGE_LOAD_TIMEOUT_SECS")
        .or(json_cfg.page_load_timeout_secs)
        .unwrap_or(DEFAULT_PAGE_LOAD_TIMEOUT_SECS);

    // CLI --max-posts > env > json > default
    let max_posts_per_tag = cli
        .max_posts
        .or_else(|| env_usize("MAX_POSTS_PER_TAG"))
        .or(json_cfg.max_posts_per_tag)
        .unwrap_or(DEFAULT_MAX_POSTS_PER_TAG);

    let max_comments_per_post = cli
        .max_comments
        .or_else(|| env_usize("MAX_COMMENTS_PER_POST"))
        .or(json_cfg.max_comments_per_post)
        .unwrap_or(usize::MAX);

    let min_comment_len = cli
        .min_comment_len
        .or_else(|| env_usize("MIN_COMMENT_LEN"))
        .or(json_cfg.min_comment_len)
        .unwrap_or(0);


    // CLI --workers > env > json > default
    let workers = cli
        .workers
        .or_else(|| env_usize("WORKERS"))
        .or(json_cfg.workers)
        .unwrap_or(DEFAULT_WORKERS)
        .max(1);

    let accept_language = env_string("ACCEPT_LANGUAGE")
        .or_else(|| nonempty(json_cfg.accept_language.clone()))
        .unwrap_or_else(|| DEFAULT_ACCEPT_LANGUAGE.to_string());

    let browser_locale = env_string("BROWSER_LOCALE")
        .or_else(|| nonempty(json_cfg.browser_locale.clone()))
        .unwrap_or_else(|| DEFAULT_BROWSER_LOCALE.to_string());

    let proxy_url = env_string("PROXY_URL").or_else(|| nonempty(json_cfg.proxy_url.clone()));
    let user_agent = env_string("USER_AGENT").or_else(|| nonempty(json_cfg.user_agent.clone()));

    let window_width = env_u64("WINDOW_WIDTH")
        .map(|v| v as u32)
        .or(json_cfg.window_width)
        .unwrap_or(DEFAULT_WINDOW_WIDTH);

    let window_height = env_u64("WINDOW_HEIGHT")
        .map(|v| v as u32)
        .or(json_cfg.window_height)
        .unwrap_or(DEFAULT_WINDOW_HEIGHT);

    let block_images = env_bool("BLOCK_IMAGES")
        .or(json_cfg.block_images)
        .unwrap_or(false);

    let disable_webrtc = env_bool("DISABLE_WEBRTC")
        .or(json_cfg.disable_webrtc)
        .unwrap_or(false);

    Ok(AppConfig {
        username,
        password,
        manual_login,
        webdriver_url,
        chrome_binary,
        headless,
        input_file,
        output_dir,
        implicit_wait_secs,
        page_load_timeout_secs,
        max_posts_per_tag,
        max_comments_per_post,
        min_comment_len,
        workers,
        accept_language,
        browser_locale,
        proxy_url,
        user_agent,
        window_width,
        window_height,
        block_images,
        disable_webrtc,
        urls_only: cli.urls_only,
        from_urls: cli.from_urls,
    })
}

fn parse_args() -> Result<CliArgs> {
    let mut cli = CliArgs::default();
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config"     => cli.config_path = Some(PathBuf::from(args.next().context("missing value after --config")?)),
            "--input"      => cli.input_path  = Some(PathBuf::from(args.next().context("missing value after --input")?)),
            "--output-dir" => cli.output_dir  = Some(PathBuf::from(args.next().context("missing value after --output-dir")?)),
            "--max-posts"        => cli.max_posts        = Some(args.next().context("missing value after --max-posts")?.parse().context("--max-posts must be a number")?),
            "--max-comments"     => cli.max_comments     = Some(args.next().context("missing value after --max-comments")?.parse().context("--max-comments must be a number")?),
            "--min-comment-len"  => cli.min_comment_len  = Some(args.next().context("missing value after --min-comment-len")?.parse().context("must be a number")?),
            "--workers"          => cli.workers          = Some(args.next().context("missing value after --workers")?.parse().context("--workers must be a number")?),
            "--urls-only"        => cli.urls_only        = true,
            "--from-urls"        => cli.from_urls        = true,
            _ => {}
        }
    }

    Ok(cli)
}

fn load_json_config(path: &Path) -> Result<JsonConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("failed to read config json: {}", path.display()))?;
    let cfg: JsonConfig = serde_json::from_str(&content)
        .with_context(|| format!("invalid json config: {}", path.display()))?;
    Ok(cfg)
}

fn read_jobs(path: &Path) -> Result<Vec<TagJob>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open input file: {}", path.display()))?;
    let reader = BufReader::new(file);

    let mut jobs = Vec::new();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = trimmed.split('\t').map(|v| v.trim()).collect();
        let (label, keyword) = match parts.as_slice() {
            [one] => ((*one).to_string(), make_keyword(one)),
            [label, keyword, ..] => ((*label).to_string(), make_keyword(keyword)),
            _ => continue,
        };

        if keyword.is_empty() {
            continue;
        }

        jobs.push(TagJob { label, keyword });
    }

    Ok(jobs)
}

fn make_keyword(value: &str) -> String {
    let tmp = value.trim().trim_start_matches('#');
    let tmp = tmp.replace(',', "");
    let tmp = collapse_ws(&tmp).replace(' ', "");
    tmp
}

/// worker_id 0 → base URL 그대로, 1 → base port+1, 2 → base port+2 ...
fn assign_worker_url(base_url: &str, worker_id: usize) -> String {
    if worker_id == 0 {
        return base_url.to_string();
    }
    if let Ok(mut u) = url::Url::parse(base_url) {
        if let Some(port) = u.port() {
            if u.set_port(Some(port + worker_id as u16)).is_ok() {
                return u.to_string();
            }
        }
    }
    base_url.to_string()
}

/// chromedriver + Chrome 프로세스를 모두 종료하고 필요한 포트 수만큼 새로 실행한다.
/// 반환된 Child 핸들을 유지해야 프로세스가 살아있다.
async fn restart_drivers(
    base_url: &str,
    n_workers: usize,
) -> Result<Vec<std::process::Child>> {
    // 기존 프로세스 종료
    #[cfg(target_os = "windows")]
    {
        let _ = std::process::Command::new("taskkill").args(["/F", "/IM", "chromedriver.exe"]).output();
        let _ = std::process::Command::new("taskkill").args(["/F", "/IM", "chrome.exe"]).output();
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = std::process::Command::new("pkill").args(["-f", "chromedriver"]).output();
        let _ = std::process::Command::new("pkill").args(["-f", "chrome"]).output();
    }
    sleep(Duration::from_millis(800)).await;

    // chromedriver 실행 파일 위치 결정
    let chrome_bin = {
        let exe_dir = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.to_path_buf()));
        let cwd = std::env::current_dir().ok();
        let candidates = [
            exe_dir.as_ref().map(|d| d.join("chromedriver.exe")),
            cwd.as_ref().map(|d| d.join("chromedriver.exe")),
            exe_dir.as_ref().map(|d| d.join("chromedriver")),
            cwd.as_ref().map(|d| d.join("chromedriver")),
        ];
        candidates
            .into_iter()
            .flatten()
            .find(|p| p.exists())
            .unwrap_or_else(|| PathBuf::from("chromedriver"))
    };

    // 베이스 포트 추출
    let base_port = url::Url::parse(base_url)
        .ok()
        .and_then(|u| u.port())
        .unwrap_or(4444);

    let mut handles = Vec::new();
    for i in 0..n_workers {
        let port = base_port + i as u16;
        eprintln!("[INFO] chromedriver 시작: 포트 {port}");
        let child = std::process::Command::new(&chrome_bin)
            .arg(format!("--port={port}"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .with_context(|| format!("chromedriver 실행 실패: {}", chrome_bin.display()))?;
        handles.push(child);
    }

    // chromedriver가 준비될 때까지 대기
    sleep(Duration::from_secs(2)).await;
    Ok(handles)
}


async fn build_driver(cfg: &AppConfig, url: &str) -> Result<WebDriver> {
    let mut caps = ChromeCapabilities::new();

    caps.add_arg("--disable-blink-features=AutomationControlled")?;
    caps.add_arg("--no-sandbox")?;
    caps.add_arg("--disable-dev-shm-usage")?;
    caps.add_arg(&format!("--window-size={},{}", cfg.window_width, cfg.window_height))?;

    if cfg.headless {
        caps.add_arg("--headless=new")?;
    }
    if let Some(ua) = &cfg.user_agent {
        caps.add_arg(&format!("--user-agent={ua}"))?;
    }
    if let Some(proxy) = &cfg.proxy_url {
        if let Some((host, port, scheme)) = parse_proxy(proxy) {
            let proxy_str = format!("{scheme}://{host}:{port}");
            caps.add_arg(&format!("--proxy-server={proxy_str}"))?;
        }
    }
    if let Some(binary) = &cfg.chrome_binary {
        caps.set_binary(&binary.display().to_string())?;
    }

    caps.set_page_load_strategy(thirtyfour::PageLoadStrategy::Normal)?;

    let driver = WebDriver::new(url, caps)
        .await
        .with_context(|| format!("failed to connect to webdriver: {url}"))?;

    driver.set_implicit_wait_timeout(Duration::from_secs(cfg.implicit_wait_secs)).await?;
    driver.set_page_load_timeout(Duration::from_secs(cfg.page_load_timeout_secs)).await?;
    // navigator.webdriver 속성 제거 (페이지 로드마다 실행)
    let _ = driver.execute(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})",
        vec![],
    ).await;
    Ok(driver)
}


async fn dismiss_optional_dialogs(driver: &WebDriver) {
    // "나중에 하기" 류 텍스트 버튼
    let texts = ["Not now", "Not Now", "나중에 하기", "나중에", "지금은 안 함"];
    for _ in 0..3 {
        if click_optional_button_by_text(driver, &texts, 3).await {
            sleep(Duration::from_millis(500)).await;
        } else {
            break;
        }
    }

    // SVG aria-label="닫기" X 버튼 (로그인 유도 팝업 등)
    let close_script = r#"
        const svg = document.querySelector('svg[aria-label="닫기"], svg[aria-label="Close"]');
        if (svg) {
            const btn = svg.closest('[role="button"]') || svg.closest('button');
            if (btn) { btn.click(); return true; }
        }
        return false;
    "#;
    for _ in 0..3 {
        match driver.execute(close_script, vec![]).await {
            Ok(v) if v.json() == &serde_json::Value::Bool(true) => {
                sleep(Duration::from_millis(500)).await;
            }
            _ => break,
        }
    }
}

async fn process_job(driver: &WebDriver, cfg: &AppConfig, job: &TagJob) -> Result<()> {
    let posts_path = cfg.output_dir.join(format!("{}_insta.csv", safe_file_stem(&job.keyword)));
    let comments_path = cfg
        .output_dir
        .join(format!("{}_comments.csv", safe_file_stem(&job.keyword)));

    let mut posts_writer = open_csv_writer(
        &posts_path,
        &["label","keyword","no","date","author","article","hashtags","likes","comment_count","url","platform"],
    )?;
    let mut comments_writer = open_csv_writer(
        &comments_path,
        &["post_no","keyword","author","text","datetime","likes"],
    )?;

    // 1단계: URL 수집 (--from-urls이면 파일에서 읽기, 아니면 그리드 스크롤)
    let post_urls = if cfg.from_urls {
        let url_path = cfg.output_dir.join(format!("{}_urls.txt", safe_file_stem(&job.keyword)));
        if !url_path.exists() {
            eprintln!("[WARN] URL 파일 없음 (skip): {}", url_path.display());
            return Ok(());
        }
        let content = fs::read_to_string(&url_path)
            .with_context(|| format!("URL 파일 읽기 실패: {}", url_path.display()))?;
        content.lines().filter(|l| !l.trim().is_empty()).map(|l| l.trim().to_string()).collect::<Vec<_>>()
    } else {
        collect_post_urls(driver, cfg, job).await?
    };
    eprintln!("[INFO] {} ({}): 포스트 URL {}개 수집 완료", job.label, job.keyword, post_urls.len());

    // --urls-only: URL 파일만 저장하고 종료
    if cfg.urls_only {
        let url_path = cfg.output_dir.join(format!("{}_urls.txt", safe_file_stem(&job.keyword)));
        let content = post_urls.join("\n");
        fs::write(&url_path, content)
            .with_context(|| format!("URL 파일 저장 실패: {}", url_path.display()))?;
        eprintln!("[INFO] URL 저장 완료: {}", url_path.display());
        return Ok(());
    }

    // 2단계: 수집된 URL 하나씩 방문하여 내용 추출
    let mut article_number = 1usize;
    let mut saved_posts = 0usize;

    for post_url in &post_urls {
        if saved_posts >= cfg.max_posts_per_tag {
            break;
        }

        // 포스트 간 랜덤 딜레이 (2~5초) — 429 방지
        let delay_ms = rand::thread_rng().gen_range(2000..5000u64);
        sleep(Duration::from_millis(delay_ms)).await;

        // 429 감지 시 재시도 (최대 3회)
        let mut goto_ok = false;
        for attempt in 0..3u32 {
            if let Err(e) = driver.goto(post_url).await {
                let e = anyhow::Error::from(e);
                if is_session_dead(&e) { return Err(e); }
                eprintln!("[WARN] 포스트 이동 실패 (skip): {e:#}");
                break;
            }
            sleep(Duration::from_millis(1500)).await;
            // 429 확인
            let is_429 = driver
                .execute("return document.title.includes('429') || document.body?.innerText?.includes('429')", vec![])
                .await.ok().and_then(|v| v.json().as_bool()).unwrap_or(false);
            if is_429 {
                let wait_secs = 30 * (attempt + 1) as u64;
                eprintln!("[WARN] 429 감지 — {}초 대기 후 재시도 ({}/3)", wait_secs, attempt + 1);
                sleep(Duration::from_secs(wait_secs)).await;
                continue;
            }
            goto_ok = true;
            break;
        }
        if !goto_ok { continue; }

        dismiss_optional_dialogs(driver).await;
        if wait_for_post_ready(driver, 20).await.is_err() {
            let cur = driver.current_url().await.map(|u| u.to_string()).unwrap_or_default();
            let title = driver.title().await.unwrap_or_default();
            let body_snippet = driver
                .execute("return document.body ? document.body.innerText.slice(0,200) : 'NO BODY'", vec![])
                .await.ok().and_then(|v| v.json().as_str().map(|s| s.to_string())).unwrap_or_default();
            let has_popup = driver
                .execute("return !!document.querySelector('svg[aria-label=\"닫기\"], svg[aria-label=\"Close\"]')", vec![])
                .await.ok().and_then(|v| v.json().as_bool()).unwrap_or(false);
            eprintln!("[WARN] 포스트 로드 실패 (skip): {post_url}");
            eprintln!("       현재 URL: {cur}");
            eprintln!("       페이지 제목: {title}");
            eprintln!("       팝업 있음: {has_popup}");
            eprintln!("       본문 앞부분: {body_snippet}");
            continue;
        }

        let current_url = driver.current_url().await.map(|u| u.to_string()).unwrap_or_else(|_| post_url.clone());
        let html = driver.source().await?;
        let extracted = match extract_post_from_html(&html, &current_url) {
            Some(post) => post,
            None => {
                eprintln!("[WARN] 포스트 파싱 실패: {post_url}");
                continue;
            }
        };

        // 댓글 수집
        let _ = driver.execute("window.scrollTo(0, document.body.scrollHeight)", vec![]).await;
        wait_for_comment_links(driver, 10).await;
        expand_all_comments(driver).await;
        let refreshed_html = driver.source().await?;
        let refreshed_url = driver.current_url().await.map(|u| u.to_string()).unwrap_or_else(|_| current_url.clone());
        let mut post = extract_post_from_html(&refreshed_html, &refreshed_url).unwrap_or(extracted);
        let raw_comments = extract_comments_from_driver(driver).await;
        post.comments = raw_comments
            .into_iter()
            .filter(|c| c.text.chars().count() >= cfg.min_comment_len)
            .take(cfg.max_comments_per_post)
            .collect();

        let article_number_s = article_number.to_string();
        let favorites_s = post.favorites.to_string();
        let comment_count_s = post.comments.len().to_string();
        let author_s = post.author.clone().unwrap_or_default();

        posts_writer.write_record([
            job.label.as_str(),
            job.keyword.as_str(),
            article_number_s.as_str(),
            post.date_text.as_str(),
            author_s.as_str(),
            post.article.as_str(),
            post.hashtags.as_str(),
            favorites_s.as_str(),
            comment_count_s.as_str(),
            post.post_url.as_str(),
            "instagram",
        ])?;

        for comment in &post.comments {
            comments_writer.write_record([
                article_number_s.as_str(),
                job.keyword.as_str(),
                comment.author.as_str(),
                comment.text.as_str(),
                comment.datetime.as_str(),
                comment.likes.as_str(),
            ])?;
        }

        posts_writer.flush()?;
        comments_writer.flush()?;
        article_number += 1;
        saved_posts += 1;
    }

    eprintln!("[INFO] {} ({}): 게시글 {}개 저장 완료", job.label, job.keyword, saved_posts);
    Ok(())
}

/// 해시태그 그리드 페이지를 스크롤하며 포스트 URL을 max_posts_per_tag개 이상 수집한다.
async fn collect_post_urls(driver: &WebDriver, cfg: &AppConfig, job: &TagJob) -> Result<Vec<String>> {
    // 무한스크롤이 작동하려면 창이 작아야 한다.
    // 창이 너무 크면 18개 썸네일이 모두 보여 스크롤 여지가 없고 센티넬이 안 트리거된다.
    let _ = driver.set_window_rect(0, 0, cfg.window_width, 800).await;

    let tag_url = format!("https://www.instagram.com/explore/tags/{}/", job.keyword);
    driver.goto(&tag_url).await?;
    sleep(Duration::from_secs(2)).await;
    dismiss_optional_dialogs(driver).await;

    // 그리드가 나타날 때까지 대기
    let _ = wait_for_first(
        driver,
        &[
            Locator::xpath("(//main//a[contains(@href, '/p/')])[1]"),
            Locator::css("main a[href*='/p/']"),
        ],
        30,
    ).await?;

    let mut seen: HashSet<String> = HashSet::new();
    let mut urls: Vec<String> = Vec::new();
    let mut no_new_streak = 0usize;

    loop {
        // JS로 현재 보이는 모든 포스트 링크 수집
        let result = driver.execute(
            r#"
            const links = Array.from(document.querySelectorAll('main a[href*="/p/"]'));
            return links.map(a => a.getAttribute('href')).filter(Boolean);
            "#,
            vec![],
        ).await;

        let hrefs: Vec<String> = match result {
            Ok(v) => serde_json::from_value(v.json().clone()).unwrap_or_default(),
            Err(_) => vec![],
        };

        let mut added = 0usize;
        for href in hrefs {
            let url = if href.starts_with("http") {
                href
            } else {
                format!("https://www.instagram.com{href}")
            };
            if seen.insert(url.clone()) {
                urls.push(url);
                added += 1;
            }
        }

        eprintln!("[INFO] {} ({}): URL {}개 수집 중 (+{})", job.label, job.keyword, urls.len(), added);

        if urls.len() >= cfg.max_posts_per_tag {
            break;
        }

        if added == 0 {
            no_new_streak += 1;
            if no_new_streak >= 6 {
                // 6회(약 18초) 연속 새 URL 없으면 그리드 끝으로 판단
                break;
            }
        } else {
            no_new_streak = 0;
        }

        // 마지막 링크로 scrollIntoView + End 키 병행
        let _ = driver.execute(r#"
            const links = Array.from(document.querySelectorAll('main a[href*="/p/"]'));
            if (links.length > 0) links[links.length - 1].scrollIntoView({ behavior: 'instant', block: 'end' });
            window.scrollTo(0, document.body.scrollHeight);
        "#, vec![]).await;
        if let Ok(body) = driver.find(By::Tag("body")).await {
            let _ = body.send_keys(Key::End).await;
        }
        sleep(Duration::from_millis(3000)).await;
    }

    urls.truncate(cfg.max_posts_per_tag);

    // 창 크기 복원
    let _ = driver.set_window_rect(0, 0, cfg.window_width, cfg.window_height).await;

    Ok(urls)
}

fn open_csv_writer(path: &Path, headers: &[&str]) -> Result<csv::Writer<File>> {
    let is_new = !path.exists() || path.metadata().map(|m| m.len() == 0).unwrap_or(true);
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open csv for append: {}", path.display()))?;
    let mut writer = WriterBuilder::new().has_headers(false).from_writer(file);
    if is_new {
        writer.write_record(headers)?;
        writer.flush()?;
    }
    Ok(writer)
}

async fn wait_for_post_ready(driver: &WebDriver, timeout_secs: u64) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if std::time::Instant::now() >= deadline {
            anyhow::bail!("wait_for_post_ready: timeout");
        }

        // 닫기 팝업: XPath로 부모 role=button 직접 탐색 후 클릭
        let close_xpaths = [
            "//*[@role='button' and .//*[local-name()='svg'][@aria-label='닫기']]",
            "//*[@role='button' and .//*[local-name()='svg'][@aria-label='Close']]",
            "//*[local-name()='svg'][@aria-label='닫기']",
            "//*[local-name()='svg'][@aria-label='Close']",
        ];
        let mut clicked = false;
        for xpath in &close_xpaths {
            if let Ok(el) = driver.find(By::XPath(*xpath)).await {
                if el.click().await.is_ok() {
                    clicked = true;
                    break;
                }
            }
        }
        if clicked {
            sleep(Duration::from_millis(800)).await;
            continue;
        }

        // 포스트 콘텐츠 확인
        let found = driver
            .execute("return !!document.querySelector('time[datetime]')", vec![])
            .await
            .ok()
            .and_then(|v| v.json().as_bool())
            .unwrap_or(false);
        if found {
            return Ok(());
        }

        sleep(Duration::from_millis(500)).await;
    }
}


async fn expand_all_comments(driver: &WebDriver) {
    // JS로 직접 클릭: WebDriver 선택자보다 훨씬 안정적
    let script = r#"
        // 1. 댓글 더 읽기 버튼 (SVG aria-label 기준)
        const loadMore = document.querySelector('svg[aria-label="댓글 더 읽어들이기"]');
        if (loadMore) {
            const btn = loadMore.closest('button');
            if (btn) { btn.click(); return 'load_more'; }
        }

        // 2. 답글 보기 버튼 (span._a9yi 기준, 첫 번째만)
        const replySpan = document.querySelector('span._a9yi');
        if (replySpan) {
            const btn = replySpan.closest('button') || replySpan;
            btn.click();
            return 'reply';
        }

        return null;
    "#;

    for _ in 0..30 {
        match driver.execute(script, vec![]).await {
            Ok(ret) if !ret.json().is_null() => {
                sleep(Duration::from_millis(400)).await;
            }
            _ => break,
        }
    }
}

async fn click_optional_button_by_text(driver: &WebDriver, texts: &[&str], timeout_secs: u64) -> bool {
    let mut locators = Vec::new();
    for text in texts {
        let literal = xpath_literal(text);
        locators.push(Locator::xpath(format!("//button[normalize-space()={}]", literal)));
        locators.push(Locator::xpath(format!("//*[@role='button' and normalize-space()={}]", literal)));
        locators.push(Locator::xpath(format!("//*[self::button or @role='button'][contains(normalize-space(.), {})]", literal)));
    }

    if let Ok(elem) = wait_for_first(driver, &locators, timeout_secs).await {
        click_element(&elem).await.is_ok()
    } else {
        false
    }
}



async fn wait_for_first(driver: &WebDriver, locators: &[Locator], timeout_secs: u64) -> Result<WebElement> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() <= deadline {
        for locator in locators {
            if let Ok(elem) = find_element(driver, locator).await {
                return Ok(elem);
            }
        }
        sleep(Duration::from_millis(POLL_MS)).await;
    }

    bail!("element not found in {}s", timeout_secs)
}

async fn find_element(driver: &WebDriver, locator: &Locator) -> WebDriverResult<WebElement> {
    match locator {
        Locator::Css(css) => driver.find(By::Css(css.as_str())).await,
        Locator::XPath(xpath) => driver.find(By::XPath(xpath.as_str())).await,
    }
}

async fn click_element(elem: &WebElement) -> Result<()> {
    let _ = elem.scroll_into_view().await;
    if elem.click().await.is_err() {
        sleep(Duration::from_millis(300)).await;
        elem.click().await?;
    }
    Ok(())
}

/// a[href*="/c/"] 댓글 퍼마링크가 DOM에 나타날 때까지 대기
async fn wait_for_comment_links(driver: &WebDriver, timeout_secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    while Instant::now() < deadline {
        let found = driver
            .execute("return document.querySelectorAll('a[href*=\"/c/\"]').length", vec![])
            .await
            .ok()
            .and_then(|r| r.json().as_u64())
            .unwrap_or(0);
        if found > 0 {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
    eprintln!("[WARN] 댓글 링크(a[href*='/c/'])가 {}초 내에 나타나지 않았습니다", timeout_secs);
}

async fn extract_comments_from_driver(driver: &WebDriver) -> Vec<CommentRow> {
    // a[href*="/c/"] 기준으로 댓글 추출
    // h3 없이 username 패턴 링크(/username/)로 작성자 탐색
    let script = r#"
        const results = [];
        const seen = new Set();

        // /username/ 패턴 판별 (댓글 퍼마링크 /p/.../c/.../ 및 /explore/ 제외)
        function isUsernameHref(href) {
            return /^\/[^\/]+\/$/.test(href)
                && !href.includes('/p/')
                && !href.includes('/c/')
                && !href.includes('/explore/')
                && !href.includes('/reel/')
                && !href.includes('/stories/');
        }

        const anchors = Array.from(document.querySelectorAll('a[href*="/c/"]'));
        for (const anchor of anchors) {
            const permalink = anchor.getAttribute('href');
            if (!permalink || seen.has(permalink)) continue;

            // level3 = anchor의 3단계 위 (username+time div의 부모)
            // 구조: level3 > [div: username+time] + [div: 댓글텍스트]
            const level3 = anchor.parentElement?.parentElement?.parentElement;
            if (!level3) continue;

            // 텍스트: level3의 마지막 자식 div > span[dir="auto"]
            const textEl = level3.lastElementChild?.querySelector('span[dir="auto"]');
            const text = textEl ? textEl.textContent.trim() : '';
            if (!text) continue;

            // 작성자: level3 안의 username 패턴 링크
            let author = '';
            const links = Array.from(level3.querySelectorAll('a[href]'));
            for (const link of links) {
                const href = link.getAttribute('href') || '';
                if (isUsernameHref(href)) {
                    author = link.textContent.trim();
                    break;
                }
            }

            const timeEl = anchor.querySelector('time[datetime]');
            const datetime = timeEl ? timeEl.getAttribute('datetime') : '';

            seen.add(permalink);
            results.push({ author, text, datetime, likes: '' });
        }

        return results;
    "#;

    match driver.execute(script, vec![]).await {
        Ok(ret) => {
            let val = ret.json();
            let rows = val.as_array().map(|arr| {
                arr.iter().filter_map(|item| {
                    let author   = item["author"].as_str().unwrap_or("").to_string();
                    let text     = item["text"].as_str().unwrap_or("").to_string();
                    let datetime = item["datetime"].as_str().unwrap_or("").to_string();
                    let likes    = item["likes"].as_str().unwrap_or("").to_string();
                    if text.is_empty() { return None; }
                    Some(CommentRow { author, text, datetime, likes })
                }).collect::<Vec<_>>()
            }).unwrap_or_default();

            eprintln!("[DEBUG] 댓글 {}개 추출", rows.len());
            rows
        }
        Err(e) => {
            eprintln!("[WARN] JS 댓글 추출 실패: {e}");
            Vec::new()
        }
    }
}

fn extract_post_from_html(html: &str, post_url: &str) -> Option<ExtractedPost> {
    let doc = Html::parse_document(html);

    let date_text = select_first_attr(
        &doc,
        &[
            "div[role='dialog'] time[datetime]",
            "main article time[datetime]",
            "article time[datetime]",
            "time[datetime]",
        ],
        "datetime",
    )
    .map(|v| v.get(0..10).unwrap_or(v.as_str()).to_string())
    .unwrap_or_default();

    let author = select_first_text(
        &doc,
        &[
            "div[role='dialog'] header a[href^='/']",
            "main article header a[href^='/']",
            "article header a[href^='/']",
        ],
    );

    let li_nodes = select_all_best(
        &doc,
        &[
            "li._a9zj",
            "ul._a9ym li",
            "div[role='dialog'] article ul li",
            "main article ul li",
            "article ul li",
        ],
    );

    let mut article = String::new();
    let mut hashtags = String::new();
    let mut comments = Vec::new();
    let mut seen_comment_keys = HashSet::new();

    for li in li_nodes {
        let Some(li_author) = first_anchor_text(&li) else {
            continue;
        };
        let raw_text = best_text_from_li(&li);
        if raw_text.is_empty() {
            continue;
        }

        let cleaned = strip_author_prefix(&raw_text, &li_author);
        if cleaned.is_empty() {
            continue;
        }

        let key = format!("{}|{}", li_author, cleaned);
        if !seen_comment_keys.insert(key) {
            continue;
        }

        let looks_like_caption = match &author {
            Some(post_author) => post_author == &li_author && article.is_empty(),
            None => article.is_empty(),
        };

        if looks_like_caption {
            article = cleaned.clone();
            hashtags = hashtags_from_element(&li);
            if hashtags.is_empty() {
                hashtags = hashtags_from_text(&article);
            }
            continue;
        }

        comments.push(CommentRow {
            author: li_author,
            text: cleaned,
            datetime: String::new(),
            likes: String::new(),
        });
    }

    if article.is_empty() {
        article = select_first_attr(
            &doc,
            &[
                "meta[property='og:description']",
                "meta[name='description']",
            ],
            "content",
        )
        .unwrap_or_default();
        hashtags = hashtags_from_text(&article);
    }

    let favorites = extract_likes(&doc).unwrap_or(0);

    Some(ExtractedPost {
        post_url: post_url.to_string(),
        date_text,
        author,
        article,
        hashtags,
        favorites,
        comments,
    })
}

fn extract_likes(doc: &Html) -> Option<i64> {
    let meta_content = select_first_attr(
        doc,
        &[
            "meta[property='og:description']",
            "meta[name='description']",
        ],
        "content",
    )
    .or_else(|| {
        select_first_text(
            doc,
            &[
                "div[role='dialog'] section",
                "main article section",
                "article section",
            ],
        )
    })?;

    parse_likes_from_text(&meta_content)
}

fn parse_likes_from_text(text: &str) -> Option<i64> {
    let lower = text.to_ascii_lowercase();
    for keyword in [" likes", " like", "좋아요"] {
        if let Some(idx) = lower.find(keyword) {
            let prefix = text[..idx].trim();
            let candidate = prefix.split_whitespace().last()?;
            if let Some(value) = parse_human_number(candidate) {
                return Some(value);
            }
        }
    }
    None
}

fn parse_human_number(token: &str) -> Option<i64> {
    let cleaned = token
        .trim()
        .trim_matches(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == ','))
        .replace(',', "");

    if cleaned.is_empty() {
        return None;
    }

    if cleaned.ends_with('K') || cleaned.ends_with('k') {
        let value = &cleaned[..cleaned.len() - 1];
        let n = value.parse::<f64>().ok()? * 1_000.0;
        return Some(n.round() as i64);
    }

    if cleaned.ends_with('M') || cleaned.ends_with('m') {
        let value = &cleaned[..cleaned.len() - 1];
        let n = value.parse::<f64>().ok()? * 1_000_000.0;
        return Some(n.round() as i64);
    }

    cleaned.parse::<i64>().ok()
}

fn select_all_best<'a>(doc: &'a Html, selectors: &[&str]) -> Vec<ElementRef<'a>> {
    for css in selectors {
        if let Ok(selector) = Selector::parse(css) {
            let nodes: Vec<_> = doc.select(&selector).collect();
            if !nodes.is_empty() {
                return nodes;
            }
        }
    }
    Vec::new()
}

fn select_first_text(doc: &Html, selectors: &[&str]) -> Option<String> {
    for css in selectors {
        let Ok(selector) = Selector::parse(css) else { continue; };
        if let Some(node) = doc.select(&selector).next() {
            let text = text_of(&node);
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

fn select_first_attr(doc: &Html, selectors: &[&str], attr: &str) -> Option<String> {
    for css in selectors {
        let Ok(selector) = Selector::parse(css) else { continue; };
        if let Some(node) = doc.select(&selector).next() {
            if let Some(value) = node.value().attr(attr) {
                let value = value.trim().to_string();
                if !value.is_empty() {
                    return Some(value);
                }
            }
        }
    }
    None
}

fn first_anchor_text(li: &ElementRef<'_>) -> Option<String> {
    let selector = Selector::parse("a[href^='/']").ok()?;
    li.select(&selector)
        .next()
        .map(|node| text_of(&node))
        .filter(|value| !value.is_empty())
}

fn best_text_from_li(li: &ElementRef<'_>) -> String {
    // Try Instagram-specific comment text spans first (most specific to least)
    for css in &[
        "div._a9zr span[dir='auto']",
        "span._ap3a[dir='auto']",
        "span[dir='auto']",
        "div._a9zr span",
        "span._ap3a",
    ] {
        if let Ok(selector) = Selector::parse(css) {
            let texts: Vec<String> = li
                .select(&selector)
                .map(|node| text_of(&node))
                .filter(|v| !v.is_empty())
                .collect();
            if !texts.is_empty() {
                return collapse_ws(&texts.join(" "));
            }
        }
    }
    // Generic fallback: all spans
    if let Ok(selector) = Selector::parse("span") {
        let texts: Vec<String> = li
            .select(&selector)
            .map(|node| text_of(&node))
            .filter(|v| !v.is_empty())
            .collect();
        if !texts.is_empty() {
            return collapse_ws(&texts.join(" "));
        }
    }
    text_of(li)
}

fn hashtags_from_element(li: &ElementRef<'_>) -> String {
    let selector = match Selector::parse("a[href*='/explore/tags/']") {
        Ok(s) => s,
        Err(_) => return String::new(),
    };

    let tags: Vec<String> = li
        .select(&selector)
        .map(|node| text_of(&node))
        .map(|tag| tag.trim_start_matches('#').to_string())
        .filter(|tag| !tag.is_empty())
        .collect();

    tags.join(" ")
}

fn hashtags_from_text(text: &str) -> String {
    text.split_whitespace()
        .filter_map(|token| token.strip_prefix('#'))
        .map(|tag| tag.trim_matches(|c: char| c.is_ascii_punctuation() && c != '_' && c != '-').to_string())
        .filter(|tag| !tag.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn strip_author_prefix(text: &str, author: &str) -> String {
    let text = collapse_ws(text);
    let author = collapse_ws(author);
    if author.is_empty() {
        return text;
    }

    if let Some(rest) = text.strip_prefix(&author) {
        return rest.trim().to_string();
    }

    text
}

fn text_of(node: &ElementRef<'_>) -> String {
    collapse_ws(&node.text().collect::<Vec<_>>().join(" "))
}

fn collapse_ws(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn safe_file_stem(value: &str) -> String {
    value
        .chars()
        .filter(|c| !matches!(c, '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|'))
        .collect()
}

fn xpath_literal(value: &str) -> String {
    if !value.contains('"') {
        return format!("\"{}\"", value);
    }
    if !value.contains('\'') {
        return format!("'{}'", value);
    }

    let mut parts = Vec::new();
    for (idx, part) in value.split('"').enumerate() {
        if idx > 0 {
            parts.push("'\"'".to_string());
        }
        if !part.is_empty() {
            parts.push(format!("\"{}\"", part));
        }
    }
    format!("concat({})", parts.join(", "))
}

fn parse_proxy(proxy: &str) -> Option<(String, u16, String)> {
    let proxy = proxy.trim();
    let (scheme, rest) = proxy.split_once("://")?;
    let authority = rest.rsplit('@').next()?;
    let (host, port_s) = authority.rsplit_once(':')?;
    let port = port_s.parse::<u16>().ok()?;
    Some((host.to_string(), port, scheme.to_string()))
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let s = v.trim().to_string();
        if s.is_empty() { None } else { Some(s) }
    })
}

fn env_string(key: &str) -> Option<String> {
    env::var(key).ok().and_then(|v| {
        let s = v.trim().to_string();
        if s.is_empty() { None } else { Some(s) }
    })
}

fn env_bool(key: &str) -> Option<bool> {
    env::var(key).ok().and_then(|v| match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    })
}

fn env_u64(key: &str) -> Option<u64> {
    env::var(key).ok().and_then(|v| v.trim().parse::<u64>().ok())
}

fn env_usize(key: &str) -> Option<usize> {
    env::var(key).ok().and_then(|v| v.trim().parse::<usize>().ok())
}
