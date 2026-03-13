/// Plan J — Amazon 리뷰 병렬 크롤러 (thirtyfour / ChromeDriver)
///
/// 흐름:
///   1단계) 단일 드라이버(비헤드리스) — Amazon 로그인 대기 → 쿠키 추출
///   2단계) Worker Pool — 상품 URL 큐에서 워커가 하나씩 가져가 페이지 순차 수집
///          (Next 버튼 클릭으로 이동 — URL 직접 구성 시 1페이지 리다이렉트 문제 회피)
use std::collections::{HashSet, VecDeque};
use std::fs::{self, File};
use std::io::{self, Write as _};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use csv::WriterBuilder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thirtyfour::prelude::*;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};

// ─────────────────────────────────────────
// 설정 구조체
// ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PlanJConfig {
    /// 수집할 상품 리뷰 URL 목록 (여러 상품 가능)
    pub review_urls: Vec<String>,
    /// 상품당 수집할 최대 리뷰 수 (도달하면 다음 페이지 이동 중단)
    pub max_reviews: usize,
    /// 병렬 워커(Chrome 세션) 수
    pub workers: usize,
    /// ChromeDriver 엔드포인트
    pub webdriver_url: String,
    /// 헤드리스 모드 (쿠키 파일 제공 시 로그인도 헤드리스)
    pub headless: bool,
    /// 쿠키 파일 경로 (있으면 로그인 단계 생략, 완전 헤드리스 가능)
    pub cookie_file: Option<String>,
    /// Read More 버튼 자동 클릭
    pub click_read_more: bool,
    /// 결과 저장 디렉토리
    pub out_dir: String,
}

impl Default for PlanJConfig {
    fn default() -> Self {
        Self {
            review_urls: Vec::new(),
            max_reviews: 100,
            workers: 3,
            webdriver_url: "http://localhost:9515".into(),
            headless: false,
            cookie_file: None,
            click_read_more: true,
            out_dir: "amazon_output".into(),
        }
    }
}

// ─────────────────────────────────────────
// 쿠키 직렬화 구조체 (파일 저장/로드용)
// ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SavedCookie {
    name: String,
    value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    domain: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    secure: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_only: Option<bool>,
}

// ─────────────────────────────────────────
// 데이터 모델
// ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ReviewRow {
    product_url: String,
    page_number: usize,
    product_title: String,
    total_rating: String,
    total_review_count: String,
    review_id: String,
    author: String,
    review_title: String,
    rating: String,
    review_country: String,
    review_date: String,
    review_date_raw: String,
    verified_purchase: String,
    helpful_votes: String,
    review_text: String,
}

// ─────────────────────────────────────────
// 진입점
// ─────────────────────────────────────────

pub async fn run(cfg: PlanJConfig) -> Result<()> {
    if cfg.review_urls.is_empty() {
        return Err(anyhow!("수집할 URL이 없습니다. --url을 하나 이상 지정하세요."));
    }

    fs::create_dir_all(&cfg.out_dir)
        .with_context(|| format!("출력 디렉토리 생성 실패: {}", cfg.out_dir))?;

    info!(urls = cfg.review_urls.len(), workers = cfg.workers, "수집 대상 상품 수");

    // 쿠키 획득: 파일 로드 or 수동 로그인
    let cookies = if let Some(ref path) = cfg.cookie_file {
        info!("쿠키 파일 로드: {path}");
        load_cookies_from_file(path)?
    } else {
        info!("1단계: Amazon 로그인 창 열기 ({})", cfg.webdriver_url);
        let cookies = login_and_get_cookies(&cfg).await?;
        // 다음 실행을 위해 자동 저장
        let save_path = format!("{}/cookies.json", cfg.out_dir);
        if let Err(e) = save_cookies_to_file(&cookies, &save_path) {
            warn!("쿠키 파일 저장 실패: {e}");
        } else {
            info!("쿠키 저장 완료: {save_path}  (다음 실행 시 --cookie-file {save_path} --headless 사용 가능)");
        }
        cookies
    };
    info!(count = cookies.len(), "쿠키 준비 완료");

    // 병렬 상품 수집
    info!("병렬 수집 시작 (headless={})", cfg.headless);
    let rows = scrape_parallel(&cfg, Arc::new(cookies)).await?;
    info!(reviews = rows.len(), "전체 수집 완료");

    let csv_path = format!("{}/amazon_reviews.csv", cfg.out_dir);
    write_reviews_csv(&csv_path, &rows)?;

    Ok(())
}

// ─────────────────────────────────────────
// 쿠키 파일 저장 / 로드
// ─────────────────────────────────────────

fn save_cookies_to_file(
    cookies: &[thirtyfour::cookie::Cookie],
    path: &str,
) -> Result<()> {
    // thirtyfour 0.35: Cookie 필드는 pub (name, value, domain, path 등)
    let saved: Vec<SavedCookie> = cookies
        .iter()
        .map(|c| SavedCookie {
            name: c.name.clone(),
            value: c.value.clone(),
            domain: c.domain.clone(),
            path: c.path.clone(),
            secure: None,
            http_only: None,
        })
        .collect();
    let json = serde_json::to_string_pretty(&saved)?;
    fs::write(path, json)?;
    Ok(())
}

fn load_cookies_from_file(path: &str) -> Result<Vec<thirtyfour::cookie::Cookie>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("쿠키 파일 읽기 실패: {path}"))?;

    let raw: Vec<Value> = serde_json::from_str(&text)
        .with_context(|| format!("쿠키 JSON 파싱 실패: {path}"))?;

    let mut cookies = Vec::new();
    for v in raw {
        let name = v["name"].as_str().unwrap_or("").to_string();
        let value = v["value"].as_str().unwrap_or("").to_string();
        if name.is_empty() {
            continue;
        }
        let mut c = thirtyfour::cookie::Cookie::new(&name, &value);
        if let Some(domain) = v["domain"].as_str() {
            c.set_domain(domain);
        }
        if let Some(p) = v["path"].as_str() {
            c.set_path(p);
        }
        if let Some(secure) = v["secure"].as_bool() {
            c.set_secure(secure);
        }
        cookies.push(c);
    }

    info!(count = cookies.len(), path, "쿠키 파일 로드 완료");
    Ok(cookies)
}

// ─────────────────────────────────────────
// 1단계: 로그인 & 쿠키 추출
// ─────────────────────────────────────────

async fn login_and_get_cookies(cfg: &PlanJConfig) -> Result<Vec<thirtyfour::cookie::Cookie>> {
    let mut caps = DesiredCapabilities::chrome();
    caps.add_arg("--start-maximized")?;
    caps.add_arg("--disable-blink-features=AutomationControlled")?;
    caps.add_experimental_option("excludeSwitches", vec!["enable-automation"])?;
    caps.add_experimental_option("useAutomationExtension", false)?;

    let driver = WebDriver::new(&cfg.webdriver_url, caps)
        .await
        .with_context(|| format!("WebDriver 연결 실패: {}", cfg.webdriver_url))?;

    driver.goto("https://www.amazon.com/").await?;
    sleep(Duration::from_secs(2)).await;

    print!("\nAmazon 브라우저에서 로그인 완료 후 Enter를 누르세요... ");
    io::stdout().flush().ok();
    let mut buf = String::new();
    io::stdin().read_line(&mut buf)?;

    wait_stable(&driver, 45_000).await?;

    let cookies = driver.get_all_cookies().await.context("쿠키 추출 실패")?;
    driver.quit().await.ok();
    Ok(cookies)
}

// ─────────────────────────────────────────
// 2단계: 병렬 Worker Pool
// ─────────────────────────────────────────

async fn scrape_parallel(
    cfg: &PlanJConfig,
    cookies: Arc<Vec<thirtyfour::cookie::Cookie>>,
) -> Result<Vec<ReviewRow>> {
    // 상품 URL 큐
    let queue: Arc<Mutex<VecDeque<String>>> =
        Arc::new(Mutex::new(cfg.review_urls.iter().cloned().collect()));
    let results: Arc<Mutex<Vec<ReviewRow>>> = Arc::new(Mutex::new(Vec::new()));

    let worker_count = cfg.workers.min(cfg.review_urls.len());
    let mut join_set = JoinSet::new();

    for worker_id in 0..worker_count {
        let queue = Arc::clone(&queue);
        let results = Arc::clone(&results);
        let cookies = Arc::clone(&cookies);
        let cfg = cfg.clone();

        join_set.spawn(async move {
            // 워커 시작 시간을 살짝 분산 (동시 접속 완화)
            sleep(Duration::from_millis(worker_id as u64 * 1500)).await;
            if let Err(e) = run_worker(worker_id, queue, results, cookies, cfg).await {
                warn!("워커 {worker_id} 오류: {e:#}");
            }
        });
    }

    while join_set.join_next().await.is_some() {}

    let rows = Arc::try_unwrap(results)
        .map_err(|_| anyhow!("결과 Arc 해제 실패"))?
        .into_inner();

    Ok(rows)
}

// ─────────────────────────────────────────
// 개별 워커: URL 큐에서 상품 하나씩 처리
// ─────────────────────────────────────────

async fn run_worker(
    id: usize,
    queue: Arc<Mutex<VecDeque<String>>>,
    results: Arc<Mutex<Vec<ReviewRow>>>,
    cookies: Arc<Vec<thirtyfour::cookie::Cookie>>,
    cfg: PlanJConfig,
) -> Result<()> {
    let mut caps = DesiredCapabilities::chrome();
    caps.add_arg("--start-maximized")?;
    caps.add_arg("--disable-blink-features=AutomationControlled")?;
    caps.add_experimental_option("excludeSwitches", vec!["enable-automation"])?;
    caps.add_experimental_option("useAutomationExtension", false)?;
    if cfg.headless {
        caps.add_arg("--headless=new")?;
    }

    let driver = WebDriver::new(&cfg.webdriver_url, caps)
        .await
        .with_context(|| format!("워커 {id}: WebDriver 연결 실패"))?;

    // 쿠키 주입
    driver.goto("https://www.amazon.com/").await?;
    sleep(Duration::from_millis(1500)).await;
    for cookie in cookies.iter() {
        driver.add_cookie(cookie.clone()).await.ok();
    }
    sleep(Duration::from_millis(500)).await;

    loop {
        let product_url = {
            let mut q = queue.lock().await;
            q.pop_front()
        };

        let product_url = match product_url {
            Some(u) => u,
            None => break,
        };

        info!("워커 {id} → 상품 수집 시작: {product_url}");

        match scrape_product(&driver, &cfg, &product_url).await {
            Ok(rows) => {
                info!("워커 {id} → {}건 수집 완료: {product_url}", rows.len());
                results.lock().await.extend(rows);
            }
            Err(e) => {
                warn!("워커 {id} 실패 ({product_url}): {e:#}");
            }
        }

        sleep(Duration::from_millis(2000)).await;
    }

    driver.quit().await.ok();
    info!("워커 {id} 종료");
    Ok(())
}

// ─────────────────────────────────────────
// 단일 상품 전체 페이지 수집 (Next 버튼 클릭)
// ─────────────────────────────────────────

async fn scrape_product(
    driver: &WebDriver,
    cfg: &PlanJConfig,
    start_url: &str,
) -> Result<Vec<ReviewRow>> {
    // 페이지 1 이동
    driver.goto(start_url).await?;
    wait_stable(driver, 30_000).await?;
    sleep(Duration::from_millis(1500)).await;

    if is_blocked(driver).await {
        return Err(anyhow!("로그인/차단 페이지 감지"));
    }

    let mut all_rows: Vec<ReviewRow> = Vec::new();
    let mut page_num = 1usize;

    loop {
        let current_url = driver.current_url().await.map(|u| u.to_string()).unwrap_or_default();
        info!("  페이지 {page_num} 수집 중 (현재 {}건 / 목표 {}건)", all_rows.len(), cfg.max_reviews);

        soft_scroll(driver, 6, 300).await;

        if cfg.click_read_more {
            let n = click_read_more(driver).await;
            if n > 0 {
                info!("  페이지 {page_num}: Read More {n}회 클릭");
            }
        }

        let mut rows = extract_reviews(driver).await?;
        for row in &mut rows {
            row.page_number = page_num;
            row.product_url = current_url.clone();
        }

        info!("  페이지 {page_num} → {}건", rows.len());

        if rows.is_empty() {
            info!("  리뷰 없음 → 수집 종료");
            break;
        }

        all_rows.extend(rows);

        if all_rows.len() >= cfg.max_reviews {
            info!("  목표 리뷰 수 {}건 도달 → 종료 (수집: {}건)", cfg.max_reviews, all_rows.len());
            break;
        }

        // Next 버튼 클릭으로 페이지 이동
        match click_next_page(driver).await {
            NextPageResult::Clicked => {
                wait_stable(driver, 20_000).await?;
                sleep(Duration::from_millis(1500)).await;
                page_num += 1;
            }
            NextPageResult::NotFound => {
                info!("  Next 버튼 없음 → 마지막 페이지");
                break;
            }
            NextPageResult::Error(e) => {
                warn!("  Next 버튼 클릭 오류: {e}");
                break;
            }
        }
    }

    Ok(all_rows)
}

// ─────────────────────────────────────────
// Next 페이지 버튼 클릭
// ─────────────────────────────────────────

enum NextPageResult {
    Clicked,
    NotFound,
    Error(String),
}

async fn click_next_page(driver: &WebDriver) -> NextPageResult {
    // 실제 버튼 클릭 방식 — Amazon 리뷰 페이지네이션
    // HTML: <ul class="a-pagination"><li class="a-last"><a href="...">Next page</a></li></ul>
    let selectors = [
        "ul.a-pagination li.a-last a",
        "li.a-last a",
        ".a-pagination .a-last:not(.a-disabled) a",
        "a[data-hook=\"pagination-bar-next\"]",
    ];

    for sel in &selectors {
        match driver.find(By::Css(*sel)).await {
            Ok(el) => {
                // disabled 여부 확인 (부모 li에 a-disabled 클래스가 있으면 마지막 페이지)
                let is_disabled = driver
                    .execute(
                        &format!(
                            "const el = document.querySelector('{sel}'); \
                             return el ? el.closest('li')?.classList.contains('a-disabled') ?? false : true;"
                        ),
                        vec![],
                    )
                    .await
                    .ok()
                    .and_then(|r| r.json().as_bool())
                    .unwrap_or(false);

                if is_disabled {
                    return NextPageResult::NotFound;
                }

                match el.click().await {
                    Ok(_) => return NextPageResult::Clicked,
                    Err(e) => return NextPageResult::Error(e.to_string()),
                }
            }
            Err(_) => continue,
        }
    }

    NextPageResult::NotFound
}

// ─────────────────────────────────────────
// 헬퍼: 페이지 안정화 대기
// ─────────────────────────────────────────

async fn wait_stable(driver: &WebDriver, timeout_ms: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut last_url = String::new();
    let mut stable = 0usize;

    while Instant::now() < deadline {
        sleep(Duration::from_millis(800)).await;

        let cur = driver
            .current_url()
            .await
            .map(|u| u.to_string())
            .unwrap_or_default();

        let ready = driver
            .execute("return document.readyState;", vec![])
            .await
            .ok()
            .and_then(|r| r.json().as_str().map(|s| s.to_string()))
            .unwrap_or_default();

        if cur == last_url && (ready == "interactive" || ready == "complete") {
            stable += 1;
        } else {
            stable = 0;
        }
        last_url = cur;

        if stable >= 2 {
            return Ok(());
        }
    }

    sleep(Duration::from_millis(1500)).await;
    Ok(())
}

// ─────────────────────────────────────────
// 헬퍼: 로그인/차단 감지
// ─────────────────────────────────────────

async fn is_blocked(driver: &WebDriver) -> bool {
    let title = driver.title().await.unwrap_or_default().to_lowercase();
    let body = driver
        .execute(
            "return (document.body ? document.body.innerText : '').slice(0, 3000).toLowerCase();",
            vec![],
        )
        .await
        .ok()
        .and_then(|r| r.json().as_str().map(|s| s.to_string()))
        .unwrap_or_default();

    title.contains("sign-in")
        || title.contains("sign in")
        || body.contains("sign in or create account")
        || body.contains("robot")
        || body.contains("captcha")
}

// ─────────────────────────────────────────
// 헬퍼: 소프트 스크롤
// ─────────────────────────────────────────

async fn soft_scroll(driver: &WebDriver, steps: usize, delay_ms: u64) {
    let total = driver
        .execute("return document.body ? document.body.scrollHeight : 3000;", vec![])
        .await
        .ok()
        .and_then(|r| r.json().as_f64())
        .unwrap_or(3000.0);

    for i in 1..=steps {
        let y = (total * i as f64 / steps as f64) as i64;
        let _ = driver
            .execute(&format!("window.scrollTo(0, {y}); return true;"), vec![])
            .await;
        sleep(Duration::from_millis(delay_ms)).await;
    }

    let _ = driver
        .execute("window.scrollTo(0, 0); return true;", vec![])
        .await;
    sleep(Duration::from_millis(500)).await;
}

// ─────────────────────────────────────────
// 헬퍼: Read More 클릭
// ─────────────────────────────────────────

async fn click_read_more(driver: &WebDriver) -> usize {
    let js = r#"
return (() => {
  const selectors = [
    'span.a-expander-prompt',
    '[data-hook="review-body"] span.a-expander-prompt'
  ];
  let clicked = 0;
  for (const sel of selectors) {
    for (const node of Array.from(document.querySelectorAll(sel))) {
      const text = (node.innerText || node.textContent || '').trim().toLowerCase();
      if (!text.includes('read more')) continue;
      try { node.scrollIntoView({ block: 'center' }); node.click(); clicked++; } catch(_) {}
    }
  }
  for (const node of Array.from(document.querySelectorAll('*')).filter(el =>
    (el.innerText || el.textContent || '').trim().toLowerCase() === 'read more')) {
    try { node.scrollIntoView({ block: 'center' }); node.click(); clicked++; } catch(_) {}
  }
  return clicked;
})();"#;

    driver
        .execute(js, vec![])
        .await
        .ok()
        .and_then(|r| r.json().as_u64())
        .unwrap_or(0) as usize
}

// ─────────────────────────────────────────
// 리뷰 DOM 추출
// ─────────────────────────────────────────

async fn extract_reviews(driver: &WebDriver) -> Result<Vec<ReviewRow>> {
    let js = r#"
return JSON.stringify((() => {
  const txt = el => el ? (el.innerText || el.textContent || '').trim() : '';
  const attr = (el, name) => el ? (el.getAttribute(name) || '').trim() : '';

  const parseDateCountry = raw => {
    raw = (raw || '').trim();
    let m = raw.match(/Reviewed in\s+(.*?)\s+on\s+(.*)$/i);
    if (m) return { country: m[1].trim(), review_date: m[2].trim() };
    m = raw.match(/Review in\s+(.*?)\s+on\s+(.*)$/i);
    if (m) return { country: m[1].trim(), review_date: m[2].trim() };
    return { country: '', review_date: raw };
  };

  const getBody = review => {
    const candidates = [
      ...review.querySelectorAll('div[data-hook="review-expanded"] span'),
      ...review.querySelectorAll('div[data-hook="review-collapsed"] span'),
      ...review.querySelectorAll('span[data-hook="review-body"] span'),
      ...review.querySelectorAll('span[data-hook="review-body"]'),
      ...review.querySelectorAll('div.a-expander-content span'),
    ].map(el => txt(el)).filter(Boolean);
    if (!candidates.length) return '';
    candidates.sort((a, b) => b.length - a.length);
    return candidates[0];
  };

  const productLink = document.querySelector('a[data-hook="product-link"]');
  const productTitle = txt(productLink);
  const totalRating =
    txt(document.querySelector('span[data-hook="rating-out-of-text"]')) ||
    txt(document.querySelector('span.a-size-medium.a-color-base'));

  let totalReviewCount = '';
  for (const n of document.querySelectorAll(
    'div[data-hook="cr-filter-info-review-rating-count"] span, span[data-hook="total-review-count"]'
  )) {
    const v = txt(n);
    if (/\d/.test(v)) { totalReviewCount = v; break; }
  }

  const seen = new Set();
  const blocks = [
    ...document.querySelectorAll('div[data-hook="review"]'),
    ...document.querySelectorAll('div[id^="customer_review-"]'),
  ].filter(el => { if (seen.has(el)) return false; seen.add(el); return true; });

  return blocks.map(review => {
    const rawDate = txt(review.querySelector('span[data-hook="review-date"]'));
    const parsed = parseDateCountry(rawDate);
    const ratingText =
      txt(review.querySelector('i[data-hook="review-star-rating"] span')) ||
      txt(review.querySelector('i[data-hook="cmps-review-star-rating"] span')) ||
      txt(review.querySelector('span.a-icon-alt'));

    return {
      product_url: '',
      page_number: 0,
      product_title: productTitle,
      total_rating: totalRating,
      total_review_count: totalReviewCount,
      review_id: attr(review, 'id'),
      author: txt(review.querySelector('.a-profile-name')),
      review_title:
        txt(review.querySelector('a[data-hook="review-title"] span')) ||
        txt(review.querySelector('span[data-hook="review-title"]')),
      rating: ratingText,
      review_country: parsed.country,
      review_date: parsed.review_date,
      review_date_raw: rawDate,
      verified_purchase: txt(review.querySelector('span[data-hook="avp-badge"]')),
      helpful_votes: txt(review.querySelector('span[data-hook="helpful-vote-statement"]')),
      review_text: getBody(review),
    };
  });
})());"#;

    let raw = driver
        .execute(js, vec![])
        .await
        .context("리뷰 JS 실행 실패")?
        .json()
        .as_str()
        .unwrap_or("")
        .to_string();

    if raw.trim().is_empty() || raw == "null" {
        return Ok(Vec::new());
    }

    let rows: Vec<ReviewRow> = serde_json::from_str(&raw)
        .with_context(|| format!("리뷰 JSON 파싱 실패: {}", &raw[..raw.len().min(200)]))?;

    Ok(rows)
}

// ─────────────────────────────────────────
// CSV 출력
// ─────────────────────────────────────────

fn write_reviews_csv(path: &str, rows: &[ReviewRow]) -> Result<()> {
    if rows.is_empty() {
        println!("수집된 리뷰가 없습니다.");
        return Ok(());
    }

    let mut seen = HashSet::new();
    let deduped: Vec<&ReviewRow> = rows
        .iter()
        .filter(|r| {
            let key = format!("{}\x1f{}\x1f{}", r.review_id, r.author, r.review_text);
            seen.insert(key)
        })
        .collect();

    let mut file = File::create(path).with_context(|| format!("파일 생성 실패: {path}"))?;
    file.write_all(b"\xEF\xBB\xBF")?; // UTF-8 BOM

    let mut wtr = WriterBuilder::new().has_headers(true).from_writer(file);
    for row in &deduped {
        wtr.serialize(row)?;
    }
    wtr.flush()?;

    info!(path, count = deduped.len(), "CSV 저장 완료");
    println!("저장 완료: {path}  ({}건)", deduped.len());

    Ok(())
}
