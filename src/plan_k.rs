/// Plan K — Goodreads 리뷰 병렬 크롤러 (thirtyfour / ChromeDriver)
///
/// 흐름:
///   1단계) 단일 드라이버(profile-dir) — 로그인 대기 → 쿠키 추출
///   2단계) Worker Pool — URL 큐에서 하나씩 가져가 무한 스크롤 + Show more reviews 수집
use std::collections::{HashMap, VecDeque};
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
pub struct PlanKConfig {
    /// 수집할 Goodreads 리뷰 페이지 URL 목록
    pub target_urls: Vec<String>,
    /// 결과 저장 디렉토리
    pub out_dir: String,
    /// 워커 헤드리스 모드 (로그인 창은 항상 비헤드리스)
    pub headless: bool,
    /// ChromeDriver 엔드포인트
    pub webdriver_url: String,
    /// Chrome 프로필 디렉토리 (로그인 세션 유지 — 로그인 단계에서만 사용)
    pub profile_dir: String,
    /// 쿠키 파일 경로 (있으면 로그인 단계 생략)
    pub cookie_file: Option<String>,
    /// 병렬 워커(Chrome 세션) 수
    pub workers: usize,
    /// 책당 수집할 최대 리뷰 수 (0 = 무제한)
    pub max_reviews: usize,
    /// 새 리뷰가 없을 때 종료까지 허용하는 연속 라운드 수
    pub max_idle_rounds: usize,
}

impl Default for PlanKConfig {
    fn default() -> Self {
        Self {
            target_urls: Vec::new(),
            out_dir: "goodreads_output".to_string(),
            headless: false,
            webdriver_url: "http://localhost:9515".to_string(),
            profile_dir: "goodreads_profile".to_string(),
            cookie_file: None,
            workers: 3,
            max_reviews: 0,
            max_idle_rounds: 5,
        }
    }
}

// ─────────────────────────────────────────
// 데이터 모델
// ─────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReviewRow {
    reviewer: String,
    rating: Option<i64>,
    date: String,
    review_url: String,
    review_text: String,
}

// ─────────────────────────────────────────
// 진입점
// ─────────────────────────────────────────

pub async fn run(cfg: PlanKConfig) -> Result<()> {
    if cfg.target_urls.is_empty() {
        return Err(anyhow!("수집할 URL이 없습니다. --url을 하나 이상 지정하세요."));
    }

    fs::create_dir_all(&cfg.out_dir)?;
    fs::create_dir_all(&cfg.profile_dir)?;

    info!(urls = cfg.target_urls.len(), workers = cfg.workers, "수집 대상");

    // 쿠키 획득: 파일 로드 or 수동 로그인
    let cookies = if let Some(ref path) = cfg.cookie_file {
        info!("쿠키 파일 로드: {path}");
        load_cookies_from_file(path)?
    } else {
        info!("1단계: Goodreads 로그인 창 열기");
        let cookies = login_and_get_cookies(&cfg).await?;
        let save_path = format!("{}/cookies.json", cfg.out_dir);
        if let Err(e) = save_cookies_to_file(&cookies, &save_path) {
            warn!("쿠키 저장 실패: {e}");
        } else {
            info!("쿠키 저장: {save_path}  (다음 실행 시 --cookie-file {save_path} --headless 사용 가능)");
        }
        cookies
    };
    info!(count = cookies.len(), "쿠키 준비 완료");

    // 병렬 수집
    info!("병렬 수집 시작 (headless={})", cfg.headless);
    let rows = scrape_parallel(&cfg, Arc::new(cookies)).await?;
    info!(total = rows.len(), "전체 수집 완료");

    let csv_path = format!("{}/goodreads_reviews.csv", cfg.out_dir);
    save_csv(&rows, &csv_path)?;

    Ok(())
}

// ─────────────────────────────────────────
// 1단계: 로그인 & 쿠키 추출
// ─────────────────────────────────────────

async fn login_and_get_cookies(cfg: &PlanKConfig) -> Result<Vec<thirtyfour::cookie::Cookie>> {
    let abs_profile = fs::canonicalize(&cfg.profile_dir)
        .unwrap_or_else(|_| std::path::PathBuf::from(&cfg.profile_dir));

    let mut caps = DesiredCapabilities::chrome();
    caps.add_arg("--window-size=1440,2200")?;
    caps.add_arg("--lang=en-US")?;
    caps.add_arg("--disable-blink-features=AutomationControlled")?;
    caps.add_experimental_option("excludeSwitches", vec!["enable-automation"])?;
    caps.add_experimental_option("useAutomationExtension", false)?;
    caps.add_arg(&format!("--user-data-dir={}", abs_profile.display()))?;

    let driver = WebDriver::new(&cfg.webdriver_url, caps)
        .await
        .with_context(|| format!("WebDriver 연결 실패: {}", cfg.webdriver_url))?;

    let first_url = cfg.target_urls.first().map(|s| s.as_str()).unwrap_or("https://www.goodreads.com/");
    wait_for_manual_login(&driver, first_url).await?;

    let cookies = driver.get_all_cookies().await.context("쿠키 추출 실패")?;
    driver.quit().await.ok();
    Ok(cookies)
}

// ─────────────────────────────────────────
// 2단계: 병렬 Worker Pool
// ─────────────────────────────────────────

async fn scrape_parallel(
    cfg: &PlanKConfig,
    cookies: Arc<Vec<thirtyfour::cookie::Cookie>>,
) -> Result<Vec<ReviewRow>> {
    let queue: Arc<Mutex<VecDeque<String>>> =
        Arc::new(Mutex::new(cfg.target_urls.iter().cloned().collect()));
    let results: Arc<Mutex<Vec<ReviewRow>>> = Arc::new(Mutex::new(Vec::new()));

    let worker_count = cfg.workers.min(cfg.target_urls.len());
    let mut join_set = JoinSet::new();

    for worker_id in 0..worker_count {
        let queue = Arc::clone(&queue);
        let results = Arc::clone(&results);
        let cookies = Arc::clone(&cookies);
        let cfg = cfg.clone();

        join_set.spawn(async move {
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
// 개별 워커
// ─────────────────────────────────────────

async fn run_worker(
    id: usize,
    queue: Arc<Mutex<VecDeque<String>>>,
    results: Arc<Mutex<Vec<ReviewRow>>>,
    cookies: Arc<Vec<thirtyfour::cookie::Cookie>>,
    cfg: PlanKConfig,
) -> Result<()> {
    let mut caps = DesiredCapabilities::chrome();
    caps.add_arg("--window-size=1440,2200")?;
    caps.add_arg("--lang=en-US")?;
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
    driver.goto("https://www.goodreads.com/").await?;
    sleep(Duration::from_millis(1500)).await;
    for cookie in cookies.iter() {
        driver.add_cookie(cookie.clone()).await.ok();
    }
    sleep(Duration::from_millis(500)).await;

    loop {
        let target_url = {
            let mut q = queue.lock().await;
            q.pop_front()
        };

        let target_url = match target_url {
            Some(u) => u,
            None => break,
        };

        info!("워커 {id} → 수집 시작: {target_url}");

        match crawl_one(&driver, &cfg, &target_url).await {
            Ok(rows) => {
                info!("워커 {id} → {}건 수집: {target_url}", rows.len());
                results.lock().await.extend(rows);
            }
            Err(e) => warn!("워커 {id} 실패 ({target_url}): {e:#}"),
        }

        sleep(Duration::from_millis(2000)).await;
    }

    driver.quit().await.ok();
    info!("워커 {id} 종료");
    Ok(())
}

// ─────────────────────────────────────────
// 단일 URL 전체 수집 (무한 스크롤)
// ─────────────────────────────────────────

async fn crawl_one(driver: &WebDriver, cfg: &PlanKConfig, target_url: &str) -> Result<Vec<ReviewRow>> {
    safe_goto(driver, target_url, 5, 1500).await?;
    sleep(Duration::from_millis(3000)).await;
    dismiss_overlays(driver).await?;

    let mut all_reviews: HashMap<String, ReviewRow> = HashMap::new();
    let mut idle_rounds = 0usize;
    let mut round_no = 0usize;

    loop {
        round_no += 1;
        info!("  [ROUND {round_no}] 누적 {}건", all_reviews.len());

        dismiss_overlays(driver).await?;

        let n1 = click_all_show_more(driver, 20).await?;
        if n1 > 0 {
            sleep(Duration::from_millis(1200)).await;
        }

        smart_scroll(driver, 4, 2200, 900).await;

        let n2 = click_all_show_more(driver, 20).await?;
        if n2 > 0 {
            sleep(Duration::from_millis(1000)).await;
        }

        let before = all_reviews.len();
        for row in extract_reviews(driver).await? {
            all_reviews.insert(review_key(&row), row);
        }
        let gained = all_reviews.len().saturating_sub(before);
        info!("  [ROUND {round_no}] 누적 {}건 (+{gained})", all_reviews.len());

        if cfg.max_reviews > 0 && all_reviews.len() >= cfg.max_reviews {
            info!("  목표 리뷰 수 {}건 도달 → 종료", cfg.max_reviews);
            break;
        }

        if click_show_more_reviews(driver).await? {
            idle_rounds = 0;
            sleep(Duration::from_millis(2500)).await;
            continue;
        }

        smart_scroll(driver, 2, 2200, 900).await;

        if click_show_more_reviews(driver).await? {
            idle_rounds = 0;
            sleep(Duration::from_millis(2500)).await;
            continue;
        }

        idle_rounds = if gained == 0 { idle_rounds + 1 } else { 0 };

        if idle_rounds >= cfg.max_idle_rounds {
            info!("  더 이상 새 리뷰 없음 → 종료");
            break;
        }
    }

    // 최종 확장
    let _ = click_all_show_more(driver, 20).await?;
    sleep(Duration::from_millis(1200)).await;
    for row in extract_reviews(driver).await? {
        all_reviews.insert(review_key(&row), row);
    }

    Ok(all_reviews.into_values().collect())
}

// ─────────────────────────────────────────
// 쿠키 저장 / 로드
// ─────────────────────────────────────────

fn save_cookies_to_file(cookies: &[thirtyfour::cookie::Cookie], path: &str) -> Result<()> {
    let saved: Vec<serde_json::Value> = cookies
        .iter()
        .map(|c| {
            serde_json::json!({
                "name":   c.name.clone(),
                "value":  c.value.clone(),
                "domain": c.domain.clone(),
                "path":   c.path.clone(),
            })
        })
        .collect();
    fs::write(path, serde_json::to_string_pretty(&saved)?)?;
    Ok(())
}

fn load_cookies_from_file(path: &str) -> Result<Vec<thirtyfour::cookie::Cookie>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("쿠키 파일 읽기 실패: {path}"))?;
    let raw: Vec<Value> = serde_json::from_str(&text)
        .with_context(|| "쿠키 JSON 파싱 실패")?;

    let mut cookies = Vec::new();
    for v in raw {
        let name = v["name"].as_str().unwrap_or("").to_string();
        let value = v["value"].as_str().unwrap_or("").to_string();
        if name.is_empty() { continue; }
        let mut c = thirtyfour::cookie::Cookie::new(&name, &value);
        if let Some(d) = v["domain"].as_str() { c.set_domain(d); }
        if let Some(p) = v["path"].as_str()   { c.set_path(p); }
        if let Some(s) = v["secure"].as_bool() { c.set_secure(s); }
        cookies.push(c);
    }

    info!(count = cookies.len(), path, "쿠키 로드 완료");
    Ok(cookies)
}

// ─────────────────────────────────────────
// 헬퍼: JS 실행
// ─────────────────────────────────────────

async fn eval_js(driver: &WebDriver, js: &str) -> Value {
    driver
        .execute(js, vec![])
        .await
        .ok()
        .map(|r| r.json().clone())
        .unwrap_or(Value::Null)
}

// ─────────────────────────────────────────
// 헬퍼: 페이지 안정화 대기
// ─────────────────────────────────────────

async fn wait_until_stable(driver: &WebDriver, timeout_ms: u64) {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        let ready = eval_js(driver, "return document.readyState;").await;
        let s = ready.as_str().unwrap_or("");
        if s == "interactive" || s == "complete" {
            return;
        }
        sleep(Duration::from_millis(500)).await;
    }
}

// ─────────────────────────────────────────
// 헬퍼: 안전한 페이지 이동 (재시도 포함)
// ─────────────────────────────────────────

async fn safe_goto(driver: &WebDriver, url: &str, retries: usize, wait_ms: u64) -> Result<()> {
    let mut last_err = String::new();
    for attempt in 1..=retries {
        match driver.goto(url).await {
            Ok(_) => {
                wait_until_stable(driver, 15_000).await;
                return Ok(());
            }
            Err(e) => {
                last_err = e.to_string();
                warn!("[재시도] goto 실패 ({}/{}): {}", attempt, retries, last_err);
                sleep(Duration::from_millis(wait_ms)).await;
            }
        }
    }
    Err(anyhow!("safe_goto 실패: {last_err}"))
}

// ─────────────────────────────────────────
// 헬퍼: 로그인 페이지 감지
// ─────────────────────────────────────────

async fn is_login_page(driver: &WebDriver) -> bool {
    let url = driver
        .current_url()
        .await
        .map(|u| u.to_string().to_lowercase())
        .unwrap_or_default();

    if url.contains("sign_in") || url.contains("login") {
        return true;
    }

    let body = eval_js(
        driver,
        "return document.body ? (document.body.innerText || '').toLowerCase() : '';",
    )
    .await;
    let body = body.as_str().unwrap_or("");
    ["sign in", "email", "password"]
        .iter()
        .filter(|k| body.contains(**k))
        .count()
        >= 2
}

// ─────────────────────────────────────────
// 헬퍼: 팝업 / 오버레이 닫기
// ─────────────────────────────────────────

async fn dismiss_overlays(driver: &WebDriver) -> Result<()> {
    let js = r#"
return (() => {
    function visible(el) {
        if (!el) return false;
        const s = window.getComputedStyle(el);
        const r = el.getBoundingClientRect();
        return s && s.display !== "none" && s.visibility !== "hidden" && r.width > 0 && r.height > 0;
    }
    function textMatch(el) {
        const t = ((el.innerText || el.textContent || "").trim()).toLowerCase();
        return t === "accept" || t === "i agree" || t === "got it";
    }
    let clicked = 0;
    for (const el of Array.from(document.querySelectorAll('button, [aria-label="Close"]'))) {
        const aria = (el.getAttribute("aria-label") || "").trim().toLowerCase();
        if ((textMatch(el) || aria === "close") && visible(el)) {
            try { el.scrollIntoView({ block: "center" }); } catch (_) {}
            try { el.click(); clicked++; } catch (_) {
                try { el.dispatchEvent(new MouseEvent("click", { bubbles: true })); clicked++; } catch (_) {}
            }
        }
    }
    return clicked;
})();"#;

    eval_js(driver, js).await;
    sleep(Duration::from_millis(500)).await;
    Ok(())
}

// ─────────────────────────────────────────
// 헬퍼: 수동 로그인 대기
// ─────────────────────────────────────────

async fn wait_for_manual_login(driver: &WebDriver, target_url: &str) -> Result<()> {
    safe_goto(driver, target_url, 5, 1500).await?;
    sleep(Duration::from_millis(2500)).await;

    if !is_login_page(driver).await {
        info!("[세션] 기존 로그인 상태 사용");
        return Ok(());
    }

    println!("\n[로그인 필요]");
    println!("1. 브라우저에서 Goodreads 로그인");
    println!("2. 로그인 완료 후 리뷰 페이지가 보이게 둠");
    print!("3. 완료 후 Enter: ");
    io::stdout().flush().ok();
    let mut buf = String::new();
    io::stdin().read_line(&mut buf)?;

    wait_until_stable(driver, 25_000).await;
    sleep(Duration::from_millis(2000)).await;

    // 아직 로그인 페이지면 리뷰 페이지로 재이동
    let cur = driver
        .current_url()
        .await
        .map(|u| u.to_string().to_lowercase())
        .unwrap_or_default();
    if cur.contains("sign_in") || is_login_page(driver).await {
        info!("[안내] 리뷰 페이지로 재이동 시도");
        safe_goto(driver, target_url, 5, 1500).await?;
        sleep(Duration::from_millis(3000)).await;
    }

    dismiss_overlays(driver).await?;

    if is_login_page(driver).await {
        return Err(anyhow!(
            "Goodreads 로그인 상태가 아닙니다. 로그인 후 Enter를 눌러야 합니다."
        ));
    }

    Ok(())
}

// ─────────────────────────────────────────
// 헬퍼: 소프트 스크롤
// ─────────────────────────────────────────

async fn smart_scroll(driver: &WebDriver, steps: usize, step_px: i64, wait_ms: u64) {
    for _ in 0..steps {
        eval_js(
            driver,
            &format!("window.scrollBy(0, {}); return true;", step_px),
        )
        .await;
        sleep(Duration::from_millis(wait_ms)).await;
    }
}

// ─────────────────────────────────────────
// 헬퍼: 리뷰 내 "Show more" 버튼 전체 클릭
// ─────────────────────────────────────────

async fn click_all_show_more(driver: &WebDriver, max_rounds: usize) -> Result<i64> {
    let js = format!(
        r#"
return (() => {{
    function visible(el) {{
        if (!el) return false;
        const s = window.getComputedStyle(el);
        const r = el.getBoundingClientRect();
        return s && s.display !== "none" && s.visibility !== "hidden" && r.width > 0 && r.height > 0;
    }}
    function safeClick(el) {{
        try {{ el.click(); return true; }} catch (_) {{
            try {{ el.dispatchEvent(new MouseEvent("click", {{ bubbles: true }})); return true; }} catch (_) {{ return false; }}
        }}
    }}
    let total = 0;
    for (let round = 0; round < {max_rounds}; round++) {{
        let hit = 0;
        for (const btn of Array.from(document.querySelectorAll("button"))) {{
            const text = ((btn.innerText || btn.textContent || "").replace(/\s+/g, " ").trim()).toLowerCase();
            if (text !== "show more") continue;
            if (!visible(btn)) continue;
            try {{ btn.scrollIntoView({{ block: "center" }}); }} catch (_) {{}}
            if (safeClick(btn)) {{ total++; hit++; }}
        }}
        if (hit === 0) break;
    }}
    return total;
}})();"#
    );

    let count = eval_js(driver, &js).await.as_i64().unwrap_or(0);
    if count > 0 {
        sleep(Duration::from_millis(250)).await;
    }
    Ok(count)
}

// ─────────────────────────────────────────
// 헬퍼: "Show more reviews" 버튼 클릭 (다음 배치 로드)
// ─────────────────────────────────────────

async fn click_show_more_reviews(driver: &WebDriver) -> Result<bool> {
    let js = r#"
return (() => {
    function visible(el) {
        if (!el) return false;
        const s = window.getComputedStyle(el);
        const r = el.getBoundingClientRect();
        return s && s.display !== "none" && s.visibility !== "hidden" && r.width > 0 && r.height > 0;
    }
    function safeClick(el) {
        try { el.click(); return true; } catch (_) {
            try { el.dispatchEvent(new MouseEvent("click", { bubbles: true })); return true; } catch (_) { return false; }
        }
    }
    const candidates = [];
    const byTestId = document.querySelector("button:has([data-testid='loadMore'])");
    if (byTestId) candidates.push(byTestId);
    for (const btn of Array.from(document.querySelectorAll("button"))) {
        const text = ((btn.innerText || btn.textContent || "").replace(/\s+/g, " ").trim()).toLowerCase();
        if (text === "show more reviews") candidates.push(btn);
    }
    for (const el of candidates) {
        if (!visible(el)) continue;
        try { el.scrollIntoView({ block: "center" }); } catch (_) {}
        if (safeClick(el)) return true;
    }
    return false;
})();"#;

    let ok = eval_js(driver, js).await.as_bool().unwrap_or(false);
    if ok {
        sleep(Duration::from_millis(2500)).await;
    }
    Ok(ok)
}

// ─────────────────────────────────────────
// 리뷰 DOM 추출
// ─────────────────────────────────────────

async fn extract_reviews(driver: &WebDriver) -> Result<Vec<ReviewRow>> {
    let js = r#"
return JSON.stringify((() => {
    function findRoot(node) {
        let el = node;
        for (let i = 0; i < 12 && el; i++, el = el.parentElement) {
            if (el.querySelector('a[href*="/review/show/"]') || el.querySelector('[aria-label*="Rating"]')) return el;
        }
        return node.closest('article, section, div') || node;
    }
    function getReviewer(root) {
        for (const sel of ['a[href*="/user/show/"]', 'a[href*="/review/list/"]', 'a[data-testid*="name"]']) {
            const el = root.querySelector(sel);
            if (el && (el.textContent || "").trim()) return el.textContent.trim();
        }
        return "";
    }
    function getDateAndUrl(root) {
        const el = root.querySelector('a[href*="/review/show/"]');
        if (!el) return { date: "", review_url: "" };
        return { date: (el.textContent || "").trim(), review_url: el.href || "" };
    }
    function getRating(root) {
        const el = root.querySelector('[aria-label*="Rating"]');
        if (!el) return null;
        const m = (el.getAttribute('aria-label') || '').match(/Rating\s+(\d+)\s+out of\s+5/i);
        return m ? Number(m[1]) : null;
    }
    return Array.from(document.querySelectorAll('[data-testid="contentContainer"]'))
        .filter(node => (node.innerText || "").trim())
        .map(node => {
            const root = findRoot(node);
            const { date, review_url } = getDateAndUrl(root);
            return {
                reviewer: getReviewer(root),
                rating:   getRating(root),
                date,
                review_url,
                review_text: (node.innerText || "").trim()
            };
        });
})());"#;

    let raw = eval_js(driver, js).await;
    let raw_str = raw.as_str().unwrap_or("[]");
    let rows: Vec<ReviewRow> = serde_json::from_str(raw_str).unwrap_or_default();

    let mut seen = std::collections::HashSet::new();
    let deduped = rows
        .into_iter()
        .filter(|r| {
            if r.review_text.trim().is_empty() {
                return false;
            }
            seen.insert(review_key(r))
        })
        .collect();

    Ok(deduped)
}

// ─────────────────────────────────────────
// 유틸
// ─────────────────────────────────────────

fn review_key(row: &ReviewRow) -> String {
    if !row.review_url.is_empty() {
        row.review_url.clone()
    } else {
        let prefix: String = row.review_text.chars().take(120).collect();
        format!("{}|{}|{}", row.reviewer, row.date, prefix)
    }
}

fn save_csv(rows: &[ReviewRow], path: &str) -> Result<()> {
    if rows.is_empty() {
        println!("수집된 리뷰가 없습니다.");
        return Ok(());
    }
    let mut file = File::create(path).with_context(|| format!("파일 생성 실패: {path}"))?;
    file.write_all(b"\xEF\xBB\xBF")?; // UTF-8 BOM
    let mut wtr = WriterBuilder::new().has_headers(true).from_writer(file);
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    info!(path, count = rows.len(), "CSV 저장 완료");
    Ok(())
}
