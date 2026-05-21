use anyhow::{bail, Context, Result};
use csv::Writer;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use thirtyfour::extensions::cdp::ChromeDevTools;
use thirtyfour::prelude::*;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;

const MAX_PAGES_PER_PRODUCT: usize = 300;
const REVIEW_API_BATCH_SIZE: usize = 5;
const SLEEP_SHORT_MS: u64 = 800;
const SLEEP_MEDIUM_MS: u64 = 1500;

#[derive(Debug, Clone, Serialize)]
pub struct ReviewRow {
    pub product_url: String,
    pub page: u32,
    pub idx_in_page: usize,
    pub review: String,
    pub rating: Option<f32>,
    pub date: Option<String>,
    pub raw_text: String,
}

/// `workers`개의 Chrome 세션을 병렬로 띄워 상품 URL 목록을 크롤링한다.
/// 결과 Vec<ReviewRow>를 반환하며, CSV 저장은 호출자(main.rs)가 담당한다.
pub async fn run_plan_e_parallel(
    webdriver_url: &str,
    urls: Vec<String>,
    workers: usize,
    headless: bool,
) -> Result<Vec<ReviewRow>> {
    let workers = workers.max(1);
    let total = urls.len();
    println!("[INFO] Plan E 시작: 상품 {total}개 / 워커 {workers}개");

    let queue: Arc<Mutex<VecDeque<String>>> =
        Arc::new(Mutex::new(VecDeque::from(urls)));
    let mut joinset: JoinSet<Vec<ReviewRow>> = JoinSet::new();

    for worker_id in 0..workers {
        let wd      = webdriver_url.to_string();
        let queue   = queue.clone();

        joinset.spawn(async move {
            let profile_dir = smartstore_profile_base().join(format!("worker_{worker_id}"));
            let driver = match build_driver(&wd, headless, Some(&profile_dir)).await {
                Ok(d)  => d,
                Err(e) => {
                    eprintln!("[ERROR] 워커 {worker_id} 드라이버 생성 실패: {e}");
                    return vec![];
                }
            };
            println!("[INFO] 워커 {worker_id} 준비 완료");

            let mut results = Vec::new();
            loop {
                let url = queue.lock().await.pop_front();
                let Some(url) = url else { break };

                match crawl_product_reviews_next_only(&driver, &wd, &url, MAX_PAGES_PER_PRODUCT).await {
                    Ok(rows) => {
                        println!("[INFO] 워커 {worker_id} 완료: {url} | {}개", rows.len());
                        results.extend(rows);
                    }
                    Err(e) => {
                        eprintln!("[ERROR] 워커 {worker_id} 실패: {url}");
                        eprintln!("{e:#}");
                    }
                }
            }

            if let Err(e) = driver.quit().await {
                eprintln!("[WARN] 워커 {worker_id} driver.quit 실패: {e}");
            }
            println!("[INFO] 워커 {worker_id} 종료");
            results
        });
    }

    let mut all = Vec::new();
    while let Some(res) = joinset.join_next().await {
        match res {
            Ok(rows) => all.extend(rows),
            Err(e)   => eprintln!("[ERROR] Join 오류: {e}"),
        }
    }

    Ok(dedupe_rows(all))
}

fn smartstore_profile_base() -> PathBuf {
    std::env::var_os("SMARTSTORE_CHROME_PROFILE_BASE")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join("target")
                .join("smartstore_chrome_profiles")
        })
}

pub async fn build_driver(
    webdriver_url: &str,
    headless: bool,
    profile_dir: Option<&Path>,
) -> Result<WebDriver> {
    let mut caps = DesiredCapabilities::chrome();
    caps.add_arg("--start-maximized")?;
    caps.add_arg("--window-size=1600,1400")?;
    caps.add_arg("--lang=ko-KR")?;
    caps.add_arg("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36")?;
    caps.add_arg("--disable-blink-features=AutomationControlled")?;
    caps.add_experimental_option("excludeSwitches", vec!["enable-automation"])?;
    caps.add_experimental_option("useAutomationExtension", false)?;
    caps.set_base_capability("goog:loggingPrefs", json!({ "performance": "ALL" }))?;

    if headless {
        caps.add_arg("--headless=new")?;
    }

    if let Some(profile_dir) = profile_dir {
        std::fs::create_dir_all(profile_dir)
            .with_context(|| format!("Chrome profile directory create failed: {}", profile_dir.display()))?;
        caps.add_arg(&format!("--user-data-dir={}", profile_dir.display()))?;
    }

    let driver = WebDriver::new(webdriver_url, caps)
        .await
        .context("WebDriver 연결 실패")?;

    let dev_tools = ChromeDevTools::new(driver.handle.clone());
    let _ = dev_tools
        .execute_cdp_with_params(
            "Page.addScriptToEvaluateOnNewDocument",
            json!({
                "source": "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            }),
        )
        .await;

    driver
        .execute(
            r#"
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            "#,
            Vec::<serde_json::Value>::new(),
        )
        .await
        .ok();

    Ok(driver)
}

fn text_clean(x: &str) -> String {
    let ws = Regex::new(r"\s+").unwrap();
    ws.replace_all(x, " ").trim().to_string()
}

async fn safe_text(el: &WebElement) -> String {
    match el.text().await {
        Ok(t) => text_clean(&t),
        Err(_) => String::new(),
    }
}

async fn safe_click(driver: &WebDriver, element: &WebElement) -> bool {
    let _ = driver
        .execute(
            "arguments[0].scrollIntoView({block:'center'});",
            vec![element.to_json().unwrap_or(serde_json::Value::Null)],
        )
        .await;

    sleep(Duration::from_millis(200)).await;

    if element.click().await.is_ok() {
        return true;
    }

    driver
        .execute(
            "arguments[0].click();",
            vec![element.to_json().unwrap_or(serde_json::Value::Null)],
        )
        .await
        .is_ok()
}

async fn wait_until_review_cards_present(driver: &WebDriver, timeout_secs: u64) -> Result<bool> {
    let started = tokio::time::Instant::now();
    while started.elapsed() < Duration::from_secs(timeout_secs) {
        let cards = driver
            .find_all(By::Css(r#"[data-shp-inventory="revlist"]"#))
            .await
            .unwrap_or_default();

        let mut valid = Vec::new();
        for c in cards {
            let txt = safe_text(&c).await;
            if txt.len() < 10 {
                continue;
            }
            if txt.contains("최신순") && txt.contains("랭킹순") {
                continue;
            }
            if txt.contains("이전") && txt.contains("다음") {
                continue;
            }
            if txt.contains("전체보기") && (txt.contains("포토/동영상") || txt.contains("리뷰 유형")) {
                continue;
            }
            valid.push(c);
        }

        if !valid.is_empty() {
            return Ok(true);
        }
        sleep(Duration::from_millis(300)).await;
    }
    Ok(false)
}

/// 네이버 reCAPTCHA가 감지되면 최대 `timeout_secs`초 동안 사용자가 풀 때까지 대기.
async fn wait_if_captcha(driver: &WebDriver, timeout_secs: u64) {
    let captcha_selectors = [
        "iframe[src*='recaptcha']",
        "iframe[src*='captcha']",
        "#captcha-container",
        ".captcha_wrap",
        "#naver-recaptcha",
        "div[class*='captcha']",
    ];

    let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);
    let mut warned = false;

    loop {
        let has_captcha = {
            let mut found = false;
            for sel in &captcha_selectors {
                if driver.find_all(By::Css(*sel)).await.map(|v| !v.is_empty()).unwrap_or(false) {
                    found = true;
                    break;
                }
            }
            found
        };

        if !has_captcha {
            break;
        }

        if !warned {
            eprintln!("[CAPTCHA] 브라우저 창에서 캡챠를 직접 풀어주세요. 최대 {timeout_secs}초 대기...");
            warned = true;
        }

        if tokio::time::Instant::now() >= deadline {
            eprintln!("[CAPTCHA] 제한 시간 초과 — 캡챠 미해결 상태로 계속 진행합니다.");
            break;
        }

        sleep(Duration::from_secs(2)).await;
    }

    if warned {
        eprintln!("[CAPTCHA] 캡챠 해제 확인, 크롤링 계속합니다.");
        sleep(Duration::from_millis(SLEEP_MEDIUM_MS)).await;
    }
}

#[derive(Clone, Debug)]
struct ReviewQueryRequest {
    api_url: String,
    headers: Value,
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct WebDriverLogResponse {
    value: Vec<WebDriverLogEntry>,
}

#[derive(Debug, Deserialize)]
struct WebDriverLogEntry {
    message: String,
}

fn sanitize_review_headers(headers: &Map<String, Value>) -> Value {
    let mut out = Map::new();
    for (key, value) in headers {
        let lower = key.to_ascii_lowercase();
        let allowed = matches!(
            lower.as_str(),
            "accept"
                | "content-type"
                | "x-client-lct"
                | "x-client-rtk"
                | "x-client-rts"
                | "x-client-version"
                | "x-service-type"
        );
        if allowed {
            out.insert(key.clone(), value.clone());
        }
    }
    Value::Object(out)
}

fn normalize_smartstore_product_url(raw_url: &str) -> String {
    let Ok(mut parsed) = ::url::Url::parse(raw_url) else {
        return raw_url.to_string();
    };

    let is_smartstore = parsed
        .host_str()
        .map(|host| host.eq_ignore_ascii_case("smartstore.naver.com"))
        .unwrap_or(false);
    if !is_smartstore || !parsed.path().contains("/products/") {
        return raw_url.to_string();
    }

    parsed.set_query(None);
    parsed.set_fragment(Some("REVIEW"));
    parsed.to_string()
}

fn review_query_request_from_log(entry: &WebDriverLogEntry) -> Option<ReviewQueryRequest> {
    let event: Value = serde_json::from_str(&entry.message).ok()?;
    let message = event.get("message")?;
    if message.get("method")?.as_str()? != "Network.requestWillBeSent" {
        return None;
    }

    let request = message.get("params")?.get("request")?;
    let api_url = request.get("url")?.as_str()?;
    if !api_url.contains("/i/v1/contents/reviews/query-pages") {
        return None;
    }

    let payload: Value = serde_json::from_str(request.get("postData")?.as_str()?).ok()?;
    let headers = sanitize_review_headers(request.get("headers")?.as_object()?);

    Some(ReviewQueryRequest {
        api_url: api_url.to_string(),
        headers,
        payload,
    })
}

async fn read_performance_logs(driver: &WebDriver, webdriver_url: &str) -> Result<Vec<WebDriverLogEntry>> {
    let session_id = driver.session_id().to_string();
    let endpoint = format!(
        "{}/session/{}/log",
        webdriver_url.trim_end_matches('/'),
        session_id
    );
    let response = reqwest::Client::new()
        .post(endpoint)
        .json(&json!({ "type": "performance" }))
        .send()
        .await
        .context("WebDriver performance log request failed")?;

    let status = response.status();
    let body = response
        .text()
        .await
        .context("WebDriver performance log response read failed")?;
    if !status.is_success() {
        bail!("WebDriver performance log failed: {status} {body}");
    }

    let parsed: WebDriverLogResponse =
        serde_json::from_str(&body).context("WebDriver performance log parse failed")?;
    Ok(parsed.value)
}

async fn wait_for_review_query_request(
    driver: &WebDriver,
    webdriver_url: &str,
    timeout_secs: u64,
) -> Result<ReviewQueryRequest> {
    let started = tokio::time::Instant::now();
    let mut last_request = None;

    while started.elapsed() < Duration::from_secs(timeout_secs) {
        let logs = read_performance_logs(driver, webdriver_url).await.unwrap_or_default();
        for entry in &logs {
            if let Some(request) = review_query_request_from_log(entry) {
                last_request = Some(request);
            }
        }

        if let Some(request) = last_request.take() {
            return Ok(request);
        }

        sleep(Duration::from_millis(400)).await;
    }

    bail!("SmartStore review API request was not captured");
}

async fn click_review_all_button(driver: &WebDriver) -> Result<bool> {
    let js = r#"
    const done = arguments[arguments.length - 1];
    const REVIEW = '\uB9AC\uBDF0';
    const ALL = '\uC804\uCCB4\uBCF4\uAE30';
    const textOf = (el) => (el.innerText || el.textContent || '').replace(/\s+/g, ' ').trim();
    const visible = (el) => !!(el.offsetParent || el.getClientRects().length);
    let attempts = 0;

    function step() {
        const controls = Array.from(document.querySelectorAll('button,a,[role="button"]'));
        const target = controls.find((el) => {
            const text = textOf(el);
            return visible(el) && text.includes(REVIEW) && text.includes(ALL);
        });

        if (target) {
            target.scrollIntoView({ block: 'center' });
            setTimeout(() => {
                target.click();
                done(true);
            }, 150);
            return;
        }

        if (attempts === 0) {
            const tab = controls.find((el) => {
                const text = textOf(el);
                return visible(el) && text.includes(REVIEW) && !text.includes(ALL) && text.length < 40;
            });
            if (tab) {
                tab.scrollIntoView({ block: 'center' });
                tab.click();
            }
        }

        window.scrollBy(0, Math.max(500, window.innerHeight * 0.7));
        attempts += 1;
        if (attempts >= 12) {
            done(false);
        } else {
            setTimeout(step, 450);
        }
    }

    step();
    "#;

    let clicked: bool = driver
        .execute_async(js, Vec::<Value>::new())
        .await
        .ok()
        .and_then(|v| serde_json::from_value(v.json().clone()).ok())
        .unwrap_or(false);

    if clicked {
        sleep(Duration::from_millis(SLEEP_SHORT_MS)).await;
    }

    Ok(clicked)
}

async fn open_product_and_capture_review_query(
    driver: &WebDriver,
    webdriver_url: &str,
    url: &str,
    timeout_secs: u64,
) -> Result<ReviewQueryRequest> {
    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 1..=3 {
        let _ = read_performance_logs(driver, webdriver_url).await;

        driver.get(url).await?;
        driver
            .query(By::Tag("body"))
            .wait(Duration::from_secs(timeout_secs), Duration::from_millis(300))
            .first()
            .await
            .context("body load failed")?;

        let captcha_wait = if attempt == 1 { 120 } else { 10 };
        wait_if_captcha(driver, captcha_wait).await;
        sleep(Duration::from_millis(SLEEP_MEDIUM_MS)).await;

        let clicked = click_review_all_button(driver).await?;
        if !clicked {
            println!("[WARN] review-all button was not found on attempt {attempt}");
        }

        match wait_for_review_query_request(driver, webdriver_url, timeout_secs).await {
            Ok(request) => return Ok(request),
            Err(e) => {
                last_error = Some(e);
                if attempt < 3 {
                    println!("[WARN] SmartStore review API capture retry {attempt}/3");
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    match last_error {
        Some(e) => Err(e).context("SmartStore review API capture failed"),
        None => bail!("SmartStore review API capture failed"),
    }
}

async fn fetch_review_pages_batch(
    driver: &WebDriver,
    request: &ReviewQueryRequest,
    pages: &[u32],
) -> Result<Vec<Value>> {
    let js = r#"
    const url = arguments[0];
    const headers = arguments[1];
    const basePayload = arguments[2];
    const pages = arguments[3];
    const done = arguments[arguments.length - 1];

    Promise.all(pages.map(async (page) => {
        const payload = Object.assign({}, basePayload, { page });
        const response = await fetch(url, {
            method: 'POST',
            headers,
            credentials: 'include',
            body: JSON.stringify(payload),
        });
        const text = await response.text();
        if (!response.ok) {
            return { __error: true, page, status: response.status, body: text.slice(0, 1000) };
        }
        try {
            return JSON.parse(text);
        } catch (error) {
            return { __error: true, page, status: response.status, body: text.slice(0, 1000) };
        }
    })).then(done).catch((error) => done([{ __error: true, message: String(error) }]));
    "#;

    let value = driver
        .execute_async(
            js,
            vec![
                Value::String(request.api_url.clone()),
                request.headers.clone(),
                request.payload.clone(),
                json!(pages),
            ],
        )
        .await
        .context("SmartStore review API fetch failed")?;

    let pages: Vec<Value> =
        serde_json::from_value(value.json().clone()).context("SmartStore review API JSON parse failed")?;

    for page in &pages {
        if page.get("__error").and_then(Value::as_bool).unwrap_or(false) {
            bail!("SmartStore review API page fetch failed: {page}");
        }
    }

    Ok(pages)
}

fn api_review_date(item: &Value) -> Option<String> {
    let date = item.get("createDate")?.as_str()?.trim();
    if date.len() >= 10 {
        Some(date[..10].to_string())
    } else if date.is_empty() {
        None
    } else {
        Some(date.to_string())
    }
}

fn review_rows_from_api_page(product_url: &str, page_no: u32, page: &Value) -> Vec<ReviewRow> {
    let mut rows = Vec::new();
    let Some(contents) = page.get("contents").and_then(Value::as_array) else {
        return rows;
    };

    for (idx, item) in contents.iter().enumerate() {
        let review = item
            .get("reviewContent")
            .and_then(Value::as_str)
            .map(text_clean)
            .unwrap_or_default();
        if review.is_empty() {
            continue;
        }

        let rating = item
            .get("reviewScore")
            .and_then(Value::as_f64)
            .map(|value| value as f32);
        let date = api_review_date(item);
        let writer = item
            .get("maskedWriterId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let id = item.get("id").map(Value::to_string).unwrap_or_default();
        let raw_text = text_clean(&format!(
            "id={id} writer={writer} date={} rating={} content={review}",
            date.clone().unwrap_or_default(),
            rating.map(|value| value.to_string()).unwrap_or_default()
        ));

        rows.push(ReviewRow {
            product_url: product_url.to_string(),
            page: page_no,
            idx_in_page: idx + 1,
            review,
            rating,
            date,
            raw_text,
        });
    }

    rows
}

async fn crawl_product_reviews_with_review_api(
    driver: &WebDriver,
    webdriver_url: &str,
    url: &str,
    max_pages: usize,
) -> Result<Vec<ReviewRow>> {
    let crawl_url = normalize_smartstore_product_url(url);
    let request = open_product_and_capture_review_query(driver, webdriver_url, &crawl_url, 20).await?;
    let first_pages = fetch_review_pages_batch(driver, &request, &[1]).await?;
    let Some(first_page) = first_pages.first() else {
        return Ok(Vec::new());
    };

    let total_pages = first_page
        .get("totalPages")
        .and_then(Value::as_u64)
        .unwrap_or(1) as usize;
    let page_limit = total_pages.min(max_pages);
    println!("[INFO] SmartStore review API pages: {page_limit}/{total_pages}");

    let mut all_rows = review_rows_from_api_page(url, 1, first_page);

    let mut next_page = 2usize;
    while next_page <= page_limit {
        let end_page = (next_page + REVIEW_API_BATCH_SIZE - 1).min(page_limit);
        let pages: Vec<u32> = (next_page..=end_page).map(|page| page as u32).collect();
        let batch = fetch_review_pages_batch(driver, &request, &pages).await?;

        for (page_no, page_value) in pages.into_iter().zip(batch.iter()) {
            all_rows.extend(review_rows_from_api_page(url, page_no, page_value));
        }

        println!("[INFO] SmartStore review API fetched page {end_page}/{page_limit}");
        next_page = end_page + 1;
    }

    Ok(dedupe_rows(all_rows))
}

async fn open_product_and_go_review_tab(driver: &WebDriver, url: &str, timeout_secs: u64) -> Result<()> {
    driver.get(url).await?;
    driver
        .query(By::Tag("body"))
        .wait(Duration::from_secs(timeout_secs), Duration::from_millis(300))
        .first()
        .await
        .context("body 로드 실패")?;

    // 페이지 로드 후 캡챠 여부 확인 — 있으면 사용자가 풀 때까지 대기
    wait_if_captcha(driver, 120).await;

    sleep(Duration::from_millis(SLEEP_MEDIUM_MS)).await;

    let review_tab_selectors = vec![
        By::Css(r#"a[data-name="REVIEW"]"#),
        By::XPath("//a[contains(normalize-space(.), '리뷰')]") ,
        By::XPath("//button[contains(normalize-space(.), '리뷰')]") ,
        By::XPath("//*[@role='tab'][contains(normalize-space(.), '리뷰')]") ,
    ];

    let mut clicked = false;
    for by in review_tab_selectors {
        let elems = driver.find_all(by).await.unwrap_or_default();
        for el in elems {
            if !el.is_displayed().await.unwrap_or(false) {
                continue;
            }
            let txt = safe_text(&el).await;
            let data_name = el.attr("data-name").await.ok().flatten().unwrap_or_default();
            if data_name.trim() == "REVIEW" || txt.contains("리뷰") {
                if safe_click(driver, &el).await {
                    clicked = true;
                    break;
                }
            }
        }
        if clicked {
            break;
        }
    }

    if !clicked {
        bail!("리뷰 탭 클릭 실패");
    }

    if !wait_until_review_cards_present(driver, timeout_secs).await? {
        bail!("리뷰 목록이 로드되지 않았습니다");
    }

    sleep(Duration::from_millis(SLEEP_MEDIUM_MS)).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_smartstore_review_api_content_to_review_rows() {
        let page = json!({
            "contents": [{
                "id": 4967111202_u64,
                "reviewScore": 5,
                "reviewContent": "  good\nwasher  ",
                "createDate": "2026-05-02T06:27:58.071+00:00",
                "maskedWriterId": "z2****"
            }]
        });

        let rows = review_rows_from_api_page("https://smartstore.naver.com/s/products/1", 3, &page);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].page, 3);
        assert_eq!(rows[0].idx_in_page, 1);
        assert_eq!(rows[0].review, "good washer");
        assert_eq!(rows[0].rating, Some(5.0));
        assert_eq!(rows[0].date.as_deref(), Some("2026-05-02"));
        assert!(rows[0].raw_text.contains("z2****"));
    }

    #[test]
    fn keeps_only_fetch_safe_review_headers() {
        let headers = json!({
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Cookie": "NID=secret",
            "Referer": "https://smartstore.naver.com/s/products/1",
            "x-client-lct": "/s/products/1",
            "x-client-rtk": "token",
            "x-client-rts": "123",
            "x-client-version": "20260521092348",
            "x-service-type": "NONE"
        });

        let sanitized = sanitize_review_headers(headers.as_object().unwrap());
        let map = sanitized.as_object().unwrap();

        assert!(map.contains_key("Accept"));
        assert!(map.contains_key("Content-Type"));
        assert!(map.contains_key("x-client-rtk"));
        assert!(!map.contains_key("Cookie"));
        assert!(!map.contains_key("Referer"));
    }

    #[test]
    fn normalizes_smartstore_product_urls_before_browser_entry() {
        let normalized = normalize_smartstore_product_url(
            "https://smartstore.naver.com/lgbestjisung/products/7779247404?NaPm=tracking#REVIEW",
        );

        assert_eq!(
            normalized,
            "https://smartstore.naver.com/lgbestjisung/products/7779247404#REVIEW"
        );
    }

    #[test]
    fn write_csv_creates_empty_output_file() {
        let path = Path::new("target").join("plan_e_empty_reviews.csv");
        let _ = std::fs::remove_file(&path);

        write_csv(path.to_str().unwrap(), &[]).unwrap();

        assert!(path.exists());
        let _ = std::fs::remove_file(&path);
    }
}

async fn try_click_latest_order(driver: &WebDriver) -> bool {
    let xpaths = vec![
        "//a[contains(normalize-space(.), '최신순')]",
        "//button[contains(normalize-space(.), '최신순')]",
        "//*[@role='button'][contains(normalize-space(.), '최신순')]",
    ];

    for xp in xpaths {
        let elems = driver.find_all(By::XPath(xp)).await.unwrap_or_default();
        for el in elems {
            if el.is_displayed().await.unwrap_or(false) && safe_click(driver, &el).await {
                sleep(Duration::from_millis(SLEEP_SHORT_MS)).await;
                return true;
            }
        }
    }
    false
}

async fn expand_review_more_buttons(driver: &WebDriver, _max_clicks: usize) -> usize {
    // JS로 접힌 더보기 버튼 전체를 한 번에 클릭
    let js = r#"
    (() => {
        const btns = Array.from(document.querySelectorAll('a.DpXj3MxW8W[aria-expanded="false"]'));
        let count = 0;
        for (const btn of btns) {
            if (btn.offsetParent !== null) { btn.click(); count++; }
        }
        return count;
    })()
    "#;

    let count: usize = driver
        .execute(js, vec![])
        .await
        .ok()
        .and_then(|v| serde_json::from_value(v.json().clone()).ok())
        .unwrap_or(0);

    if count > 0 {
        sleep(Duration::from_millis(300)).await;
    }
    count
}

async fn collect_reviews_from_current_page(driver: &WebDriver, product_url: &str, page_no: u32) -> Vec<ReviewRow> {
    let expand_count = expand_review_more_buttons(driver, 200).await;
    println!("[INFO] 리뷰 영역 더보기 클릭 수: {expand_count}");

    // 리뷰 필터링 + 텍스트/평점/날짜 추출을 단일 JS 호출로 처리
    #[derive(serde::Deserialize)]
    struct RawReview {
        review: String,
        date: Option<String>,
        rating: Option<f32>,
        raw_text: String,
    }

    let js = r#"
    (() => {
        const DATE_RE = /(\d{2}\.\d{2}\.\d{2}\.|\d{4}\.\d{2}\.\d{2}\.|\d{4}-\d{2}-\d{2})/;
        const RATING_RE = /(?:평점\s*)?([1-5](?:\.\d)?)\s*점?/;
        const DROP = new Set(['리뷰 더보기/접기','평점','신고','한달사용리뷰','재구매','포토/동영상','스토어PICK','이 구매자의 처음 리뷰보기','최신순','랭킹순','전체보기','포토','동영상','도움돼요','도움이 돼요','답글','펼치기','더보기']);

        const candidates = Array.from(document.querySelectorAll('[data-shp-inventory="revlist"]'));
        const results = [];

        for (const c of candidates) {
            const txt = (c.innerText || c.textContent || '').replace(/\s+/g, ' ').trim();
            if (txt.length < 15) continue;
            if (txt.includes('랭킹순') && txt.includes('최신순')) continue;
            if (txt.includes('전체보기') && (txt.includes('포토/동영상') || txt.includes('리뷰 유형'))) continue;
            if (txt.includes('이전') && txt.includes('다음')) continue;
            if (!DATE_RE.test(txt)) continue;
            if (!txt.includes('신고') && !c.querySelector('a.DpXj3MxW8W')) continue;

            // 리뷰 텍스트 추출
            let review = '';
            const spans = Array.from(c.querySelectorAll('span.kUQb6452SL'));
            if (spans.length) {
                review = [...new Set(spans.map(s => s.innerText.trim()).filter(Boolean))].join(' ');
            }
            if (!review) {
                const div = c.querySelector('div.Tf5fecQ5mT');
                review = div ? (div.innerText || '').trim() : '';
            }
            if (!review) {
                review = txt.split('\n').map(l => l.trim()).filter(l =>
                    l.length >= 2 && !DROP.has(l) && !DATE_RE.test(l) &&
                    !/^[1-5](?:\.0)?$/.test(l) && !/^[A-Za-z0-9_*]+$/.test(l)
                ).join(' ').trim();
            }

            // 날짜
            const dm = txt.match(DATE_RE);
            const date = dm ? dm[0] : null;

            // 평점
            let rating = null;
            for (const el of c.querySelectorAll('[aria-label],[title]')) {
                const label = (el.getAttribute('aria-label') || el.getAttribute('title') || '');
                const m = label.match(RATING_RE);
                if (m) { const v = parseFloat(m[1]); if (v >= 1 && v <= 5) { rating = v; break; } }
            }
            if (rating === null) {
                for (const el of c.querySelectorAll('span,em,strong,div')) {
                    const t = (el.innerText || '').trim();
                    if (/^[1-5](?:\.0)?$/.test(t)) { rating = parseFloat(t); break; }
                }
            }

            results.push({ review: review.trim(), date, rating, raw_text: txt });
        }
        return results;
    })()
    "#;

    let raw_items: Vec<RawReview> = match driver.execute(js, vec![]).await {
        Ok(v) => serde_json::from_value(v.json().clone()).unwrap_or_default(),
        Err(_) => vec![],
    };

    let mut rows = Vec::new();
    let mut seen_page_reviews = HashSet::new();

    for (idx, item) in raw_items.into_iter().enumerate() {
        let key = text_clean(&item.review);
        if key.is_empty() || key.len() < 3 { continue; }
        if !seen_page_reviews.insert(key) { continue; }

        rows.push(ReviewRow {
            product_url: product_url.to_string(),
            page: page_no,
            idx_in_page: idx + 1,
            review: item.review,
            rating: item.rating,
            date: item.date,
            raw_text: item.raw_text,
        });
    }

    println!("[INFO] 현재 페이지 리뷰 수집: {}개", rows.len());
    rows
}

async fn get_pagination_container(driver: &WebDriver) -> Result<WebElement> {
    let el = driver
        .query(By::Css(r#"div[role="menubar"]"#))
        .wait(Duration::from_secs(10), Duration::from_millis(300))
        .first()
        .await
        .context("페이지네이션 컨테이너 탐색 실패")?;
    Ok(el)
}

async fn get_current_page(driver: &WebDriver) -> Result<u32> {
    let bar = get_pagination_container(driver).await?;
    let current_btn = bar
        .find(By::Css(r#"a[role="menuitem"][aria-current="true"]"#))
        .await
        .context("현재 페이지 버튼 탐색 실패")?;
    let txt = text_clean(&safe_text(&current_btn).await);
    txt.parse::<u32>()
        .with_context(|| format!("현재 페이지 숫자 파싱 실패: {txt}"))
}

async fn get_next_button(driver: &WebDriver) -> Option<WebElement> {
    let bar = get_pagination_container(driver).await.ok()?;
    let buttons = bar.find_all(By::Css(r#"a[role="button"]"#)).await.ok()?;

    for btn in buttons {
        let txt = text_clean(&safe_text(&btn).await);
        let aria_hidden = btn
            .attr("aria-hidden")
            .await
            .ok()
            .flatten()
            .unwrap_or_default()
            .to_lowercase();
        if txt == "다음" && aria_hidden != "true" {
            return Some(btn);
        }
    }
    None
}

async fn has_next_button(driver: &WebDriver) -> bool {
    get_next_button(driver).await.is_some()
}

async fn get_first_review_signature(driver: &WebDriver) -> String {
    let js = r#"
    (() => {
        const DATE_RE = /(\d{2}\.\d{2}\.\d{2}\.|\d{4}\.\d{2}\.\d{2}\.|\d{4}-\d{2}-\d{2})/;
        for (const c of document.querySelectorAll('[data-shp-inventory="revlist"]')) {
            const txt = (c.innerText || c.textContent || '').replace(/\s+/g, ' ').trim();
            if (txt.length < 15) continue;
            if (txt.includes('랭킹순') && txt.includes('최신순')) continue;
            if (txt.includes('이전') && txt.includes('다음')) continue;
            if (!DATE_RE.test(txt)) continue;
            return txt.substring(0, 200);
        }
        return '';
    })()
    "#;
    driver.execute(js, vec![]).await
        .ok()
        .and_then(|v| v.json().as_str().map(|s| s.to_string()))
        .unwrap_or_default()
}

async fn click_next_page_only(driver: &WebDriver) -> bool {
    let before_page = match get_current_page(driver).await {
        Ok(v) => v,
        Err(_) => return false,
    };
    let before_sig = get_first_review_signature(driver).await;
    let next_btn = match get_next_button(driver).await {
        Some(v) => v,
        None => {
            println!("[INFO] 다음 버튼 없음");
            return false;
        }
    };

    if !safe_click(driver, &next_btn).await {
        println!("[WARN] 다음 버튼 클릭 실패");
        return false;
    }

    let started = tokio::time::Instant::now();
    while started.elapsed() < Duration::from_secs(12) {
        if let Ok(after_page) = get_current_page(driver).await {
            if after_page != before_page {
                sleep(Duration::from_millis(SLEEP_MEDIUM_MS)).await;
                println!("[INFO] 다음 버튼 클릭: {before_page} -> {after_page}");
                return true;
            }
        }

        let after_sig = get_first_review_signature(driver).await;
        if !before_sig.is_empty() && !after_sig.is_empty() && after_sig != before_sig {
            sleep(Duration::from_millis(SLEEP_MEDIUM_MS)).await;
            match get_current_page(driver).await {
                Ok(after_page) => println!("[INFO] 다음 버튼 클릭: {before_page} -> {after_page}"),
                Err(_) => println!("[INFO] 다음 버튼 클릭 완료: {before_page} -> ?"),
            }
            return true;
        }
        sleep(Duration::from_millis(250)).await;
    }

    println!("[WARN] 다음 클릭 후 변화 감지 실패");
    false
}

async fn crawl_product_reviews_next_only(
    driver: &WebDriver,
    webdriver_url: &str,
    url: &str,
    max_pages: usize,
) -> Result<Vec<ReviewRow>> {
    println!("{}", "=".repeat(80));
    println!("[INFO] 상품 시작: {url}");

    match crawl_product_reviews_with_review_api(driver, webdriver_url, url, max_pages).await {
        Ok(rows) => {
            println!("[INFO] SmartStore review API collected {} rows", rows.len());
            return Ok(rows);
        }
        Err(e) => {
            eprintln!("[WARN] SmartStore review API failed; falling back to DOM pagination");
            eprintln!("{e:#}");
        }
    }

    open_product_and_go_review_tab(driver, url, 15).await?;
    let _ = try_click_latest_order(driver).await;

    let mut all_rows = Vec::new();
    let mut seen_keys: HashSet<String> = HashSet::new();
    let mut visited_pages: HashSet<u32> = HashSet::new();

    for _ in 0..max_pages {
        let current_page = get_current_page(driver).await?;

        if visited_pages.contains(&current_page) {
            println!("[INFO] 이미 방문한 페이지 {current_page} -> 종료");
            break;
        }

        visited_pages.insert(current_page);
        println!("[INFO] 현재 페이지: {current_page}");

        let rows = collect_reviews_from_current_page(driver, url, current_page).await;

        let mut new_count = 0usize;
        for row in rows {
            let key = format!(
                "{}\u{1f}|{}\u{1f}|{}\u{1f}|{}",
                row.product_url,
                row.date.clone().unwrap_or_default(),
                row.rating.map(|x| x.to_string()).unwrap_or_default(),
                row.review
            );
            if seen_keys.insert(key) {
                all_rows.push(row);
                new_count += 1;
            }
        }

        println!("[INFO] 중복 제거 후 추가: {new_count}개 | 누적: {}개", all_rows.len());

        if !has_next_button(driver).await {
            println!("[INFO] 마지막 페이지로 판단 -> 종료");
            break;
        }

        let moved = click_next_page_only(driver).await;
        if !moved {
            println!("[INFO] 다음 페이지 이동 실패 -> 종료");
            break;
        }
    }

    println!("[INFO] 상품 종료: {url} | 총 수집 {}개", all_rows.len());
    Ok(all_rows)
}

fn dedupe_rows(rows: Vec<ReviewRow>) -> Vec<ReviewRow> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for row in rows {
        let key = format!(
            "{}\u{1f}|{}\u{1f}|{}\u{1f}|{}",
            row.product_url,
            row.date.clone().unwrap_or_default(),
            row.rating.map(|x| x.to_string()).unwrap_or_default(),
            row.review
        );
        if seen.insert(key) {
            out.push(row);
        }
    }

    out
}

pub fn write_csv(path: &str, rows: &[ReviewRow]) -> Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("CSV output directory create failed: {}", parent.display()))?;
        }
    }

    let mut wtr = Writer::from_path(path).with_context(|| format!("CSV 생성 실패: {path}"))?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

