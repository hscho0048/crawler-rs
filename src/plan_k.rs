use csv::Writer;
use headless_chrome::{Browser, LaunchOptionsBuilder, Tab};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

const TARGET_URL: &str = "https://www.goodreads.com/book/show/8576972/reviews?reviewFilters=eyJhZnRlciI6Ik1qUXhNQ3d4TXpjd01UazVOell5TURBdyJ9";
const OUTPUT_CSV: &str = "goodreads_reviews_8576972.csv";

const HEADLESS: bool = false;
const PROFILE_DIR: &str = "goodreads_profile";

const SCROLL_STEP_PX: i64 = 2200;
const SCROLL_WAIT_MS: u64 = 900;
const MAX_SCROLL_STEPS_PER_ROUND: usize = 4;
const MAX_IDLE_ROUNDS: usize = 5;

type AnyResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReviewRow {
    reviewer: String,
    rating: Option<i64>,
    date: String,
    review_url: String,
    review_text: String,
}

fn sleep_ms(ms: u64) {
    sleep(Duration::from_millis(ms));
}

fn eval_value(tab: &Arc<Tab>, js: &str) -> AnyResult<Value> {
    let result = tab.evaluate(js, false)?;
    Ok(result.value.unwrap_or(Value::Null))
}

fn eval_bool(tab: &Arc<Tab>, js: &str) -> AnyResult<bool> {
    let v = eval_value(tab, js)?;
    Ok(v.as_bool().unwrap_or(false))
}

fn eval_i64(tab: &Arc<Tab>, js: &str) -> AnyResult<i64> {
    let v = eval_value(tab, js)?;
    Ok(v.as_i64().unwrap_or(0))
}

fn wait_until_not_busy(tab: &Arc<Tab>, max_wait_ms: u64) -> AnyResult<()> {
    let start = Instant::now();

    while start.elapsed().as_millis() < max_wait_ms as u128 {
        let ready = eval_value(tab, "document.readyState")
            .ok()
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_default();

        if ready == "interactive" || ready == "complete" {
            return Ok(());
        }

        sleep_ms(500);
    }

    Ok(())
}

fn safe_goto(tab: &Arc<Tab>, url: &str, retries: usize, wait_ms: u64) -> AnyResult<()> {
    let mut last_err: Option<Box<dyn Error + Send + Sync>> = None;

    for attempt in 1..=retries {
        let result: AnyResult<()> = (|| {
            tab.navigate_to(url)?;
            tab.wait_until_navigated()?;
            wait_until_not_busy(tab, 15_000)?;
            Ok(())
        })();

        match result {
            Ok(_) => return Ok(()),
            Err(e) => {
                let msg = e.to_string().to_lowercase();
                last_err = Some(e);

                if msg.contains("interrupted by another navigation")
                    || msg.contains("net::err")
                    || msg.contains("timeout")
                {
                    eprintln!("[재시도] goto 충돌 감지 ({}/{})", attempt, retries);
                    sleep_ms(wait_ms);
                    continue;
                }

                break;
            }
        }
    }

    Err(last_err.unwrap_or_else(|| "safe_goto 실패".into()))
}

fn get_current_url(tab: &Arc<Tab>) -> String {
    tab.get_url()
        .map(|s| s.to_string())
        .unwrap_or_default()
        .to_lowercase()
}

fn get_body_text_lower(tab: &Arc<Tab>) -> String {
    eval_value(
        tab,
        r#"
        (() => {
            const body = document.body;
            return body ? (body.innerText || "").toLowerCase() : "";
        })()
        "#,
    )
    .ok()
    .and_then(|v| v.as_str().map(|s| s.to_string()))
    .unwrap_or_default()
}

fn is_login_page(tab: &Arc<Tab>) -> bool {
    let url = get_current_url(tab);
    if url.contains("sign_in") || url.contains("login") {
        return true;
    }

    let body = get_body_text_lower(tab);
    let keywords = ["sign in", "email", "password"];
    let hit = keywords.iter().filter(|k| body.contains(**k)).count();

    hit >= 2
}

fn dismiss_overlays(tab: &Arc<Tab>) -> AnyResult<()> {
    let js = r#"
    (() => {
        const selectors = [
            "button",
            "[aria-label='Close']",
            "button[aria-label='Close']"
        ];

        function visible(el) {
            if (!el) return false;
            const style = window.getComputedStyle(el);
            const rect = el.getBoundingClientRect();
            return style &&
                   style.display !== "none" &&
                   style.visibility !== "hidden" &&
                   rect.width > 0 &&
                   rect.height > 0;
        }

        function textMatch(el) {
            const t = ((el.innerText || el.textContent || "").trim()).toLowerCase();
            return t === "accept" || t === "i agree" || t === "got it";
        }

        let clicked = 0;

        for (const sel of selectors) {
            const nodes = Array.from(document.querySelectorAll(sel));
            for (const el of nodes) {
                const aria = (el.getAttribute("aria-label") || "").trim().toLowerCase();
                if ((textMatch(el) || aria === "close") && visible(el)) {
                    try {
                        el.scrollIntoView({ block: "center", inline: "center" });
                    } catch (_) {}
                    try {
                        el.click();
                        clicked += 1;
                    } catch (_) {
                        try {
                            el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
                            clicked += 1;
                        } catch (_) {}
                    }
                }
            }
        }
        return clicked;
    })()
    "#;

    let _ = eval_i64(tab, js)?;
    sleep_ms(500);
    Ok(())
}

fn wait_for_manual_login(tab: &Arc<Tab>) -> AnyResult<()> {
    safe_goto(tab, TARGET_URL, 5, 1500)?;
    sleep_ms(2500);

    if !is_login_page(tab) {
        println!("[세션] 기존 로그인 상태 사용");
        return Ok(());
    }

    println!("\n[로그인 필요]");
    println!("1. 브라우저에서 Goodreads 로그인");
    println!("2. 로그인 완료 후 같은 탭에서 리뷰 페이지가 보이게 둠");
    println!("3. 그 다음 터미널에서 Enter");
    print!("로그인 완료 후 Enter: ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;

    wait_until_not_busy(tab, 25_000)?;
    sleep_ms(2000);

    let current_url = get_current_url(tab);

    if current_url.contains("sign_in") || is_login_page(tab) {
        println!("[안내] 아직 로그인 페이지에 머물러 있어서 리뷰 페이지로 다시 이동 시도");
        safe_goto(tab, TARGET_URL, 5, 1500)?;
        sleep_ms(3000);
    }

    dismiss_overlays(tab)?;

    if is_login_page(tab) {
        return Err(
            "아직 Goodreads 로그인 상태가 아닙니다. 로그인 완료 후 리뷰 페이지가 열린 상태에서 Enter를 눌러야 합니다."
                .into(),
        );
    }

    Ok(())
}

fn smart_scroll(tab: &Arc<Tab>, steps: usize) -> AnyResult<()> {
    for _ in 0..steps {
        let js = format!("window.scrollBy(0, {});", SCROLL_STEP_PX);
        let _ = eval_value(tab, &js)?;
        sleep_ms(SCROLL_WAIT_MS);
    }
    Ok(())
}

fn click_all_review_show_more(tab: &Arc<Tab>, max_rounds: usize) -> AnyResult<i64> {
    let js = format!(
        r#"
        (() => {{
            function visible(el) {{
                if (!el) return false;
                const style = window.getComputedStyle(el);
                const rect = el.getBoundingClientRect();
                return style &&
                       style.display !== "none" &&
                       style.visibility !== "hidden" &&
                       rect.width > 0 &&
                       rect.height > 0;
            }}

            function safeClick(el) {{
                try {{
                    el.click();
                    return true;
                }} catch (_) {{
                    try {{
                        el.dispatchEvent(new MouseEvent("click", {{ bubbles: true, cancelable: true }}));
                        return true;
                    }} catch (_) {{
                        return false;
                    }}
                }}
            }}

            let clickedTotal = 0;

            for (let round = 0; round < {max_rounds}; round++) {{
                let clickedThisRound = 0;
                const buttons = Array.from(document.querySelectorAll("button"));

                for (const btn of buttons) {{
                    const text = ((btn.innerText || btn.textContent || "").replace(/\s+/g, " ").trim()).toLowerCase();
                    if (text !== "show more") continue;
                    if (!visible(btn)) continue;

                    try {{
                        btn.scrollIntoView({{ block: "center", inline: "center" }});
                    }} catch (_) {{}}

                    if (safeClick(btn)) {{
                        clickedTotal += 1;
                        clickedThisRound += 1;
                    }}
                }}

                if (clickedThisRound === 0) {{
                    break;
                }}
            }}

            return clickedTotal;
        }})()
        "#
    );

    let count = eval_i64(tab, &js)?;
    if count > 0 {
        sleep_ms(250);
    }
    Ok(count)
}

fn click_show_more_reviews(tab: &Arc<Tab>) -> AnyResult<bool> {
    let js = r#"
    (() => {
        function visible(el) {
            if (!el) return false;
            const style = window.getComputedStyle(el);
            const rect = el.getBoundingClientRect();
            return style &&
                   style.display !== "none" &&
                   style.visibility !== "hidden" &&
                   rect.width > 0 &&
                   rect.height > 0;
        }

        function safeClick(el) {
            try {
                el.click();
                return true;
            } catch (_) {
                try {
                    el.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }));
                    return true;
                } catch (_) {
                    return false;
                }
            }
        }

        const candidates = [];

        const byTestId = document.querySelector("button:has([data-testid='loadMore'])");
        if (byTestId) candidates.push(byTestId);

        const buttons = Array.from(document.querySelectorAll("button"));
        for (const btn of buttons) {
            const text = ((btn.innerText || btn.textContent || "").replace(/\s+/g, " ").trim()).toLowerCase();
            if (text === "show more reviews") {
                candidates.push(btn);
            }
        }

        for (const el of candidates) {
            if (!visible(el)) continue;
            try {
                el.scrollIntoView({ block: "center", inline: "center" });
            } catch (_) {}
            if (safeClick(el)) return true;
        }

        return false;
    })()
    "#;

    let ok = eval_bool(tab, js)?;
    if ok {
        sleep_ms(2500);
    }
    Ok(ok)
}

fn extract_reviews(tab: &Arc<Tab>) -> AnyResult<Vec<ReviewRow>> {
    let js = r#"
    (() => {
        function findRoot(node) {
            let el = node;
            for (let i = 0; i < 12 && el; i++, el = el.parentElement) {
                const hasDate = !!el.querySelector('a[href*="/review/show/"]');
                const hasRating = !!el.querySelector('[aria-label*="Rating"]');
                if (hasDate || hasRating) return el;
            }
            return node.closest('article, section, div') || node;
        }

        function getReviewer(root) {
            const candidates = [
                'a[href*="/user/show/"]',
                'a[href*="/review/list/"]',
                'a[data-testid*="name"]'
            ];
            for (const sel of candidates) {
                const el = root.querySelector(sel);
                if (el && el.textContent && el.textContent.trim()) {
                    return el.textContent.trim();
                }
            }
            return "";
        }

        function getDateAndUrl(root) {
            const dateEl = root.querySelector('a[href*="/review/show/"]');
            if (!dateEl) return { date: "", review_url: "" };
            return {
                date: (dateEl.textContent || "").trim(),
                review_url: dateEl.href || ""
            };
        }

        function getRating(root) {
            const ratingEl = root.querySelector('[aria-label*="Rating"]');
            if (!ratingEl) return null;
            const aria = ratingEl.getAttribute('aria-label') || '';
            const m = aria.match(/Rating\s+(\d+)\s+out of\s+5/i);
            return m ? Number(m[1]) : null;
        }

        const containers = Array.from(document.querySelectorAll('[data-testid="contentContainer"]'));
        const out = [];

        for (const node of containers) {
            const text = (node.innerText || "").trim();
            if (!text) continue;

            const root = findRoot(node);
            const { date, review_url } = getDateAndUrl(root);
            const reviewer = getReviewer(root);
            const rating = getRating(root);

            out.push({
                reviewer,
                rating,
                date,
                review_url,
                review_text: text
            });
        }

        return out;
    })()
    "#;

    let value = eval_value(tab, js)?;
    let data: Vec<ReviewRow> = serde_json::from_value(value).unwrap_or_default();

    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();

    for row in data {
        let reviewer = row.reviewer.trim().to_string();
        let date = row.date.trim().to_string();
        let review_url = row.review_url.trim().to_string();
        let review_text = row.review_text.trim().to_string();
        let rating = row.rating;

        if review_text.is_empty() {
            continue;
        }

        let key = if !review_url.is_empty() {
            review_url.clone()
        } else {
            let prefix: String = review_text.chars().take(120).collect();
            format!("{}|{}|{}", reviewer, date, prefix)
        };

        if seen.contains(&key) {
            continue;
        }
        seen.insert(key);

        rows.push(ReviewRow {
            reviewer,
            rating,
            date,
            review_url,
            review_text,
        });
    }

    Ok(rows)
}

fn save_csv(rows: &[ReviewRow], path: &str) -> AnyResult<()> {
    let mut wtr = Writer::from_path(path)?;
    for row in rows {
        wtr.serialize(row)?;
    }
    wtr.flush()?;
    Ok(())
}

fn crawl_goodreads_reviews() -> AnyResult<()> {
    fs::create_dir_all(PROFILE_DIR)?;

    let launch_options = LaunchOptionsBuilder::default()
        .headless(HEADLESS)
        .window_size(Some((1440, 2200)))
        .user_data_dir(Some(PathBuf::from(PROFILE_DIR)))
        .args(vec!["--lang=en-US".to_string()])
        .build()
        .map_err(|e| format!("브라우저 옵션 생성 실패: {e}"))?;

    let browser = Browser::new(launch_options)?;
    let tab = browser.wait_for_initial_tab()?;
    tab.set_default_timeout(Duration::from_millis(12_000));

    let mut all_reviews: HashMap<String, ReviewRow> = HashMap::new();

    wait_for_manual_login(&tab)?;
    dismiss_overlays(&tab)?;

    safe_goto(&tab, TARGET_URL, 5, 1500)?;
    sleep_ms(3000);

    let mut idle_rounds = 0usize;
    let mut round_no = 0usize;

    loop {
        round_no += 1;
        println!("\n===== ROUND {} =====", round_no);

        dismiss_overlays(&tab)?;

        let n1 = click_all_review_show_more(&tab, 20)?;
        if n1 > 0 {
            println!("[확장] Show more 클릭 수: {}", n1);
            sleep_ms(1200);
        }

        smart_scroll(&tab, MAX_SCROLL_STEPS_PER_ROUND)?;

        let n2 = click_all_review_show_more(&tab, 20)?;
        if n2 > 0 {
            println!("[재확장] Show more 클릭 수: {}", n2);
            sleep_ms(1000);
        }

        let current = extract_reviews(&tab)?;
        let before = all_reviews.len();

        for row in current {
            let key = if !row.review_url.is_empty() {
                row.review_url.clone()
            } else {
                let prefix: String = row.review_text.chars().take(120).collect();
                format!("{}|{}|{}", row.reviewer, row.date, prefix)
            };
            all_reviews.insert(key, row);
        }

        let after = all_reviews.len();
        let gained = after.saturating_sub(before);

        println!("[수집] 현재 누적 리뷰 수: {} (이번 라운드 +{})", after, gained);

        if click_show_more_reviews(&tab)? {
            println!("[로드] Show more reviews 클릭 성공");
            idle_rounds = 0;
            sleep_ms(2500);
            continue;
        }

        smart_scroll(&tab, 2)?;

        if click_show_more_reviews(&tab)? {
            println!("[로드] 스크롤 후 Show more reviews 클릭 성공");
            idle_rounds = 0;
            sleep_ms(2500);
            continue;
        }

        if gained == 0 {
            idle_rounds += 1;
        } else {
            idle_rounds = 0;
        }

        println!("[상태] idle_rounds={}", idle_rounds);

        if idle_rounds >= MAX_IDLE_ROUNDS {
            println!("[종료] 더 이상 새 리뷰가 로드되지 않음");
            break;
        }
    }

    let _ = click_all_review_show_more(&tab, 20)?;
    sleep_ms(1200);

    let final_rows = extract_reviews(&tab)?;
    for row in final_rows {
        let key = if !row.review_url.is_empty() {
            row.review_url.clone()
        } else {
            let prefix: String = row.review_text.chars().take(120).collect();
            format!("{}|{}|{}", row.reviewer, row.date, prefix)
        };
        all_reviews.insert(key, row);
    }

    let result: Vec<ReviewRow> = all_reviews.into_values().collect();
    save_csv(&result, OUTPUT_CSV)?;

    println!("\n[완료] 총 리뷰 수: {}", result.len());
    println!("[저장] CSV: {}", OUTPUT_CSV);

    Ok(())
}

fn main() {
    if let Err(e) = crawl_goodreads_reviews() {
        eprintln!("[오류] {}", e);
        std::process::exit(1);
    }
}
