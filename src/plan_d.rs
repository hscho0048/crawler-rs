use std::sync::Arc;
use std::time::Duration;
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::cdp::browser_protocol::page::AddScriptToEvaluateOnNewDocumentParams;
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use url::Url;

use crate::{
    csv_out::{write_comments_csv, write_posts_csv},
    models::{Comment, PostData, Source},
};

// ─────────────────────────────────────────────────────────────────
// 내부 역직렬화 전용 구조체
// ─────────────────────────────────────────────────────────────────
#[derive(Debug, Deserialize)]
struct ScrapedComment {
    pub id: String,
    pub is_reply: bool,
    pub author: String,
    pub date: String,
    pub content: String,
}

#[derive(Debug, Deserialize)]
struct ScrapedDetail {
    pub body: String,
    pub comments: Vec<ScrapedComment>,
}

// ─────────────────────────────────────────────────────────────────
// 퍼블릭 진입점
// ─────────────────────────────────────────────────────────────────
pub async fn run(
    url: &str,
    max_posts: usize,
    workers: usize,
    out_dir: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("🚀 크롤링 시작!");
    println!(" - 타겟 URL: {}", url);
    println!(" - 목표 게시글 수: {}", max_posts);
    println!(" - 워커 수: {}", workers);
    println!(" - 저장 위치: {}", out_dir);

    tokio::fs::create_dir_all(out_dir).await?;

    let browser = launch_browser().await?;
    let browser = Arc::new(browser);

    let mut post_refs = Vec::new();
    let mut current_page = 1;

    // 기존 url에서 page 파라미터가 있다면 무시하고 base url 추출
    let base_url = url.split("&page=").next().unwrap_or(url);

    println!("\n🔍 [1단계] 게시글 링크 수집 중...");
    while post_refs.len() < max_posts {
        let page_url = format!("{}&page={}", base_url, current_page);

        let page = match open_stealth_page(&browser, &page_url).await {
            Ok(p) => p,
            Err(e) => {
                println!("❌ 페이지 열기 실패: {}", e);
                break;
            }
        };
        // 게시글 행이 나타날 때까지 polling (최대 6초)
        {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
            loop {
                let ready = page.evaluate("!!document.querySelector('tr.ub-content.us-post')")
                    .await.ok().and_then(|v| v.into_value::<bool>().ok()).unwrap_or(false);
                if ready || tokio::time::Instant::now() >= deadline { break; }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        let list_js = r#"
            Array.from(document.querySelectorAll('tr.ub-content.us-post')).map(tr => {
                const a = tr.querySelector('a');
                if (!a) return null;
                const em = a.querySelector('em');
                if (em) em.remove();

                const title = a.innerText.trim();
                const url = a.href;
                const dateElem = tr.querySelector('td.gall_date');
                const date = dateElem ? (dateElem.getAttribute('title') || dateElem.innerText.trim()) : '';
                return { title, url, date };
            }).filter(Boolean)
        "#;

        if let Ok(val) = page.evaluate(list_js).await {
            if let Ok(refs) = val.into_value::<Vec<serde_json::Value>>() {
                if refs.is_empty() {
                    println!("더 이상 수집할 게시글이 없습니다.");
                    page.close().await?;
                    break;
                }
                for r in refs {
                    if post_refs.len() < max_posts {
                        post_refs.push(r);
                    }
                }
            }
        }
        println!("  ↳ {}페이지 완료 (누적: {}/{})", current_page, post_refs.len(), max_posts);
        page.close().await?;
        current_page += 1;
    }

    println!("\n⚡ [2단계] {}개의 게시글 병렬 스크래핑 시작 (워커: {})", post_refs.len(), workers);

    let sem = Arc::new(Semaphore::new(workers));
    let mut joinset = JoinSet::new();

    for (idx, p_ref) in post_refs.into_iter().enumerate() {
        let browser = browser.clone();
        let sem = sem.clone();

        let title = p_ref["title"].as_str().unwrap_or("").to_string();
        let post_url = p_ref["url"].as_str().unwrap_or("").to_string();
        let date = p_ref["date"].as_str().unwrap_or("").to_string();

        joinset.spawn(async move {
            let _permit = sem.acquire_owned().await;

            let detail_page = match open_stealth_page(&browser, &post_url).await {
                Ok(p) => p,
                Err(_) => return None,
            };

            // 본문 영역이 나타날 때까지 polling (최대 6초)
            {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
                loop {
                    let ready = detail_page.evaluate("!!document.querySelector('.write_div')")
                        .await.ok().and_then(|v| v.into_value::<bool>().ok()).unwrap_or(false);
                    if ready || tokio::time::Instant::now() >= deadline { break; }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }

            let mut is_first_pass = true;
            let mut post_body = String::new();
            let mut result_comments: Vec<Comment> = Vec::new();

            // 댓글 페이지네이션 루프
            loop {
                let detail_js = r#"
                    (() => {
                        const bodyDiv = document.querySelector('.write_div');
                        const body = bodyDiv ? bodyDiv.innerText.trim() : '';

                        const comments = Array.from(document.querySelectorAll('ul.cmt_list > li.ub-content')).map(li => {
                            const is_reply = !!li.querySelector('div.reply_info');
                            const info = li.querySelector('div.cmt_info') || li.querySelector('div.reply_info');
                            if (!info) return null;

                            const id = li.getAttribute('id') || '';
                            const author = info.querySelector('.nickname em')?.innerText?.trim() || 'ㅇㅇ';
                            const date = info.querySelector('.date_time')?.innerText?.trim() || '';
                            const content = info.querySelector('p.usertxt')?.innerText?.trim() || '';

                            return { id, is_reply, author, date, content };
                        }).filter(Boolean);

                        return { body, comments };
                    })()
                "#;

                if let Ok(val) = detail_page.evaluate(detail_js).await {
                    if let Ok(detail) = val.into_value::<ScrapedDetail>() {
                        if is_first_pass {
                            post_body = detail.body;
                            is_first_pass = false;
                        }

                        for c in detail.comments {
                            result_comments.push(Comment {
                                comment_id: c.id,
                                is_reply: c.is_reply,
                                author: Some(c.author),
                                author_level_icon: None,
                                author_avatar: None,
                                date: c.date,
                                content: c.content,
                            });
                        }
                    }
                }

                let next_page_js = r#"
                    (() => {
                        const currentEm = document.querySelector('.cmt_paging em');
                        if (!currentEm) return false;

                        const currentPageNum = parseInt(currentEm.innerText.trim());
                        const nextTarget = currentPageNum + 1;

                        const links = Array.from(document.querySelectorAll('.cmt_paging a'));
                        const nextLink = links.find(a => parseInt(a.innerText.trim()) === nextTarget);

                        if (nextLink) {
                            nextLink.click();
                            return true;
                        }
                        return false;
                    })()
                "#;

                // 클릭 전 DOM 댓글 수 저장
                let before_dom_count = detail_page
                    .evaluate("document.querySelectorAll('ul.cmt_list > li.ub-content').length")
                    .await.ok().and_then(|v| v.into_value::<usize>().ok()).unwrap_or(0);

                if let Ok(val) = detail_page.evaluate(next_page_js).await {
                    if let Ok(has_next) = val.into_value::<bool>() {
                        if has_next {
                            // 클릭 전 DOM count 기준으로 변화 감지
                            let dl = tokio::time::Instant::now() + Duration::from_millis(3000);
                            loop {
                                tokio::time::sleep(Duration::from_millis(200)).await;
                                let count = detail_page
                                    .evaluate("document.querySelectorAll('ul.cmt_list > li.ub-content').length")
                                    .await.ok().and_then(|v| v.into_value::<usize>().ok()).unwrap_or(0);
                                if count != before_dom_count || tokio::time::Instant::now() >= dl { break; }
                            }
                            continue;
                        }
                    }
                }
                break;
            }

            detail_page.close().await.ok();
            println!("  ✓ [{}] 완료: {} (댓글 {}개)", idx + 1, title, result_comments.len());

            let parsed_url = Url::parse(&post_url).ok()?;
            let result_post = PostData {
                source: Source::Unknown,
                url: parsed_url,
                title: title.clone(),
                author: String::new(),
                author_level: String::new(),
                written_at: date,
                views: String::new(),
                body: post_body,
                body_images: vec![],
                comments: result_comments,
            };

            Some(result_post)
        });
    }

    let mut all_posts: Vec<PostData> = Vec::new();

    while let Some(res) = joinset.join_next().await {
        if let Ok(Some(post)) = res {
            all_posts.push(post);
        }
    }

    let out_path = std::path::Path::new(out_dir);
    write_posts_csv(out_path, &all_posts)
        .map_err(|e| format!("posts.csv 저장 실패: {e}"))?;
    write_comments_csv(out_path, &all_posts)
        .map_err(|e| format!("comments.csv 저장 실패: {e}"))?;

    println!("\n🎉 크롤링 및 저장 완료!");
    println!(" - 저장 위치: {}/results.csv, {}/comments.csv", out_dir, out_dir);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────
// 유틸리티 함수
// ─────────────────────────────────────────────────────────────────
async fn launch_browser() -> Result<Browser, Box<dyn std::error::Error + Send + Sync>> {
    // 실행마다 고유한 디렉토리를 사용해 잠금 충돌 방지
    let unique_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    let user_data = std::env::temp_dir().join(format!("chromiumoxide-plan-d-{}", unique_id));
    std::fs::create_dir_all(&user_data)?;

    let config = BrowserConfig::builder()
        .arg("--headless=new")
        .arg("--no-sandbox")
        .arg("--disable-gpu")
        .arg("--disable-dev-shm-usage")
        .arg("--no-first-run")
        .arg("--no-default-browser-check")
        .arg("--window-size=1920,1080")
        .arg("--disable-blink-features=AutomationControlled")
        .arg(format!("--user-data-dir={}", user_data.display()))
        .arg("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        .build()?;

    let (browser, mut handler) = Browser::launch(config).await?;
    let cleanup_dir = user_data.clone();
    tokio::spawn(async move {
        while let Some(_) = handler.next().await {}
        let _ = std::fs::remove_dir_all(&cleanup_dir);
    });
    Ok(browser)
}

async fn open_stealth_page(browser: &Browser, url: &str) -> Result<chromiumoxide::Page, Box<dyn std::error::Error + Send + Sync>> {
    let page = browser.new_page("about:blank").await?;
    page.execute(AddScriptToEvaluateOnNewDocumentParams::new(
        "Object.defineProperty(navigator, 'webdriver', { get: () => undefined })",
    )).await?;
    page.goto(url).await?;
    page.wait_for_navigation().await?;
    Ok(page)
}

