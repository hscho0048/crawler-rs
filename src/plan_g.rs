use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use csv::Writer;
use urlencoding;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use serde_json::Value;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{info, warn};

// ─────────────────────────────────────────────────────────────────
// 설정
// ─────────────────────────────────────────────────────────────────

pub struct RedditConfig {
    pub subreddit:    Option<String>,
    pub sort:         String,   // "new" | "hot" | "top" | "rising" | "relevance"
    pub limit:        usize,    // 페이지당 최대 게시글 수 (Reddit 최대 100)
    pub max_pages:    usize,
    pub max_comments: usize,    // 게시글당 최대 댓글 수
    pub keywords:     Vec<String>, // 빈 벡터 = 전체 수집 (제목+본문 필터)
    pub search_query: Option<String>, // 서브레딧 내 검색어 (Reddit 검색 API 사용)
    pub workers:      usize,    // 댓글 병렬 수집 동시성
    pub user_agent:   String,   // Reddit 요구 형식: "platform:appid:v1.0 (by /u/username)"
    pub page_delay_ms: u64,     // 페이지 사이 딜레이 (ms)
    pub out_dir:      String,
}

// ─────────────────────────────────────────────────────────────────
// 데이터 모델
// ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct PostRow {
    subreddit:                String,
    post_id:                  String,
    post_title:               String,
    post_text:                String,
    post_author:              String,
    post_score:               String,
    post_upvote_ratio:        String,
    post_num_comments:        String,
    post_created_utc:         String,
    post_created_datetime_utc: String,
    post_url:                 String,
    post_permalink:           String,
    post_link_url:            String,
    post_flair:               String,
    post_is_self:             String,
    post_domain:              String,
}

#[derive(Debug, Clone)]
struct CommentRow {
    subreddit:                 String,
    post_id:                   String,
    post_title:                String,
    post_created_datetime_utc: String,
    post_url:                  String,
    comment_id:                String,
    comment_parent_id:         String,
    comment_author:            String,
    comment_body:              String,
    comment_score:             String,
    comment_created_utc:       String,
    comment_created_datetime_utc: String,
    comment_depth:             String,
    comment_permalink:         String,
}

// ─────────────────────────────────────────────────────────────────
// 진입점
// ─────────────────────────────────────────────────────────────────

pub async fn run(cfg: RedditConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Arc::new(build_client(&cfg.user_agent)?);
    let sem    = Arc::new(Semaphore::new(cfg.workers));

    let mut all_posts:    Vec<PostRow>    = Vec::new();
    let mut all_comments: Vec<CommentRow> = Vec::new();

    let mut after: Option<String> = None;
    let mut seen_count: usize = 0;

    let scope_label = cfg.subreddit.as_deref().unwrap_or("all");

    for page_num in 0..cfg.max_pages {
        info!("[r/{}] 페이지 {} 요청 중...", scope_label, page_num + 1);

        let listing = match fetch_listing(&client, cfg.subreddit.as_deref(), &cfg.sort, cfg.limit, after.as_deref(), seen_count, cfg.search_query.as_deref()).await {
            Some(v) => v,
            None    => { warn!("[중단] 응답 없음"); break; }
        };

        let data = listing.get("data").unwrap_or(&Value::Null);
        after = data.get("after").and_then(|v| v.as_str()).map(String::from);

        let children: Vec<Value> = data
            .get("children")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        if children.is_empty() {
            info!("[종료] r/{} 게시글 없음", scope_label);
            break;
        }

        // 게시글 메타 파싱 + 키워드 필터
        let page_posts: Vec<PostRow> = children
            .iter()
            .filter(|item| item.get("kind").and_then(|v| v.as_str()).unwrap_or("") == "t3")
            .filter_map(|item| {
                let subreddit_name = item.get("data")
                    .and_then(|d| d.get("subreddit"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(scope_label)
                    .to_string();
                parse_post(item.get("data")?, &subreddit_name, &cfg.keywords)
            })
            .collect();

        seen_count += children.len();
        info!("  필터 통과: {}개", page_posts.len());

        // 댓글 병렬 수집
        let mut joinset: JoinSet<(PostRow, Vec<CommentRow>)> = JoinSet::new();

        for post_row in page_posts {
            let client      = client.clone();
            let sem         = sem.clone();
            let max_comments = cfg.max_comments;
            let permalink   = post_row.post_permalink.clone();

            joinset.spawn(async move {
                let _permit  = sem.acquire_owned().await.unwrap();
                let comments = fetch_post_comments(&client, &permalink, &post_row, max_comments).await;
                info!("  [{}] 댓글 {}개", post_row.post_id, comments.len());
                (post_row, comments)
            });
        }

        while let Some(res) = joinset.join_next().await {
            if let Ok((post, comments)) = res {
                all_posts.push(post);
                all_comments.extend(comments);
            }
        }

        if after.is_none() {
            info!("[종료] r/{} 마지막 페이지", scope_label);
            break;
        }

        tokio::time::sleep(Duration::from_millis(cfg.page_delay_ms)).await;
    }

    // CSV 저장
    let out = Path::new(&cfg.out_dir);
    tokio::fs::create_dir_all(out).await?;
    write_posts_csv(&all_posts, &out.join("reddit_posts.csv"))?;
    write_comments_csv(&all_comments, &out.join("reddit_comments.csv"))?;

    info!(
        "완료 | 게시글 {}개, 댓글 {}개 → {}",
        all_posts.len(), all_comments.len(), cfg.out_dir
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────
// HTTP
// ─────────────────────────────────────────────────────────────────

fn build_client(user_agent: &str) -> Result<reqwest::Client, reqwest::Error> {
    let mut headers = HeaderMap::new();
    headers.insert(
        USER_AGENT,
        HeaderValue::from_str(user_agent).unwrap_or_else(|_| HeaderValue::from_static("RedditCrawler/1.0")),
    );
    reqwest::Client::builder()
        .default_headers(headers)
        .build()
}

async fn fetch_json(client: &reqwest::Client, url: &str) -> Option<Value> {
    for attempt in 1u64..=5 {
        let resp = match client.get(url).send().await {
            Ok(r)  => r,
            Err(e) => {
                warn!("네트워크 오류 | {url} | {e} | 시도 {attempt}/5");
                tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
                continue;
            }
        };

        match resp.status().as_u16() {
            200..=299 => {
                return match resp.json::<Value>().await {
                    Ok(json) => Some(json),
                    Err(e)   => { warn!("JSON 파싱 실패: {e} | {url}"); None }
                };
            }
            429 => {
                // Retry-After 헤더 준수 (없으면 60초)
                let wait = resp.headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(120);
                warn!("429 Too Many Requests | {wait}초 대기 후 재시도 ({attempt}/5) | {url}");
                tokio::time::sleep(Duration::from_secs(wait)).await;
            }
            status => {
                warn!("HTTP {status} | {url} | 시도 {attempt}/5");
                tokio::time::sleep(Duration::from_secs(2 * attempt)).await;
            }
        }
    }
    None
}

async fn fetch_listing(
    client:       &reqwest::Client,
    subreddit:    Option<&str>,
    sort:         &str,
    limit:        usize,
    after:        Option<&str>,
    count:        usize,
    search_query: Option<&str>,
) -> Option<Value> {
    let mut url = match (subreddit, search_query) {
        (Some(sr), Some(q)) => {
            // 서브레딧 내 검색 API
            format!(
                "https://www.reddit.com/r/{sr}/search.json?q={q}&restrict_sr=1&sort={sort}&limit={limit}&count={count}&raw_json=1",
                q = urlencoding::encode(q)
            )
        }
        (None, Some(q)) => {
            // 전체 Reddit 검색 API
            format!(
                "https://www.reddit.com/search.json?q={q}&sort={sort}&limit={limit}&count={count}&raw_json=1",
                q = urlencoding::encode(q)
            )
        }
        (Some(sr), None) => {
            format!(
                "https://www.reddit.com/r/{sr}/{sort}.json?limit={limit}&count={count}&raw_json=1"
            )
        }
        (None, None) => {
            // 서브레딧 없이 검색어도 없으면 전체 new/hot 피드
            format!(
                "https://www.reddit.com/{sort}.json?limit={limit}&count={count}&raw_json=1"
            )
        }
    };
    if let Some(a) = after {
        url.push_str("&after=");
        url.push_str(a);
    }
    fetch_json(client, &url).await
}

async fn fetch_post_comments(
    client:       &reqwest::Client,
    permalink:    &str,
    post_meta:    &PostRow,
    max_comments: usize,
) -> Vec<CommentRow> {
    let url = format!("https://www.reddit.com{permalink}.json?raw_json=1");
    let data = match fetch_json(client, &url).await {
        Some(v) => v,
        None    => return vec![],
    };

    let arr = match data.as_array() {
        Some(a) if a.len() >= 2 => a,
        _ => return vec![],
    };

    let children: Vec<Value> = arr[1]
        .get("data")
        .and_then(|v| v.get("children"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let mut rows = Vec::new();
    flatten_comments(&children, post_meta, 0, max_comments, &mut rows);
    rows
}

// ─────────────────────────────────────────────────────────────────
// 파싱 유틸
// ─────────────────────────────────────────────────────────────────

fn parse_post(post: &Value, subreddit: &str, keywords: &[String]) -> Option<PostRow> {
    let title    = safe_str(post.get("title"));
    let selftext = safe_str(post.get("selftext"));

    if !keywords.is_empty() {
        let merged = format!("{title}\n{selftext}").to_lowercase();
        if !keywords.iter().any(|kw| merged.contains(&kw.to_lowercase())) {
            return None;
        }
    }

    let permalink       = safe_str(post.get("permalink"));
    let created_utc_f   = post.get("created_utc").and_then(|v| v.as_f64());

    Some(PostRow {
        subreddit:                subreddit.to_string(),
        post_id:                  safe_str(post.get("id")),
        post_title:               title,
        post_text:                selftext,
        post_author:              safe_str(post.get("author")),
        post_score:               safe_str(post.get("score")),
        post_upvote_ratio:        safe_str(post.get("upvote_ratio")),
        post_num_comments:        safe_str(post.get("num_comments")),
        post_created_utc:         safe_str(post.get("created_utc")),
        post_created_datetime_utc: utc_to_str(created_utc_f),
        post_url:                 format!("https://www.reddit.com{permalink}"),
        post_permalink:           permalink,
        post_link_url:            safe_str(post.get("url")),
        post_flair:               safe_str(post.get("link_flair_text")),
        post_is_self:             safe_str(post.get("is_self")),
        post_domain:              safe_str(post.get("domain")),
    })
}

fn flatten_comments(
    nodes:        &[Value],
    post_meta:    &PostRow,
    depth:        usize,
    max_comments: usize,
    rows:         &mut Vec<CommentRow>,
) {
    for node in nodes {
        if rows.len() >= max_comments { return; }
        if node.get("kind").and_then(|v| v.as_str()).unwrap_or("") != "t1" { continue; }

        let data = node.get("data").unwrap_or(&Value::Null);

        rows.push(CommentRow {
            subreddit:                post_meta.subreddit.clone(),
            post_id:                  post_meta.post_id.clone(),
            post_title:               post_meta.post_title.clone(),
            post_created_datetime_utc: post_meta.post_created_datetime_utc.clone(),
            post_url:                 post_meta.post_url.clone(),
            comment_id:               safe_str(data.get("id")),
            comment_parent_id:        safe_str(data.get("parent_id")),
            comment_author:           safe_str(data.get("author")),
            comment_body:             safe_str(data.get("body")),
            comment_score:            safe_str(data.get("score")),
            comment_created_utc:      safe_str(data.get("created_utc")),
            comment_created_datetime_utc: utc_to_str(data.get("created_utc").and_then(|v| v.as_f64())),
            comment_depth:            depth.to_string(),
            comment_permalink:        format!(
                "https://www.reddit.com{}",
                safe_str(data.get("permalink"))
            ),
        });

        // 대댓글 재귀
        if let Some(Value::Object(_)) = data.get("replies") {
            let children: Vec<Value> = data
                .get("replies")
                .and_then(|v| v.get("data"))
                .and_then(|v| v.get("children"))
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default();
            flatten_comments(&children, post_meta, depth + 1, max_comments, rows);
        }
    }
}

fn safe_str(v: Option<&Value>) -> String {
    match v {
        Some(Value::Null) | None => String::new(),
        Some(Value::String(s))   => s.clone(),
        Some(other)              => other.to_string(),
    }
}

fn utc_to_str(ts: Option<f64>) -> String {
    let Some(secs) = ts.map(|f| f as i64) else { return String::new() };
    DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

// ─────────────────────────────────────────────────────────────────
// CSV 출력
// ─────────────────────────────────────────────────────────────────

fn write_posts_csv(rows: &[PostRow], path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = File::create(path)?;
    let mut buf = BufWriter::new(file);
    use std::io::Write as _;
    buf.write_all(b"\xef\xbb\xbf")?;
    let mut wtr = Writer::from_writer(buf);
    wtr.write_record([
        "subreddit","post_id","post_title","post_text","post_author",
        "post_score","post_upvote_ratio","post_num_comments",
        "post_created_utc","post_created_datetime_utc",
        "post_url","post_permalink","post_link_url",
        "post_flair","post_is_self","post_domain",
    ])?;
    for r in rows {
        wtr.write_record([
            &r.subreddit, &r.post_id, &r.post_title, &r.post_text, &r.post_author,
            &r.post_score, &r.post_upvote_ratio, &r.post_num_comments,
            &r.post_created_utc, &r.post_created_datetime_utc,
            &r.post_url, &r.post_permalink, &r.post_link_url,
            &r.post_flair, &r.post_is_self, &r.post_domain,
        ])?;
    }
    wtr.flush()?;
    info!("reddit_posts.csv 저장 rows={}", rows.len());
    Ok(())
}

fn write_comments_csv(rows: &[CommentRow], path: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let file = File::create(path)?;
    let mut buf = BufWriter::new(file);
    use std::io::Write as _;
    buf.write_all(b"\xef\xbb\xbf")?;
    let mut wtr = Writer::from_writer(buf);
    wtr.write_record([
        "subreddit","post_id","post_title","post_created_datetime_utc","post_url",
        "comment_id","comment_parent_id","comment_author","comment_body",
        "comment_score","comment_created_utc","comment_created_datetime_utc",
        "comment_depth","comment_permalink",
    ])?;
    for r in rows {
        wtr.write_record([
            &r.subreddit, &r.post_id, &r.post_title, &r.post_created_datetime_utc, &r.post_url,
            &r.comment_id, &r.comment_parent_id, &r.comment_author, &r.comment_body,
            &r.comment_score, &r.comment_created_utc, &r.comment_created_datetime_utc,
            &r.comment_depth, &r.comment_permalink,
        ])?;
    }
    wtr.flush()?;
    info!("reddit_comments.csv 저장 rows={}", rows.len());
    Ok(())
}
