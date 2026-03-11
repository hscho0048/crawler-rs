use std::{sync::Arc, time::Duration};

use reqwest::{Client, StatusCode};
use scraper::{Html, Selector};
use serde_json::Value;
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::{instrument, warn};
use url::Url;

use crate::{errors::CrawlError, models::{Comment, PostData, Source}};

#[derive(Clone)]
pub struct PlanAHttpCrawler {
    client: Client,
}

impl PlanAHttpCrawler {
    pub fn new() -> Result<Self, CrawlError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(10))
            .cookie_store(true)
            .redirect(reqwest::redirect::Policy::limited(10))
            .user_agent(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
                 AppleWebKit/537.36 (KHTML, like Gecko) \
                 Chrome/122.0.0.0 Safari/537.36",
            )
            .build()?;
        Ok(Self { client })
    }

    #[instrument(skip(self))]
    pub async fn crawl_one(&self, input_url: Url) -> Result<PostData, CrawlError> {
        let (source, url) = normalize_to_mobile(input_url)?;
        let html = self.fetch_html(&url).await?;

        let doc = Html::parse_document(&html);

        let title = parse_title(&doc).unwrap_or_default();
        let written_at = parse_written_at(&doc).unwrap_or_default();
        let body = parse_body(&doc).unwrap_or_default();
        let comments = parse_comments_from_html(&doc);

        if title.trim().is_empty() || written_at.trim().is_empty() || body.trim().is_empty() {
            warn!(%url, "missing required fields in Plan A; escalate to Plan B");
            return Err(CrawlError::RequiresJsOrBlocked);
        }

        Ok(PostData {
            source,
            url,
            title,
            author: String::new(),
            author_level: String::new(),
            written_at,
            views: String::new(),
            body,
            body_images: vec![],
            comments,
        })
    }

    async fn fetch_html(&self, url: &Url) -> Result<String, CrawlError> {
        for attempt in 0..3u32 {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(300 * 2u64.pow(attempt - 1))).await;
            }

            let resp = match self.client.get(url.clone()).send().await {
                Ok(r) => r,
                Err(e) => {
                    if attempt == 2 { return Err(CrawlError::from(e)); }
                    continue;
                }
            };

            let status = resp.status();
            if status == StatusCode::FORBIDDEN || status == StatusCode::TOO_MANY_REQUESTS {
                return Err(CrawlError::Blocked(status));
            }

            let text = resp.text().await?;
            if text.to_lowercase().contains("captcha") || text.contains("로봇") {
                return Err(CrawlError::RequiresJsOrBlocked);
            }

            return Ok(text);
        }

        unreachable!()
    }
}

/// Crawl many URLs with bounded concurrency.
#[allow(dead_code)]
pub async fn crawl_many_plan_a(
    crawler: Arc<PlanAHttpCrawler>,
    urls: Vec<Url>,
    max_in_flight: usize,
) -> Vec<Result<PostData, CrawlError>> {
    let sem = Arc::new(Semaphore::new(max_in_flight.max(1)));
    let mut joinset = JoinSet::new();

    for url in urls {
        let permit = sem.clone().acquire_owned().await.expect("semaphore closed");
        let crawler = crawler.clone();
        joinset.spawn(async move {
            let _permit = permit;
            crawler.crawl_one(url).await
        });
    }

    let mut out = Vec::new();
    while let Some(res) = joinset.join_next().await {
        match res {
            Ok(r) => out.push(r),
            Err(e) => out.push(Err(CrawlError::Parse(format!("join error: {e}")))),
        }
    }
    out
}

pub fn normalize_to_mobile(input_url: Url) -> Result<(Source, Url), CrawlError> {
    let host = input_url.host_str().unwrap_or_default();

    if host.contains("blog.naver.com") && !host.contains("m.blog.naver.com") {
        let mut url = input_url.clone();
        url.set_host(Some("m.blog.naver.com")).ok();
        return Ok((Source::NaverBlog, url));
    }

    if host.contains("cafe.naver.com") && !host.contains("m.cafe.naver.com") {
        let mut url = input_url.clone();
        url.set_host(Some("m.cafe.naver.com")).ok();
        return Ok((Source::NaverCafe, url));
    }

    let source = if host.contains("m.blog.naver.com") {
        Source::NaverBlog
    } else if host.contains("m.cafe.naver.com") {
        Source::NaverCafe
    } else {
        Source::Unknown
    };

    Ok((source, input_url))
}

fn parse_title(doc: &Html) -> Option<String> {
    let sel = Selector::parse(r#"meta[property=\"og:title\"]"#).ok()?;
    if let Some(meta) = doc.select(&sel).next() {
        if let Some(content) = meta.value().attr("content") {
            let s = content.trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        }
    }

    let sel = Selector::parse("title").ok()?;
    doc.select(&sel)
        .next()
        .map(|e| e.text().collect::<String>().trim().to_string())
        .filter(|s| !s.is_empty())
}

fn parse_written_at(doc: &Html) -> Option<String> {
    let sel = Selector::parse(r#"meta[property=\"article:published_time\"]"#).ok()?;
    if let Some(meta) = doc.select(&sel).next() {
        if let Some(content) = meta.value().attr("content") {
            let s = content.trim().to_string();
            if !s.is_empty() {
                return Some(s);
            }
        }
    }

    parse_json_ld(doc).and_then(|v| {
        v.get("datePublished")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string())
    })
}

fn parse_body(doc: &Html) -> Option<String> {
    if let Some(v) = parse_json_ld(doc) {
        if let Some(body) = v.get("articleBody").and_then(|x| x.as_str()) {
            let s = normalize_ws(body);
            if !s.is_empty() {
                return Some(s);
            }
        }
    }

    let sel = Selector::parse("article").ok()?;
    if let Some(article) = doc.select(&sel).next() {
        let s = normalize_ws(&article.text().collect::<Vec<_>>().join(" "));
        if !s.is_empty() {
            return Some(s);
        }
    }

    None
}

fn parse_comments_from_html(doc: &Html) -> Vec<Comment> {
    let item_sel_candidates = [".comment_item", ".u_cbox_comment", "[data-comment]"];
    let author_sel_candidates = [".author", ".u_cbox_name", "[data-author]"];
    let content_sel_candidates = [".content", ".u_cbox_contents", "[data-content]"];

    for item_sel in item_sel_candidates {
        let Ok(item_sel) = Selector::parse(item_sel) else { continue };
        let items: Vec<_> = doc.select(&item_sel).collect();
        if items.is_empty() {
            continue;
        }

        let author_sels: Vec<_> = author_sel_candidates
            .iter()
            .filter_map(|s| Selector::parse(s).ok())
            .collect();

        let content_sels: Vec<_> = content_sel_candidates
            .iter()
            .filter_map(|s| Selector::parse(s).ok())
            .collect();

        let mut out = Vec::new();
        for item in items {
            let author = author_sels
                .iter()
                .find_map(|sel| item.select(sel).next())
                .map(|e| normalize_ws(&e.text().collect::<String>()))
                .filter(|s| !s.is_empty());

            let content = content_sels
                .iter()
                .find_map(|sel| item.select(sel).next())
                .map(|e| normalize_ws(&e.text().collect::<String>()))
                .unwrap_or_default();

            if !content.is_empty() {
                out.push(Comment {
                    comment_id: String::new(),
                    is_reply: false,
                    author,
                    author_level_icon: None,
                    author_avatar: None,
                    date: String::new(),
                    content,
                });
            }
        }
        return out;
    }

    vec![]
}

fn parse_json_ld(doc: &Html) -> Option<Value> {
    let sel = Selector::parse(r#"script[type=\"application/ld+json\"]"#).ok()?;
    for script in doc.select(&sel) {
        let raw = script.text().collect::<String>();
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }

        if let Ok(v) = serde_json::from_str::<Value>(raw) {
            if v.is_object() {
                return Some(v);
            }
            if let Some(arr) = v.as_array() {
                if let Some(first_obj) = arr.iter().find(|x| x.is_object()) {
                    return Some(first_obj.clone());
                }
            }
        }
    }
    None
}

fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
