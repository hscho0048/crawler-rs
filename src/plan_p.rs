use std::collections::{HashSet, VecDeque};
use std::io::Write as _;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use regex::Regex;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE, REFERER, USER_AGENT};
use scraper::{ElementRef, Html, Selector};
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{info, warn};
use url::Url;

#[derive(Debug, Clone)]
pub struct PlanPConfig {
    pub search_url: String,
    pub start_page: usize,
    pub max_pages: usize,
    pub max_posts: usize,
    pub workers: usize,
    pub out_dir: String,
    pub page_delay_ms: u64,
    pub detail_delay_ms: u64,
}

impl Default for PlanPConfig {
    fn default() -> Self {
        Self {
            search_url: String::new(),
            start_page: 1,
            max_pages: 10,
            max_posts: 0,
            workers: 3,
            out_dir: "out".to_string(),
            page_delay_ms: 500,
            detail_delay_ms: 300,
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct KinSearchRow {
    pub search_page: usize,
    pub rank_in_page: usize,
    pub url: String,
    pub doc_id: String,
    pub dir_id: String,
    pub title: String,
    pub date: String,
    pub snippet: String,
    pub category: String,
    pub answer_count: String,
    pub up_count: String,
    pub thumbnail_url: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct KinQuestionRow {
    pub url: String,
    pub doc_id: String,
    pub dir_id: String,
    pub title: String,
    pub author: String,
    pub written_at: String,
    pub views: String,
    pub category: String,
    pub question_body: String,
    pub question_images_json: String,
    pub search_page: usize,
    pub rank_in_page: usize,
    pub search_title: String,
    pub search_date: String,
    pub search_snippet: String,
    pub search_category: String,
    pub status: String,
    pub error: String,
}

pub async fn run(cfg: PlanPConfig) -> Result<()> {
    if cfg.search_url.trim().is_empty() {
        return Err(anyhow!("--url is required"));
    }
    if cfg.max_pages == 0 {
        return Err(anyhow!("--max-pages must be greater than 0"));
    }

    tokio::fs::create_dir_all(&cfg.out_dir)
        .await
        .with_context(|| format!("output directory create failed: {}", cfg.out_dir))?;

    let client = Arc::new(build_client()?);
    let search_rows = collect_search_rows(&client, &cfg).await?;
    let out_dir = Path::new(&cfg.out_dir);
    write_csv(&out_dir.join("kin_search_results.csv"), &search_rows)?;

    if search_rows.is_empty() {
        warn!("no Kin search results collected");
        write_csv::<KinQuestionRow>(&out_dir.join("kin_questions.csv"), &[])?;
        return Ok(());
    }

    let question_rows = scrape_questions_parallel(client, &cfg, search_rows).await?;
    write_csv(&out_dir.join("kin_questions.csv"), &question_rows)?;

    info!(
        questions = question_rows.len(),
        out_dir = %cfg.out_dir,
        "kin question csv saved"
    );
    Ok(())
}

async fn collect_search_rows(
    client: &reqwest::Client,
    cfg: &PlanPConfig,
) -> Result<Vec<KinSearchRow>> {
    let base_url = Url::parse(&cfg.search_url).context("invalid --url")?;
    let start_page = cfg.start_page.max(1);
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for page in start_page..start_page + cfg.max_pages {
        let page_url = search_page_url(&base_url, page);
        let html = fetch_text(client, page_url.as_str(), Some(base_url.as_str())).await?;
        let rows = parse_search_rows(&html, page, page_url.as_str());

        if rows.is_empty() {
            info!(page, "kin search page had no results");
            break;
        }

        let before = out.len();
        for row in rows {
            if seen.insert(row.url.clone()) {
                out.push(row);
                if cfg.max_posts > 0 && out.len() >= cfg.max_posts {
                    return Ok(out);
                }
            }
        }

        info!(
            page,
            new_rows = out.len().saturating_sub(before),
            total = out.len(),
            "kin search page parsed"
        );

        if cfg.page_delay_ms > 0 {
            sleep(Duration::from_millis(cfg.page_delay_ms)).await;
        }
    }

    Ok(out)
}

async fn scrape_questions_parallel(
    client: Arc<reqwest::Client>,
    cfg: &PlanPConfig,
    search_rows: Vec<KinSearchRow>,
) -> Result<Vec<KinQuestionRow>> {
    let total = search_rows.len();
    let worker_count = cfg.workers.max(1).min(total.max(1));
    let queue = Arc::new(Mutex::new(VecDeque::from(search_rows)));
    let mut joinset = JoinSet::new();

    for worker_id in 0..worker_count {
        let client = client.clone();
        let queue = queue.clone();
        let detail_delay_ms = cfg.detail_delay_ms;

        joinset.spawn(async move {
            let mut rows = Vec::new();
            loop {
                let search_row = queue.lock().await.pop_front();
                let Some(search_row) = search_row else {
                    break;
                };

                match fetch_text(&client, &search_row.url, Some(&search_row.url)).await {
                    Ok(html) => {
                        let row = parse_question_page(&html, &search_row);
                        info!(
                            worker = worker_id,
                            doc_id = %row.doc_id,
                            "kin question parsed"
                        );
                        rows.push(row);
                    }
                    Err(err) => {
                        warn!(
                            worker = worker_id,
                            url = %search_row.url,
                            "kin question fetch failed: {err:#}"
                        );
                        rows.push(KinQuestionRow {
                            url: search_row.url.clone(),
                            doc_id: search_row.doc_id.clone(),
                            dir_id: search_row.dir_id.clone(),
                            search_page: search_row.search_page,
                            rank_in_page: search_row.rank_in_page,
                            search_title: search_row.title.clone(),
                            search_date: search_row.date.clone(),
                            search_snippet: search_row.snippet.clone(),
                            search_category: search_row.category.clone(),
                            status: "error".to_string(),
                            error: err.to_string(),
                            ..Default::default()
                        });
                    }
                }

                if detail_delay_ms > 0 {
                    sleep(Duration::from_millis(detail_delay_ms)).await;
                }
            }
            rows
        });
    }

    let mut out = Vec::new();
    while let Some(result) = joinset.join_next().await {
        match result {
            Ok(mut rows) => out.append(&mut rows),
            Err(err) => warn!("kin worker join failed: {err}"),
        }
    }
    Ok(out)
}

fn parse_search_rows(html: &str, page: usize, base_url: &str) -> Vec<KinSearchRow> {
    let document = Html::parse_document(html);
    let item_sel = sel("ul.basic1 > li");
    let title_sel = sel("a._searchListTitleAnchor[href]");
    let mut out = Vec::new();

    for (idx, item) in document.select(&item_sel).enumerate() {
        let Some(title_node) = item.select(&title_sel).next() else {
            continue;
        };
        let Some(raw_href) = title_node.value().attr("href") else {
            continue;
        };
        let Some(url) = absolutize(raw_href, base_url) else {
            continue;
        };

        let date = first_text(&item, &["dd.txt_inline"]);
        let snippet = search_snippet(&item);
        let footer = first_text(&item, &["dd.txt_block"]);
        let (doc_id, dir_id) = ids_from_url(&url);

        out.push(KinSearchRow {
            search_page: page,
            rank_in_page: idx + 1,
            url,
            doc_id,
            dir_id,
            title: text_of(Some(title_node)),
            date,
            snippet,
            category: search_category(&item),
            answer_count: capture_first_number(&footer, r"답변수\s*([0-9,]+)"),
            up_count: capture_first_number(&footer, r"UP\s*([0-9,]+)"),
            thumbnail_url: first_attr(
                &item,
                &["a._nclicks\\:qna\\.img img[src]", "img[src]"],
                "src",
            )
            .and_then(|src| absolutize(&src, base_url))
            .unwrap_or_default(),
        });
    }

    out
}

fn parse_question_page(html: &str, search_row: &KinSearchRow) -> KinQuestionRow {
    let document = Html::parse_document(html);
    let (mut doc_id, mut dir_id) = ids_from_url(&search_row.url);
    if doc_id.is_empty() || dir_id.is_empty() {
        let (html_doc_id, html_dir_id) = ids_from_detail_script(html);
        if doc_id.is_empty() {
            doc_id = html_doc_id;
        }
        if dir_id.is_empty() {
            dir_id = html_dir_id;
        }
    }

    let title = first_doc_text(
        &document,
        &["meta[property='og:title']", "div.endTitleSection", "title"],
    )
    .unwrap_or_else(|| search_row.title.clone());

    let question_node = first_doc_node(&document, &["div.questionDetail"]);
    let question_body = question_node
        .map(|node| text_of(Some(node)))
        .unwrap_or_default();
    let question_images = question_node
        .map(|node| image_urls(node, &search_row.url))
        .unwrap_or_default();

    let user_info = first_doc_node(&document, &["div.userInfo"]);
    let (author, views, written_at) = user_info.map(parse_user_info).unwrap_or_default();

    let category = first_doc_text(&document, &["div.tagList a.tag"])
        .map(|value| clean_text(&value.replace("새 창", "")))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| search_row.category.clone());

    KinQuestionRow {
        url: search_row.url.clone(),
        doc_id,
        dir_id,
        title,
        author,
        written_at,
        views,
        category,
        question_body,
        question_images_json: serde_json::to_string(&question_images)
            .unwrap_or_else(|_| "[]".to_string()),
        search_page: search_row.search_page,
        rank_in_page: search_row.rank_in_page,
        search_title: search_row.title.clone(),
        search_date: search_row.date.clone(),
        search_snippet: search_row.snippet.clone(),
        search_category: search_row.category.clone(),
        status: "ok".to_string(),
        error: String::new(),
    }
}

async fn fetch_text(client: &reqwest::Client, url: &str, referer: Option<&str>) -> Result<String> {
    let mut req = client.get(url);
    if let Some(referer) = referer {
        req = req.header(REFERER, referer);
    }

    let response = req
        .send()
        .await
        .with_context(|| format!("request failed: {url}"))?
        .error_for_status()
        .with_context(|| format!("bad status: {url}"))?;

    response.text().await.context("response text read failed")
}

fn build_client() -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
             (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        ),
    );
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    );
    headers.insert(
        ACCEPT_LANGUAGE,
        HeaderValue::from_static("ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6"),
    );

    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .cookie_store(true)
        .gzip(true)
        .brotli(true)
        .zstd(true)
        .build()?)
}

fn search_page_url(base_url: &Url, page: usize) -> Url {
    let mut url = base_url.clone();
    set_query_param(&mut url, "page", &page.max(1).to_string());
    url
}

fn set_query_param(url: &mut Url, key: &str, value: &str) {
    let mut pairs = url
        .query_pairs()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<Vec<_>>();

    let mut replaced = false;
    for (k, v) in &mut pairs {
        if k == key {
            *v = value.to_string();
            replaced = true;
        }
    }
    if !replaced {
        pairs.push((key.to_string(), value.to_string()));
    }

    let mut query = url.query_pairs_mut();
    query.clear();
    for (k, v) in pairs {
        query.append_pair(&k, &v);
    }
}

fn ids_from_url(url: &str) -> (String, String) {
    if let Ok(parsed) = Url::parse(url) {
        let doc_id = parsed
            .query_pairs()
            .find(|(key, _)| key == "docId")
            .map(|(_, value)| value.to_string())
            .unwrap_or_default();
        let dir_id = parsed
            .query_pairs()
            .find(|(key, _)| key == "dirId")
            .map(|(_, value)| value.to_string())
            .unwrap_or_default();
        return (doc_id, dir_id);
    }
    (String::new(), String::new())
}

fn ids_from_detail_script(html: &str) -> (String, String) {
    let doc_id = Regex::new(r"docId\s*:\s*([0-9]+)")
        .ok()
        .and_then(|re| re.captures(html))
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_default();
    let dir_id = Regex::new(r"dirId\s*:\s*([0-9]+)")
        .ok()
        .and_then(|re| re.captures(html))
        .and_then(|caps| caps.get(1).map(|m| m.as_str().to_string()))
        .unwrap_or_default();
    (doc_id, dir_id)
}

fn search_snippet(item: &ElementRef<'_>) -> String {
    let dd_sel = sel("dd");
    for dd in item.select(&dd_sel) {
        let class = dd.value().attr("class").unwrap_or("");
        if class.contains("txt_inline") || class.contains("tag_area") || class.contains("txt_block")
        {
            continue;
        }

        let text = text_of(Some(dd));
        if !text.is_empty() {
            return text;
        }
    }
    String::new()
}

fn search_category(item: &ElementRef<'_>) -> String {
    let selector = sel("dd.txt_block a.txt_g1");
    item.select(&selector)
        .map(|node| text_of(Some(node)))
        .filter(|text| !text.is_empty() && text != "Q&A")
        .collect::<Vec<_>>()
        .join(" > ")
}

fn parse_user_info(node: ElementRef<'_>) -> (String, String, String) {
    let author = first_text(&node, &["span.infoHeadItem"]);
    let mut views = String::new();
    let mut written_at = String::new();
    let date_re = Regex::new(r"\d{4}\.\d{1,2}\.\d{1,2}").unwrap();
    let item_sel = sel("span.infoItem");

    for item in node.select(&item_sel) {
        let text = text_of(Some(item));
        if text.contains("조회수") {
            views = clean_text(text.trim_start_matches("조회수"));
        } else if let Some(found) = date_re.find(&text) {
            written_at = found.as_str().to_string();
        }
    }

    (author, views, written_at)
}

fn image_urls(root: ElementRef<'_>, base_url: &str) -> Vec<String> {
    let img_sel = sel("img[src]");
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for img in root.select(&img_sel) {
        let Some(src) = img.value().attr("src") else {
            continue;
        };
        let Some(url) = absolutize(src, base_url) else {
            continue;
        };
        if seen.insert(url.clone()) {
            out.push(url);
        }
    }
    out
}

fn first_doc_node<'a>(document: &'a Html, selectors: &[&str]) -> Option<ElementRef<'a>> {
    for css in selectors {
        let selector = sel(css);
        if let Some(node) = document.select(&selector).next() {
            return Some(node);
        }
    }
    None
}

fn first_doc_text(document: &Html, selectors: &[&str]) -> Option<String> {
    for css in selectors {
        let selector = sel(css);
        if let Some(node) = document.select(&selector).next() {
            let text = text_or_content(node);
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

fn first_text(root: &ElementRef<'_>, selectors: &[&str]) -> String {
    for css in selectors {
        let selector = sel(css);
        if let Some(node) = root.select(&selector).next() {
            let text = text_or_content(node);
            if !text.is_empty() {
                return text;
            }
        }
    }
    String::new()
}

fn first_attr(root: &ElementRef<'_>, selectors: &[&str], attr: &str) -> Option<String> {
    for css in selectors {
        let selector = sel(css);
        if let Some(value) = root
            .select(&selector)
            .find_map(|node| node.value().attr(attr).map(str::to_string))
        {
            return Some(value);
        }
    }
    None
}

fn text_or_content(node: ElementRef<'_>) -> String {
    if node.value().name() == "meta" {
        return node
            .value()
            .attr("content")
            .map(clean_text)
            .unwrap_or_default();
    }
    text_of(Some(node))
}

fn text_of(node: Option<ElementRef<'_>>) -> String {
    node.map(|node| clean_text(&node.text().collect::<Vec<_>>().join(" ")))
        .unwrap_or_default()
}

fn clean_text<T: AsRef<str>>(text: T) -> String {
    let text = text.as_ref();
    let mut value = text
        .replace('\u{200b}', " ")
        .replace('\u{00a0}', " ")
        .replace('\r', "\n");
    let ws_re = Regex::new(r"[ \t\n]+").unwrap();
    value = ws_re.replace_all(&value, " ").to_string();
    value.trim().to_string()
}

fn capture_first_number(text: &str, pattern: &str) -> String {
    Regex::new(pattern)
        .ok()
        .and_then(|re| re.captures(text))
        .and_then(|caps| caps.get(1).map(|m| m.as_str().replace(',', "")))
        .unwrap_or_default()
}

fn absolutize(raw: &str, base_url: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.is_empty() || raw.starts_with("javascript:") || raw.starts_with('#') {
        return None;
    }

    Url::parse(raw)
        .or_else(|_| Url::parse(base_url).and_then(|base| base.join(raw)))
        .ok()
        .map(|url| url.to_string())
}

fn sel(selector: &str) -> Selector {
    Selector::parse(selector).unwrap()
}

fn write_csv<T: Serialize>(path: &Path, rows: &[T]) -> Result<()> {
    let file = std::fs::File::create(path)?;
    let mut buf = std::io::BufWriter::new(file);
    buf.write_all(b"\xef\xbb\xbf")?;
    let mut writer = csv::WriterBuilder::new().has_headers(true).from_writer(buf);
    for row in rows {
        writer.serialize(row)?;
    }
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_kin_search_rows() {
        let html = r#"
            <ul class="basic1">
                <li>
                    <dl>
                        <dt><a class="_searchListTitleAnchor" href="https://kin.naver.com/qna/detail.naver?dirId=80510&docId=493425786">Title <b>One</b></a></dt>
                        <dd class="txt_inline">2026.05.16.</dd>
                        <dd>Preview body</dd>
                        <dd class="txt_block">
                            <a class="txt_g1">Q&amp;A</a> &gt; <a class="txt_g1">Category</a>
                            <span class="hit">답변수 5</span> UP 1
                        </dd>
                    </dl>
                </li>
            </ul>
        "#;

        let rows = parse_search_rows(
            html,
            2,
            "https://kin.naver.com/search/list.naver?query=x&page=2",
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].search_page, 2);
        assert_eq!(rows[0].title, "Title One");
        assert_eq!(rows[0].doc_id, "493425786");
        assert_eq!(rows[0].dir_id, "80510");
        assert_eq!(rows[0].answer_count, "5");
        assert_eq!(rows[0].up_count, "1");
        assert_eq!(rows[0].category, "Category");
    }

    #[test]
    fn parses_question_detail_without_answer_body() {
        let search = KinSearchRow {
            url: "https://kin.naver.com/qna/detail.naver?dirId=80901&docId=493067603".to_string(),
            title: "Search title".to_string(),
            category: "Search category".to_string(),
            ..Default::default()
        };
        let html = r#"
            <meta property="og:title" content="Question title">
            <div class="userInfo userInfo__bullet">
                <span class="infoHeadItem">비공개</span>
                <span class="infoItem">조회수 203</span>
                <span class="infoItem"><span class="blind">작성일</span>2026.04.22</span>
            </div>
            <div class="questionDetail">
                Question body<br>
                <img src="/image.jpg">
            </div>
            <div class="tagList"><a class="tag">청소, 수리<span class="blind">새 창</span></a></div>
            <div class="answerDetail">Answer body must not be collected</div>
        "#;

        let row = parse_question_page(html, &search);

        assert_eq!(row.title, "Question title");
        assert_eq!(row.author, "비공개");
        assert_eq!(row.views, "203");
        assert_eq!(row.written_at, "2026.04.22");
        assert_eq!(row.category, "청소, 수리");
        assert!(row.question_body.contains("Question body"));
        assert!(!row.question_body.contains("Answer body"));
        assert!(row
            .question_images_json
            .contains("https://kin.naver.com/image.jpg"));
    }
}
