use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
};

use csv::Writer;
use tracing::info;

fn writer_with_bom(path: &PathBuf) -> Result<Writer<fs::File>, csv::Error> {
    let mut file = fs::File::create(path)?;
    file.write_all(b"\xEF\xBB\xBF")?;
    Ok(Writer::from_writer(file))
}

use crate::models::{PostData, Source};

pub fn ensure_out_dir(out_dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(out_dir)
}

/// results.csv — 게시글 1행: 제목, url, 날짜, 본문, 댓글
pub fn write_posts_csv(out_dir: &Path, posts: &[PostData]) -> Result<PathBuf, csv::Error> {
    let path = out_dir.join("results.csv");
    let mut w = writer_with_bom(&path)?;

    w.write_record(["제목", "url", "날짜", "본문", "댓글"])?;

    for p in posts {
        // 댓글 텍스트를 한 셀에 " | " 로 구분해서 합침
        let comments_joined: String = p
            .comments
            .iter()
            .map(|c| {
                let author = c.author.as_deref().unwrap_or("익명");
                format!("[{}] {}", author, c.content)
            })
            .collect::<Vec<_>>()
            .join(" | ");

        w.write_record([
            &p.title,
            p.url.as_str(),
            &p.written_at,
            &p.body,
            &comments_joined,
        ])?;
    }
    w.flush()?;
    info!(path = %path.display(), rows = posts.len(), "results.csv 저장");
    Ok(path)
}

/// comments.csv — 댓글 상세 (별도 분석용)
pub fn write_comments_csv(out_dir: &Path, posts: &[PostData]) -> Result<PathBuf, csv::Error> {
    let path = out_dir.join("comments.csv");
    let mut w = writer_with_bom(&path)?;

    w.write_record([
        "post_url",
        "comment_id",
        "is_reply",
        "author",
        "date",
        "content",
    ])?;

    let mut rows = 0usize;
    for p in posts {
        for c in &p.comments {
            w.write_record([
                p.url.as_str(),
                &c.comment_id,
                if c.is_reply { "true" } else { "false" },
                c.author.as_deref().unwrap_or(""),
                &c.date,
                &c.content,
            ])?;
            rows += 1;
        }
    }
    w.flush()?;
    info!(path = %path.display(), rows, "comments.csv 저장");
    Ok(path)
}

#[allow(dead_code)]
/// images.csv — 본문 이미지 원본 정보
pub fn write_images_csv(out_dir: &Path, posts: &[PostData]) -> Result<PathBuf, csv::Error> {
    let path = out_dir.join("images.csv");
    let mut w = writer_with_bom(&path)?;

    w.write_record([
        "post_url",
        "display_src",
        "original_src",
        "original_width",
        "original_height",
        "file_size",
    ])?;

    let mut rows = 0usize;
    for p in posts {
        for img in &p.body_images {
            w.write_record([
                p.url.as_str(),
                &img.src,
                img.original_src.as_deref().unwrap_or(""),
                &img.original_width
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
                &img.original_height
                    .map(|n| n.to_string())
                    .unwrap_or_default(),
                &img.file_size.map(|n| n.to_string()).unwrap_or_default(),
            ])?;
            rows += 1;
        }
    }
    w.flush()?;
    info!(path = %path.display(), rows, "images.csv 저장");
    Ok(path)
}

#[allow(dead_code)]
fn source_to_str(s: Source) -> &'static str {
    match s {
        Source::NaverBlog => "naver_blog",
        Source::NaverCafe => "naver_cafe",
        Source::Unknown => "unknown",
    }
}
