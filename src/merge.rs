use std::collections::{HashMap, HashSet};

use crate::models::PostData;

/// URL 기준으로 게시글 병합.
/// - 스칼라 필드: 먼저 들어온 비어있지 않은 값 우선
/// - 댓글: (comment_id, content) 기준 중복 제거
pub fn merge_posts(posts: Vec<PostData>) -> Vec<PostData> {
    let mut map: HashMap<String, PostData> = HashMap::new();

    for p in posts {
        let key = p.key();
        map.entry(key)
            .and_modify(|existing| {
                if existing.title.trim().is_empty() && !p.title.trim().is_empty() {
                    existing.title = p.title.clone();
                }
                if existing.written_at.trim().is_empty() && !p.written_at.trim().is_empty() {
                    existing.written_at = p.written_at.clone();
                }
                if existing.body.trim().is_empty() && !p.body.trim().is_empty() {
                    existing.body = p.body.clone();
                }
                if existing.body_images.is_empty() && !p.body_images.is_empty() {
                    existing.body_images = p.body_images.clone();
                }

                let mut seen: HashSet<(String, String)> = existing
                    .comments
                    .iter()
                    .map(|c| (c.comment_id.clone(), c.content.clone()))
                    .collect();

                for c in p.comments.iter() {
                    let k = (c.comment_id.clone(), c.content.clone());
                    if seen.insert(k) {
                        existing.comments.push(c.clone());
                    }
                }
            })
            .or_insert(p);
    }

    map.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{Comment, Source};
    use url::Url;

    fn make_comment(id: &str, content: &str) -> Comment {
        Comment {
            comment_id: id.into(),
            is_reply: false,
            author: Some("kim".into()),
            author_level_icon: None,
            author_avatar: None,
            date: "2020-01-01".into(),
            content: content.into(),
        }
    }

    #[test]
    fn merges_scalars_and_dedups_comments() {
        let url = Url::parse("https://example.com/a").unwrap();
        let a = PostData {
            source: Source::Unknown,
            url: url.clone(),
            title: "".into(),
            author: "".into(),
            author_level: "".into(),
            written_at: "2020-01-01".into(),
            views: "".into(),
            body: "hello".into(),
            body_images: vec![],
            comments: vec![make_comment("1", "nice")],
        };
        let b = PostData {
            source: Source::Unknown,
            url: url.clone(),
            title: "title".into(),
            author: "".into(),
            author_level: "".into(),
            written_at: "".into(),
            views: "".into(),
            body: "".into(),
            body_images: vec![],
            comments: vec![make_comment("1", "nice")],
        };

        let merged = merge_posts(vec![a, b]);
        assert_eq!(merged.len(), 1);
        let m = &merged[0];
        assert_eq!(m.title, "title");
        assert_eq!(m.written_at, "2020-01-01");
        assert_eq!(m.body, "hello");
        assert_eq!(m.comments.len(), 1);
    }
}
