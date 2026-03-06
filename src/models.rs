use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Source {
    NaverBlog,
    NaverCafe,
    Unknown,
}

/// 본문 이미지 1장
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BodyImage {
    /// 표시용 src (.se-image-resource src 속성)
    pub src: String,
    /// 원본 src (data-linkdata JSON 내 src)
    pub original_src: Option<String>,
    pub original_width: Option<u32>,
    pub original_height: Option<u32>,
    pub file_size: Option<u64>,
}

/// 댓글 1건 (대댓글 포함)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Comment {
    /// li의 id 속성
    pub comment_id: String,
    /// 대댓글 여부 (CommentItem--reply 클래스)
    pub is_reply: bool,
    /// 닉네임 (.comment_nickname)
    pub author: Option<String>,
    /// 레벨 아이콘 style에서 추출한 이미지 URL
    pub author_level_icon: Option<String>,
    /// 프로필 이미지 URL (a.comment_thumb img src)
    pub author_avatar: Option<String>,
    /// 작성일시 (.comment_info_date)
    pub date: String,
    /// 댓글 내용 (span.text_comment)
    pub content: String,
}

/// 게시글 1건
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostData {
    pub source: Source,
    pub url: Url,
    pub title: String,
    /// 작성자 닉네임 (.profile_info .nickname)
    pub author: String,
    /// 작성자 등급 (.profile_info .nick_level)
    pub author_level: String,
    /// 작성일시 (.article_info .date)
    pub written_at: String,
    /// 조회수 (.article_info .count)
    pub views: String,
    /// 본문 텍스트 (p.se-text-paragraph 문단 줄바꿈 결합)
    pub body: String,
    /// 본문 이미지 목록
    pub body_images: Vec<BodyImage>,
    pub comments: Vec<Comment>,
}

impl PostData {
    pub fn key(&self) -> String {
        self.url.as_str().to_string()
    }
}
