use std::{fs, path::Path};

use url::Url;

use crate::errors::CrawlError;

pub fn read_urls_from_file(path: &Path) -> Result<Vec<Url>, CrawlError> {
    let raw = fs::read_to_string(path)
        .map_err(|e| CrawlError::Parse(format!("failed to read {}: {e}", path.display())))?;

    let mut out = Vec::new();
    for (i, line) in raw.lines().enumerate() {
        let s = line.trim();
        if s.is_empty() || s.starts_with('#') {
            continue;
        }
        let url = Url::parse(s).map_err(|e| {
            CrawlError::Parse(format!("invalid url at line {}: {s} ({e})", i + 1))
        })?;
        out.push(url);
    }
    Ok(out)
}
