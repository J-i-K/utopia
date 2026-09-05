//! Canonical HTML conversion shared by file, URL and feed ingestion.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum HtmlError {
    #[error("HTML conversion failed: {0}")]
    Conversion(String),
    #[error("HTML is an authentication or player interstitial")]
    Interstitial,
}

/// Extract a full page; Readability failure uses one raw conversion fallback.
pub fn page_to_markdown(html: &str, base_url: Option<&str>) -> Result<String, HtmlError> {
    if looks_like_challenge_html(html) {
        return Err(HtmlError::Interstitial);
    }
    let article = dom_smoothie::Readability::new(html, base_url, None)
        .and_then(|mut readability| readability.parse());
    match article {
        Ok(article) => {
            let markdown = markdown_from_html(&article.content)?;
            let title = article.title.trim();
            if title.is_empty() || markdown.contains(title) {
                Ok(markdown)
            } else {
                Ok(sanitize_markdown_links(&format!("# {title}\n\n{markdown}")))
            }
        }
        Err(_) => markdown_from_html(html),
    }
}

/// Feed fragments are already scoped; never run Readability over them.
pub fn fragment_to_markdown(html: &str) -> Result<String, HtmlError> {
    markdown_from_html(html)
}

fn looks_like_challenge_html(html: &str) -> bool {
    let lowered = html.to_ascii_lowercase();
    let auth_form = lowered.contains("<form")
        && (lowered.contains("password")
            || lowered.contains("type=\"email\"")
            || lowered.contains("type='email'"));
    let player_shell = [
        "jwplayer",
        "brightcove",
        "data-player",
        "<video",
        "youtube.com/embed",
        "player.vimeo.com",
    ]
    .iter()
    .filter(|marker| lowered.contains(**marker))
    .count()
        >= 2;
    auth_form || player_shell
}

fn markdown_from_html(html: &str) -> Result<String, HtmlError> {
    if looks_like_challenge_html(html) {
        return Err(HtmlError::Interstitial);
    }
    let markdown = htmd::HtmlToMarkdown::builder()
        .skip_tags(vec![
            "script", "style", "iframe", "object", "embed", "img", "svg", "math",
        ])
        .scripting_enabled(false)
        .build()
        .convert(html)
        .map_err(|e| HtmlError::Conversion(e.to_string()))?;
    normalize_markdown(&markdown)
}

/// Normalize direct Markdown with the same link policy as HTML conversion.
pub fn normalize_markdown(markdown: &str) -> Result<String, HtmlError> {
    Ok(sanitize_markdown_links(&post_process(markdown)))
}

fn post_process(markdown: &str) -> String {
    let mut result = String::with_capacity(markdown.len());
    let mut newlines = 0;
    for ch in markdown.chars() {
        if ch == '\n' {
            newlines += 1;
            if newlines <= 3 {
                result.push(ch);
            }
        } else {
            newlines = 0;
            result.push(ch);
        }
    }
    result.trim().to_string()
}

fn sanitize_markdown_links(markdown: &str) -> String {
    let mut result = String::with_capacity(markdown.len());
    let bytes = markdown.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let Some(relative_start) = markdown[cursor..].find('[') else {
            result.push_str(&markdown[cursor..]);
            break;
        };
        let start = cursor + relative_start;
        result.push_str(&markdown[cursor..start]);
        if start > 0 && bytes[start - 1] == b'!' {
            result.push('[');
            cursor = start + 1;
            continue;
        }
        let Some(relative_close_label) = markdown[start + 1..].find("](") else {
            result.push('[');
            cursor = start + 1;
            continue;
        };
        let close_label = start + 1 + relative_close_label;
        let Some(close_link) = find_unescaped_byte(bytes, close_label + 2, b')') else {
            result.push('[');
            cursor = start + 1;
            continue;
        };
        let destination = &markdown[close_label + 2..close_link];
        if !is_unsafe_markdown_destination(destination) {
            result.push_str(&markdown[start..=close_link]);
        } else {
            result.push_str(&markdown[start + 1..close_label]);
        }
        cursor = close_link + 1;
    }
    result
}

fn find_unescaped_byte(bytes: &[u8], start: usize, wanted: u8) -> Option<usize> {
    let mut escaped = false;
    for (offset, byte) in bytes.iter().enumerate().skip(start) {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == wanted {
            return Some(offset);
        }
    }
    None
}

fn is_unsafe_markdown_destination(destination: &str) -> bool {
    let destination = destination
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .trim();
    let decoded = percent_encoding::percent_decode_str(destination).decode_utf8_lossy();
    let decoded =
        decoded.trim_start_matches(|ch: char| ch.is_ascii_control() || ch.is_ascii_whitespace());
    let Some((scheme, _)) = decoded.split_once(':') else {
        return false;
    };
    matches!(
        scheme.to_ascii_lowercase().as_str(),
        "javascript" | "vbscript" | "data" | "file" | "blob"
    )
}
