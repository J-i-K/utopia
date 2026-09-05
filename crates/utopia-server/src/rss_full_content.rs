use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::net::IpAddr;
use std::time::{Duration, Instant};
use url::Url;
use utopia_ingest::html::{self, HtmlError};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum ContentError {
    #[error("invalid article URL")]
    InvalidUrl,
    #[error("article destination is not public")]
    BlockedAddress,
    #[error("article DNS resolution failed")]
    DnsResolution,
    #[error("article connection failed")]
    Network,
    #[error("article request timed out")]
    Timeout,
    #[error("article response is too large")]
    TooLarge,
    #[error("article response is not HTML")]
    NotHtml,
    #[error("article response encoding is unsupported")]
    UnsupportedEncoding,
    #[error("article redirect is invalid")]
    InvalidRedirect,
    #[error("article redirect downgraded HTTPS to HTTP")]
    BlockedRedirect,
    #[error("article redirect limit exceeded")]
    RedirectLimit,
    #[error("article response status is not acceptable")]
    HttpStatus { retryable: bool },
    #[error("HTML conversion failed: {0}")]
    Conversion(String),
    #[error("article page is a challenge or consent shell")]
    ChallengePage,
    #[error("article page requires unsupported video or player content")]
    VideoRequired,
    #[error("article content is not substantive")]
    NonSubstantive,
    #[error("article has no usable content source")]
    NoUsableSource,
    #[error("canonical Markdown is too large")]
    MarkdownTooLarge,
}

impl ContentError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::InvalidUrl => "invalid_url",
            Self::BlockedAddress => "blocked_address",
            Self::DnsResolution => "dns_timeout",
            Self::Network => "connect_timeout",
            Self::Timeout => "connect_timeout",
            Self::TooLarge => "response_too_large",
            Self::NotHtml => "unsupported_content_type",
            Self::UnsupportedEncoding => "unsupported_content_type",
            Self::InvalidRedirect | Self::BlockedRedirect | Self::RedirectLimit => {
                "blocked_redirect"
            }
            Self::HttpStatus { retryable: true } => "http_retryable",
            Self::HttpStatus { retryable: false } => "http_denied",
            Self::Conversion(_) => "conversion_failed",
            Self::ChallengePage => "challenge_page",
            Self::VideoRequired => "video_required",
            Self::NonSubstantive => "non_substantive",
            Self::NoUsableSource => "non_substantive",
            Self::MarkdownTooLarge => "conversion_failed",
        }
    }

    pub(crate) fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::DnsResolution
                | Self::Network
                | Self::Timeout
                | Self::HttpStatus { retryable: true }
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FetchConfig {
    pub(crate) max_response_bytes: usize,
    pub(crate) max_redirects: usize,
    pub(crate) dns_timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) read_timeout: Duration,
    pub(crate) overall_timeout: Duration,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            max_response_bytes: 8 * 1024 * 1024,
            max_redirects: 5,
            dns_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(15),
            overall_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FetchDeadline(Instant);

impl FetchDeadline {
    fn new(timeout: Duration) -> Self {
        Self(Instant::now() + timeout)
    }

    fn remaining(self) -> Result<Duration, ContentError> {
        let remaining = self.0.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            Err(ContentError::Timeout)
        } else {
            Ok(remaining)
        }
    }
}

#[derive(Debug)]
pub(crate) struct FetchedPage {
    pub(crate) final_url: Url,
    pub(crate) content_type: String,
    pub(crate) body: String,
}

pub(crate) async fn fetch_article(raw_url: &str) -> Result<FetchedPage, ContentError> {
    fetch_article_with_config(raw_url, FetchConfig::default()).await
}

const MAX_MARKDOWN_BYTES: usize = 2 * 1024 * 1024;
const HYDRATION_ATTEMPTS: i32 = 5;

#[derive(Debug)]
struct AcceptedContent {
    markdown: String,
    source: &'static str,
    final_url: Option<String>,
}

/// Durable job entry point. The job payload contains only UUIDs; all bounded
/// feed data comes from the ledger row, so a later replay does not depend on a
/// feed still exposing the item.
pub(crate) async fn hydrate_entry(
    state: &crate::state::AppState,
    hydration_job_id: i64,
    source_id: uuid::Uuid,
    entry_id: uuid::Uuid,
) -> anyhow::Result<()> {
    let entry = utopia_store::rss_full_content::get_entry(&state.pool, entry_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("RSS hydration entry was deleted"))?;
    if entry.source_id != source_id {
        return Err(anyhow::Error::new(utopia_core::Terminal).context("source_mismatch"));
    }
    let source = utopia_store::sources::get(&state.pool, source_id).await?;
    let Some(attempt) =
        utopia_store::rss_full_content::current_attempt(&state.pool, entry_id, hydration_job_id)
            .await?
    else {
        return Ok(());
    };
    state.emit_source(source.kb_id);

    let result = hydrate_content(&entry).await;
    let accepted = match result {
        Ok(content) => content,
        Err(error) => {
            let result = handle_hydration_failure(
                &state.pool,
                entry_id,
                hydration_job_id,
                attempt,
                error.into(),
            )
            .await;
            state.emit_source(source.kb_id);
            return result;
        }
    };
    let digest = sha256_hex(accepted.markdown.as_bytes());
    if let Err(error) =
        crate::ingest_sources::write_blob(state, &digest, accepted.markdown.as_bytes()).await
    {
        let result =
            handle_hydration_failure(&state.pool, entry_id, hydration_job_id, attempt, error).await;
        state.emit_source(source.kb_id);
        return result;
    }
    let completed = match utopia_store::rss_full_content::complete_hydration(
        &state.pool,
        entry_id,
        hydration_job_id,
        source.id,
        entry.activation_generation,
        source.kb_id,
        &entry.external_key,
        &format!("{}.md", hydration_filename(&entry.title)),
        "text/markdown",
        accepted.markdown.len() as i64,
        &digest,
        entry.doc_time,
        accepted.source,
        accepted.final_url.as_deref(),
    )
    .await
    {
        Ok(completed) => completed,
        Err(error) => {
            let result = handle_hydration_failure(
                &state.pool,
                entry_id,
                hydration_job_id,
                attempt,
                error.into(),
            )
            .await;
            state.emit_source(source.kb_id);
            return result;
        }
    };
    if let Some(document_id) = completed {
        state.emit_document(source.kb_id, document_id);
    }
    state.emit_source(source.kb_id);
    Ok(())
}

async fn hydrate_content(
    entry: &utopia_store::rss_full_content::Entry,
) -> Result<AcceptedContent, ContentError> {
    if let Some(embedded_html) = entry.embedded_html.as_deref() {
        if let Ok(markdown) = normalize_feed_html(embedded_html) {
            if quality_check(&markdown, &entry.summary, false).is_ok() {
                return canonical_content(markdown, "feed", None);
            }
        }
    }

    let Some(article_url) = entry.article_url.as_deref() else {
        return Err(ContentError::NoUsableSource);
    };
    let page = fetch_article(article_url).await?;
    let markdown = if page.content_type == "text/markdown" {
        normalize_markdown(&page.body)?
    } else {
        extract_article_markdown(&page.body, page.final_url.as_str())?
    };
    accept_linked_markdown(markdown, &entry.summary, Some(page.final_url.to_string()))
}

fn canonical_content(
    markdown: String,
    source: &'static str,
    final_url: Option<String>,
) -> Result<AcceptedContent, ContentError> {
    if markdown.is_empty() {
        return Err(ContentError::NonSubstantive);
    }
    if markdown.len() > MAX_MARKDOWN_BYTES {
        return Err(ContentError::MarkdownTooLarge);
    }
    Ok(AcceptedContent {
        markdown,
        source,
        final_url,
    })
}

fn accept_linked_markdown(
    markdown: String,
    summary: &str,
    final_url: Option<String>,
) -> Result<AcceptedContent, ContentError> {
    quality_check(&markdown, summary, true)?;
    canonical_content(markdown, "web", final_url)
}

async fn handle_hydration_failure(
    pool: &sqlx::PgPool,
    entry_id: uuid::Uuid,
    hydration_job_id: i64,
    attempt: i32,
    error: anyhow::Error,
) -> anyhow::Result<()> {
    let (code, retryable) = error
        .downcast_ref::<ContentError>()
        .map(|error| (error.code(), error.is_retryable()))
        .unwrap_or(("ingest_failed", true));
    let terminal = !retryable || attempt >= HYDRATION_ATTEMPTS;
    if utopia_store::rss_full_content::current_attempt(pool, entry_id, hydration_job_id)
        .await?
        .is_none()
    {
        return Ok(());
    }
    if terminal {
        return Err(anyhow::Error::new(utopia_core::Terminal).context(code));
    }
    Err(anyhow::anyhow!(code))
}

fn hydration_filename(title: &str) -> String {
    let filename: String = title
        .chars()
        .map(|ch| if ch.is_alphanumeric() { ch } else { '-' })
        .take(80)
        .collect();
    let filename = filename.trim_matches('-');
    if filename.is_empty() {
        "untitled".into()
    } else {
        filename.into()
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) async fn fetch_article_with_config(
    raw_url: &str,
    config: FetchConfig,
) -> Result<FetchedPage, ContentError> {
    let initial_url = validate_article_url(raw_url)?;
    let deadline = FetchDeadline::new(config.overall_timeout);
    tokio::time::timeout(
        config.overall_timeout,
        fetch_redirect_chain(initial_url, config, deadline),
    )
    .await
    .map_err(|_| ContentError::Timeout)?
}

async fn fetch_redirect_chain(
    mut current_url: Url,
    config: FetchConfig,
    deadline: FetchDeadline,
) -> Result<FetchedPage, ContentError> {
    for redirect_count in 0..=config.max_redirects {
        let response = fetch_once(&current_url, config, deadline).await?;
        if response.status().is_redirection() {
            if redirect_count == config.max_redirects {
                return Err(ContentError::RedirectLimit);
            }
            let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
                return Err(ContentError::InvalidRedirect);
            };
            let location = location
                .to_str()
                .map_err(|_| ContentError::InvalidRedirect)?;
            let next_url = redirect_target(&current_url, location)?;
            current_url = next_url;
            continue;
        }

        if !response.status().is_success() {
            return Err(ContentError::HttpStatus {
                retryable: matches!(response.status().as_u16(), 401 | 403 | 408 | 425 | 429)
                    || response.status().is_server_error(),
            });
        }
        return read_page_response(response, current_url, config, deadline).await;
    }
    Err(ContentError::RedirectLimit)
}

async fn fetch_once(
    url: &Url,
    config: FetchConfig,
    deadline: FetchDeadline,
) -> Result<reqwest::Response, ContentError> {
    let host = url.host_str().ok_or(ContentError::InvalidUrl)?;
    let addresses = resolve_public_addresses(host, url, config, deadline).await?;
    let remaining = deadline.remaining()?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .no_hickory_dns()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(std::cmp::min(config.connect_timeout, remaining))
        .read_timeout(std::cmp::min(config.read_timeout, remaining))
        .timeout(remaining)
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .user_agent("Utopia RSS content fetcher/1")
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(|_| ContentError::Network)?;
    send_once(&client, url).await
}

async fn send_once(client: &reqwest::Client, url: &Url) -> Result<reqwest::Response, ContentError> {
    client
        .get(url.clone())
        .header(
            reqwest::header::ACCEPT,
            "text/html,application/xhtml+xml,text/markdown",
        )
        .header(reqwest::header::ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(|error| {
            if error.is_timeout() {
                ContentError::Timeout
            } else {
                ContentError::Network
            }
        })
}

async fn resolve_public_addresses(
    host: &str,
    url: &Url,
    config: FetchConfig,
    deadline: FetchDeadline,
) -> Result<Vec<std::net::SocketAddr>, ContentError> {
    deadline.remaining()?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        return validate_resolved_addresses([std::net::SocketAddr::new(ip, 0)]);
    }

    let port = url
        .port_or_known_default()
        .ok_or(ContentError::InvalidUrl)?;
    let dns_timeout = std::cmp::min(config.dns_timeout, deadline.remaining()?);
    let lookup = tokio::time::timeout(dns_timeout, tokio::net::lookup_host((host, port)))
        .await
        .map_err(|_| ContentError::Timeout)?
        .map_err(|_| ContentError::DnsResolution)?;
    validate_resolved_addresses(lookup)
}

fn validate_resolved_addresses(
    addresses: impl IntoIterator<Item = std::net::SocketAddr>,
) -> Result<Vec<std::net::SocketAddr>, ContentError> {
    let mut unique = HashSet::new();
    let mut public = Vec::new();
    for address in addresses {
        if !is_public_ip(address.ip()) {
            return Err(ContentError::BlockedAddress);
        }
        if unique.insert(address.ip()) {
            public.push(std::net::SocketAddr::new(address.ip(), 0));
        }
    }
    if public.is_empty() {
        return Err(ContentError::DnsResolution);
    }
    Ok(public)
}

async fn read_page_response(
    response: reqwest::Response,
    final_url: Url,
    config: FetchConfig,
    deadline: FetchDeadline,
) -> Result<FetchedPage, ContentError> {
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase);
    let Some(content_type) = content_type else {
        return Err(ContentError::NotHtml);
    };
    if !matches!(
        content_type.as_str(),
        "text/html" | "application/xhtml+xml" | "text/markdown"
    ) {
        return Err(ContentError::NotHtml);
    }

    if response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .is_some_and(|value| {
            value
                .to_str()
                .map(|value| !value.trim().is_empty() && !value.eq_ignore_ascii_case("identity"))
                .unwrap_or(true)
        })
    {
        return Err(ContentError::UnsupportedEncoding);
    }

    if response
        .content_length()
        .is_some_and(|length| length > config.max_response_bytes as u64)
    {
        return Err(ContentError::TooLarge);
    }

    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    loop {
        let chunk = tokio::time::timeout(deadline.remaining()?, stream.next())
            .await
            .map_err(|_| ContentError::Timeout)?;
        let Some(chunk) = chunk else {
            break;
        };
        let chunk = chunk.map_err(|_| ContentError::Network)?;
        if body.len().saturating_add(chunk.len()) > config.max_response_bytes {
            return Err(ContentError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    let body =
        String::from_utf8(body).map_err(|_| ContentError::Conversion("invalid UTF-8".into()))?;
    Ok(FetchedPage {
        final_url,
        content_type,
        body,
    })
}

impl From<HtmlError> for ContentError {
    fn from(error: HtmlError) -> Self {
        match error {
            HtmlError::Interstitial => Self::ChallengePage,
            HtmlError::Conversion(detail) => Self::Conversion(detail),
        }
    }
}

pub(crate) fn normalize_feed_html(raw: &str) -> Result<String, ContentError> {
    html::fragment_to_markdown(raw).map_err(Into::into)
}

pub(crate) fn extract_article_markdown(raw: &str, url: &str) -> Result<String, ContentError> {
    html::page_to_markdown(raw, Some(url)).map_err(Into::into)
}

pub(crate) fn normalize_markdown(markdown: &str) -> Result<String, ContentError> {
    let markdown = html::normalize_markdown(markdown)?;
    if markdown.is_empty() {
        return Err(ContentError::NonSubstantive);
    }
    Ok(markdown)
}

fn looks_like_challenge_shell(lowered: &str) -> bool {
    const HIGH_CONFIDENCE_MARKERS: &[&str] = &[
        "verify you are human",
        "checking your browser",
        "enable javascript and cookies",
        "accept cookies to continue",
        "subscribe to continue",
        "subscribe to read",
        "sign in to continue",
        "log in to continue",
        "this content is behind a paywall",
        "content is behind a paywall",
        "unlock this article",
        "captcha challenge",
        "complete the captcha",
        "solve the captcha",
        "enter the captcha",
        "captcha verification",
    ];
    if HIGH_CONFIDENCE_MARKERS
        .iter()
        .any(|marker| lowered.contains(marker))
        || lowered
            .lines()
            .take(3)
            .any(|line| line.trim().trim_start_matches('#').trim() == "access denied")
    {
        return true;
    }

    // Readability can retain a long login/consent shell without any of the
    // exact phrases above. Require two short, control-like lines so ordinary
    // articles that mention one of these topics are not rejected.
    const STRUCTURAL_MARKERS: &[&str] = &[
        "sign in",
        "log in",
        "login",
        "create account",
        "register",
        "cookie settings",
        "consent preferences",
        "accept all cookies",
        "enable javascript",
        "access denied",
        "play video",
        "watch now",
    ];
    let structural_hits = lowered
        .lines()
        .take(40)
        .filter(|line| {
            let line = line.trim().trim_start_matches('#').trim();
            line.len() <= 96
                && STRUCTURAL_MARKERS
                    .iter()
                    .any(|marker| line.contains(marker))
        })
        .count();
    let has_auth_fields = (lowered.contains("password")
        && (lowered.contains("email") || lowered.contains("username")))
        || lowered.contains("<form");
    structural_hits >= 2 || (structural_hits >= 1 && has_auth_fields)
}

pub(crate) fn quality_check(
    markdown: &str,
    summary: &str,
    linked_page: bool,
) -> Result<(), ContentError> {
    if !is_substantive(markdown, summary, linked_page) {
        return Err(ContentError::NonSubstantive);
    }
    let lowered = markdown.to_ascii_lowercase();
    if looks_like_challenge_shell(&lowered) {
        return Err(ContentError::ChallengePage);
    }
    let link_count = markdown.matches("](").count();
    if lowered.contains("video player")
        || lowered.contains("watch the video")
        || (link_count > 20 && markdown.split_whitespace().count() < link_count * 8)
    {
        return Err(ContentError::VideoRequired);
    }
    Ok(())
}

fn is_substantive(markdown: &str, summary: &str, linked_page: bool) -> bool {
    let non_whitespace = markdown.chars().filter(|ch| !ch.is_whitespace()).count();
    let words = markdown.split_whitespace().count();
    let (min_words, min_chars) = if linked_page { (200, 1_200) } else { (80, 500) };
    if words < min_words && non_whitespace < min_chars {
        return false;
    }
    if summary.trim().is_empty() {
        return true;
    }
    let normalized = |value: &str| {
        value
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    };
    let body = normalized(markdown);
    let summary = normalized(summary);
    body != summary
        && !(body.starts_with(&summary)
            && body.len().saturating_sub(summary.len()) < if linked_page { 300 } else { 160 })
}

pub(crate) fn validate_article_url(raw: &str) -> Result<Url, ContentError> {
    let parsed = Url::parse(raw).map_err(|_| ContentError::InvalidUrl)?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || parsed.fragment().is_some()
        || parsed.port().is_some_and(|port| !matches!(port, 80 | 443))
        || parsed.username() != ""
        || parsed.password().is_some()
        || raw_authority_contains_userinfo(raw)
    {
        return Err(ContentError::InvalidUrl);
    }
    if let Some(ip) = parsed.host().and_then(|host| match host {
        url::Host::Ipv4(ip) => Some(IpAddr::V4(ip)),
        url::Host::Ipv6(ip) => Some(IpAddr::V6(ip)),
        url::Host::Domain(_) => None,
    }) {
        if !is_public_ip(ip) {
            return Err(ContentError::BlockedAddress);
        }
    }
    Ok(parsed)
}

fn raw_authority_contains_userinfo(raw: &str) -> bool {
    let Some(authority) = raw
        .split_once("://")
        .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or(rest))
    else {
        return false;
    };
    authority.contains('@')
}

fn redirect_target(current_url: &Url, location: &str) -> Result<Url, ContentError> {
    let next_url = current_url
        .join(location)
        .map_err(|_| ContentError::InvalidRedirect)?;
    if current_url.scheme() == "https" && next_url.scheme() == "http" {
        return Err(ContentError::BlockedRedirect);
    }
    validate_article_url(next_url.as_str())
}

fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, d] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && b == 0)
                || (a == 192 && b == 0 && c == 2)
                || (a == 192 && b == 88 && c == 99)
                || (a == 192 && b == 168)
                || (a == 198 && (b == 18 || b == 19))
                || (a == 198 && b == 51 && c == 100)
                || (a == 203 && b == 0 && c == 113)
                || a >= 224
                || (a == 255 && b == 255 && c == 255 && d == 255))
        }
        IpAddr::V6(ip) => {
            let segments = ip.segments();
            let first = segments[0];
            let is_v4_mapped = segments[..5] == [0, 0, 0, 0, 0] && segments[5] == 0xffff;
            let mapped_v4 = u32::from(segments[6]) << 16 | u32::from(segments[7]);
            let is_nat64 = segments[..6] == [0x0064, 0xff9b, 0, 0, 0, 0];
            let is_private_nat64 = segments[..3] == [0x0064, 0xff9b, 1];
            let is_6to4 = first == 0x2002;
            let embedded_v4 = if is_v4_mapped || is_nat64 {
                Some(mapped_v4)
            } else if is_6to4 {
                Some(u32::from(segments[1]) << 16 | u32::from(segments[2]))
            } else {
                None
            };
            !(ip.is_loopback()
                || ip.is_unspecified()
                || (first & 0xff00) == 0xff00
                || (first & 0xfe00) == 0xfc00
                || (first & 0xffc0) == 0xfe80
                || is_private_nat64
                || (first == 0x2001 && segments[1] == 0x0db8)
                || (first == 0x2001 && segments[1] == 0x0000)
                || (first == 0x2001 && (segments[1] & 0xfff0) == 0x0010)
                || (first == 0x2001 && segments[1] == 0x0002)
                || (first == 0x3fff && (segments[1] & 0xf000) == 0))
                && embedded_v4
                    .is_none_or(|v4| is_public_ip(IpAddr::V4(std::net::Ipv4Addr::from(v4))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::net::SocketAddr;
    use uuid::Uuid;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn generic_html_and_rss_share_article_body_with_base_dependent_links() {
        let body = "Substantive reporting with detailed supporting evidence. ".repeat(80);
        let raw = format!("<html><head><title>Story</title></head><body><nav>Navigation noise</nav><article><h1>Story</h1><p>{body}</p><p><a href='related'>Related reporting</a></p></article><footer>Footer noise</footer></body></html>");
        let generic = utopia_ingest::parse("page.html", raw.as_bytes())
            .unwrap()
            .text;
        let rss = extract_article_markdown(&raw, "https://example.com/redirected/story").unwrap();
        for markdown in [&generic, &rss] {
            assert!(markdown.contains(body.trim()));
            assert!(markdown.contains("Story"));
            assert!(!markdown.contains("Navigation noise"));
            assert!(!markdown.contains("Footer noise"));
        }
        assert!(rss.contains("https://example.com/redirected/related"));
        let body_semantics = |text: &str| text.split_whitespace().collect::<Vec<_>>().join(" ");
        assert_eq!(
            body_semantics(&generic),
            body_semantics(&rss.replace("https://example.com/redirected/related", "related"))
        );
    }

    #[test]
    fn direct_markdown_uses_shared_normalization() {
        let raw = "  # Story\n\n\n\n[bad](%6Aavascript:evil) [safe](https://example.com)  ";
        assert_eq!(
            normalize_markdown(raw).unwrap(),
            html::normalize_markdown(raw).unwrap()
        );
        assert!(!normalize_markdown(raw).unwrap().contains("%6Aavascript"));
        assert_eq!(normalize_markdown("  "), Err(ContentError::NonSubstantive));
    }

    #[test]
    fn html_interstitial_cannot_be_lost_by_readability() {
        let body = "Detailed reporting with plenty of supporting evidence. ".repeat(80);
        let html = format!("<form><input type='password'></form><article><p>{body}</p></article>");
        assert_eq!(
            extract_article_markdown(&html, "https://example.com/story"),
            Err(ContentError::ChallengePage)
        );
    }

    #[test]
    fn feed_html_is_normalized_to_markdown() {
        let html = "<article><h1>Article title</h1><p>Article body.</p></article>";
        let markdown = normalize_feed_html(html).expect("normalization should succeed");
        assert!(markdown.contains("# Article title"));
        assert!(markdown.contains("Article body."));
    }

    #[test]
    fn article_url_policy_rejects_unsafe_shapes() {
        for raw in [
            "file:///etc/passwd",
            "ftp://example.com/article",
            "https://user:pass@example.com/article",
            "https://@example.com/article",
            "https://example.com:8443/article",
            "https://example.com/article#fragment",
            "http://127.0.0.1/article",
            "http://[::1]/article",
        ] {
            assert!(
                validate_article_url(raw).is_err(),
                "unsafe URL was accepted: {raw}"
            );
        }
        assert!(validate_article_url("https://example.com/article").is_ok());
    }

    #[test]
    fn feed_normalization_removes_active_and_image_content() {
        let html = r#"<p>Keep this text.</p><script>alert('x')</script><img src="https://example.com/a.png"><p><a href="javascript:alert(1)">bad link</a></p>"#;
        let markdown = normalize_feed_html(html).expect("normalization should succeed");
        assert!(markdown.contains("Keep this text."));
        assert!(!markdown.contains("alert"));
        assert!(!markdown.contains("!["));
        assert!(!markdown.contains("javascript:"));
    }

    #[test]
    fn feed_normalization_preserves_utf8_and_safe_links() {
        let html = r#"<p>Привет, мир — 你好。</p><p><a href="https://example.com/article">安全链接</a></p>"#;
        let markdown = normalize_feed_html(html).expect("normalization should succeed");
        assert!(markdown.contains("Привет, мир — 你好。"));
        assert!(markdown.contains("[安全链接](https://example.com/article)"));
    }

    #[test]
    fn article_extraction_drops_navigation_chrome() {
        let article_body = (0..240)
            .map(|index| format!("article-word-{index}"))
            .collect::<Vec<_>>()
            .join(" ");
        let html = format!(
            "<html><head><title>Story</title></head><body><nav>{}</nav><aside>{}</aside><article><h1>Story</h1><p>{article_body}</p></article></body></html>",
            "navigation-link ".repeat(200),
            "sidebar-link ".repeat(200),
        );
        let markdown = extract_article_markdown(&html, "https://example.com/story")
            .expect("readability should extract the article");
        assert!(markdown.contains("article-word-0"));
        assert!(!markdown.contains("navigation-link"));
        assert!(!markdown.contains("sidebar-link"));
    }

    #[test]
    fn article_url_policy_blocks_special_and_embedded_addresses() {
        for raw in [
            "http://10.0.0.1/article",
            "http://100.64.0.1/article",
            "http://169.254.1.1/article",
            "http://192.168.1.1/article",
            "http://224.0.0.1/article",
            "http://[::]/article",
            "http://[fc00::1]/article",
            "http://[fe80::1]/article",
            "http://[::ffff:192.168.1.1]/article",
            "http://[64:ff9b::c0a8:0101]/article",
            "http://[2002:c0a8:0101::]/article",
        ] {
            assert!(
                validate_article_url(raw).is_err(),
                "special address was accepted: {raw}"
            );
        }
        assert!(validate_article_url("http://[2001:4860:4860::8888]/article").is_ok());
    }

    #[test]
    fn mixed_dns_answers_fail_closed_even_when_one_address_is_public() {
        let addresses = vec![
            "93.184.216.34:0".parse().unwrap(),
            "192.168.1.10:0".parse().unwrap(),
        ];
        assert!(matches!(
            validate_resolved_addresses(addresses),
            Err(ContentError::BlockedAddress)
        ));
    }

    #[test]
    fn quality_policy_rejects_summary_shells_and_challenges() {
        let summary = "A short summary.";
        assert_eq!(
            quality_check(summary, summary, false),
            Err(ContentError::NonSubstantive)
        );

        let body = (0..210)
            .map(|index| format!("word{index}"))
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(
            quality_check(&format!("{body} verify you are human"), "", true),
            Err(ContentError::ChallengePage)
        );
        assert_eq!(
            quality_check(&format!("{body} watch the video"), "", true),
            Err(ContentError::VideoRequired)
        );
        assert!(!ContentError::ChallengePage.is_retryable());
        assert!(!ContentError::VideoRequired.is_retryable());
        assert!(!ContentError::NonSubstantive.is_retryable());
        assert!(ContentError::Timeout.is_retryable());
        assert!(ContentError::HttpStatus { retryable: true }.is_retryable());
    }

    #[test]
    fn redirect_policy_rejects_https_downgrade_and_special_targets() {
        let https = Url::parse("https://example.com/article").unwrap();
        assert_eq!(
            redirect_target(&https, "http://example.com/final"),
            Err(ContentError::BlockedRedirect)
        );
        assert_eq!(
            redirect_target(&https, "https://127.0.0.1/admin"),
            Err(ContentError::BlockedAddress)
        );
        assert_eq!(
            redirect_target(&https, "/final").unwrap(),
            Url::parse("https://example.com/final").unwrap()
        );
    }

    #[tokio::test]
    async fn guarded_send_does_not_follow_redirects_and_sends_identity_encoding() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redirect"))
            .and(header("accept-encoding", "identity"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("location", "/final")
                    .insert_header("content-type", "text/html"),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/final"))
            .respond_with(ResponseTemplate::new(200).set_body_string("should not be fetched"))
            .expect(0)
            .mount(&server)
            .await;

        let port = server.address().port();
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .resolve("example.com", SocketAddr::new(server.address().ip(), port))
            .build()
            .expect("test client should build");
        let url = Url::parse(&format!("http://example.com:{port}/redirect"))
            .expect("test URL should parse");
        let response = send_once(&client, &url)
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        assert_eq!(
            response.headers().get(reqwest::header::LOCATION),
            Some(&reqwest::header::HeaderValue::from_static("/final"))
        );
    }

    #[tokio::test]
    async fn guarded_response_reader_enforces_html_and_byte_limits() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/text"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/plain")
                    .set_body_string("not html"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/large"))
            .respond_with(ResponseTemplate::new(200).set_body_raw("0123456789", "text/html"))
            .mount(&server)
            .await;

        let port = server.address().port();
        let client = reqwest::Client::builder()
            .no_proxy()
            .resolve("example.com", SocketAddr::new(server.address().ip(), port))
            .build()
            .expect("test client should build");
        let config = FetchConfig {
            max_response_bytes: 8,
            ..FetchConfig::default()
        };
        let text_url =
            Url::parse(&format!("http://example.com:{port}/text")).expect("test URL should parse");
        let text_response = send_once(&client, &text_url)
            .await
            .expect("request should succeed");
        assert!(matches!(
            read_page_response(
                text_response,
                text_url,
                config,
                FetchDeadline::new(Duration::from_secs(5)),
            )
            .await,
            Err(ContentError::NotHtml)
        ));

        let large_url =
            Url::parse(&format!("http://example.com:{port}/large")).expect("test URL should parse");
        let large_response = send_once(&client, &large_url)
            .await
            .expect("request should succeed");
        let result = read_page_response(
            large_response,
            large_url,
            config,
            FetchDeadline::new(Duration::from_secs(5)),
        )
        .await;
        assert!(matches!(result, Err(ContentError::TooLarge)));
    }

    #[tokio::test]
    async fn summary_only_entries_are_not_marked_as_full_content() {
        let now = Utc::now();
        let entry = utopia_store::rss_full_content::Entry {
            id: Uuid::nil(),
            source_id: Uuid::nil(),
            activation_generation: 1,
            external_key: "summary-only".into(),
            title: "Summary only".into(),
            article_url: None,
            summary: "A short but useful RSS summary.".into(),
            embedded_html: None,
            doc_time: None,
            state: "hydrating".into(),
            hydration_job_id: None,
            attempt_count: 1,
            document_id: None,
            error_code: None,
            error_detail: None,
            first_seen_at: now,
            updated_at: now,
            completed_at: None,
        };
        assert!(matches!(
            hydrate_content(&entry).await,
            Err(ContentError::NoUsableSource)
        ));
    }

    #[test]
    fn generic_login_shells_fail_the_quality_gate() {
        let body = (0..210)
            .map(|index| format!("shell{index}"))
            .collect::<Vec<_>>()
            .join(" ");
        let shell = format!("# Login\nEmail\nPassword\n{body}");
        assert!(matches!(
            quality_check(&shell, "", true),
            Err(ContentError::ChallengePage)
        ));
    }

    #[test]
    fn private_nat64_prefix_is_blocked() {
        assert!(matches!(
            validate_article_url("http://[64:ff9b:1::1]/article"),
            Err(ContentError::BlockedAddress)
        ));
    }

    #[test]
    fn linked_markdown_must_pass_the_linked_page_quality_gate() {
        assert!(matches!(
            accept_linked_markdown("short shell".into(), "", None),
            Err(ContentError::NonSubstantive)
        ));
    }

    #[test]
    fn quality_policy_allows_an_article_that_discusses_paywalls_and_captchas() {
        let body = (0..210)
            .map(|index| format!("article{index}"))
            .collect::<Vec<_>>()
            .join(" ");
        let body = format!(
            "{body} This analysis compares paywall economics and captcha usability in publishing."
        );
        assert_eq!(quality_check(&body, "", true), Ok(()));
    }
}
