use super::auth::{CodexAuthError, CodexAuthManager, CredentialSnapshot};
use crate::background::{BackgroundAuthFailure, BackgroundModelIdentity, BackgroundTextError};
use crate::ChatMessage;
use futures_util::StreamExt;
use reqwest::header::{HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

const CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
const RESPONSES_PATH: &str = "/responses";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_SSE_FRAME: usize = 1024 * 1024;
const MAX_OUTPUT: usize = 8 * 1024 * 1024;
const MAX_HTTP_BODY: usize = 1024 * 1024;

/// Minimal, non-interactive Responses client for background extraction and adjudication.
#[derive(Clone)]
pub struct CodexResponsesClient {
    auth: Arc<CodexAuthManager>,
    http: reqwest::Client,
    base_url: String,
    model: String,
}

impl std::fmt::Debug for CodexResponsesClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodexResponsesClient")
            .field("provider", &"codex_responses")
            .field("base_url", &"fixed")
            .field("model", &self.model)
            .finish()
    }
}

impl CodexResponsesClient {
    pub fn new(
        auth: Arc<CodexAuthManager>,
        model: impl Into<String>,
    ) -> Result<Self, BackgroundTextError> {
        Self::build(auth, model.into(), CODEX_BASE_URL.to_string())
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn with_base_url(
        auth: Arc<CodexAuthManager>,
        model: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Result<Self, BackgroundTextError> {
        Self::build(auth, model.into(), base_url.into())
    }

    fn build(
        auth: Arc<CodexAuthManager>,
        model: String,
        base_url: String,
    ) -> Result<Self, BackgroundTextError> {
        if model.trim().is_empty() {
            return Err(BackgroundTextError::InvalidInput(
                "Codex model is empty".into(),
            ));
        }
        if !base_url.starts_with("https://") && !cfg!(any(test, feature = "test-util")) {
            return Err(BackgroundTextError::InvalidInput(
                "Codex base URL must use HTTPS".into(),
            ));
        }
        let http = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .read_timeout(READ_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| {
                BackgroundTextError::Upstream("Responses HTTP client cannot be created")
            })?;
        Ok(Self {
            auth,
            http,
            base_url: base_url.trim_end_matches('/').to_string(),
            model,
        })
    }

    pub fn identity(&self) -> BackgroundModelIdentity {
        BackgroundModelIdentity {
            provider: "codex_responses",
            endpoint_key: self.base_url.clone(),
            model: self.model.clone(),
        }
    }

    pub async fn complete(&self, messages: &[ChatMessage]) -> Result<String, BackgroundTextError> {
        let body = build_request(&self.model, messages)?;
        let snapshot = self.auth.snapshot().await.map_err(map_auth_error)?;
        match self.send_once(&body, &snapshot).await {
            Ok(text) => Ok(text),
            Err(AttemptError::Unauthorized) => {
                let refreshed = self
                    .auth
                    .snapshot_after_unauthorized(&snapshot.access_token)
                    .await
                    .map_err(map_auth_error)?;
                match self.send_once(&body, &refreshed).await {
                    Ok(text) => Ok(text),
                    Err(AttemptError::Unauthorized) => Err(BackgroundTextError::Authentication {
                        kind: BackgroundAuthFailure::Permanent,
                        detail: "provider rejected refreshed credentials",
                    }),
                    Err(AttemptError::Response(error)) => Err(error),
                }
            }
            Err(AttemptError::Response(error)) => Err(error),
        }
    }

    async fn send_once(
        &self,
        body: &Value,
        snapshot: &CredentialSnapshot,
    ) -> Result<String, AttemptError> {
        let authorization = HeaderValue::from_str(&format!("Bearer {}", snapshot.access_token))
            .map_err(|_| {
                AttemptError::Response(BackgroundTextError::InvalidInput(
                    "credential token cannot be placed in an HTTP header".into(),
                ))
            })?;
        let account_id = HeaderValue::from_str(&snapshot.account_id).map_err(|_| {
            AttemptError::Response(BackgroundTextError::InvalidInput(
                "credential account identifier cannot be placed in an HTTP header".into(),
            ))
        })?;
        let response = self
            .http
            .post(format!("{}{}", self.base_url, RESPONSES_PATH))
            .header(AUTHORIZATION, authorization)
            .header("ChatGPT-Account-ID", account_id)
            .header(ACCEPT, "text/event-stream")
            .header(CONTENT_TYPE, "application/json")
            .json(body)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    AttemptError::Response(BackgroundTextError::Timeout)
                } else {
                    AttemptError::Response(BackgroundTextError::Upstream(
                        "Responses transport failure",
                    ))
                }
            })?;
        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(AttemptError::Unauthorized);
        }
        if status == reqwest::StatusCode::FORBIDDEN {
            return Err(AttemptError::Response(
                BackgroundTextError::Authentication {
                    kind: BackgroundAuthFailure::Permanent,
                    detail: "provider denied subscription access",
                },
            ));
        }
        if status.is_redirection() {
            return Err(AttemptError::Response(BackgroundTextError::Protocol(
                "Responses redirect refused",
            )));
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status == reqwest::StatusCode::PAYMENT_REQUIRED
        {
            let headers = response.headers().clone();
            let body = read_bounded_body(response)
                .await
                .map_err(AttemptError::Response)?;
            return Err(AttemptError::Response(http_failure(
                status,
                retry_after_header(&headers),
                &body,
            )));
        }
        if !status.is_success() {
            let body = read_bounded_body(response)
                .await
                .map_err(AttemptError::Response)?;
            let _ = body;
            return Err(AttemptError::Response(BackgroundTextError::Upstream(
                "Responses provider returned an HTTP failure",
            )));
        }
        parse_sse(response).await.map_err(AttemptError::Response)
    }
}

enum AttemptError {
    Unauthorized,
    Response(BackgroundTextError),
}

fn build_request(model: &str, messages: &[ChatMessage]) -> Result<Value, BackgroundTextError> {
    if messages.len() != 2 {
        return Err(BackgroundTextError::InvalidInput(
            "Responses background input requires one system and one user message".into(),
        ));
    }
    let system = &messages[0];
    let user = &messages[1];
    if system.role != "system" || user.role != "user" {
        return Err(BackgroundTextError::InvalidInput(
            "Responses background input supports only system then user messages".into(),
        ));
    }
    if system.content.trim().is_empty() || user.content.trim().is_empty() {
        return Err(BackgroundTextError::InvalidInput(
            "Responses background messages cannot be empty".into(),
        ));
    }
    Ok(json!({
        "model": model,
        "instructions": system.content,
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": user.content}]
        }],
        "tool_choice": "auto",
        "parallel_tool_calls": false,
        "reasoning": null,
        "store": false,
        "stream": true,
        "include": []
    }))
}

fn map_auth_error(error: CodexAuthError) -> BackgroundTextError {
    let kind = if error.is_permanent() {
        BackgroundAuthFailure::Permanent
    } else {
        BackgroundAuthFailure::Transient
    };
    BackgroundTextError::Authentication {
        kind,
        detail: match kind {
            BackgroundAuthFailure::Permanent => "dedicated credentials are unavailable",
            BackgroundAuthFailure::Transient => "credential refresh is temporarily unavailable",
        },
    }
}

async fn read_bounded_body(response: reqwest::Response) -> Result<Vec<u8>, BackgroundTextError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_HTTP_BODY as u64)
    {
        return Err(BackgroundTextError::Protocol(
            "Responses HTTP error body exceeded its bound",
        ));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            if error.is_timeout() {
                BackgroundTextError::Timeout
            } else {
                BackgroundTextError::Upstream("Responses HTTP error body could not be read")
            }
        })?;
        if body.len().saturating_add(chunk.len()) > MAX_HTTP_BODY {
            return Err(BackgroundTextError::Protocol(
                "Responses HTTP error body exceeded its bound",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

async fn parse_sse(response: reqwest::Response) -> Result<String, BackgroundTextError> {
    let mut stream = response.bytes_stream();
    let mut parser = SseParser::default();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            if error.is_timeout() {
                BackgroundTextError::Timeout
            } else {
                BackgroundTextError::Upstream("Responses stream read failed")
            }
        })?;
        parser.push(&chunk)?;
        while let Some(frame) = parser.take_frame() {
            parser.apply_frame(&frame)?;
        }
        if parser.buffer.len() > MAX_SSE_FRAME && find_frame_end(&parser.buffer).is_none() {
            return Err(BackgroundTextError::Protocol(
                "Responses SSE frame exceeded its bound",
            ));
        }
    }
    parser.finish()
}

#[derive(Default)]
struct SseParser {
    buffer: Vec<u8>,
    output: String,
    terminal: bool,
    unknown_events: usize,
}

impl SseParser {
    fn push(&mut self, chunk: &[u8]) -> Result<(), BackgroundTextError> {
        self.buffer.extend_from_slice(chunk);
        if self.buffer.len() > MAX_SSE_FRAME && find_frame_end(&self.buffer).is_none() {
            return Err(BackgroundTextError::Protocol(
                "Responses SSE frame exceeded its bound",
            ));
        }
        Ok(())
    }

    fn take_frame(&mut self) -> Option<Vec<u8>> {
        let (index, delimiter_len) = find_frame_end(&self.buffer)?;
        Some(self.buffer.drain(..index + delimiter_len).collect())
    }

    fn apply_frame(&mut self, frame: &[u8]) -> Result<(), BackgroundTextError> {
        if frame.len() > MAX_SSE_FRAME {
            return Err(BackgroundTextError::Protocol(
                "Responses SSE frame exceeded its bound",
            ));
        }
        let (event_name, data) = parse_sse_frame(frame)?;
        let Some(data) = data else {
            return Ok(());
        };
        if data.trim() == "[DONE]" {
            return Ok(());
        }
        let value: Value = serde_json::from_str(&data).map_err(|_| {
            BackgroundTextError::Protocol(if is_recognized_event(event_name.as_deref()) {
                "recognized Responses event is malformed"
            } else {
                "Responses SSE data is malformed"
            })
        })?;
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .or(event_name.as_deref());
        let Some(kind) = kind else {
            self.unknown_events = self.unknown_events.saturating_add(1);
            return Ok(());
        };

        if self.terminal {
            if is_recognized_event(Some(kind)) {
                return Err(BackgroundTextError::Protocol(
                    "Responses stream emitted an event after completion",
                ));
            }
            self.unknown_events = self.unknown_events.saturating_add(1);
            return Ok(());
        }

        match kind {
            "response.created" => {
                if !value.is_object() {
                    return Err(BackgroundTextError::Protocol(
                        "Responses created event is malformed",
                    ));
                }
            }
            "response.output_text.delta" => {
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .filter(|delta| !delta.is_empty())
                    .ok_or(BackgroundTextError::Protocol(
                        "Responses text delta is missing",
                    ))?;
                if self.output.len().saturating_add(delta.len()) > MAX_OUTPUT {
                    return Err(BackgroundTextError::Protocol(
                        "Responses reconstructed output exceeded its bound",
                    ));
                }
                self.output.push_str(delta);
            }
            "response.completed" => {
                if value.get("response").and_then(Value::as_object).is_none() {
                    return Err(BackgroundTextError::Protocol(
                        "Responses completion event is missing its response",
                    ));
                }
                if self.output.trim().is_empty() {
                    return Err(BackgroundTextError::Protocol(
                        "Responses completed without usable text",
                    ));
                }
                self.terminal = true;
            }
            "response.failed" => return Err(response_failure(&value, false)),
            "response.incomplete" => {
                if value.get("response").and_then(Value::as_object).is_none() {
                    return Err(BackgroundTextError::Protocol(
                        "Responses incomplete event is malformed",
                    ));
                }
                return Err(BackgroundTextError::Protocol(
                    "Responses response was incomplete",
                ));
            }
            "error" => return Err(response_failure(&value, true)),
            _ => {
                self.unknown_events = self.unknown_events.saturating_add(1);
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<String, BackgroundTextError> {
        if !self.buffer.is_empty() {
            return Err(BackgroundTextError::Protocol(
                "Responses stream ended with an incomplete SSE frame",
            ));
        }
        if !self.terminal {
            return Err(BackgroundTextError::Protocol(
                "Responses stream ended before completion",
            ));
        }
        tracing::debug!(
            provider = "codex_responses",
            unknown_event_count = self.unknown_events,
            status = "completed",
            "Responses background stream completed"
        );
        Ok(self.output)
    }
}

fn parse_sse_frame(frame: &[u8]) -> Result<(Option<String>, Option<String>), BackgroundTextError> {
    let text = std::str::from_utf8(frame)
        .map_err(|_| BackgroundTextError::Protocol("Responses SSE frame is not UTF-8"))?;
    let mut event_name = None;
    let mut data_lines = Vec::new();
    for raw_line in text.lines() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        if let Some(value) = line.strip_prefix("event:") {
            event_name = Some(value.trim_start().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.strip_prefix(' ').unwrap_or(value).to_string());
        }
    }
    if data_lines.is_empty() {
        return Ok((event_name, None));
    }
    Ok((event_name, Some(data_lines.join("\n"))))
}

fn find_frame_end(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if crlf < lf => Some((crlf, 4)),
        (Some(lf), _) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }
}

fn is_recognized_event(kind: Option<&str>) -> bool {
    matches!(
        kind,
        Some(
            "response.created"
                | "response.output_text.delta"
                | "response.completed"
                | "response.failed"
                | "response.incomplete"
                | "error"
        )
    )
}

fn response_failure(value: &Value, top_level: bool) -> BackgroundTextError {
    let error = if top_level {
        value.get("error")
    } else {
        value
            .get("response")
            .and_then(|response| response.get("error"))
    };
    let code = error
        .and_then(|error| {
            error
                .get("code")
                .and_then(Value::as_str)
                .or_else(|| error.get("type").and_then(Value::as_str))
        })
        .unwrap_or_default();
    let retry_after = error.and_then(retry_after_json);
    match code {
        "rate_limit_exceeded" => BackgroundTextError::RateLimited(crate::RateLimited {
            status: 429,
            retry_after,
            detail: "Responses rate limit".into(),
        }),
        "insufficient_quota" | "quota_exceeded" => {
            BackgroundTextError::OutOfCredit(crate::OutOfCredit {
                status: 429,
                detail: "Responses quota exhausted".into(),
            })
        }
        "unauthorized" | "invalid_api_key" | "authentication_error" => {
            BackgroundTextError::Authentication {
                kind: BackgroundAuthFailure::Permanent,
                detail: "Responses provider rejected authentication",
            }
        }
        _ => BackgroundTextError::Upstream("Responses provider reported a failure"),
    }
}

fn http_failure(
    status: reqwest::StatusCode,
    retry_after: Option<Duration>,
    body: &[u8],
) -> BackgroundTextError {
    let value: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    if status == reqwest::StatusCode::PAYMENT_REQUIRED
        || response_code(&value) == Some("insufficient_quota")
        || response_code(&value) == Some("quota_exceeded")
    {
        return BackgroundTextError::OutOfCredit(crate::OutOfCredit {
            status: status.as_u16(),
            detail: "Responses quota exhausted".into(),
        });
    }
    BackgroundTextError::RateLimited(crate::RateLimited {
        status: status.as_u16(),
        retry_after: retry_after.or_else(|| value.get("error").and_then(retry_after_json)),
        detail: "Responses rate limit".into(),
    })
}

fn response_code(value: &Value) -> Option<&str> {
    value
        .get("error")
        .and_then(|error| error.get("code").and_then(Value::as_str))
}

fn retry_after_header(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

fn retry_after_json(value: &Value) -> Option<Duration> {
    value
        .get("retry_after")
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .map(|seconds| Duration::from_secs(seconds.min(3600)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn jwt(exp: u64, account_id: &str) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
        let claims = json!({
            "exp": exp,
            "https://api.openai.com/auth": {"chatgpt_account_id": account_id}
        });
        format!(
            "{}.{}.sig",
            header,
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        )
    }

    fn client_fixture(refresh_url: Option<String>) -> (tempfile::TempDir, Arc<CodexAuthManager>) {
        let dir = tempfile::tempdir().unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let token = jwt(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600,
            "account-test",
        );
        let value = json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": token,
                "access_token": "access-fixture",
                "refresh_token": "refresh-fixture",
                "account_id": "account-test"
            },
            "last_refresh": "2099-01-01T00:00:00Z"
        });
        let path = dir.path().join("auth.json");
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let manager = match refresh_url {
            Some(url) => CodexAuthManager::with_refresh_url(path.parent().unwrap(), url).unwrap(),
            None => CodexAuthManager::new(path.parent().unwrap()).unwrap(),
        };
        (dir, Arc::new(manager))
    }

    fn messages() -> [ChatMessage; 2] {
        [
            ChatMessage {
                role: "system".into(),
                content: "system bytes".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "user bytes".into(),
            },
        ]
    }

    #[test]
    fn request_rejects_unsupported_roles_before_network() {
        let bad = [ChatMessage {
            role: "assistant".into(),
            content: "not supported".into(),
        }];
        assert!(matches!(
            build_request("model", &bad),
            Err(BackgroundTextError::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn request_and_stream_contract_are_exact_and_fail_closed() {
        let server = MockServer::start().await;
        let expected = json!({
            "model": "codex-model",
            "instructions": "system bytes",
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "user bytes"}]
            }],
            "tool_choice": "auto",
            "parallel_tool_calls": false,
            "reasoning": null,
            "store": false,
            "stream": true,
            "include": []
        });
        let stream = concat_sse(&[
            r#"event: response.created\ndata: {"type":"response.created"}\n\n"#,
            r#"event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"hello "}\n\n"#,
            r#"event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"world"}\n\n"#,
            r#"event: response.completed\ndata: {"type":"response.completed","response":{"id":"response-fixture"}}\n\n"#,
        ]);
        Mock::given(method("POST"))
            .and(path("/responses"))
            .and(header("accept", "text/event-stream"))
            .and(header("chatgpt-account-id", "account-test"))
            .and(body_json(expected))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_raw(stream, "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let (_dir, auth) = client_fixture(None);
        let client =
            CodexResponsesClient::with_base_url(auth, "codex-model", server.uri()).unwrap();
        assert_eq!(client.complete(&messages()).await.unwrap(), "hello world");
    }

    #[tokio::test]
    async fn malformed_or_premature_streams_do_not_succeed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n",
                "text/event-stream",
            ))
            .mount(&server)
            .await;
        let (_dir, auth) = client_fixture(None);
        let client =
            CodexResponsesClient::with_base_url(auth, "codex-model", server.uri()).unwrap();
        assert!(matches!(
            client.complete(&messages()).await,
            Err(BackgroundTextError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn rate_limit_failed_event_uses_existing_marker() {
        let server = MockServer::start().await;
        let body = "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"rate_limit_exceeded\",\"retry_after\":2}}}\n\n";
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
            .mount(&server)
            .await;
        let (_dir, auth) = client_fixture(None);
        let client =
            CodexResponsesClient::with_base_url(auth, "codex-model", server.uri()).unwrap();
        let error = client.complete(&messages()).await.unwrap_err();
        assert!(matches!(&error, BackgroundTextError::RateLimited(_)));
        let wrapped = anyhow::Error::new(error);
        assert!(crate::rate_limited(&wrapped).is_some());
    }

    #[tokio::test]
    async fn http_rate_limit_uses_existing_marker_and_retry_after() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("retry-after", "7")
                    .set_body_json(json!({"error": {"code": "rate_limit_exceeded"}})),
            )
            .mount(&server)
            .await;
        let (_dir, auth) = client_fixture(None);
        let client =
            CodexResponsesClient::with_base_url(auth, "codex-model", server.uri()).unwrap();
        match client.complete(&messages()).await.unwrap_err() {
            BackgroundTextError::RateLimited(rate) => {
                assert_eq!(rate.status, 429);
                assert_eq!(rate.retry_after, Some(Duration::from_secs(7)));
            }
            other => panic!("expected rate limit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unauthorized_is_replayed_once_after_refresh() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "error": "invalid_grant"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let (_dir, auth) = client_fixture(Some(format!("{}/oauth/token", server.uri())));
        let client =
            CodexResponsesClient::with_base_url(auth, "codex-model", server.uri()).unwrap();
        let error = client.complete(&messages()).await.unwrap_err();
        assert!(matches!(
            error,
            BackgroundTextError::Authentication {
                kind: BackgroundAuthFailure::Permanent,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn oversized_complete_frame_is_rejected() {
        let server = MockServer::start().await;
        let delta = "x".repeat(MAX_SSE_FRAME);
        let body = format!(
            "event: response.output_text.delta\\ndata: {}\\n\\nevent: response.completed\\ndata: {{\"type\":\"response.completed\",\"response\":{{}}}}\\n\\n",
            serde_json::to_string(&json!({
                "type": "response.output_text.delta",
                "delta": delta,
            }))
            .unwrap()
        );
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
            .mount(&server)
            .await;
        let (_dir, auth) = client_fixture(None);
        let client =
            CodexResponsesClient::with_base_url(auth, "codex-model", server.uri()).unwrap();
        assert!(matches!(
            client.complete(&messages()).await,
            Err(BackgroundTextError::Protocol(
                "Responses SSE frame exceeded its bound"
            ))
        ));
    }

    #[tokio::test]
    async fn oversized_incomplete_frame_after_valid_frame_is_rejected() {
        let server = MockServer::start().await;
        let body = format!(
            "event: response.created\\ndata: {{\"type\":\"response.created\"}}\\n\\nevent: response.output_text.delta\\ndata: {{\"type\":\"response.output_text.delta\",\"delta\":\"{}\"",
            "x".repeat(MAX_SSE_FRAME)
        )
        .replace("\\n", "\n");
        Mock::given(method("POST"))
            .and(path("/responses"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(body, "text/event-stream"))
            .mount(&server)
            .await;
        let (_dir, auth) = client_fixture(None);
        let client =
            CodexResponsesClient::with_base_url(auth, "codex-model", server.uri()).unwrap();
        assert!(matches!(
            client.complete(&messages()).await,
            Err(BackgroundTextError::Protocol(
                "Responses SSE frame exceeded its bound"
            ))
        ));
    }

    fn concat_sse(parts: &[&str]) -> String {
        parts.concat().replace("\\n", "\n")
    }
}
