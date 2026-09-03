use crate::{ChatMessage, LlmClient, OutOfCredit, RateLimited};

use crate::codex::CodexResponsesClient;

/// Non-secret identity used for background concurrency, provenance, and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackgroundModelIdentity {
    pub provider: &'static str,
    pub endpoint_key: String,
    pub model: String,
}

impl BackgroundModelIdentity {
    /// The value persisted in the existing adjudication model column.
    /// Chat Completions keeps its historical raw model value; Codex is qualified so
    /// new subscription provenance remains visible even though cache keys stay compatible.
    pub fn provenance_label(&self) -> String {
        if self.provider == "codex_responses" {
            format!("codex_responses:{}", self.model)
        } else {
            self.model.clone()
        }
    }

    /// The cache namespace used by adjudication. Both providers deliberately use the historical
    /// provider-agnostic key so a valid verdict can be reused after a provider switch.
    pub fn cache_namespace(&self) -> String {
        String::new()
    }
}

/// Provider-neutral contract for the two background text workloads.
///
/// This intentionally exposes only complete plain text. Interactive tools, streamed
/// assistant turns, and provider-managed conversation state remain outside this seam.
#[derive(Clone)]
pub enum BackgroundTextClient {
    ChatCompletions(LlmClient),
    CodexResponses(CodexResponsesClient),
}

impl BackgroundTextClient {
    pub async fn complete(&self, messages: &[ChatMessage]) -> Result<String, BackgroundTextError> {
        match self {
            Self::ChatCompletions(client) => client.chat(messages).await.map_err(map_chat_error),
            Self::CodexResponses(client) => client.complete(messages).await,
        }
    }

    pub fn identity(&self) -> BackgroundModelIdentity {
        match self {
            Self::ChatCompletions(client) => BackgroundModelIdentity {
                provider: "chat_completions",
                endpoint_key: "chat_completions".into(),
                model: client.model.clone(),
            },
            Self::CodexResponses(client) => client.identity(),
        }
    }

    pub fn is_codex_responses(&self) -> bool {
        matches!(self, Self::CodexResponses(_))
    }
}

/// Stable provider-neutral error boundary. Secret-bearing upstream bodies and headers
/// are intentionally not retained in this type's display text.
#[derive(Debug, thiserror::Error)]
pub enum BackgroundTextError {
    #[error("background input is invalid: {0}")]
    InvalidInput(String),
    #[error("background authentication failed ({kind}): {detail}")]
    Authentication {
        kind: BackgroundAuthFailure,
        detail: &'static str,
    },
    #[error(transparent)]
    RateLimited(#[from] RateLimited),
    #[error(transparent)]
    OutOfCredit(#[from] OutOfCredit),
    #[error("background request timed out")]
    Timeout,
    #[error("background request was cancelled")]
    Cancelled,
    #[error("background protocol failed: {0}")]
    Protocol(&'static str),
    #[error("background upstream request failed: {0}")]
    Upstream(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundAuthFailure {
    Permanent,
    Transient,
}

impl std::fmt::Display for BackgroundAuthFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Permanent => "permanent",
            Self::Transient => "transient",
        })
    }
}

impl BackgroundTextError {
    pub fn is_permanent_authentication(&self) -> bool {
        matches!(
            self,
            Self::Authentication {
                kind: BackgroundAuthFailure::Permanent,
                ..
            }
        )
    }

    pub(crate) fn rate_limited(&self) -> Option<&RateLimited> {
        match self {
            Self::RateLimited(error) => Some(error),
            _ => None,
        }
    }

    pub(crate) fn out_of_credit(&self) -> Option<&OutOfCredit> {
        match self {
            Self::OutOfCredit(error) => Some(error),
            _ => None,
        }
    }
}

fn map_chat_error(error: anyhow::Error) -> BackgroundTextError {
    if let Some(rate) = crate::rate_limited(&error) {
        return BackgroundTextError::RateLimited(RateLimited {
            status: rate.status,
            retry_after: rate.retry_after,
            detail: "chat completions rate limit".into(),
        });
    }
    if let Some(credit) = crate::out_of_credit(&error) {
        return BackgroundTextError::OutOfCredit(OutOfCredit {
            status: credit.status,
            detail: "chat completions quota exhausted".into(),
        });
    }
    if crate::is_unreachable(&error) {
        return BackgroundTextError::Upstream("chat completions transport failure");
    }
    BackgroundTextError::Upstream("chat completions request failure")
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn chat_completions_background_contract_delegates_without_widening_surface() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(body_json(serde_json::json!({
                "model": "test-model",
                "messages": [
                    { "role": "system", "content": "system" },
                    { "role": "user", "content": "user" }
                ],
                "stream": false
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "choices": [{ "message": { "content": "reply" } }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = BackgroundTextClient::ChatCompletions(LlmClient::new(
            &server.uri(),
            None,
            "test-model",
        ));
        let messages = [
            ChatMessage {
                role: "system".into(),
                content: "system".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "user".into(),
            },
        ];

        assert_eq!(client.identity().provider, "chat_completions");
        assert_eq!(client.identity().model, "test-model");
        assert_eq!(client.complete(&messages).await.unwrap(), "reply");
    }

    #[test]
    fn cache_namespace_is_provider_agnostic_but_provenance_is_qualified() {
        let chat = BackgroundModelIdentity {
            provider: "chat_completions",
            endpoint_key: "chat_completions".into(),
            model: "model".into(),
        };
        let codex = BackgroundModelIdentity {
            provider: "codex_responses",
            endpoint_key: "codex".into(),
            model: "model".into(),
        };
        assert_eq!(chat.cache_namespace(), "");
        assert_eq!(codex.cache_namespace(), "");
        assert_eq!(chat.provenance_label(), "model");
        assert_eq!(codex.provenance_label(), "codex_responses:model");
    }
}
