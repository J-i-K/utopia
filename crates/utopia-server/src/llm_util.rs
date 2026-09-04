//! LLM clients and concurrency gates for interactive and background work.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use utopia_core::config::{AppConfig, BackgroundLlmProvider};
use utopia_core::models::{ChatAccessMode, LlmSettings};
use utopia_llm::{BackgroundTextClient, ChatMessage, LlmClient};

use crate::state::AppState;

pub fn chat_client(s: &LlmSettings) -> Option<LlmClient> {
    if !s.chat_ready() {
        return None;
    }
    Some(LlmClient::new(
        s.chat_base_url.as_deref()?,
        s.chat_api_key.as_deref(),
        s.chat_model.as_deref()?,
    ))
}

pub fn embed_client(s: &LlmSettings) -> Option<LlmClient> {
    if !s.embed_ready() {
        return None;
    }
    Some(LlmClient::new(
        s.embed_base_url.as_deref()?,
        s.embed_api_key.as_deref(),
        s.embed_model.as_deref()?,
    ))
}

/// Deployment-scoped background capability. Interactive chat never reads this object.
#[derive(Clone)]
pub struct BackgroundRuntime {
    codex_client: Option<Arc<utopia_llm::CodexResponsesClient>>,
    codex_gate: Option<Arc<Semaphore>>,
}

impl BackgroundRuntime {
    pub fn from_config(cfg: &AppConfig) -> anyhow::Result<Self> {
        cfg.validate()?;
        match cfg.background_llm_provider {
            BackgroundLlmProvider::ChatCompletions => Ok(Self {
                codex_client: None,
                codex_gate: None,
            }),
            BackgroundLlmProvider::CodexResponses => {
                let home = cfg
                    .codex_home
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("Codex credential home is missing"))?;
                let model = cfg
                    .codex_model
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("Codex model is missing"))?;
                let auth = Arc::new(utopia_llm::CodexAuthManager::new(home)?);
                let client = Arc::new(utopia_llm::CodexResponsesClient::new(auth, model)?);
                Ok(Self {
                    codex_client: Some(client),
                    codex_gate: Some(Arc::new(Semaphore::new(cfg.codex_max_concurrency as usize))),
                })
            }
        }
    }

    pub fn subscription_available(&self) -> bool {
        self.codex_client.is_some()
    }

    pub fn ready(&self, settings: Option<&LlmSettings>) -> bool {
        self.client(settings).is_some()
    }

    pub fn client(&self, settings: Option<&LlmSettings>) -> Option<BackgroundTextClient> {
        let settings = settings?;
        match settings.chat_access_mode {
            ChatAccessMode::Api => chat_client(settings).map(BackgroundTextClient::ChatCompletions),
            ChatAccessMode::Subscription => {
                let model = settings.chat_model.as_deref()?.trim();
                let client = self.codex_client.as_ref()?.for_model(model).ok()?;
                Some(BackgroundTextClient::CodexResponses(client))
            }
        }
    }

    async fn acquire(
        &self,
        state: &AppState,
        settings: Option<&LlmSettings>,
        client: &BackgroundTextClient,
    ) -> Option<OwnedSemaphorePermit> {
        if client.is_codex_responses() {
            return self.codex_gate.as_ref()?.clone().acquire_owned().await.ok();
        }
        match settings {
            Some(settings) => acquire_chat(state, settings).await,
            None => None,
        }
    }
}

/// Background text completion with the same bounded rate-limit retry policy for both providers.
pub async fn complete_with_rate_limit_retry(
    state: &AppState,
    settings: Option<&LlmSettings>,
    runtime: &BackgroundRuntime,
    client: &BackgroundTextClient,
    messages: &[ChatMessage],
) -> anyhow::Result<String> {
    const RATE_LIMIT_TRIES: u32 = 5;
    const RATE_LIMIT_CAP: Duration = Duration::from_secs(60);
    let mut backoff = Duration::from_secs(2);
    for attempt in 1..=RATE_LIMIT_TRIES {
        let outcome = {
            let _permit = runtime.acquire(state, settings, client).await;
            client.complete(messages).await.map_err(anyhow::Error::new)
        };
        let err = match outcome {
            Ok(reply) => return Ok(reply),
            Err(error) => error,
        };
        let Some(hit) = utopia_llm::rate_limited(&err) else {
            return Err(err);
        };
        if attempt == RATE_LIMIT_TRIES {
            return Err(err.context(format!("限流退避 {RATE_LIMIT_TRIES} 次仍未通过")));
        }
        let delay = jitter(hit.retry_after.unwrap_or(backoff).min(RATE_LIMIT_CAP));
        tracing::warn!(
            attempt,
            delay_ms = delay.as_millis() as u64,
            from_header = hit.retry_after.is_some(),
            provider = client.identity().provenance_label(),
            "后台模型限流，退避后重试"
        );
        tokio::time::sleep(delay).await;
        backoff = (backoff * 2).min(RATE_LIMIT_CAP);
    }
    unreachable!("限流循环内必定 return")
}

fn jitter(base: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::from(duration.subsec_nanos()))
        .unwrap_or(0);
    let half = (base.as_millis() as u64) / 2;
    base / 2 + Duration::from_millis(if half == 0 { 0 } else { nanos % half })
}

/// 按模型的信号量注册表。限额变了就换一把新的——旧的在飞许可自然跑完，
/// 换的瞬间可能短暂超出新限额，可接受；换来的是"改完即时生效"而不必做
/// 缓存失效，也不必和 tokio Semaphore 不能缩容的限制搏斗。
#[derive(Default)]
pub struct ModelGates {
    inner: std::sync::Mutex<HashMap<String, (usize, Arc<Semaphore>)>>,
}

impl ModelGates {
    fn gate(&self, key: &str, limit: usize) -> Arc<Semaphore> {
        let mut m = self.inner.lock().unwrap();
        match m.get(key) {
            Some((n, sem)) if *n == limit => sem.clone(),
            _ => {
                let sem = Arc::new(Semaphore::new(limit));
                m.insert(key.to_string(), (limit, sem.clone()));
                sem
            }
        }
    }
}

/// 后台任务调模型前取一张许可，持有到调用结束。
pub async fn acquire(
    state: &AppState,
    base_url: &str,
    model: &str,
) -> Option<OwnedSemaphorePermit> {
    let limit = utopia_store::model_limits::limit_for(&state.pool, base_url, model)
        .await
        .ok()?;
    let key = format!("{base_url}|{model}");
    state
        .model_gates
        .gate(&key, limit)
        .acquire_owned()
        .await
        .ok()
}

pub async fn acquire_chat(state: &AppState, s: &LlmSettings) -> Option<OwnedSemaphorePermit> {
    let (base, model) = (s.chat_base_url.as_deref()?, s.chat_model.as_deref()?);
    acquire(state, base, model).await
}

pub async fn acquire_embed(state: &AppState, s: &LlmSettings) -> Option<OwnedSemaphorePermit> {
    let (base, model) = (s.embed_base_url.as_deref()?, s.embed_model.as_deref()?);
    acquire(state, base, model).await
}

#[cfg(test)]
mod background_selection_tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use std::fs;
    use utopia_core::models::{ChatAccessMode, LlmSettings};
    use utopia_llm::BackgroundTextClient;

    fn settings(mode: ChatAccessMode) -> LlmSettings {
        LlmSettings {
            workspace_id: uuid::Uuid::nil(),
            chat_base_url: Some("https://api.openai.com/v1".into()),
            chat_api_key: Some("api-fixture".into()),
            chat_model: Some("workspace-model".into()),
            chat_access_mode: mode,
            embed_base_url: None,
            embed_api_key: None,
            embed_model: None,
            embed_dim: None,
            updated_at: Utc::now(),
        }
    }

    fn codex_home() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::write(
            dir.path().join("auth.json"),
            json!({
                "auth_mode": "chatgpt",
                "tokens": {
                    "access_token": "access-fixture",
                    "refresh_token": "refresh-fixture",
                    "id_token": "id-fixture",
                    "account_id": "account-fixture"
                },
                "last_refresh": "2099-01-01T00:00:00Z"
            })
            .to_string(),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                dir.path().join("auth.json"),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        dir
    }

    #[test]
    fn workspace_access_mode_selects_background_transport_without_changing_chat() {
        let api_settings = settings(ChatAccessMode::Api);
        let subscription_settings = settings(ChatAccessMode::Subscription);
        let api_runtime = BackgroundRuntime::from_config(&AppConfig::default()).unwrap();

        assert!(matches!(
            api_runtime.client(Some(&api_settings)),
            Some(BackgroundTextClient::ChatCompletions(_))
        ));
        assert!(api_runtime.client(Some(&subscription_settings)).is_none());
        assert!(chat_client(&subscription_settings).is_some());
    }

    #[test]
    fn subscription_mode_uses_the_workspace_selected_model_when_enabled() {
        let home = codex_home();
        let mut cfg = AppConfig {
            background_llm_provider: BackgroundLlmProvider::CodexResponses,
            codex_model: Some("deployment-default".into()),
            codex_home: Some(home.path().to_string_lossy().into_owned()),
            ..AppConfig::default()
        };
        cfg.codex_max_concurrency = 1;
        let runtime = BackgroundRuntime::from_config(&cfg).unwrap();
        let client = runtime.client(Some(&settings(ChatAccessMode::Subscription)));

        match client {
            Some(BackgroundTextClient::CodexResponses(client)) => {
                assert_eq!(client.identity().model, "workspace-model");
            }
            _ => panic!("expected the selected workspace model to use Codex Responses"),
        }
    }
}

#[cfg(test)]
mod codex_builder_contract_tests {
    use serde_json::{json, Value};
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;
    use utopia_extract::{
        build_adjudication_messages, build_messages, AdjudicationPair, AdjudicationSide,
        PromptRelation,
    };
    use utopia_llm::{BackgroundTextClient, ChatMessage, CodexAuthManager, CodexResponsesClient};
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn auth_fixture() -> (TempDir, Arc<CodexAuthManager>) {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
        let auth_path = dir.path().join("auth.json");
        fs::write(
            &auth_path,
            json!({
                "auth_mode": "chatgpt",
                "tokens": {
                    "access_token": "access-fixture",
                    "refresh_token": "refresh-fixture",
                    "id_token": "id-fixture",
                    "account_id": "account-fixture"
                },
                "last_refresh": "2099-01-01T00:00:00Z"
            })
            .to_string(),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&auth_path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let auth = Arc::new(CodexAuthManager::new(dir.path()).unwrap());
        (dir, auth)
    }

    fn request_body(model: &str, messages: &[ChatMessage]) -> Value {
        json!({
            "model": model,
            "instructions": messages[0].content,
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": messages[1].content}]
            }],
            "tool_choice": "auto",
            "parallel_tool_calls": false,
            "reasoning": null,
            "store": false,
            "stream": true,
            "include": []
        })
    }

    fn completed_stream() -> &'static str {
        "event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"{\\\"verdicts\\\":[{\\\"i\\\":0,\\\"verdict\\\":\\\"same\\\",\\\"confidence\\\":0.9}]}\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
    }

    #[tokio::test]
    async fn real_extraction_and_adjudication_builders_use_the_same_codex_contract() {
        let extraction_messages = build_messages(
            &[("person".into(), "Person".into(), "a named person".into())],
            &[PromptRelation {
                key: "works_at".into(),
                label: "works at".into(),
                description: "employment relationship".into(),
                signature: "person → organization".into(),
            }],
            &[],
            Some("2026-01-02"),
            "fixture.md",
            &[],
            "Alice works at Acme.",
        );
        let adjudication_messages = build_adjudication_messages(&[AdjudicationPair {
            left: AdjudicationSide {
                name: "Alice".into(),
                type_label: "person".into(),
                facts: vec!["works_at: Acme".into()],
            },
            right: AdjudicationSide {
                name: "Alice Smith".into(),
                type_label: "person".into(),
                facts: vec!["works_at: Acme".into()],
            },
        }]);
        assert_eq!(extraction_messages.len(), 2);
        assert_eq!(adjudication_messages.len(), 2);

        let server = MockServer::start().await;
        for messages in [&extraction_messages, &adjudication_messages] {
            let request = Mock::given(method("POST"))
                .and(path("/responses"))
                .and(body_json(request_body("codex-model", messages)))
                .and(header("authorization", "Bearer access-fixture"))
                .and(header("ChatGPT-Account-ID", "account-fixture"));
            request
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_raw(completed_stream(), "text/event-stream"),
                )
                .expect(1)
                .mount(&server)
                .await;
        }
        let (_dir, auth) = auth_fixture();
        let client =
            CodexResponsesClient::with_base_url(auth, "codex-model", server.uri()).unwrap();
        let background = BackgroundTextClient::CodexResponses(client);
        let extraction_reply = background.complete(&extraction_messages).await.unwrap();
        let extraction = utopia_extract::parse_response(&extraction_reply).unwrap();
        assert!(extraction.entities.is_empty());
        assert!(extraction.facts.is_empty());

        let adjudication_reply = background.complete(&adjudication_messages).await.unwrap();
        let verdicts = utopia_extract::parse_adjudication(&adjudication_reply).unwrap();
        assert_eq!(verdicts.len(), 1);
        assert_eq!(verdicts[0].i, 0);
        assert_eq!(verdicts[0].verdict, "same");
    }
}
