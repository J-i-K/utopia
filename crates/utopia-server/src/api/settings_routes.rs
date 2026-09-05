use axum::extract::{Path, State};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use url::Url;
use utopia_core::models::{ChatAccessMode, Role};
use utopia_llm::ChatMessage;
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::error::ApiResult;
use crate::llm_util;
use crate::state::AppState;
use utopia_core::AppError;

fn is_openai_api_url(value: &str) -> bool {
    let Ok(url) = Url::parse(value) else {
        return false;
    };
    url.scheme() == "https"
        && url.host_str() == Some("api.openai.com")
        && url.port().is_none()
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
}

fn invalid_mode() -> utopia_core::AppError {
    utopia_core::AppError::invalid(
        "invalid_chat_access_mode",
        "chat_access_mode must be api or subscription",
    )
}

fn effective_string(
    incoming: &Option<String>,
    current: Option<&String>,
    nonempty: &impl Fn(&Option<String>) -> Option<String>,
) -> Option<String> {
    match incoming {
        Some(_) => nonempty(incoming),
        None => current.cloned(),
    }
}

/// GET：脱敏视图（密钥只回传是否已配置）。
pub async fn get(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(workspace_id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    utopia_store::workspaces::require_role(&state.pool, user.id, workspace_id, Role::Admin).await?;
    let s = utopia_store::settings::get(&state.pool, workspace_id).await?;
    Ok(Json(match s {
        None => json!({
            "subscription_available": state.background.subscription_available(),
            "subscription_authenticated": matches!(
                state.background.subscription_auth_status(),
                Some(utopia_llm::CodexAuthStatus::Authenticated)
            ),
        }),
        Some(s) => json!({
            "chat_base_url": s.chat_base_url,
            "chat_model": s.chat_model,
            "chat_access_mode": s.chat_access_mode,
            "has_chat_key": s.chat_api_key.as_deref().is_some_and(|k| !k.is_empty()),
            "embed_base_url": s.embed_base_url,
            "embed_model": s.embed_model,
            "embed_dim": s.embed_dim,
            "has_embed_key": s.embed_api_key.as_deref().is_some_and(|k| !k.is_empty()),
            "subscription_available": state.background.subscription_available(),
            "subscription_authenticated": matches!(
                state.background.subscription_auth_status(),
                Some(utopia_llm::CodexAuthStatus::Authenticated)
            ),
        }),
    }))
}

#[derive(Deserialize)]
pub struct PutSettingsReq {
    pub chat_base_url: Option<String>,
    /// None 或空串 = 保留旧密钥
    pub chat_api_key: Option<String>,
    pub chat_model: Option<String>,
    pub chat_access_mode: Option<String>,
    pub embed_base_url: Option<String>,
    pub embed_api_key: Option<String>,
    pub embed_model: Option<String>,
    pub embed_dim: Option<i32>,
}

pub async fn put(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(workspace_id): Path<Uuid>,
    Json(req): Json<PutSettingsReq>,
) -> ApiResult<Json<serde_json::Value>> {
    utopia_store::workspaces::require_role(&state.pool, user.id, workspace_id, Role::Admin).await?;
    let nonempty = |v: &Option<String>| -> Option<String> {
        v.as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(String::from)
    };
    let current = utopia_store::settings::get(&state.pool, workspace_id).await?;
    let access_mode = match req.chat_access_mode.as_deref() {
        None => current
            .as_ref()
            .map(|s| s.chat_access_mode)
            .unwrap_or_default(),
        Some(value) => ChatAccessMode::parse(value).ok_or_else(invalid_mode)?,
    };
    let effective_base = effective_string(
        &req.chat_base_url,
        current.as_ref().and_then(|s| s.chat_base_url.as_ref()),
        &nonempty,
    );
    let effective_model = effective_string(
        &req.chat_model,
        current.as_ref().and_then(|s| s.chat_model.as_ref()),
        &nonempty,
    );
    if access_mode == ChatAccessMode::Subscription {
        if !matches!(
            state.background.subscription_auth_status(),
            Some(utopia_llm::CodexAuthStatus::Authenticated)
        ) {
            return Err(AppError::invalid(
                "subscription_authentication_required",
                "ChatGPT subscription access must be authenticated for this deployment",
            )
            .into());
        }
        if !effective_base.as_deref().is_some_and(is_openai_api_url) {
            return Err(AppError::invalid(
                "subscription_requires_openai",
                "ChatGPT subscription mode requires https://api.openai.com",
            )
            .into());
        }
        if effective_model.is_none() {
            return Err(AppError::invalid(
                "subscription_model_required",
                "A model is required for ChatGPT subscription mode",
            )
            .into());
        }
    }
    utopia_store::settings::upsert(
        &state.pool,
        workspace_id,
        nonempty(&req.chat_base_url).as_deref(),
        nonempty(&req.chat_api_key).as_deref(),
        nonempty(&req.chat_model).as_deref(),
        Some(access_mode),
        nonempty(&req.embed_base_url).as_deref(),
        nonempty(&req.embed_api_key).as_deref(),
        nonempty(&req.embed_model).as_deref(),
        req.embed_dim,
    )
    .await?;
    Ok(Json(json!({ "ok": true })))
}

/// 连通性测试：对话发一条最小消息；embedding 试算一条并返回维度。
pub async fn test(
    State(state): State<AppState>,
    AuthUser(user): AuthUser,
    Path(workspace_id): Path<Uuid>,
) -> ApiResult<Json<serde_json::Value>> {
    utopia_store::workspaces::require_role(&state.pool, user.id, workspace_id, Role::Admin).await?;
    let Some(s) = utopia_store::settings::get(&state.pool, workspace_id).await? else {
        return Ok(Json(
            json!({ "chat": { "ok": false, "error": "Not configured" },
                               "embed": { "ok": false, "error": "Not configured" } }),
        ));
    };

    let chat_result = match llm_util::chat_client(&s) {
        None => json!({ "ok": false, "error": "Not configured" }),
        Some(client) => {
            let msg = [ChatMessage {
                role: "user".into(),
                content: "Reply with exactly one word: OK".into(),
            }];
            match client.chat(&msg).await {
                Ok(reply) => {
                    json!({ "ok": true, "reply": reply.chars().take(50).collect::<String>() })
                }
                Err(e) => json!({ "ok": false, "error": e.to_string() }),
            }
        }
    };

    let embed_result = match llm_util::embed_client(&s) {
        None => json!({ "ok": false, "error": "Not configured" }),
        Some(client) => match client.embed(&["connectivity test".to_string()]).await {
            Ok(v) if !v.is_empty() => json!({ "ok": true, "dim": v[0].len() }),
            Ok(_) => json!({ "ok": false, "error": "Empty response" }),
            Err(e) => json!({ "ok": false, "error": e.to_string() }),
        },
    };

    Ok(Json(json!({ "chat": chat_result, "embed": embed_result })))
}

#[cfg(test)]
mod tests {
    use super::is_openai_api_url;

    #[test]
    fn subscription_host_guard_is_exact_and_https_only() {
        assert!(is_openai_api_url("https://api.openai.com/v1"));
        assert!(is_openai_api_url("https://api.openai.com"));
        assert!(!is_openai_api_url("http://api.openai.com/v1"));
        assert!(!is_openai_api_url("https://evil.example/v1"));
        assert!(!is_openai_api_url("https://api.openai.com:8443/v1"));
        assert!(!is_openai_api_url("https://user:pass@api.openai.com/v1"));
        assert!(!is_openai_api_url("https://api.openai.com/v1?tenant=other"));
    }
}
