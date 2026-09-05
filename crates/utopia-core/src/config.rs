use figment::{
    providers::{Env, Serialized},
    Figment,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum BackgroundLlmProvider {
    #[default]
    ChatCompletions,
    CodexResponses,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CodexAuthSource {
    #[default]
    Internal,
    Preauthenticated,
}

/// 全局配置。来源优先级：环境变量（前缀 `UTOPIA_`）> 默认值。
/// `.env` 文件由二进制入口通过 dotenvy 预加载进环境变量。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub database_url: String,
    /// 跑迁移用的连接串。迁移要建表建触发器，运行时不需要那些权限——分开之后
    /// 应用可以用一个只读写业务表、对台账只增不改的受限角色连库。
    /// 不设则回落到 `database_url`：既有部署无需改动即可照常升级。
    pub migration_url: Option<String>,
    pub bind_addr: String,
    /// JWT 签名密钥。留空则首次启动时自动生成并存进 deployment_settings——
    /// 要求部署者手填一个随机串，现实中的结果是默认值原样上生产。
    /// 显式给出时优先于库里那条：密钥轮换与多实例显式对齐走这条路。
    pub jwt_secret: Option<String>,
    /// 前端构建产物目录；存在时由服务端托管 SPA（history fallback）。
    pub web_dist: String,
    /// 数据目录：原始文件（files/）与 Tantivy 索引（index/）。
    pub data_dir: String,
    /// 数据库连接池上限。缺省 32，与 worker 并发的缺省对齐——池子小于并发时
    /// 症状是请求变慢而不是任何一处说"池子不够"，所以它必须可调。
    pub db_max_connections: Option<u32>,
    /// 强制给会话 cookie 打 Secure。缺省 false：由请求的 X-Forwarded-Proto 判定，
    /// 走 TLS 才打。只有代理不发那个头时才需要在这里强制打开。
    pub cookie_secure: bool,
    /// 是否开放注册。false 时仅首个用户（引导部署）可注册，其余需管理员开放。
    pub open_registration: bool,
    /// Deployment-wide capability gate for workspace-selected background transport.
    /// Interactive chat and embeddings do not use it.
    #[serde(default)]
    pub background_llm_provider: BackgroundLlmProvider,
    /// Required only when `background_llm_provider=codex_responses`.
    pub codex_model: Option<String>,
    /// Dedicated, absolute, file-backed Codex credential directory for preauthenticated mode.
    pub codex_home: Option<String>,
    /// Internal Utopia login is the default; CODEX_HOME is read-only fallback when selected.
    #[serde(default)]
    pub codex_auth_source: CodexAuthSource,
    /// Codex background call cap. Deliberately bounded to avoid subscription request storms.
    pub codex_max_concurrency: u32,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://utopia:***@localhost:1517/utopia".into(),
            migration_url: None,
            bind_addr: "0.0.0.0:1516".into(),
            jwt_secret: None,
            web_dist: "web/dist".into(),
            data_dir: "data".into(),
            db_max_connections: None,
            cookie_secure: false,
            open_registration: true,
            background_llm_provider: BackgroundLlmProvider::ChatCompletions,
            codex_model: None,
            codex_home: None,
            codex_auth_source: CodexAuthSource::Internal,
            codex_max_concurrency: 1,
        }
    }
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        let cfg: AppConfig = Figment::from(Serialized::defaults(AppConfig::default()))
            .merge(Env::prefixed("UTOPIA_"))
            .extract()?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.background_llm_provider == BackgroundLlmProvider::CodexResponses {
            if self
                .codex_model
                .as_deref()
                .is_none_or(|model| model.trim().is_empty())
            {
                anyhow::bail!("UTOPIA_CODEX_MODEL is required for Codex Responses mode");
            }
            if self.codex_auth_source == CodexAuthSource::Preauthenticated
                && self
                    .codex_home
                    .as_deref()
                    .is_none_or(|home| home.trim().is_empty())
            {
                anyhow::bail!("UTOPIA_CODEX_HOME is required for preauthenticated Codex mode");
            }
            if !(1..=8).contains(&self.codex_max_concurrency) {
                anyhow::bail!("UTOPIA_CODEX_MAX_CONCURRENCY must be between 1 and 8");
            }
        }
        Ok(())
    }

    /// 迁移连接串：未单独配置时用运行时那一个。
    pub fn migration_url(&self) -> &str {
        self.migration_url.as_deref().unwrap_or(&self.database_url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_background_provider_is_chat_completions() {
        let cfg = AppConfig::default();
        assert_eq!(
            cfg.background_llm_provider,
            BackgroundLlmProvider::ChatCompletions
        );
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn codex_mode_requires_explicit_prerequisites_and_bounded_concurrency() {
        let mut cfg = AppConfig {
            background_llm_provider: BackgroundLlmProvider::CodexResponses,
            ..AppConfig::default()
        };
        assert!(cfg.validate().is_err());
        cfg.codex_model = Some("model".into());
        cfg.codex_home = Some("/run/utopia-codex".into());
        assert!(cfg.validate().is_ok());
        cfg.codex_max_concurrency = 0;
        assert!(cfg.validate().is_err());
        cfg.codex_max_concurrency = 9;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn unknown_provider_is_rejected_by_serde() {
        let error =
            serde_json::from_value::<BackgroundLlmProvider>(serde_json::json!("arbitrary_host"))
                .unwrap_err();
        assert!(error.to_string().contains("unknown variant"));
    }

    #[test]
    fn chat_access_mode_wire_values_are_stable_and_closed() {
        use crate::models::ChatAccessMode;

        assert_eq!(
            serde_json::to_string(&ChatAccessMode::Api).unwrap(),
            "\"api\""
        );
        assert_eq!(
            serde_json::to_string(&ChatAccessMode::Subscription).unwrap(),
            "\"subscription\""
        );
        assert!(serde_json::from_str::<ChatAccessMode>("\"responses\"").is_err());
    }
}
