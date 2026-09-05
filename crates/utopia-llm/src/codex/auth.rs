use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use fs2::FileExt;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use uuid::Uuid;

const AUTH_FILE: &str = "auth.json";
const LOCK_FILE: &str = ".utopia-codex.lock";
const REFRESH_ENDPOINT: &str = "https://auth.openai.com/oauth/token";
const DEVICE_USERCODE_ENDPOINT: &str = "https://auth.openai.com/api/accounts/deviceauth/usercode";
const DEVICE_TOKEN_ENDPOINT: &str = "https://auth.openai.com/api/accounts/deviceauth/token";
const DEVICE_REDIRECT_URI: &str = "https://auth.openai.com/deviceauth/callback";
const OAUTH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const REFRESH_WINDOW: Duration = Duration::from_secs(5 * 60);
const UNPARSEABLE_EXPIRY_REFRESH_INTERVAL: Duration = Duration::from_secs(8 * 24 * 60 * 60);
const MAX_REFRESH_BODY: usize = 1024 * 1024;
const MAX_AUTH_FILE: u64 = 1024 * 1024;
const CREDENTIAL_DIR_MODE: u32 = 0o700;
const CREDENTIAL_FILE_MODE: u32 = 0o600;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REFRESH_TIMEOUT: Duration = Duration::from_secs(30);
const REFRESH_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// The error boundary for the dedicated subscription credential source.
///
/// Messages intentionally contain only stable classifications. In particular, no
/// response body, URL supplied by a response, token, account identifier, or path
/// containing secret material is included in the display text.
#[derive(Debug, thiserror::Error)]
pub enum CodexAuthError {
    #[error("Codex credentials are missing or invalid: {0}")]
    Invalid(&'static str),
    #[error("Codex authentication failed permanently: {0}")]
    Permanent(&'static str),
    #[error("Codex authentication is temporarily unavailable: {0}")]
    Transient(&'static str),
    #[error("Codex credential state could not be made durable: {0}")]
    Durability(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodexAuthStatus {
    Unauthenticated,
    Authenticated,
    Invalid,
}

impl CodexAuthError {
    pub(crate) fn is_permanent(&self) -> bool {
        matches!(
            self,
            Self::Invalid(_) | Self::Permanent(_) | Self::Durability(_)
        )
    }
}

/// Secret-bearing token material is kept private and has a deliberately redacted
/// `Debug` implementation. Its fields are visible only to the Responses adapter.
#[derive(Clone)]
pub(crate) struct CredentialSnapshot {
    pub(crate) access_token: String,
    refresh_token: String,
    id_token: String,
    pub(crate) account_id: String,
    pub(crate) access_expiry: Option<SystemTime>,
    last_refresh: Option<SystemTime>,
    pub(crate) fedramp: bool,
}

impl fmt::Debug for CredentialSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CredentialSnapshot")
            .field("access_token", &"[REDACTED]")
            .field("refresh_token", &"[REDACTED]")
            .field("id_token", &"[REDACTED]")
            .field("account_id", &"[REDACTED]")
            .field("access_expiry_present", &self.access_expiry.is_some())
            .field("last_refresh_present", &self.last_refresh.is_some())
            .field("fedramp", &self.fedramp)
            .finish()
    }
}

#[derive(Clone)]
pub struct CodexAuthManager {
    home: PathBuf,
    auth_path: PathBuf,
    // The lock file is held for the lifetime of every clone of this manager.
    _ownership_lock: Arc<File>,
    refresh_lock: Arc<Mutex<()>>,
    http: Client,
    refresh_url: String,
    device_usercode_url: String,
    device_token_url: String,
    device_oauth_url: String,
    disabled: Arc<AtomicBool>,
}

impl fmt::Debug for CodexAuthManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CodexAuthManager")
            .field("auth_source", &"dedicated_file")
            .field("refresh_endpoint", &"fixed")
            .field("disabled", &self.disabled.load(Ordering::Acquire))
            .finish()
    }
}

impl CodexAuthManager {
    pub fn new(home: impl AsRef<Path>) -> Result<Self, CodexAuthError> {
        Self::build(home.as_ref(), REFRESH_ENDPOINT.to_string(), true)
    }

    /// Opens the dedicated credential home before the first login. The auth file
    /// is intentionally optional here; dispatch still fails closed until login
    /// has completed and a validated file has been atomically persisted.
    pub fn new_for_auth(home: impl AsRef<Path>) -> Result<Self, CodexAuthError> {
        Self::build(home.as_ref(), REFRESH_ENDPOINT.to_string(), false)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn with_refresh_url(
        home: impl AsRef<Path>,
        refresh_url: impl Into<String>,
    ) -> Result<Self, CodexAuthError> {
        Self::build(home.as_ref(), refresh_url.into(), true)
    }

    fn build(
        home: &Path,
        refresh_url: String,
        require_auth_file: bool,
    ) -> Result<Self, CodexAuthError> {
        if !home.is_absolute() {
            return Err(CodexAuthError::Invalid(
                "credential directory must be an absolute path",
            ));
        }
        validate_path_components(home)?;
        validate_directory(home)?;
        let auth_path = home.join(AUTH_FILE);
        if require_auth_file || auth_path.exists() {
            validate_regular_file(&auth_path, "credential file")?;
        }

        let lock_path = home.join(LOCK_FILE);
        validate_lock_path(&lock_path)?;
        let mut lock_options = OpenOptions::new();
        lock_options
            .create(true)
            .read(true)
            .write(true)
            .truncate(false);
        #[cfg(unix)]
        lock_options.mode(0o600);
        configure_no_follow(&mut lock_options);
        let ownership_lock = lock_options
            .open(&lock_path)
            .map_err(|_| CodexAuthError::Invalid("credential ownership lock is unavailable"))?;
        validate_private_file(&lock_path, "credential ownership lock")?;
        ownership_lock
            .try_lock_exclusive()
            .map_err(|_| CodexAuthError::Permanent("credential directory is already owned"))?;

        let http = build_http_client()?;
        let manager = Self {
            home: home.to_path_buf(),
            auth_path,
            _ownership_lock: Arc::new(ownership_lock),
            refresh_lock: Arc::new(Mutex::new(())),
            http,
            refresh_url,
            device_usercode_url: DEVICE_USERCODE_ENDPOINT.into(),
            device_token_url: DEVICE_TOKEN_ENDPOINT.into(),
            device_oauth_url: REFRESH_ENDPOINT.into(),
            disabled: Arc::new(AtomicBool::new(false)),
        };
        // Startup validates only local state. It deliberately performs no network call.
        if require_auth_file {
            manager.load_snapshot()?;
        }
        Ok(manager)
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn with_device_urls(mut self, usercode: String, token: String, oauth: String) -> Self {
        self.device_usercode_url = usercode;
        self.device_token_url = token;
        self.device_oauth_url = oauth;
        self
    }

    pub async fn begin_device_auth(&self) -> Result<DeviceAuthSession, CodexAuthError> {
        let verifier = Uuid::new_v4().to_string() + &Uuid::new_v4().to_string();
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        let response = self
            .http
            .post(&self.device_usercode_url)
            .json(&json!({ "client_id": OAUTH_CLIENT_ID }))
            .send()
            .await
            .map_err(|_| CodexAuthError::Transient("device authentication transport failure"))?;
        let status = response.status();
        let body = read_bounded_body(response).await?;
        if !status.is_success() {
            return Err(CodexAuthError::Permanent(
                "device authentication was rejected",
            ));
        }
        let value: Value = serde_json::from_slice(&body).map_err(|_| {
            CodexAuthError::Permanent("device authentication response is malformed")
        })?;
        let device_auth_id = required_string(&value, "device_auth_id").ok_or(
            CodexAuthError::Permanent("device authentication response omitted its id"),
        )?;
        let user_code = required_string(&value, "user_code").ok_or(CodexAuthError::Permanent(
            "device authentication response omitted its code",
        ))?;
        let verification_url = required_string(&value, "verification_url")
            .or_else(|| required_string(&value, "verification_uri"))
            .ok_or(CodexAuthError::Permanent(
                "device authentication response omitted its URL",
            ))?;
        let interval = value
            .get("interval")
            .and_then(Value::as_u64)
            .unwrap_or(5)
            .clamp(1, 30);
        let expires_in = value
            .get("expires_in")
            .and_then(Value::as_u64)
            .unwrap_or(900)
            .clamp(1, 1800);
        Ok(DeviceAuthSession {
            device_auth_id,
            user_code,
            verification_url,
            verifier,
            challenge,
            interval: Duration::from_secs(interval),
            expires_in: Duration::from_secs(expires_in),
        })
    }

    pub async fn complete_device_auth(
        &self,
        session: &DeviceAuthSession,
    ) -> Result<(), CodexAuthError> {
        let deadline = tokio::time::Instant::now() + session.expires_in;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(CodexAuthError::Permanent("device authentication expired"));
            }
            tokio::time::sleep(session.interval).await;
            let response = self.http.post(&self.device_token_url).json(&json!({ "device_auth_id": session.device_auth_id, "user_code": session.user_code })).send().await.map_err(|_| CodexAuthError::Transient("device authentication polling failed"))?;
            let status = response.status();
            let body = read_bounded_body(response).await?;
            let value: Value = serde_json::from_slice(&body).map_err(|_| {
                CodexAuthError::Permanent("device authentication poll response is malformed")
            })?;
            if status.is_success() {
                let code = required_string(&value, "authorization_code")
                    .or_else(|| required_string(&value, "code"))
                    .ok_or(CodexAuthError::Permanent(
                        "device authentication omitted authorization code",
                    ))?;
                let form = [
                    ("grant_type", "authorization_code"),
                    ("code", code.as_str()),
                    ("redirect_uri", DEVICE_REDIRECT_URI),
                    ("client_id", OAUTH_CLIENT_ID),
                    ("code_verifier", session.verifier.as_str()),
                ]
                .into_iter()
                .map(|(key, value)| format!("{}={}", key, form_escape(value)))
                .collect::<Vec<_>>()
                .join("&");
                let response = self
                    .http
                    .post(&self.device_oauth_url)
                    .header(
                        reqwest::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .body(form)
                    .send()
                    .await
                    .map_err(|_| {
                        CodexAuthError::Transient("device authentication exchange failed")
                    })?;
                let exchange_status = response.status();
                let exchange_body = read_bounded_body(response).await?;
                if !exchange_status.is_success() {
                    return Err(CodexAuthError::Permanent(
                        "device authentication exchange was rejected",
                    ));
                }
                let tokens: Value = serde_json::from_slice(&exchange_body).map_err(|_| {
                    CodexAuthError::Permanent(
                        "device authentication exchange response is malformed",
                    )
                })?;
                let access =
                    required_string(&tokens, "access_token").ok_or(CodexAuthError::Permanent(
                        "device authentication exchange omitted access token",
                    ))?;
                let refresh =
                    required_string(&tokens, "refresh_token").ok_or(CodexAuthError::Permanent(
                        "device authentication exchange omitted refresh token",
                    ))?;
                let id = required_string(&tokens, "id_token").ok_or(CodexAuthError::Permanent(
                    "device authentication exchange omitted ID token",
                ))?;
                let account_id = parse_jwt_claims(&id)
                    .and_then(|claims| claims.account_id)
                    .ok_or(CodexAuthError::Permanent(
                        "device authentication ID token omitted account",
                    ))?;
                let value = json!({ "auth_mode": "chatgpt", "tokens": { "access_token": access, "refresh_token": refresh, "id_token": id, "account_id": account_id }, "last_refresh": DateTime::<Utc>::from(SystemTime::now()).to_rfc3339() });
                self.persist(&value)?;
                return Ok(());
            }
            let error = value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if matches!(error, "authorization_pending" | "slow_down") {
                continue;
            }
            return Err(CodexAuthError::Permanent(
                "device authentication was rejected",
            ));
        }
    }

    pub fn status(&self) -> CodexAuthStatus {
        match self.load_snapshot() {
            Ok((_, snapshot))
                if snapshot
                    .access_expiry
                    .is_none_or(|expiry| expiry > SystemTime::now()) =>
            {
                CodexAuthStatus::Authenticated
            }
            Ok(_) => CodexAuthStatus::Unauthenticated,
            Err(CodexAuthError::Invalid("credential file cannot be read")) => {
                CodexAuthStatus::Unauthenticated
            }
            Err(_) => CodexAuthStatus::Invalid,
        }
    }

    pub(crate) async fn snapshot(&self) -> Result<CredentialSnapshot, CodexAuthError> {
        self.snapshot_at(SystemTime::now(), false, None).await
    }

    pub(crate) async fn snapshot_after_unauthorized(
        &self,
        observed_access_token: &str,
    ) -> Result<CredentialSnapshot, CodexAuthError> {
        self.snapshot_at(SystemTime::now(), true, Some(observed_access_token))
            .await
    }

    async fn snapshot_at(
        &self,
        now: SystemTime,
        force_refresh: bool,
        observed_access_token: Option<&str>,
    ) -> Result<CredentialSnapshot, CodexAuthError> {
        if self.disabled.load(Ordering::Acquire) {
            return Err(CodexAuthError::Durability(
                "credential dispatch is disabled until reauthentication",
            ));
        }

        let (_value, current) = self.load_snapshot_with_value()?;
        let needs_refresh = force_refresh || refresh_due(&current, now);
        if !needs_refresh {
            return Ok(current);
        }
        if force_refresh
            && observed_access_token.is_some_and(|observed| observed != current.access_token)
        {
            // Another request already rotated the token while this request was in flight.
            return Ok(current);
        }

        let _single_flight = self.refresh_lock.lock().await;
        if self.disabled.load(Ordering::Acquire) {
            return Err(CodexAuthError::Durability(
                "credential dispatch is disabled until reauthentication",
            ));
        }
        let (current_value, current) = self.load_snapshot_with_value()?;
        if force_refresh
            && observed_access_token.is_some_and(|observed| observed != current.access_token)
        {
            return Ok(current);
        }
        if !force_refresh && !refresh_due(&current, now) {
            return Ok(current);
        }

        let updated = match self.request_refresh(&current).await {
            Ok(updated) => updated,
            Err(error) => {
                if error.is_permanent() {
                    self.disabled.store(true, Ordering::Release);
                }
                return Err(error);
            }
        };
        let mut updated_value = current_value;
        apply_refresh(&mut updated_value, &updated)?;
        if let Err(error) = self.persist(&updated_value) {
            // The provider may already have rotated the refresh token. The old file
            // is not a safe recovery artifact, so dispatch is terminal until reauth.
            self.disabled.store(true, Ordering::Release);
            return Err(error);
        }
        self.load_snapshot().map(|(_, snapshot)| snapshot)
    }

    fn load_snapshot(&self) -> Result<(Value, CredentialSnapshot), CodexAuthError> {
        self.load_snapshot_with_value()
    }

    fn load_snapshot_with_value(&self) -> Result<(Value, CredentialSnapshot), CodexAuthError> {
        let mut read_options = OpenOptions::new();
        read_options.read(true);
        configure_no_follow(&mut read_options);
        let file = read_options
            .open(&self.auth_path)
            .map_err(|_| CodexAuthError::Invalid("credential file cannot be read"))?;
        let metadata = file
            .metadata()
            .map_err(|_| CodexAuthError::Invalid("credential file cannot be read"))?;
        if !metadata.is_file() {
            return Err(CodexAuthError::Invalid(
                "credential file is not a regular file",
            ));
        }
        if metadata.len() > MAX_AUTH_FILE {
            return Err(CodexAuthError::Invalid("credential file exceeds its bound"));
        }
        validate_private_permissions(&metadata, "credential file", CREDENTIAL_FILE_MODE)?;
        let mut bytes = Vec::new();
        file.take(MAX_AUTH_FILE.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|_| CodexAuthError::Invalid("credential file cannot be read"))?;
        if bytes.len() as u64 > MAX_AUTH_FILE {
            return Err(CodexAuthError::Invalid("credential file exceeds its bound"));
        }
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|_| CodexAuthError::Invalid("credential file is malformed JSON"))?;
        let snapshot = parse_snapshot(&value)?;
        Ok((value, snapshot))
    }

    async fn request_refresh(
        &self,
        current: &CredentialSnapshot,
    ) -> Result<RefreshMaterial, CodexAuthError> {
        let response = self
            .http
            .post(&self.refresh_url)
            .json(&json!({
                "client_id": OAUTH_CLIENT_ID,
                "grant_type": "refresh_token",
                "refresh_token": current.refresh_token,
            }))
            .send()
            .await
            .map_err(|_| CodexAuthError::Transient("credential refresh transport failure"))?;

        let status = response.status();
        if status.is_redirection() {
            return Err(CodexAuthError::Permanent(
                "credential refresh returned a redirect",
            ));
        }
        let body = read_bounded_body(response).await?;
        if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(CodexAuthError::Transient(
                "credential refresh service is temporarily unavailable",
            ));
        }
        let value: Value = serde_json::from_slice(&body)
            .map_err(|_| CodexAuthError::Permanent("credential refresh response is malformed"))?;
        if !status.is_success() {
            return Err(CodexAuthError::Permanent(refresh_failure_class(&value)));
        }

        let access_token = required_string(&value, "access_token").ok_or(
            CodexAuthError::Permanent("credential refresh omitted access token"),
        )?;
        let refresh_token = optional_string(&value, "refresh_token")
            .unwrap_or_else(|| current.refresh_token.clone());
        let id_token =
            optional_string(&value, "id_token").unwrap_or_else(|| current.id_token.clone());
        let claims = parse_jwt_claims(&id_token);
        if let Some(account_id) = claims
            .as_ref()
            .and_then(|claims| claims.account_id.as_deref())
        {
            if account_id != current.account_id {
                return Err(CodexAuthError::Permanent(
                    "refreshed credential account does not match",
                ));
            }
        }
        if claims.as_ref().is_some_and(|claims| claims.fedramp) {
            return Err(CodexAuthError::Permanent(
                "FedRAMP subscription profiles are unsupported",
            ));
        }
        Ok(RefreshMaterial {
            access_token,
            refresh_token,
            id_token,
        })
    }

    fn persist(&self, value: &Value) -> Result<(), CodexAuthError> {
        let encoded = serde_json::to_vec_pretty(value)
            .map_err(|_| CodexAuthError::Durability("credential JSON cannot be encoded"))?;
        let tmp_path = self
            .home
            .join(format!(".auth.json.tmp-{}", std::process::id()));
        let result = (|| {
            let mut temp_options = OpenOptions::new();
            temp_options.create_new(true).write(true);
            #[cfg(unix)]
            temp_options.mode(0o600);
            let mut file = temp_options.open(&tmp_path).map_err(|_| {
                CodexAuthError::Durability("credential temp file cannot be created")
            })?;
            file.write_all(&encoded).map_err(|_| {
                CodexAuthError::Durability("credential temp file cannot be written")
            })?;
            file.sync_all()
                .map_err(|_| CodexAuthError::Durability("credential temp file cannot be synced"))?;
            fs::rename(&tmp_path, &self.auth_path).map_err(|_| {
                CodexAuthError::Durability("credential file cannot be atomically replaced")
            })?;
            File::open(&self.home)
                .and_then(|directory| directory.sync_all())
                .map_err(|_| CodexAuthError::Durability("credential directory cannot be synced"))?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        result
    }
}

/// Short-lived, server-only device flow state. Never serialize this type to a client.
#[derive(Clone, Debug)]
pub struct DeviceAuthSession {
    pub user_code: String,
    pub verification_url: String,
    device_auth_id: String,
    verifier: String,
    #[allow(dead_code)]
    challenge: String,
    interval: Duration,
    expires_in: Duration,
}

struct RefreshMaterial {
    access_token: String,
    refresh_token: String,
    id_token: String,
}

impl fmt::Debug for RefreshMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RefreshMaterial")
            .field("access_token", &"[REDACTED]")
            .field("refresh_token", &"[REDACTED]")
            .field("id_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug)]
struct JwtClaims {
    expiry: Option<SystemTime>,
    account_id: Option<String>,
    fedramp: bool,
}

fn build_http_client() -> Result<Client, CodexAuthError> {
    Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REFRESH_TIMEOUT)
        .read_timeout(REFRESH_READ_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| CodexAuthError::Invalid("credential HTTP client cannot be created"))
}

async fn read_bounded_body(response: reqwest::Response) -> Result<Vec<u8>, CodexAuthError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_REFRESH_BODY as u64)
    {
        return Err(CodexAuthError::Permanent(
            "credential refresh response exceeded its bound",
        ));
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk =
            chunk.map_err(|_| CodexAuthError::Transient("credential refresh body read failed"))?;
        if body.len().saturating_add(chunk.len()) > MAX_REFRESH_BODY {
            return Err(CodexAuthError::Permanent(
                "credential refresh response exceeded its bound",
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn parse_snapshot(value: &Value) -> Result<CredentialSnapshot, CodexAuthError> {
    let mode = value
        .get("auth_mode")
        .and_then(Value::as_str)
        .unwrap_or("chatgpt");
    if mode != "chatgpt" {
        return Err(CodexAuthError::Invalid(
            "credential file is not a managed ChatGPT profile",
        ));
    }
    let tokens = value
        .get("tokens")
        .and_then(Value::as_object)
        .ok_or(CodexAuthError::Invalid("credential tokens are missing"))?;
    let access_token = required_string_from(tokens, "access_token")
        .ok_or(CodexAuthError::Invalid("access token is missing"))?;
    let refresh_token = required_string_from(tokens, "refresh_token")
        .ok_or(CodexAuthError::Invalid("refresh token is missing"))?;
    let id_token = required_string_from(tokens, "id_token")
        .ok_or(CodexAuthError::Invalid("ID token is missing"))?;
    let account_id = required_string_from(tokens, "account_id")
        .ok_or(CodexAuthError::Invalid("account identifier is missing"))?;

    let id_claims = parse_jwt_claims(&id_token);
    if let Some(claim_account_id) = id_claims
        .as_ref()
        .and_then(|claims| claims.account_id.as_deref())
    {
        if claim_account_id != account_id {
            return Err(CodexAuthError::Invalid(
                "credential account identifier is inconsistent",
            ));
        }
    }
    if id_claims.as_ref().is_some_and(|claims| claims.fedramp) {
        return Err(CodexAuthError::Permanent(
            "FedRAMP subscription profiles are unsupported",
        ));
    }
    let access_expiry = parse_jwt_claims(&access_token).and_then(|claims| claims.expiry);
    let last_refresh = value
        .get("last_refresh")
        .and_then(Value::as_str)
        .and_then(parse_timestamp);
    Ok(CredentialSnapshot {
        access_token,
        refresh_token,
        id_token,
        account_id,
        access_expiry,
        last_refresh,
        fedramp: false,
    })
}

fn refresh_due(snapshot: &CredentialSnapshot, now: SystemTime) -> bool {
    if let Some(expiry) = snapshot.access_expiry {
        return now
            .checked_add(REFRESH_WINDOW)
            .is_none_or(|refresh_at| refresh_at >= expiry);
    }
    snapshot
        .last_refresh
        .and_then(|last| last.checked_add(UNPARSEABLE_EXPIRY_REFRESH_INTERVAL))
        .is_none_or(|refresh_at| now >= refresh_at)
}

fn apply_refresh(value: &mut Value, updated: &RefreshMaterial) -> Result<(), CodexAuthError> {
    let tokens = value
        .get_mut("tokens")
        .and_then(Value::as_object_mut)
        .ok_or(CodexAuthError::Durability("credential tokens disappeared"))?;
    tokens.insert(
        "access_token".into(),
        Value::String(updated.access_token.clone()),
    );
    tokens.insert(
        "refresh_token".into(),
        Value::String(updated.refresh_token.clone()),
    );
    tokens.insert("id_token".into(), Value::String(updated.id_token.clone()));
    value["last_refresh"] = Value::String(DateTime::<Utc>::from(SystemTime::now()).to_rfc3339());
    Ok(())
}

fn parse_jwt_claims(token: &str) -> Option<JwtClaims> {
    let payload = token.split('.').nth(1)?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| URL_SAFE.decode(payload))
        .ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    let auth = value
        .get("https://api.openai.com/auth")
        .or_else(|| value.get("auth"));
    let account_id = auth
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .or_else(|| value.get("account_id").and_then(Value::as_str))
        .map(str::to_owned);
    let fedramp = auth
        .and_then(|auth| auth.get("chatgpt_account_is_fedramp"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let expiry = value
        .get("exp")
        .and_then(|exp| {
            exp.as_u64()
                .or_else(|| exp.as_i64().and_then(|value| u64::try_from(value).ok()))
        })
        .and_then(|seconds| UNIX_EPOCH.checked_add(Duration::from_secs(seconds)));
    Some(JwtClaims {
        expiry,
        account_id,
        fedramp,
    })
}

fn parse_timestamp(raw: &str) -> Option<SystemTime> {
    let timestamp = DateTime::parse_from_rfc3339(raw).ok()?.timestamp();
    if timestamp < 0 {
        return None;
    }
    UNIX_EPOCH.checked_add(Duration::from_secs(timestamp as u64))
}

fn form_escape(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            b' ' => vec!['+'],
            other => format!("%{other:02X}").chars().collect(),
        })
        .collect()
}

fn required_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    required_string(value, key)
}

fn required_string_from(value: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

fn refresh_failure_class(value: &Value) -> &'static str {
    let code = value
        .get("error")
        .and_then(|error| {
            error
                .as_str()
                .or_else(|| error.get("code").and_then(Value::as_str))
        })
        .unwrap_or_default();
    match code {
        "invalid_grant"
        | "refresh_token_expired"
        | "refresh_token_reused"
        | "refresh_token_revoked" => "refresh credential was rejected",
        _ => "refresh credential was rejected",
    }
}

fn validate_directory(path: &Path) -> Result<(), CodexAuthError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| CodexAuthError::Invalid("credential directory is missing"))?;
    if !metadata.is_dir() {
        return Err(CodexAuthError::Invalid(
            "credential directory is not a directory",
        ));
    }
    validate_private_permissions(&metadata, "credential directory", CREDENTIAL_DIR_MODE)
}

fn validate_path_components(path: &Path) -> Result<(), CodexAuthError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => current.push(std::path::MAIN_SEPARATOR.to_string()),
            Component::Normal(part) => {
                current.push(part);
                let metadata = fs::symlink_metadata(&current).map_err(|_| {
                    CodexAuthError::Invalid("credential directory path is unavailable")
                })?;
                if metadata.file_type().is_symlink() {
                    return Err(CodexAuthError::Invalid(
                        "credential directory path contains a symlink",
                    ));
                }
            }
            Component::CurDir | Component::ParentDir | Component::Prefix(_) => {
                return Err(CodexAuthError::Invalid(
                    "credential directory path is not canonical",
                ));
            }
        }
    }
    Ok(())
}

fn configure_no_follow(options: &mut OpenOptions) {
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW);
}

fn validate_regular_file(path: &Path, what: &'static str) -> Result<(), CodexAuthError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| CodexAuthError::Invalid("credential file is missing"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CodexAuthError::Invalid(what));
    }
    validate_private_permissions(&metadata, what, CREDENTIAL_FILE_MODE)
}

fn validate_lock_path(path: &Path) -> Result<(), CodexAuthError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(
            CodexAuthError::Invalid("credential ownership lock is invalid"),
        ),
        Ok(metadata) => validate_private_permissions(
            &metadata,
            "credential ownership lock",
            CREDENTIAL_FILE_MODE,
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(CodexAuthError::Invalid(
            "credential ownership lock is unavailable",
        )),
    }
}

fn validate_private_file(path: &Path, what: &'static str) -> Result<(), CodexAuthError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| CodexAuthError::Invalid(what))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CodexAuthError::Invalid(what));
    }
    validate_private_permissions(&metadata, what, CREDENTIAL_FILE_MODE)
}

fn validate_private_permissions(
    metadata: &std::fs::Metadata,
    what: &'static str,
    expected_mode: u32,
) -> Result<(), CodexAuthError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o7777 != expected_mode {
            return Err(CodexAuthError::Invalid(what));
        }
    }
    #[cfg(not(unix))]
    let _ = expected_mode;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use wiremock::matchers::{body_json, body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn jwt(exp: Option<u64>, account_id: &str, fedramp: bool) -> String {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let mut claims = json!({
            "https://api.openai.com/auth": {
                "chatgpt_account_id": account_id,
                "chatgpt_account_is_fedramp": fedramp
            }
        });
        if let Some(exp) = exp {
            claims["exp"] = json!(exp);
        }
        format!(
            "{}.{}.sig",
            header,
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        )
    }

    fn home_with_tokens(exp: Option<u64>) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let id_token = jwt(Some(now + 3600), "account-test", false);
        let access_token = jwt(
            exp.map(|_| now + exp.unwrap_or(3600)),
            "account-test",
            false,
        );
        let value = json!({
            "auth_mode": "chatgpt",
            "unknown_field": {"keep": true},
            "tokens": {
                "id_token": id_token,
                "access_token": access_token,
                "refresh_token": "refresh-fixture",
                "account_id": "account-test"
            },
            "last_refresh": "2099-01-01T00:00:00Z"
        });
        let path = dir.path().join(AUTH_FILE);
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        dir
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_credential_file_is_rejected() {
        let dir = home_with_tokens(Some(3600));
        let auth_path = dir.path().join(AUTH_FILE);
        let target_path = dir.path().join("credential-target.json");
        fs::rename(&auth_path, &target_path).unwrap();
        std::os::unix::fs::symlink(&target_path, &auth_path).unwrap();
        assert!(CodexAuthManager::new(dir.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_parent_component_is_rejected() {
        let source = home_with_tokens(Some(3600));
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let real = root.path().join("real");
        let home = real.join("home");
        fs::create_dir_all(&home).unwrap();
        fs::set_permissions(&real, fs::Permissions::from_mode(0o700)).unwrap();
        fs::set_permissions(&home, fs::Permissions::from_mode(0o700)).unwrap();
        fs::copy(source.path().join(AUTH_FILE), home.join(AUTH_FILE)).unwrap();
        fs::set_permissions(home.join(AUTH_FILE), fs::Permissions::from_mode(0o600)).unwrap();
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        assert!(CodexAuthManager::new(link.join("home")).is_err());
    }

    #[test]
    fn valid_file_is_redacted_and_rejects_weak_permissions() {
        let dir = home_with_tokens(Some(3600));
        let manager = CodexAuthManager::new(dir.path()).unwrap();
        let snapshot = manager.load_snapshot().unwrap().1;
        let debug = format!("{snapshot:?}{manager:?}");
        assert!(!debug.contains("refresh-fixture"));
        assert!(!debug.contains("account-test"));

        let material = RefreshMaterial {
            access_token: "access-material-fixture".into(),
            refresh_token: "refresh-material-fixture".into(),
            id_token: "id-material-fixture".into(),
        };
        let material_debug = format!("{material:?}");
        assert!(!material_debug.contains("access-material-fixture"));
        assert!(!material_debug.contains("refresh-material-fixture"));
        assert!(!material_debug.contains("id-material-fixture"));

        drop(manager);
        fs::set_permissions(
            dir.path().join(AUTH_FILE),
            fs::Permissions::from_mode(0o640),
        )
        .unwrap();
        assert!(CodexAuthManager::new(dir.path()).is_err());

        fs::set_permissions(
            dir.path().join(AUTH_FILE),
            fs::Permissions::from_mode(0o400),
        )
        .unwrap();
        assert!(CodexAuthManager::new(dir.path()).is_err());
        fs::set_permissions(
            dir.path().join(AUTH_FILE),
            fs::Permissions::from_mode(CREDENTIAL_FILE_MODE),
        )
        .unwrap();

        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o500)).unwrap();
        assert!(CodexAuthManager::new(dir.path()).is_err());
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(CREDENTIAL_DIR_MODE)).unwrap();
    }

    #[test]
    fn auth_status_distinguishes_valid_and_expired_credentials() {
        let valid = home_with_tokens(Some(3600));
        let manager = CodexAuthManager::new(valid.path()).unwrap();
        assert_eq!(manager.status(), CodexAuthStatus::Authenticated);

        let expired = home_with_tokens(Some(0));
        let manager = CodexAuthManager::new(expired.path()).unwrap();
        assert_eq!(manager.status(), CodexAuthStatus::Unauthenticated);
    }

    #[test]
    fn unparseable_expiry_uses_the_eight_day_last_refresh_fallback() {
        let dir = home_with_tokens(None);
        let value = json!({
            "tokens": {
                "id_token": "not-a-jwt",
                "access_token": "opaque-access",
                "refresh_token": "refresh-fixture",
                "account_id": "account-test"
            },
            "last_refresh": "2026-01-01T00:00:00Z"
        });
        fs::write(
            dir.path().join(AUTH_FILE),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
        let manager = CodexAuthManager::new(dir.path()).unwrap();
        let snapshot = manager.load_snapshot().unwrap().1;
        let last = parse_timestamp("2026-01-01T00:00:00Z").unwrap();
        assert!(refresh_due(
            &snapshot,
            last + Duration::from_secs(8 * 24 * 60 * 60)
        ));
    }

    #[tokio::test]
    async fn refresh_is_single_flight_and_preserves_unknown_fields() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .and(body_json(json!({
                "client_id": OAUTH_CLIENT_ID,
                "grant_type": "refresh_token",
                "refresh_token": "refresh-fixture"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": "rotated-access",
                "refresh_token": "rotated-refresh",
                "id_token": jwt(Some(4102444800), "account-test", false)
            })))
            .expect(1)
            .mount(&server)
            .await;
        let dir = home_with_tokens(Some(0));
        let manager = Arc::new(
            CodexAuthManager::with_refresh_url(dir.path(), format!("{}/oauth/token", server.uri()))
                .unwrap(),
        );
        let (a, b) = tokio::join!(manager.snapshot(), manager.snapshot());
        assert_eq!(a.unwrap().access_token, "rotated-access");
        assert_eq!(b.unwrap().access_token, "rotated-access");
        let persisted: Value =
            serde_json::from_slice(&fs::read(dir.path().join(AUTH_FILE)).unwrap()).unwrap();
        assert_eq!(persisted["unknown_field"]["keep"], true);
        assert_eq!(persisted["tokens"]["refresh_token"], "rotated-refresh");
    }

    #[tokio::test]
    async fn transient_refresh_http_failures_are_not_permanent_for_non_json_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream unavailable"))
            .expect(1)
            .mount(&server)
            .await;
        let dir = home_with_tokens(Some(0));
        let manager =
            CodexAuthManager::with_refresh_url(dir.path(), format!("{}/oauth/token", server.uri()))
                .unwrap();
        assert!(matches!(
            manager.snapshot().await.unwrap_err(),
            CodexAuthError::Transient(_)
        ));
    }

    #[tokio::test]
    async fn redirect_refresh_is_not_followed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oauth/token"))
            .respond_with(
                ResponseTemplate::new(307)
                    .insert_header("location", format!("{}/attacker", server.uri())),
            )
            .expect(1)
            .mount(&server)
            .await;
        let dir = home_with_tokens(Some(0));
        let manager =
            CodexAuthManager::with_refresh_url(dir.path(), format!("{}/oauth/token", server.uri()))
                .unwrap();
        let error = manager.snapshot().await.unwrap_err();
        assert!(matches!(error, CodexAuthError::Permanent(_)));
    }

    #[tokio::test]
    async fn device_flow_uses_bounded_server_side_exchange_and_persists_private_auth() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/usercode"))
            .and(body_json(json!({"client_id": OAUTH_CLIENT_ID})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "device_auth_id": "device-fixture", "user_code": "ABCD-EFGH",
                "verification_url": "https://auth.example/activate", "interval": 1, "expires_in": 2
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/device-token"))
            .and(body_json(
                json!({"device_auth_id": "device-fixture", "user_code": "ABCD-EFGH"}),
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"authorization_code": "code-fixture"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/oauth-token"))
            .and(body_string_contains("grant_type=authorization_code"))
            .and(body_string_contains("code_verifier="))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": "access-fixture", "refresh_token": "refresh-fixture",
                "id_token": jwt(Some(4102444800), "account-fixture", false)
            })))
            .mount(&server)
            .await;
        let dir = tempfile::tempdir().unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let manager = CodexAuthManager::new_for_auth(dir.path())
            .unwrap()
            .with_device_urls(
                format!("{}/usercode", server.uri()),
                format!("{}/device-token", server.uri()),
                format!("{}/oauth-token", server.uri()),
            );
        let session = manager.begin_device_auth().await.unwrap();
        assert_eq!(session.user_code, "ABCD-EFGH");
        manager.complete_device_auth(&session).await.unwrap();
        assert_eq!(manager.status(), CodexAuthStatus::Authenticated);
        assert_eq!(
            fs::metadata(dir.path().join(AUTH_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}
