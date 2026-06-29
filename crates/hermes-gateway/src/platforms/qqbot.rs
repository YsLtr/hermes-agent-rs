//! QQ Bot (official QQ v2 API) adapter.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{debug, info, warn};

use hermes_core::errors::GatewayError;
use hermes_core::traits::{ParseMode, PlatformAdapter, PlatformTurnMetadata};

use crate::adapter::{AdapterProxyConfig, BasePlatformAdapter};
use crate::gateway::IncomingMessage;

const QQ_TOKEN_URL: &str = "https://bots.qq.com/app/getAppAccessToken";
const QQ_API_BASE: &str = "https://api.sgroup.qq.com";
const QQ_GATEWAY_URL: &str = "https://api.sgroup.qq.com/gateway";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QqBotConfig {
    pub app_id: String,
    pub client_secret: String,
    #[serde(default)]
    pub markdown_support: bool,
    #[serde(default = "default_true")]
    pub c2c_streaming: bool,
    #[serde(default = "default_true")]
    pub progress_coalesce: bool,
    #[serde(default = "default_true")]
    pub metadata_footer: bool,
    #[serde(default = "default_true")]
    pub notify_on_stream_end: bool,
    #[serde(default = "default_max_progress_messages")]
    pub max_progress_messages: usize,
    #[serde(default)]
    pub proxy: AdapterProxyConfig,
}

pub struct QqBotAdapter {
    base: BasePlatformAdapter,
    config: QqBotConfig,
    client: Client,
    stop_signal: Arc<Notify>,
    access_token: RwLock<Option<(String, Instant)>>,
    inbound_tx: RwLock<Option<mpsc::Sender<IncomingMessage>>>,
    ws_state: Mutex<QqWsState>,
    c2c_stream_state: Mutex<std::collections::HashMap<String, C2cStreamState>>,
    turn_metadata: Mutex<std::collections::HashMap<String, PlatformTurnMetadata>>,
    progress_state: Mutex<std::collections::HashMap<String, ProgressState>>,
    notice_state: Mutex<StreamNoticeState>,
}

#[derive(Debug, Clone, Default)]
struct QqWsState {
    session_id: Option<String>,
    last_seq: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct C2cStreamState {
    id: Option<String>,
    index: u64,
    active: bool,
    msg_type: Option<i64>,
}

#[derive(Debug, Clone, Default)]
struct ProgressState {
    sent_count: usize,
    last_sent_idx: usize,
    last_line: Option<String>,
}

#[derive(Debug, Clone)]
struct StreamNoticeState {
    last_sent: Option<Instant>,
    recent_sent: Vec<Instant>,
}

impl Default for StreamNoticeState {
    fn default() -> Self {
        Self {
            last_sent: None,
            recent_sent: Vec::new(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_max_progress_messages() -> usize {
    2
}

impl QqBotAdapter {
    pub fn new(config: QqBotConfig) -> Result<Self, GatewayError> {
        if config.app_id.trim().is_empty() {
            return Err(GatewayError::Platform(
                "QQBot requires app_id (platforms.qqbot.extra.app_id)".into(),
            ));
        }
        if config.client_secret.trim().is_empty() {
            return Err(GatewayError::Platform(
                "QQBot requires client_secret (platforms.qqbot.extra.client_secret)".into(),
            ));
        }

        let base = BasePlatformAdapter::new(&config.app_id).with_proxy(config.proxy.clone());
        base.validate_token()?;
        let client = base.build_client()?;
        Ok(Self {
            base,
            config,
            client,
            stop_signal: Arc::new(Notify::new()),
            access_token: RwLock::new(None),
            inbound_tx: RwLock::new(None),
            ws_state: Mutex::new(QqWsState::default()),
            c2c_stream_state: Mutex::new(std::collections::HashMap::new()),
            turn_metadata: Mutex::new(std::collections::HashMap::new()),
            progress_state: Mutex::new(std::collections::HashMap::new()),
            notice_state: Mutex::new(StreamNoticeState::default()),
        })
    }

    pub async fn set_inbound_sender(&self, tx: mpsc::Sender<IncomingMessage>) {
        *self.inbound_tx.write().await = Some(tx);
    }

    pub fn spawn_inbound(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.websocket_supervisor().await })
    }

    async fn get_access_token(&self) -> Result<String, GatewayError> {
        if let Some((token, expiry)) = self.access_token.read().await.clone() {
            if Instant::now() < expiry {
                return Ok(token);
            }
        }

        let body = serde_json::json!({
            "appId": self.config.app_id,
            "clientSecret": self.config.client_secret
        });
        let resp = self
            .client
            .post(QQ_TOKEN_URL)
            .json(&body)
            .send()
            .await
            .map_err(|e| GatewayError::Auth(format!("QQBot auth request failed: {e}")))?;
        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::Auth(format!(
                "QQBot token endpoint returned non-success: {text}"
            )));
        }
        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| GatewayError::Auth(format!("QQBot auth parse failed: {e}")))?;
        let token = value
            .get("access_token")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                GatewayError::Auth("QQBot token response missing access_token".into())
            })?;
        let expires_in = value
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(7200);
        let expires_at = Instant::now() + Duration::from_secs(expires_in.saturating_sub(60));
        *self.access_token.write().await = Some((token.clone(), expires_at));
        Ok(token)
    }

    fn looks_like_group_chat(chat_id: &str) -> bool {
        let id = chat_id.trim().to_ascii_lowercase();
        id.starts_with("group_") || id.starts_with("grp_") || id.starts_with("qqgroup_")
    }

    fn next_msg_seq(seed: &str) -> i64 {
        let base = chrono::Utc::now().timestamp_millis();
        let salt = seed
            .bytes()
            .fold(0_i64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as i64));
        (base.wrapping_add(salt).rem_euclid(65_535)).max(1)
    }

    async fn format_metadata_footer(&self, chat_id: &str) -> String {
        if !self.config.metadata_footer {
            return String::new();
        }
        let meta = self.turn_metadata.lock().await.get(chat_id).cloned();
        let Some(meta) = meta else {
            return String::new();
        };

        let mut parts = Vec::new();
        if let Some(model) = meta.model.filter(|s| !s.trim().is_empty()) {
            parts.push(format!("model {}", model));
        }
        if let Some(provider) = meta.provider.filter(|s| !s.trim().is_empty()) {
            parts.push(format!("provider {}", provider));
        }
        if let Some(ttft) = meta.ttft_ms {
            parts.push(format!("ttft {}ms", ttft));
        }
        if let Some(total) = meta.total_ms {
            parts.push(format!("time {:.1}s", total as f64 / 1000.0));
        }
        if let Some(tools) = meta.tool_count {
            if tools > 0 {
                parts.push(format!("tools {}", tools));
            }
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!("\n\n---\n`{}`", parts.join(" | "))
        }
    }

    async fn post_qq_message(
        &self,
        endpoint: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayError> {
        let token = self.get_access_token().await?;
        let resp = self
            .client
            .post(endpoint)
            .header("Authorization", format!("QQBot {token}"))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| GatewayError::SendFailed(format!("QQBot send request failed: {e}")))?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::SendFailed(format!(
                "QQBot API error ({status}): {text}"
            )));
        }
        resp.json()
            .await
            .map_err(|e| GatewayError::SendFailed(format!("QQBot send parse failed: {e}")))
    }

    async fn get_gateway_url(&self) -> Result<String, GatewayError> {
        let token = self.get_access_token().await?;
        let resp = self
            .client
            .get(QQ_GATEWAY_URL)
            .header("Authorization", format!("QQBot {token}"))
            .header("User-Agent", "hermes-agent-rs")
            .send()
            .await
            .map_err(|e| {
                GatewayError::ConnectionFailed(format!("QQ gateway request failed: {e}"))
            })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::ConnectionFailed(format!(
                "QQ gateway endpoint returned {status}: {text}"
            )));
        }
        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| GatewayError::ConnectionFailed(format!("QQ gateway parse failed: {e}")))?;
        value
            .get("url")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| GatewayError::ConnectionFailed("QQ gateway response missing url".into()))
    }

    async fn websocket_supervisor(self: Arc<Self>) {
        let mut backoff = Duration::from_secs(2);
        while !self.base.is_running() {
            tokio::select! {
                _ = self.stop_signal.notified() => return,
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
            }
        }

        while self.base.is_running() {
            match self.run_websocket_once().await {
                Ok(()) => {
                    backoff = Duration::from_secs(2);
                }
                Err(e) => {
                    warn!("QQBot websocket disconnected: {}", e);
                    tokio::select! {
                        _ = self.stop_signal.notified() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                }
            }
        }
    }

    async fn run_websocket_once(&self) -> Result<(), GatewayError> {
        let gateway_url = self.get_gateway_url().await?;
        let (ws, _) = tokio_tungstenite::connect_async(&gateway_url)
            .await
            .map_err(|e| {
                GatewayError::ConnectionFailed(format!("QQ websocket connect failed: {e}"))
            })?;
        info!("QQBot websocket connected");
        let (mut write, mut read) = ws.split();
        let mut heartbeat: Option<tokio::time::Interval> = None;

        loop {
            tokio::select! {
                _ = self.stop_signal.notified() => {
                    let _ = write.close().await;
                    return Ok(());
                }
                _ = async {
                    if let Some(interval) = &mut heartbeat {
                        interval.tick().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    let seq = self.ws_state.lock().await.last_seq;
                    write
                        .send(WsMessage::Text(serde_json::json!({"op": 1, "d": seq}).to_string()))
                        .await
                        .map_err(|e| GatewayError::ConnectionFailed(format!("QQ heartbeat failed: {e}")))?;
                }
                msg = read.next() => {
                    let Some(msg) = msg else {
                        return Err(GatewayError::ConnectionFailed("QQ websocket closed".into()));
                    };
                    let msg = msg.map_err(|e| GatewayError::ConnectionFailed(format!("QQ websocket read failed: {e}")))?;
                    match msg {
                        WsMessage::Text(text) => {
                            let value: serde_json::Value = serde_json::from_str(&text)
                                .map_err(|e| GatewayError::Platform(format!("QQ websocket JSON parse failed: {e}")))?;
                            if let Some(interval_ms) = self.handle_ws_payload(&value, &mut write).await? {
                                let every = Duration::from_millis((interval_ms as f64 * 0.8) as u64);
                                heartbeat = Some(tokio::time::interval(every.max(Duration::from_secs(1))));
                            }
                        }
                        WsMessage::Close(frame) => {
                            return Err(GatewayError::ConnectionFailed(format!("QQ websocket close: {:?}", frame)));
                        }
                        WsMessage::Ping(data) => {
                            let _ = write.send(WsMessage::Pong(data)).await;
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    async fn handle_ws_payload<S>(
        &self,
        value: &serde_json::Value,
        write: &mut S,
    ) -> Result<Option<u64>, GatewayError>
    where
        S: futures::Sink<WsMessage, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        let op = value.get("op").and_then(|v| v.as_i64());
        if let Some(seq) = value.get("s").and_then(|v| v.as_i64()) {
            self.ws_state.lock().await.last_seq = Some(seq);
        }

        match op {
            Some(10) => {
                let interval_ms = value
                    .get("d")
                    .and_then(|d| d.get("heartbeat_interval"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(30_000);
                self.send_identify_or_resume(write).await?;
                Ok(Some(interval_ms))
            }
            Some(0) => {
                let event_type = value.get("t").and_then(|v| v.as_str()).unwrap_or("");
                let data = value.get("d").unwrap_or(&serde_json::Value::Null);
                match event_type {
                    "READY" => {
                        if let Some(session_id) = data.get("session_id").and_then(|v| v.as_str()) {
                            self.ws_state.lock().await.session_id = Some(session_id.to_string());
                            info!("QQBot websocket ready");
                        }
                    }
                    "C2C_MESSAGE_CREATE"
                    | "GROUP_AT_MESSAGE_CREATE"
                    | "DIRECT_MESSAGE_CREATE"
                    | "GUILD_MESSAGE_CREATE"
                    | "GUILD_AT_MESSAGE_CREATE" => {
                        self.dispatch_inbound_message(event_type, data).await;
                    }
                    other => debug!("QQBot unhandled websocket event: {}", other),
                }
                Ok(None)
            }
            Some(7) | Some(9) => Err(GatewayError::ConnectionFailed(
                "QQ gateway requested reconnect".into(),
            )),
            Some(11) => Ok(None),
            _ => Ok(None),
        }
    }

    async fn send_identify_or_resume<S>(&self, write: &mut S) -> Result<(), GatewayError>
    where
        S: futures::Sink<WsMessage, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        let token = self.get_access_token().await?;
        let state = self.ws_state.lock().await.clone();
        let payload = if let (Some(session_id), Some(seq)) = (state.session_id, state.last_seq) {
            serde_json::json!({
                "op": 6,
                "d": {
                    "token": format!("QQBot {token}"),
                    "session_id": session_id,
                    "seq": seq
                }
            })
        } else {
            serde_json::json!({
                "op": 2,
                "d": {
                    "token": format!("QQBot {token}"),
                    "intents": (1u64 << 25) | (1u64 << 30) | (1u64 << 12) | (1u64 << 26),
                    "shard": [0, 1],
                    "properties": {
                        "$os": "linux",
                        "$browser": "hermes-agent-rs",
                        "$device": "hermes-agent-rs"
                    }
                }
            })
        };
        write
            .send(WsMessage::Text(payload.to_string()))
            .await
            .map_err(|e| GatewayError::ConnectionFailed(format!("QQ identify failed: {e}")))
    }

    async fn dispatch_inbound_message(&self, event_type: &str, data: &serde_json::Value) {
        let Some(tx) = self.inbound_tx.read().await.clone() else {
            warn!("QQBot inbound message dropped because no gateway receiver is configured");
            return;
        };
        let msg_id = data.get("id").and_then(|v| v.as_str()).map(str::to_string);
        let text = data
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if text.is_empty() {
            return;
        }
        let author = data.get("author").and_then(|v| v.as_object());
        let (chat_id, user_id, is_dm) = match event_type {
            "C2C_MESSAGE_CREATE" => {
                let id = author
                    .and_then(|a| a.get("user_openid"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                (id.to_string(), id.to_string(), false)
            }
            "GROUP_AT_MESSAGE_CREATE" => {
                let group = data
                    .get("group_openid")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let user = author
                    .and_then(|a| a.get("member_openid"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                (group.to_string(), user.to_string(), false)
            }
            "DIRECT_MESSAGE_CREATE" => {
                let guild = data.get("guild_id").and_then(|v| v.as_str()).unwrap_or("");
                let user = author
                    .and_then(|a| a.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                (guild.to_string(), user.to_string(), false)
            }
            "GUILD_MESSAGE_CREATE" | "GUILD_AT_MESSAGE_CREATE" => {
                let channel = data
                    .get("channel_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let user = author
                    .and_then(|a| a.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                (channel.to_string(), user.to_string(), false)
            }
            _ => return,
        };
        if chat_id.is_empty() || user_id.is_empty() {
            return;
        }
        let incoming = IncomingMessage {
            platform: "qqbot".to_string(),
            chat_id,
            user_id,
            text,
            message_id: msg_id,
            is_dm,
        };
        if let Err(e) = tx.send(incoming).await {
            warn!("QQBot inbound message queue send failed: {}", e);
        }
    }

    async fn send_text_inner(
        &self,
        chat_id: &str,
        text: &str,
        append_footer: bool,
        clear_after: bool,
    ) -> Result<(), GatewayError> {
        let endpoint = if Self::looks_like_group_chat(chat_id) {
            format!("{QQ_API_BASE}/v2/groups/{chat_id}/messages")
        } else {
            format!("{QQ_API_BASE}/v2/users/{chat_id}/messages")
        };
        let footer = if append_footer {
            self.format_metadata_footer(chat_id).await
        } else {
            String::new()
        };
        let text = if footer.is_empty() {
            text.to_string()
        } else {
            format!("{}{}", text.trim_end(), footer)
        };
        let body = if self.config.markdown_support {
            serde_json::json!({
                "msg_type": 2,
                "markdown": { "content": text },
                "msg_seq": Self::next_msg_seq(chat_id)
            })
        } else {
            serde_json::json!({
                "msg_type": 0,
                "content": text,
                "msg_seq": Self::next_msg_seq(chat_id)
            })
        };
        self.post_qq_message(&endpoint, body).await?;
        if clear_after {
            self.clear_turn_state(chat_id).await;
        }
        Ok(())
    }

    async fn send_text(&self, chat_id: &str, text: &str) -> Result<(), GatewayError> {
        self.send_text_inner(chat_id, text, true, true).await
    }

    async fn clear_stream_progress_state(&self, chat_id: &str) {
        self.progress_state.lock().await.remove(chat_id);
        self.c2c_stream_state.lock().await.remove(chat_id);
    }

    async fn clear_metadata_state(&self, chat_id: &str) {
        self.turn_metadata.lock().await.remove(chat_id);
    }

    async fn clear_turn_state(&self, chat_id: &str) {
        self.clear_stream_progress_state(chat_id).await;
        self.clear_metadata_state(chat_id).await;
    }
}

#[async_trait]
impl PlatformAdapter for QqBotAdapter {
    async fn start(&self) -> Result<(), GatewayError> {
        info!("QQBot adapter starting (app_id={})", self.config.app_id);
        self.base.mark_running();
        Ok(())
    }

    async fn stop(&self) -> Result<(), GatewayError> {
        info!("QQBot adapter stopping");
        self.base.mark_stopped();
        self.stop_signal.notify_one();
        Ok(())
    }

    async fn send_message(
        &self,
        chat_id: &str,
        text: &str,
        _parse_mode: Option<ParseMode>,
    ) -> Result<(), GatewayError> {
        self.send_text(chat_id, text).await
    }

    async fn edit_message(
        &self,
        _chat_id: &str,
        _message_id: &str,
        _text: &str,
    ) -> Result<(), GatewayError> {
        debug!("QQBot does not support message editing in this adapter");
        Ok(())
    }

    async fn send_file(
        &self,
        chat_id: &str,
        file_path: &str,
        caption: Option<&str>,
    ) -> Result<(), GatewayError> {
        let file_name = std::path::Path::new(file_path)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file");
        let text = if let Some(c) = caption {
            if c.trim().is_empty() {
                format!("[Attachment: {file_name}]")
            } else {
                format!("[Attachment: {file_name}] {c}")
            }
        } else {
            format!("[Attachment: {file_name}]")
        };
        self.send_text(chat_id, &text).await
    }

    fn is_running(&self) -> bool {
        self.base.is_running()
    }

    fn platform_name(&self) -> &str {
        "qqbot"
    }

    fn supports_native_streaming(&self) -> bool {
        self.config.c2c_streaming
    }

    fn supports_progress_without_edit(&self) -> bool {
        self.config.progress_coalesce
    }

    async fn set_turn_metadata(
        &self,
        chat_id: &str,
        meta: PlatformTurnMetadata,
    ) -> Result<(), GatewayError> {
        self.turn_metadata
            .lock()
            .await
            .insert(chat_id.to_string(), meta);
        Ok(())
    }

    async fn send_stream_chunk(
        &self,
        chat_id: &str,
        text: &str,
        reply_to: Option<&str>,
        final_chunk: bool,
    ) -> Result<bool, GatewayError> {
        if !self.config.c2c_streaming || Self::looks_like_group_chat(chat_id) {
            return Ok(false);
        }

        let mut content = text.chars().take(4096).collect::<String>();
        if final_chunk {
            let footer = self.format_metadata_footer(chat_id).await;
            if !footer.is_empty() {
                content = format!("{}{}", content.trim_end(), footer);
            }
            if !content.ends_with('\n') {
                content.push('\n');
            }
        }
        if content.trim().is_empty() && !final_chunk {
            return Ok(false);
        }

        let mut states = self.c2c_stream_state.lock().await;
        let state = states.entry(chat_id.to_string()).or_default();
        let mut stream_payload = serde_json::json!({
            "state": if final_chunk { 10 } else { 1 },
            "index": state.index,
            "reset": false
        });
        if let Some(id) = &state.id {
            stream_payload["id"] = serde_json::Value::String(id.clone());
        }

        let use_markdown = self.config.markdown_support && state.msg_type != Some(0);
        let mut body = if use_markdown {
            serde_json::json!({
                "msg_type": 2,
                "markdown": { "content": content },
                "msg_seq": Self::next_msg_seq(chat_id),
                "stream": stream_payload
            })
        } else {
            serde_json::json!({
                "msg_type": 0,
                "content": content,
                "msg_seq": Self::next_msg_seq(chat_id),
                "stream": stream_payload
            })
        };
        if let Some(reply_to) = reply_to.filter(|s| !s.trim().is_empty()) {
            body["msg_id"] = serde_json::Value::String(reply_to.to_string());
        }

        let endpoint = format!("{QQ_API_BASE}/v2/users/{chat_id}/messages");
        debug!(
            chat_id = %chat_id,
            final_chunk,
            content_chars = content.chars().count(),
            stream_state = if final_chunk { 10 } else { 1 },
            msg_type = if use_markdown { 2 } else { 0 },
            stream_id = state.id.as_deref().unwrap_or(""),
            stream_index = state.index,
            "QQBot sending native stream chunk"
        );
        drop(states);
        let mut actual_msg_type = if use_markdown { 2 } else { 0 };
        let result = self.post_qq_message(&endpoint, body.clone()).await;
        let data = match result {
            Ok(data) => data,
            Err(err) if use_markdown => {
                let lowered = err.to_string().to_ascii_lowercase();
                let text = err.to_string();
                if lowered.contains("markdown")
                    || lowered.contains("md")
                    || lowered.contains("not allowed")
                    || text.contains("markdown")
                    || text.contains("消息类型")
                {
                    warn!("QQBot stream markdown rejected, locking stream to plain text");
                    body.as_object_mut().map(|obj| {
                        obj.remove("markdown");
                        obj.insert("msg_type".to_string(), serde_json::json!(0));
                        obj.insert("content".to_string(), serde_json::json!(content));
                    });
                    actual_msg_type = 0;
                    let data = self.post_qq_message(&endpoint, body).await?;
                    let mut states = self.c2c_stream_state.lock().await;
                    states.entry(chat_id.to_string()).or_default().msg_type = Some(0);
                    data
                } else {
                    return Err(err);
                }
            }
            Err(err) => return Err(err),
        };

        let mut states = self.c2c_stream_state.lock().await;
        let state = states.entry(chat_id.to_string()).or_default();
        if final_chunk {
            *state = C2cStreamState::default();
        } else {
            state.msg_type = Some(actual_msg_type);
            state.id = data
                .get("id")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .or_else(|| Some(uuid::Uuid::new_v4().to_string()));
            state.index = state.index.saturating_add(1);
            state.active = true;
        }
        drop(states);
        if final_chunk {
            self.clear_stream_progress_state(chat_id).await;
            if !self.config.notify_on_stream_end {
                self.clear_metadata_state(chat_id).await;
            }
        }
        Ok(true)
    }

    async fn send_progress_card(
        &self,
        chat_id: &str,
        lines: &[String],
    ) -> Result<(), GatewayError> {
        if !self.config.progress_coalesce || lines.is_empty() {
            return Ok(());
        }

        let mut state_map = self.progress_state.lock().await;
        let state = state_map.entry(chat_id.to_string()).or_default();
        if state.sent_count >= self.config.max_progress_messages {
            return Ok(());
        }

        let mut fresh = Vec::new();
        for line in lines.iter().skip(state.last_sent_idx) {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if state.last_line.as_deref() == Some(trimmed) {
                state.last_sent_idx += 1;
                continue;
            }
            fresh.push(trimmed.to_string());
            state.last_line = Some(trimmed.to_string());
            state.last_sent_idx += 1;
        }
        if fresh.is_empty() {
            return Ok(());
        }
        state.sent_count += 1;
        drop(state_map);

        let body = format!("**Progress**\n{}", fresh.join("\n"));
        self.send_text_inner(chat_id, &body, false, false).await
    }

    async fn send_stream_end_notice(&self, chat_id: &str) -> Result<(), GatewayError> {
        if !self.config.notify_on_stream_end {
            return Ok(());
        }

        let now = Instant::now();
        let mut state = self.notice_state.lock().await;
        if state
            .last_sent
            .map(|last| now.duration_since(last) < Duration::from_secs(3))
            .unwrap_or(false)
        {
            return Ok(());
        }
        state
            .recent_sent
            .retain(|t| now.duration_since(*t) < Duration::from_secs(300));
        if state.recent_sent.len() >= 3 {
            return Ok(());
        }
        state.last_sent = Some(now);
        state.recent_sent.push(now);
        drop(state);

        let footer = self.format_metadata_footer(chat_id).await;
        let line = if footer.is_empty() {
            "completed".to_string()
        } else {
            format!(
                "completed {}",
                footer.replace('\n', " ").replace("---", "").trim()
            )
        };
        self.send_text_inner(chat_id, &line, false, true).await
    }

    async fn maintenance_prune(&self) {
        // Keep the tiny maps bounded if sessions vanish without a final send.
        const MAX_TRACKED_CHATS: usize = 512;
        let mut progress = self.progress_state.lock().await;
        if progress.len() > MAX_TRACKED_CHATS {
            progress.clear();
        }
        drop(progress);
        let mut stream = self.c2c_stream_state.lock().await;
        if stream.len() > MAX_TRACKED_CHATS {
            stream.clear();
        }
        drop(stream);
        let mut meta = self.turn_metadata.lock().await;
        if meta.len() > MAX_TRACKED_CHATS {
            meta.clear();
        }
    }
}
