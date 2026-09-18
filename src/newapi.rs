//! new-api 管理 API 客户端。
//!
//! 两件事：
//!   1. 鉴权——优先用配置里的 admin_token(Bearer)；没有就用 root 账号登录拿会话 cookie。
//!   2. 渠道——列出/创建（sync 按 key 列表对齐渠道并解析 channel_id）、以及运行期改 priority。
//!
//! 改 priority 仍用「GET 渠道 → 只改 priority → PUT 回」，整体搬运，对版本差异最鲁棒。
//! ⚠️ channel_path / 建渠道字段 / 是否需要 New-Api-User，请用 F12 抓真实请求核实。

use crate::config::{ChannelTemplate, KeyMapping, ModelDiscoveryConfig, NewApiConfig};
use crate::model_catalog::{model_sets_equal, normalize_models_csv, ModelCatalogClient};
use crate::status::{ChannelState, RequestLog};
use std::sync::Arc;
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

/// new-api 列表响应兼容：新版 `data.items[]`，旧版 `data[]`。
fn extract_items(body: &Value) -> Vec<Value> {
    body.get("data")
        .and_then(|d| {
            d.get("items")
                .and_then(|v| v.as_array())
                .or_else(|| d.as_array())
        })
        .cloned()
        .unwrap_or_default()
}

fn s(v: &Value, k: &str) -> String {
    v.get(k)
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string()
}
fn i(v: &Value, k: &str) -> Option<i64> {
    v.get(k).and_then(|x| x.as_i64())
}

#[derive(Clone)]
enum Auth {
    Token(String),
    /// 已登录，会话在 cookie 里；user_id 用于 New-Api-User 头
    Session { user_id: Option<i64> },
    /// 还没登录（admin_token 为空，需调 login）
    Pending,
}

/// 鉴权状态 + 最近一次登录尝试时刻。
/// last_login_attempt 用于重登冷却：login 接口有 CriticalRateLimit（20 次/20 分钟），
/// 会话真正坏掉（如密码被改）时防止每个管理调用各自重登，瞬间烧穿限额把自己锁死。
struct AuthState {
    auth: Auth,
    last_login_attempt: Option<tokio::time::Instant>,
}

/// 建渠道参数：两种格式模板（OpenAI/Anthropic）归一到同一 payload 形状。
/// 字段私有——外部只能经 From 转换拿到，杜绝手拼。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChannelParams<'a> {
    channel_type: i64,
    base_url: &'a str,
    models: &'a str,
    group: &'a str,
    model_discovery: Option<&'a ModelDiscoveryConfig>,
}

impl<'a> From<&'a ChannelTemplate> for ChannelParams<'a> {
    fn from(t: &'a ChannelTemplate) -> Self {
        Self {
            channel_type: t.channel_type,
            base_url: &t.base_url,
            models: &t.models,
            group: &t.group,
            model_discovery: t.model_discovery.as_ref(),
        }
    }
}
#[derive(Debug, PartialEq)]
enum ChannelOp<'a> {
    /// 已存在（**id 优先匹配**：config 显式 channel_id 且存活 → 认 id，容忍渠道改名；
    /// 否则按 name 匹配）。有模板/发现配置时还要对账 models，不能无条件跳过。
    Skip {
        id: i64,
        name: String,
        params: Option<ChannelParams<'a>>,
        owner_name: &'a str,
        key: &'a str,
    },
    /// 需要创建
    Create {
        name: String,
        params: ChannelParams<'a>,
        owner_name: &'a str,
        key: &'a str,
    },
    /// openai 槽缺渠道且未配模板（现有 warn 语义）
    Missing { name: String },
    /// 渠道名匹配**弃用** key → 删除（弃用时删失败/漏删的兜底；
    /// 只碰与 config key 名精确匹配的渠道，绝不碰用户自建渠道）
    Delete { name: String, id: i64 },
}

/// 纯函数：keys × 现有渠道（name→id）× 模板 → 渠道操作计划（不执行、零 IO）。
fn plan_channel_ops<'a>(
    keys: &'a [KeyMapping],
    existing: &HashMap<String, i64>,
    template: Option<&'a ChannelTemplate>,
) -> Vec<ChannelOp<'a>> {
    let mut ops = Vec::new();
    let live_ids: HashSet<i64> = existing.values().copied().collect();
    for k in keys {
        if k.is_deprecated() {
            if let Some(id) = existing.get(&k.name) {
                ops.push(ChannelOp::Delete {
                    name: k.name.clone(),
                    id: *id,
                });
            }
            continue;
        }
        // id 优先：显式 channel_id 且渠道还活着 → 认它（名字漂移容忍，如面板改过名）。
        // id 失效（渠道被删过）→ 自然回落按名匹配，陈旧 id 下次启动对齐时被覆盖落盘。
        let matched = k
            .channel_id
            .filter(|id| live_ids.contains(id))
            .or_else(|| existing.get(&k.name).copied());
        if let Some(id) = matched {
            ops.push(ChannelOp::Skip {
                id,
                name: k.name.clone(),
                params: template.map(Into::into),
                owner_name: &k.name,
                key: &k.zhipu_api_key,
            });
        } else if let Some(t) = template {
            ops.push(ChannelOp::Create {
                name: k.name.clone(),
                params: t.into(),
                owner_name: &k.name,
                key: &k.zhipu_api_key,
            });
        } else {
            ops.push(ChannelOp::Missing { name: k.name.clone() });
        }
    }
    ops
}

/// sync 结果：按 key 名索引其唯一上游渠道 id。
#[derive(Debug, Default)]
pub struct SyncOutcome {
    pub primary: HashMap<String, i64>,
}

/// qt-proxy 中继令牌（F4 逐请求指定渠道的凭据）。真实 key 只在内存持有。
#[derive(Debug, Clone)]
pub struct RelayTokens {
    /// /v1/chat/completions 入口用（保留 new-api 日志 token_name 归因）
    pub openai: String,
    /// /v1/messages 入口用
    pub claude: String,
}

/// 解析 GetTokenKey 响应：rc.20 是 `{success, data:{key}}`；兼容顶层裸 `{key}`。
fn parse_token_key(body: &Value) -> Option<String> {
    body.get("data")
        .and_then(|d| d.get("key"))
        .or_else(|| body.get("key"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// 新渠道最终采用的模型来源。供 AddKey 日志/回执说明是否发生了降级。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelSource {
    Discovered,
    Fallback,
}

impl ModelSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discovered => "discovered",
            Self::Fallback => "fallback",
        }
    }
}

type DiscoveryCache =
    HashMap<(String, ModelDiscoveryConfig), std::result::Result<Vec<String>, String>>;

pub struct NewApiClient {
    client: reqwest::Client,
    catalog: ModelCatalogClient,
    base_url: String,
    channel_path: String,
    /// tokio::Mutex：try_relogin 临界区含登录 .await；客户端整体以 Arc 共享、方法保持 &self
    auth: Arc<tokio::sync::Mutex<AuthState>>,
    root_username: String,
    root_password: String,
    extra_headers: Vec<(String, String)>,
}

impl NewApiClient {
    /// `upstream_base`：内部 new-api 地址（F4 起与「客户端入口」base_url 分离；
    /// cache_pool 开启时管理面直连内部端口——面板可用性与代理解耦、不占代理跳数）
    pub fn new(cfg: &NewApiConfig, upstream_base: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .cookie_store(true)
            .build()
            .context("构建 HTTP client 失败")?;
        let auth = if cfg.admin_token.trim().is_empty() {
            Auth::Pending
        } else {
            Auth::Token(cfg.admin_token.clone())
        };
        Ok(Self {
            catalog: ModelCatalogClient::new(client.clone()),
            client,
            base_url: upstream_base.trim_end_matches('/').to_string(),
            channel_path: cfg.channel_path.clone(),
            auth: Arc::new(tokio::sync::Mutex::new(AuthState {
                auth,
                last_login_attempt: None,
            })),
            root_username: cfg.root_username.clone(),
            root_password: cfg.root_password.clone(),
            extra_headers: cfg
                .extra_headers
                .iter()
                .map(|h| (h.key.clone(), h.value.clone()))
                .collect(),
        })
    }

    /// 按鉴权快照加头（同步、无锁；快照由调用方从 auth 锁取出后传入）
    fn apply_headers(&self, auth: &Auth, mut rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match auth {
            Auth::Token(t) => {
                rb = rb.header("Authorization", format!("Bearer {t}"));
            }
            Auth::Session { user_id } => {
                if let Some(id) = user_id {
                    rb = rb.header("New-Api-User", id.to_string());
                }
            }
            Auth::Pending => {}
        }
        for (k, v) in &self.extra_headers {
            rb = rb.header(k.as_str(), v.as_str());
        }
        rb
    }

    /// 统一的管理 API 请求入口：带鉴权头发送；**Session 模式遇 401 自动重登一次并重试**
    /// （new-api 重启会作废会话，此路径让面板与 priority 下发自愈）。
    /// make 闭包按鉴权快照构造请求——重试时用新快照重建（RequestBuilder 一次性）。
    /// Token 模式 401 = admin_token 配置错误，重试无意义，原样透传由调用方报错。
    async fn send_authed<F>(&self, ctx: &str, make: F) -> Result<reqwest::Response>
    where
        F: Fn(&Auth) -> reqwest::RequestBuilder,
    {
        // reqwest 0.11 的 send() future 是 'static，ctx 需 owned 才能 accompany await
        let ctx = ctx.to_string();
        let snapshot = self.auth.lock().await.auth.clone();
        let resp = make(&snapshot).send().await.context(ctx.clone())?;
        if resp.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(resp);
        }
        let session_mode = matches!(snapshot, Auth::Session { .. });
        if session_mode && self.try_relogin().await.is_ok() {
            info!("管理会话失效（401），已自动重登并重试");
            let fresh = self.auth.lock().await.auth.clone();
            return Ok(make(&fresh).send().await.context(ctx)?);
        }
        Ok(resp)
    }

    /// 会话失效后的原地重登（持有 auth 锁进行）。带 10s 尝试冷却，理由见 AuthState。
    async fn try_relogin(&self) -> Result<()> {
        let mut guard = self.auth.lock().await;
        if let Some(at) = guard.last_login_attempt {
            if at.elapsed() < tokio::time::Duration::from_secs(10) {
                bail!("会话失效且 10 秒内已尝试过重登（冷却中；若持续失败请检查 root 密码）");
            }
        }
        guard.last_login_attempt = Some(tokio::time::Instant::now());
        self.do_login(&mut guard).await
    }

    /// 用 root 账号换会话（cookie 由 cookie_store 自动保存）。调用方须持有 auth 锁；
    /// 本函数不触碰 auth 锁本身（避免自锁死锁）。
    async fn do_login(&self, state: &mut AuthState) -> Result<()> {
        let url = format!("{}/api/user/login", self.base_url);
        let resp = self
            .client
            .post(&url)
            .json(&json!({ "username": self.root_username, "password": self.root_password }))
            .send()
            .await
            .context("登录 new-api 失败")?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        let ok = body.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            bail!(
                "new-api 登录失败: HTTP {} body={}（默认 root/123456，改过密码就填 admin_token 或 root_password）",
                status,
                body
            );
        }
        let user_id = body
            .get("data")
            .and_then(|d| d.get("id"))
            .and_then(|v| v.as_i64());
        info!(user_id = ?user_id, "已登录 new-api（会话模式）");
        state.auth = Auth::Session { user_id };
        Ok(())
    }

    /// 新版 new-api 首启不再自带 root：需先 POST /api/setup 建管理员。幂等——已初始化则跳过。
    async fn ensure_setup(&self) -> Result<()> {
        let url = format!("{}/api/setup", self.base_url);
        let body: Value = self
            .client
            .get(&url)
            .send()
            .await
            .context("查询 new-api setup 状态失败")?
            .json()
            .await
            .unwrap_or(Value::Null);
        let data = body.get("data");
        let status = data
            .and_then(|d| d.get("status"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let root_init = data
            .and_then(|d| d.get("root_init"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if status || root_init {
            return Ok(()); // 已初始化
        }
        info!(user = %self.root_username, "new-api 首启未初始化，创建管理员");
        let payload = json!({
            "username": self.root_username,
            "password": self.root_password,
            "confirmPassword": self.root_password,
            "SelfUseModeEnabled": true,   // 自用网关，关掉多租户计费等检查
            "DemoSiteEnabled": false,
        });
        let resp = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("初始化 new-api 失败")?;
        let st = resp.status();
        let rb: Value = resp.json().await.unwrap_or(Value::Null);
        let ok = rb.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
        if !ok {
            bail!("new-api 初始化失败: HTTP {st} body={rb}（密码需≥8位、用户名≤12）");
        }
        info!("new-api 管理员已创建");
        Ok(())
    }

    /// 确保已鉴权：Token 模式无需动作；Pending 则（必要时先 setup）用 root 登录换会话。
    /// 会话失效的自愈不走这里（authenticate 只在 Pending 时登录），走 send_authed 的 401 重试。
    pub async fn authenticate(&self) -> Result<()> {
        let mut guard = self.auth.lock().await;
        if !matches!(guard.auth, Auth::Pending) {
            return Ok(());
        }
        self.ensure_setup().await?;
        guard.last_login_attempt = Some(tokio::time::Instant::now());
        self.do_login(&mut guard).await
    }

    /// 列出渠道，返回 name → id。兼容 data.items 和 data 直接数组两种结构。
    pub async fn list_channels(&self) -> Result<HashMap<String, i64>> {
        let url = format!("{}{}/?p=0&page_size=100", self.base_url, self.channel_path);
        let body: Value = self
            .send_authed("列出渠道失败", |auth| {
                self.apply_headers(auth, self.client.get(&url))
            })
            .await?
            .json()
            .await
            .context("解析渠道列表失败")?;

        let mut map = HashMap::new();
        for it in extract_items(&body) {
            if let (Some(name), Some(id)) = (
                it.get("name").and_then(|v| v.as_str()),
                it.get("id").and_then(|v| v.as_i64()),
            ) {
                map.insert(name.to_string(), id);
            }
        }
        Ok(map)
    }

    /// 【看板】拉取渠道**完整状态**（status / priority / weight / used_quota / auto_ban）。
    ///
    /// **纯读，零副作用**——已核实 new-api 的 `GetAllChannels` 内无任何写/测试调用。
    /// 首要用途：暴露「渠道被 new-api 自动禁用」这个盲区——我们只改 priority、从不碰 status，
    /// 渠道一旦被禁，priority=100 也不会有流量。
    pub async fn list_channel_states(&self) -> Result<Vec<ChannelState>> {
        let url = format!("{}{}/?p=0&page_size=100", self.base_url, self.channel_path);
        let body: Value = self
            .send_authed("拉取渠道状态失败", |auth| {
                self.apply_headers(auth, self.client.get(&url))
            })
            .await?
            .json()
            .await
            .context("解析渠道状态失败")?;

        let mut out: Vec<ChannelState> = extract_items(&body)
            .iter()
            .filter_map(|it| {
                let id = i(it, "id")?;
                let status_raw = i(it, "status").unwrap_or(0);
                Some(ChannelState {
                    id,
                    name: s(it, "name"),
                    enabled: status_raw == 1,
                    status_raw,
                    priority: i(it, "priority"),
                    weight: i(it, "weight"),
                    used_quota: i(it, "used_quota").unwrap_or(0),
                    auto_ban: i(it, "auto_ban"),
                    models: s(it, "models"),
                    group: s(it, "group"),
                })
            })
            .collect();
        out.sort_by_key(|c| c.id); // 顺序稳定，看板不跳动
        Ok(out)
    }

    /// 【看板】最近 n 条**真实请求**。纯读（`/api/log/` handler 无写操作）。
    ///
    /// 过滤依据用**字段语义**（model_name 非空 且 channel != 0）而非 `type` 枚举值——
    /// 后者随 new-api 版本可能变，前者稳。日志里混有登录等系统条目（实测 type=7）。
    pub async fn recent_logs(&self, n: usize) -> Result<Vec<RequestLog>> {
        let url = format!("{}/api/log/?p=0&page_size={n}", self.base_url);
        let body: Value = self
            .send_authed("拉取请求日志失败", |auth| {
                self.apply_headers(auth, self.client.get(&url))
            })
            .await?
            .json()
            .await
            .context("解析请求日志失败")?;

        let mut out: Vec<RequestLog> = extract_items(&body)
            .iter()
            .filter_map(|it| {
                let model_name = s(it, "model_name");
                let channel = i(it, "channel").unwrap_or(0);
                if model_name.is_empty() || channel == 0 {
                    return None; // 系统日志（登录等），非真实请求
                }
                Some(RequestLog {
                    created_at: i(it, "created_at").unwrap_or(0),
                    channel,
                    channel_name: s(it, "channel_name"),
                    model_name,
                    prompt_tokens: i(it, "prompt_tokens").unwrap_or(0),
                    completion_tokens: i(it, "completion_tokens").unwrap_or(0),
                    quota: i(it, "quota").unwrap_or(0),
                    use_time: i(it, "use_time").unwrap_or(0),
                    is_stream: it.get("is_stream").and_then(|v| v.as_bool()).unwrap_or(false),
                    token_name: s(it, "token_name"),
                })
            })
            .collect();
        // 自行排序，不依赖服务端返回顺序
        out.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(out)
    }

    fn fallback_models(p: &ChannelParams<'_>) -> Result<String> {
        let models = normalize_models_csv(p.models);
        anyhow::ensure!(
            !models.is_empty(),
            "渠道模板的 fallback models 为空，无法创建渠道"
        );
        Ok(models.join(","))
    }

    /// 单次 sync 的发现缓存。key 只以非敏感的 owner name 作为缓存身份；真实 API key
    /// 不进入 HashMap key、日志或错误。同一 key + 发现配置只请求一次上游。
    async fn discover_cached(
        &self,
        owner_name: &str,
        key: &str,
        cfg: &ModelDiscoveryConfig,
        cache: &mut DiscoveryCache,
    ) -> std::result::Result<Vec<String>, String> {
        let cache_key = (owner_name.to_string(), cfg.clone());
        if let Some(result) = cache.get(&cache_key) {
            return result.clone();
        }
        let result = self
            .catalog
            .discover(cfg, key)
            .await
            .map_err(|e| e.to_string());
        cache.insert(cache_key, result.clone());
        result
    }

    async fn resolve_models_for_create(
        &self,
        owner_name: &str,
        key: &str,
        p: &ChannelParams<'_>,
        cache: &mut DiscoveryCache,
    ) -> Result<(String, ModelSource)> {
        if let Some(cfg) = p.model_discovery {
            match self.discover_cached(owner_name, key, cfg, cache).await {
                Ok(models) => return Ok((models.join(","), ModelSource::Discovered)),
                Err(error) => warn!(
                    owner = owner_name,
                    url = %cfg.url,
                    error,
                    "模型目录探测失败，新渠道降级使用配置 fallback"
                ),
            }
        }
        Ok((Self::fallback_models(p)?, ModelSource::Fallback))
    }

    /// AddKey 使用的入口：在请求发生时即时发现，不复用进程启动时的静态模型字符串。
    pub async fn create_channel_resolving_models(
        &self,
        name: &str,
        owner_name: &str,
        key: &str,
        priority: i64,
        p: &ChannelParams<'_>,
    ) -> Result<ModelSource> {
        let mut cache = DiscoveryCache::new();
        let (models, source) = self
            .resolve_models_for_create(owner_name, key, p, &mut cache)
            .await?;
        self.create_channel_with_models(name, key, priority, p, &models)
            .await?;
        Ok(source)
    }

    /// 创建一个渠道（把 name/key/priority 与已经解析好的 models 合并进模板参数 POST）。
    async fn create_channel_with_models(
        &self,
        name: &str,
        key: &str,
        priority: i64,
        p: &ChannelParams<'_>,
        models: &str,
    ) -> Result<()> {
        anyhow::ensure!(!normalize_models_csv(models).is_empty(), "渠道 models 不能为空");
        // new-api 的 AddChannel 期望 { mode, channel:{...} }，channel 是指针，缺了会 nil-panic。
        let payload = json!({
            "mode": "single",
            "channel": {
                "name": name,
                "type": p.channel_type,
                "key": key,
                "base_url": p.base_url,
                "models": models,
                "group": p.group,
                "priority": priority,
                "weight": 0,
                "status": 1,
            }
        });
        let url = format!("{}{}", self.base_url, self.channel_path);
        let resp = self
            .send_authed("创建渠道失败", |auth| {
                self.apply_headers(auth, self.client.post(&url))
                    .json(&payload)
            })
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        let ok = body
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(status.is_success());
        if !ok {
            bail!("创建渠道 {name} 失败: HTTP {status} body={body}");
        }
        Ok(())
    }

    /// 已有渠道只对账 models。集合相同零写；漂移时整体搬运渠道对象，只替换 models，
    /// 并回读验证模型与 priority/group/status 三个调度不变量。
    async fn ensure_channel_models(&self, id: i64, name: &str, desired: &str) -> Result<bool> {
        anyhow::ensure!(
            !normalize_models_csv(desired).is_empty(),
            "渠道 {name} 的目标 models 为空"
        );
        let mut channel = self.get_channel(id).await?;
        let current = s(&channel, "models");
        if model_sets_equal(&current, desired) {
            return Ok(false);
        }

        let before_priority = i(&channel, "priority");
        let before_status = i(&channel, "status");
        let before_group = s(&channel, "group");
        let obj = channel
            .as_object_mut()
            .with_context(|| format!("渠道 {id} 返回的不是 JSON 对象"))?;
        obj.insert("models".to_string(), Value::from(desired));
        // new-api UpdateChannel 拒绝带 status；GET 返回的空 key 表示保留原 key。
        obj.remove("status");

        let url = format!("{}{}", self.base_url, self.channel_path);
        let resp = self
            .send_authed("更新渠道失败", |auth| {
                self.apply_headers(auth, self.client.put(&url))
                    .json(&channel)
            })
            .await
            .with_context(|| format!("更新渠道 {name} models 失败"))?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        let ok = body
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(status.is_success());
        if !ok {
            bail!("更新渠道 {name} models 失败: HTTP {status} body={body}");
        }

        let after = self.get_channel(id).await?;
        anyhow::ensure!(
            model_sets_equal(&s(&after, "models"), desired),
            "渠道 {name} models 更新后回读不一致"
        );
        anyhow::ensure!(
            i(&after, "priority") == before_priority
                && i(&after, "status") == before_status
                && s(&after, "group") == before_group,
            "渠道 {name} models 更新意外改变了 priority/status/group"
        );
        Ok(true)
    }

    /// 按 key 列表对齐唯一的上游渠道：缺失则按模板创建；存在时按 `/models` 对账
    /// （**id 优先匹配**，容忍渠道改名）；**弃用 key 的残留渠道列为 Delete 删除**。
    /// Claude 是 NewAPI 已支持的下游请求格式，复用同一渠道与访问 key，不在这里复制渠道。
    pub async fn sync_channels(
        &self,
        keys: &[KeyMapping],
        template: Option<&ChannelTemplate>,
        standby_priority: i64,
    ) -> Result<SyncOutcome> {
        let existing = self.list_channels().await?;
        let plan = plan_channel_ops(keys, &existing, template);

        let mut created = false;
        let mut discovery_cache = DiscoveryCache::new();
        for op in &plan {
            match op {
                ChannelOp::Skip {
                    id,
                    name,
                    params,
                    owner_name,
                    key,
                } => {
                    let Some(params) = params else {
                        info!(name = %name, "渠道已存在；未配模板，跳过模型对账");
                        continue;
                    };
                    let Some(discovery) = params.model_discovery else {
                        info!(name = %name, "渠道已存在；未开启模型发现，跳过模型对账");
                        continue;
                    };
                    match self
                        .discover_cached(owner_name, key, discovery, &mut discovery_cache)
                        .await
                    {
                        Ok(models) => {
                            let desired = models.join(",");
                            match self.ensure_channel_models(*id, name, &desired).await {
                                Ok(true) => info!(name = %name, count = models.len(), "已按上游 /models 更新渠道模型"),
                                Ok(false) => info!(name = %name, count = models.len(), "渠道模型已与上游一致"),
                                Err(e) => return Err(e),
                            }
                        }
                        Err(error) => warn!(
                            name = %name,
                            url = %discovery.url,
                            error,
                            "模型目录探测失败，已有渠道 models 保持不变"
                        ),
                    }
                }
                ChannelOp::Missing { name } => warn!(
                    name = %name,
                    "渠道不存在且未配 channel_template，无法自动创建"
                ),
                ChannelOp::Create {
                    name,
                    params,
                    owner_name,
                    key,
                } => {
                    let result = match self
                        .resolve_models_for_create(
                            owner_name,
                            key,
                            params,
                            &mut discovery_cache,
                        )
                        .await
                    {
                        Ok((models, source)) => {
                            info!(name = %name, models_source = source.as_str(), "创建渠道");
                            self.create_channel_with_models(
                                name,
                                key,
                                standby_priority,
                                params,
                                &models,
                            )
                            .await
                        }
                        Err(e) => Err(e),
                    };
                    match result {
                        Ok(()) => created = true,
                        Err(e) => return Err(e),
                    }
                }
                ChannelOp::Delete { name, id } => {
                    // 弃用残留的兜底删除：失败只 warn（下次启动再试），不阻断整体对齐
                    match self.delete_channel(*id).await {
                        Ok(()) => info!(name = %name, channel_id = id, "已删除弃用 key 的残留渠道"),
                        Err(e) => warn!(name = %name, channel_id = id, error = %e, "删除弃用残留渠道失败（下次启动重试）"),
                    }
                }
            }
        }

        // 有新建就重新拉一遍，拿到新 id；否则用第一遍的
        let latest = if created {
            self.list_channels().await?
        } else {
            existing
        };

        let mut out = SyncOutcome::default();
        for k in keys.iter().filter(|k| !k.is_deprecated()) {
            if let Some(id) = latest.get(&k.name) {
                out.primary.insert(k.name.clone(), *id);
            }
        }
        Ok(out)
    }

    /// 【看板】用量统计（new-api 自己按**小时**聚合好的 `quota_data`）。纯读。
    /// 返回 (model, hour_epoch_sec, tokens, count)。供时序曲线 + 按模型汇总两用。
    ///
    /// ⚠️ 该接口 `Group("model_name, created_at")` —— **不带渠道维度**（new-api 从不暴露按渠道的用量）。
    pub async fn usage_data(&self, start: i64, end: i64) -> Result<Vec<(String, i64, i64, i64)>> {
        let url = format!(
            "{}/api/data/?start_timestamp={start}&end_timestamp={end}",
            self.base_url
        );
        let body: Value = self
            .send_authed("拉取用量统计失败", |auth| {
                self.apply_headers(auth, self.client.get(&url))
            })
            .await?
            .json()
            .await
            .context("解析用量统计失败")?;
        let items = body
            .get("data")
            .and_then(|d| d.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(items
            .iter()
            .filter_map(|it| {
                let m = s(it, "model_name");
                if m.is_empty() {
                    return None;
                }
                Some((
                    m,
                    i(it, "created_at").unwrap_or(0),
                    i(it, "token_used").unwrap_or(0),
                    i(it, "count").unwrap_or(0),
                ))
            })
            .collect())
    }

    /// 【看板】读 new-api 的**内部虚拟余额**（当前登录用户）。纯读。
    ///
    /// ⚠️ new-api 按「按量付费倍率」给包月编码套餐虚构记账，余额见底会**直接挡住转发**
    /// （报「预扣费额度失败」），跟智谱额度毫无关系。看板据此在见底前告警。
    pub async fn user_quota(&self) -> Result<i64> {
        let url = format!("{}/api/user/self", self.base_url);
        let body: Value = self
            .send_authed("拉取 new-api 用户余额失败", |auth| {
                self.apply_headers(auth, self.client.get(&url))
            })
            .await?
            .json()
            .await
            .context("解析用户余额失败")?;
        body.get("data")
            .and_then(|d| d.get("quota"))
            .and_then(|v| v.as_i64())
            .context("响应缺少 data.quota")
    }

    /// GET /api/channel/{id} → 渠道对象（从 data 取出）
    pub async fn get_channel(&self, id: i64) -> Result<Value> {
        let url = format!("{}{}/{}", self.base_url, self.channel_path, id);
        let body: Value = self
            .send_authed("获取渠道失败", |auth| {
                self.apply_headers(auth, self.client.get(&url))
            })
            .await?
            .json()
            .await
            .context("解析渠道响应失败")?;
        body.get("data")
            .cloned()
            .context("渠道响应缺少 data 字段（请用 F12 核实实际结构）")
    }

    /// 取渠道 → 改某整数字段 → PUT 回。整体搬运，只动这一个字段。
    async fn set_channel_field(&self, id: i64, field: &str, value: i64) -> Result<()> {
        let mut channel = self.get_channel(id).await?;
        match channel.as_object_mut() {
            Some(obj) => {
                obj.insert(field.to_string(), Value::from(value));
                // new-api 的 UpdateChannel 明确拒绝请求体里带 status（判为 Invalid parameters），
                // 必须剔除。GET 回来的 key 是空串，UpdateChannel 对空 key 会保留原值，安全。
                obj.remove("status");
            }
            None => bail!("渠道 {id} 返回的不是 JSON 对象"),
        }
        let url = format!("{}{}", self.base_url, self.channel_path);
        let resp = self
            .send_authed("更新渠道失败", |auth| {
                self.apply_headers(auth, self.client.put(&url)).json(&channel)
            })
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        let ok = body
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(status.is_success());
        if !ok {
            bail!("更新渠道 {id} 字段 {field} 失败: HTTP {status} body={body}");
        }
        Ok(())
    }

    /// 设置渠道 priority——本工具「钉住单把活动 key」的唯一运行期杠杆。
    pub async fn set_channel_priority(&self, id: i64, priority: i64) -> Result<()> {
        self.set_channel_field(id, "priority", priority).await
    }

    /// 删除渠道（DELETE {channel_path}/{id}）。弃用 key 用——渠道随弃用删除；
    /// new-api 会级联清 abilities，但**不动**日志/用量数据（历史留存）。
    pub async fn delete_channel(&self, id: i64) -> Result<()> {
        let url = format!("{}{}/{}", self.base_url, self.channel_path, id);
        let resp = self
            .send_authed("删除渠道失败", |auth| {
                self.apply_headers(auth, self.client.delete(&url))
            })
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        let ok = body
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(status.is_success());
        if !ok {
            bail!("删除渠道 {id} 失败: HTTP {status} body={body}");
        }
        Ok(())
    }

    /// 取渠道 → 改某字符串字段 → PUT 回（与 int 版同一「GET→只改→remove status→PUT」模式）。
    async fn set_channel_field_str(&self, id: i64, field: &str, value: &str) -> Result<()> {
        let mut channel = self.get_channel(id).await?;
        match channel.as_object_mut() {
            Some(obj) => {
                obj.insert(field.to_string(), Value::from(value));
                obj.remove("status"); // UpdateChannel 拒绝带 status（同 int 版）
            }
            None => bail!("渠道 {id} 返回的不是 JSON 对象"),
        }
        let url = format!("{}{}", self.base_url, self.channel_path);
        let resp = self
            .send_authed("更新渠道失败", |auth| {
                self.apply_headers(auth, self.client.put(&url))
                    .json(&channel)
            })
            .await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        let ok = body
            .get("success")
            .and_then(|v| v.as_bool())
            .unwrap_or(status.is_success());
        if !ok {
            bail!("更新渠道 {id} 字段 {field} 失败: HTTP {status} body={body}");
        }
        Ok(())
    }

    /// 渠道改名（面板改 key name 时联动；name 是 config↔渠道的匹配键之一）。
    pub async fn rename_channel(&self, id: i64, new_name: &str) -> Result<()> {
        self.set_channel_field_str(id, "name", new_name).await
    }

    /// 手动模型重对账：用这把 key 的上游 `/models` 刷新渠道 models
    /// （启动 sync 与 AddKey 之外的第三入口，面板按钮用）。
    /// 探测失败就报错——**不用模板 fallback 覆盖**（可能把好渠道冲成陈旧静态表）。
    pub async fn reconcile_channel_models(
        &self,
        id: i64,
        name: &str,
        zhipu_key: &str,
        p: &ChannelParams<'_>,
    ) -> Result<bool> {
        let discovery = p
            .model_discovery
            .ok_or_else(|| anyhow::anyhow!("模板未开启 model_discovery，无从对账"))?;
        let models = self
            .discover_cached(name, zhipu_key, discovery, &mut DiscoveryCache::new())
            .await
            .map_err(anyhow::Error::msg)?;
        let desired = models.join(",");
        self.ensure_channel_models(id, name, &desired).await
    }

    // ——— qt-proxy 中继令牌（F4 缓存池代理的逐请求指定渠道底座）———
    // 机制：`Authorization: Bearer sk-<key>-<channelId>`（new-api 原生，要求令牌所属
    // 用户 role≥10 即管理员）。两把按入口路径分开，保留 new-api 日志里的 token_name 归因。
    // ⚠️ 真实 key 只在内存持有，绝不落日志/config（鉴权红线）。

    /// 列出令牌 (name, id)。列表里的 key 是打码的，真实值另走 /api/token/:id/key。
    async fn list_tokens(&self) -> Result<Vec<(String, i64)>> {
        let url = format!("{}/api/token/?p=0&page_size=100", self.base_url);
        let body: Value = self
            .send_authed("列出令牌失败", |auth| {
                self.apply_headers(auth, self.client.get(&url))
            })
            .await?
            .json()
            .await
            .context("解析令牌列表失败")?;
        Ok(extract_items(&body)
            .iter()
            .filter_map(|it| {
                Some((
                    s(it, "name"),
                    it.get("id").and_then(|v| v.as_i64())?,
                ))
            })
            .collect())
    }

    /// 幂等保证一把中继令牌存在，返回其真实 key。
    async fn ensure_relay_token(&self, name: &str) -> Result<String> {
        let id = match self.list_tokens().await?.iter().find(|(n, _)| n == name) {
            Some((_, id)) => *id,
            None => {
                // 创建（注意集合端点的尾斜杠）。unlimited 额度绕开 token 级额度闸。
                let url = format!("{}/api/token/", self.base_url);
                let payload = json!({
                    "name": name,
                    "unlimited_quota": true,
                    "expired_time": -1,
                    "remain_quota": 0,
                    "group": "",
                });
                let resp = self
                    .send_authed("创建中继令牌失败", |auth| {
                        self.apply_headers(auth, self.client.post(&url))
                            .json(&payload)
                    })
                    .await?;
                let status = resp.status();
                let body: Value = resp.json().await.unwrap_or(Value::Null);
                if !body.get("success").and_then(|v| v.as_bool()).unwrap_or(false) {
                    bail!("创建中继令牌 {name} 失败: HTTP {status} body={body}");
                }
                self.list_tokens()
                    .await?
                    .iter()
                    .find(|(n, _)| n == name)
                    .map(|(_, id)| *id)
                    .ok_or_else(|| anyhow::anyhow!("中继令牌 {name} 已创建但列不出来"))?
            }
        };
        // 真实 key：POST /api/token/:id/key（列表/详情里都是打码的）。
        // rc.20 的 GetTokenKey 走 common.ApiSuccess 包装 → {success, data:{key}}；
        // 兼容顶层裸 key 的旧/异构形态（PR review #1：只读顶层会让令牌永远加载失败，
        // 整个 F4b 路由静默退化为透传）
        let url = format!("{}/api/token/{id}/key", self.base_url);
        let resp = self
            .send_authed("取中继令牌 key 失败", |auth| {
                self.apply_headers(auth, self.client.post(&url))
            })
            .await?;
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !body.get("success").and_then(|v| v.as_bool()).unwrap_or(true) {
            bail!("取中继令牌 {name} 的 key 失败: {body}");
        }
        parse_token_key(&body).context("中继令牌 key 响应缺少 key 字段")
    }

    /// 确保两把 qt-proxy 中继令牌（qt-proxy-openai / qt-proxy-claude）就绪。
    /// 启动对齐时调用；F4 代理用 `sk-<key>-<channelId>` 逐请求指定渠道。
    pub async fn ensure_relay_tokens(&self) -> Result<RelayTokens> {
        let openai = self.ensure_relay_token("qt-proxy-openai").await?;
        let claude = self.ensure_relay_token("qt-proxy-claude").await?;
        info!("qt-proxy 中继令牌就绪（openai/claude 各一把，unlimited；真实 key 仅内存持有）");
        Ok(RelayTokens { openai, claude })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> KeyMapping {
        KeyMapping {
            name: name.into(),
            zhipu_api_key: format!("k-{name}"),
            channel_id: None,
            note: String::new(),
            deprecated: None,
            quota_headers: Vec::new(),
        }
    }

    fn openai_tpl() -> ChannelTemplate {
        ChannelTemplate {
            channel_type: 8,
            base_url: "https://open.bigmodel.cn/api/coding/paas/v4/chat/completions".into(),
            models: "glm-5.2".into(),
            group: "default".into(),
            model_discovery: None,
        }
    }

    fn names(ops: &[ChannelOp]) -> Vec<String> {
        ops.iter()
            .map(|o| match o {
                ChannelOp::Skip { name, .. }
                | ChannelOp::Create { name, .. }
                | ChannelOp::Missing { name }
                | ChannelOp::Delete { name, .. } => name.clone(),
            })
            .collect()
    }

    /// name→id 映射；id 按 enumerate 编（每名单调唯一即可）
    fn existing(list: &[&str]) -> HashMap<String, i64> {
        list.iter()
            .enumerate()
            .map(|(i, s)| (s.to_string(), (i + 1) as i64))
            .collect()
    }

    /// GetTokenKey 响应解析：rc.20 包 data 层，兼容顶层裸 key（PR review #1）
    #[test]
    fn 令牌key解析_data包裹与顶层裸key两形态() {
        let wrapped = serde_json::json!({"success": true, "data": {"key": "sk-abc"}});
        assert_eq!(parse_token_key(&wrapped).as_deref(), Some("sk-abc"));
        let bare = serde_json::json!({"success": true, "key": "sk-xyz"});
        assert_eq!(parse_token_key(&bare).as_deref(), Some("sk-xyz"));
        assert_eq!(parse_token_key(&serde_json::json!({"success": true})), None);
    }

    #[test]
    fn 全新建_每把key只创建一个上游渠道() {
        let keys = [key("zhipu-1")];
        let template = openai_tpl();
        let ops = plan_channel_ops(&keys, &existing(&[]), Some(&template));
        assert_eq!(names(&ops), vec!["zhipu-1"]);
        let ChannelOp::Create { key, .. } = &ops[0] else { unreachable!() };
        assert_eq!(*key, "k-zhipu-1");
    }

    #[test]
    fn 已存在_进入模型对账而不重复创建() {
        let keys = [key("zhipu-1")];
        let template = openai_tpl();
        let ops = plan_channel_ops(&keys, &existing(&["zhipu-1"]), Some(&template));
        let ChannelOp::Skip { params, key, owner_name, .. } = &ops[0] else { unreachable!() };
        assert!(params.is_some(), "配了模板的存量渠道必须进入模型对账");
        assert_eq!(*key, "k-zhipu-1");
        assert_eq!(*owner_name, "zhipu-1");
    }

    #[test]
    fn openai无模板且渠道缺_产missing() {
        let keys = [key("zhipu-1")];
        let ops = plan_channel_ops(&keys, &existing(&[]), None);
        assert_eq!(
            ops,
            vec![ChannelOp::Missing { name: "zhipu-1".into() }]
        );
    }

    #[test]
    fn 混合_key1_skip_key2_create() {
        let keys = [key("zhipu-1"), key("zhipu-2")];
        let template = openai_tpl();
        let ops = plan_channel_ops(&keys, &existing(&["zhipu-1"]), Some(&template));
        assert_eq!(names(&ops), vec!["zhipu-1", "zhipu-2"]);
        assert!(matches!(ops[0], ChannelOp::Skip { .. }));
        assert!(matches!(ops[1], ChannelOp::Create { .. }));
    }

    /// id 优先：config 显式 channel_id 且存活 → 认 id（渠道已改名也不建新的）
    #[test]
    fn 显式channel_id存活_按id匹配_忽略名字漂移() {
        let mut k = key("zhipu-1");
        k.channel_id = Some(7);
        // 渠道 7 现在叫别的名字（面板改过名），"zhipu-1" 这个名字无人用
        let ex = HashMap::from([("renamed-elsewhere".to_string(), 7i64)]);
        let ks = [k];
        let tpl = openai_tpl();
        let ops = plan_channel_ops(&ks, &ex, Some(&tpl));
        assert!(matches!(ops[0], ChannelOp::Skip { id: 7, .. }));
    }

    /// id 失效（渠道被删）→ 回落按名匹配；陈旧 id 不至于让 key 变 Missing
    #[test]
    fn 陈旧channel_id_回落按名匹配() {
        let mut k = key("zhipu-1");
        k.channel_id = Some(99); // 不存在
        let ex = HashMap::from([("zhipu-1".to_string(), 3i64)]);
        let ks = [k];
        let tpl = openai_tpl();
        let ops = plan_channel_ops(&ks, &ex, Some(&tpl));
        assert!(matches!(ops[0], ChannelOp::Skip { id: 3, .. }));
    }

    /// 弃用 key：同名渠道列为 Delete；无渠道则不动。用户自建渠道永不进 ops。
    #[test]
    fn 弃用key_同名渠道删除_无渠道不产op() {
        let mk = || {
            let mut k = key("zhipu-1");
            k.deprecated = Some(true);
            k
        };
        let tpl = openai_tpl();
        let ex = HashMap::from([
            ("zhipu-1".to_string(), 5i64),
            ("user-made".to_string(), 6i64),
        ]);
        let ks = [mk()];
        let ops = plan_channel_ops(&ks, &ex, Some(&tpl));
        assert_eq!(
            ops,
            vec![ChannelOp::Delete {
                name: "zhipu-1".into(),
                id: 5
            }]
        );

        let ks2 = [mk()];
        let ops = plan_channel_ops(&ks2, &existing(&["user-made"]), Some(&tpl));
        assert!(ops.is_empty(), "无同名渠道时弃用 key 不产任何 op");
    }
}
