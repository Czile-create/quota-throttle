//! 状态查询接口 + HTML 看板。
//!
//! orchestrator 每轮 tick 末尾把「每把 key 的 5h/周 用量、档位、priority、当前活动 key、
//! new-api 健康」整体写入共享快照；本模块把它暴露为：
//!   GET /api/status              → JSON（opencode 插件等外部消费者也读这个）
//!   GET /api/usage?start=&end=   → 任意区间的用量（按需查 new-api，不进快照）
//!   GET /                        → 自包含 HTML 看板（内联 CSS/JS，无外部 CDN）
//!
//! **两条刷新路径，节奏各自匹配数据的真实变化频率**：
//!   · 快照（卡片 / rpm / 请求流水 / 近 24h 曲线）→ 5 秒，走 /api/status
//!   · 历史区间（近 30 天 / 某一天）→ **5 分钟**，走 /api/usage
//!     5 分钟 = new-api 把 quota_data 落库的节奏（`DataExportInterval`，见设计文档 §0.3）。
//!     刷得再勤也拿不到更新的数——源头就是 5 分钟才写一次。
//!   · 纯过去的区间（非今天）数据已不再变 → 前端拉一次冻住，根本不刷。
//!
//! 设计原则：**看板是附属，绝不拖垮主循环**——bind 失败只降级记 error，切换循环照常跑。

use crate::config::NewKeySpec;
use crate::newapi::NewApiClient;
use crate::orchestrator::Command;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Default)]
pub struct KeyStatus {
    pub name: String,
    /// 人类可读备注（如持有人名字），只显示；空则前端不渲染
    pub note: String,
    pub channel_id: i64,
    /// 5 小时窗口已用%；None = 本轮未取到
    pub five_hour_pct: Option<f64>,
    /// 每周窗口已用%
    pub weekly_pct: Option<f64>,
    /// 窗口重置时间（epoch ms）
    pub five_hour_reset: Option<i64>,
    pub weekly_reset: Option<i64>,
    /// 监控窗口取最大（决策依据）；None = 查询失败（**不是 0%**）
    pub max_pct: Option<f64>,
    /// keyrot-1：是否临期（周窗口即将重置且还有余量）。与决策同源，仅供看板/日志。
    #[serde(default)]
    pub imminent: bool,
    /// "active" | "standby" | "exhausted" | "unknown"
    pub tier: String,
    /// 本工具最近成功下发的 priority
    pub priority: Option<i64>,
    /// 查询失败原因
    pub error: Option<String>,
    /// selector 预填（编辑表单用；从 quota_headers 反解，空串 = 未配）
    #[serde(default)]
    pub org: String,
    #[serde(default)]
    pub project: String,
}

/// new-api 侧的渠道实况。补的是我们看不见的盲区：
/// 本工具只改 priority、**从不碰 status**——渠道一旦被 new-api 自动禁用，
/// priority=100 也不会有流量，而我们毫无察觉。
#[derive(Debug, Clone, Serialize, Default)]
pub struct ChannelState {
    pub id: i64,
    pub name: String,
    /// status == 1
    pub enabled: bool,
    /// 原始 status（1=启用；2=手动禁用；3=自动禁用 …）
    pub status_raw: i64,
    /// new-api 中的**权威** priority，用于和本工具下发值对账
    pub priority: Option<i64>,
    pub weight: Option<i64>,
    /// new-api 内部虚构计费额度（按「按量付费倍率」记账）。**对包月编码套餐无意义**，
    /// 故看板不展示；保留在 JSON 里仅供参考。
    /// 「这把 key 消耗了多少」看 key 卡片的智谱 5h/周 百分比（权威）；
    /// 「流量怎么分布」看请求流水表。new-api 不提供按渠道的 token 汇总。
    pub used_quota: i64,
    pub auto_ban: Option<i64>,
    pub models: String,
    pub group: String,
}

/// 一条真实请求（仅用于推导「最后一次请求」，不进快照）。
#[derive(Debug, Clone, Serialize, Default)]
pub struct RequestLog {
    /// epoch **秒**（注意与 snapshot.updated_at 的毫秒不同单位）
    pub created_at: i64,
    /// 实际服务的渠道 id
    pub channel: i64,
    pub channel_name: String,
    pub model_name: String,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub quota: i64,
    /// 耗时（秒）
    pub use_time: i64,
    pub is_stream: bool,
    /// 哪个调用令牌发起的（如 "opencode"）
    pub token_name: String,
}

/// 某渠道的实时指标 —— 回答「有没有连上 / 烧多快」。
///
/// ⚠️ **必须独立于 KeyStatus**：决策循环每轮整体重建 `keys`，若把这些字段塞进 KeyStatus
/// 会被冲掉。两个循环写的字段必须严格不相交。
#[derive(Debug, Clone, Serialize, Default)]
pub struct LiveMetric {
    pub channel_id: i64,
    /// 最近 **60 秒**该渠道的请求数（窗口由 new-api 服务端固定）。> 0 ⟺ 流量正在走这把 key
    pub rpm: i64,
    /// 最近 60 秒该渠道的 tokens
    pub tpm: i64,
    /// 该渠道最后一次请求的时间（epoch 秒）
    pub last_request_at: Option<i64>,
    pub last_request_model: Option<String>,
}

/// 时序点（new-api 按小时聚合好的 token 用量）
#[derive(Debug, Clone, Serialize, Default)]
pub struct UsagePoint {
    /// 小时桶起点（epoch 秒）
    pub hour: i64,
    pub tokens: i64,
    pub count: i64,
}

/// 按模型的用量
#[derive(Debug, Clone, Serialize, Default)]
pub struct ModelUsage {
    pub model: String,
    pub tokens: i64,
    pub count: i64,
}

/// 高峰时段状态（智谱的概念：高峰期同一请求烧掉的额度更多）。
#[derive(Debug, Clone, Serialize, Default)]
pub struct PeakInfo {
    /// 配了 [peak] 才为 true；false 时看板不显示这块
    pub enabled: bool,
    pub is_peak: bool,
    /// 人类可读的窗口，如 "14:00–18:00 (UTC+8)"
    pub window: String,
    /// 下次切换时刻（epoch ms）。前端据此做倒计时
    pub next_change_at: i64,
    pub models: Vec<PeakModel>,
    /// 备注（如「非高峰 1x 是限时福利，到 9 月底」）
    pub note: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PeakModel {
    pub model: String,
    /// 当前生效的系数
    pub current: f64,
    pub peak: f64,
    pub off_peak: f64,
}

/// pin 因越出合格线被自动解除的事件（供看板提示）。
#[derive(Debug, Clone, Copy, Serialize)]
pub struct PinReleaseInfo {
    pub channel_id: i64,
    pub pct: f64,
    /// 当时的合格线（正常档=throttle，降级档=exhausted）
    pub limit: f64,
}

/// 弃用的 key（config.toml 条目保留、渠道已删，可恢复）。面板据此渲染灰显卡片。
#[derive(Debug, Clone, Serialize, Default)]
pub struct DeprecatedKeyInfo {
    pub name: String,
    pub note: String,
}

/// 缓存池代理状态（F4b；**只由代理任务写**——与决策字段/面板字段不相交的第三个写者）
#[derive(Debug, Clone, Serialize, Default)]
pub struct CachePoolStatus {
    pub enabled: bool,
    /// false = 中继令牌未就绪，LLM 路径整体降级透传（面板要能看出这状态，
    /// 否则「代理在跑但没路由」与「代理没开」不可区分——PR review #11）
    #[serde(default)]
    pub routing: bool,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    /// 当前占用并发坑的连接数（慢客户端占坑=背压）
    pub in_flight: usize,
    pub per_channel: Vec<CachePoolChan>,
}

#[derive(Debug, Clone, Copy, Serialize, Default)]
pub struct CachePoolChan {
    pub channel_id: i64,
    pub count: u64,
}

/// 评分分解（面板展示；Panel 循环写，与代理选路同一套公式算出）。
/// score = 0.6·week(周刷新临期) + 0.2·cap(容量) + 0.2·load(本机负载)
#[derive(Debug, Clone, Copy, Serialize, Default)]
pub struct ScoreEntry {
    pub channel_id: i64,
    pub week: f64,
    pub cap: f64,
    pub load: f64,
    pub total: f64,
    /// 429 冷却中（真实选路会避开它，直到全员冷却才兜底用回）
    pub cooled: bool,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct StatusSnapshot {
    pub updated_at: i64,
    pub dry_run: bool,
    /// 预防线：还有余量就提前换走
    pub throttle_threshold: f64,
    pub restore_threshold: f64,
    /// 真·用尽线：物理上没余量了。降级档下的合格线
    pub exhausted_threshold: f64,
    pub new_api_base: String,
    pub new_api_healthy: bool,
    pub active_channel_id: Option<i64>,
    /// 用户手动 pin 的渠道（只在合格集内生效）
    pub pinned_channel_id: Option<i64>,
    /// "normal" | "degraded"
    pub regime: String,
    /// 合格集：自动逻辑允许把流量放上去的渠道。前端据此决定 pin 按钮灰不灰
    pub eligible: Vec<i64>,
    /// keyrot-1：周窗口临期时间窗（小时）。0 = 策略关闭（前端显示「周临期优先 关」）
    #[serde(default)]
    pub weekly_reset_lookahead_hours: u64,
    pub last_pin_release: Option<PinReleaseInfo>,
    /// 智谱高峰时段（只显示，不参与调度决策）
    pub peak: PeakInfo,
    pub keys: Vec<KeyStatus>,
    /// 弃用的 key（不参与调度；卡片灰显，可一键恢复）
    #[serde(default)]
    pub deprecated_keys: Vec<DeprecatedKeyInfo>,
    /// opencode 客户端应连的地址（= new_api_base + /v1）
    pub client_endpoint: String,
    /// Claude Code 应填的 ANTHROPIC_BASE_URL（= new_api_base）。
    /// NewAPI 原生支持该下游格式，因此服务健康时始终提供。
    pub claude_endpoint: String,
    /// new-api **内部虚拟余额**（root 用户）。它按「按量付费倍率」给包月套餐虚构记账，
    /// 一旦见底会**直接挡住转发**（「预扣费额度失败」），跟智谱额度毫无关系。
    /// 低于阈值时看板告警，避免无声卡死。
    pub newapi_user_quota: i64,
    // —— 以下为**面板循环**独占写的字段（与上面决策字段严格不相交）——
    /// new-api 侧渠道实况（合并进 key 卡片；不在 keys 里的即「野生渠道」）
    pub channels: Vec<ChannelState>,
    /// 每渠道实时指标（rpm/tpm/最后请求）
    pub live: Vec<LiveMetric>,
    /// 近 24 小时 token 时序（按小时）
    pub hourly: Vec<UsagePoint>,
    /// 按模型的 token 用量
    pub model_usage: Vec<ModelUsage>,
    /// 缓存池代理状态（None = 未启用；启用后由代理任务在每次路由后写入）
    #[serde(default)]
    pub cache_pool: Option<CachePoolStatus>,
    /// 各合格渠道的评分分解（空 = 无合格渠道；不在里面 = 出局/未知）
    #[serde(default)]
    pub scores: Vec<ScoreEntry>,
}

pub type Shared = Arc<RwLock<StatusSnapshot>>;

/// 读快照。锁 poison 时用 into_inner 恢复——避免一次 panic 让看板永久黑屏。
fn read_snap(snap: &Shared) -> StatusSnapshot {
    snap.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// 当前管辖的渠道 id。面板循环据此拉实时指标——
/// **从快照读而不是自持一份副本**，这样看板加/删 key 之后无需重启，面板数据就能跟上。
pub fn tracked_channels(snap: &Shared) -> Vec<i64> {
    snap.read()
        .unwrap_or_else(|e| e.into_inner())
        .keys
        .iter()
        .map(|k| k.channel_id)
        .collect()
}

/// **字段级原子更新**：在写锁内就地修改。
///
/// 决策循环与面板循环各自 read-modify-write，但两者写的字段**严格不相交**
/// （决策：keys/active/阈值/健康；面板：channels/live/hourly/model_usage/内部余额），
/// 加上写锁互斥 ⇒ 不会互相覆盖对方的更新。
pub fn update(snap: &Shared, f: impl FnOnce(&mut StatusSnapshot)) {
    let mut g = snap.write().unwrap_or_else(|e| e.into_inner());
    f(&mut g);
}

/// 把 new-api 的 `(model, hour, tokens, count)` 行聚合成「按小时时序」+「按模型汇总」。
/// 面板循环（近 24h）与 `/api/usage`（任意区间）共用，口径必然一致。
pub fn aggregate_usage(
    rows: Vec<(String, i64, i64, i64)>,
) -> (Vec<UsagePoint>, Vec<ModelUsage>) {
    let mut by_hour: HashMap<i64, (i64, i64)> = HashMap::new();
    let mut by_model: HashMap<String, (i64, i64)> = HashMap::new();
    for (model, hour, tokens, count) in rows {
        let h = by_hour.entry(hour).or_insert((0, 0));
        h.0 += tokens;
        h.1 += count;
        let m = by_model.entry(model).or_insert((0, 0));
        m.0 += tokens;
        m.1 += count;
    }
    let mut hourly: Vec<UsagePoint> = by_hour
        .into_iter()
        .map(|(hour, (tokens, count))| UsagePoint {
            hour,
            tokens,
            count,
        })
        .collect();
    hourly.sort_by_key(|p| p.hour);
    let mut models: Vec<ModelUsage> = by_model
        .into_iter()
        .map(|(model, (tokens, count))| ModelUsage {
            model,
            tokens,
            count,
        })
        .collect();
    models.sort_by(|a, b| b.tokens.cmp(&a.tokens));
    (hourly, models)
}

/// `/api/usage` 的响应。**只返回小时桶，不做日聚合**——「一天」是本地时区概念，
/// 后端做就得引时区依赖；前端本来就在用 `new Date()` 转本地时区，交给它更准也更简单。
#[derive(Debug, Serialize)]
struct UsageResponse {
    start: i64,
    end: i64,
    points: Vec<UsagePoint>,
    models: Vec<ModelUsage>,
}

/// 常驻状态服务。bind 失败 → 记 error 并**返回**（降级），主循环不受影响。
///
/// `tx` 是通往控制循环的命令通道（pin / 加 key 等写操作）。
pub async fn serve(addr: String, snap: Shared, api: Arc<NewApiClient>, tx: mpsc::Sender<Command>) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %addr, error = %e, "状态看板监听失败，已降级（切换循环照常运行）");
            return;
        }
    };
    if !addr.starts_with("127.0.0.1") && !addr.starts_with("localhost") {
        warn!(addr = %addr, "看板绑定在**非回环地址**上：它能改调度状态（后续还会收智谱 key），\
                             等于把控制面暴露给整个网段。除非你清楚在做什么，否则请改回 127.0.0.1");
    }
    info!("状态看板已启动 → http://{addr}");
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let (s, a, t) = (snap.clone(), api.clone(), tx.clone());
                tokio::spawn(async move {
                    if let Err(e) = handle_conn(stream, s, a, t).await {
                        debug!(error = %e, "看板连接处理失败");
                    }
                });
            }
            // accept 出错不退出监听循环
            Err(e) => debug!(error = %e, "accept 失败"),
        }
    }
}

/// 解析好的请求。
struct Req {
    method: String,
    path: String,
    query: String,
    /// 头名一律小写
    headers: HashMap<String, String>,
    body: String,
}

impl Req {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|s| s.as_str())
    }
}

/// 头 8 KiB / 体 8 KiB 上界：防畸形请求撑爆内存。
const MAX_HEAD: usize = 8 * 1024;
const MAX_BODY: usize = 8 * 1024;

/// 读一个完整请求：先读到 `\r\n\r\n`，再按 `Content-Length` 把 body 读满。
///
/// ⚠️ 原来的实现是「`read()` 一次 1 KiB 就当整个请求」——对只读 GET 够用，但**带 body 的
/// POST 会被截断**（TCP 不保证一次 read 拿全）。这是本次必须先修的地基。
async fn read_request(stream: &mut TcpStream) -> std::io::Result<Result<Req, (&'static str, &'static str)>> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    let head_end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p;
        }
        if buf.len() > MAX_HEAD {
            return Ok(Err(("431 Request Header Fields Too Large", "请求头过大")));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(Err(("400 Bad Request", "连接提前关闭")));
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.lines();
    let mut start = lines.next().unwrap_or("").split_whitespace();
    let method = start.next().unwrap_or("").to_string();
    let target = start.next().unwrap_or("");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    if method.is_empty() || path.is_empty() {
        return Ok(Err(("400 Bad Request", "请求行非法")));
    }
    let headers: HashMap<String, String> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();

    let clen: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    if clen > MAX_BODY {
        return Ok(Err(("413 Payload Too Large", "请求体过大（上限 8 KiB）")));
    }
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < clen {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(clen);

    Ok(Ok(Req {
        method,
        path: path.to_string(),
        query: query.to_string(),
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    }))
}

fn err_json(msg: impl AsRef<str>) -> String {
    serde_json::json!({ "error": msg.as_ref() }).to_string()
}

/// 把命令投给控制循环并等回执。
///
/// - 队列满 → **503 立刻返回**：既不阻塞 HTTP 线程，也不阻塞控制循环。
/// - 超时未回 → 504。普通控制命令给 10 秒；AddKey 还要 quota 探活、`/models`、建渠道和
///   发布新快照，单独给 60 秒，避免模型发现 10 秒 fallback 后产生“前端失败、后台成功”。
/// - 命令自身失败（如 pin 一把不合格的 key）→ **409 + 原因**，前端直接显示这句话。
async fn dispatch<T: Serialize>(
    tx: &mpsc::Sender<Command>,
    make: impl FnOnce(oneshot::Sender<Result<T, String>>) -> Command,
    ok_status: &'static str,
    timeout: Duration,
) -> (&'static str, String) {
    let (rtx, rrx) = oneshot::channel();
    if tx.try_send(make(rtx)).is_err() {
        return ("503 Service Unavailable", err_json("控制循环忙，请稍后重试"));
    }
    match tokio::time::timeout(timeout, rrx).await {
        Ok(Ok(Ok(v))) => {
            // 无返回值的命令（pin/unpin）序列化成 "null"，前端不好判 ⇒ 统一给 {"ok":true}
            let mut s = serde_json::to_string(&v).unwrap_or_default();
            if s == "null" || s.is_empty() {
                s = r#"{"ok":true}"#.to_string();
            }
            (ok_status, s)
        }
        Ok(Ok(Err(msg))) => ("409 Conflict", err_json(msg)),
        Ok(Err(_)) => ("500 Internal Server Error", err_json("控制循环已退出")),
        Err(_) => ("504 Gateway Timeout", err_json("控制循环超时未响应")),
    }
}

/// 从 query string 取整数参数。
fn query_i64(query: &str, name: &str) -> Option<i64> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == name).then(|| v.parse().ok())?
    })
}

/// `/api/usage?start=<unix>&end=<unix>`：任意区间的用量（按需查，不进 5 秒快照——
/// 30 天的数据每 5 秒重算纯属浪费）。数据源同近 24h 图：new-api 的 `quota_data`
/// （小时预聚合，每 5 分钟落库一次，见设计文档 §0）。
async fn usage_endpoint(query: &str, api: &NewApiClient) -> (&'static str, String) {
    let (Some(start), Some(end)) = (query_i64(query, "start"), query_i64(query, "end")) else {
        return (
            "400 Bad Request",
            r#"{"error":"缺少或非法的 start / end（unix 秒）"}"#.to_string(),
        );
    };
    if end <= start {
        return (
            "400 Bad Request",
            r#"{"error":"end 必须大于 start"}"#.to_string(),
        );
    }
    match api.usage_data(start, end).await {
        Ok(rows) => {
            let (points, models) = aggregate_usage(rows);
            let body = serde_json::to_string(&UsageResponse {
                start,
                end,
                points,
                models,
            })
            .unwrap_or_else(|_| "{}".to_string());
            ("200 OK", body)
        }
        Err(e) => {
            // 查询失败如实报错，不返回空数组——否则前端会画成「这段时间没用量」，是撒谎
            debug!(error = %e, "拉取用量区间失败");
            (
                "502 Bad Gateway",
                serde_json::json!({ "error": format!("向 new-api 查询用量失败: {e}") }).to_string(),
            )
        }
    }
}

/// 反 CSRF：写操作必须带 `X-QT-Panel` 头。
///
/// 看板绑在 127.0.0.1，但那**挡不住浏览器**——用户浏览的任何网页都能朝 127.0.0.1:3001
/// 发跨域表单 POST。而看板现在能改调度状态（往后还要收智谱 key）。
/// 自定义头是最省事的门槛：跨域请求带自定义头会先触发 preflight，而我们不回任何 CORS 头，
/// 浏览器就把它拦下了。同源的看板自己发请求则不受影响。
const CSRF_HEADER: &str = "x-qt-panel";

async fn handle_conn(
    mut stream: TcpStream,
    snap: Shared,
    api: Arc<NewApiClient>,
    tx: mpsc::Sender<Command>,
) -> std::io::Result<()> {
    let req = match read_request(&mut stream).await? {
        Ok(r) => r,
        Err((status, msg)) => return respond(&mut stream, status, "text/plain; charset=utf-8", msg).await,
    };

    // 写操作一律先过 CSRF 门槛
    let writing = req.method != "GET";
    if writing && req.header(CSRF_HEADER).is_none() {
        let body = err_json("缺少 X-QT-Panel 头（防 CSRF：拒绝来自其它网页的跨域写请求）");
        return respond(&mut stream, "403 Forbidden", JSON, &body).await;
    }

    let (status, ctype, body) = match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/api/status") => {
            let json =
                serde_json::to_string(&read_snap(&snap)).unwrap_or_else(|_| "{}".to_string());
            ("200 OK", JSON, json)
        }
        ("GET", "/api/usage") => {
            let (st, body) = usage_endpoint(&req.query, &api).await;
            (st, JSON, body)
        }
        ("GET", "/") | ("GET", "/index.html") => {
            ("200 OK", "text/html; charset=utf-8", render_html())
        }

        // —— 写操作 ——
        ("POST", "/api/pin") => {
            let id = serde_json::from_str::<serde_json::Value>(&req.body)
                .ok()
                .and_then(|v| v.get("channel_id")?.as_i64());
            match id {
                Some(channel_id) => {
                    let (st, b) = dispatch(
                        &tx,
                        |reply| Command::Pin { channel_id, reply },
                        "200 OK",
                        Duration::from_secs(10),
                    )
                    .await;
                    (st, JSON, b)
                }
                None => (
                    "400 Bad Request",
                    JSON,
                    err_json("请求体需要 {\"channel_id\": <整数>}"),
                ),
            }
        }
        ("DELETE", "/api/pin") => {
            let (st, b) = dispatch(
                &tx,
                |reply| Command::Unpin { reply },
                "200 OK",
                Duration::from_secs(10),
            )
            .await;
            (st, JSON, b)
        }
        ("POST", "/api/keys") => match serde_json::from_str::<NewKeySpec>(&req.body) {
            Ok(spec) => {
                let (st, b) = dispatch(
                    &tx,
                    |reply| Command::AddKey { spec, reply },
                    "201 Created",
                    Duration::from_secs(60),
                )
                .await;
                (st, JSON, b)
            }
            Err(e) => (
                "400 Bad Request",
                JSON,
                err_json(format!("请求体非法（要 name / api_key，org / project 可选）：{e}")),
            ),
        },
        // 恢复弃用 key（探活 + 重建渠道，走 AddKey 同款 60s 超时）
        ("POST", "/api/keys/restore") => {
            let name = serde_json::from_str::<serde_json::Value>(&req.body)
                .ok()
                .and_then(|v| v.get("name")?.as_str().map(str::to_string));
            match name {
                Some(name) => {
                    let (st, b) = dispatch(
                        &tx,
                        |reply| Command::RestoreKey { name, reply },
                        "200 OK",
                        Duration::from_secs(60),
                    )
                    .await;
                    (st, JSON, b)
                }
                None => (
                    "400 Bad Request",
                    JSON,
                    err_json("请求体需要 {\"name\": \"<key 名>\"}"),
                ),
            }
        }
        // 编辑活跃 key 元数据（name/note/org/project；org/project 任一出现即重建 selector）
        ("POST", "/api/keys/update") => {
            let v = serde_json::from_str::<serde_json::Value>(&req.body).ok();
            let channel_id = v.as_ref().and_then(|v| v.get("channel_id")?.as_i64());
            let patch = v
                .and_then(|v| serde_json::from_value::<crate::config::KeyPatch>(v).ok());
            match (channel_id, patch) {
                (Some(channel_id), Some(patch)) => {
                    let (st, b) = dispatch(
                        &tx,
                        |reply| Command::UpdateKey {
                            channel_id,
                            patch,
                            reply,
                        },
                        "200 OK",
                        Duration::from_secs(30),
                    )
                    .await;
                    (st, JSON, b)
                }
                _ => (
                    "400 Bad Request",
                    JSON,
                    err_json("请求体需要 {\"channel_id\": <整数>, name/note/org/project 可选}"),
                ),
            }
        }
        // 手动模型重对账（上游 /models → 渠道 models）
        ("POST", "/api/keys/resync-models") => {
            let id = serde_json::from_str::<serde_json::Value>(&req.body)
                .ok()
                .and_then(|v| v.get("channel_id")?.as_i64());
            match id {
                Some(channel_id) => {
                    let (st, b) = dispatch(
                        &tx,
                        |reply| Command::ResyncModels { channel_id, reply },
                        "200 OK",
                        Duration::from_secs(30),
                    )
                    .await;
                    (st, JSON, b)
                }
                None => (
                    "400 Bad Request",
                    JSON,
                    err_json("请求体需要 {\"channel_id\": <整数>}"),
                ),
            }
        }
        // 弃用 key：删渠道 + config 打标志（要动 new-api，给足超时）
        ("DELETE", p) if p.starts_with("/api/keys/") => {
            match p.trim_start_matches("/api/keys/").parse::<i64>() {
                Ok(channel_id) => {
                    let (st, b) = dispatch(
                        &tx,
                        |reply| Command::DeprecateKey { channel_id, reply },
                        "200 OK",
                        Duration::from_secs(30),
                    )
                    .await;
                    (st, JSON, b)
                }
                Err(_) => ("400 Bad Request", JSON, err_json("路径要 /api/keys/<渠道 id>")),
            }
        }

        ("GET", _) => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found".to_string(),
        ),
        _ => (
            "405 Method Not Allowed",
            JSON,
            err_json("不支持的方法"),
        ),
    };

    respond(&mut stream, status, ctype, &body).await
}

const JSON: &str = "application/json; charset=utf-8";

async fn respond(
    stream: &mut TcpStream,
    status: &str,
    ctype: &str,
    body: &str,
) -> std::io::Result<()> {
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
         Connection: close\r\nCache-Control: no-store\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(resp.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// 自包含看板：内联 CSS/JS，不引用任何外部 CDN。
fn render_html() -> String {
    r##"<!doctype html>
<html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>quota-throttle</title>
<style>
 :root{--bg:#0b0d12;--card:#151922;--line:#252b38;--txt:#e8eaef;--dim:#8b94a3;
       --ok:#3ecf8e;--warn:#f5b942;--bad:#f2555a;--accent:#5b8cff}
 *{box-sizing:border-box}
 body{margin:0;padding:32px 28px;background:
      radial-gradient(1200px 600px at 20% -10%,#1a2030 0%,transparent 60%),var(--bg);
      color:var(--txt);min-height:100vh;
      font:14px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif}
 .wrap{max-width:1100px;margin:0 auto}
 header{display:flex;align-items:baseline;gap:12px;margin-bottom:6px}
 h1{margin:0;font-size:22px;letter-spacing:-.01em}
 .tag{font-size:11px;color:var(--dim);border:1px solid var(--line);border-radius:5px;padding:2px 7px}
 .sub{color:var(--dim);margin-bottom:22px;font-size:13px}
 h2{font-size:12px;margin:30px 0 12px;color:var(--dim);font-weight:600;
    text-transform:uppercase;letter-spacing:.09em}
 .chips{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:22px}
 .chip{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:11px 16px;
       font-size:13px;display:flex;align-items:center;gap:8px}
 .chip .k{color:var(--dim)} .chip .v{font-weight:600}
 .dot{width:8px;height:8px;border-radius:50%}
 .copy{cursor:pointer;border:1px dashed var(--line);border-radius:6px;padding:3px 9px;
       font-family:ui-monospace,SFMono-Regular,monospace;font-size:12px;color:var(--accent)}
 .copy:hover{border-color:var(--accent);background:rgba(91,140,255,.08)}
 .grid{display:grid;gap:14px}
 .card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px 20px;
       transition:border-color .3s,background .3s}
 .card.act{border-color:rgba(62,207,142,.45);background:linear-gradient(180deg,rgba(62,207,142,.06),transparent 60%),var(--card)}
 .card.dead{opacity:.7}
 .card.off{border-color:rgba(242,85,90,.5);background:linear-gradient(180deg,rgba(242,85,90,.08),transparent 60%),var(--card)}
 .chead{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
 .name{font-size:16px;font-weight:650}
 .note{margin-left:7px;padding:2px 8px;border-radius:999px;background:rgba(91,140,255,.14);
       color:var(--accent);font-size:12px;font-weight:600;vertical-align:middle}
 .tier{padding:2px 10px;border-radius:999px;font-size:11px;font-weight:700;letter-spacing:.04em}
 .t-active{background:rgba(62,207,142,.16);color:var(--ok)}
 .t-standby{background:rgba(139,148,163,.16);color:#aeb6c2}
 .t-exhausted{background:rgba(242,85,90,.16);color:var(--bad)}
 .t-unknown{background:rgba(245,185,66,.16);color:var(--warn)}
 .badge{padding:2px 8px;border-radius:6px;font-size:11px;font-weight:700}
 .b-on{background:rgba(62,207,142,.13);color:var(--ok)}
 .b-off{background:rgba(242,85,90,.22);color:var(--bad)}
 .b-imminent{background:rgba(91,140,255,.15);color:var(--accent)}
 .cid{color:var(--dim);font-size:12px;font-variant-numeric:tabular-nums}
 .live{display:flex;align-items:center;gap:9px;margin:12px 0 16px;font-size:13px;
       font-variant-numeric:tabular-nums;color:var(--dim)}
 .live b{color:var(--txt)}
 .pulse{width:8px;height:8px;border-radius:50%;background:var(--ok);
        box-shadow:0 0 0 0 rgba(62,207,142,.6);animation:p 1.6s infinite}
 @keyframes p{0%{box-shadow:0 0 0 0 rgba(62,207,142,.5)}70%{box-shadow:0 0 0 7px rgba(62,207,142,0)}100%{box-shadow:0 0 0 0 rgba(62,207,142,0)}}
 .idle{width:8px;height:8px;border-radius:50%;background:#3a4150}
 .win{display:grid;grid-template-columns:1fr 1fr;gap:22px}
 @media(max-width:640px){.win{grid-template-columns:1fr}}
 .wlab{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:7px}
 .wlab .l{color:var(--dim);font-size:12px}
 .wlab .p{font-size:17px;font-weight:700;font-variant-numeric:tabular-nums}
 .bar{position:relative;height:9px;background:#20252f;border-radius:999px;overflow:hidden}
 .fill{height:100%;border-radius:999px;transition:width .6s cubic-bezier(.4,0,.2,1)}
 .thr{position:absolute;top:-3px;bottom:-3px;width:2px;background:var(--bad);opacity:.6;border-radius:2px}
 .rst{margin-top:6px;color:var(--dim);font-size:12px;font-variant-numeric:tabular-nums}
 .meta{margin-top:16px;padding-top:13px;border-top:1px solid var(--line);
       color:var(--dim);font-size:12px;display:flex;gap:16px;flex-wrap:wrap;font-variant-numeric:tabular-nums}
 .err{color:var(--bad);font-size:12px;margin-top:4px}
 .warn{color:var(--warn);font-size:11px}
 .chart{display:flex;align-items:flex-end;gap:3px;height:110px;padding:0 2px}
 .cb{flex:1;background:linear-gradient(180deg,var(--accent),rgba(91,140,255,.35));
     border-radius:3px 3px 0 0;min-height:2px;transition:height .5s;position:relative}
 .cb:hover{background:var(--accent)}
 .cb.zero{background:#20252f}
 .cb.tap{cursor:pointer}
 .cb.tap:hover{background:var(--ok)}
 .cb span{display:none;position:absolute;bottom:100%;left:50%;transform:translateX(-50%);
          background:#0b0d12;border:1px solid var(--line);border-radius:6px;padding:4px 8px;
          font-size:11px;white-space:nowrap;margin-bottom:6px;z-index:9}
 .cb:hover span{display:block}
 .xax{display:flex;justify-content:space-between;color:var(--dim);font-size:11px;margin-top:8px}
 .hrow{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin:30px 0 12px}
 .hrow h2{margin:0}
 .seg{display:flex;gap:3px;align-items:center;background:#171b23;border:1px solid var(--line);
      border-radius:8px;padding:3px}
 .seg button{background:none;border:0;color:var(--dim);font:inherit;font-size:12px;
             padding:5px 11px;border-radius:6px;cursor:pointer}
 .seg button:hover{color:var(--txt)}
 .seg button.on{background:rgba(91,140,255,.16);color:var(--accent)}
 .cnote{margin-top:10px;color:var(--dim);font-size:12px;display:flex;align-items:center;
        gap:8px;flex-wrap:wrap;font-variant-numeric:tabular-nums}
 .lnk{color:var(--accent);cursor:pointer}
 .lnk:hover{text-decoration:underline}
 .pbtn{margin-left:auto;background:#1b212c;border:1px solid var(--line);color:var(--dim);
       font:inherit;font-size:11px;padding:4px 10px;border-radius:6px;cursor:pointer;white-space:nowrap}
 .pbtn:hover:not(:disabled){border-color:var(--accent);color:var(--accent)}
 .pbtn:disabled{opacity:.35;cursor:not-allowed}
 .pbtn.on{border-color:rgba(62,207,142,.5);color:var(--ok);background:rgba(62,207,142,.1)}
 .ban{padding:11px 14px;border-radius:9px;margin-bottom:10px;font-size:13px;
      display:flex;align-items:center;gap:10px;flex-wrap:wrap;line-height:1.5}
 .ban-warn{background:rgba(245,185,66,.09);border:1px solid rgba(245,185,66,.38);color:var(--warn)}
 .ban-info{background:rgba(91,140,255,.09);border:1px solid rgba(91,140,255,.35);color:var(--txt)}
 .ban .x{margin-left:auto;color:var(--dim);cursor:pointer;font-size:12px}
 .ban .x:hover{color:var(--txt)}
 .toast{position:fixed;right:18px;bottom:18px;max-width:440px;padding:11px 14px;border-radius:9px;
        background:#1b212c;border:1px solid var(--bad);color:var(--txt);font-size:13px;z-index:99;
        line-height:1.5;box-shadow:0 10px 30px rgba(0,0,0,.45)}
 .frow{display:flex;gap:10px;margin-bottom:10px;flex-wrap:wrap;align-items:center}
 .frow input{flex:1;min-width:220px;background:#0f131a;border:1px solid var(--line);border-radius:8px;
             padding:9px 12px;color:var(--txt);font:inherit;font-size:13px}
 .frow input:focus{outline:none;border-color:var(--accent)}
 .sbtn{background:rgba(91,140,255,.16);border:1px solid rgba(91,140,255,.5);color:var(--accent);
       font:inherit;font-size:13px;padding:9px 18px;border-radius:8px;cursor:pointer;white-space:nowrap}
 .sbtn:hover{background:rgba(91,140,255,.26)}
 .sbtn:disabled{opacity:.5;cursor:progress}
 .fhint{flex:1;min-width:280px;color:var(--dim);font-size:11.5px;line-height:1.6}
 .fhint b{color:var(--warn)}
 .del{background:none;border:0;color:var(--dim);font:inherit;font-size:11px;cursor:pointer;padding:0}
 .editd{margin-top:12px;font-size:12px}
 .editd summary{color:var(--dim);cursor:pointer;list-style:none;user-select:none}
 .editd summary:hover{color:var(--accent)}
 .editd form{margin-top:10px}
 .emsg{color:var(--warn)}
 .del:hover{color:var(--bad)}
 .tbl{width:100%;border-collapse:collapse;font-size:13px}
 .tbl th{text-align:left;color:var(--dim);font-weight:500;font-size:11px;text-transform:uppercase;
         letter-spacing:.05em;padding:0 12px 9px 0;border-bottom:1px solid var(--line)}
 .tbl td{padding:10px 12px 10px 0;border-bottom:1px solid rgba(37,43,56,.55);font-variant-numeric:tabular-nums}
 .tbl tr:last-child td{border-bottom:none}
 .empty{color:var(--dim);font-size:13px;padding:6px 0}
 .foot{margin-top:26px;color:var(--dim);font-size:12px}
</style></head><body><div class="wrap">
<header><h1>quota-throttle</h1><span class="tag">智谱 GLM Coding Plan</span></header>
<div class="sub" id="sub">加载中…</div>
<div class="chips" id="chips"></div>
<div id="bans"></div>
<div class="grid" id="grid"></div>
<h2>添加 key</h2>
<div class="card">
  <div id="addmsg"></div>
  <form id="addf" autocomplete="off">
    <div class="frow">
      <input name="name" placeholder="名称（如 zhipu-3，同时用作 new-api 渠道名）" required>
      <input name="api_key" type="password" placeholder="智谱 API key" required>
    </div>
    <div class="frow">
      <input name="org" placeholder="Bigmodel-Organization（团体套餐必填）">
      <input name="project" placeholder="Bigmodel-Project（团体套餐必填）">
    </div>
    <div class="frow">
      <button class="sbtn" type="submit">探活并添加</button>
      <span class="fhint">先用这把 key 真查一次智谱用量。<b>探活不通过就什么都不改</b>——不建渠道、不动 config.toml。
        团体套餐漏填 selector 会「查得通但 limits 为空」，那把 key 会被当成 0% 用量、永不切换，所以必须在这里挡住。
        org/project 取法：浏览器开 bigmodel.cn 的用量页 → F12 → 找 quota/limit 请求 → 抄这两个请求头。</span>
    </div>
  </form>
</div>
<div id="wild"></div>
<div class="hrow">
  <h2 id="ctitle">近 24 小时用量</h2>
  <div class="seg" id="seg">
    <button data-r="24h" class="on">近 24 小时</button>
    <button data-r="30d">近 30 天</button>
  </div>
</div>
<div class="card"><div id="chart"></div><div class="cnote" id="cnote"></div></div>
<h2>按模型</h2>
<div class="card"><div id="models"></div></div>
<div class="foot" id="foot"></div>
</div><script>
const pad=n=>String(n).padStart(2,'0');
const clock=ms=>{const d=new Date(ms);return `${pad(d.getMonth()+1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`};
const left=ms=>{if(!ms)return'—';let s=Math.floor((ms-Date.now())/1000);if(s<=0)return'即将重置';
  const d=Math.floor(s/86400);s%=86400;const h=Math.floor(s/3600);const m=Math.floor(s%3600/60);
  return d?`${d} 天 ${h} 小时后重置`:h?`${h} 小时 ${m} 分后重置`:`${m} 分后重置`};
const ago=sec=>{if(!sec)return null;const s=Math.floor(Date.now()/1000)-sec;
  if(s<60)return `${s} 秒前`; if(s<3600)return `${Math.floor(s/60)} 分钟前`;
  if(s<86400)return `${Math.floor(s/3600)} 小时前`; return `${Math.floor(s/86400)} 天前`};
const kfmt=n=>n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':String(n||0);
/* HTML 转义：key 名/备注会进 innerHTML 和属性（名字虽在录入时挡了引号尖括号，
   note 和历史存量仍可能有——统一转义，杜绝面板自注入） */
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const hue=(p,thr)=>p>=thr?'var(--bad)':p>=thr*0.8?'var(--warn)':'var(--ok)';
const TIER={active:'ACTIVE',standby:'STANDBY',exhausted:'耗尽',unknown:'未知'};
const LOW=10000000;

/* ——— pin：在「合格集」内表达偏好 ———
   pin 不是安全豁免——它只能钉自动逻辑判定为**合格**的 key（正常档 <95%，降级档 <100%）。
   所以不合格的 key，按钮直接置灰：点不了比「点了之后被静默解除」清楚。 */
async function call(method,path,body){
  const r=await fetch(path,{method,cache:'no-store',
    headers:{'Content-Type':'application/json','X-QT-Panel':'1'},   // 自定义头 = 反 CSRF 门槛
    body:body?JSON.stringify(body):undefined});
  const j=await r.json().catch(()=>({}));
  if(!r.ok) throw new Error(j.error||('HTTP '+r.status));
  return j;
}
let toastT=null;
function toast(msg){
  let el=document.getElementById('toast');
  if(!el){ el=document.createElement('div'); el.id='toast'; el.className='toast'; document.body.appendChild(el); }
  el.textContent=msg; clearTimeout(toastT);
  toastT=setTimeout(()=>el.remove(),6000);
}
/* 按钮置灰已挡住绝大多数误操作；但快照到点击之间 key 可能刚好越线 ⇒ 后端仍会 409，如实显示 */
async function pin(id){ try{ await call('POST','/api/pin',{channel_id:id}); }catch(e){ toast(e.message); } tick(); }
async function unpin(){ try{ await call('DELETE','/api/pin'); }catch(e){ toast(e.message); } tick(); }

let seenRelease=null;   // 「pin 已自动解除」提示：本地关掉后不再弹，除非又发生了新的一次

/* ——— 加 / 删 key ——— */
document.getElementById('addf').addEventListener('submit',async e=>{
  e.preventDefault();
  const f=new FormData(e.target), g=n=>(f.get(n)||'').trim();
  const body={name:g('name'), api_key:g('api_key'), org:g('org')||null, project:g('project')||null};
  const msg=document.getElementById('addmsg'), btn=e.target.querySelector('.sbtn');
  btn.disabled=true;
  msg.innerHTML='<div class="ban ban-info">正在探活……（用这把 key + selector 真查一次智谱用量）</div>';
  try{
    const j=await call('POST','/api/keys',body);
    msg.innerHTML=`<div class="ban ban-info">✅ 已添加 <b>${body.name}</b>（渠道 #${j.channel_id}）
      · 套餐 <b>${j.level||'—'}</b> · 模型 <b>${j.models_source==='discovered'?'实时探测':'配置 fallback'}</b> · 5 小时窗口 <b>${j.five_hour_pct??'—'}%</b> · 每周 <b>${j.weekly_pct??'—'}%</b>
      · 已写回 config.toml，重启后仍在。以 standby 入场，下一轮自动决策决定要不要转正。</div>`;
    e.target.reset(); tick();
  }catch(err){
    msg.innerHTML=`<div class="ban ban-warn">❌ ${err.message}</div>`;   // 智谱的错误原文原样显示
  }finally{ btn.disabled=false; }
});

async function delKey(id,name){
  if(!confirm(`弃用 ${name}？\n\n· new-api 渠道**删除**（历史用量和日志保留在 new-api）\n`
            +`· config.toml 条目保留并打「弃用」标志，凭据不丢，随时可恢复`)) return;
  try{ await call('DELETE','/api/keys/'+id); }catch(e){ toast(e.message); }
  tick();
}
async function restoreKey(name){
  toast(`正在恢复 ${name}（探活 + 重建渠道）……`);
  try{ await call('POST','/api/keys/restore',{name}); toast(`✅ ${name} 已恢复，standby 入场`); }
  catch(e){ toast(e.message); }
  tick();
}
async function resyncModels(id){
  toast('模型重对账中（上游 /models 探测）……');
  try{ await call('POST','/api/keys/resync-models',{channel_id:id}); toast('✅ 模型重对账完成'); }
  catch(e){ toast(e.message); }
  tick();
}
/* 编辑表单：grid 每 5 秒重建，绑不了元素级事件——document 级委托 submit */
document.addEventListener('submit',async e=>{
  const f=e.target;
  if(!(f instanceof HTMLFormElement)||!f.classList.contains('editf')) return;
  e.preventDefault();
  const g=n=>((f.elements[n]&&f.elements[n].value)||'').trim();
  const btn=f.querySelector('.sbtn'), msg=f.querySelector('.emsg');
  btn.disabled=true; msg.textContent='保存中……';
  // org/project 只在**改过**时提交——后端「任一出现即重建 selector」会把
  // config 里手写的额外 header/未 trim 值抹掉（PR review #6）；没改就不触发探活
  const body={channel_id:+f.dataset.id, name:g('name'), note:g('note')};
  if(g('org')!==f.dataset.org || g('project')!==f.dataset.project){
    body.org=g('org'); body.project=g('project');
    msg.textContent='保存中（selector 变了，先探活）……';
  }
  try{
    await call('POST','/api/keys/update', body);
    msg.textContent='✅ 已保存';
    setTimeout(tick,400);
  }catch(err){ msg.textContent='❌ '+err.message; }   // 智谱错误原文回显
  finally{ btn.disabled=false; }
});

/* ——— 智谱高峰时段（纯显示）———
   智谱的「高峰」影响的不是限额，而是**扣减系数**：同一个请求在 14:00–18:00 烧掉的额度
   是其他时间的数倍（glm-5.2 现为 3x vs 1x）。这件事没有任何接口能查，只能按时钟算，
   而看板不提醒的话，你赶工赶到下午就默默多烧了 3 倍。 */
const dur=ms=>{let s=Math.max(0,Math.floor((ms-Date.now())/1000));
  const h=Math.floor(s/3600), m=Math.floor(s%3600/60);
  return h?`${h} 小时 ${m} 分`:`${m} 分`};

/* 缓存池代理状态（未启用不渲染）。命中率持续偏低 ≈ key 指纹抖动（如 system 含
   时间戳），见排障文档 */
function poolChip(cp){
  if(!cp||!cp.enabled) return '';
  const tot=cp.hits+cp.misses;
  const rate=tot?Math.round(cp.hits/tot*100)+'%':'—';
  if(!cp.routing) return `<div class="chip" style="border-color:rgba(245,185,66,.5)">
    <span class="v" style="color:var(--warn)">缓存池降级透传</span>
    <span class="k">中继令牌未就绪（重启本工具可重试）</span></div>`;
  return `<div class="chip"><span class="k">缓存池</span>
    <span class="v">${cp.entries} 条 · 命中 ${rate}</span>
    <span class="k">${cp.in_flight} 并发中</span></div>`;
}

function peakChip(p){
  if(!p||!p.enabled) return '';
  // 系数只展示受影响的模型（未配的一律 1x，没什么好说的）
  const coef=p.models.map(m=>`${m.model} ${m.current}x`).join(' · ')||'—';
  const next=dur(p.next_change_at);
  if(p.is_peak){
    const off=p.models.map(m=>m.off_peak).filter((v,i,a)=>a.indexOf(v)===i).join('/');
    return `<div class="chip" style="border-color:var(--bad)">
      <span class="v" style="color:var(--bad)">⚠️ 高峰期 · ${coef}</span>
      <span class="k">同样的请求烧掉更多额度 · ${next}后回落到 ${off}x · 窗口 ${p.window}</span></div>`;
  }
  const pk=p.models.map(m=>m.peak).filter((v,i,a)=>a.indexOf(v)===i).join('/');
  return `<div class="chip">
    <span class="v" style="color:var(--ok)">非高峰 · ${coef}</span>
    <span class="k">${next}后进入高峰（${pk}x）· 窗口 ${p.window}${p.note?' · '+p.note:''}</span></div>`;
}

function win(label,pct,reset,thr){
  if(pct==null) return `<div><div class="wlab"><span class="l">${label}</span></div><div class="err">查询失败</div></div>`;
  const w=Math.max(0,Math.min(100,pct));
  return `<div>
    <div class="wlab"><span class="l">${label}</span><span class="p" style="color:${hue(pct,thr)}">${pct}%</span></div>
    <div class="bar"><div class="fill" style="width:${w}%;background:${hue(pct,thr)}"></div>
      <div class="thr" style="left:${thr}%" title="切换阈值 ${thr}%"></div></div>
    <div class="rst">${left(reset)}</div></div>`;
}

/* ——— 用量图：三态（近 24 小时 / 近 30 天 / 某一天） ———
   · 近 24 小时走**快照**（5 秒刷，实时），一如既往。
   · 历史两态走 /api/usage 按需查。刷新节奏 5 分钟 = new-api 把 quota_data 落库的节奏
     （DataExportInterval），刷得再勤也拿不到更新的数。
   · 纯过去的某一天数据已不再变 → 拉一次冻住，根本不刷。 */
const DAY=86400, HIST_MS=5*60*1000;
const dayStart=sec=>{const d=new Date(sec*1000); d.setHours(0,0,0,0); return Math.floor(d/1000)};
const today=()=>dayStart(Date.now()/1000);
const md=sec=>{const d=new Date(sec*1000); return `${pad(d.getMonth()+1)}-${pad(d.getDate())}`};

let live24=[];                       // 快照里的近 24h 小时桶
let view={mode:'24h'};               // {mode:'24h'|'30d'} | {mode:'day', day:<当地零点 epoch>}
let hist=null, histAt=0, histErr=null, histBusy=false;

/* 当前视图的区间是否**含 now** ⇒ 含则要刷；纯过去的不刷 */
const histLive=()=>view.mode==='30d'||(view.mode==='day'&&view.day===today());

async function loadHist(){
  if(view.mode==='24h'||histBusy) return;
  const [start,end]= view.mode==='30d'
    ? [today()-29*DAY, Math.floor(Date.now()/1000)+3600]
    : [view.day, view.day+DAY];
  histBusy=true; drawChart();
  try{
    const r=await fetch(`/api/usage?start=${start}&end=${end}`,{cache:'no-store'});
    const j=await r.json();
    if(!r.ok) throw new Error(j.error||('HTTP '+r.status));
    hist=j; histErr=null; histAt=Date.now();
  }catch(e){ hist=null; histErr=String(e.message||e); }   // 查询失败如实说，不画成「没用量」
  histBusy=false; drawChart();
}

/* bars: [{label, tokens, count, tip, day?}]；day 非空 ⇒ 该柱可点击下钻 */
function bars(list,peakUnit){
  const max=Math.max(...list.map(b=>b.tokens),1);
  const bs=list.map(b=>`<div class="cb${b.tokens?'':' zero'}${b.day?' tap':''}"
      style="height:${b.tokens?Math.max(3,b.tokens/max*100):2}%"
      ${b.day?`onclick="drill(${b.day})"`:''}><span>${b.tip}</span></div>`).join('');
  return `<div class="chart">${bs}</div>
    <div class="xax"><span>${list[0].label}</span>
      <span>峰值 ${kfmt(max)} tokens/${peakUnit}</span>
      <span>${list[list.length-1].label}</span></div>`;
}

function drill(day){ view={mode:'day',day}; hist=null; syncSeg(); loadHist(); }
function back(){ view={mode:'30d'}; hist=null; syncSeg(); loadHist(); }

function syncSeg(){
  document.querySelectorAll('#seg button[data-r]').forEach(b=>
    b.classList.toggle('on', b.dataset.r===view.mode));
}

function drawChart(){
  const el=document.getElementById('chart'), note=document.getElementById('cnote');
  const title=document.getElementById('ctitle');

  if(view.mode==='24h'){
    title.textContent='近 24 小时用量';
    el.innerHTML = !live24.length ? '<div class="empty">近 24 小时暂无用量</div>'
      : bars(live24.map(p=>({tokens:p.tokens, count:p.count,
          label:new Date(p.hour*1000).getHours()+':00',
          tip:`${new Date(p.hour*1000).getHours()}:00 · ${p.tokens.toLocaleString()} tokens · ${p.count} 次`})),'小时');
    note.innerHTML='<span class="pulse"></span> 实时 · 每 5 秒刷新';
    return;
  }

  if(histBusy&&!hist){ el.innerHTML='<div class="empty">加载中…</div>'; note.textContent=''; return; }
  if(histErr){ el.innerHTML=`<div class="empty" style="color:var(--bad)">用量查询失败：${histErr}</div>`;
    note.innerHTML='<span class="lnk" onclick="loadHist()">重试</span>'; return; }
  if(!hist){ el.innerHTML='<div class="empty">—</div>'; note.textContent=''; return; }

  // 小时桶 → 按**浏览器本地时区**归日（后端不做日聚合，就是为了绕开时区）
  const byH={}, byD={};
  for(const p of hist.points){
    byH[p.hour]=p;
    const d0=dayStart(p.hour);
    (byD[d0] ||= {tokens:0,count:0});
    byD[d0].tokens+=p.tokens; byD[d0].count+=p.count;
  }

  if(view.mode==='30d'){
    title.textContent='近 30 天用量';
    const t0=today();
    const list=[...Array(30)].map((_,i)=>{           // 补零：没用量的那天也占一格，不压缩
      const d0=t0-(29-i)*DAY, v=byD[d0]||{tokens:0,count:0};
      return {tokens:v.tokens, count:v.count, label:md(d0), day:v.tokens?d0:null,   // 空白日不可点
        tip:`${md(d0)} · ${v.tokens.toLocaleString()} tokens · ${v.count} 次${v.tokens?' — 点击看小时':''}`};
    });
    el.innerHTML=bars(list,'天');
    const first=list.find(b=>b.tokens);
    note.innerHTML=`每 5 分钟刷新（= new-api 落库节奏）· ${clock(histAt)}
      <span class="lnk" onclick="loadHist()">立即刷新</span>
      ${first&&first.label!==list[0].label?`<span style="opacity:.7">· 最早记录 ${first.label}（此前 new-api 尚未运行）</span>`:''}
      <span style="opacity:.7">· 点柱下钻到小时</span>`;
    return;
  }

  // 某一天 → 24 根小时柱
  const d0=view.day, isToday=d0===today();
  title.textContent=`${md(d0)} 用量（按小时）`;
  const list=[...Array(24)].map((_,h)=>{
    const p=byH[d0+h*3600]||{tokens:0,count:0};
    return {tokens:p.tokens, count:p.count, label:h+':00',
      tip:`${h}:00 · ${p.tokens.toLocaleString()} tokens · ${p.count} 次`};
  });
  el.innerHTML=bars(list,'小时');
  note.innerHTML=`<span class="lnk" onclick="back()">← 返回近 30 天</span>
    · ${isToday ? `今天，每 5 分钟刷新 · ${clock(histAt)} <span class="lnk" onclick="loadHist()">立即刷新</span>`
                : '该日已归档，数据不再变化'}`;
}

document.getElementById('seg').addEventListener('click',e=>{
  const r=e.target.dataset.r; if(!r||r===view.mode) return;
  view={mode:r}; hist=null; histErr=null; syncSeg();
  r==='24h' ? drawChart() : loadHist();
});
// 历史视图只在「区间含 now」时才刷（纯过去的已冻结）
setInterval(()=>{ if(histLive()) loadHist(); }, HIST_MS);

async function tick(){
  let d;
  try{ d=await (await fetch('/api/status',{cache:'no-store'})).json(); }
  catch(e){ document.getElementById('sub').textContent='连不上 quota-throttle（进程没跑？）'; return; }
  if(!d.keys||!d.keys.length){ document.getElementById('sub').textContent='等待首轮采集…'; return; }

  const thr=d.throttle_threshold;
  const act=d.keys.find(k=>k.channel_id===d.active_channel_id);
  const chOf=id=>(d.channels||[]).find(c=>c.id===id);
  const lvOf=id=>(d.live||[]).find(l=>l.channel_id===id);

  document.getElementById('sub').textContent=
    `任一窗口达 ${thr}% 即切换 · 挑新活动 key 要求低于 ${d.restore_threshold}%`
    + ` · 全部 key 都超线时，榨到 ${d.exhausted_threshold}% 再流转（不硬撞 429）`
    + ` · 周临期优先 ${d.weekly_reset_lookahead_hours>0?d.weekly_reset_lookahead_hours+' 小时内重置的 key': '关'}`;

  const q=d.newapi_user_quota;
  document.getElementById('chips').innerHTML=`
   <div class="chip"><span class="dot" style="background:${d.new_api_healthy?'var(--ok)':'var(--bad)'}"></span>
     <span class="k">new-api</span><span class="v">${d.new_api_healthy?'健康':'不可达'}</span></div>
   <div class="chip"><span class="k">活动 key</span>
     <span class="v" style="color:var(--ok)">${act?act.name:'无 · 全部无额度'}${d.pinned_channel_id!=null?' 📌':''}</span></div>
   <div class="chip"><span class="k">opencode</span>
     <span class="copy" title="点击复制" onclick="navigator.clipboard.writeText('${d.client_endpoint||''}');this.textContent='已复制';setTimeout(()=>this.textContent='${d.client_endpoint||''}',900)">${d.client_endpoint||'—'}</span></div>
   ${d.claude_endpoint?`<div class="chip"><span class="k">Claude Code</span>
     <span class="copy" title="ANTHROPIC_AUTH_TOKEN 使用同一把 NewAPI key；点击复制 ANTHROPIC_BASE_URL" onclick="navigator.clipboard.writeText('${d.claude_endpoint}');this.textContent='已复制';setTimeout(()=>this.textContent='${d.claude_endpoint}',900)">${d.claude_endpoint}</span><span class="k">共用 key</span></div>`:''}
   ${peakChip(d.peak)}
   ${poolChip(d.cache_pool)}
   ${(q!=null&&q>=0&&q<LOW)?'<div class="chip" style="border-color:var(--bad)"><span class="v" style="color:var(--bad)">new-api 内部余额即将耗尽</span><span class="k">见底会挡住转发（与智谱额度无关）</span></div>':''}
   ${d.dry_run?'<div class="chip" style="border-color:rgba(245,185,66,.5)"><span class="v" style="color:var(--warn)">dry_run</span><span class="k">只打印决策，不真改 new-api</span></div>':''}`;

  // —— 横幅：降级档 / pin 被自动解除 ——
  // 提示的去重键只能由数字/冒号组成——它要嵌进 onclick 属性，带引号的 JSON 会把属性截断
  const rel=d.last_pin_release, relKey=rel?`${rel.channel_id}:${rel.pct}:${rel.limit}`:null;
  const relName=rel?(d.keys.find(k=>k.channel_id===rel.channel_id)||{}).name||('#'+rel.channel_id):'';
  document.getElementById('bans').innerHTML=`
   ${d.regime==='degraded'?`<div class="ban ban-warn">
      <b>降级档</b>：全部 key 都已越过 ${thr}% 预防线。正在把活动 key 榨到
      ${d.exhausted_threshold}% 再流转到还有余量的那把——此时撞 429 的风险由 new-api 的 priority 阶梯兜底。
     </div>`:''}
   ${(rel&&relKey!==seenRelease)?`<div class="ban ban-info">
      📌 <b>${relName}</b> 的固定已自动解除：用量 ${rel.pct}% 越过了合格线 ${rel.limit}%，已回到自动选择。
      <span class="x" onclick="seenRelease='${relKey}';tick()">知道了</span>
     </div>`:''}`;

  // —— 合并卡片：智谱用量 + new-api 渠道状态 + 实时指标，一把 key 全在这 ——
  // 编辑表单展开时**跳过 grid 重渲染**（每 5 秒重建会把正在输入的内容打断/清空）；
  // 其余区域照常刷新
  const eligible=new Set(d.eligible||[]);
  const scoreOf=id=>(d.scores||[]).find(s=>s.channel_id===id);
  if(!document.querySelector('#grid .editd[open]')){
  document.getElementById('grid').innerHTML=d.keys.map(k=>{
    const c=chOf(k.channel_id), l=lvOf(k.channel_id);
    const disabled = c && !c.enabled;
    const mism = c && c.priority!=null && k.priority!=null && c.priority!==k.priority;
    const on = l && l.rpm>0;
    const lastOf=l;
    const lastTxt = lastOf&&lastOf.last_request_at ? `最后请求 ${ago(lastOf.last_request_at)}${lastOf.last_request_model?` (${lastOf.last_request_model})`:''}` : '暂无请求记录';
    // pin 按钮：只有合格的才点得动（pin 是优先级，不是安全豁免）
    const isPinned = k.channel_id===d.pinned_channel_id;
    const ok = eligible.has(k.channel_id);
    const why = k.max_pct==null ? '本轮用量查询失败，状态未知，暂不能固定'
              : `用量 ${k.max_pct}% 已越过合格线 ${d.regime==='degraded'?d.exhausted_threshold:thr}%，自动逻辑不允许固定`;
    const btn = isPinned
      ? `<button class="pbtn on" onclick="unpin()" title="回到自动选择">📌 已固定 · 取消</button>`
      : `<button class="pbtn" ${ok?'':'disabled'} title="${ok?'把流量钉在这把 key 上（仍受自动逻辑约束：越线会自动解除）':why}"
           onclick="pin(${k.channel_id})">📌 固定到这把</button>`;
    return `
   <div class="card ${k.tier==='active'?'act':''} ${k.tier==='exhausted'?'dead':''} ${disabled?'off':''}">
     <div class="chead">
       <span class="name">${esc(k.name)}</span>${k.note?`<span class="note">${esc(k.note)}</span>`:''}
       <span class="tier t-${k.tier}">${TIER[k.tier]||k.tier}</span>
       ${k.imminent?'<span class="badge b-imminent" title="周窗口即将重置且还有余量 — 切换时会优先烧它">⏳ 临期</span>':''}
       <span class="cid">渠道 #${k.channel_id}</span>
       ${c ? (c.enabled ? '<span class="badge b-on">启用</span>'
             : `<span class="badge b-off">已被 new-api 禁用</span>`) : ''}
       ${btn}
     </div>
     ${disabled?`<div class="err">status=${c.status_raw} · priority 不起作用，流量不会来这把 key</div>`:''}
     ${k.error?`<div class="err">${esc(k.error)}</div>`:''}

     <div class="live">
       <span class="${on?'pulse':'idle'}"></span>
       <span><b>${l?l.rpm:0}</b> req/min</span><span>·</span>
       <span><b>${kfmt(l?l.tpm:0)}</b> tok/min</span>
       <span>·</span>
       <span>${lastTxt}</span>
     </div>

     <div class="win">
       ${win('5 小时窗口',k.five_hour_pct,k.five_hour_reset,thr)}
       ${win('每周窗口',k.weekly_pct,k.weekly_reset,thr)}
     </div>

     <div class="meta">
       <span>priority <b style="color:${mism?'var(--warn)':'var(--txt)'}">${k.priority??'—'}</b>${mism?` <span class="warn">（new-api 侧是 ${c.priority}，不一致！）</span>`:''}</span>
       ${(()=>{const sc=scoreOf(k.channel_id);return sc?`<span title="新对话的选路评分 = 0.6·周刷新临期 + 0.2·容量 + 0.2·负载（与代理选路同一公式；负载只计本机）">评分 <b>${sc.total.toFixed(3)}</b><span style="opacity:.75">（周 ${sc.week.toFixed(2)} · 容 ${sc.cap.toFixed(2)} · 载 ${sc.load.toFixed(2)}）</span>${sc.cooled?' <span class="warn">⏸ 429 冷却中</span>':''}</span>`:'';})()}
       ${c?`<span>分组 ${esc(c.group||'—')}</span><span>auto_ban ${c.auto_ban?'开':'关'}</span><span style="opacity:.7">${esc(c.models||'')}</span>`:''}
       <button class="del" onclick="resyncModels(${k.channel_id})"
         title="用这把 key 的上游 /models 刷新渠道模型列表（探测失败不覆盖）">⟳ 对账模型</button>
       <button class="del" style="margin-left:auto" data-n="${esc(k.name)}" onclick="delKey(${k.channel_id},this.dataset.n)"
         title="删 new-api 渠道 + config.toml 打弃用标志（条目保留，可恢复）">🗑 弃用</button>
     </div>

     <details class="editd"><summary>✎ 编辑</summary>
      <form class="editf" data-id="${k.channel_id}" data-org="${esc(k.org)}" data-project="${esc(k.project)}">
       <div class="frow"><input name="name" value="${esc(k.name)}" placeholder="名字（引号/尖括号/反斜杠不可）"></div>
       <div class="frow"><input name="note" value="${esc(k.note)}" placeholder="备注（如持有人，留空不显示）"></div>
       <div class="frow"><input name="org" value="${esc(k.org)}" placeholder="Bigmodel-Organization（留空=清掉，会先探活）">
        <input name="project" value="${esc(k.project)}" placeholder="Bigmodel-Project"></div>
       <div class="frow"><button class="sbtn" type="submit">保存</button><span class="emsg"></span></div>
      </form>
      <div class="fhint">改 org/project 先用新 selector 探活，失败不改；改名字会同步改 new-api 渠道名。仅改名字/备注时不会动 selector。</div>
     </details>
   </div>`}).join('');
  }

  // 野生渠道：new-api 里有、但不在我们管辖的 keys 里——可能偷偷接到流量
  const mine=new Set(d.keys.map(k=>k.channel_id));
  const wild=(d.channels||[]).filter(c=>!mine.has(c.id));
  document.getElementById('wild').innerHTML = (!wild.length ? '' : `
    <h2>野生渠道（不在 config.keys 里，我们不管它）</h2>
    <div class="card"><table class="tbl"><thead><tr><th>渠道</th><th>状态</th><th>priority</th><th>分组</th><th>模型</th></tr></thead><tbody>${
      wild.map(c=>`<tr><td><b>${esc(c.name)}</b> <span class="cid">#${c.id}</span></td>
        <td>${c.enabled?'<span class="badge b-on">启用</span><div class="warn">可能接到流量</div>':'<span class="badge b-off">禁用</span>'}</td>
        <td>${c.priority??'—'}</td><td style="color:var(--dim)">${c.group||'—'}</td>
        <td style="color:var(--dim);font-size:12px">${esc(c.models||'—')}</td></tr>`).join('')}</tbody></table></div>`)
    // 弃用 key：灰显 + 恢复按钮（凭据还在 config.toml，恢复会探活并重建渠道）
    + (!(d.deprecated_keys||[]).length ? '' : `
    <h2>已弃用（不参与调度；config 条目保留，可恢复）</h2>
    ${(d.deprecated_keys||[]).map(k=>`
    <div class="card dead" style="opacity:.6">
      <div class="chead">
        <span class="name">${esc(k.name)}</span>${k.note?`<span class="note">${esc(k.note)}</span>`:''}
        <span class="tier t-exhausted">弃用</span>
        <button class="pbtn" style="margin-left:auto" data-n="${esc(k.name)}" onclick="restoreKey(this.dataset.n)"
          title="探活并重建渠道（standby 入场），config 去掉弃用标志">↩ 恢复</button>
      </div>
    </div>`).join('')}`);

  // 近 24 小时视图直接吃快照（实时，5 秒刷）；历史视图走 /api/usage，见下方 chart 引擎
  live24=d.hourly||[];
  if(view.mode==='24h') drawChart();

  // 按模型
  const ms=d.model_usage||[];
  document.getElementById('models').innerHTML = !ms.length ? '<div class="empty">暂无数据</div>'
    : `<table class="tbl"><thead><tr><th>模型</th><th>tokens</th><th>请求数</th><th>占比</th></tr></thead><tbody>${
      (()=>{const tot=ms.reduce((a,m)=>a+m.tokens,0)||1;
        return ms.map(m=>`<tr><td><b>${m.model}</b></td>
          <td>${m.tokens.toLocaleString()}</td><td style="color:var(--dim)">${m.count}</td>
          <td><div class="bar" style="height:6px;width:120px"><div class="fill" style="width:${m.tokens/tot*100}%;background:var(--accent)"></div></div></td></tr>`).join('')})()
      }</tbody></table>`;

  document.getElementById('foot').textContent=
    `决策更新 ${clock(d.updated_at)} · 面板每 ${5} 秒刷新（智谱用量按 poll_interval_secs 轮询）`;
}
tick(); setInterval(tick,5000);
</script></body></html>"##
        .to_string()
}

#[cfg(test)]
mod panel_tests {
    use super::render_html;

    /// 面板 JS 语法自检（有 node 才跑，没有则跳过）。
    /// 血泪：F2 拼弃用区块少一个右括号，服务端/JSON 全正常但浏览器整块 script 挂掉、
    /// 页面永远「加载中」——这类错 curl 测不出来，只有真解析器（浏览器/node）能抓住。
    #[test]
    fn 面板js语法自检() {
        let html = render_html();
        let (s, e) = (html.find("<script>").unwrap() + 8, html.find("</script>").unwrap());
        let dir = std::env::temp_dir().join(format!("qt-panel-js-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("panel.js");
        std::fs::write(&f, &html[s..e]).unwrap();
        match std::process::Command::new("node").arg("--check").arg(&f).output() {
            Ok(o) => assert!(
                o.status.success(),
                "面板 JS 语法错误:\n{}",
                String::from_utf8_lossy(&o.stderr)
            ),
            Err(_) => {} // 本机没 node：跳过（不阻断无 node 环境）
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
