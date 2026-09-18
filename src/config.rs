//! 配置加载。所有可能随 new-api 版本变化的东西（路径、header）都放到配置里，
//! 不写死在代码，方便你 F12 抓到真实接口后直接改。

use anyhow::{bail, Context};
use serde::Deserialize;
use std::io::Write;
use std::path::Path;
use tracing::warn;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// 本配置文件的路径。面板加/删 key 时要写回它（**config.toml 是唯一数据源**）。
    #[serde(skip)]
    pub source_path: String,

    /// 轮询间隔（秒）
    pub poll_interval_secs: u64,

    /// **预防线**：活动 key 的最大窗口用量达到这个百分比 → 在撞墙前切到下一把。
    /// 正常情况下的「还能用」判定线：pct < throttle 才会被选作活动 key。
    #[serde(default = "default_throttle")]
    pub throttle_threshold: f64,

    /// 挑「新活动 key」时要求 pct < 这个值（比 throttle 低，多留余量，
    /// 让新活动 key 能撑更久 → 切换更少 → 缓存局部性更好）。
    #[serde(default = "default_restore")]
    pub restore_threshold: f64,

    /// **真·用尽线**：这把 key 真的没余量了。与 throttle 是两条不同的线——
    /// throttle(95%) 是「还有余量但该提前换了」，exhausted(100%) 是「物理上没了」。
    /// 全部 key 都过了预防线时（降级档），判定放宽到这条线：只要 pct < exhausted
    /// 就还能用，避免明知有余量还去撞 429。
    /// ⚠️ 智谱的 TOKENS_LIMIT 只返回**整数** percentage（无 usage/remaining），
    /// 所以这里的分辨率就是 1%，取 100 意味着「榨到智谱自己报 100 为止」。
    #[serde(default = "default_exhausted")]
    pub exhausted_threshold: f64,

    /// 空跑模式：只打印决策，不真的调用 new-api。先用它验证逻辑。
    #[serde(default)]
    pub dry_run: bool,

    /// 状态看板监听地址。留空则不启用看板（只跑切换循环）。
    /// 看板是附属功能：监听失败只降级记 error，绝不影响切换。
    #[serde(default = "default_status_addr")]
    pub status_addr: String,

    /// 看板**面板数据**的刷新间隔（秒）。new-api 是**本地**服务（毫秒级纯读），可以高频；
    /// 与智谱用量轮询 `poll_interval_secs` **分离**——后者是外部 API，该低频。
    #[serde(default = "default_panel_interval")]
    pub panel_interval_secs: u64,

    /// 周窗口重置进入这个时间窗（小时）内的 key 视为「临期」：切换时优先选它
    /// （先烧快清零的额度，最大化使用效率）。0 = 关闭本策略（行为与旧版完全一致）。
    /// 建议 6–48h；上限 7 天（周窗口最长一周，再大等于全年轮询）。
    #[serde(default = "default_weekly_lookahead")]
    pub weekly_reset_lookahead_hours: u64,

    /// 监控哪些窗口。默认同时看 5 小时和每周（取最大使用率）。
    #[serde(default = "default_windows")]
    pub watch_windows: Vec<Window>,

    /// 三档 priority 的取值。new-api 优先路由最高 priority 的渠道；
    /// active 独占最高档 → 所有正常流量都走它；standby 作 429 兜底；
    /// exhausted 是最后手段。一般无需改。
    #[serde(default = "default_p_active")]
    pub priority_active: i64,
    #[serde(default = "default_p_standby")]
    pub priority_standby: i64,
    #[serde(default = "default_p_exhausted")]
    pub priority_exhausted: i64,

    pub zhipu: ZhipuConfig,
    pub new_api: NewApiConfig,
    pub keys: Vec<KeyMapping>,

    /// 缓存命中池代理（F4）。默认关——关 = 逐字节回到旧拓扑。
    #[serde(default)]
    pub cache_pool: CachePoolConfig,

    /// 高峰时段（智谱自己的概念）。缺省则看板不显示这块。
    #[serde(default)]
    pub peak: Option<PeakConfig>,
}

/// 智谱的**高峰时段扣减系数**。这不是限额，是「同一个请求在高峰期烧掉几倍额度」。
///
/// 依据（2026-07 官方文档交叉验证：coding-plan/faq + coding-plan/overview）：
///   · 高峰期 = **每日 14:00–18:00（UTC+8）**，固定，不随流量浮动。
///   · GLM-5.2 / GLM-5-Turbo：高峰 **3 倍**，非高峰 **2 倍**；
///     限时福利——非高峰仅 **1 倍**，**持续到 9 月底**（到期后要把 off_peak 改回 2.0）。
///   · GLM-4.7 等普通模型：1 倍。
///
/// ⚠️ 智谱**没有任何接口**能查「现在是不是高峰」（quota/limit 的响应里没有这个字段，
/// 官方文档也没有该接口）。所以只能按时钟算——好在窗口是固定的，纯函数即可。
#[derive(Debug, Clone, Deserialize)]
pub struct PeakConfig {
    #[serde(default = "default_peak_start")]
    pub start_hour: i64,
    #[serde(default = "default_peak_end")]
    pub end_hour: i64,
    /// 高峰窗口是按 **UTC+8** 定义的。**不要用本机时区**——换台机器就错了。
    #[serde(default = "default_peak_tz")]
    pub tz_offset_hours: i64,
    /// 看板上显示的备注（如福利到期日）
    #[serde(default)]
    pub note: String,
    /// 受系数影响的模型；未列出的模型一律按 1 倍，不展示。
    #[serde(default)]
    pub coefficients: Vec<PeakCoefficient>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeakCoefficient {
    pub model: String,
    pub peak: f64,
    pub off_peak: f64,
}

fn default_peak_start() -> i64 {
    14
}
fn default_peak_end() -> i64 {
    18
}
fn default_peak_tz() -> i64 {
    8
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Window {
    FiveHour,
    Weekly,
}

fn default_status_addr() -> String {
    "127.0.0.1:3001".to_string()
}
fn default_panel_interval() -> u64 {
    5
}
fn default_throttle() -> f64 {
    95.0
}
fn default_restore() -> f64 {
    90.0
}
fn default_exhausted() -> f64 {
    100.0
}
fn default_weekly_lookahead() -> u64 {
    24
}
fn default_windows() -> Vec<Window> {
    vec![Window::FiveHour, Window::Weekly]
}
fn default_p_active() -> i64 {
    100
}
fn default_p_standby() -> i64 {
    10
}
fn default_p_exhausted() -> i64 {
    0
}

#[derive(Debug, Clone, Deserialize)]
pub struct ZhipuConfig {
    /// 智谱用量查询端点。
    /// ⚠️ 团体套餐必须带 `?type=2`（团队额度作用域）——不带会返回「当前用户不存在coding plan」。
    /// 个人套餐去掉该查询参数即可。国际版 z.ai 换成对应主机。
    #[serde(default = "default_quota_url")]
    pub quota_url: String,

    /// 全局默认 selector header（各 key 未单独配置时的兜底）。
    /// 多把 key 同组织/项目时只写一遍即可。
    #[serde(default)]
    pub extra_headers: Vec<HeaderKV>,
}

fn default_quota_url() -> String {
    "https://open.bigmodel.cn/api/monitor/usage/quota/limit?type=2".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct NewApiConfig {
    /// 例如 http://127.0.0.1:3000
    #[serde(default = "default_base_url")]
    pub base_url: String,
    /// 管理认证优先用这个“系统访问令牌”；留空则用下面的 root 账号自动登录拿会话。
    #[serde(default)]
    pub admin_token: String,
    /// admin_token 为空时用它登录 new-api（首启默认 root/123456）。
    #[serde(default = "default_root_user")]
    pub root_username: String,
    #[serde(default = "default_root_pass")]
    pub root_password: String,
    /// 渠道管理路径。用 F12 核实你的版本，默认 /api/channel
    #[serde(default = "default_channel_path")]
    pub channel_path: String,
    /// 部分版本的管理 API 需要额外 header（如 New-Api-User: <管理员 user id>）。
    #[serde(default)]
    pub extra_headers: Vec<HeaderKV>,
    /// 由本工具下载并托管 new-api 进程（选项二：零前置一键起）。
    #[serde(default)]
    pub manage: Option<ManageConfig>,
    /// sync 建渠道用的模板（版本相关字段，F12 对齐）。缺省则不自动建渠道，只按 name 解析已有渠道。
    #[serde(default)]
    pub channel_template: Option<ChannelTemplate>,
}

fn default_base_url() -> String {
    "http://127.0.0.1:3000".to_string()
}
fn default_root_user() -> String {
    "root".to_string()
}
fn default_root_pass() -> String {
    // 新版 new-api 首启要求密码 ≥8 位；本工具首启会用它创建管理员。建议改成你自己的。
    "changeme123".to_string()
}
fn default_channel_path() -> String {
    "/api/channel".to_string()
}

/// 让本工具自己下载 new-api release 二进制并作为原生进程托管。
#[derive(Debug, Clone, Deserialize)]
pub struct ManageConfig {
    /// GitHub release tag，例如 v1.0.0-rc.20
    #[serde(default = "default_newapi_version")]
    pub version: String,
    /// 监听端口（应与 base_url 里的端口一致）
    #[serde(default = "default_newapi_port")]
    pub port: u16,
    /// 存放二进制 / SQLite / 日志 / PID 的目录
    #[serde(default = "default_newapi_data_dir")]
    pub data_dir: String,
    /// GitHub 仓库，默认官方 new-api
    #[serde(default = "default_newapi_repo")]
    pub repo: String,
    /// 启动时把管理用户（root_username）的 new-api 内部额度自动调到多少**货币单位**
    /// （1 单位 = 500000 quota）。默认 2 亿。**只调大不调小**。
    /// new-api 按「按量付费倍率」给包月套餐虚构记账，额度见底会 403 挡转发（预扣费），
    /// 管理面无改额度 API（EditWithTx 白名单不含 quota 还假成功）——托管模式直写 SQLite。
    #[serde(default = "default_root_user_quota_units")]
    pub root_user_quota_units: u64,
}

fn default_newapi_version() -> String {
    "v1.0.0-rc.20".to_string()
}
fn default_newapi_port() -> u16 {
    3000
}
fn default_newapi_data_dir() -> String {
    "./.newapi".to_string()
}
fn default_newapi_repo() -> String {
    "QuantumNous/new-api".to_string()
}
fn default_root_user_quota_units() -> u64 {
    200_000_000 // 2 亿货币单位 = 1e14 quota，按倍率记账基本烧不完
}

/// 缓存命中池（F4）：代理接管 base_url 端口做逐请求路由。
/// `enabled = false`（默认）= 完全回到旧拓扑（客户端直连 new-api），存量配置零迁移。
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct CachePoolConfig {
    /// 开启后：代理监听 base_url 的 host:port，new-api 挪到 upstream（见校验）
    pub enabled: bool,
    /// 仅**非托管模式**（无 [new_api.manage]）必填：外部 new-api 的地址。
    /// 托管模式自动 = http://127.0.0.1:{manage.port}
    pub upstream_url: String,
    /// 周额度总量 : 5小时额度总量（默认 15.5/3.5）。评分用：周剩余×该比值折算成
    /// 相当于多少比例的 5h 容量，与 5h 剩余取 min——找一个能扛住新请求上下文的渠道。
    pub weekly_to_five_hour_ratio: f64,
    /// 并发上限（信号量 permits，按连接计——慢客户端占坑即背压）
    pub max_concurrency: usize,
    /// 单请求体上限（字节），超限 413。防 OOM 先于防 413。
    pub max_body_bytes: usize,
    /// 命中渠道限速后的等待退避表（毫秒，依次用尽）——命中请求 429 不迁移，
    /// 等待重试原渠道（保缓存；冷却只挡新请求的评分选路）。
    #[serde(default = "default_affinity_retry_wait_ms")]
    pub affinity_retry_wait_ms: Vec<u64>,
}

impl Default for CachePoolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            upstream_url: String::new(),
            weekly_to_five_hour_ratio: 15.5 / 3.5,
            max_concurrency: 64,
            max_body_bytes: 16 * 1024 * 1024,
            affinity_retry_wait_ms: default_affinity_retry_wait_ms(),
        }
    }
}

fn default_affinity_retry_wait_ms() -> Vec<u64> {
    vec![1500, 3000]
}

/// 建渠道模板：sync 时把每把 key 的 name/key/priority 合并进来 POST /api/channel。
#[derive(Debug, Clone, Deserialize)]
pub struct ChannelTemplate {
    /// 渠道类型码（F12 核实；智谱可用 OpenAI 兼容或专用类型）
    #[serde(rename = "type", default = "default_channel_type")]
    pub channel_type: i64,
    /// 智谱上游 base_url，例如 https://open.bigmodel.cn/api/paas/v4
    pub base_url: String,
    /// 逗号分隔的模型名，例如 "glm-4.6,glm-4.5"
    pub models: String,
    /// 分组名
    #[serde(default = "default_group")]
    pub group: String,
    /// 可选的上游 `/models` 发现。成功结果是权威目录；`models` 仅作新建时 fallback。
    /// 旧智谱 Coding 配置缺省此块时，`Config::load` 会自动补官方 models 端点。
    #[serde(default)]
    pub model_discovery: Option<ModelDiscoveryConfig>,
}

/// 上游模型目录的鉴权形态。不同 OpenAI 兼容网关并不统一：智谱用 Bearer，
/// 火山自定义 APIG 实测使用原始 Authorization。
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ModelDiscoveryAuth {
    #[default]
    Bearer,
    AuthorizationRaw,
    XApiKey,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Hash)]
pub struct ModelDiscoveryConfig {
    /// 完整 models URL，例如 https://open.bigmodel.cn/api/coding/paas/v4/models
    pub url: String,
    #[serde(default)]
    pub auth: ModelDiscoveryAuth,
}

fn default_channel_type() -> i64 {
    // 8 = Custom：原样透传 base_url 全路径。智谱 coding 口 /v4/chat/completions
    // 用 OpenAI 类型(1) 会被拼成 /v4/v1/... 而 404，故默认 Custom。
    8
}
fn default_group() -> String {
    "default".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct HeaderKV {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KeyMapping {
    /// 便于日志辨认，例如 zhipu-1；sync 时也作为 new-api 渠道名，用于解析 channel_id
    pub name: String,
    /// 这把智谱 key（探针直接拿它调智谱用量 API；sync 建渠道时也用它）
    pub zhipu_api_key: String,
    /// 这把 key 在 new-api 里对应的渠道 id。可留空，交给 sync 按 name 自动解析/创建。
    /// 统一规则：**活跃 key 持有 channel_id，弃用时清空**（渠道被删，id 必失效）。
    #[serde(default)]
    pub channel_id: Option<i64>,

    /// 人类可读的备注（如持有人名字），只在看板显示，不参与任何逻辑。留空即不显示。
    #[serde(default)]
    pub note: String,

    /// **弃用标志**。true = 不再调度、new-api 渠道已删，但条目保留在 config.toml
    /// （凭据还在，可随时恢复重建渠道）。旧配置无此字段 = 活跃。
    #[serde(default)]
    pub deprecated: Option<bool>,

    /// 该 key 查询用量时附加的 selector header。团体套餐必需
    /// （Bigmodel-Organization / Bigmodel-Project）——**不同 key 可能属于不同组织/项目，
    /// 故按 key 配置**。留空则回退到 [zhipu].extra_headers 的全局兜底。
    #[serde(default)]
    pub quota_headers: Vec<HeaderKV>,
}

impl KeyMapping {
    pub fn is_deprecated(&self) -> bool {
        self.deprecated.unwrap_or(false)
    }
}

/// channel_id 解析完成后的可用条目（orchestrator 直接用它）。
#[derive(Debug, Clone)]
pub struct ResolvedKey {
    pub name: String,
    pub zhipu_api_key: String,
    pub channel_id: i64,
    /// 人类可读的备注（透传自 KeyMapping），只显示不参与逻辑
    pub note: String,
    /// per-key 的用量查询 selector header（透传自 KeyMapping）
    pub quota_headers: Vec<HeaderKV>,
}

/// 面板提交的新 key。
#[derive(Debug, Clone, Deserialize)]
pub struct NewKeySpec {
    pub name: String,
    pub api_key: String,
    /// 人类可读备注（如持有人名字），可选
    #[serde(default)]
    pub note: String,
    /// 团体套餐的 selector。个人套餐留空。
    #[serde(default)]
    pub org: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
}

/// org/project → 查用量时要带的 selector header。**团体套餐缺了它就查不到**
/// （返回 limits 空），而空 limits 会被误当成 0% 用量 → 这把 key 永远不切换。
/// 录入/改 selector 时必须探活。空白视同没填。
pub fn selector_headers(org: Option<&str>, project: Option<&str>) -> Vec<HeaderKV> {
    [("Bigmodel-Organization", org), ("Bigmodel-Project", project)]
        .into_iter()
        .filter_map(|(k, v)| {
            let v = v.map(str::trim).filter(|s| !s.is_empty())?;
            Some(HeaderKV {
                key: k.to_string(),
                value: v.to_string(),
            })
        })
        .collect()
}

impl NewKeySpec {
    pub fn headers(&self) -> Vec<HeaderKV> {
        selector_headers(self.org.as_deref(), self.project.as_deref())
    }
}

/// 原子写：临时文件 → fsync → rename。
/// 直接覆写 config.toml 的话，进程若在写一半时挂掉，用户的配置就被截断了。
fn write_atomic(path: &str, bytes: &[u8]) -> anyhow::Result<()> {
    let tmp = format!("{path}.tmp");
    {
        let mut f = std::fs::File::create(&tmp)
            .with_context(|| format!("创建临时文件失败: {tmp}"))?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("替换 {path} 失败"))?;
    Ok(())
}

fn validate_model_discovery(label: &str, cfg: Option<&ModelDiscoveryConfig>) -> anyhow::Result<()> {
    let Some(cfg) = cfg else { return Ok(()) };
    anyhow::ensure!(
        !cfg.url.trim().is_empty() && cfg.url == cfg.url.trim(),
        "{label}.model_discovery.url 非法：不能为空或带首尾空白"
    );
    let url = reqwest::Url::parse(&cfg.url)
        .with_context(|| format!("{label}.model_discovery.url 不是合法绝对 URL: {}", cfg.url))?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https") && url.has_host(),
        "{label}.model_discovery.url 只允许带主机的 http/https 绝对 URL: {}",
        cfg.url
    );
    Ok(())
}

/// 只对已经明确识别出的智谱 Coding Plan 模板补默认发现配置；其它 Custom 上游不猜。
fn infer_zhipu_model_discovery(template: &ChannelTemplate) -> Option<ModelDiscoveryConfig> {
    let url = reqwest::Url::parse(&template.base_url).ok()?;
    if url.host_str() != Some("open.bigmodel.cn")
        || url.path().trim_end_matches('/')
            != "/api/coding/paas/v4/chat/completions"
    {
        return None;
    }
    Some(ModelDiscoveryConfig {
        url: "https://open.bigmodel.cn/api/coding/paas/v4/models".to_string(),
        auth: ModelDiscoveryAuth::Bearer,
    })
}

/// 往 config.toml 追加一条 `[[keys]]`。
///
/// 用 **toml_edit**（格式保留式编辑）而不是 `toml::to_string` 重新序列化——后者会把用户
/// 手写的注释、空行、排版**全部冲掉**，而这个项目的 config.toml 里写满了踩坑说明。
pub fn append_key(path: &str, spec: &NewKeySpec) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(path).with_context(|| format!("读取 {path} 失败"))?;
    let mut doc: toml_edit::DocumentMut = text.parse().context("config.toml 不是合法 TOML")?;

    if !doc.contains_key("keys") {
        doc["keys"] = toml_edit::Item::ArrayOfTables(toml_edit::ArrayOfTables::new());
    }
    let keys = doc["keys"]
        .as_array_of_tables_mut()
        .context("config.toml 里的 keys 不是 [[keys]] 表数组")?;
    if keys
        .iter()
        .any(|t| t.get("name").and_then(|v| v.as_str()) == Some(spec.name.as_str()))
    {
        bail!("config.toml 里已存在同名 key: {}", spec.name);
    }

    let mut t = toml_edit::Table::new();
    t["name"] = toml_edit::value(spec.name.clone());
    t["zhipu_api_key"] = toml_edit::value(spec.api_key.clone());
    if !spec.note.trim().is_empty() {
        t["note"] = toml_edit::value(spec.note.trim());
    }
    let hs = spec.headers();
    if !hs.is_empty() {
        let mut arr = toml_edit::ArrayOfTables::new();
        for h in hs {
            let mut ht = toml_edit::Table::new();
            ht["key"] = toml_edit::value(h.key);
            ht["value"] = toml_edit::value(h.value);
            arr.push(ht);
        }
        t.insert("quota_headers", toml_edit::Item::ArrayOfTables(arr));
    }
    keys.push(t);

    write_atomic(path, doc.to_string().as_bytes())
}

/// 定位同名 `[[keys]]` 条目（找返回可变引用；找不到返回 None）。
fn key_table_mut<'a>(
    keys: &'a mut toml_edit::ArrayOfTables,
    name: &str,
) -> Option<&'a mut toml_edit::Table> {
    keys.iter_mut()
        .find(|t| t.get("name").and_then(|v| v.as_str()) == Some(name))
}

/// 把一条 `[[keys]]` 标记为弃用：置 `deprecated = true` 并清掉 `channel_id`
/// （渠道将被删除，id 必失效；恢复时会重建渠道拿新 id）。
/// **条目本身保留**——弃用的语义是「不再调度但凭据留存、可恢复」，不是「抹掉这把 key」。
pub fn deprecate_key(path: &str, name: &str) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(path).with_context(|| format!("读取 {path} 失败"))?;
    let mut doc: toml_edit::DocumentMut = text.parse().context("config.toml 不是合法 TOML")?;
    let keys = doc["keys"]
        .as_array_of_tables_mut()
        .context("config.toml 里的 keys 不是 [[keys]] 表数组")?;
    let t = key_table_mut(keys, name).ok_or_else(|| anyhow::anyhow!("config.toml 里没有名为 {name} 的 key"))?;
    t["deprecated"] = toml_edit::value(true);
    t.remove("channel_id");
    write_atomic(path, doc.to_string().as_bytes())
}

/// 恢复一条弃用的 `[[keys]]`：单次原子写——去 deprecated 标志 + 落新 channel_id
/// （恢复流程重建渠道拿到的 id）。两次分开写之间崩溃会留下「磁盘说活跃、
/// 内存说弃用」的半恢复态，故合并。
pub fn restore_key(path: &str, name: &str, channel_id: i64) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(path).with_context(|| format!("读取 {path} 失败"))?;
    let mut doc: toml_edit::DocumentMut = text.parse().context("config.toml 不是合法 TOML")?;
    let keys = doc["keys"]
        .as_array_of_tables_mut()
        .context("config.toml 里的 keys 不是 [[keys]] 表数组")?;
    let t = key_table_mut(keys, name).ok_or_else(|| anyhow::anyhow!("config.toml 里没有名为 {name} 的 key"))?;
    t.remove("deprecated");
    t["channel_id"] = toml_edit::value(channel_id);
    write_atomic(path, doc.to_string().as_bytes())
}

/// 面板编辑 key 元数据的补丁。任何字段 None = 不改；org/project 出现任一非 None
/// 即进入「重建 selector」模式（两个都按传入值算，None/空 = 清该 header）。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct KeyPatch {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub org: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
}

/// 单次原子写更新一条 `[[keys]]` 的元数据（name/note/quota_headers，各字段可选）。
/// 改名只是换 `name` 值——channel_id 等其余字段原样保留。拆成多次写会有半更新态。
pub fn update_key_meta(
    path: &str,
    old_name: &str,
    new_name: Option<&str>,
    note: Option<&str>,
    quota_headers: Option<&[HeaderKV]>,
) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(path).with_context(|| format!("读取 {path} 失败"))?;
    let mut doc: toml_edit::DocumentMut = text.parse().context("config.toml 不是合法 TOML")?;
    let keys = doc["keys"]
        .as_array_of_tables_mut()
        .context("config.toml 里的 keys 不是 [[keys]] 表数组")?;
    let t =
        key_table_mut(keys, old_name).ok_or_else(|| anyhow::anyhow!("config.toml 里没有名为 {old_name} 的 key"))?;
    if let Some(n) = new_name {
        t["name"] = toml_edit::value(n);
    }
    if let Some(n) = note {
        let n = n.trim();
        if n.is_empty() {
            t.remove("note");
        } else {
            t["note"] = toml_edit::value(n);
        }
    }
    if let Some(hs) = quota_headers {
        t.remove("quota_headers");
        if !hs.is_empty() {
            let mut arr = toml_edit::ArrayOfTables::new();
            for h in hs {
                let mut ht = toml_edit::Table::new();
                ht["key"] = toml_edit::value(h.key.clone());
                ht["value"] = toml_edit::value(h.value.clone());
                arr.push(ht);
            }
            t.insert("quota_headers", toml_edit::Item::ArrayOfTables(arr));
        }
    }
    write_atomic(path, doc.to_string().as_bytes())
}

/// 把解析/新建得到的 channel_id 落进 `[[keys]]`（活跃 key 持有 id 的统一规则）。
/// 显式落盘后按 id 匹配可容忍渠道改名（F1 的启动对齐依赖它）。
pub fn set_key_channel_id(path: &str, name: &str, channel_id: i64) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(path).with_context(|| format!("读取 {path} 失败"))?;
    let mut doc: toml_edit::DocumentMut = text.parse().context("config.toml 不是合法 TOML")?;
    let keys = doc["keys"]
        .as_array_of_tables_mut()
        .context("config.toml 里的 keys 不是 [[keys]] 表数组")?;
    let t = key_table_mut(keys, name).ok_or_else(|| anyhow::anyhow!("config.toml 里没有名为 {name} 的 key"))?;
    t["channel_id"] = toml_edit::value(channel_id);
    write_atomic(path, doc.to_string().as_bytes())
}

/// 从 config.toml 读回一条 key（恢复流程用：弃用条目不在 orchestrator 内存里，
/// 凭据只能从「唯一数据源」重新取）。
pub fn load_key(path: &str, name: &str) -> anyhow::Result<Option<KeyMapping>> {
    let cfg: Config = toml::from_str(
        &std::fs::read_to_string(path).with_context(|| format!("读取 {path} 失败"))?,
    )
    .context("config.toml 不是合法 TOML")?;
    Ok(cfg.keys.into_iter().find(|k| k.name == name))
}

impl Config {
    /// **内部 new-api 的地址**（管理面/健康检查/代理上游一律用它，与「客户端入口」
    /// base_url 分离）：cache_pool 关 = base_url（旧拓扑，逐字节等价）；开 = 托管模式
    /// `http://127.0.0.1:{manage.port}`，非托管模式 = cache_pool.upstream_url。
    pub fn upstream_base(&self) -> String {
        if !self.cache_pool.enabled {
            return self.new_api.base_url.trim_end_matches('/').to_string();
        }
        match &self.new_api.manage {
            Some(m) => format!("http://127.0.0.1:{}", m.port),
            None => self.cache_pool.upstream_url.trim_end_matches('/').to_string(),
        }
    }

    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)?;
        let mut cfg: Config = toml::from_str(&text)?;
        cfg.source_path = path.to_string_lossy().into_owned();
        if let Some(template) = cfg.new_api.channel_template.as_mut() {
            if template.model_discovery.is_none() {
                template.model_discovery = infer_zhipu_model_discovery(template);
            }
        }
        cfg.validate()?;
        Ok(cfg)
    }

    /// 阈值必须满足 0 < restore ≤ throttle ≤ exhausted ≤ 100。
    /// 配错阈值是**静默灾难**（例如 throttle > exhausted 会让合格集恒空、永不切换），
    /// 所以启动就失败，别等到线上才发现。
    fn validate(&self) -> anyhow::Result<()> {
        let (r, t, e) = (
            self.restore_threshold,
            self.throttle_threshold,
            self.exhausted_threshold,
        );
        anyhow::ensure!(
            r > 0.0 && r <= t && t <= e && e <= 100.0,
            "阈值非法：要求 0 < restore({r}) ≤ throttle({t}) ≤ exhausted({e}) ≤ 100"
        );
        // 周临期时间窗：0 = 关闭；上限 7 天 = 周窗口周期（再大等于把全部 key 都算临期）
        anyhow::ensure!(
            self.weekly_reset_lookahead_hours <= 24 * 7,
            "weekly_reset_lookahead_hours 非法：{}（要求 0–168；0 = 关闭临期优先，168 = 一周 = 全临期）",
            self.weekly_reset_lookahead_hours
        );
        if let Some(p) = &self.peak {
            anyhow::ensure!(
                (0..=24).contains(&p.start_hour)
                    && (0..=24).contains(&p.end_hour)
                    && p.start_hour < p.end_hour,
                "[peak] 时段非法：要求 0 ≤ start_hour({}) < end_hour({}) ≤ 24",
                p.start_hour,
                p.end_hour
            );
        }
        validate_model_discovery(
            "[new_api.channel_template]",
            self.new_api
                .channel_template
                .as_ref()
                .and_then(|t| t.model_discovery.as_ref()),
        )?;
        self.validate_cache_pool()?;
        Ok(())
    }

    /// cache_pool 端口拓扑校验（F4）。原则：enabled=false 时逐字节回到旧拓扑、
    /// 零迁移；enabled=true 时三个端口（代理=base_url / upstream / 看板）必须互不相等
    /// ——相等意味着「代理打到自己」或「new-api 和代理抢一个端口」。
    fn validate_cache_pool(&self) -> anyhow::Result<()> {
        let cp = &self.cache_pool;
        if !cp.enabled {
            if !cp.upstream_url.trim().is_empty() {
                warn!("[cache_pool].upstream_url 已配置但 enabled=false，忽略");
            }
            return Ok(());
        }
        anyhow::ensure!(
            cp.weekly_to_five_hour_ratio > 0.0,
            "[cache_pool].weekly_to_five_hour_ratio 非法：{}（须 > 0）",
            cp.weekly_to_five_hour_ratio
        );
        anyhow::ensure!(cp.max_concurrency >= 1, "[cache_pool].max_concurrency 须 ≥ 1");
        anyhow::ensure!(cp.max_body_bytes >= 1024, "[cache_pool].max_body_bytes 须 ≥ 1 KiB");
        if self.new_api.manage.is_none() {
            anyhow::ensure!(
                !cp.upstream_url.trim().is_empty(),
                "非托管模式（无 [new_api.manage]）开 cache_pool 必须显式配 [cache_pool].upstream_url"
            );
        }
        let port_of = |url: &str| -> anyhow::Result<u16> {
            reqwest::Url::parse(url)
                .with_context(|| format!("URL 非法：{url}"))?
                .port_or_known_default()
                .ok_or_else(|| anyhow::anyhow!("URL 缺端口：{url}"))
        };
        let proxy_port = port_of(&self.new_api.base_url).context("[new_api].base_url")?;
        let upstream_port = port_of(&self.upstream_base()).context("cache_pool upstream")?;
        anyhow::ensure!(
            proxy_port != upstream_port,
            "cache_pool 开启但 base_url 端口({proxy_port}) = upstream 端口({upstream_port})：\
             代理会打到自己。托管模式请把 [new_api.manage].port 改成内部端口（如 13000）"
        );
        if !self.status_addr.trim().is_empty() {
            let status_port = port_of(&format!("http://{}", self.status_addr))
                .context("status_addr")
                .unwrap_or_else(|_| {
                    self.status_addr
                        .rsplit(':')
                        .next()
                        .and_then(|p| p.parse().ok())
                        .unwrap_or(0)
                });
            anyhow::ensure!(
                status_port != proxy_port && status_port != upstream_port,
                "status_addr 端口({status_port}) 与代理({proxy_port})/upstream({upstream_port}) 相冲"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一份**带注释、带排版**的配置：这正是我们要保住的东西。
    const SAMPLE: &str = r#"# 顶部注释：别被冲掉
poll_interval_secs = 60
throttle_threshold = 95.0   # 行尾注释
restore_threshold = 90.0

[zhipu]
quota_url = "https://open.bigmodel.cn/api/monitor/usage/quota/limit?type=2"

[new_api]
base_url = "http://127.0.0.1:3000"

# 下面是 key 列表
[[keys]]
name = "zhipu-1"
zhipu_api_key = "k1"
[[keys.quota_headers]]
key = "Bigmodel-Organization"
value = "org-1"
"#;

    fn tmp(tag: &str) -> String {
        let p = std::env::temp_dir().join(format!("qt-cfg-{}-{tag}.toml", std::process::id()));
        std::fs::write(&p, SAMPLE).unwrap();
        p.to_string_lossy().into_owned()
    }

    fn spec(name: &str) -> NewKeySpec {
        NewKeySpec {
            name: name.into(),
            api_key: "k2".into(),
            note: String::new(),
            org: Some("org-2".into()),
            project: Some("proj-2".into()),
        }
    }

    #[test]
    fn 追加key_不冲掉注释与排版() {
        let p = tmp("append");
        append_key(&p, &spec("zhipu-2")).unwrap();
        let out = std::fs::read_to_string(&p).unwrap();

        // 注释、行尾注释、原有内容一个字符都不能少
        assert!(out.contains("# 顶部注释：别被冲掉"));
        assert!(out.contains("throttle_threshold = 95.0   # 行尾注释"));
        assert!(out.contains("# 下面是 key 列表"));
        assert!(out.contains(r#"value = "org-1""#));

        // 新 key 进去了，且能被正常解析回来
        let cfg: Config = toml::from_str(&out).unwrap();
        assert_eq!(cfg.keys.len(), 2);
        let k = &cfg.keys[1];
        assert_eq!(k.name, "zhipu-2");
        assert_eq!(k.zhipu_api_key, "k2");
        assert_eq!(k.quota_headers.len(), 2);
        assert_eq!(k.quota_headers[0].key, "Bigmodel-Organization");
        assert_eq!(k.quota_headers[1].value, "proj-2");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 同名key_拒绝追加() {
        let p = tmp("dup");
        assert!(append_key(&p, &spec("zhipu-1")).is_err());
        // 失败时不能留下任何改动
        assert_eq!(std::fs::read_to_string(&p).unwrap(), SAMPLE);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 弃用key_保留条目打标志_其余原样() {
        let p = tmp("deprecate");
        set_key_channel_id(&p, "zhipu-1", 7).unwrap();
        deprecate_key(&p, "zhipu-1").unwrap();
        let out = std::fs::read_to_string(&p).unwrap();

        // 注释排版保住；条目还在但带标志，channel_id 已清（渠道将删除，id 必失效）
        assert!(out.contains("# 顶部注释：别被冲掉"));
        assert!(out.contains("# 下面是 key 列表"));
        assert!(out.contains("deprecated = true"));
        assert!(!out.contains("channel_id"));
        let cfg: Config = toml::from_str(&out).unwrap();
        assert_eq!(cfg.keys.len(), 1);
        assert!(cfg.keys[0].is_deprecated());
        assert_eq!(cfg.keys[0].channel_id, None);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 恢复key_单次写去掉标志并落id_旧配置无字段视为活跃() {
        let p = tmp("restore");
        deprecate_key(&p, "zhipu-1").unwrap();
        restore_key(&p, "zhipu-1", 99).unwrap();
        let out = std::fs::read_to_string(&p).unwrap();
        assert!(!out.contains("deprecated"));
        let cfg: Config = toml::from_str(&out).unwrap();
        assert!(!cfg.keys[0].is_deprecated());
        assert_eq!(cfg.keys[0].channel_id, Some(99));

        // 旧配置（无该字段）= 活跃
        let cfg: Config = toml::from_str(SAMPLE).unwrap();
        assert!(!cfg.keys[0].is_deprecated());
        std::fs::remove_file(&p).ok();
    }

    /// 多条目下按名定位必须命中**正确那张表**、其余条目一个字符不动
    /// （code-review：旧「删除key」测试守过这半个面，替换它的测试全是单条目）。
    #[test]
    fn 多条目_只动目标条目_其余原样() {
        let p = tmp("multi");
        append_key(&p, &spec("zhipu-2")).unwrap();
        deprecate_key(&p, "zhipu-1").unwrap();
        set_key_channel_id(&p, "zhipu-2", 5).unwrap();
        restore_key(&p, "zhipu-1", 7).unwrap();
        let out = std::fs::read_to_string(&p).unwrap();

        let cfg: Config = toml::from_str(&out).unwrap();
        assert_eq!(cfg.keys.len(), 2);
        assert!(!cfg.keys[0].is_deprecated(), "zhipu-1 应已恢复");
        assert_eq!(cfg.keys[0].channel_id, Some(7));
        assert_eq!(cfg.keys[1].name, "zhipu-2");
        assert_eq!(cfg.keys[1].channel_id, Some(5));
        assert_eq!(cfg.keys[1].quota_headers.len(), 2, "zhipu-2 的 selector 不能被动");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 弃用不存在的key_报错() {
        let p = tmp("nomatch");
        assert!(deprecate_key(&p, "不存在").is_err());
        // 失败时不能留下任何改动
        assert_eq!(std::fs::read_to_string(&p).unwrap(), SAMPLE);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 落channel_id_保留注释并可读回() {
        let p = tmp("setid");
        set_key_channel_id(&p, "zhipu-1", 42).unwrap();
        let out = std::fs::read_to_string(&p).unwrap();
        assert!(out.contains("# 顶部注释：别被冲掉"));
        let cfg: Config = toml::from_str(&out).unwrap();
        assert_eq!(cfg.keys[0].channel_id, Some(42));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 读回key_按名取条目() {
        let p = tmp("loadkey");
        let k = load_key(&p, "zhipu-1").unwrap().unwrap();
        assert_eq!(k.zhipu_api_key, "k1");
        assert_eq!(k.quota_headers.len(), 1);
        assert!(load_key(&p, "不存在").unwrap().is_none());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 个人套餐_不填selector_则不写quota_headers() {
        let p = tmp("nosel");
        append_key(
            &p,
            &NewKeySpec {
                name: "personal".into(),
                api_key: "k3".into(),
                note: String::new(),
                org: None,
                project: Some("  ".into()), // 空白应被当作没填
            },
        )
        .unwrap();
        let cfg: Config = toml::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        assert!(cfg.keys[1].quota_headers.is_empty());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 阈值非法_启动即失败() {
        // throttle > exhausted 会让合格集恒空、永不切换 —— 必须在启动时就拦住
        let p = std::env::temp_dir().join(format!("qt-cfg-{}-bad.toml", std::process::id()));
        std::fs::write(&p, SAMPLE.replace("throttle_threshold = 95.0", "throttle_threshold = 101.0")).unwrap();
        assert!(Config::load(&p).is_err());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 周临期时间窗_缺省24_填0合法_超一周失败() {
        // 缺省 = 24（策略默认开）
        let p = tmp("lk-default");
        let cfg = Config::load(&p).unwrap();
        assert_eq!(cfg.weekly_reset_lookahead_hours, 24);
        std::fs::remove_file(&p).ok();

        // 0 = 显式关闭（⚠️ 必须写在 [[keys]] 之前——TOML 里 key 归属最近的表头，
        // 追加在 keys 表之后会被挂进 keys.quota_headers 而不是顶层）
        let p = tmp("lk-zero");
        std::fs::write(
            &p,
            SAMPLE.replace("poll_interval_secs", "weekly_reset_lookahead_hours = 0\npoll_interval_secs"),
        )
        .unwrap();
        assert_eq!(Config::load(&p).unwrap().weekly_reset_lookahead_hours, 0);
        std::fs::remove_file(&p).ok();

        // 168 = 一周（上限，合法）；169 = 手误，启动即失败
        let p = tmp("lk-max");
        std::fs::write(
            &p,
            SAMPLE.replace("poll_interval_secs", "weekly_reset_lookahead_hours = 169\npoll_interval_secs"),
        )
        .unwrap();
        assert!(Config::load(&p).is_err(), "169h 超过周周期，应拦截");
        std::fs::remove_file(&p).ok();
    }

    /// SAMPLE + 追加模型发现配置，落盘后返回路径。
    fn model_cfg(extra: &str) -> String {
        let p = std::env::temp_dir().join(format!(
            "qt-cfg-{}-models-{}.toml",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&p, format!("{SAMPLE}{extra}")).unwrap();
        p.to_string_lossy().into_owned()
    }

    const OPENAI_TPL: &str = r#"
[new_api.channel_template]
type = 8
base_url = "https://open.bigmodel.cn/api/coding/paas/v4/chat/completions"
models = "glm-5.2"
group = "default"
"#;

    #[test]
    fn 模型发现_旧智谱配置自动启用且鉴权默认bearer() {
        let p = model_cfg(&format!(
            "{OPENAI_TPL}\n[new_api.channel_template.model_discovery]\n\
             url = \"https://open.bigmodel.cn/api/coding/paas/v4/models\"\n"
        ));
        let cfg = Config::load(&p).unwrap();
        let discovery = cfg
            .new_api
            .channel_template
            .as_ref()
            .unwrap()
            .model_discovery
            .as_ref()
            .unwrap();
        assert_eq!(discovery.auth, ModelDiscoveryAuth::Bearer);
        std::fs::remove_file(&p).ok();

        let p = model_cfg(OPENAI_TPL);
        let cfg = Config::load(&p).unwrap();
        let inferred = cfg
            .new_api
            .channel_template
            .as_ref()
            .unwrap()
            .model_discovery
            .as_ref()
            .unwrap();
        assert_eq!(
            inferred.url,
            "https://open.bigmodel.cn/api/coding/paas/v4/models"
        );
        assert_eq!(inferred.auth, ModelDiscoveryAuth::Bearer);
        std::fs::remove_file(&p).ok();

        // 非智谱 Custom 上游缺省时保持 None，不能凭空猜鉴权和 models URL。
        let custom = OPENAI_TPL.replace(
            "https://open.bigmodel.cn/api/coding/paas/v4/chat/completions",
            "https://gateway.example/v1/chat/completions",
        );
        let p = model_cfg(&custom);
        let cfg = Config::load(&p).unwrap();
        assert!(cfg
            .new_api
            .channel_template
            .as_ref()
            .unwrap()
            .model_discovery
            .is_none());
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn 模型发现_支持raw与x_api_key鉴权() {
        for (auth, expected) in [
            ("authorization_raw", ModelDiscoveryAuth::AuthorizationRaw),
            ("x_api_key", ModelDiscoveryAuth::XApiKey),
        ] {
            let p = model_cfg(&format!(
                "{OPENAI_TPL}\n[new_api.channel_template.model_discovery]\n\
                 url = \"https://gateway.example/v1/models\"\n\
                 auth = \"{auth}\"\n"
            ));
            let cfg = Config::load(&p).unwrap();
            assert_eq!(
                cfg.new_api
                    .channel_template
                    .as_ref()
                    .unwrap()
                    .model_discovery
                    .as_ref()
                    .unwrap()
                    .auth,
                expected
            );
            std::fs::remove_file(&p).ok();
        }
    }

    #[test]
    fn 模型发现_非法url启动即失败() {
        for url in ["models", "file:///tmp/models", " https://example/models"] {
            let p = model_cfg(&format!(
                "{OPENAI_TPL}\n[new_api.channel_template.model_discovery]\nurl = \"{url}\"\n"
            ));
            assert!(Config::load(&p).is_err(), "应拒绝 URL: {url}");
            std::fs::remove_file(&p).ok();
        }
    }

    #[test]
    fn 示例配置可解析且默认开启智谱模型发现() {
        let cfg: Config = toml::from_str(include_str!("../config.example.toml")).unwrap();
        cfg.validate().unwrap();
        let openai = cfg.new_api.channel_template.as_ref().unwrap();
        assert_eq!(
            openai.model_discovery.as_ref().unwrap().auth,
            ModelDiscoveryAuth::Bearer
        );
    }
}
