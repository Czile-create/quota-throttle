//! 控制循环：维护「单把活动 key」，每轮对每把 key 查用量，重排各渠道 priority。
//!
//! 目标——把「选哪把 key」的决策从 new-api 的加权随机收回到本程序，钉住一把活动 key，
//! 让 prompt 缓存能连续命中，只有切换那一下才 miss。
//!
//! 路由杠杆用 priority（不是 weight）：new-api 优先路由最高 priority 的渠道。
//!   - active   (最高档)：所有正常流量都走它。
//!   - standby  (中档)  ：合格但非活动，平时不碰，只作 429 反应式兜底目标。
//!   - exhausted(最低档)：不合格，最后手段。
//! 用 priority 而非 weight=0，是为了在活动 key 撞墙时，new-api 仍能沿 priority 阶梯
//! 自动跌到还有额度的 standby 渠道——weight=0 会把渠道从选择集里抹掉，破坏这层兜底。
//!
//! ## 两条线，不是一条
//!
//! throttle(95%) 是**预防线**——还有余量，但该提前换走了（护缓存 + 别撞墙）。
//! exhausted(100%) 是**真·用尽线**——物理上没了。
//! 二者据此分出两档「合格集」（= 自动逻辑允许把流量放上去的 key）：
//!   - **正常档**：有 key < throttle ⇒ 只在这些宽裕的里选。
//!   - **降级档**：全员 ≥ throttle ⇒ 放宽到「只要 < exhausted 就能用」。
//!     否则会出现「明知 96% 那把还有余量，却继续钉着已经 100% 的活动 key 撞 429」。
//!
//! ## 选择策略（贴合缓存局部性：能不换就不换）
//!
//!   - **pin**（用户从看板手动指定）只在合格集内生效：它是**优先级**，不是安全豁免——
//!     能覆盖「粘滞 + 挑最低」的选择偏好，但**不能把不合格的 key 拉上来用**，越线即自动解除。
//!   - **粘滞**：现任只要还在合格集里就不换（两档皆然；降级档下就是「榨干到 100% 再流转」）。
//!   - **挑选**：只在合格集内挑 pct 最低的（正常档优先要求 < restore，多留余量 → 切换更少）。
//!   - **抖动保护**：本轮查询失败的 key 不进合格集（不会被主动选中），也不动它的 priority；
//!     但若它恰好是现任或被 pin 的，则保持不变——一次瞬时失败不该丢缓存、也不该抖掉 pin。

use crate::config::{Config, NewKeySpec, ResolvedKey};
use crate::newapi::NewApiClient;
use crate::quota::{QuotaProbe, QuotaStatus};
use crate::status::{self, KeyStatus, LiveMetric, RequestLog, Shared};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

pub struct Orchestrator {
    cfg: Config,
    probe: QuotaProbe,
    api: Arc<NewApiClient>,
    /// channel_id 已解析好的 key 列表
    keys: Vec<ResolvedKey>,
    /// 弃用的 key（不在调度集；凭据留在 config.toml，恢复时从那读回）
    deprecated: Vec<status::DeprecatedKeyInfo>,
    /// 当前钉住的活动渠道 id
    active: Option<i64>,
    /// 用户从看板手动 pin 的渠道（只在合格集内生效，见 decide）
    pinned: Option<i64>,
    /// 上一次 pin 被自动解除的事件（供看板提示）
    last_pin_release: Option<PinRelease>,
    /// 上一轮的档位（判 pin 是否合法时要用它换算合格线）
    regime: Regime,
    /// 上一轮的合格集与 pct —— pin 命令据此**当场**判定能不能钉，不必等下一轮
    last_eligible: Vec<i64>,
    last_pct: HashMap<i64, f64>,
    /// 已下发到 new-api 的 priority（channel_id → priority），幂等用，避免每轮重复 PUT
    applied: HashMap<i64, i64>,
    /// 状态看板共享快照（**只写决策字段**，面板字段由 Panel 循环独占）
    snapshot: Shared,
    /// 缓存池路由状态（弃用渠道时按 channel_id 清池条目/计数；选路本身只在代理里）
    router: std::sync::Arc<crate::router::RouterState>,
    /// new-api 健康探测用
    http: reqwest::Client,
}

/// 取该 key 在所有监控窗口里的最大使用率（5h 墙和周墙谁高听谁的）。
fn max_watch_pct(cfg: &Config, status: &QuotaStatus) -> f64 {
    let mut max_pct = 0.0f64;
    for w in &cfg.watch_windows {
        if let Some(ws) = status.window(*w) {
            if ws.percentage > max_pct {
                max_pct = ws.percentage;
            }
        }
    }
    max_pct
}

/// 周窗口的决策用快照。仅决策层使用，不进快照结构。
#[derive(Debug, Clone, Copy)]
struct WeeklyInfo {
    /// 周窗口重置时刻（epoch **毫秒**，探针原样——注意与 now_secs 单位不同）
    reset_ms: i64,
    /// 周窗口已用%（临期判定的「没用完」依据）
    pct: f64,
    /// max_watch_pct（5h 与周取大）——判定「现在能不能服务」（5h 已 100% 则不可服务）
    max_pct: f64,
}

/// 是否临期：周窗口即将重置、且还有真实余量、且现在还能服务。
///
/// 四条件缺一不可（见研究文档 §5 H1–H4）：
///   1. 查到了周窗口（调用方构造了 WeeklyInfo ⇒ 隐含满足）；
///   2. `reset_ms ∈ (now, now + lookahead]` —— 左开右闭：恰好此刻清零→窗口已开始新周期，
///      不算临期；恰好 lookahead 整点→还剩 0 秒也算（端点含，避免边界抖动）；
///   3. 周窗口已用 < exhausted（还有额度可用；100% 即「没用完」的取反）；
///   4. max_pct < exhausted（5h 窗口死了 = 现在接不了流量，选了立即 429，不算可用）。
///
/// 纯函数，零网络调用。lookahead = 0 ⇒ 恒 false（条件 2 与「reset > now」互斥）⇒ 策略关闭。
fn imminent(weekly: &WeeklyInfo, now_ms: i64, lookahead_ms: i64, exhausted: f64) -> bool {
    if weekly.pct >= exhausted || weekly.max_pct >= exhausted {
        return false;
    }
    let until = weekly.reset_ms - now_ms;
    until > 0 && until <= lookahead_ms
}

/// 现在是不是高峰时段 + 下次切换的时刻（epoch **秒**）。**纯函数**，零网络调用。
///
/// 智谱没有任何接口能查这个（quota/limit 的响应里没有该字段，官方文档也没有该接口），
/// 好在窗口是固定的：**每日 14:00–18:00（UTC+8）**，按时钟算即可。
///
/// ⚠️ **按 tz_offset 算，不是按本机时区**。高峰是智谱用 UTC+8 定义的；用本机时区的话，
/// 这台机器恰好在 UTC+8 就看不出错，换台机器/换时区就全错。
fn peak_state(now_secs: i64, start_hour: i64, end_hour: i64, tz_offset_hours: i64) -> (bool, i64) {
    const DAY: i64 = 86_400;
    // 换算到「智谱时区」的当日秒数
    let sod = (now_secs + tz_offset_hours * 3600).rem_euclid(DAY);
    let (s, e) = (start_hour * 3600, end_hour * 3600);
    let is_peak = sod >= s && sod < e;
    // 下一个边界：峰中 → 峰尾；峰前 → 峰头；峰后 → 明天的峰头
    let target = if is_peak {
        e
    } else if sod < s {
        s
    } else {
        s + DAY
    };
    (is_peak, now_secs + (target - sod))
}

/// 合格集是在哪一档算出来的。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Regime {
    /// 还有 key 低于预防线 ⇒ 只在这些宽裕的 key 里选
    Normal,
    /// 全员都过了预防线 ⇒ 放宽到「只要还有余量（< exhausted）就能用」
    Degraded,
}

impl Regime {
    pub fn as_str(self) -> &'static str {
        match self {
            Regime::Normal => "normal",
            Regime::Degraded => "degraded",
        }
    }
}

/// pin 因越出合格线被**自动解除**的事件（供看板提示）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PinRelease {
    pub channel_id: i64,
    /// 解除时该 key 的 pct
    pub pct: f64,
    /// 当时的合格线（正常档=throttle，降级档=exhausted）
    pub limit: f64,
}

/// 看板（HTTP）→ 控制循环的命令。
///
/// **为什么走命令通道而不是给状态加锁**：keys / active / pinned / applied 现在只有
/// orchestrator 一个写者，无锁、无重入，并发安全性是**显然**的。让 HTTP 直接改这些状态
/// 会毁掉这个不变量。命令通道把「谁能写」这件事继续锁死在一个地方。
///
/// 每条命令带一个 oneshot 回执 ⇒ HTTP 侧能**同步拿到成败**（比如加 key 时智谱的错误原文
/// 要回显到面板上，而不是让用户去翻日志）。
#[derive(Debug)]
pub enum Command {
    /// 手动钉住某把 key（只在合格集内生效，见 decide）
    Pin {
        channel_id: i64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// 取消手动 pin，回到自动选择
    Unpin {
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// 从看板加一把 key：探活 → 建渠道 → 写回 config.toml → 热加载
    AddKey {
        spec: NewKeySpec,
        reply: oneshot::Sender<Result<AddKeyOk, String>>,
    },
    /// 弃用某把 key：删 new-api 渠道 + config.toml 打弃用标志（**条目保留**可恢复）
    DeprecateKey {
        channel_id: i64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// 恢复一把弃用 key：探活 → 重建渠道 → config 去标志并落新 channel_id → 热加载
    RestoreKey {
        name: String,
        reply: oneshot::Sender<Result<AddKeyOk, String>>,
    },
    /// 编辑活跃 key 的元数据（name/note/org/project，None=不改）。
    /// 改 selector 先探活；改 name 联动改 new-api 渠道名；config 单次原子写。
    UpdateKey {
        channel_id: i64,
        patch: crate::config::KeyPatch,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// 手动触发该 key 的上游 /models 重对账
    ResyncModels {
        channel_id: i64,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// 加 key 成功后回给面板的东西：探活结果原样奉上，让用户当场确认
/// 「这把 key 确实是我以为的那把」（套餐档位 + 两个窗口的真实用量）。
#[derive(Debug, Clone, Serialize)]
pub struct AddKeyOk {
    pub channel_id: i64,
    pub level: Option<String>,
    pub five_hour_pct: Option<f64>,
    pub weekly_pct: Option<f64>,
    /// `discovered` = 采用该 key 的实时 `/models`；`fallback` = 探测不可用时用静态配置。
    pub models_source: String,
}

/// 命令回执：**主循环在 tick 之后才发出**，于是「HTTP 200 返回」⇔「状态已落地且已发布」。
pub enum Ack {
    Unit(oneshot::Sender<Result<(), String>>, Result<(), String>),
    Key(
        oneshot::Sender<Result<AddKeyOk, String>>,
        Result<AddKeyOk, String>,
    ),
}

impl Ack {
    /// 命令是否真的改了状态。失败的命令什么也没改 ⇒ 不必再跑一轮 tick
    /// （tick 会去查智谱，不该被无效命令白白触发）。
    pub fn changed(&self) -> bool {
        match self {
            Ack::Unit(_, r) => r.is_ok(),
            Ack::Key(_, r) => r.is_ok(),
        }
    }
    pub fn send(self) {
        match self {
            Ack::Unit(tx, r) => {
                let _ = tx.send(r);
            }
            Ack::Key(tx, r) => {
                let _ = tx.send(r);
            }
        }
    }
}

/// 一轮决策的完整结果。
#[derive(Debug, Clone, PartialEq)]
pub struct Decision {
    /// 本轮活动 key。`None` = 无人合格 ⇒ 调用方**保留原活动**（不清空）
    pub active: Option<i64>,
    /// 合格集：自动逻辑允许把流量放上去的 key
    pub eligible: Vec<i64>,
    /// 临期集：合格集里「周窗口即将重置且还有余量」的 key（本次切换的优先对象）。
    /// 空 = 无人临期（含 lookahead=0 策略关闭）。仅供日志/看板，决策身份仍是 active。
    pub imminent: Vec<i64>,
    pub regime: Regime,
    /// 非空则调用方须把 `pinned` 置 None
    pub pin_release: Option<PinRelease>,
}

/// 算合格集 —— **自动逻辑的地盘，pin 无权染指**。
///
/// - 已知集 = 本轮成功查到 pct 的 key（查询失败的不在其中，状态未知）
/// - 正常集 = 已知集里 pct < throttle 的；非空 ⇒ 正常档，合格集 = 正常集
/// - 否则   ⇒ 降级档，合格集 = 已知集里 pct < exhausted 的（还有余量就能用）
/// - 临期**不改变合格线**，只在当前档位的合格集内改变挑选顺序：正常档仍严格 `< throttle`；
///   只有全员都越过预防线进入降级档后，才允许使用 `< exhausted` 的 95–100% key。
///
/// 全部查询失败（已知集为空）⇒ 返回空集 + 正常档：没有任何证据表明降级了。
fn eligible_set(
    ids: &[i64],
    pct: &HashMap<i64, f64>,
    throttle: f64,
    exhausted: f64,
) -> (Vec<i64>, Regime) {
    let under = |limit: f64| -> Vec<i64> {
        ids.iter()
            .copied()
            .filter(|id| pct.get(id).is_some_and(|p| *p < limit))
            .collect()
    };
    if !ids.iter().any(|id| pct.contains_key(id)) {
        return (Vec::new(), Regime::Normal);
    }
    let normal = under(throttle);
    if !normal.is_empty() {
        return (normal, Regime::Normal);
    }
    (under(exhausted), Regime::Degraded)
}

/// 选活动 key。**纯函数**（分支多、又是安全核心，必须可单测）。
///
/// 三层，从强到弱：
///   1. pin —— 只在合格集内生效。pin 是**优先级**，不是安全豁免：它能覆盖「粘滞 + 挑最低」
///      这个选择偏好，但**不能把自动逻辑判定为不合格的 key 拉上来用**。越线即自动解除。
///   2. 粘滞 —— 现任只要还合格就不换（护 prompt 缓存：能不换就不换）。
///   3. 挑选 —— 只在合格集内挑（keyrot-1：**临期 key 优先**，按周重置时刻升序——先烧快清零的；
///      无临期则回到旧策略：正常档优先要求 < restore，再挑 pct 最低）。
///
/// **抖动保护**：本轮查询失败的 key 不进合格集（不会被主动选中），但若它恰好是 current
/// 或 pinned，则**保持不变** —— 一次瞬时失败不该丢缓存，也不该抖掉用户的 pin。
/// 反过来说，解除 pin 必须基于「查到了 **且** 确实超线」这个正面证据。
///
/// keyrot-1 的 pin 说明：临期只影响合格集内排序，不放宽 pin 门限；正常档仍是 throttle，
/// 降级档仍是 exhausted，与旧版一致。
#[allow(clippy::too_many_arguments)]
fn decide(
    ids: &[i64],
    pct: &HashMap<i64, f64>,
    weekly: &HashMap<i64, WeeklyInfo>,
    now_ms: i64,
    lookahead_ms: i64,
    current: Option<i64>,
    pinned: Option<i64>,
    throttle: f64,
    restore: f64,
    exhausted: f64,
) -> Decision {
    let (eligible, regime) = eligible_set(ids, pct, throttle, exhausted);
    let imminents: Vec<i64> = eligible
        .iter()
        .copied()
        .filter(|id| weekly.get(id).is_some_and(|w| imminent(w, now_ms, lookahead_ms, exhausted)))
        .collect();
    let mut pin_release = None;
    let done = |active, eligible, imminents, pin_release| Decision {
        active,
        eligible,
        imminent: imminents,
        regime,
        pin_release,
    };

    // 1. pin（只在合格集内）
    if let Some(p) = pinned.filter(|p| ids.contains(p)) {
        match pct.get(&p) {
            // 查询失败 → 保持 pin（抖动保护）
            None => return done(Some(p), eligible, imminents, None),
            Some(_) if eligible.contains(&p) => return done(Some(p), eligible, imminents, None),
            // 越线 → 解除 pin，落到自动逻辑。临期不放宽门限，仍按档位取。
            Some(&pp) => {
                let limit = match regime {
                    Regime::Normal => throttle,
                    Regime::Degraded => exhausted,
                };
                pin_release = Some(PinRelease {
                    channel_id: p,
                    pct: pp,
                    limit,
                });
            }
        }
    }

    // 2. 粘滞
    if let Some(c) = current.filter(|c| ids.contains(c)) {
        match pct.get(&c) {
            // 查询失败 → 保持（抖动保护）
            None => return done(Some(c), eligible, imminents, pin_release),
            Some(_) if eligible.contains(&c) => return done(Some(c), eligible, imminents, pin_release),
            Some(_) => {}
        }
    }

    // 3. 在合格集内挑。keyrot-1：临期者优先（EDF——周重置时刻升序，平手取 max_pct 低者，多留安全垫）。
    //    无临期 ⇒ 完全沿用旧策略（正常档 restore 优先，降级档最低）。
    let lowest = |cands: &[i64]| -> Option<i64> {
        cands
            .iter()
            .copied()
            .min_by(|a, b| pct[a].total_cmp(&pct[b]))
    };
    let active = if !imminents.is_empty() {
        imminents
            .iter()
            .copied()
            .min_by(|a, b| {
                let wa = &weekly[a];
                let wb = &weekly[b];
                wa.reset_ms
                    .cmp(&wb.reset_ms)
                    .then_with(|| wa.max_pct.total_cmp(&wb.max_pct))
            })
    } else {
        match regime {
            Regime::Normal => {
                // 优先挑余量更足的（< restore），让新活动 key 撑更久 → 切换更少
                let roomy: Vec<i64> = eligible
                    .iter()
                    .copied()
                    .filter(|id| pct[id] < restore)
                    .collect();
                lowest(&roomy).or_else(|| lowest(&eligible))
            }
            // 降级档全员已 ≥ throttle，restore 在这一档没有意义
            Regime::Degraded => lowest(&eligible),
        }
    };

    // 4. active == None ⇒ 合格集为空（全员 ≥ exhausted 或全查询失败）⇒ 调用方保留原活动
    done(active, eligible, imminents, pin_release)
}

impl Orchestrator {
    pub fn new(
        cfg: Config,
        api: Arc<NewApiClient>,
        keys: Vec<ResolvedKey>,
        snapshot: Shared,
        router: std::sync::Arc<crate::router::RouterState>,
    ) -> Self {
        let probe = QuotaProbe::new(&cfg.zhipu);
        let deprecated = cfg
            .keys
            .iter()
            .filter(|k| k.is_deprecated())
            .map(|k| status::DeprecatedKeyInfo {
                name: k.name.clone(),
                note: k.note.clone(),
            })
            .collect();
        Self {
            cfg,
            probe,
            api,
            keys,
            deprecated,
            active: None,
            pinned: None,
            last_pin_release: None,
            regime: Regime::Normal,
            last_eligible: Vec::new(),
            last_pct: HashMap::new(),
            applied: HashMap::new(),
            snapshot,
            router,
            http: reqwest::Client::new(),
        }
    }

    /// 处理一条看板命令，返回**尚未发出**的回执。
    ///
    /// 调用方须：改了状态 → 先跑一轮 tick（priority 秒级下发）→ 再 `ack.send()`。
    /// 顺序很重要：回执一旦发出，HTTP 就 200 了；若此时 tick 还没跑，前端立刻拉
    /// `/api/status` 会看到**旧快照**，像是按钮没生效。
    pub async fn handle(&mut self, cmd: Command) -> Ack {
        match cmd {
            Command::Pin { channel_id, reply } => Ack::Unit(reply, self.pin(channel_id)),
            Command::Unpin { reply } => {
                if self.pinned.is_some() {
                    info!("已取消 pin，回到自动选择");
                }
                self.pinned = None;
                self.last_pin_release = None;
                Ack::Unit(reply, Ok(()))
            }
            Command::AddKey { spec, reply } => {
                let r = self.add_key(spec).await;
                Ack::Key(reply, r)
            }
            Command::DeprecateKey { channel_id, reply } => {
                let r = self.deprecate_key(channel_id).await;
                Ack::Unit(reply, r)
            }
            Command::RestoreKey { name, reply } => {
                let r = self.restore_key(&name).await;
                Ack::Key(reply, r)
            }
            Command::UpdateKey {
                channel_id,
                patch,
                reply,
            } => {
                let r = self.update_key(channel_id, patch).await;
                Ack::Unit(reply, r)
            }
            Command::ResyncModels { channel_id, reply } => {
                let r = self.resync_models(channel_id).await;
                Ack::Unit(reply, r)
            }
        }
    }

    /// 加一把 key：**探活 → 建渠道 → 写回 config.toml → 热加载**。顺序是有讲究的。
    ///
    /// ① 探活先行，是这个功能最值钱的地方。CLAUDE.md 里那三个坑（url 缺 `?type=2`、缺
    ///    `Bearer`、缺 org/project selector）**都不会报错**——它们表现为「查得通但 limits 为空」，
    ///    而空 limits 会被当成 0% 用量，于是这把 key **永远不会被切走**，直到某天线上撞墙才发现。
    ///    把这个错挡在录入口，比事后 debug 便宜一万倍。探活不过 ⇒ 什么都不改。
    /// ② 探活过了才建渠道、才动 config.toml。
    async fn add_key(&mut self, spec: NewKeySpec) -> Result<AddKeyOk, String> {
        let name = spec.name.trim().to_string();
        if name.is_empty() || spec.api_key.trim().is_empty() {
            return Err("名称和 API key 都不能为空".into());
        }
        // 名字要进面板 HTML/onclick 和渠道名——拒绝引号/尖括号/反斜杠，
        // 从源头杜绝转义遗漏（面板另有 esc() 兜底）。
        if name.chars().any(|c| matches!(c, '"' | '\'' | '<' | '>' | '\\') || c.is_control()) {
            return Err(format!("名字 {name} 含引号/尖括号/反斜杠/控制字符，换一个"));
        }
        if self.keys.iter().any(|k| k.name == name) {
            return Err(format!("已存在同名 key：{name}"));
        }
        // 弃用条目也占名（config 是唯一数据源、append_key 会拒）——提前拦，
        // 否则渠道已建才发现写不进 config，留下无主野生渠道。
        if self.deprecated.iter().any(|k| k.name == name) {
            return Err(format!(
                "与一把**弃用中**的 key 同名：{name}。想复用这把 key 请点「恢复」；想用新凭据请换个名字"
            ));
        }
        let spec = NewKeySpec {
            name: name.clone(),
            api_key: spec.api_key.trim().to_string(),
            ..spec
        };

        // ① 探活
        let headers = spec.headers();
        let status = self
            .probe
            .query(&spec.api_key, &headers)
            .await
            .map_err(|e| format!("探活失败，未做任何改动：{e}"))?;

        // ② 建唯一的上游渠道（新 key 一律以 standby 入场；要不要转正交给下一轮自动决策）。
        //    OpenAI/Claude 是 NewAPI 的两种下游格式，共用该渠道和同一把 NewAPI 访问 key。
        let tpl = self
            .cfg
            .new_api
            .channel_template
            .as_ref()
            .ok_or("配置里没有 [new_api.channel_template]，无法自动建渠道")?;
        let models_source = self
            .api
            .create_channel_resolving_models(
                &name,
                &name,
                &spec.api_key,
                self.cfg.priority_standby,
                &tpl.into(),
            )
            .await
            .map_err(|e| format!("建渠道失败：{e}"))?;
        let channels = self.api.list_channels().await.ok();
        let channel_id = channels
            .as_ref()
            .and_then(|m| m.get(&name).copied())
            .ok_or_else(|| format!("渠道已建好，但在 new-api 里解析不到它的 id：{name}"))?;

        // ③ 写回 config.toml（唯一数据源 ⇒ 重启后仍在）
        if let Err(e) = crate::config::append_key(&self.cfg.source_path, &spec) {
            error!(name = %name, error = %e, "写回 config.toml 失败");
            return Err(format!(
                "渠道已建好（#{channel_id}），但写回 config.toml 失败：{e}。\
                 本次运行内它不会生效，请手动补一条 [[keys]] 后重启"
            ));
        }

        // ④ 热加载：下一轮 tick 就会查它的用量并纳入调度
        self.keys.push(ResolvedKey {
            name: name.clone(),
            zhipu_api_key: spec.api_key.clone(),
            channel_id,
            claude_channel_id: None,
            note: spec.note.trim().to_string(),
            quota_headers: headers,
        });
        info!(
            name = %name,
            channel_id,
            level = ?status.level,
            models_source = models_source.as_str(),
            "已加入新 key（探活通过）"
        );

        Ok(AddKeyOk {
            channel_id,
            level: status.level,
            five_hour_pct: status.five_hour.as_ref().map(|w| w.percentage),
            weekly_pct: status.weekly.as_ref().map(|w| w.percentage),
            models_source: models_source.as_str().to_string(),
        })
    }

    /// 弃用某把 key：**config.toml 打弃用标志（条目保留）+ 删 new-api 渠道**。
    ///
    /// 顺序有讲究（code-review 修正版）：
    /// ① 先把 priority 压到最低档——幂等、非破坏；失败只 warn（渠道若还活着至少不吃流量）。
    /// ② **config 打标志（唯一数据源）先行**——失败则整体中止，此时只发生了一次幂等的
    ///    priority PUT，无撕裂（旧序「先删渠道后写 config」会在 config 写失败时留下
    ///    指向已删渠道的 channel_id，重启后按 id 优先匹配管理一个幽灵渠道）。
    /// ③ 内存摘除（keys/applied/active/pinned/弃用列表）。
    /// ④ 删渠道——失败只 warn：启动对齐会把「名字匹配弃用 key」的渠道列为 Delete 兜底重删。
    ///    dry_run 跳过 ①④（不动 new-api），但要 warn 明示渠道残留需人工/下次非 dry_run 启动清理。
    async fn deprecate_key(&mut self, id: i64) -> Result<(), String> {
        let key = self
            .keys
            .iter()
            .find(|k| k.channel_id == id)
            .cloned()
            .ok_or_else(|| format!("渠道 #{id} 不在管辖的 key 列表里"))?;
        // 守卫按**活跃** key 数算（self.keys 已排除弃用条目）
        if self.keys.len() <= 1 {
            return Err("这是最后一把活跃 key，弃用后就没有可路由的渠道了".into());
        }

        if self.cfg.dry_run {
            warn!(name = %key.name, "dry_run：只写 config 弃用标志，不动 new-api；残留渠道请手动删或用非 dry_run 启动一次由对齐清理");
        } else {
            // 压 priority 失败 = 硬失败（旧 remove_key 的闸门：放行可能留下
            // priority_active 的无监控渠道——双 active 平分流量）。
            // **例外（review #5）**：渠道已被外部删除（UI 手删/并发 sync）时 GET 必失败，
            // 卡死在这里 config 标志永远写不进——查实「渠道确实没了」就放行继续
            // （弃用语义本就要删它，删无可删正是目标状态）。
            if let Err(e) = self
                .api
                .set_channel_priority(id, self.cfg.priority_exhausted)
                .await
            {
                let still_exists = self
                    .api
                    .list_channels()
                    .await
                    .map(|m| m.values().any(|&v| v == id))
                    .unwrap_or(true); // 查不动时保守按「还在」处理（保持硬失败闸门）
                if still_exists {
                    return Err(format!(
                        "把 {} 的 priority 压到最低失败，未做任何改动：{e}",
                        key.name
                    ));
                }
                warn!(name = %key.name, channel_id = id, "渠道已被外部删除，跳过压 priority 继续弃用");
            }
        }
        crate::config::deprecate_key(&self.cfg.source_path, &key.name)
            .map_err(|e| format!("在 config.toml 打弃用标志失败：{e}"))?;

        self.keys.retain(|k| k.channel_id != id);
        self.applied.remove(&id);
        if self.active == Some(id) {
            self.active = None; // 下一轮自动重选
        }
        if self.pinned == Some(id) {
            self.pinned = None;
        }
        // 缓存池也按 channel_id 清（池/负载/冷却/统计都是 id 索引，改名无需处理）
        self.router.retain_channel(|cid| cid != id);
        self.deprecated.push(status::DeprecatedKeyInfo {
            name: key.name.clone(),
            note: key.note.clone(),
        });
        if !self.cfg.dry_run {
            if let Err(e) = self.api.delete_channel(id).await {
                warn!(name = %key.name, error = %e, "删除 new-api 渠道失败（config 已弃用；下次启动对齐会重删，面板上它将显示为野生渠道）");
            }
        }
        info!(name = %key.name, channel_id = id, "已弃用（config 条目保留，渠道已删/待删）");
        Ok(())
    }

    /// 恢复一把弃用 key：凭据从 config.toml（唯一数据源）读回 → 探活 → 重建渠道 →
    /// config 一次性去标志并落新 channel_id → 热加载。
    ///
    /// · 探活先行——selector 若已失效，什么都不改，把智谱原文回显给面板。
    /// · 同名残留渠道（弃用时删除失败留的）**删后重建**而非复用——残留渠道身上可能
    ///   挂着别的凭据（同名 add 被拦后手工建的），复用会让「调度盯的 key」和「渠道烧的
    ///   key」错配；删不掉就中止，让人去 new-api 手动清。
    /// · config 用**单次原子写**同时去标志+落 id——两次写之间崩溃会留下
    ///   「磁盘说活跃、内存说弃用」的半恢复态。
    /// · dry_run 不执行（会真实建渠道/改 new-api，与 dry_run 语义相反）。
    async fn restore_key(&mut self, name: &str) -> Result<AddKeyOk, String> {
        if !self.deprecated.iter().any(|k| k.name == name) {
            return Err(format!("{name} 不在弃用列表里（活跃 key 无需恢复）"));
        }
        if self.cfg.dry_run {
            return Err("dry_run 模式不执行恢复（会真实建渠道并写 config）".into());
        }
        let km = crate::config::load_key(&self.cfg.source_path, name)
            .map_err(|e| format!("读 config.toml 失败：{e}"))?
            .ok_or_else(|| format!("config.toml 里找不到 {name} 的条目"))?;

        // ① 探活（与 add_key 同一防线：防 selector 失效被当成 0% 用量的老坑）
        let status = self
            .probe
            .query(&km.zhipu_api_key, &km.quota_headers)
            .await
            .map_err(|e| format!("探活失败，未做任何改动：{e}"))?;

        // ② 残留同名渠道：删掉再重建。list 拉不到也算失败（把「拉取失败」误判成
        //    「无残留」会造成同名双渠道）。
        let existing = self
            .api
            .list_channels()
            .await
            .map_err(|e| format!("查 new-api 渠道列表失败，未做任何改动：{e}"))?;
        if let Some(id) = existing.get(name) {
            info!(name = name, channel_id = id, "发现弃用时未删净的残留渠道，先删除再重建");
            self.api
                .delete_channel(*id)
                .await
                .map_err(|e| format!("删除残留渠道 #{id} 失败（凭据可能错配，不冒险复用）。请到 new-api 手动删除后重试：{e}"))?;
        }

        // ③ 重建渠道（standby 入场，要不要转正交给下一轮自动决策）
        let tpl = self
            .cfg
            .new_api
            .channel_template
            .as_ref()
            .ok_or("配置里没有 [new_api.channel_template]，无法自动建渠道")?;
        let models_source = self
            .api
            .create_channel_resolving_models(
                name,
                name,
                &km.zhipu_api_key,
                self.cfg.priority_standby,
                &tpl.into(),
            )
            .await
            .map_err(|e| format!("重建渠道失败：{e}"))?;
        let channels = self.api.list_channels().await.ok();
        let channel_id = channels
            .as_ref()
            .and_then(|m| m.get(name).copied())
            .ok_or_else(|| format!("渠道已建好，但在 new-api 里解析不到它的 id：{name}"))?;

        // ④ config：单次原子写——去标志 + 落新 channel_id（活跃 key 持有 id 的统一规则）
        crate::config::restore_key(&self.cfg.source_path, name, channel_id, None)
            .map_err(|e| format!("config.toml 恢复失败（渠道已建好 #{channel_id}）：{e}"))?;

        // ⑤ 热加载
        self.keys.push(ResolvedKey {
            name: name.to_string(),
            zhipu_api_key: km.zhipu_api_key.clone(),
            channel_id,
            claude_channel_id: None,
            note: km.note.clone(),
            quota_headers: km.quota_headers.clone(),
        });
        self.deprecated.retain(|k| k.name != name);
        info!(name = name, channel_id, level = ?status.level, "已恢复弃用 key（探活通过，渠道重建）");

        Ok(AddKeyOk {
            channel_id,
            level: status.level,
            five_hour_pct: status.five_hour.as_ref().map(|w| w.percentage),
            weekly_pct: status.weekly.as_ref().map(|w| w.percentage),
            models_source: models_source.as_str().to_string(),
        })
    }

    /// 编辑活跃 key 元数据。顺序（探活先行、new-api 先于 config、config 单次原子写）：
    /// ① 校验改名（字符集 + 与其它 key/弃用条目/config 不重名）
    /// ② 要改 selector → 先用新 header 探活（失败什么都不改，回显智谱原文）
    /// ③ 要改名 → new-api 渠道先改名（失败中止——否则 config 新名对不上旧渠道名，
    ///    虽有 id 优先匹配兜底，但不留这种脏状态）
    /// ④ config 单次原子写 + 内存联动
    /// dry_run 拒绝执行（会动 new-api 与 config）。
    async fn update_key(&mut self, id: i64, patch: crate::config::KeyPatch) -> Result<(), String> {
        if self.cfg.dry_run {
            return Err("dry_run 模式不执行编辑（会动 new-api 与 config）".into());
        }
        let idx = self
            .keys
            .iter()
            .position(|k| k.channel_id == id)
            .ok_or_else(|| format!("渠道 #{id} 不在管辖的 key 列表里"))?;
        let old = self.keys[idx].clone();

        // ① 改名校验
        let new_name = patch.name.as_deref().map(str::trim).filter(|s| !s.is_empty());
        if let Some(n) = new_name {
            if n != old.name {
                if n.chars().any(|c| matches!(c, '"' | '\'' | '<' | '>' | '\\') || c.is_control()) {
                    return Err(format!("名字 {n} 含引号/尖括号/反斜杠/控制字符，换一个"));
                }
                if self.keys.iter().enumerate().any(|(i, k)| i != idx && k.name == n)
                    || self.deprecated.iter().any(|k| k.name == n)
                {
                    return Err(format!("已存在同名 key：{n}"));
                }
                // config 里可能还有本进程不知道的条目（如手工加的）——以唯一数据源为准拦一道
                match crate::config::load_key(&self.cfg.source_path, n) {
                    Ok(Some(_)) => return Err(format!("config.toml 里已存在同名 key：{n}")),
                    Ok(None) => {}
                    Err(e) => return Err(format!("读 config.toml 校验重名失败：{e}")),
                }
            }
        }

        // ② selector 改动 → 先探活
        let new_headers = if patch.org.is_some() || patch.project.is_some() {
            let hs = crate::config::selector_headers(patch.org.as_deref(), patch.project.as_deref());
            if hs != old.quota_headers {
                self.probe
                    .query(&old.zhipu_api_key, &hs)
                    .await
                    .map_err(|e| format!("新 selector 探活失败，未做任何改动：{e}"))?;
                Some(hs)
            } else {
                None
            }
        } else {
            None
        };

        // ③ 渠道改名先行
        if let Some(n) = new_name {
            if n != old.name {
                self.api
                    .rename_channel(id, n)
                    .await
                    .map_err(|e| format!("new-api 渠道改名失败，未做任何改动：{e}"))?;
            }
        }

        // ④ config 单次原子写 + 内存联动
        crate::config::update_key_meta(
            &self.cfg.source_path,
            &old.name,
            new_name.filter(|n| *n != old.name),
            patch.note.as_deref(),
            new_headers.as_deref(),
        )
        .map_err(|e| format!("config.toml 更新失败（渠道侧已改好）：{e}"))?;

        let entry = &mut self.keys[idx];
        if let Some(n) = new_name {
            entry.name = n.to_string();
        }
        if let Some(n) = patch.note.as_deref() {
            entry.note = n.trim().to_string();
        }
        let selector_changed = new_headers.is_some();
        if let Some(hs) = new_headers {
            entry.quota_headers = hs;
        }
        info!(key = %old.name, new_name = ?new_name, note_changed = patch.note.is_some(), selector_changed, "已编辑 key 元数据");
        Ok(())
    }

    /// 手动触发模型重对账（上游 /models → 渠道 models；探测失败报错，不用 fallback 覆盖）。
    async fn resync_models(&mut self, id: i64) -> Result<(), String> {
        let key = self
            .keys
            .iter()
            .find(|k| k.channel_id == id)
            .cloned()
            .ok_or_else(|| format!("渠道 #{id} 不在管辖的 key 列表里"))?;
        if self.cfg.dry_run {
            return Err("dry_run 模式不执行模型重对账（会动 new-api）".into());
        }
        let tpl = self
            .cfg
            .new_api
            .channel_template
            .as_ref()
            .ok_or("配置里没有 [new_api.channel_template]，无从对账")?;
        let updated = self
            .api
            .reconcile_channel_models(id, &key.name, &key.zhipu_api_key, &tpl.into())
            .await
            .map_err(|e| format!("模型重对账失败：{e}"))?;
        info!(key = %key.name, updated, "手动模型重对账完成");
        Ok(())
    }

    /// 钉住某把 key。**只能钉合格集内的** —— pin 是优先级，不是安全豁免：
    /// 它能覆盖「粘滞 + 挑最低」的选择偏好，但不能把自动逻辑判定为不合格的 key 拉上来用。
    fn pin(&mut self, id: i64) -> Result<(), String> {
        let name = self
            .keys
            .iter()
            .find(|k| k.channel_id == id)
            .map(|k| k.name.clone())
            .ok_or_else(|| format!("渠道 #{id} 不在管辖的 key 列表里"))?;

        // 判据用**上一轮**的合格集：命令是异步来的，此刻没有更新的证据
        if !self.last_eligible.contains(&id) {
            let limit = match self.regime {
                Regime::Normal => self.cfg.throttle_threshold,
                Regime::Degraded => self.cfg.exhausted_threshold,
            };
            return Err(match self.last_pct.get(&id) {
                Some(p) => format!(
                    "{name} 用量 {p}% 已越过合格线 {limit}%，自动逻辑不允许钉住（pin 是优先级，不是安全豁免）"
                ),
                None => format!("{name} 本轮用量查询失败，状态未知，暂不能钉住"),
            });
        }

        self.pinned = Some(id);
        self.last_pin_release = None; // 手动重新 pin ⇒ 清掉上一次的「自动解除」提示
        info!(name = %name, channel_id = id, "已手动钉住 key");
        Ok(())
    }

    /// new-api 健康探测（只入快照，不影响本轮决策）
    async fn newapi_healthy(&self) -> bool {
        // F4：探内部 upstream——代理挂 ≠ new-api 挂，打错对象会误导排障
        let url = format!("{}/api/status", self.cfg.upstream_base());
        matches!(
            self.http.get(&url).timeout(Duration::from_secs(3)).send().await,
            Ok(r) if r.status().is_success()
        )
    }

    pub async fn tick(&mut self) {
        let keys = self.keys.clone();

        // 1. 采集每把 key 的最大窗口使用率；查询失败的不进 map（不参与本轮决策）。
        //    windows / errors 仅供状态看板，不参与决策。
        let mut pct: HashMap<i64, f64> = HashMap::new();
        let mut windows: HashMap<i64, QuotaStatus> = HashMap::new();
        let mut errors: HashMap<i64, String> = HashMap::new();
        // keyrot-1：周窗口快照（决策层用）。查询失败或周窗口缺失 ⇒ 无条目 ⇒ 非临期。
        let mut weekly: HashMap<i64, WeeklyInfo> = HashMap::new();
        for k in &keys {
            match self.probe.query(&k.zhipu_api_key, &k.quota_headers).await {
                Ok(status) => {
                    let p = max_watch_pct(&self.cfg, &status);
                    info!(name = %k.name, channel_id = k.channel_id, pct = p, "用量");
                    pct.insert(k.channel_id, p);
                    if let Some(w) = &status.weekly {
                        weekly.insert(
                            k.channel_id,
                            WeeklyInfo {
                                reset_ms: w.next_reset_time,
                                pct: w.percentage,
                                max_pct: p,
                            },
                        );
                    }
                    windows.insert(k.channel_id, status);
                }
                Err(e) => {
                    warn!(key = %k.name, error = %e, "查询智谱用量失败，本轮不参与决策");
                    errors.insert(k.channel_id, e.to_string());
                }
            }
        }

        let throttle = self.cfg.throttle_threshold;
        let restore = self.cfg.restore_threshold;
        let exhausted = self.cfg.exhausted_threshold;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let lookahead_ms = self.cfg.weekly_reset_lookahead_hours as i64 * 3_600_000;

        // 2. 选出活动 key（合格集 + pin + 粘滞 + 临期优先，见 decide 的文档）
        let ids: Vec<i64> = keys.iter().map(|k| k.channel_id).collect();
        let name_of = |id: i64| -> &str {
            keys.iter()
                .find(|k| k.channel_id == id)
                .map_or("?", |k| k.name.as_str())
        };
        let d = decide(
            &ids,
            &pct,
            &weekly,
            now_ms,
            lookahead_ms,
            self.active,
            self.pinned,
            throttle,
            restore,
            exhausted,
        );

        // pin 越线 → 自动解除（pin 不得覆盖自动逻辑）
        if let Some(rel) = d.pin_release {
            warn!(
                name = name_of(rel.channel_id),
                channel_id = rel.channel_id,
                pct = rel.pct,
                limit = rel.limit,
                "pin 的 key 已越出合格线，自动解除 pin，回到自动选择"
            );
            self.pinned = None;
            self.last_pin_release = Some(rel);
        }

        // 记下本轮判据，供 pin 命令**当场**判合法性（命令是异步来的，不能等下一轮）
        self.last_eligible = d.eligible.clone();
        self.last_pct = pct.clone();

        if d.regime != self.regime {
            match d.regime {
                Regime::Degraded => warn!(
                    throttle,
                    exhausted, "进入降级档：全部 key 均已越过预防线，将榨干活动 key 至用尽再流转"
                ),
                Regime::Normal => info!("回到正常档：已有 key 低于预防线"),
            }
            self.regime = d.regime;
        }

        if d.active != self.active {
            match (self.active, d.active) {
                (_, Some(id)) => {
                    // 切换原因：临期优先（EDF）还是旧策略（低 pct）——日志里说清楚
                    let switched_for_imminent = d.imminent.contains(&id);
                    info!(
                        name = name_of(id),
                        channel_id = id,
                        imminent = switched_for_imminent,
                        "切换活动 key"
                    );
                }
                (Some(prev), None) => {
                    warn!(prev, "所有 key 都无额度，保留原活动并交给 new-api 429 兜底")
                }
                (None, None) => warn!("暂无任何可用 key"),
            }
            // 全无额度时不要把 active 清空（清空会误把原活动降到 exhausted），保留原值。
            if d.active.is_some() {
                self.active = d.active;
            }
        }
        let active = self.active;

        // 3. 计算每把 key 的目标 priority。OpenAI/Claude 两种下游格式共用同一个渠道，
        //    因此这里只需要维护一次 priority。
        //    单一分层规则（正常档下与旧的三分支逐字节等价；降级档下自动变准——**还有余量**的
        //    key 拿 standby 而非 exhausted，于是万一活动 key 仍撞 429，new-api 的 priority
        //    阶梯会优先跌到还有余量的那把，而不是随机跌到一把已经死透的）。
        for k in &keys {
            let id = k.channel_id;
            let target = if Some(id) == active {
                self.cfg.priority_active
            } else if d.eligible.contains(&id) {
                self.cfg.priority_standby
            } else if pct.contains_key(&id) {
                self.cfg.priority_exhausted
            } else {
                // 本轮查询失败：状态未知，整把 key（两个渠道）都不动
                continue;
            };

            if self.applied.get(&id) == Some(&target) {
                continue;
            }
            if self.cfg.dry_run {
                info!(name = %k.name, channel_id = id, priority = target, "dry_run: 将设 priority");
                self.applied.insert(id, target);
            } else {
                match self.api.set_channel_priority(id, target).await {
                    Ok(_) => {
                        self.applied.insert(id, target);
                        info!(name = %k.name, channel_id = id, priority = target, "已设 priority");
                    }
                    Err(e) => error!(name = %k.name, channel_id = id, error = %e, "设 priority 失败"),
                }
            }
        }

        // 4. 发布**决策部分**快照（在决策与下发之后生成 ⇒ 与 new-api 实际状态一致；
        //    下发失败的 key 其 applied 未更新，快照如实显示旧 priority，不撒谎）。
        //    面板字段（channels/live/hourly/model_usage/内部余额）由 Panel 循环独占写，此处不碰。
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let healthy = self.newapi_healthy().await;
        let client_endpoint = format!("{}/v1", self.cfg.new_api.base_url.trim_end_matches('/'));
        // Claude Code 使用同一 NewAPI key；客户端会在这个地址后拼 /v1/messages。
        let claude_endpoint = self.cfg.new_api.base_url.trim_end_matches('/').to_string();

        // 高峰时段（**纯显示**，不参与任何调度决策——它影响的是「同一请求烧掉几倍额度」，
        // 而额度消耗本来就已经如实反映在智谱返回的 pct 里了，不需要我们再折算一遍）
        let peak = self.cfg.peak.as_ref().map_or_else(status::PeakInfo::default, |p| {
            let (is_peak, next) =
                peak_state(now_ms / 1000, p.start_hour, p.end_hour, p.tz_offset_hours);
            status::PeakInfo {
                enabled: true,
                is_peak,
                window: format!(
                    "{:02}:00–{:02}:00 (UTC{:+})",
                    p.start_hour, p.end_hour, p.tz_offset_hours
                ),
                next_change_at: next * 1000,
                models: p
                    .coefficients
                    .iter()
                    .map(|c| status::PeakModel {
                        model: c.model.clone(),
                        current: if is_peak { c.peak } else { c.off_peak },
                        peak: c.peak,
                        off_peak: c.off_peak,
                    })
                    .collect(),
                note: p.note.clone(),
            }
        });

        let key_statuses = keys
            .iter()
            .map(|k| {
                let id = k.channel_id;
                let w = windows.get(&id);
                let max = pct.get(&id).copied();
                // 档位与 priority 下发规则同源（合格集），两者永不打架
                let tier = if Some(id) == active {
                    "active"
                } else if d.eligible.contains(&id) {
                    "standby"
                } else if max.is_some() {
                    "exhausted"
                } else {
                    "unknown"
                };
                KeyStatus {
                    name: k.name.clone(),
                    note: k.note.clone(),
                    channel_id: id,
                    five_hour_pct: w.and_then(|q| q.five_hour.as_ref().map(|x| x.percentage)),
                    weekly_pct: w.and_then(|q| q.weekly.as_ref().map(|x| x.percentage)),
                    five_hour_reset: w.and_then(|q| q.five_hour.as_ref().map(|x| x.next_reset_time)),
                    weekly_reset: w.and_then(|q| q.weekly.as_ref().map(|x| x.next_reset_time)),
                    max_pct: max,
                    // keyrot-1：临期状态与决策同源（在合格集里且满足 imminent 四条件）
                    imminent: d.imminent.contains(&id),
                    tier: tier.to_string(),
                    priority: self.applied.get(&id).copied(),
                    error: errors.get(&id).cloned(),
                    // selector 反解（编辑表单预填用）
                    org: k
                        .quota_headers
                        .iter()
                        .find(|h| h.key == "Bigmodel-Organization")
                        .map(|h| h.value.clone())
                        .unwrap_or_default(),
                    project: k
                        .quota_headers
                        .iter()
                        .find(|h| h.key == "Bigmodel-Project")
                        .map(|h| h.value.clone())
                        .unwrap_or_default(),
                }
            })
            .collect();

        // 只写决策字段（面板字段不动）
        status::update(&self.snapshot, |s| {
            s.updated_at = now_ms;
            s.dry_run = self.cfg.dry_run;
            s.throttle_threshold = throttle;
            s.restore_threshold = restore;
            s.exhausted_threshold = exhausted;
            s.weekly_reset_lookahead_hours = self.cfg.weekly_reset_lookahead_hours;
            s.new_api_base = self.cfg.new_api.base_url.clone();
            s.new_api_healthy = healthy;
            s.active_channel_id = active;
            s.pinned_channel_id = self.pinned;
            s.regime = d.regime.as_str().to_string();
            s.eligible = d.eligible.clone();
            s.last_pin_release = self.last_pin_release.map(|r| status::PinReleaseInfo {
                channel_id: r.channel_id,
                pct: r.pct,
                limit: r.limit,
            });
            s.peak = peak;
            s.keys = key_statuses;
            s.deprecated_keys = self.deprecated.clone();
            s.client_endpoint = client_endpoint;
            s.claude_endpoint = claude_endpoint;
        });
    }
}

/// 面板循环：只刷**看板数据**，与切换循环完全隔离。
///
/// 为什么独立：智谱是**外部 API**（该低频，60s），new-api 是**本地**服务（毫秒级纯读，可 5s 高频）。
/// 绑在一起会让看板 60 秒才动一次，看不到实时流量。
///
/// **并发安全**：本循环只写面板字段（channels / live / hourly / model_usage / newapi_user_quota），
/// 决策循环只写决策字段，两者严格不相交 + 写锁互斥 ⇒ 不会互相覆盖。
/// 本循环整个挂掉也不影响切换。
pub struct Panel {
    pub api: Arc<NewApiClient>,
    pub snapshot: Shared,
    /// 评分展示用（负载计数/冷却），与代理共用同一 RouterState——
    /// 面板上看到的分就是选路用的分（同一公式、同一份本机负载数据）
    pub router: std::sync::Arc<crate::router::RouterState>,
    pub ratio: f64,
}

impl Panel {
    pub async fn run(self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            self.refresh().await;
        }
    }

    async fn refresh(&self) {
        // 渠道实况（含「被 new-api 自动禁用」这个盲区）
        let channels = self.api.list_channel_states().await.unwrap_or_else(|e| {
            debug!(error = %e, "面板：拉取渠道状态失败");
            Vec::new()
        });

        // new-api 内部虚拟余额（见底会直接挡住转发）
        let newapi_user_quota = self.api.user_quota().await.unwrap_or(-1);

        // 每渠道实时指标：**全部从 recent_logs 一次请求推导**（rpm/tpm = 最近 60 秒内
        // 的条数与 token 和；最后请求 = 该渠道最新一条）。
        // ⚠️ 不再逐渠道调 /api/log/stat：new-api 对 /api 有全局限流（默认 360 次/180s
        // ≈ 2 次/秒），逐渠道轮询在渠道数多时（N 把 key）**单面板就可能超预算**，
        // 会把控制循环的 priority 写入饿出 429、渠道 priority 卡在旧值（2026-08-24 实测踩坑）。
        // 渠道列表**从快照读**（决策循环发布的）——加/删 key 后无需重启，指标就能跟上。
        let tracked = status::tracked_channels(&self.snapshot);
        let live = match self.api.recent_logs(100).await {
            Ok(logs) => live_metrics_from_logs(&tracked, &logs),
            Err(e) => {
                debug!(error = %e, "面板：拉取请求日志失败");
                tracked
                    .into_iter()
                    .map(|channel_id| LiveMetric {
                        channel_id,
                        rpm: 0,
                        tpm: 0,
                        last_request_at: None,
                        last_request_model: None,
                    })
                    .collect()
            }
        };

        // 用量统计（new-api 自己按小时聚合的 quota_data）：近 24h 时序 + 按模型汇总
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let (mut hourly, mut model_usage) = (Vec::new(), Vec::new());
        if let Ok(rows) = self.api.usage_data(now - 24 * 3600, now + 3600).await {
            // 与 /api/usage 共用聚合函数 ⇒ 近 24h 图和历史图口径必然一致
            (hourly, model_usage) = status::aggregate_usage(rows);
            model_usage.sort_by(|a, b| b.tokens.cmp(&a.tokens)); // 用量降序
        }

        // 评分分解：读锁内取最小集，锁外算（与代理选路同一公式/同一 RouterState）
        let scores = {
            let g = self.snapshot.read().unwrap_or_else(|e| e.into_inner());
            let view = crate::router::RouteView::from_snap(&g);
            drop(g);
            if view.has_data {
                crate::router::display_scores(&view, &self.router, now * 1000, self.ratio)
            } else {
                Vec::new()
            }
        };

        // 只写面板字段
        status::update(&self.snapshot, |s| {
            s.channels = channels;
            s.newapi_user_quota = newapi_user_quota;
            s.live = live;
            s.hourly = hourly;
            s.model_usage = model_usage;
            s.scores = scores;
        });
    }
}

/// 从请求日志推导每渠道实时指标。纯函数（可单测）。
///
/// · `logs` 按时间**倒序**（`recent_logs` 已保证），每渠道第一条即最后请求；
/// · rpm/tpm 只统计最近 **60 秒**内的条目（与原 /api/log/stat 的固定窗口语义一致；
///   受拉取条数上界约束，高流量渠道可能略有低估——显示用途，可接受）。
fn live_metrics_from_logs(tracked: &[i64], logs: &[RequestLog]) -> Vec<LiveMetric> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    tracked
        .iter()
        .map(|&cid| {
            let mut m = LiveMetric {
                channel_id: cid,
                ..Default::default()
            };
            for l in logs.iter().filter(|l| l.channel == cid) {
                if m.last_request_at.is_none() {
                    m.last_request_at = Some(l.created_at);
                    m.last_request_model = Some(l.model_name.clone());
                }
                if now - l.created_at <= 60 {
                    m.rpm += 1;
                    m.tpm += l.prompt_tokens + l.completion_tokens;
                }
            }
            m
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const THROTTLE: f64 = 95.0;
    const RESTORE: f64 = 90.0;
    const EXHAUSTED: f64 = 100.0;
    /// 测试用固定「现在」（epoch ms）：只用于 reset_ms - now_ms 的差值，绝对值无意义
    const NOW_MS: i64 = 1_800_000_000_000;
    const HOUR_MS: i64 = 3_600_000;
    const LOOKAHEAD_H: i64 = 24;

    /// pct 列表 → (ids, map)。`None` 表示该 key 本轮**查询失败**（状态未知）。
    fn setup(pcts: &[(i64, Option<f64>)]) -> (Vec<i64>, HashMap<i64, f64>) {
        let ids = pcts.iter().map(|(id, _)| *id).collect();
        let map = pcts
            .iter()
            .filter_map(|(id, p)| p.map(|p| (*id, p)))
            .collect();
        (ids, map)
    }

    /// 每把 key 的周窗口：reset 距现在的小时数（负数 = 已重置）+ 周 pct + max_pct。
    fn weekly_of(pairs: &[(i64, i64, f64, f64)]) -> HashMap<i64, WeeklyInfo> {
        pairs
            .iter()
            .map(|(id, in_hours, pct, max_pct)| {
                (
                    *id,
                    WeeklyInfo {
                        reset_ms: NOW_MS + in_hours * HOUR_MS,
                        pct: *pct,
                        max_pct: *max_pct,
                    },
                )
            })
            .collect()
    }

    /// 旧接口语义：不传周窗口 ⇒ 无临期 ⇒ 行为与 keyrot-1 之前逐字节等价。
    fn decide_with(
        pcts: &[(i64, Option<f64>)],
        current: Option<i64>,
        pinned: Option<i64>,
    ) -> Decision {
        let (ids, pct) = setup(pcts);
        decide(
            &ids,
            &pct,
            &HashMap::new(),
            NOW_MS,
            LOOKAHEAD_H * HOUR_MS,
            current,
            pinned,
            THROTTLE,
            RESTORE,
            EXHAUSTED,
        )
    }

    /// 带周窗口数据的 decide。
    fn decide_weekly(
        pcts: &[(i64, Option<f64>)],
        weekly: &[(i64, i64, f64, f64)], // (id, reset_in_hours, week_pct, max_pct)
        current: Option<i64>,
        pinned: Option<i64>,
    ) -> Decision {
        let (ids, pct) = setup(pcts);
        decide(
            &ids,
            &pct,
            &weekly_of(weekly),
            NOW_MS,
            LOOKAHEAD_H * HOUR_MS,
            current,
            pinned,
            THROTTLE,
            RESTORE,
            EXHAUSTED,
        )
    }

    /// lookahead 可变的 decide（测 0 = 关闭策略的等价性）。
    fn decide_weekly_lookahead(
        pcts: &[(i64, Option<f64>)],
        weekly: &[(i64, i64, f64, f64)],
        lookahead_hours: i64,
    ) -> Decision {
        let (ids, pct) = setup(pcts);
        decide(
            &ids,
            &pct,
            &weekly_of(weekly),
            NOW_MS,
            lookahead_hours * HOUR_MS,
            None,
            None,
            THROTTLE,
            RESTORE,
            EXHAUSTED,
        )
    }

    // ——— 正常档：与改造前的行为必须逐条等价（防回归）———

    #[test]
    fn 正常档_粘滞_现任未越预防线就不换() {
        // 现任 94%（快到线了但没过），另一把才 10% —— 依然不换，护 prompt 缓存
        let d = decide_with(&[(1, Some(94.0)), (2, Some(10.0))], Some(1), None);
        assert_eq!(d.active, Some(1));
        assert_eq!(d.regime, Regime::Normal);
    }

    #[test]
    fn 正常档_现任越线_换到余量最足的() {
        let d = decide_with(
            &[(1, Some(95.0)), (2, Some(60.0)), (3, Some(30.0))],
            Some(1),
            None,
        );
        assert_eq!(d.active, Some(3), "该挑 pct 最低的");
        assert!(!d.eligible.contains(&1), "越线的不该进合格集");
    }

    #[test]
    fn 正常档_没有低于restore的_退而取低于throttle里最低的() {
        // 都在 90~95 之间：没有「宽裕」的候选，但它们仍然合格
        let d = decide_with(&[(1, Some(96.0)), (2, Some(93.0)), (3, Some(91.0))], Some(1), None);
        assert_eq!(d.active, Some(3));
        assert_eq!(d.regime, Regime::Normal, "只要有人 < throttle 就还是正常档");
    }

    #[test]
    fn 正常档_首次选择_无现任() {
        let d = decide_with(&[(1, Some(60.0)), (2, Some(30.0))], None, None);
        assert_eq!(d.active, Some(2));
    }

    // ——— 降级档：本次要修的漏洞 ———

    #[test]
    fn 降级档_全员越预防线但现任还有余量_继续榨干不换() {
        // 用户提的场景的前半段：全员 >95%，现任 97% 还有余量 → 不换（护缓存）
        let d = decide_with(&[(1, Some(97.0)), (2, Some(96.0))], Some(1), None);
        assert_eq!(d.regime, Regime::Degraded);
        assert_eq!(d.active, Some(1), "降级档同样粘滞：能不换就不换");
        assert_eq!(d.eligible, vec![1, 2], "有余量的都合格");
    }

    #[test]
    fn 降级档_现任真用尽_流转到还有余量的() {
        // 用户提的场景：全员 >95%，现任到了 100% → 必须流转到 96% 那把，而不是继续钉着死 key
        let d = decide_with(&[(1, Some(100.0)), (2, Some(96.0)), (3, Some(98.0))], Some(1), None);
        assert_eq!(d.regime, Regime::Degraded);
        assert_eq!(d.active, Some(2), "流转到还有余量里 pct 最低的");
        assert!(!d.eligible.contains(&1), "100% 的不合格");
    }

    #[test]
    fn 全员真用尽_保留原活动交给429兜底() {
        let d = decide_with(&[(1, Some(100.0)), (2, Some(100.0))], Some(1), None);
        assert_eq!(d.active, None, "None ⇒ 调用方保留原活动，不清空");
        assert!(d.eligible.is_empty());
    }

    #[test]
    fn 有key低于预防线就该回到正常档() {
        let d = decide_with(&[(1, Some(99.0)), (2, Some(50.0))], Some(1), None);
        assert_eq!(d.regime, Regime::Normal);
        assert_eq!(d.active, Some(2), "现任 99% 已越预防线 → 换到 50% 那把");
    }

    // ——— 抖动保护 ———

    #[test]
    fn 现任查询失败_保持不变() {
        let d = decide_with(&[(1, None), (2, Some(10.0))], Some(1), None);
        assert_eq!(d.active, Some(1), "一次瞬时失败不该丢缓存");
        assert!(!d.eligible.contains(&1), "但它不进合格集（状态未知）");
    }

    #[test]
    fn 全部查询失败_不算降级档() {
        let d = decide_with(&[(1, None), (2, None)], Some(1), None);
        assert_eq!(d.regime, Regime::Normal, "没有任何证据表明降级了");
        assert_eq!(d.active, Some(1));
    }

    // ——— pin：合格集内的偏好，不能覆盖自动逻辑 ———

    #[test]
    fn pin_覆盖挑最低的偏好() {
        // 自动逻辑本会选 3（最低），但用户 pin 了 2 → 听用户的
        let d = decide_with(&[(1, Some(96.0)), (2, Some(60.0)), (3, Some(30.0))], Some(1), Some(2));
        assert_eq!(d.active, Some(2));
        assert_eq!(d.pin_release, None);
    }

    #[test]
    fn pin_覆盖粘滞() {
        // 现任 1 还合格（粘滞本会留它），但用户 pin 了 2
        let d = decide_with(&[(1, Some(50.0)), (2, Some(60.0))], Some(1), Some(2));
        assert_eq!(d.active, Some(2));
    }

    #[test]
    fn pin_不能把不合格的key拉上来用() {
        // 正常档下 pin 一把 96% 的 → 不合格 → 自动解除，回到自动选择
        let d = decide_with(&[(1, Some(50.0)), (2, Some(96.0))], Some(1), Some(2));
        assert_eq!(
            d.pin_release,
            Some(PinRelease { channel_id: 2, pct: 96.0, limit: THROTTLE })
        );
        assert_eq!(d.active, Some(1), "回到自动逻辑（粘滞留现任）");
    }

    #[test]
    fn pin_在降级档下的合格线是exhausted而非throttle() {
        // 全员 >95%：96% 的 key 在降级档里是**合格**的 → pin 得以保持
        let d = decide_with(&[(1, Some(97.0)), (2, Some(96.0))], Some(1), Some(2));
        assert_eq!(d.regime, Regime::Degraded);
        assert_eq!(d.active, Some(2), "降级档下 96% 合格，pin 生效");
        assert_eq!(d.pin_release, None);
    }

    #[test]
    fn pin_的key真用尽_自动解除() {
        let d = decide_with(&[(1, Some(96.0)), (2, Some(100.0))], Some(1), Some(2));
        assert_eq!(
            d.pin_release,
            Some(PinRelease { channel_id: 2, pct: 100.0, limit: EXHAUSTED })
        );
        assert_eq!(d.active, Some(1));
    }

    #[test]
    fn pin_的key查询失败_保持pin不解除() {
        // 解除 pin 必须基于「查到了且确实超线」的正面证据，不能被一次抖动抖掉
        let d = decide_with(&[(1, Some(50.0)), (2, None)], Some(1), Some(2));
        assert_eq!(d.active, Some(2));
        assert_eq!(d.pin_release, None);
    }

    #[test]
    fn pin_指向已被删除的key_忽略() {
        let d = decide_with(&[(1, Some(50.0))], Some(1), Some(99));
        assert_eq!(d.active, Some(1));
        assert_eq!(d.pin_release, None, "不该为不存在的 key 报解除事件");
    }

    // ——— keyrot-1：周窗口临期优先（EDF）———

    #[test]
    fn 正常档_临期98不能越过预防线() {
        // 仍有 30% 的正常 key 时，98% 即使临期也不能绕过 95% 预防线。
        let d = decide_weekly(
            &[(1, Some(98.0)), (2, Some(30.0))],
            &[(1, 2, 98.0, 98.0)],
            None,
            None,
        );
        assert_eq!(d.active, Some(2));
        assert_eq!(d.regime, Regime::Normal, "key2 在 throttle 下 ⇒ 仍是正常档");
        assert!(!d.eligible.contains(&1), "正常档不能把 98% 临期 key 拉回合格集");
        assert_eq!(d.imminent, Vec::<i64>::new());
    }

    #[test]
    fn 临期内部_先烧最先重置的() {
        // EDF：20h 后重置的 85%（临期）先于 23h 后重置的 80%（临期）——
        // 即使后者 pct 更低（旧策略会选它）。
        let d = decide_weekly(
            &[(1, Some(85.0)), (2, Some(80.0)), (3, Some(20.0))],
            &[(1, 20, 85.0, 85.0), (2, 23, 80.0, 80.0)],
            None,
            None,
        );
        assert_eq!(d.active, Some(1));
        assert_eq!(d.imminent, vec![1, 2]);
    }

    #[test]
    fn 临期平手_取max_pct低者() {
        // 重置时刻相同 → 余量多的（max_pct 低）先上，多留安全垫
        let d = decide_weekly(
            &[(1, Some(80.0)), (2, Some(60.0))],
            &[(1, 10, 80.0, 80.0), (2, 10, 60.0, 60.0)],
            None,
            None,
        );
        assert_eq!(d.active, Some(2));
    }

    #[test]
    fn 临期边界_左开右闭() {
        // 左开：恰好 now 重置 = 窗口已走完新周期，不算临期
        let w = WeeklyInfo { reset_ms: NOW_MS, pct: 50.0, max_pct: 50.0 };
        assert!(!imminent(&w, NOW_MS, LOOKAHEAD_H * HOUR_MS, EXHAUSTED));
        // 右闭：恰好 lookahead 整点 = 还剩 0 秒，算（避免边界抖动）
        let w = WeeklyInfo { reset_ms: NOW_MS + LOOKAHEAD_H * HOUR_MS, pct: 50.0, max_pct: 50.0 };
        assert!(imminent(&w, NOW_MS, LOOKAHEAD_H * HOUR_MS, EXHAUSTED));
        // 超窗 1ms：不算
        let w = WeeklyInfo { reset_ms: NOW_MS + LOOKAHEAD_H * HOUR_MS + 1, pct: 50.0, max_pct: 50.0 };
        assert!(!imminent(&w, NOW_MS, LOOKAHEAD_H * HOUR_MS, EXHAUSTED));
    }

    #[test]
    fn 临期三线_各拒一种() {
        // 周 pct 已 100%：没有「没用完」可言
        let w = WeeklyInfo { reset_ms: NOW_MS + HOUR_MS, pct: 100.0, max_pct: 100.0 };
        assert!(!imminent(&w, NOW_MS, LOOKAHEAD_H * HOUR_MS, EXHAUSTED));
        // max_pct 100（5h 窗口死了）：现在接不了流量，不算可用
        let w = WeeklyInfo { reset_ms: NOW_MS + HOUR_MS, pct: 98.0, max_pct: 100.0 };
        assert!(!imminent(&w, NOW_MS, LOOKAHEAD_H * HOUR_MS, EXHAUSTED));
        // 满一周才重置：不在 lookahead 内
        let w = WeeklyInfo { reset_ms: NOW_MS + 168 * HOUR_MS, pct: 50.0, max_pct: 50.0 };
        assert!(!imminent(&w, NOW_MS, LOOKAHEAD_H * HOUR_MS, EXHAUSTED));
    }

    #[test]
    fn 无周窗口_非临期_行为同旧版() {
        // key1 96%（无周窗口数据，可能是只有 5h 窗口的套餐）；key2 94%。
        // 旧策略：2 在 throttle 下 ⇒ 正常档挑 2；1 不进合格集（96 ≥ 95）。
        let d = decide_weekly(&[(1, Some(96.0)), (2, Some(94.0))], &[], None, None);
        assert_eq!(d.active, Some(2));
        assert_eq!(d.imminent, Vec::<i64>::new());
        assert!(!d.eligible.contains(&1));
    }

    #[test]
    fn 周98但5h已100_不可服务_不纳入() {
        // key1：周窗口 98%、2h 后重置，但 max_pct=100（5h 死了）→ 不算临期、不进合格集
        let d = decide_weekly(
            &[(1, Some(100.0)), (2, Some(60.0))],
            &[(1, 2, 98.0, 100.0)],
            None,
            None,
        );
        assert_eq!(d.active, Some(2));
        assert_eq!(d.imminent, Vec::<i64>::new());
        assert!(!d.eligible.contains(&1));
    }

    #[test]
    fn 粘滞_现任临期_不换() {
        // 现任 1 是临期且合格 → 粘滞优先，不切（哪怕 2 更宽裕）
        let d = decide_weekly(
            &[(1, Some(94.0)), (2, Some(30.0))],
            &[(1, 2, 94.0, 94.0)],
            Some(1),
            None,
        );
        assert_eq!(d.active, Some(1));
    }

    #[test]
    fn 粘滞_现任98临期_不换() {
        // 现任 98% 但临期（在合格集）→ 粘滞继续用（降级档语义：有余量就继续榨）
        let d = decide_weekly(
            &[(1, Some(98.0)), (2, Some(96.0))],
            &[(1, 2, 98.0, 98.0), (2, 100, 96.0, 96.0)],
            Some(1),
            None,
        );
        assert_eq!(d.regime, Regime::Degraded, "全员 ≥95 ⇒ 降级档（98 临期 + 96 常规）");
        assert_eq!(d.active, Some(1), "粘滞留临期现任");
    }

    #[test]
    fn 降级档_临期优先于低pct() {
        // 全员 ≥95：旧策略挑 96%（最低）；EDF 挑 2h 后重置的 97%
        let d = decide_weekly(
            &[(1, Some(97.0)), (2, Some(96.0)), (3, Some(98.0))],
            &[(1, 2, 97.0, 97.0), (2, 30, 96.0, 96.0), (3, 100, 98.0, 98.0)],
            None,
            None,
        );
        assert_eq!(d.regime, Regime::Degraded);
        assert_eq!(d.active, Some(1));
        assert_eq!(d.imminent, vec![1], "只有 key1 在 24h 内重置");
    }

    #[test]
    fn 降级档_无临期_回到旧策略() {
        let d = decide_weekly(
            &[(1, Some(97.0)), (2, Some(96.0))],
            &[(1, 100, 97.0, 97.0), (2, 30, 96.0, 96.0)],
            None,
            None,
        );
        assert_eq!(d.regime, Regime::Degraded);
        assert_eq!(d.active, Some(2), "无临期 ⇒ 挑 pct 最低（旧行为）");
        assert_eq!(d.imminent, Vec::<i64>::new());
    }

    #[test]
    fn lookahead为0_策略关闭_等价旧版() {
        // 即便有 2h 后重置的 98%，lookahead=0 应完全忽略临期：96% 的才是活动 key
        let d = decide_weekly_lookahead(
            &[(1, Some(98.0)), (2, Some(96.0)), (3, Some(90.0))],
            &[(1, 2, 98.0, 98.0)],
            0,
        );
        assert_eq!(d.regime, Regime::Normal, "90% 在 throttle 下 ⇒ 正常档");
        assert_eq!(d.active, Some(3), "无临期 ⇒ 挑 restore 以下最低的（旧行为）");
        assert_eq!(d.imminent, Vec::<i64>::new());
    }

    #[test]
    fn 正常档_pin临期98仍按预防线解除() {
        // 正常档还有 50% key：98% 即使临期，pin 仍须按 95% 预防线解除。
        let d = decide_weekly(
            &[(1, Some(50.0)), (2, Some(98.0))],
            &[(2, 2, 98.0, 98.0)],
            Some(1),
            Some(2),
        );
        assert_eq!(d.active, Some(1));
        assert_eq!(
            d.pin_release,
            Some(PinRelease { channel_id: 2, pct: 98.0, limit: THROTTLE })
        );
    }

    #[test]
    fn pin_非临期98_照旧解除() {
        // 对照：同样 98% 但**不临期**（一周后才重置）→ 正常档越 95 线，pin 必解除
        let d = decide_weekly(
            &[(1, Some(50.0)), (2, Some(98.0))],
            &[(2, 168, 98.0, 98.0)],
            Some(1),
            Some(2),
        );
        assert_eq!(
            d.pin_release,
            Some(PinRelease { channel_id: 2, pct: 98.0, limit: THROTTLE })
        );
        assert_eq!(d.active, Some(1), "回到自动逻辑（粘滞留现任）");
    }

    // ——— 面板实时指标：从日志推导（2026-08-24 限流踩坑后改的）———

    fn log_at(channel: i64, age_secs: i64, tokens: i64) -> RequestLog {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        RequestLog {
            created_at: now - age_secs,
            channel,
            channel_name: format!("c{channel}"),
            model_name: "glm-5.3".into(),
            prompt_tokens: tokens,
            completion_tokens: tokens / 2,
            ..Default::default()
        }
    }

    /// 回归：rpm/tpm 只统计最近 60 秒；最后请求取倒序首条；未跟踪渠道不产出指标。
    /// 这替代了逐渠道 /api/log/stat 轮询（那个会把 /api 全局限流吃光，饿死 priority 写入）。
    #[test]
    fn 实时指标_从日志推导_窗口与最后请求正确() {
        let logs = vec![
            log_at(1, 10, 100),  // 渠道1：60s 内 → 计入 rpm/tpm；最新
            log_at(1, 300, 900), // 渠道1：窗口外 → 只算「最后请求」的更早候选，不计 rpm
            log_at(3, 30, 40),   // 渠道3：60s 内
            log_at(9, 5000, 1),  // 未跟踪渠道
        ];
        let out = live_metrics_from_logs(&[1, 2, 3], &logs);
        let get = |id: i64| out.iter().find(|m| m.channel_id == id).unwrap();

        let m1 = get(1);
        assert_eq!(m1.rpm, 1, "300 秒前那条不计入 rpm");
        assert_eq!(m1.tpm, 150, "只有 60s 内那条的 tokens");
        assert_eq!(m1.last_request_model.as_deref(), Some("glm-5.3"));

        let m2 = get(2);
        assert_eq!((m2.rpm, m2.tpm), (0, 0), "无日志的渠道给零值");

        let m3 = get(3);
        assert_eq!(m3.rpm, 1);
        assert_eq!(m3.tpm, 60);

        assert!(out.iter().all(|m| m.channel_id != 9), "未跟踪渠道不产出指标");
        assert_eq!(out.len(), 3, "tracked 顺序一一对应");
    }

    // ——— 高峰时段（智谱：每日 14:00–18:00 UTC+8）———

    /// 给定「UTC+8 的某日某时某分」，返回 epoch 秒。
    fn at(h: i64, m: i64) -> i64 {
        // 2026-07-12 00:00 UTC+8 = 1783785600
        1_783_785_600 + h * 3600 + m * 60
    }
    fn peak_at(h: i64, m: i64) -> (bool, i64) {
        peak_state(at(h, m), 14, 18, 8)
    }

    #[test]
    fn 高峰窗口边界_左闭右开() {
        assert!(!peak_at(13, 59).0, "13:59 还不是高峰");
        assert!(peak_at(14, 0).0, "14:00 整点进入高峰（闭）");
        assert!(peak_at(17, 59).0, "17:59 仍在高峰");
        assert!(!peak_at(18, 0).0, "18:00 整点离开高峰（开）");
    }

    #[test]
    fn 峰前_倒计时指向今天的峰头() {
        let (is_peak, next) = peak_at(9, 30);
        assert!(!is_peak);
        assert_eq!(next, at(14, 0), "9:30 → 4.5 小时后进入高峰");
    }

    #[test]
    fn 峰中_倒计时指向峰尾() {
        let (is_peak, next) = peak_at(15, 0);
        assert!(is_peak);
        assert_eq!(next, at(18, 0), "15:00 → 3 小时后离开高峰");
    }

    #[test]
    fn 峰后_倒计时跨天指向明天的峰头() {
        let (is_peak, next) = peak_at(20, 40);
        assert!(!is_peak);
        assert_eq!(next, at(14, 0) + 86_400, "20:40 → 明天 14:00");
    }

    #[test]
    fn 跨零点也不出错() {
        let (is_peak, next) = peak_at(0, 5);
        assert!(!is_peak);
        assert_eq!(next, at(14, 0), "凌晨 0:05 → 今天 14:00");
    }

    #[test]
    fn 按配置时区算而不是本机时区() {
        // 同一个绝对时刻：在 UTC+8 是 15:00（高峰），在 UTC+0 就是 07:00（非高峰）。
        // 若代码里图省事用了本机时区，这条会挂。
        let t = at(15, 0);
        assert!(peak_state(t, 14, 18, 8).0, "UTC+8 视角：15:00 是高峰");
        assert!(!peak_state(t, 14, 18, 0).0, "UTC+0 视角：同一时刻是 07:00，非高峰");
    }
}
