//! 缓存池路由（F4b）：逐请求选渠道的纯逻辑 + 单锁共享状态。
//!
//! 选路优先级：**pin（合格集内）> 缓存命中（渠道可用）> 评分公式**。
//! 公式：score = 0.6·周刷新临期 + 0.2·容量 + 0.2·负载
//!   · 周临期 = 1 − remaining/Σremaining（EDF 平滑版：A 还剩 1h、B 剩 24h → A 得 24/25）
//!   · 容量 = clamp[0,1] min(5h 剩余%, 周剩余% × ratio)（找能扛住新请求上下文的渠道）
//!   · 负载 = 1 − 该渠道 60s 请求数占比（Σ=0 时全 1，负载中性）
//! 可用性 = 共享快照的合格集（95% 不可用 / 全员 95% 退到 100%）——与现有调度完全同源。
//!
//! 单锁原则：CachePool/LRU/负载计数/429 冷却/统计全在 `RouterState` 一把 std Mutex 里，
//! 临界区全 O(1) 纯同步不跨 await——LLM 请求速率（个位数 rps）下无争用可言。

use crate::status::StatusSnapshot;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Mutex, RwLockReadGuard};
use std::time::{Duration, Instant};

/// 缓存池 LRU 上界（条目几十字节，4096 条内存可忽略）
pub const POOL_CAP: usize = 4096;
/// 429 冷却时长：eligible 快照 60s 才刷新，冷却挡住「缓存命中反复撞刚 429 的渠道」
pub const COOLDOWN: Duration = Duration::from_secs(15);
/// 负载计数窗口
pub const LOAD_WINDOW: Duration = Duration::from_secs(60);

/// 一次路由决策所需的快照最小集（read guard 内提取，不克隆整包面板字段）
#[derive(Debug, Clone, Default)]
pub struct RouteView {
    pub eligible: Vec<i64>,
    pub pinned: Option<i64>,
    /// channel_id → (five_hour_pct, weekly_pct, weekly_reset_ms, max_pct)；pct None = 未取到
    pub keys: HashMap<i64, KeyQuota>,
    pub has_data: bool,
}

#[derive(Debug, Clone, Default)]
pub struct KeyQuota {
    pub five_hour_pct: Option<f64>,
    pub weekly_pct: Option<f64>,
    pub weekly_reset_ms: Option<i64>,
    pub max_pct: Option<f64>,
}

impl RouteView {
    /// 在读锁内提取（锁外不做任何事——面板循环 5s 一刷，不能被路由读拖住）
    pub fn from_snap(snap: &RwLockReadGuard<'_, StatusSnapshot>) -> Self {
        Self {
            eligible: snap.eligible.clone(),
            pinned: snap.pinned_channel_id,
            keys: snap
                .keys
                .iter()
                .map(|k| {
                    (
                        k.channel_id,
                        KeyQuota {
                            five_hour_pct: k.five_hour_pct,
                            weekly_pct: k.weekly_pct,
                            weekly_reset_ms: k.weekly_reset,
                            max_pct: k.max_pct,
                        },
                    )
                })
                .collect(),
            has_data: !snap.keys.is_empty(),
        }
    }
}

/// 单锁路由状态：缓存池 + 负载计数 + 429 冷却 + 统计。
/// 弃用 key 时按 channel_id 清条目（`retain_channel`）。
pub struct RouterState {
    inner: Mutex<Inner>,
    pub pool_cap: usize,
}

struct Inner {
    /// cache_key → 渠道。LRU：容量满时驱逐 last_seen 最旧（条目小，插入频率低，扫一遍可接受）
    pool: HashMap<u64, PoolEntry>,
    /// 每渠道 60s 请求时间戳（记**每次发出**含重试——重试热点要反映在负载里）
    loads: HashMap<i64, std::collections::VecDeque<Instant>>,
    /// 429 冷却到何时
    cooldown_until: HashMap<i64, Instant>,
    hits: u64,
    misses: u64,
    /// 每渠道累计路由数（面板展示）
    routed: HashMap<i64, u64>,
}

#[derive(Clone, Copy)]
struct PoolEntry {
    channel_id: i64,
    last_seen: Instant,
}

/// 看板用的统计快照
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RouterStats {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub routed_per_channel: Vec<(i64, u64)>,
}

impl RouterState {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                pool: HashMap::new(),
                loads: HashMap::new(),
                cooldown_until: HashMap::new(),
                hits: 0,
                misses: 0,
                routed: HashMap::new(),
            }),
            pool_cap: POOL_CAP,
        }
    }

    /// 缓存命中查询：命中要求**渠道在本轮合格集内**（可用性与调度同源）。
    /// 冷却判定放 `choose`（命中+冷却也走公式，但全灭时仍可用它兜底）。
    pub fn lookup(&self, key: u64, eligible: &[i64]) -> Option<i64> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let hit = g
            .pool
            .get_mut(&key)
            .map(|e| {
                e.last_seen = now;
                e.channel_id
            })
            .filter(|id| eligible.contains(id));
        if hit.is_some() {
            g.hits += 1;
        } else {
            g.misses += 1;
        }
        hit
    }

    /// 记录/更新 cache_key → 渠道（重试成功后也指向新渠道）
    pub fn record(&self, key: Option<u64>, channel_id: i64) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(key) = key {
            if g.pool.len() >= self.pool_cap && !g.pool.contains_key(&key) {
                // 驱逐最旧
                if let Some(oldest) = g
                    .pool
                    .iter()
                    .min_by_key(|(_, e)| e.last_seen)
                    .map(|(k, _)| *k)
                {
                    g.pool.remove(&oldest);
                }
            }
            g.pool.insert(
                key,
                PoolEntry {
                    channel_id,
                    last_seen: Instant::now(),
                },
            );
        }
        g.loads.entry(channel_id).or_default().push_back(Instant::now());
        *g.routed.entry(channel_id).or_insert(0) += 1;
    }

    /// 某渠道 429 → 记冷却（选路时叠加 eligible 判定）
    pub fn cool(&self, channel_id: i64) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.cooldown_until
            .insert(channel_id, Instant::now() + COOLDOWN);
    }

    /// 每渠道 60s 内发出次数（含重试；时间源 Instant 单调，不受系统时钟跳变影响）
    fn load_counts(&self) -> HashMap<i64, usize> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let mut out = HashMap::new();
        for (id, q) in g.loads.iter_mut() {
            while let Some(t) = q.front() {
                if now.duration_since(*t) > LOAD_WINDOW {
                    q.pop_front();
                } else {
                    break;
                }
            }
            if !q.is_empty() {
                out.insert(*id, q.len());
            }
        }
        out
    }

    /// 选路用：当前是否在 429 冷却中
    pub(crate) fn is_cooled(&self, id: i64) -> bool {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.cooldown_until
            .get(&id)
            .map(|t| *t > Instant::now())
            .unwrap_or(false)
    }

    /// 选路用：负载计数快照（评分公式输入）
    pub(crate) fn load_counts_pub(&self) -> HashMap<i64, usize> {
        self.load_counts()
    }

    /// 弃用/删除渠道时清掉它的池条目与计数（channel_id 索引，改名无需处理）
    pub fn retain_channel(&self, keep: impl Fn(i64) -> bool) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.pool.retain(|_, e| keep(e.channel_id));
        g.loads.retain(|id, _| keep(*id));
        g.cooldown_until.retain(|id, _| keep(*id));
        g.routed.retain(|id, _| keep(*id));
    }

    pub fn stats(&self) -> RouterStats {
        let g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        RouterStats {
            entries: g.pool.len(),
            hits: g.hits,
            misses: g.misses,
            routed_per_channel: {
                let mut v: Vec<(i64, u64)> = g.routed.iter().map(|(k, c)| (*k, *c)).collect();
                v.sort_by_key(|(id, _)| *id);
                v
            },
        }
    }
}

impl Default for RouterState {
    fn default() -> Self {
        Self::new()
    }
}

/// 选路结果
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Choice {
    /// 用这个渠道（附带是不是 pin/缓存命中，供日志与统计）
    Channel { id: i64, via: Via },
    /// 快照无任何 key 数据（刚启动）→ 调用方降级透传，不 503
    NoData,
    /// 合格集空（全员 ≥ exhausted）→ 调用方透传给 new-api 自己跌（对齐「不清空、交给兜底」）
    /// —— 也可以理解为 best-effort：让 new-api 的 priority 阶梯决定落点
    NoEligible,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Via {
    Pin,
    CacheHit,
    Score,
}

/// 选渠道（纯函数，可单测）。候选 = eligible ∖ tried；
/// 冷却渠道视为不可用，但若「全灭」则忽略冷却用回 eligible ∖ tried（best-effort）。
pub fn choose(
    view: &RouteView,
    router: &RouterState,
    tried: &[i64],
    cache_hint: Option<i64>,
    now_ms: i64,
    ratio: f64,
) -> Choice {
    if !view.has_data {
        return Choice::NoData;
    }
    let base: Vec<i64> = view
        .eligible
        .iter()
        .copied()
        .filter(|id| !tried.contains(id))
        .collect();
    if base.is_empty() {
        return Choice::NoEligible;
    }

    // pin 最高（合格集内、未试过、未冷却——pin 撞 429 也该退位）
    if let Some(p) = view.pinned {
        if base.contains(&p) && !router.is_cooled(p) {
            return Choice::Channel {
                id: p,
                via: Via::Pin,
            };
        }
    }
    // 缓存命中（合格集内、未试过、未冷却）
    if let Some(h) = cache_hint {
        if base.contains(&h) && !router.is_cooled(h) {
            return Choice::Channel {
                id: h,
                via: Via::CacheHit,
            };
        }
    }

    // 评分：先按「eligible∖tried 且未冷却」；全灭则忽略冷却
    let mut candidates: Vec<i64> = base
        .iter()
        .copied()
        .filter(|id| !router.is_cooled(*id))
        .collect();
    if candidates.is_empty() {
        candidates = base;
    }
    let loads = router.load_counts_pub();
    let best = argmax_score(&candidates, view, &loads, now_ms, ratio);
    Choice::Channel {
        id: best,
        via: Via::Score,
    }
}

/// 评分取 argmax（纯函数）。平手取 max_pct 低者，再平取 channel_id 小者。
fn argmax_score(
    candidates: &[i64],
    view: &RouteView,
    loads: &HashMap<i64, usize>,
    now_ms: i64,
    ratio: f64,
) -> i64 {
    // —— 周临期分（EDF 平滑）——
    // 无周窗口（如个人套餐）按「最远重置」处理（周窗口周期上限 7 天）：
    // 若直接给 0 分，会推出「唯一有 deadline 的候选 = Σ 的全部 ⇒ 得 0 分」的悖论
    // （1−r/Σ 恒为 0）——它与 EDF 直觉相反；当最远值入 Σ 后，
    // 有 deadline 者天然高分、无窗口者天然低分，且不改变用户示例（两把都有窗口）的数值。
    const MAX_WEEK_MS: f64 = 7.0 * 24.0 * 3600.0 * 1000.0;
    let remainings: Vec<f64> = candidates
        .iter()
        .map(|id| {
            view.keys
                .get(id)
                .and_then(|k| k.weekly_reset_ms)
                .map(|reset| (((reset - now_ms).max(0)) as f64).min(MAX_WEEK_MS))
                .unwrap_or(MAX_WEEK_MS)
        })
        .collect();
    let sum_remaining: f64 = remainings.iter().sum();
    // —— 负载分 ——
    let sum_load: usize = candidates.iter().map(|id| loads.get(id).copied().unwrap_or(0)).sum();

    let mut best = candidates[0];
    let mut best_score = f64::MIN;
    for (i, id) in candidates.iter().enumerate() {
        let q = view.keys.get(id);
        // 周临期：1 − remaining/Σ。Σ=0 只剩一种真实情形：全部候选的 reset 时刻都已过
        // （同团队共享重置边界 + 快照 60s 陈旧跨过边界）——0/0=NaN 会静默绕过整个评分
        // （NaN 比较全 false → 永远选第一个候选），此时全按「最临期」处理。
        let week = if sum_remaining > 0.0 {
            1.0 - remainings[i] / sum_remaining
        } else {
            1.0
        };
        // 容量：min(5h 剩余, 周剩余 × ratio)；窗口缺失按 1.0（缺哪项用另一项）
        let five_avail = q
            .and_then(|k| k.five_hour_pct)
            .map(|p| (1.0 - p / 100.0).clamp(0.0, 1.0))
            .unwrap_or(1.0);
        let week_avail = q
            .and_then(|k| k.weekly_pct)
            .map(|p| (1.0 - p / 100.0).clamp(0.0, 1.0))
            .unwrap_or(1.0);
        let cap = (five_avail).min(week_avail * ratio).clamp(0.0, 1.0);
        // 负载：Σ=0 → 全 1（中性）
        let load = if sum_load == 0 {
            1.0
        } else {
            1.0 - loads.get(id).copied().unwrap_or(0) as f64 / sum_load as f64
        };
        let score = 0.6 * week + 0.2 * cap + 0.2 * load;
        let better = score > best_score
            || (score == best_score && {
                let a = view.keys.get(id).and_then(|k| k.max_pct);
                let b = view.keys.get(&best).and_then(|k| k.max_pct);
                match (a, b) {
                    (Some(a), Some(b)) => a < b,
                    (Some(_), None) => true,
                    _ => false,
                }
            });
        if better {
            best_score = score;
            best = *id;
        }
    }
    best
}

// —— cache_key：跨轮稳定的「对话指纹」——

/// 首条用户消息 canonical 序列化后参与哈希的字节上界（首条消息开头就是任务文本，
/// 前几十字节就分化；截断只影响「路由共槽」不影响正确性）
const KEY_BYTES_CAP: usize = 32 * 1024;

/// 提取对话指纹。None = 解析失败/找不到 user 消息（评分路由但不记池）。
/// serde_json Value 默认 BTreeMap → to_vec 天然 canonical（键序无关）；
/// ⚠️ 不得启用 serde_json 的 preserve_order feature（会破坏 canonical 性）。
pub fn cache_key(path: &str, body: &[u8]) -> Option<u64> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let is_claude = path.contains("/v1/messages");
    let model = v.get("model")?.as_str()?.to_string();

    let messages = v.get("messages")?.as_array()?;
    // system：Claude 顶层 string/blocks；OpenAI 是 messages 里的 role=system
    let system: String = if is_claude {
        normalize_system(v.get("system"))
    } else {
        messages
            .iter()
            .filter(|m| m.get("role").and_then(|r| r.as_str()) == Some("system"))
            .map(|m| strip_cache_control(m).to_string())
            .collect::<Vec<_>>()
            .join("\n")
    };
    let tools = strip_cache_control(&v.get("tools").cloned().unwrap_or(Value::Null));

    // 首条用户消息：整体 Value canonical 序列化截前 32KiB（覆盖 string/blocks/tool_result/image）。
    // **递归剥 cache_control**：Claude Code 把 ephemeral 断点标在「最新」消息上——第 1 轮在
    // 首条 user 消息、第 2 轮起挪到更新的消息——不剥的话每个对话第 2 轮起指纹全变，
    // 恰好在「前缀与第 1 轮完全重合」（缓存价值最高）的那一拍 miss。
    let first_user = messages
        .iter()
        .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("user"))?;
    let msg_bytes = serde_json::to_vec(&strip_cache_control(first_user)).ok()?;
    let prefix = &msg_bytes[..msg_bytes.len().min(KEY_BYTES_CAP)];

    let mut h = Sha256::new();
    h.update(model.as_bytes());
    h.update([0u8]);
    h.update(system.as_bytes());
    h.update([0u8]);
    h.update(&serde_json::to_vec(&tools).ok()?);
    h.update([0u8]);
    h.update(prefix);
    let digest = h.finalize();
    Some(u64::from_be_bytes(digest[..8].try_into().ok()?))
}

/// 递归剥掉所有对象里的 `cache_control` 键（原地拷贝，不改输入）。
/// cache_control 是客户端的缓存断点**摆放指令**，随对话推进会移动位置——
/// 它属于「会话状态」不属于「对话内容」，进指纹只会制造无谓漂移。
fn strip_cache_control(v: &Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut out = serde_json::Map::new();
            for (k, val) in m {
                if k == "cache_control" {
                    continue;
                }
                out.insert(k.clone(), strip_cache_control(val));
            }
            Value::Object(out)
        }
        Value::Array(a) => Value::Array(a.iter().map(strip_cache_control).collect()),
        other => other.clone(),
    }
}

/// Claude 的 system 归一化：string 直接用；blocks 按序拼 text 并**剥 cache_control**
/// （Claude Code 会把 cache_control 标记在最后一个 block 上来回挪，不剥则 key 无谓漂移）。
fn normalize_system(sys: Option<&Value>) -> String {
    match sys {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(blocks)) => blocks
            .iter()
            .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::StatusSnapshot;

    fn view(eligible: &[i64], keys: Vec<(i64, Option<f64>, Option<f64>, Option<i64>, Option<f64>)>) -> RouteView {
        // (id, five_pct, weekly_pct, weekly_reset_ms, max_pct)
        RouteView {
            eligible: eligible.to_vec(),
            pinned: None,
            keys: keys
                .into_iter()
                .map(|(id, f, w, r, m)| {
                    (
                        id,
                        KeyQuota {
                            five_hour_pct: f,
                            weekly_pct: w,
                            weekly_reset_ms: r,
                            max_pct: m,
                        },
                    )
                })
                .collect(),
            has_data: true,
        }
    }

    #[test]
    fn 评分_周临期主导_a剩1小时得高分() {
        // A 1h 后重置、B 24h：week_A = 1 - 1/25 = 0.96
        let v = view(
            &[1, 2],
            vec![
                (1, Some(30.0), Some(30.0), Some(3_600_000), Some(30.0)),
                (2, Some(30.0), Some(30.0), Some(86_400_000), Some(30.0)),
            ],
        );
        let r = RouterState::new();
        match choose(&v, &r, &[], None, 0, 4.43) {
            Choice::Channel { id, .. } => assert_eq!(id, 1, "1 小时后重置的应胜出"),
            c => panic!("{c:?}"),
        }
    }

    #[test]
    fn 评分_无周窗口视为最远重置_不抢优先() {
        let v = view(
            &[1, 2],
            vec![
                (1, None, None, None, None), // 无周窗口（如个人套餐）→ 按 7 天最远重置
                (2, Some(50.0), Some(50.0), Some(3600_000), Some(50.0)),
            ],
        );
        let r = RouterState::new();
        match choose(&v, &r, &[], None, 0, 4.43) {
            // 2 的 week≈0.994（快重置的额度先烧）；1 的容量分虽高（1.0 vs 0.5）
            // 但 0.6 权重下 2 胜出
            Choice::Channel { id, .. } => assert_eq!(id, 2),
            c => panic!("{c:?}"),
        }
    }

    #[test]
    fn 评分_5h烧满压低容量分() {
        // A 的 5h 已 99%（cap≈0.01），B 5h 30%（cap 高）——其余同
        let v = view(
            &[1, 2],
            vec![
                (1, Some(99.0), Some(30.0), Some(3_600_000), Some(99.0)),
                (2, Some(30.0), Some(30.0), Some(3_600_000), Some(30.0)),
            ],
        );
        let r = RouterState::new();
        match choose(&v, &r, &[], None, 0, 4.43) {
            Choice::Channel { id, .. } => assert_eq!(id, 2, "容量更足的应胜出"),
            c => panic!("{c:?}"),
        }
    }

    #[test]
    fn 评分_负载分散() {
        let v = view(
            &[1, 2],
            vec![
                (1, Some(30.0), Some(30.0), Some(3_600_000), Some(30.0)),
                (2, Some(30.0), Some(30.0), Some(3_600_000), Some(30.0)),
            ],
        );
        let r = RouterState::new();
        r.record(None, 1);
        r.record(None, 1);
        r.record(None, 1); // 渠道 1 已有 3 连发
        match choose(&v, &r, &[], None, 0, 4.43) {
            Choice::Channel { id, .. } => assert_eq!(id, 2, "同分下负载低的胜出"),
            c => panic!("{c:?}"),
        }
    }

    #[test]
    fn 选路_排除已试_全试完报noeligible() {
        let v = view(&[1], vec![(1, Some(10.0), Some(10.0), Some(3600_000), Some(10.0))]);
        let r = RouterState::new();
        assert_eq!(
            choose(&v, &r, &[1], None, 0, 4.43),
            Choice::NoEligible,
            "唯一候选已试过 → 交给透传兜底"
        );
    }

    #[test]
    fn 评分_全部重置时刻已过_不产生nan绕过评分() {
        // 同团队共享周重置边界，快照 60s 陈旧跨过边界 → 全部 remaining=0（PR review #8）
        let v = view(
            &[1, 2],
            vec![
                (1, Some(80.0), Some(80.0), Some(1000), Some(80.0)), // reset 已过
                (2, Some(10.0), Some(10.0), Some(2000), Some(10.0)),
            ],
        );
        let r = RouterState::new();
        match choose(&v, &r, &[], None, 10_000, 4.43) {
            // 全按最临期（week=1）平手 → 容量/负载正常参与：2 的 max_pct 更低应胜出
            Choice::Channel { id, .. } => assert_eq!(id, 2, "Σ=0 时不应 NaN 绕过评分"),
            c => panic!("{c:?}"),
        }
    }

    #[test]
    fn 选路_无数据降级_不503() {
        let v = RouteView::default();
        let r = RouterState::new();
        assert_eq!(choose(&v, &r, &[], None, 0, 4.43), Choice::NoData);
    }

    #[test]
    fn 选路_pin与缓存命中优先() {
        let v = view(&[1, 2], vec![(1, None, None, None, None), (2, None, None, None, None)]);
        let r = RouterState::new();
        // pin 合格集内 → Pin
        let mut vp = v.clone();
        vp.pinned = Some(2);
        assert_eq!(
            choose(&vp, &r, &[], None, 0, 4.43),
            Choice::Channel { id: 2, via: Via::Pin }
        );
        // pin 不在合格集 → 回公式
        let mut vp2 = v.clone();
        vp2.pinned = Some(9);
        assert!(matches!(choose(&vp2, &r, &[], None, 0, 4.43), Choice::Channel { .. }));
    }

    #[test]
    fn 池_命中要求合格_容量驱逐() {
        let v = view(&[1], vec![(1, None, None, None, None)]);
        let r = RouterState::new();
        r.record(Some(100), 1);
        assert_eq!(r.lookup(100, &[1]), Some(1), "渠道合格 → 命中");
        assert_eq!(r.lookup(100, &[2]), None, "渠道不在合格集 → 不命中（miss）");

        let mut st = r.stats();
        assert_eq!(st.entries, 1);
        // 容量驱逐：pool_cap=2，插 3 个不同 key，最旧的 100 应被驱逐
        let r2 = RouterState { pool_cap: 2, ..RouterState::new() };
        r2.record(Some(100), 1);
        r2.record(Some(200), 1);
        r2.record(Some(300), 1);
        st = r2.stats();
        assert_eq!(st.entries, 2);
    }

    #[test]
    fn 缓存键_claude格式_键序无关_blocks_system剥cache_control() {
        let a = br#"{"model":"m","system":"hi","tools":[{"name":"t"}],"messages":[{"role":"user","content":"hello"}]}"#;
        let b = br#"{"tools":[{"name":"t"}],"messages":[{"role":"user","content":"hello"}],"system":"hi","model":"m"}"#;
        assert_eq!(cache_key("/v1/messages", a), cache_key("/v1/messages", b));

        // blocks 形式的 system：cache_control 挪位置不影响 key
        let c = br#"{"model":"m","system":[{"type":"text","text":"s1","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"x"}]}"#;
        let d = br#"{"model":"m","system":[{"type":"text","text":"s1"}],"messages":[{"role":"user","content":"x"}]}"#;
        assert_eq!(cache_key("/v1/messages", c), cache_key("/v1/messages", d));
    }

    #[test]
    fn 缓存键_openai格式_system拼接_消息追加不换键() {
        // OpenAI：system 在 messages 里
        let t1 = br#"{"model":"m","messages":[{"role":"system","content":"sys"},{"role":"user","content":"task"}]}"#;
        let t2 = br#"{"model":"m","messages":[{"role":"system","content":"sys"},{"role":"user","content":"task"},{"role":"assistant","content":"ok"},{"role":"user","content":"more"}]}"#;
        assert_eq!(
            cache_key("/v1/chat/completions", t1),
            cache_key("/v1/chat/completions", t2),
            "对话追加轮次不应换指纹（头部稳定）"
        );
        // 不同任务文本 → 不同 key
        let t3 = br#"{"model":"m","messages":[{"role":"system","content":"sys"},{"role":"user","content":"other"}]}"#;
        assert_ne!(cache_key("/v1/chat/completions", t1), cache_key("/v1/chat/completions", t3));
    }

    #[test]
    fn 缓存键_claude_断点挪到新消息不换指纹() {
        // 第 1 轮：ephemeral 断点在首条 user 消息上
        let t1 = br#"{"model":"m","system":"s","messages":[
            {"role":"user","content":[{"type":"text","text":"task","cache_control":{"type":"ephemeral"}}]}]}"#;
        // 第 2 轮：断点挪到最新消息，首条已无标记——指纹必须不变（PR review #5）
        let t2 = br#"{"model":"m","system":"s","messages":[
            {"role":"user","content":[{"type":"text","text":"task"}]},
            {"role":"assistant","content":"ok"},
            {"role":"user","content":[{"type":"text","text":"more","cache_control":{"type":"ephemeral"}}]}]}"#;
        assert_eq!(
            cache_key("/v1/messages", t1),
            cache_key("/v1/messages", t2),
            "断点位置属于会话状态不属于对话内容，进指纹会致第 2 轮起永远 miss"
        );
    }

    #[test]
    fn 缓存键_坏输入返回none() {
        assert_eq!(cache_key("/v1/messages", b"not json"), None);
        assert_eq!(cache_key("/v1/messages", br#"{"model":"m"}"#), None);
        assert_eq!(
            cache_key("/v1/messages", br#"{"model":"m","messages":[]}"#),
            None,
            "无 user 消息 → 不记池"
        );
    }

    #[test]
    fn 冷却_429后挡缓存命中_冷却全灭时兜底可用() {
        let v = view(&[1, 2], vec![(1, None, None, None, None), (2, None, None, None, None)]);
        let r = RouterState::new();
        r.record(Some(100), 1);
        r.cool(1);
        // 命中渠道 1 在冷却 → 回公式；两把同分，但 1 被冷却排除 → 选 2
        match choose(&v, &r, &[], r.lookup(100, &[1, 2]), 0, 4.43) {
            Choice::Channel { id, .. } => assert_eq!(id, 2),
            c => panic!("{c:?}"),
        }
        // 全员冷却 → 忽略冷却用回候选（best-effort）
        r.cool(2);
        match choose(&v, &r, &[], None, 0, 4.43) {
            Choice::Channel { .. } => {}
            c => panic!("全灭时应兜底选一个：{c:?}"),
        }
    }

    #[test]
    fn 快照提取_fields映射() {
        let mut snap = StatusSnapshot::default();
        snap.eligible = vec![3];
        snap.pinned_channel_id = Some(3);
        snap.keys.push(crate::status::KeyStatus {
            channel_id: 3,
            five_hour_pct: Some(11.0),
            weekly_pct: Some(22.0),
            weekly_reset: Some(123),
            max_pct: Some(22.0),
            ..Default::default()
        });
        let lock = std::sync::RwLock::new(snap);
        let g = lock.read().unwrap();
        let v = RouteView::from_snap(&g);
        assert_eq!(v.eligible, vec![3]);
        assert_eq!(v.pinned, Some(3));
        assert_eq!(v.keys.get(&3).unwrap().weekly_reset_ms, Some(123));
        assert!(v.has_data);
    }
}
