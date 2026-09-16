# 架构设计 — cache-pool（缓存命中池：逐请求路由代理）

> 2026-09-16 四项改造方案（已获批）之 F4。实施记录（F4a 透传拓扑 eaa48b7 / F4b 评分路由 85e3b87）；
> 行为以代码为准，冲突先修本文。

## 1. 结构

```text
opencode / Claude Code ──> 代理(hyper :3000, src/proxy.rs)
                            ├─ POST /v1/chat/completions、/v1/messages
                            │    → route_llm（src/router.rs 评分选路）
                            │    → Bearer sk-<qt-proxy 令牌>-<channelId>（N1 原生指定渠道）
                            │    → new-api(127.0.0.1:13000) → 流式回传
                            └─ 其余路径（/、/api/*）→ 原样透传
```

- `[cache_pool] enabled=false`（默认）= 逐字节回到旧拓扑（客户端直连 new-api），存量配置零迁移。
- base_url/upstream 语义分离：base_url = 客户端入口（代理监听、快照 endpoint 展示）；
  upstream = 内部 new-api（管理面/健康检查/代理上游，见 `Config::upstream_base()`）。
- bind 失败 = fail-fast（与看板降级相反）；代理任务退出 → run_loop select → 整进程退出。

## 2. 选路（src/router.rs）

优先级：**pin（合格集内）> 缓存命中（渠道合格且未冷却）> 评分 argmax**。

`score = 0.6·week + 0.2·cap + 0.2·load`：
- `week = 1 − remaining/Σremaining`（remaining = max(0, weekly_reset − now)；
  **无周窗口按最远重置（7 天哨兵）入 Σ**——直接给 0 分会推出「唯一有 deadline 者
  = Σ 全部 ⇒ 得 0 分」的悖论，与 EDF 直觉相反；两把都有窗口时与用户示例数值一致）
- `cap = clamp[0,1] min(1−5h_pct/100, (1−周_pct/100)×ratio)`，ratio 可配（默认 15.5/3.5）
- `load = 1 − cnt(60s)/Σcnt`；Σ=0 全 1
- 平手取 max_pct 低者。可用性 = 共享快照 eligible（95% / 全员 95% 退 100%，与调度同源）

缓存指纹 `SHA256(model ‖ system ‖ tools ‖ 首条 user 消息前 32KiB)`：
- serde_json 默认 BTreeMap → canonical 键序无关（**禁启用 preserve_order**）
- Claude system 剥 cache_control（客户端会挪标记位置致 key 漂移）；首条 user 消息整体
  canonical——对话追加轮次不换指纹，compact 后换（旧缓存已失效，正好重算）
- 坏输入 → key=None → 评分路由不记池

## 3. 关键决策

| 决策 | 理由 |
|---|---|
| N1 后缀机制（sk-key-channelId）而非每渠道分组+令牌 | 零分组配置；分组方案须同时写 UserUsableGroups+GroupRatio 两个 option（fallback 备案）。⚠️ 无版本兼容承诺，升级 new-api 须回归 |
| 两把中继令牌（openai/claude 按入口分） | 保留 new-api 日志 token_name 归因；均 unlimited、root 名下（role≥10 满足 N1） |
| 快照无数据/候选耗尽 → 降级透传而非 503 | 启动窗口 60s 内不该拒绝全部客户端；priority 阶梯兜底 |
| 重试 {429,500,502,503,504}+连接错误，≤2 次换道 | 500 含上游转发失败；401/403 不重试（user 级问题）。取舍：可能双渠道各扣一次费 |
| 429 后 15s 冷却 | eligible 60s 才刷新；否则缓存命中反复撞刚 429 的渠道。全员冷却时兜底忽略冷却 |
| RouterState 单把 std Mutex | 池/负载/冷却/统计临界区全 O(1) 纯同步不跨 await，无锁序问题 |
| 连接级信号量（默认 64） | permit 活到 serve_connection 返回（含响应体流尽）——慢客户端占坑即背压 |
| 请求体聚合（Limited 16MiB）仅 LLM 路径 | cache_key + 重试复用（Bytes clone 零拷贝）；透传路径纯流式不落缓冲 |
| 双 hyper 栈（server 1.x + reqwest 0.11 内置 0.14） | 不为此升级 reqwest；两套 http crate 头/状态码按字节转换 |

## 4. 模块规约（摘要）

- `router::choose(view, router, tried, hint, now_ms, ratio) -> Choice{Channel|NoData|NoEligible}`（纯函数）
- `router::RouterState::{lookup, record, cool, retain_channel, stats}`（单锁）
- `proxy::route_llm`：聚合→指纹→[选路→转发→(可重试? 冷却+换道)]→record+stats
- 写者分工：决策字段=orchestrator、面板字段=Panel、`cache_pool` 字段=代理（互不相交）
- 日志红线：任何分支不得打印 Authorization / x-api-key / 中继令牌

## 5. 验证（单测 12 + 集成 4 已就位；e2e 留待真环境）

单测：评分（临期主导/无周窗口/容量/负载/平手）、排除已试、无数据降级、pin/命中优先、
池合格约束与驱逐、缓存键（Claude/OpenAI 键序无关、追加稳定、坏输入 None）、冷却挡命中与
全灭兜底、快照提取。集成：mock 上游透传、上游挂 502、429 换道+后缀生效、缺鉴权 401。
真环境 e2e：Claude Code x-api-key/流式、同会话粘渠道（hit 日志）、95% 切走、命中率观测
（key 抖动排障：`debug` 日志 key 前 8 hex + hit/miss）。
