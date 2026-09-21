# 子计划 ccr-1 — Claude 下游改原生 Anthropic 透传（双渠道）

> 状态：架构稿（待用户确认 → 细化 → v2 审核 → 实施）
> 调研：`docs/research/claude-code-routing-research.md` §6（由本子计划引入）
> 拍板：2026-09-17 用户在 A0/A1/A2 三方案中选定 **A1 双渠道**。

## 0. 背景与动机（为什么推翻 2026-08-30 的单渠道拍板）

现行架构（claude-code-routing 主架构）让 `/v1/messages` 走 new-api OpenAI adaptor 的
**Anthropic→OpenAI→Anthropic 双向转换**。2026-08-30 实测「普通+流式+tools 能通」成立，
但真实使用暴露两类问题，且调研拿到了新的硬证据：

1. **有损转换的实际伤害**：用户报告间歇性「分类器错误、工具调用错误」（复杂 schema /
   并行工具 / thinking 块 / 流式 input_json_delta 等边缘场景）。简单场景实测至今能通
   （2026-09-17 复测），错误是间歇性的——与转换层的边缘 case 特征吻合。
2. **缓存提示被剥掉（2026-09-17 实测）**：带 `cache_control: ephemeral` 断点的两次相同
   请求过中转，`cache_creation_input_tokens` 与 `cache_read_input_tokens` 均为 0——
   OpenAI 线上格式没有 cache_control 字段，转换必然丢弃。本项目首要目标「护住 prompt
   缓存局部性」在 Claude 路径上完全落空。
3. **新事实：智谱原生 Anthropic 口可用**（三方交叉验证）：官方文档 Coding Plan 双协议
   （`https://open.bigmodel.cn/api/anthropic`）；用户 Claude Code 直连在用；用本仓 key
   实测 `x-api-key` 鉴权直打成功。
4. **新事实：new-api rc.20 Claude 渠道(type 14) 是真透传**（源码）：
   `ConvertClaudeRequest` 原样返回；URL=`{base_url}/v1/messages`；dto 保留
   `cache_control`/`thinking`/`tools`/`tool_choice`（`dto/claude.go`）。

**对旧否决理由的回应**：主架构否决的旧双渠道草案把「下游格式」固化进了**凭据**
（Claude 专用 token/group）和**调度状态**（独立 priority 联动）。本子计划的双渠道不碰
这两层：token/group 不分叉（I2/I5 原样），quota/pin/合格集/冷却/负载/亲和**仍每 key
一份**（I4 原样），Claude 渠道只是同一把 key 在 Anthropic 协议上的**第二个物理出口**，
调度逻辑感知的唯一标识仍是 key（= 现有 OpenAI 渠道 id，下称**逻辑 id**）。

## 1. 结构（目标拓扑）

```text
                        ┌─ POST /v1/chat/completions ──→ new-api ─→ <name>      渠道(type 8,  coding口)  ─OpenAI原生─→ 智谱
opencode / Claude Code ─┤                                 ↑N1后缀
  → 代理 :3000          └─ POST /v1/messages ──────────→ new-api ─→ <name>-claude 渠道(type 14, /api/anthropic) ─Anthropic原生透传─→ 智谱

  同一把 key 的两个渠道：同一 zhipu key 凭据、同一 quota 状态、同一 priority（同步双写）、
  同一冷却/负载/亲和归属（逻辑 id）。协议差异只在代理发送拼 N1 后缀的一瞬间换 id。
```

## 2. 核心数据结构（跨模块共享）

```
ChannelTemplate（复用现有类型，零新字段）
  claude 模板实例：type=14, base_url="https://open.bigmodel.cn/api/anthropic"
  ⚠️ base_url 语义与 Custom(8) 不同：Claude 渠道 new-api 自动拼 "{base_url}/v1/messages"，
     base_url 只填到 /api/anthropic 为止（不得带 /v1/messages 尾巴，config 校验拦截）。

SyncOutcome
  primary: HashMap<key名, i64>          （不变）
  claude:  HashMap<key名, i64>          （新增；claude 模板启用时填充）

ResolvedKey
  channel_id: i64                       （不变 = 逻辑 id）
  claude_channel_id: Option<i64>        （新增；None = 无 claude 渠道）

RouteView
  eligible / pinned / keys / has_data   （不变，全部以逻辑 id 为键）
  claude_of: HashMap<i64, i64>          （新增：逻辑 id → claude 渠道 id，仅发送点消费）
```

类型不变量：
- `claude_of` 的值集 ⊆ 该快照时刻受管 claude 渠道 id 集；键集 ⊆ eligible∪keys 的逻辑 id 集。
- 同一 key：`claude_channel_id ≠ channel_id`；claude 渠道名恒为 `{key.name}-claude`。

## 3. 模块划分与功能规约

### config.rs
- 功能：加载 `[new_api.claude_channel_template]`（可选段，完整复用 ChannelTemplate）。
  **自动补默认**：主模板命中智谱 coding 识别条件（同 `infer_zhipu_model_discovery`）且
  用户未显式写 claude 段时，自动补 `{type=14, base_url=/api/anthropic, models/group/
  model_discovery 沿主模板}`（跟随 model_discovery 自动补默认的既有先例；存量部署零迁移
  自动获益，非智谱上游不猜）。
- 校验：type=14 时 base_url 不得以 `/v1/messages` 结尾（启动即失败，防照抄 Custom 习惯）。

### newapi.rs（sync / 渠道生命周期）
- `plan_channel_ops` 扩展：claude 模板启用时，每把**活跃** key 在主渠道操作之外追加
  `Create/Skip/Delete`（名字 `{name}-claude`，同一 discovery 缓存复用，模型目录同源）。
- 弃用 key：两个渠道都删；恢复 / AddKey：两个渠道都建。**部分失败语义**：主渠道成功、
  claude 渠道失败 → key 仍算可用（I6 回落），warn + 下轮 sync 重试。
- **I7 清理**：claude 模板**未启用**但存在 `{活跃或弃用 key 名}-claude` 受管渠道 → 列为
  Delete（模板被移除/回滚后不留野生渠道；只碰名字与受管 key 精确匹配的渠道，红线不变）。

### orchestrator.rs
- `ResolvedKey` 增 `claude_channel_id`；**决策逻辑零变更**（eligible/pin/threshold/临期
  全按逻辑 id，语义 = key）。
- priority 下发：对每 key 的 `[channel_id, claude_channel_id?]` 同值双写；幂等 map 仍按
  channel_id 分键。
- `ResyncModels`：对两渠道都生效。

### router.rs
- **零语义变更**。池/冷却/负载/评分继续以逻辑 id 工作；`RouteView::from_snap` 额外带出
  `claude_of`。压测内核（score_all/choose）不动。

### proxy.rs
- 发送点（拼 `Bearer sk-{relay}-{id}` 处）单点注入：
  `send_id = (path=="/v1/messages").then(|| claude_of.get(&id)) .unwrap_or(id)`。
  负载/冷却/池归属/tried/日志/routed 统计**仍用逻辑 id**。
- 降级透传、401 自愈、429 等待/换道状态机全部不变（它们作用于逻辑 id，协议无关）。

### status.rs / main.rs
- 面板 per-key 行：实时指标（live_metrics_from_logs 按渠道分桶）**聚合两渠道求和**；
  `routed_per_channel` 继续逻辑 id（= key 粒度，展示语义不变）。
- `claude_endpoint` 展示与 `up`/`sync` 打印：name → (channel_id, claude_channel_id?)。

## 4. 不变量（修订后全集）

- **I1（修订）**：一把上游 key 恰好一个 OpenAI 协议受管渠道；claude 模板启用时至多再一个
  Claude 协议受管渠道（`{name}-claude`）。**调度状态每 key 恰好一份**，以逻辑 id 为键。
- **I2（保持）**：OpenAI 与 Claude 下游使用同一访问 token/group 与同一对 qt-proxy 中继
  令牌（二者仅 new-api 日志归因不同，不是凭据/出口分叉）。
- **I3（保持，重述）**：两种下游共享同一活动 key 与 priority（两渠道 priority 恒同值）。
- **I4（保持）**：quota、pin、阈值、冷却、负载、亲和池只维护一份，不因下游格式分叉。
- **I5（保持）**：不写 NewAPI option；不新建面向用户的 token。
- **I6（新）**：`/v1/messages` 仅当所选 key 有 claude 渠道映射时走原生透传；无映射回落
  现行转换路径（同逻辑渠道，行为同今日）。渠道运行中被禁用属既有失败语义（403 → 换道
  重试），不新增处理。
- **I7（新）**：claude 模板停用后，sync 删除全部 `{key}-claude` 受管渠道。

## 5. 关键设计决策

| # | 决策 | 理由 |
|---|------|------|
| D1 | 路由语义单位 = key（逻辑 id = 现有 OpenAI 渠道 id），claude id 只在发送点出现 | 保 I3/I4：调度状态零分叉；注入点唯一（proxy 发送处），review/回归面最小 |
| D2 | 冷却/负载按逻辑 id 聚合（429 打任一协议渠道 → 整把 key 冷却） | 智谱限速/额度在 key（账号）级，两渠道共享同一池（假设 H1） |
| D3 | claude 模板对智谱 coding 主模板**自动补默认**（无 opt-out） | 跟随 model_discovery 先例；多机部署零迁移自动生效；本仓两实例皆智谱 |
| D4 | 模型目录与主渠道同源（同一 discovery URL 结果） | 模型名跨协议一致（glm-5.3/glm-5.3-flash 双协议实测在用）；不引入第二目录真相 |
| D5 | 渠道名后缀 `-claude` | 与旧草案 `-cc` 区分（避免与历史遗留混淆）；语义直白 |
| D6 | claude 渠道建失败不阻塞主渠道（部分成功语义） | 可用性优先于保真；I6 回落兜底 |

## 6. 架构正确性论证

goal → 模块映射：
- G1「Claude 流量原生保真（工具/缓存/thinking/流式）」→ newapi.rs（建 type 14 渠道）+
  proxy.rs（发送点换 id）+ config.rs（模板与校验）
- G2「opencode 路径零回归」→ 主渠道与 `/v1/chat/completions` 路径逐字节不变（send_id
  替换只在 `/v1/messages` 分支）
- G3「调度状态不分叉」→ router/orchestrator 无语义变更，claude id 不进任何调度结构
- G4「可用性不回退」→ I6 回落 + D6 部分成功语义 + 既有 429/403 状态机原样复用

模块协作论证：
- G1：config 提供模板 → sync 建出 type 14 渠道（`ConvertClaudeRequest` 源码级透传，
  cache_control 保留）→ proxy 在 `/v1/messages` 分支把 N1 后缀换成 claude 渠道 id →
  new-api 以 RelayFormatClaude 直连智谱原生口。每一环都有源码或实测证据（research §6）。
- G3：claude_of 只被 proxy 发送点读取；router 的 choose/score/pool 输入无 claude id，
  故调度行为与今日逐决策一致（既有单测/压测全数适用的充分条件）。
- G4：claude 映射缺失（模板关/建失败/对账删）⇒ send_id = 逻辑 id ⇒ 请求走今日路径。

关键假设：
- H1：智谱 `/api/anthropic` 与 coding 口共享同一把 key 的额度与限速（双协议同一套餐）。
  依据：官方 quick-start 双协议同套餐表 + 同 key 计费。**D2 的成立前提**；e2e 验证点
  （claude 渠道消耗要反映到该 key 的 pct 探针——探针查的是 key 级 usage，天然满足）。
- H2：new-api Claude 渠道请求/响应双向透传无损（rc.20 源码已核；升级 new-api 是回归点）。
- H3：N1 后缀机制与渠道类型无关（middleware/auth.go 拆 `-`，auth 层机制）；e2e 确认。
- H4：Claude Code 请求的模型名（用户 env：glm-5.3 / glm-5.3-flash）∈ 渠道 models 列表。

模块级 invariant + preservation：
- I1：sync 是唯一创建/删除受管渠道的地方（面板 AddKey 走同一入口）；每轮 sync 对账
  名单缺失即删（I7），漂移即重建。I3：orchestrator priority 双写在同一轮循环内完成，
  两 PUT 间崩溃 ⇒ 下轮幂等对齐（last_applied 未命中即重写）。
- I4：proxy/router 全部状态结构不含 claude id ⇒ 结构上不可分叉（类型系统保证）。

## 7. 验证策略

- 单测（新增）：config 自动补默认/校验拦截；plan 双渠道计划（建/跳/删/I7 清理/名字精确
  匹配）；`claude_of` 构造；发送点 send_id 解析（纯函数化）；部分失败语义。
- 既有测试：router 压测内核、代理状态机测试**零改动全过**（G3 的回归证明）。
- e2e 验收标准（真环境，全部满足才算完成）：
  1. sync 后出现 7 个 `-claude` 渠道，priority 与主渠道同值；
  2. `/v1/messages` 过代理返回原生 Anthropic 结构（`msg_` 前缀 id、智谱原生 usage 字段）；
  3. tools + `cache_control`：≥2048 token 可缓存前缀，第二次相同请求
     `cache_read_input_tokens > 0`（对照实验证明缓存真的接通）；
  4. 流式：完整 SSE 事件序列含 tool_use 增量块；
  5. opencode 回归：`/v1/chat/completions` 正常 + 面板 per-key 指标聚合正确；
  6. I6 回落：删掉一把 key 的 claude 渠道映射（或停模板）后 `/v1/messages` 仍 200（走转换）。
- 用户侧切换（不由本工具代做）：`ANTHROPIC_BASE_URL=http://127.0.0.1:3000`，其余 env
  （AUTH_TOKEN/model 映射）不变。

## 8. 回滚

删 `[new_api.claude_channel_template]` 段（或主模板不再是智谱 coding）+ 重启：
I7 清理全部 `-claude` 渠道，行为与配置逐字节回到今日。零数据迁移，无残留状态。

## 9. 文档同步清单（实施时执行）

- 主 architecture.md：I1 修订 + 结构图更新（标注由 ccr-1 引入）
- `docs/research/claude-code-routing-research.md`：append §6（调研证据，随本子计划提交）
- CLAUDE.md：「Claude Code 下游接入」条目改写（原生透传 + `-claude` 渠道 + I6 回落）
- config.example.toml：claude 模板段（含 base_url 语义警告注释）

## 10. 细化设计（函数级，2026-09-21 定稿，经用户计划批准）

> 覆盖 M1–M4。规约按 §3.2；非平凡函数附正确性论证（§4.3.2 精简版）。
> 调用关系图：`main::bootstrap → NewApiClient::sync_channels(双模板) → resolve_keys
> → Orchestrator::run(tick 双写 priority) ⇄ StatusSnapshot ⇄ proxy::route_llm
> (router::choose 逻辑id → send_id_for 协议换id → forward_once)`。

### M1 config.rs

**`is_zhipu_coding_template(t: &ChannelTemplate) -> bool`**（新，从
`infer_zhipu_model_discovery` 抽出的判定：host==open.bigmodel.cn 且 path==
/api/coding/paas/v4/chat/completions）。
- 后置：`infer_zhipu_model_discovery` 与 claude 自动补默认共用本判定，单一真相。

**`Config::load`**（改）：主模板模型发现推断之后追加——
`claude_channel_template.is_none() && is_zhipu_coding_template(主模板)` ⇒ 补默认
`{type:14, base_url:"https://open.bigmodel.cn/api/anthropic", models/group/model_discovery
沿主模板 clone}`。显式段存在则完全不碰（用户为准）。

**`Config::validate`**（改）：claude 段存在时——
① `type==14 && base_url.trim_end_matches('/').ends_with("/v1/messages")` ⇒ bail
（new-api 会自动拼 `/v1/messages`，带尾巴会 404）；② `validate_model_discovery`
对 claude 段同样执行。

**`set_key_channel_ids(path, name, primary: i64, claude: Option<i64>)`**（新，替代
`set_key_channel_id` 的落 id 职责）：单次原子写同时落 `channel_id` 与
`claude_channel_id`（None ⇒ remove 该字段，与「活跃 key 持有 id」规则一致）。
两次分开写会留半更新态（主 id 新、claude id 旧）。
- `deprecate_key`：置 deprecated 后同时 remove 两字段。
- `restore_key(name, primary, claude)`：同时落两 id（单次原子写）。

`KeyMapping` 增 `#[serde(default)] claude_channel_id: Option<i64>`（旧配置无字段 =
None，兼容）。

### M1 newapi.rs

**`plan_channel_ops(keys, existing, template, claude_template)`**（改签名）：
每把 key 产 1–2 个 op，`ChannelOp::{Skip,Create,Delete}` 增 `claude: bool` 标记
（Missing 保持主渠道语义）：
- 活跃 key：主渠道逻辑不变；claude_template=Some ⇒ 追加 `{name}-claude` 的
  Skip/Create（**id 优先**：`k.claude_channel_id.filter(live).or(按名)`，与主渠道同规则）。
- 弃用 key：主名与 `-claude` 名都列 Delete（残留兜底）。
- **I7**：claude_template=None ⇒ 对每把 key（活跃或弃用），existing 含 `{name}-claude`
  即列 Delete（模板移除后的清理；只精确匹配受管 key 名，用户自建渠道红线不变）。

**`sync_channels(keys, template, claude_template, standby_priority)`**（改）：
- claude op 执行语义（D6 部分失败）：Create/Skip 对账/Delete 任一失败 ⇒ `warn!` 后
  继续，**不 return Err**（主渠道失败仍整体 Err，保持既有闸门）。
- 模型发现共用同一 `DiscoveryCache`（同 key 同 URL 只打一次上游 `/models`）。
- `SyncOutcome` 增 `claude: HashMap<String, i64>`（从 latest 按名解析，活跃 key 填充）。
- 正确性论证：claude 渠道任何失败最多损失「该 key 的原生 Anthropic 出口」，
  I6 回落保证 `/v1/messages` 仍可用（走主渠道转换路径）；主渠道路径与今日逐字节一致。

### M2 main.rs / orchestrator.rs

**`bootstrap`**（main.rs）：`sync_channels` 传 `cfg.new_api.claude_channel_template`；
启动写回 config 用 `set_key_channel_ids`（primary 必有；claude 按 SyncOutcome，
解析不到则 None）。
**`resolve_keys`**：`claude_channel_id` = SyncOutcome.claude 优先，config 持久值兜底。
**`print_mapping`**：`name → #id (+claude #cid)`。

**`Orchestrator::tick` priority 下发**（orchestrator.rs:1038 循环）：对每 key 依次
`[Some(channel_id), k.claude_channel_id]` 同 target 双写；`applied` 幂等 map 按
channel_id 分键（两渠道独立幂等，无新增竞态面）。查询失败的 key（pct 未取到）
两渠道都 continue 不动——既有语义，claude 渠道天然跟随。
- 正确性论证：两 PUT 无跨渠道原子性需求——中间态（主已写、claude 未写）下
  new-api 直连流量的 priority 阶梯短暂不一致一轮（≤poll_interval），代理流量不受影响
  （N1 显式指定渠道，不看 priority）；下轮幂等收敛。可接受，无 invariant 破坏。

**`add_key`**：主渠道建成后，若有 claude 模板 ⇒ 建 `{name}-claude`（standby 档）。
失败仅 `warn!`，AddKeyOk 照常返回（主渠道成功即 key 可用）。
**`deprecate_key`**：①压 priority 与 ④删渠道对两渠道执行（claude 失败 warn，
启动对齐兜底重删）。**`restore_key`**：两渠道都重建，config 双 id 单次原子落盘。
**`resync_models`**：对 key 的两渠道都跑 `ensure_channel_models`。
**tick 快照**：`KeyStatus` 增 `claude_channel_id`（透传自 ResolvedKey）。

### M3 router.rs / proxy.rs / status.rs

**`RouteView.claude_of: HashMap<i64,i64>`** + `from_snap` 收集
（`KeyStatus.claude_channel_id` Some ⇒ insert(channel_id → claude_channel_id)）。
choose/score/pool/cooldown/retain_channel **零改动**（输入不含 claude id）。

**`proxy::send_id_for(path: &str, logical: i64, claude_of: &HashMap<i64,i64>) -> i64`**
（新纯函数）：`path == "/v1/messages"` 且 claude_of 含 logical ⇒ 映射值；否则 logical。
调用点：`route_llm` 拼 `Bearer sk-{relay_key}-{id}` 前（proxy.rs:404 一带）。
**负载/冷却/池/tried/日志/routed 统计全部仍用逻辑 id**——换 id 只影响这一次 HTTP
发往哪个渠道，不影响任何路由状态。

### M4 status.rs 面板 + 文档

- 面板 JS：per-key live = `lvOf(k.channel_id)` 与 `lvOf(k.claude_channel_id)` 求和
  （rpm/tpm/tokens），key 行显示 `#id · claude #cid`；channels 表自然显示 `-claude`
  渠道。**不新增任何逐渠道轮询**（红线）。
- 文档：主 architecture.md I1 修订+结构图；CLAUDE.md 条目改写；
  config.example.toml 增 claude 段（含 base_url 语义警告）。

### 测试清单（与实现同步落地）

| 模块 | 用例 |
|---|---|
| config | 智谱主模板自动补 claude 默认；显式段优先；type14+尾巴 URL 启动失败；非智谱主模板不补；旧配置无 claude_channel_id 可解析 |
| newapi.plan | 带 claude 模板每 key 2 op（建/跳）；弃用两名全 Delete；I7（模板移除清理）；用户自建 `-claude` 同名不可触（名字必须精确匹配受管 key 名）——通过「existing 里名字不匹配任何 key+`-claude`」不产 Delete 验证 |
| newapi.sync | SyncOutcome.claude 填充（存在/缺失） |
| router | from_snap 构造 claude_of（Some/None/混合） |
| proxy | send_id_for 三分支：claude 路径有映射/无映射回落/openai 路径永不换 |
| 回归 | 既有全部测试不改语义全过（决策层/压测内核零改动即 G3 证明） |
