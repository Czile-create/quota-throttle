# Contract Audit Expectations — ccr-1-native-anthropic

> Step 0 产物（workflow §6.2）：从**契约文档**抽取，不回填代码。
> 契约源以子计划文档为权威；本文件在实施前定稿，实施后供 subagent 独立审对照。

## 1. 契约源（multi-doc）

| 文档 | 采用的契约 |
|---|---|
| `docs/design/claude-code-routing/subplans/ccr-1-native-anthropic.md` | §2 数据结构、§3 模块规约、§4 不变量 I1–I7、§5 决策 D1–D6、§10 细化 |
| `docs/design/claude-code-routing/architecture.md` | 未被 ccr-1 修订的部分：I2/I4/I5 原文、兼容性（客户端只设 BASE_URL+AUTH_TOKEN） |
| `docs/design/cache-pool/architecture.md` | F4 红线：bind fail-fast、无总超时、不开压缩、日志不打鉴权头、管理面打 upstream_base |
| CLAUDE.md | N1 机制语义；「每把 key 只建一个渠道」条目**由 ccr-1 修订为双渠道**（I1 修订版）；面板不得引入逐渠道轮询 |

## 2. Schema 字段（机械化）

| 字段 | 类型 | 来源 | 实现位置（audit 回填） |
|---|---|---|---|
| `[new_api.claude_channel_template]` | Option\<ChannelTemplate\>（复用：type/base_url/models/group/model_discovery） | 子计划 §3 config | |
| `KeyMapping.claude_channel_id` | Option\<i64\>，serde default=None | 子计划 §10 M1 | |
| `SyncOutcome.claude` | HashMap\<String,i64\> | 子计划 §2 | |
| `ResolvedKey.claude_channel_id` | Option\<i64\> | 子计划 §2 | |
| `RouteView.claude_of` | HashMap\<i64,i64\>（逻辑id→claude渠道id） | 子计划 §2 | |
| `KeyStatus.claude_channel_id` | Option\<i64\>，serde 透出 | 子计划 §10 M3 | |
| claude 渠道名 | `{key.name}-claude`（字面后缀） | 子计划 §2 类型不变量 | |

## 3. 枚举值（机械化）

- claude 渠道 type = **14**（new-api `constant/channel.go` ChannelTypeAnthropic；rc.20 源码核实）。
- claude 默认 base_url = `https://open.bigmodel.cn/api/anthropic`（**不带** `/v1/messages` 尾巴）。
- 无新增共享常量需求（后缀 `-claude` 以字面量出现在命名函数处，允许；多处出现时应集中为单一 helper）。

## 4. 流程步骤（机械化）

sync 对齐（每 key）：
- P1 主渠道 op（Skip/Create/Delete——既有逻辑不变）
- P2 claude op：模板启用 ⇒ 活跃 key 跳过/创建 `{name}-claude`；弃用 key 两名全删
- P3 I7：模板未启用 ⇒ 存量 `{受管key名}-claude` 列 Delete
- P4 SyncOutcome.primary/claude 按 latest 名解析回填
- P5 启动写回 config：双 id 单次原子写

代理发送（每 LLM 请求）：
- P6 router.choose 以逻辑 id 选路（含命中/评分/冷却，语义与今日一致）
- P7 `send_id_for`：`/v1/messages` 且 claude_of 有映射 ⇒ claude id；否则逻辑 id
- P8 N1 后缀用 send_id；note_attempt/cool/record/tried 全用逻辑 id

控制循环（每轮 tick）：
- P9 决策（eligible/pin/临期/降级）按逻辑 id，零语义变更
- P10 priority 双写：`[channel_id, claude_channel_id?]` 同 target

## 5. 行为契约（语义，人审）

- B1 `/v1/messages` 走 claude 渠道时为**原生 Anthropic 透传**：cache_control 保留到上游，
  usage 出现智谱原生缓存字段（e2e：相同大前缀请求第二次 `cache_read_input_tokens > 0`）。
- B2 I6 回落：无 claude 映射的 key，`/v1/messages` 行为与改造前**完全一致**（走主渠道转换）。
- B3 opencode 路径（`/v1/chat/completions`）行为与改造前**逐字节一致**。
- B4 claude 渠道失败不阻塞主渠道（sync 部分失败 warn 不 Err；add_key 同理）。
- B5 自动补默认：主模板=智谱 coding 且未显式写 claude 段 ⇒ 补默认；显式段一字不改；
  非智谱主模板不补。
- B6 校验：claude 段 type=14 且 base_url 带 `/v1/messages` 尾巴 ⇒ 启动失败。

## 6. 时序/状态契约（人审）

- S1 两渠道 priority 双写无跨渠道原子性要求：中间态至多持续一轮（≤poll_interval），
  下轮幂等收敛；代理流量不受中间态影响（N1 显式指定渠道）。
- S2 查询失败的 key：两渠道 priority 都不动（既有语义扩展到 claude 渠道）。
- S3 claude_of 随快照刷新（tick 写 KeyStatus → from_snap 读），无独立生命周期。
- S4 config 双 id 落盘必须单次原子写（tmp→fsync→rename 既有机制内完成）。

## 7. 不变量契约（property-based）

- I1：活跃 key 恒有且仅有一个主渠道；claude 模板启用时至多一个 `{name}-claude`；
  **路由决策结构（eligible/keys/pool/cooldown/loads/routed/tried/pin）不含 claude 渠道 id 值**
  ——claude id 只存在于 claude_of/SyncOutcome.claude/claude_channel_id 及 `applied`
  （priority 下发幂等记账，per-cid 分键是子计划 §10-M2 的设计内行为，非路由决策状态）。
- I3：任意时刻已下发的两渠道 priority 相等（收敛意义：每轮末相等）。
- I4：`claude_of[k] ≠ k`；claude_of 键集 ⊆ keys 的 channel_id 集。
- I7：claude 模板未启用 ⇒ sync 后不存在 `{受管key名}-claude` 渠道。
- 属性测试落地形式：Rust 单测断言（项目未引入 proptest，以构造性用例覆盖；
  记录为已知缺口，见 §8 说明）。

## 8. 性能契约（机械化）

- sync 每轮管理 API 调用量增长 ≤ 2×（每 key 至多 +1 个 claude 渠道的 GET/PUT）；
  DiscoveryCache 保证同 key 同 URL 的 `/models` 只打一次。
- 不新增任何逐渠道轮询（面板 live 指标仍从 recent_logs 单请求推导）。
- 路径 A 自动化：**N/A**——项目尚未自备 `scripts/contract_audit/`（跟踪项，
  与本子计划无关，不在本次补）。

## 9. 安全/副作用契约

- claude 渠道 key = 同一把智谱 key ⇒ new-api 渠道表照存明文（与主渠道同面，无新增暴露）。
- 日志不打印任何鉴权值（既有红线，claude 路径同守）。
- I7 删除只精确匹配 `{受管key名}-claude`，绝不碰用户自建渠道（即使名字相似）。
- 不写 NewAPI option、不建面向用户的新 token（qt-proxy-claude 为 F4 既有内部中继令牌）。

## 10. 跨实现一致性

- 参考实现对照（研究记录，不要求 bit-exact）：new-api `relay/channel/claude/adaptor.go`
  的 `ConvertClaudeRequest` passthrough 与 `{base_url}/v1/messages` URL 规则是 claude 模板
  base_url 语义的依据；升级 new-api 版本须回归验证（H2/H3，N8 清单）。
- 双机部署（本机 + fermat）行为一致：同一 config 结构 + 同码 ⇒ 同步建 claude 渠道（D3）。

---

## Step 5 验证记录（2026-09-21 回填）

- 路径 A（自动检查）：N/A——项目未自备 contract_audit 脚本（见 §8）。
- 路径 B（subagent 独立审）：`audit-report.md`，0 ❌ / 5 ⚠️；处置见 `decisions.md`
  （E2/E4/E5 已修码，E1 修正本文件 §7 措辞，E3 以 e2e 验证覆盖）。
- e2e（真环境，2026-09-21）：6 条验收全过——6 个 `-claude` 渠道同 priority 双写；
  config 落盘 claude_channel_id；/v1/messages 原生结构（msg_ id、智谱原生 usage）；
  4196-token 前缀二次请求 cache_read=4160（改造前恒 0）；SSE 完整事件序列含
  tool_use 增量；opencode /v1/chat/completions 无回归；面板 API 带 claude id。
  sync 级 SyncOutcome.claude 填充由此 e2e 覆盖（E3）。
