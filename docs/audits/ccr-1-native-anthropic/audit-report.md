# 契约对照审核报告 — ccr-1-native-anthropic（workflow §6.5 路径 B）

> 审计者：独立 subagent（未参与实施）。方法：从契约文档独立抽取 expectations，与
> `git diff 3458656..HEAD -- src/ config.example.toml` 逐条对照；不读实施者结论。
> 契约源（按权威序）：`subplans/ccr-1-native-anthropic.md`（§2/§3/§4/§5/§10 权威）、
> `architecture.md`（ccr-1 修订版）、`expectations.md`（事前 10 节框架）、CLAUDE.md 红线。
> 附带验证：`cargo test` 全量跑过（107 passed / 0 failed）；router.rs 尾部结构核对。
> 说明：expectations.md §2 的「实现位置（audit 回填）」列未回填原文件（保持事前文档
> 原样），位置信息统一落在本报告表格中。

## §1 Schema 字段（机械化）

| expectation | 判定 | 证据/说明 |
|---|---|---|
| `[new_api.claude_channel_template]`: Option\<ChannelTemplate\>（复用 type/base_url/models/group/model_discovery） | ✅ | src/config.rs:215（`#[serde(default)]`） |
| `KeyMapping.claude_channel_id`: Option\<i64\>，serde default=None | ✅ | src/config.rs:375-379 |
| `SyncOutcome.claude`: HashMap\<String,i64\>（活跃 key 填充） | ✅ | src/newapi.rs:230、858-861（与 primary 同一循环按名镜像填充） |
| `ResolvedKey.claude_channel_id`: Option\<i64\> | ✅ | src/config.rs:411 |
| `RouteView.claude_of`: HashMap\<i64,i64\>（逻辑id→claude渠道id，仅发送点消费） | ✅ | src/router.rs:36（doc 注明「只被代理发送点消费」）、73-77 |
| `KeyStatus.claude_channel_id`: Option\<i64\>，serde 透出 | ✅ | src/status.rs:39（`#[serde(default)]`） |
| claude 渠道名恒为 `{key.name}-claude`（字面后缀） | ✅（含⚠️见§3） | 命名单一 helper `claude_channel_name` src/newapi.rs:126；但后缀字面量另有 4 处（见 §3-E2） |

## §2 枚举值（机械化）

| expectation | 判定 | 证据/说明 |
|---|---|---|
| claude 渠道 type = **14** | ✅ | src/config.rs:511-519（自动补默认 `channel_type: 14`）；测试 `claude_tpl()` src/newapi.rs:1166；config.example.toml `type = 14` |
| claude 默认 base_url = `https://open.bigmodel.cn/api/anthropic`（不带 `/v1/messages` 尾巴） | ✅ | src/config.rs:516；config.example.toml:139（注释含「不要带 /v1/messages 尾巴」警告） |
| 后缀 `-claude` 集中为单一 helper | ⚠️ | helper 仅存于 newapi.rs（private fn）；main.rs:192、279 与 orchestrator.rs:604、777 共 4 处字面 `format!(\"...-claude\")` 绕过 helper（见汇总 E2） |
| 无新增共享常量需求 | ✅ | diff 中无新 const；type 14 以字面量出现在模板构造处（与契约「字面量允许」一致） |

## §3 流程步骤（机械化）

| expectation | 判定 | 证据/说明 |
|---|---|---|
| P1 主渠道 op 逻辑不变（Skip/Create/Delete/Missing） | ✅ | src/newapi.rs:142-186 主渠道分支仅追加 `claude: false` 字段，无逻辑改动；既有 5 个 plan 测试仅机械改签名全过 |
| P2 claude op：模板启用 ⇒ 活跃 key `{name}-claude` Skip/Create（**id 优先**：`k.claude_channel_id.filter(live).or(按名)`） | ✅ | src/newapi.rs:189-209（`.filter(\|id\| live_ids.contains(id)).or_else(\|\| existing.get(&cname).copied())`，与主渠道同规则）；测试 `claude显式id存活_按id匹配_陈旧回落按名` src/newapi.rs:1325-1345 |
| P2' 弃用 key：主名与 `-claude` 名都列 Delete（模板启用与否相同） | ✅ | src/newapi.rs:143-160（弃用分支先于 claude_template match，两名无条件 Delete）；测试 src/newapi.rs:1366-1379 |
| P3 I7：模板未启用 ⇒ 存量 `{受管key名}-claude` 列 Delete（活跃与弃用都清） | ✅ | 活跃：src/newapi.rs:210-218（`match claude_template` None 臂）；弃用：143-160；测试 `claude模板停用_清理受管claude渠道_自建不碰` src/newapi.rs:1350-1379 |
| P4 SyncOutcome.primary/claude 按 latest 名解析回填 | ✅ | src/newapi.rs:855-861；注释明确「删除失败留存的渠道继续映射是正确行为——它仍是这把 key 的 claude 出口」 |
| P5 启动写回 config：双 id 单次原子写 | ✅ | src/main.rs:213-221（`set_key_channel_ids`，条件 `k.channel_id != Some(id) \|\| k.claude_channel_id != claude_id`） |
| P6 router.choose 以逻辑 id 选路，语义与今日一致 | ✅ | router.rs choose/score/pool/cooldown 零改动（diff 仅加 claude_of 字段与 from_snap 收集）；压测内核测试全过 |
| P7 `send_id_for`：`/v1/messages` 且有映射 ⇒ claude id；否则逻辑 id | ✅ | src/proxy.rs:279-284（纯函数，签名与契约逐字一致）；调用点 src/proxy.rs:419 |
| P8 N1 后缀用 send_id；note_attempt/cool/record/tried 全用逻辑 id | ✅ | src/proxy.rs:419-424（`Bearer sk-{relay_key}-{send_id}`；`note_attempt(id)` 不变）、486（`record(key, id)` 不变）；tried/冷却结构 diff 未触及 |
| P9 决策（eligible/pin/临期/降级）按逻辑 id，零语义变更 | ✅ | orchestrator.rs 决策段 diff 未触及；仅 priority 下发循环与数据面字段变化 |
| P10 priority 双写：`[channel_id, claude_channel_id?]` 同 target | ✅ | src/orchestrator.rs:1142（`for cid in [Some(id), k.claude_channel_id].into_iter().flatten()`，同一 `target`） |

## §4 行为契约（语义，人审）

| expectation | 判定 | 证据/说明 |
|---|---|---|
| B1 `/v1/messages` 原生 Anthropic 透传（cache_control 保真、原生 usage） | ✅(结构)/🧪e2e | 结构前提全部就位：type14 渠道建立（newapi.rs）+ 发送点换 id（proxy.rs:419）+ base_url 语义校验（config.rs:786-793）。行为本身须真环境验收（子计划 §7 e2e 2/3/4），本 diff 审计无法证实——**待验收项，非偏离** |
| B2 I6 回落：无映射的 key `/v1/messages` 行为与改造前完全一致 | ✅ | send_id_for 无映射 ⇒ 逻辑 id（proxy.rs:282）；`/v1/messages` 路径其余代码零改动 ⇒ 走主渠道转换，行为同旧 |
| B3 opencode 路径（`/v1/chat/completions`）逐字节一致 | ✅ | send_id_for 非 messages 路径恒返 logical（proxy.rs:283）⇒ `Bearer` 串逐字节同旧；唯一变化是 debug 日志加 `send` 字段（非线上行为）；测试三分支含「openai 路径永不换」（proxy.rs:1061） |
| B4 claude 渠道失败不阻塞主渠道（D6） | ✅ | sync：Skip 对账 Err（newapi.rs:783-784）、Create Err（newapi.rs:831-832）`Err(e) if op.is_claude() => warn` 不 return Err；Delete 本就 warn；add_key：`create_claude_channel` 全程 best-effort 返 None（orchestrator.rs:598-634），AddKeyOk 照常；restore 的 claude **建**失败同样软（orchestrator.rs:810） |
| B5 自动补默认：智谱主模板+未显式写 ⇒ 补；显式段一字不改；非智谱不补 | ✅ | src/config.rs:735-743（`is_none()` 才补，置于主模板 discovery 推断之后）；测试 ×3：自动补（config.rs:1222-1240）、显式保留+非智谱不补（config.rs:1243-1269）、示例配置走 `Config::load` 真路径（config.rs:1205-1220） |
| B6 校验：claude 段 type=14 且 base_url 带 `/v1/messages` 尾巴 ⇒ 启动失败 | ✅ | src/config.rs:786-793（`trim_end_matches('/').ends_with(\"/v1/messages\")`，与 §10 M1 逐字一致）；claude 段也跑 `validate_model_discovery`（config.rs:794-798）；测试 config.rs:1272-1290 |

restore 的 claude **残留删除**用硬闸门（Err 中止，orchestrator.rs:775-784）而非 D6 软失败：判 ✅——该分支在主渠道重建**之前**执行，中止是干净可重试的；软跳过会导致「错配凭据的 `{name}-claude` 被按名接管烧错 key 额度」或同名双渠道，与主渠道「不冒险复用」既有硬闸门一致；D6 文义只覆盖「建失败」。

## §5 时序/状态契约（人审）

| expectation | 判定 | 证据/说明 |
|---|---|---|
| S1 双写无跨渠道原子性要求：中间态 ≤ 一轮，幂等收敛；代理流量不受影响 | ✅ | orchestrator.rs:1142-1160：applied 按 cid 分键，Err 不 insert ⇒ 下轮重试；N1 显式指定渠道不看 priority（既有机制，diff 未触及） |
| S2 查询失败的 key：两渠道 priority 都不动 | ✅ | pct 取数失败 `continue` 在双写循环之前（既有门，diff 上下文 orchestrator.rs:1136-1139）；双写循环嵌在同一迭代内 ⇒ 两渠道同跳过 |
| S3 claude_of 随快照刷新，无独立生命周期 | ✅ | tick 写 KeyStatus（orchestrator.rs:1220）→ from_snap 读（router.rs:73-77）；无其它写入点 |
| S4 config 双 id 落盘单次原子写 | ✅ | `set_key_channel_ids`（config.rs:673-687）/`restore_key`（config.rs:596-610）均单次 `write_atomic`，经 `apply_claude_id` helper（Some 写/None 删） |

## §6 不变量契约（property-based，构造性用例）

| expectation | 判定 | 证据/说明 |
|---|---|---|
| I1/I4 调度结构不含 claude 渠道 id（router pool/loads/cooldown/routed/tried；orchestrator eligible/pct/pin/active） | ✅ | router.rs 全部调度结构 diff 未触及（仅 RouteView 加数据面字段 claude_of）；orchestrator 决策段零改动；tried/冷却/负载/routed 均逻辑 id（§3 P8）。**例外见 E1（applied）** |
| I1' `applied` 不含 claude id（expectations.md §7 列举） | ⚠️ | **代码含**：双写循环对 claude cid 独立 insert（orchestrator.rs:1148、1154），弃用时 `applied.remove(&cid)`（orchestrator.rs:703）。**与子计划 §3/§10-M2 一致**（「幂等 map 按 channel_id 分键（两渠道独立幂等）」——权威契约明确如此设计）；与 expectations.md §7 的列举**矛盾**（该列举比子计划更严）。applied 是 priority 写合并 memo，从不被 choose/score/eligible 消费 ⇒ I4 的语义意图（调度不分叉）结构上仍成立。判定为**契约文档内部不一致**，代码从权威方（子计划 §10） |
| I3 任意轮末两渠道 priority 相等（收敛意义） | ✅ | 同 target 双写 + per-cid 幂等 + 失败下轮重试（orchestrator.rs:1142-1160） |
| I4' `claude_of[k] ≠ k`；键集 ⊆ keys 的 channel_id 集 | ✅ | from_snap 的 claude_of 与 keys 同源迭代 snap.keys（router.rs:66-77）⇒ 键集包含关系结构成立；claude 与主渠道是不同渠道实体，id 不可能相等 |
| I7 模板未启用 ⇒ sync 后不存在 `{受管key名}-claude` | ✅ | plan 生成（newapi.rs:210-218 + 弃用分支）+ 执行（Delete 失败 warn 下轮重试）+ 启动写回清 config 字段（main.rs:213-221，解析不到 ⇒ None ⇒ 删字段）；测试覆盖活跃/弃用/自建三态 |

## §7 性能契约（机械化）

| expectation | 判定 | 证据/说明 |
|---|---|---|
| sync 管理 API 调用量 ≤ 2×（每 key 至多 +1 claude 渠道 GET/PUT） | ✅ | plan 每 key 至多多 1 个 claude op（newapi.rs:189-209）；无逐渠道额外轮询 |
| DiscoveryCache：同 key 同 URL `/models` 只打一次（D4 同源） | ✅ | 单个 `DiscoveryCache` 在 ops 循环外创建、全程复用（newapi.rs:764-765 上下文）；claude 模板 discovery 从主模板 clone（config.rs:518-520）⇒ 同 URL 命中缓存；模型名同源不引入第二目录真相 |
| 面板不新增逐渠道轮询（红线） | ✅ | `lvSum` 纯粹从既有 `d.live` 数组求和（status.rs:1202-1209），tick() 拉取行为零改动——仍是 recent_logs 单请求推导 |

## §8 安全/副作用契约

| expectation | 判定 | 证据/说明 |
|---|---|---|
| 日志不打印任何鉴权值 | ✅ | 新增日志全部只含 name/channel_id/error=%e（如 orchestrator.rs:620-633、newapi.rs:832）；`Bearer sk-{relay_key}-{send_id}` 串不入日志（proxy.rs:420 仅构造） |
| I7 删除只精确匹配 `{受管key名}-claude`，绝不碰用户自建渠道 | ✅ | `existing.get(&claude_channel_name(&k.name))` 精确键查找（newapi.rs:211-212）；测试以 `someone-else-claude` 在 existing 中不产任何 op 验证（newapi.rs:1358） |
| 不写 NewAPI option、不建面向用户的新 token | ✅ | diff 无 option 写入；qt-proxy-claude 中继令牌为 F4 既有机制（newapi.rs:1134，diff 外既有代码） |
| claude 渠道 key = 同一把智谱 key（无新增暴露面） | ✅ | claude op/Create 均传 `&k.zhipu_api_key`（newapi.rs:196-207）、`api_key`（orchestrator.rs:608-611） |

## §9 跨实现一致性

| expectation | 判定 | 证据/说明 |
|---|---|---|
| base_url 语义按 new-api Claude adaptor（自动拼 `/v1/messages`） | ✅ | 校验拦截（config.rs:786-793）+ 默认值无尾巴（config.rs:516）+ 文档三处同步（config.example.toml、CLAUDE.md、architecture.md） |
| 双机部署行为一致（同 config ⇒ 同步建 claude 渠道） | ✅ | 自动补默认纯函数确定性（config.rs:511-521），无环境分支 |
| 升级 new-api 回归点（H2/H3）已写入文档 | ✅ | CLAUDE.md 新条目含「升级 NewAPI 版本的回归点：Claude 透传 + N1 后缀对 type 14 的生效（H2/H3）」 |

## §10 测试清单对照（子计划 §10）+ 回归

| expectation | 判定 | 证据/说明 |
|---|---|---|
| config 5 用例（自动补/显式优先/尾巴拦截/非智谱不补/旧配置可解析） | ✅ | config.rs:1205-1290（示例走 load 真路径、自动补、显式+非智谱、尾巴失败）；旧配置兼容由 `#[serde(default)]` + `弃用key` 测试的 `claude_channel_id == None` 断言与 SAMPLE 解析覆盖 |
| newapi.plan：双 op 建/跳、弃用两名全删、I7、自建不可触 | ✅ | newapi.rs:1300-1379 四个新测试 |
| newapi.sync：SyncOutcome.claude 填充（存在/缺失） | ⚠️ | **未落地为单测**（sync_channels 需真 NewApiClient，项目测试以 e2e 为主）；填充逻辑为 primary 的 4 行镜像（newapi.rs:858-861）。见汇总 E3 |
| router：from_snap 构造 claude_of（Some/None/混合） | ✅(含⚠️) | router.rs:1216-1239 测试存在且过；但**被放在 `mod tests` 闭括号之后**（文件顶层作用域）——cargo test 输出证实其跑在 `router::` 而非 `router::tests::`。见汇总 E4 |
| proxy：send_id_for 三分支 | ✅ | proxy.rs:1050-1062 |
| 回归：既有测试不改语义全过（G3 证明） | ✅ | 实测 `cargo test`：**107 passed / 0 failed**；对既有测试的改动均为签名机械适配（plan_channel_ops 加参、RouteView 加字段、set_key_channel_ids 改名），压测内核/代理状态机逻辑零改动 |

## 附：文档同步（子计划 §9，diff 范围外但属实施清单）

| 项 | 判定 | 证据 |
|---|---|---|
| architecture.md I1 修订+结构图 | ✅ | git diff 3458656..HEAD 含 architecture.md（+55/-31），已是 ccr-1 修订版 |
| CLAUDE.md「Claude Code 下游接入」改写 | ✅ | 同 range CLAUDE.md（+27），旧「只建一个渠道/不双写 priority」条目已改为双渠道+I6+I7+回滚说明 |
| config.example.toml claude 段 | ✅ | 含 base_url 语义⚠️注释与回滚说明（config.example.toml:133-148） |
| research §6 append | ✅ | commit 4221a86（docs/research/claude-code-routing-research.md +30） |

---

## 汇总

**❌（违反契约）：0 条。**

**⚠️（偏离但可论证）：5 条**

- **E1** `orchestrator.applied` 含 claude 渠道 id（orchestrator.rs:1148/1154/703），与 expectations.md §7 I1 的结构列举矛盾；但子计划 §3/§10-M2（权威、且为后定的细化）明确规定「applied 按 channel_id 分键、两渠道独立幂等」，且 applied 从不被路由决策消费，I4 语义意图不受影响。**这是两份契约文档之间的内部不一致，应修订 expectations.md §7 的列举（或在未来把 applied 归类为数据面）**，代码本身无需改。
- **E2** `-claude` 后缀字面量在 helper（newapi.rs:126）之外另有 4 处裸写（main.rs:192、279；orchestrator.rs:604、777），不符合 expectations.md §3「多处出现时应集中为单一 helper」；后缀属契约固定字面量，行为风险低，但改名/审查时存在 5 处单点。建议把 `claude_channel_name` 提为 crate 级 pub fn 收口。
- **E3** 子计划 §10 测试清单的「newapi.sync：SyncOutcome.claude 填充（存在/缺失）」未落地（无对应单测）；受限于 sync 需真 new-api（项目 e2e 为主），可接受但应记入 e2e 验收清单而非静默缺失。
- **E4** router.rs 新测试 `from_snap_收集claude映射`（router.rs:1216 起）被追加在 `mod tests` 闭括号**之后**，位于文件顶层作用域（测试输出路径 `router::from_snap_收集claude映射` 证实）；编译、发现、通过均正常，纯组织性缺陷，应移入 mod tests。
- **E5** 面板徽标文案「claude 渠道被禁用（/v1/messages 走转换）」（status.rs:1277）语义不准：渠道被 new-api 禁用时 claude_of 映射仍在，`/v1/messages` 仍会打到该禁用渠道并走既有失败/换道语义——I6 回落的触发条件是**无映射**而非「渠道禁用」（子计划 I6 注本身即如此表述）。仅 UI 文案，无行为影响，建议改为「claude 渠道被禁用（claude 请求将失败换道）」之类。

**总体结论：实施与契约高度一致，0 违约。** 数据结构、双渠道生命周期（建/跳/删/I7/弃用/恢复/AddKey/重对账）、priority 同值双写与幂等、发送点单点换 id（G2 逐字节保证）、I6 回落、config 自动补默认与尾巴拦截、安全红线（日志/自建渠道/无逐渠道轮询）全部按子计划 §2-§5/§10 落地且有测试佐证；107 项测试全过。5 条 ⚠️ 均为文档不一致（E1）、收口/组织性（E2/E4）、文案（E5）与测试清单缺口（E3），无一影响调度语义或线上行为。**注意：子计划 §7 的真环境 e2e 验收（7 个 -claude 渠道同 priority、msg_ 前缀、cache_read_input_tokens>0 对照、SSE tool_use 增量、opencode 回归、I6 实测回落）不在本 diff 审计可达范围内，仍是完成判定的未决项。**
