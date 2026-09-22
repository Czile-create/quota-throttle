# quota-throttle

> 轮询智谱 GLM Coding Plan 用量，通过 new-api 用 `priority` 钉住单把活动 key、逼近额度时自动切到下一把；并能替用户托管 new-api（下载二进制 / 启动 / 建渠道）。目标：护住 prompt 缓存局部性 + 预防式绕开撞墙。

## 项目结构

```
src/
  main.rs          — 子命令 up/down/sync/run 编排
  config.rs        — TOML 配置加载
  quota.rs         — 智谱用量探针（“眼睛”）
  newapi.rs        — new-api 管理 API 客户端（登录/建渠道/改 priority）
  boot.rs          — 下载 + 托管 new-api 原生进程
  orchestrator.rs  — 控制循环：按用量选活动 key、重排 priority
docs/
  workflow.md      — 开发工作流程规范（@docs/workflow.md）
  research/key-rotation-research.md   — 调研：周窗口临期优先（keyrot-1）
  design/key-rotation/                — 架构 + 细化设计（keyrot-1）
config.example.toml / config.toml(gitignored)
```

## 技术栈

- 语言：Rust 2021
- 构建：Cargo（本机 rustc 1.95 nightly）
- 依赖：tokio、reqwest(rustls,cookies)、serde/serde_json、toml、tracing、anyhow、sha2
- 测试：目前**以端到端实测为主**（对真实 new-api + 真 key 驱动），单元测试待补

## 常用命令

```bash
cargo build --release
cargo run --release -- up config.toml     # 起 new-api + 建渠道 + 切换循环
cargo run --release -- sync config.toml    # 只建/对齐渠道并打印 name→channel_id
cargo run --release -- down config.toml    # 停 new-api
# 托管的 new-api 数据在 ./.newapi/（SQLite=one-api.db、二进制、日志、PID）
```

## 代码风格

- 命名：snake_case
- 注释语言：中文
- 改 new-api 渠道字段用「GET → 只改目标字段 → PUT」整体搬运，对版本差异鲁棒

## 参考实现（交叉检验用，勿凭记忆猜其行为——查源码/实测）

- **new-api**（`QuantumNous/new-api`，旧名 `Calcium-Ion/new-api`）：管理 API、渠道类型码、setup 契约。源码用 `gh api repos/QuantumNous/new-api/contents/<path>?ref=<tag>` 拉。
- **opencode**（`sst/opencode`）：`zhipuai-coding-plan` provider 定义、config/auth 优先级。
- **上游 `/models`**：渠道模型目录的权威来源；models.dev 只用于核对 opencode 客户端是否展示该模型。

## 已知限制与注意事项（血泪，务必先读再动手）

- **用量读取（已解决，勿再走弯路）**：团体 coding plan 的用量**能用 API key 读**，三个条件缺一不可——
  ① url 带 **`?type=2`**（团队额度作用域；`type=3` 是团队小时用量）；
  ② **`Authorization: Bearer <key>`**（**必须带 Bearer**，裸 key 不行）；
  ③ 带 **`Bigmodel-Organization`** / **`Bigmodel-Project`** selector header。
  缺任一 → 返回「当前用户不存在coding plan」或 limits 空。org/project id 取法：浏览器开
  `https://bigmodel.cn/coding-plan/team/usage-stats` → F12 Network → 找 `quota/limit` 请求 → 抄这两个头。
  **selector 按 key 配**（不同 key 可能属不同组织/项目）。依据：CodexBar `docs/zai.md` + 实测。
  返回：`level`(如 max) + `limits[]`，`unit=3&number=5`→5小时窗口、`unit=6&number=1`→每周窗口、
  `TIME_LIMIT`(unit=5)=MCP 搜索次数（非用量窗口，须过滤）。
  **⚠️ 窗口 type 有两种计费模式（2026-08 双团队实测）**：`TOKENS_LIMIT`（token 型）与
  `CREDIT_LIMIT`（积分型），窗口语义相同（unit/number 定窗口、percentage=已用%），**都要算用量**；
  只认 TOKENS_LIMIT 会让积分型团队的 key 永远「limits 为空」不参与决策（踩过）。
  另：selector 决定查的是**哪个团队**的额度——账号属多个团队时，同一把 key 配不同 selector
  会查到不同团队的窗口（都能查通但语义变了），务必按 key 所属团队配。
- **⚠️ 教训（本项目最大的坑不是技术，是流程）**：我曾因用错鉴权（裸 key、缺 type/selector）就断言「用量读不到」，
  进而设计出「推理探测」的弯路，被用户三次打断。**根因是跳过调研直接下结论**。
  凡是「某接口不行」的结论，必须先查官方文档 + 社区实现 + 实测三者交叉验证，再下结论。
- **智谱 coding 口** = `https://open.bigmodel.cn/api/coding/paas/v4/chat/completions`（`/v4` 不是 `/v1`，`/coding/` 不是普通 `/paas/`）。opencode `zhipuai-coding-plan` 用 `@ai-sdk/openai-compatible` 打 `{api}/chat/completions`。
- **new-api 渠道必须 Custom 类型(8)**：base_url 原样透传全路径。OpenAI 类型(1) 会拼成 `.../v4/v1/chat/completions` → 智谱 404。（类型码：OpenAI=1, Custom=8, Zhipu=16, ZhipuV4=26）
- **new-api 建渠道 payload** 要 `{mode:"single", channel:{...}}` 包裹；`channel` 是指针，平铺会 nil-panic 500。字段：`type/key/base_url/models(逗号串)/group/priority/weight/status`。
- **new-api PUT /api/channel 拒绝带 `status` 字段**的请求体（判 Invalid parameters）；改字段前必须 `obj.remove("status")`。GET 单渠道返回的 `key` 是空串，PUT 空 key 会保留原值（安全）。
- **new-api 首启无默认 root/123456**：需先 `POST /api/setup {username,password,confirmPassword,SelfUseModeEnabled}`（密码≥8位、用户名≤12）建管理员，再登录拿会话。
- **new-api 令牌 key 在列表里打码**（`aK1A****7H3Z`），真实值从 SQLite `tokens.key` 读（或 `POST /api/token/:id/key` 直接回完整值）；POST/PUT 到 `/api/xxx/` 要带**尾斜杠**（否则 307，reqwest 会自动跟随、urllib 不会）；会话鉴权还要带 `New-Api-User: <用户id>` 头。
- **new-api 管理面没有「调用户余额」的 API**：PUT /api/user/ 的 EditWithTx 白名单只有
  username/display_name/group/remark/password（**quota 改不动、还回 success=true**）；
  ManageUser 只有 enable/disable/delete 等。最短路径：直写 SQLite
  `UPDATE users SET quota=… WHERE username=…`。quota 单位 = 货币数 × QuotaPerUnit(500000)。
  **本工具已自动化（F3，`boot::bump_user_quota`）**：托管模式每次启动把 root 额度调到
  `root_user_quota_units`（默认 2 亿），只调大不调小。⚠️ rc.20 源码（model/user.go:961-969）
  表明无 Redis 时额度**每请求直查 DB、直写立即生效无需重启**——旧「须重启」结论仅
  Redis 模式成立（待实测最终确认）。
- **⚠️ new-api `/api` 全局限流：360 次/180 秒（≈2 次/秒，env `GLOBAL_API_RATE_LIMIT`，不在 option 系统里）**。
  2026-08-24 踩坑：面板曾**逐渠道**轮询 `/api/log/stat` 拉 rpm/tpm（N 把 key），
  单面板就吃光预算 → 控制循环的 GET→PUT 被 429（且 429 响应体非 JSON，报「解析渠道响应失败」）
  → 渠道 priority 卡旧值（出现过双 active 平分流量的实际伤害）。**已改**：面板实时指标全部从
  `recent_logs` 单请求推导（`live_metrics_from_logs`）。教训：**任何面板改动都别引入逐渠道轮询**；
  管理 API 预算要留给控制循环。login 另有 CriticalRateLimit（20 次/20 分钟），脚本反复登录会把自己锁死。
- **会话失效自愈（2026-09 已修，`docs/fixes/newapi-401-relogin.md`）**：new-api **重启会作废
  本工具的管理会话**；NewApiClient 现在统一走 `send_authed`——Session 模式遇 401 自动重登一次
  并重试原调用，带 **10s 重登冷却**（防密码改坏后连环重登烧穿 login 的 CriticalRateLimit）。
  Token 模式（admin_token）的 401 是配置错误，不重登。
- **new-api release 有独立二进制**（linux/arm64/macos/win），自带 SQLite，`PORT` env 指定端口；默认只在 **401** 自动禁用渠道（429/耗尽不禁），耗尽报文是中文「已达到…使用上限」不撞其英文禁用关键词 → 恢复干净。
- **智谱 quota 返回只有整数 percentage**：`TOKENS_LIMIT` 窗口**没有** `usage`/`remaining` 字段（那俩只出现在
  `TIME_LIMIT`/MCP 搜索计数上，而它本就该被过滤掉）。⇒「还剩多少余量」的分辨率**就是 1%**，做不了更细的判断。
- **周窗口重置时刻 = `limits[].nextResetTime`（epoch 毫秒，绝对时刻）**，探针原样透传为
  `WindowStatus.next_reset_time`（`quota.rs`）。keyrot-1（周临期优先）用它做 EDF：`reset_ms - now_ms ∈ (0, lookahead]`
  且周窗口、5h 窗口都有余量 ⇒ 临期。⚠️ **临期只在当前档位合格集内排序，不扩合格集**：
  正常档仍严格 `< throttle`；只有全员 ≥ throttle 进入 Degraded 后，才允许 `< exhausted` 的 key 继续服务。
  另外**临期判定用周窗口自己的 pct**、
  可服务性用 max_pct（5h=100% 的 key 不算临期，选了立即 429）。**已知限制**：`watch_windows`
  若配成只盯单窗口（默认盯两个），max_pct 不再是「5h 与周取大」，临期可服务性判定会退化——
  该组合下整个调度语义本来就偏离设计假设，慎改。
- **🔥「limits 为空」必须当错误抛，绝不能返回空 status**（`quota.rs` 曾经只 warn，是个潜伏的灾难）：
  配错 selector 时智谱**不报错**——它 `success=true` 地回一个空 `data`。若探针把它当成「查到了，但没有窗口」，
  `max_watch_pct()` 会算出 **0.0**，于是这把 key 在调度器眼里就是**「用量 0%」**：它会被选成活动 key 并且
  **永远不会被切走**，直到线上真撞墙。抛错则安全——该 key「查询失败」⇒ 不参与决策、不动 priority、
  看板显示「查询失败」而不是骗你说 0%。**任何「查不到用量」的路径，默认值都必须是「未知」而不是「0」。**
- **new-api 用量表 `quota_data`（看板历史图的唯一数据源）**：
  · 小时桶（`created_at - created_at%3600`），由 `UpdateQuotaData()` goroutine **每 `DataExportInterval` 分钟批量刷库**，
    默认 **5 分钟**（`DataExportEnabled` 默认 true）。依据：`model/usedata.go` + `common/constants.go`。
    ⇒ **看板历史视图刷新 5 分钟一次即可**，刷得再勤也拿不到更新的数（源头就是 5 分钟才写一次）。
  · **没有滞后**：与 `logs`(type=2) 按小时分桶后逐桶逐渠道完全一致（实测）。若你看到「logs 比 quota_data 新」，
    多半是把**非消费日志**（type≠2）也算进来了——我踩过这个坑并据此错误推断出「聚合表滞后」。
  · 表里**有 `channel_id`**，但 new-api 的 `GET /api/data/` 是 `Group by (model_name, created_at)`，**把渠道维度压掉了**。
    ⇒ 想看「哪把 key 烧的」，唯一的路是直读 `.newapi/one-api.db`（当前未做，故未引入 sqlite 依赖）。
- **智谱「高峰时段」= 扣减系数，不是限额**（2026-07 官方文档 `docs.bigmodel.cn/cn/coding-plan/faq` + `overview` 交叉验证）：
  · 高峰期 = **每日 14:00–18:00（UTC+8）**，**固定**（有二手文章说「随流量浮动」，官方无此说法）。
  · GLM-5.2 / GLM-5-Turbo：高峰 **3 倍**、非高峰 **2 倍**；**限时福利**——非高峰仅 **1 倍**，**到 9 月底**（到期要改配置）。GLM-4.7 等 1 倍。
  · ⇒ 同一个请求在 14–18 点烧掉的额度是其他时间的 **3 倍**。
  · **没有任何接口能查当前是否高峰**（`quota/limit` 响应无此字段；官方文档也无该接口）——只能按时钟算。
    因窗口按 **UTC+8** 定义，代码里必须按 `tz_offset` 算而**不是本机时区**（本机恰好 UTC+8 会掩盖这个 bug）。
- **探测成本坑**：glm 是推理模型，`max_tokens:1` 挡不住思考（烧 ~660 token）；`thinking:{type:"disabled"}` 才压到 ~7 token。
- **模型目录**：`up` / `sync` / AddKey 才调用每把 key 的 `/models`，不进 quota/面板周期。
  成功结果权威；失败时存量渠道不动，新渠道才用模板 `models` fallback。鉴权值不得进日志。
  旧智谱 Coding 模板缺配置块时自动补官方 models URL；其它 Custom 上游不猜。
- **Claude Code 下游接入（ccr-1 双渠道，2026-09-21；旧转换方案已被实测推翻）**：
  智谱 Coding Plan **原生支持 Anthropic 口** `https://open.bigmodel.cn/api/anthropic`
  （官方双协议 + 实测 x-api-key 可用）；NewAPI 的 Claude 渠道(type 14) 是源码级透传
  （`ConvertClaudeRequest` 原样返回、URL=`{base_url}/v1/messages`）。旧方案（Anthropic→OpenAI
  转换进 type 8 渠道）**剥掉 cache_control**（实测两次相同请求 cache_read/creation 恒 0，
  缓存局部性目标落空）且边缘场景有工具/分类器错误——已废弃为 I6 回落路径。
  · 每把上游 key 双渠道：主渠道(type 8, coding 口, opencode) + `{name}-claude`(type 14,
    /api/anthropic, Claude Code 原生透传)。**调度状态（quota/pin/合格集/冷却/负载/亲和）仍
    每 key 一份**，以主渠道 id 为逻辑 id；claude 渠道 id 只在代理拼 N1 后缀的一瞬换入
    （`proxy::send_id_for` 单一注入点）。
  · claude 模板自动补默认：主模板是智谱 coding 且未显式配 `[new_api.claude_channel_template]`
    ⇒ 自动补 `{type=14, base_url=/api/anthropic, 其余沿主模板}`。⚠️ Claude 渠道 base_url
    只填到 `/api/anthropic` 为止（NewAPI 自动拼 `/v1/messages`，与 Custom(8) 的完整路径
    语义相反；带尾巴启动即拦）。
  · claude 渠道建失败不阻塞主渠道（D6，warn + 下轮重试）；无 claude 映射的 key 的
    `/v1/messages` 回落主渠道转换路径（I6）。回滚 = 删 claude 模板段重启，sync 自动清理
    全部 `-claude` 渠道（I7）。
  · `ANTHROPIC_AUTH_TOKEN` 仍用与 opencode 相同的 NewAPI key；不新建 Claude 专用
    token/group。`ANTHROPIC_BASE_URL=http://127.0.0.1:3000`。
  · 升级 NewAPI 版本的回归点：Claude 透传 + N1 后缀对 type 14 的生效（H2/H3）。
  · 未来的出口 key/group 功能是独立维度，禁止再把下游协议绑定成 group。
- **认证**：智谱各口用 `Authorization: Bearer <裸 key>`（coding/推理口）；monitor 口社区脚本用裸 key（无 Bearer），但对团体 coding plan 无效。
- **缓存池代理（F4，2026-09，`docs/design/cache-pool/architecture.md`）**：`[cache_pool] enabled=true` 时
  代理（hyper）接管 base_url 端口，new-api 挪 `[new_api.manage].port`（默认仍 3000，开代理须显式改如 13000；
  校验拦端口冲突）。逐请求指定渠道用 new-api 原生机制 `Bearer sk-<48位token>-<channelId>`
  （管理员令牌拼后缀，middleware/auth.go 拆 `-`；**无版本兼容承诺**，升级 new-api 须回归验证）。
  令牌 = sync 幂等建的两把 unlimited root 令牌 qt-proxy-openai/claude（真实 key 仅内存持有）。
  代理红线：bind 失败 fail-fast、不设总超时（SSE 长流）、不开 reqwest 压缩（Content-Encoding 会失配）、
  任何日志不得打鉴权头。管理面/健康检查一律打 `Config::upstream_base()`（内部端口），不是 base_url。
  reqwest 0.11（http 0.2）与 hyper 1（http 1）双栈共存——头/状态码必须按字节转换，不能直传。
  升级 new-api 版本 = N1 机制的回归点。

## 工作流程

遵循 @docs/workflow.md。核心铁律（我此前反复违反，务必守住）：

```
新功能开发：调研 → [确认] → 架构 → [确认] → 细化 → [确认] → 审查 → 逐模块实现+测试+审核
每步操作：说明计划 → [等待确认] → 执行单步 → 报告结果 → [等待反馈]
```

**不确认不实现。不跳过设计直接写码。调研靠查源码/实测，不靠猜。**
