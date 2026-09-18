# 修正方案 — newapi-401-relogin（NewApiClient 401 自动重登）

> workflow §7 流程：本文档八部分，修复前列 1-5 与 6-8 的计划，修复后回填状态。
> 工作包 F0（四项改造方案，2026-09-16 批准）。F1 的 DELETE 渠道、F3 的「重启回退」路径都依赖本修复。

## 第一部分：现象与复现

- **现象**：new-api 进程重启（`down`/`up`、容器重建、手动重启）后，本工具的管理会话作废，但客户端**不会**重登——之后：
  - 面板读数全空（channels=0、quota=-1，`/api/channel/`、`/api/user/self` 拿回 401 的 `success:false` 体，解析成空集）
  - priority PUT 全失败（「更新渠道失败: HTTP 401」）
  - 决策本身不坏（已下发的 priority 在 new-api 落了库），但工具从此失去对 new-api 的一切观察与控制，直到重启本工具进程。
- **复现**：`cargo run --release -- down config.toml && cargo run --release -- up config.toml`，第二个进程起来后（新 new-api），若**不重启第一个进程**的会话场景不可复现（up 会重新登录）；真正的复现是**同进程内** new-api 被外部重启：起 `run`，然后 `kill $(cat .newapi/new-api.pid)` 再手动拉起 new-api，观察面板与日志。
- **出错代码路径**：
  - `src/newapi.rs:250-252` `authenticate()`：`if !matches!(self.auth, Auth::Pending) { return Ok(()) }` —— Session 一旦建立永不重登；
  - `src/newapi.rs:182-198` `apply_headers()` + 各 API 方法的 `.send().await`（如 `list_channels` newapi.rs:285-292、`set_channel_field` newapi.rs:746）——**对 HTTP 状态码完全不区分**，401 与 200 走同一 `.json()` 解析路径。
- **预期行为**：管理调用收到 401（会话失效）→ 自动重登一次 → 重试原调用一次 → 仍失败才报错。
- **实际行为**：401 被当普通响应解析，读接口产出空数据/缺字段错误，写接口报「更新渠道失败」。

## 第二部分：根因分析

- **根因**：鉴权状态机不完整——只有 `Pending → Session` 一条边，缺 `Session →(401)→ Pending →(login)→ Session` 的失效恢复边；且请求发送层不看 HTTP 状态码，401 无法被识别为「可恢复的鉴权错误」而流进了业务解析。
- **症状 vs 根因**：面板全空/priority 失败都是症状；单修「面板读数兜底」或「PUT 失败重试」都只是绕过。消除根因 = 状态机补失效边 + 发送层识别 401。
- 非根因（排除）：cookie 丢失（reqwest cookie_store 会自动带，问题是服务端 session 表已随重启清空）；admin_token 模式（Token 模式无会话，不受影响，其 401 属配置错误，重试无意义，不触发重登）。

## 第三部分：参考实现对照

- **new-api v1.0.0-rc.20 服务端行为**（源码已核，`middleware/auth.go`）：会话校验失败 → `abortWithMessage(c, http.StatusUnauthorized, …)`，即 **HTTP 401** + `success:false` JSON 体。⇒ 客户端以 **HTTP 状态码 == 401** 作为唯一重登触发信号即可，不依赖 message 文案（文案随版本变）。
- **CLAUDE.md 已知限制条目**（「new-api 重启会作废本工具的管理会话…正确修法：NewApiClient 检测管理调用 401 → 重登一次重试」）：本方案与该结论一致，落地后删除该「待修」条目。
- **login 的 CriticalRateLimit（20 次/20 分钟）**（CLAUDE.md）：重登必须有**尝试冷却**，防止会话真正坏掉（密码改了）时，每轮 tick 的十几个调用各自重登，瞬间烧光限额把自己锁死——这是本修复自身引入的新风险点，必须内置防护。

## 第四部分：修复方案

**修什么**（`src/newapi.rs`）：

1. `NewApiClient.auth: Auth` → `auth: Arc<tokio::sync::Mutex<AuthState>>`，方法全部保持 `&self`（Arc 共享结构不变；`main.rs` 里 `mut api` 顺手去 mut）。
   ```
   struct AuthState { auth: Auth, last_login_attempt: Option<tokio::time::Instant> }
   ```
2. 新增私有 `send_authed(&self, ctx: &str, make: impl Fn() -> reqwest::RequestBuilder) -> Result<reqwest::Response>`：
   - `make().send()` → 若 HTTP 状态 == 401 且当前为 Session 模式 → `try_relogin()` 成功则 `make()` **重建请求**再发一次（RequestBuilder 一次性，须闭包重建；失败/冷却中则原样返回 401 响应，由调用方按现状报错）。
   - Token 模式 401 → 不重登（配置错误，重试无意义），直接透传响应。
3. `try_relogin(&self)`：锁内双检——`last_login_attempt` 距今 **< 10s** → bail（防 CriticalRateLimit 烧穿；首个失败后 10s 内的其它 401 调用直接报错，与现状一致）；否则记录时刻 → `do_login()`（登录请求本身不经 send_authed）→ 更新 `auth = Session{user_id}`（reqwest cookie_store 自动换新 cookie）。
4. `do_login()` 从现 `authenticate()` 主体抽出复用；`authenticate(&self)`：锁内 `Pending` 才（`ensure_setup` 后）`do_login`，幂等语义不变。
5. 全部带鉴权的管理调用（list_channels / list_channel_states / recent_logs / usage_data / user_quota / get_channel / set_channel_field 内的 GET+PUT / sync_channels 链 / 未来 F1 新增方法）改走 `send_authed`；`ensure_setup` / `do_login` 本身不走（无需鉴权）。
6. `apply_headers` 改为锁内 clone 当前 auth 快照后加头（锁不跨 await 之外持有）。

**为什么这样修（根因消除）**：状态机补上 `Session --401--> login --> Session` 边，发送层把 401 识别为可恢复信号——会话失效从「静默污染业务解析」变成「原地恢复」，面板/Priority/未来的 DELETE 全部自愈。

**修改后的预期行为（复现用例走一遍）**：`run` 中 kill new-api → 手动拉起 → 下一轮 Panel/tick 的首个 401 调用触发重登（日志一条 info「会话失效，已自动重登」）→ 同请求重试成功 → 面板读数 60s 内自愈，priority PUT 正常。密码错误场景：首次重登失败 → 10s 冷却生效 → 后续调用直接报「登录失败」原文，不会连环打 login。

## 第五部分：正确性论证

- **根因消除**：见第四部分「为什么这样修」。
- **不变量保持**：
  - 「config.toml 是唯一数据源」「面板出错不影响切换循环」等既有不变量零触碰（只改发送/鉴权层）；
  - `authenticate` 幂等不变（Token/已登录 → no-op）；
  - 重登后 cookie 由 cookie_store 原地更新，`New-Api-User` 头从新登录响应的 user_id 取，与首登同路径。
- **无回归引入**：
  - 正常路径（200）行为逐字节等价：send_authed 只是包了一层「状态码判断」，不改变请求构造与解析；
  - Token 模式（admin_token）完全不受影响（401 不触发重登）；
  - 并发：多个 task 共享 `Arc<NewApiClient>` 同时 401 → 锁串行化重登，10s 冷却保证最多一次/10s；
  - 新风险（login 限流烧穿）已由冷却防护，见第六部分用例 5。
- **锁与 await**：`tokio::sync::Mutex`（临界区含登录 `.await`，不能用 std Mutex）；`apply_headers` 只做快照不长期持锁，无死锁面（单锁无序问题）。

## 第六部分：测试用例清单

| 类型 | 用例描述 | 状态 |
|------|---------|------|
| 回归 | 同进程内重启 new-api（kill pid + 重拉）→ 面板 60s 内自愈、priority PUT 成功（第一部分复现用例固化） | 待加：e2e 手册步骤写回 README/验收记录 |
| 新增 | Session 失效后并发 401（连续 curl 三个面板接口）→ 日志只见**一次**重登（冷却生效） | 待加：e2e |
| 新增 | root 密码被改 → 重登失败 → 10s 内后续调用报「登录失败」原文、**不**再打 login（保护 CriticalRateLimit） | 待加：e2e |
| 新增 | Token 模式（admin_token 配置）401 → 不重登、报错行为与现状一致 | 待加：e2e |
| 新增 | 正常路径（无 401）→ 请求/响应与改动前逐字节等价（现有 e2e 冒烟即回归） | 待加：现有用例覆盖 |

## 第七部分：代码更新清单

| 文件 | 函数 / 行号 | 改动概述 | 状态 |
|------|------------|---------|------|
| `src/newapi.rs` | `Auth`/`NewApiClient` | auth 字段改 `Arc<tokio::Mutex<AuthState>>`，新增 AuthState（含 last_login_attempt） | 已改 |
| `src/newapi.rs` | `apply_headers` | 改为接收鉴权快照参数（同步、无锁） | 已改 |
| `src/newapi.rs` | `authenticate` | 拆出 `do_login`；改 `&self`；幂等语义不变 | 已改 |
| `src/newapi.rs` | 新增 `send_authed` / `try_relogin` | 401 → 冷却检查 → 重登 → 重建重试一次 | 已改 |
| `src/newapi.rs` | 各管理方法（9 个带鉴权调用点） | `.send()` 改走 `send_authed`（闭包传 RequestBuilder 构造） | 已改 |
| `src/main.rs` | `let mut api`（3 处） | 去 mut | 已改 |

## 第八部分：文档更新清单

| 文档路径 | 要改什么 | 状态 |
|---------|---------|------|
| `CLAUDE.md` | 「已知遗留（401 不自动重登，待修）」条目改写为已修复的行为描述 | 已改 |
| `docs/fixes/newapi-401-relogin.md` | 第六/七/八部分回填实际结果 | 已改（e2e 用例留待最终验收阶段统一执行） |
