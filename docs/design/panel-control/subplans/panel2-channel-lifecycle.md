# 子计划 — panel2-channel-lifecycle（渠道生命周期：启动对齐 + 弃用语义 + 面板编辑）

> 上游方案：2026-09-16 四项改造（缓存池代理前置工作包 F1+F2）。
> 本文为实施前设计记录；实现已随 commit 落地，行为以代码为准，冲突时先修本文。

## 1. 范围

- **F2 弃用语义**：「停止调度」→「弃用」：config.toml 条目**保留**并打 `deprecated = true` +
  清 `channel_id`；new-api 渠道**删除**。可恢复（探活 + 重建渠道 + 去标志落新 id）。
- **F1 启动对齐**：`run` 与 `up`/`sync` 共用 `align_startup`——sync（补建 + 模型对账 +
  **删弃用残留渠道**）+ 解析到的 channel_id 落盘 config + 确保 qt-proxy 中继令牌。
- **F1 面板「改」**：编辑 name/note/org/project（selector 先探活、改名联动渠道、config 单次
  原子写）+ 手动模型重对账按钮。
- **F1 令牌底座**：两把 root 名下 unlimited 令牌 `qt-proxy-openai`/`qt-proxy-claude`（F4 的
  `Bearer sk-<key>-<channelId>` 逐请求指定渠道机制）。

## 2. 数据结构

- `KeyMapping.deprecated: Option<bool>`（serde default；旧配置 = 活跃）。
  **统一规则：活跃 key 持有 channel_id，弃用时清空**（渠道将删，id 必失效）。
- `ChannelOp` 扩 `Delete { name, id }`（渠道名匹配**弃用** key）；`Skip` 增 `id` 字段——
  **id 优先匹配**：config 显式 channel_id 且存活 → 认 id（容忍渠道改名）；否则按 name。
  只碰与 config key 名精确匹配的渠道，用户自建渠道永不被删。
- `Command` 增 `DeprecateKey` / `RestoreKey` / `UpdateKey` / `ResyncModels`；
  `StatusSnapshot.deprecated_keys`（灰显 + 恢复按钮）。

## 3. 关键顺序（不变量）

| 操作 | 顺序 | 理由 |
|---|---|---|
| 弃用 | ①priority 压最低（**失败=硬失败**，防双 active）→ ②config 打标志+清 id（失败中止，只留幂等 PUT）→ ③内存摘除 → ④删渠道（失败 warn，启动对齐兜底重删） | 唯一数据源先行；不可逆操作（删渠道）放最后 |
| 恢复 | ①探活（失败不动）→ ②残留同名渠道删后重建（**不复用**——凭据可能错配）→ ③config 单次原子写（去标志+落新 id，防半恢复态）→ ④热加载 standby 入场 | |
| 编辑 | ①改名校验（字符集+重名，含弃用条目与 config 全量）→ ②selector 变更先探活 → ③渠道改名（失败中止）→ ④config 单次原子写+内存联动 | |
| add_key | 查重覆盖 `self.keys` **和** `self.deprecated`；名字拒绝引号/尖括号/反斜杠/控制字符 | 否则渠道建好才发现 config 写不进 → 无主野生渠道 |

## 4. 接口

- `DELETE /api/keys/<channel_id>` → 弃用（30s）；`POST /api/keys/restore {name}`（60s）
- `POST /api/keys/update {channel_id, name?, note?, org?, project?}`（30s；
  org/project 任一出现即「重建 selector」模式，None/空 = 清该 header）
- `POST /api/keys/resync-models {channel_id}`（30s；探测失败报错，**不用 fallback 覆盖**）
- 前端：编辑表单展开期间跳过 grid 重渲染（5s 快照会打断输入）；名字/备注统一 `esc()` 转义，
  onclick 经 data 属性传参。

## 5. new-api 契约（v1.0.0-rc.20 源码查证）

- `DELETE /api/channel/:id`：级联删 abilities，不动日志/用量/token。
- `POST /api/token/`（**尾斜杠**）建令牌；列表/详情 key 打码，真实 key 走
  `POST /api/token/:id/key` → `{key}`。
- `Bearer sk-<48位key>-<channelId>`：管理员令牌拼渠道 id 后缀逐请求指定渠道
  （middleware/auth.go 拆 `-` 取第二段；只校验渠道存在+启用）。⚠️ 无版本兼容承诺，
  升级 new-api 须回归验证。

## 6. 已知取舍

- 中继令牌/渠道对齐失败不阻断启动调度（warn + 下次重试）；qt-proxy 令牌在 F4 前只建不用。
- dry_run：弃用=只写 config（warn 渠道残留）；恢复/编辑/模型对账=拒绝执行（会动 new-api）。
- `load_key` 每次全量解析 config.toml（文件 KB 级，频率为人工操作级，不值得增量解析）。
