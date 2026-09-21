# Decisions — ccr-1-native-anthropic（⚠️ 项处置记录）

> 审计：`audit-report.md`（2026-09-21，subagent 独立审，0 ❌ / 5 ⚠️）
> 处置原则：❌ 必修；⚠️ 逐条决策为 修码 / 文档化 / 接受偏离，理由如下。

| # | 发现 | 判定 | 处置 | 理由 |
|---|------|------|------|------|
| E1 | `orchestrator.applied` 含 claude 渠道 id，与 expectations §7 I1 的结构列举矛盾 | ⚠️ 文档不一致 | **修文档**（expectations §7 I1 措辞修正） | 权威子计划 §10-M2 明确规定 applied 按 channel_id 分键、两渠道独立幂等——这是 priority 下发的记账，不是路由决策状态；是 expectations 抽取时列举过宽，不是实现偏离 |
| E2 | `-claude` 后缀在 helper 之外另有 4 处裸写字面量（main.rs ×2、orchestrator.rs ×2） | ⚠️ 低风险 | **修码**（helper 改 `pub`，4 处统一引用） | 后缀是契约标识（I7 精确匹配、受管集合判定都靠它），字面量漂移是真实风险；改动零行为差异，纯收敛单一真相 |
| E3 | 子计划 §10 测试清单的「SyncOutcome.claude 填充」无单测 | ⚠️ 缺口 | **文档化**（expectations Step 5 记录段注明由 e2e 覆盖） | 项目无 mock server，sync 级以 e2e 为主（既有惯例）；e2e 已实证填充正确（config 落盘 6 个 claude id、面板 API 带 claude 字段） |
| E4 | 新测试 `from_snap_收集claude映射` 落在 mod tests 闭括号之外（顶层作用域） | ⚠️ 组织缺陷 | **修码**（移入 mod tests） | 编译与断言均正常，但测试归位才能被 `router::tests` 命名空间正确聚合 |
| E5 | 面板徽标「claude 渠道被禁用（/v1/messages 走转换）」语义不准 | ⚠️ 文案 | **修码**（改为「请求将换道重试」） | 禁用 ≠ I6 回落（I6 触发条件是无映射；禁用渠道仍会被打到、走既有 403 换道语义）——文案与契约 I6 原文对齐 |

修码项（E2/E4/E5）与文档项（E1/E3）已随本文件同提交落盘；`cargo test` 107 全过，
e2e 验收六条全过（见子计划 §11 验收记录）。
