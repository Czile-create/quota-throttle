# 架构设计 — 下游双协议接入（ccr-1 修订版：双渠道原生透传）

> 调研：`docs/research/claude-code-routing-research.md`
> 本文件由子计划 ccr-1（`subplans/ccr-1-native-anthropic.md`）修订——原「单渠道 + 格式转换」
> 方案在「保真 + 缓存」维度被 2026-09-17 实测推翻（转换剥掉 cache_control、边缘场景工具/
> 分类器错误），仅保留为 claude 渠道缺失时的回落路径（I6）。

## 1. 结构

```text
                        ┌─ POST /v1/chat/completions ──→ NewAPI ─→ <name>      渠道(type 8,  coding口)  ─OpenAI原生─→ 智谱
opencode / Claude Code ─┤                                 ↑N1后缀
  → 代理 :3000          └─ POST /v1/messages ──────────→ NewAPI ─→ <name>-claude 渠道(type 14, /api/anthropic) ─Anthropic原生透传─→ 智谱
```

OpenAI/Anthropic 是下游协议；token/group 是访问与出口选择；渠道是上游凭据。三者不可混一层。
**调度状态（quota/pin/合格集/冷却/负载/亲和池）每 key 恰好一份**，以主渠道 id 为逻辑 id；
claude 渠道只是同一把 key 在 Anthropic 协议上的第二个物理出口，claude 渠道 id 只在代理发送
拼 N1 后缀的一瞬换入（单一注入点 `proxy::send_id_for`）。

## 2. 模块职责

- `config.rs`：`channel_template`（主）+ `claude_channel_template`（可选，智谱主模板时自动
  补默认）；type=14 的 base_url 不得带 `/v1/messages` 尾巴（校验拦截）。
- `newapi.rs`：每把活跃 key 对齐两个受管渠道（`<name>` + `<name>-claude`）；claude 渠道
  失败只 warn 不阻断（D6）；claude 模板停用时清理存量 `-claude` 渠道（I7）。
- `orchestrator.rs`：每把 key 一个逻辑 channel id + 可选 claude id；priority 对两渠道同值
  双写；决策逻辑全按逻辑 id，零协议感知。
- `router.rs` / `proxy.rs`：路由全程逻辑 id；`RouteView.claude_of` 仅发送点消费。
- `main.rs`：打印双渠道映射；`ANTHROPIC_AUTH_TOKEN` 复用现有 NewAPI key。
- `status.rs`：key 卡片聚合双渠道实时指标；受管集合含两渠道。

## 3. 不变量

- I1（ccr-1 修订）：一把上游 key 恰好一个 OpenAI 协议受管渠道；claude 模板启用时至多再一个
  Claude 协议受管渠道（`{name}-claude`）。调度状态每 key 恰好一份，以逻辑 id 为键。
- I2：OpenAI 与 Claude 下游使用同一 NewAPI token/group（qt-proxy 中继令牌对仅日志归因不同）。
- I3：两种下游共享同一活动 key 与 priority（两渠道 priority 恒同值，双写幂等收敛）。
- I4：quota、pin、95% 安全线、冷却、负载、亲和池只维护一份，不因下游格式分叉。
- I5：启用 Claude 下游不写 NewAPI option，不创建或打印任何面向用户的新 token。
- I6（新）：所选 key 无 claude 渠道映射时，`/v1/messages` 回落主渠道的格式转换路径。
- I7（新）：claude 模板停用后，sync 删除全部 `{key}-claude` 受管渠道。

## 4. 兼容性

现有配置无需新增字段（智谱主模板自动补 claude 默认）。Claude Code 只设置：

```text
ANTHROPIC_BASE_URL=<new_api.base_url>
ANTHROPIC_AUTH_TOKEN=<现有 NewAPI key>
```

回滚：删除/注释 `[new_api.claude_channel_template]` 后重启——I7 清理全部 `-claude` 渠道，
行为逐字节回到单渠道转换方案。
