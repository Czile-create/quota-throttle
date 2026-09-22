# quota-throttle

给智谱 GLM Coding Plan **多 key 池**做**预防式调度**的守护进程：轮询每把 key 的真实用量（5 小时 / 每周窗口），让每个请求都落在「额度最经烧、缓存能连续命中」的 key 上，在**撞墙之前**自动换道；并同时管好 new-api（下载 / 启动 / 建渠道）。

opencode（OpenAI 协议）与 Claude Code（Anthropic 协议）都接同一个入口：前者走智谱 coding 口的原生 OpenAI 通道，后者走智谱原生 Anthropic 口**无损透传**——工具调用、thinking、`cache_control` 缓存提示全部保真。

```
              opencode / Claude Code / 其它 OpenAI 兼容客户端
                        │  base_url 都指向这里
                        ▼
              ┌──────────────────┐    看板 :3001（每 key 用量/评分/实时流量/请求流水）
              │ 缓存池代理 :3000  │
              │  逐请求选路：      │
              │  pin > 缓存命中    │
              │      > 评分公式    │
              └────────┬─────────┘
                       ▼
                new-api :13000（内部端口，本工具托管）
                  ├─ <name>       渠道(type 8)  ──OpenAI 原生──▶ 智谱 coding 口
                  └─ <name>-claude 渠道(type 14) ─Anthropic 原生透传──▶ 智谱 /api/anthropic
                       ▲
       quota-throttle 控制循环：每 60s 探每把 key 的 5h/周已用%，
       维护合格集，写 priority 兜底阶梯；决策与路由同源
```

## 路由怎么工作（为什么不是加权分散）

智谱的 **prompt 缓存按 key 隔离**：请求在 key 间分散会打散缓存，opencode / Claude Code 那种「系统提示 + 长上下文大量复用」的命中率会大跌。但单纯钉死一把 key 又浪费多 key 的额度与抗 429 能力。本工具的做法是**逐请求的缓存亲和路由**：

1. **缓存命中**：同一对话（按消息指纹识别，对断点挪动免疫）一直用同一把 key——缓存连续命中；
2. **新请求评分**：`评分 = 0.6·周刷新临期 + 0.2·容量 + 0.2·本机负载`
   - 周临期（EDF 平滑版）：优先烧「周窗口快重置」的 key 的额度，别浪费；
   - 容量：`min(5h 剩余%, 周剩余% 折算)`——找扛得住这次请求上下文的；
   - 负载：60 秒内请求越少越好——自然均摊并发；
3. **pin**：看板上手动钉住某把 key 时最优先（含缓存命中迁移，仍受合格线约束）；
4. **429 换道**：命中请求先按退避表等一等服务（防会话群集体迁徙踩踏），预算用尽自动转评分换道补救，不当场把 429 甩给客户端。

用量探针（眼睛）与路由同源：任一窗口达 `throttle`(95%) 的 key 不再接新对话；全部越线进入降级档后放宽到 `exhausted`(100%) 榨干流转。决策循环同时维护一份 new-api **priority 兜底阶梯**（active/standby/exhausted 三档），只在代理降级透传的罕见窗口（如刚启动）起作用。

### 双协议双渠道（每把 key 两个受管渠道）

智谱 Coding Plan 官方支持两种协议：OpenAI 兼容的 coding 口与原生 Anthropic 口（`https://open.bigmodel.cn/api/anthropic`）。本工具为每把 key 建两个渠道：`zhipu-N`（type 8 → coding 口）与 `zhipu-N-claude`（type 14 → Anthropic 口），代理按**请求路径**选协议对应渠道。

**为什么 Claude 必须走原生口**：把 Anthropic 请求转成 OpenAI 格式再转回来，实测会**剥掉 `cache_control` 缓存提示**（转换前相同请求二次 `cache_read` 恒 0），且复杂工具调用/流式增量偶发错乱；原生通道实测 4196-token 前缀二次请求 `cache_read=4160`——缓存真正接通。调度状态（用量/合格集/冷却/负载/缓存池）**每 key 仍只有一份**，协议差异只在发送的一瞬间体现，opencode 路径零改动。

存量配置**零迁移**：主模板是智谱 coding 口时自动补 claude 渠道模板；回滚只需删掉配置段重启，`-claude` 渠道自动清理。某把 key 的 claude 渠道建失败时自动回落格式转换路径，可用性不受影响。

## 快速开始

```bash
cp config.example.toml config.toml
# 编辑 config.toml（必改三处见下）；推荐开启缓存池：
#   ① 取消 [cache_pool] 段注释（enabled = true）
#   ② [new_api.manage].port 从 3000 改成 13000（代理接管 3000，校验会拦冲突）
cargo run --release -- up config.toml   # 起 new-api + 建渠道 + 路由循环 + 看板
```

| 子命令 | 作用 |
|--------|------|
| `up` | 下载/启动 new-api → 双渠道建齐 → 路由循环 + 看板 |
| `sync` | 只建/对齐渠道并打印 `name → channel_id (+claude id)`，不进循环 |
| `run` | 假设 new-api 已在跑，只解析渠道并进入循环 |
| `down` | 停掉本工具托管的 new-api |

数据（SQLite / 二进制 / 日志 / PID）都在 `./.newapi/`。日志级别用 `RUST_LOG` 控制。`up` 幂等：已存在渠道对账模型目录，缺失补建；每次启动自动把 new-api 管理用户内部额度调大（默认 2 亿货币单位，只调大不调小——它按倍率虚构记账，见底会 403 挡转发）。

## 新用户上手：申请 key → 配置 → 跑通

### 第 1 步：申请两类 API key（别混用）

| | 上游智谱 key | 下游 NewAPI 令牌 |
|---|---|---|
| 谁发给你的 | 智谱开放平台 | 本工具托管的 new-api |
| 填在哪 | `config.toml` 的 `[[keys]]` | 客户端（opencode / Claude Code） |
| 作用 | 查用量 + 建渠道（真正烧的额度） | 客户端访问 new-api 的凭证 |

**A. 上游智谱 key**（≥2 把才有意义——多 key 池是本工具的核心价值）：

1. 登录 [bigmodel.cn](https://bigmodel.cn)（注册 + 实名），订阅 **GLM Coding Plan**（个人或团体套餐）
2. 控制台 → **API Keys** → 新建并复制（形如 `xxxxxxxx.yyyyyyyy`，中间一个点）
3. 团体套餐还要按「org / project 的值在智谱网页上怎么取」逐把抄 selector
4. 嫌麻烦可跳过手工编辑：服务跑起来后直接在看板「**探活并添加**」录入——探活不过就什么都不改

**B. 下游 NewAPI 令牌**（`up` 跑起来之后才有地方申请）：

1. 浏览器开 `http://127.0.0.1:13000`（缓存池开启时 new-api 的内部端口），用 `root` + 你设的密码登录
2. 左侧 **令牌** → 添加令牌 → 复制 `sk-xxx`（列表里打码，创建/编辑页拿全值）
3. **一把令牌通用所有下游协议**：OpenAI 兼容口（`/v1`）和 Claude Code（`/v1/messages`）都用它

### 第 2 步：填 config.toml（必改三处）

```toml
# ① 管理员密码（首启自动建 root 用，≥8 位）
root_password = "改成你自己的"

# ② 逐把填智谱 key + selector（完整格式见下节）
[[keys]]
name = "zhipu-1"
zhipu_api_key = "xxxxxxxx.yyyyyyyy"

# ③ 先空跑：确认日志里决策符合预期，再改 false 真正生效
dry_run = true
```

⚠️ **顶层配置项必须写在第一个 `[表]` 头之前**——TOML 表头一旦出现，后面的裸 `key = value` 都归那个表，不报错、只走默认值。

### 第 3 步：起服务 → 验证 → 接客户端

```bash
cargo run --release -- up config.toml
```

验证：看板 `http://127.0.0.1:3001` 每把 key 正常显示用量（出现「查询失败」= selector/鉴权没配对，见「接入要点」1）；`dry_run` 日志决策符合预期后改 `false` 重启。

接客户端（二选一或都用，**同一把 NewAPI 令牌**）：

- **opencode**：provider 的 `baseURL` 改 `http://127.0.0.1:3000/v1`（见「接入要点」3）
- **Claude Code**：`ANTHROPIC_BASE_URL=http://127.0.0.1:3000` + `ANTHROPIC_AUTH_TOKEN=<同一把令牌>` + 模型名映射（如 `ANTHROPIC_MODEL=glm-5.3`、`ANTHROPIC_SMALL_FAST_MODEL=glm-5.3-flash`）——走原生 Anthropic 透传，缓存与工具调用无损（见「接入要点」4）

## Key 配置与重载（日常操作）

**没有热加载**：`config.toml` 只在启动时读一次，改完重启进程（`pkill -f 'quota-throttle up'` 后重新 `nohup ./target/release/quota-throttle up config.toml >> .newapi/quota-throttle.log 2>&1 &`）。看板上的加/删/编辑 key 是例外（走运行时命令通道，自动写回 config，不用重启）。

```toml
[[keys]]
name = "zhipu-1"                       # 用作受管渠道名（zhipu-1 与 zhipu-1-claude）
zhipu_api_key = "xxxxxxxx.yyyyyyyy"
[[keys.quota_headers]]                 # 团体套餐必需的 selector（个人套餐删掉）
key = "Bigmodel-Organization"
value = "org-..."
[[keys.quota_headers]]
key = "Bigmodel-Project"
value = "proj_..."
```

### org / project 的值在智谱网页上怎么取（每把 key 配一次）

1. 浏览器登录 [bigmodel.cn](https://bigmodel.cn)，打开 `https://bigmodel.cn/coding-plan/team/usage-stats`
2. ⚠️ **先把页面上的团队/组织切到这把 key 所属的那个**——账号属多个团队时抄错就是查别家的额度，调度全乱
3. **F12** → Network 标签 → F5 刷新 → 过滤框输 `quota` → 点 `quota/limit` 那条请求
4. Headers → Request Headers 里抄这两行的值：
   - `Bigmodel-Organization: org-xxx` → 第一条 `[[keys.quota_headers]]` 的 `value`
   - `Bigmodel-Project: proj_xxx` → 第二条的 `value`
5. **每把 key 各抄各的**（不同 key 可能属不同团队/项目）

为什么不能省：团队套餐缺 selector 时智谱**不报错**，安静返回空 `limits`——那把 key 会被误判（本工具启动探活会挡下并明说）。

### 三种 key 变更

| 场景 | 步骤 |
|------|------|
| **新增 key** | config 加一条 → 重启；或看板「探活并添加」（先真查一次用量，通过才建双渠道） |
| **替换同名 key 的值** | ⚠️ **sync 按名幂等，不会更新渠道里的旧 key！** 除改 config + 重启外，还须到 new-api 管理页编辑 `zhipu-N` 与 `zhipu-N-claude` 两个渠道的 key |
| **移除 key** | 看板点「🗑 弃用」（删双渠道 + config 打标志，条目保留可一键恢复）；彻底抹掉条目才需手改 config |

### 常驻运行

Linux 手动：`nohup ./target/release/quota-throttle up config.toml >> .newapi/quota-throttle.log 2>&1 &`。macOS 推荐 LaunchAgent（`RunAtLoad` + `KeepAlive`，工作目录钉在项目根——`data_dir` 是相对路径），重载用 `launchctl kickstart -k gui/$(id -u)/com.quota-throttle`。

## 状态看板

`http://127.0.0.1:3001`（`status_addr` 可配，留空不启用）

- 每把 key：**5 小时 / 每周窗口进度条**（95% 处画阈值线）+ **重置倒计时** + 实时 rpm/tpm（主 + claude 双渠道聚合）+ 最后请求
- **绿框「★ 首选」** = 新流量的真实去向（评分最高或已钉住的 key，与代理选路逐分支同语义）；「📌 固定到这把」手动钉住（只是优先级不是安全豁免，越线自动解除）
- **评分分解**：三分量（周临期/容量/负载）展示，看到的分就是路由用的分
- **加/删/编辑 key**：先探活再落盘；弃用可恢复
- 用量图（近 24h 实时 / 近 30 天可下钻）、高峰时段提醒与倒计时
- 查询失败显示「查询失败」而非 0%（不骗你说还有额度）

`GET /api/status` 是同数据 JSON 接口（外部消费者用）。看板 5 秒刷新只读进程内快照，**不给 new-api 增加任何负载**（实时指标全部从单次日志请求推导，绝不逐渠道轮询）。

## 高峰时段：同一个请求，14–18 点烧掉 3 倍额度

「高峰期」影响的**不是限额，是扣减系数**——每日 **14:00–18:00（UTC+8，固定）**，GLM-5.2 / GLM-5-Turbo 高峰 **3 倍**、非高峰 2 倍（**限时福利：非高峰仅 1 倍，到 9 月底**，届时改 `[peak].off_peak` 回 2.0）；GLM-4.7 等恒 1 倍。智谱没有接口查「现在是否高峰」，看板按时钟算（窗口按 UTC+8 定义，代码用 `tz_offset_hours` 而非本机时区）。依据：[coding-plan/faq](https://docs.bigmodel.cn/cn/coding-plan/faq)。

## ⚠️ 接入要点（都是踩出来的）

### 1. 团体套餐读用量：三个条件缺一不可

```
GET  https://open.bigmodel.cn/api/monitor/usage/quota/limit?type=2   ← ① 必须带 ?type=2
Authorization: Bearer <api key>                                      ← ② 必须带 Bearer
Bigmodel-Organization: org-...                                       ← ③ 团体必需的 selector
Bigmodel-Project: proj_...
```

缺任一 → 返回「当前用户不存在coding plan」或 `limits` 为空（会被误判，永不参与调度）。返回里 `unit=3&number=5`=5 小时窗口、`unit=6&number=1`=每周窗口；`TIME_LIMIT` 是 MCP 搜索次数（须过滤）。窗口有 **token 型（TOKENS_LIMIT）与积分型（CREDIT_LIMIT）** 两种计费模式，语义相同都要算。

### 2. 渠道类型：OpenAI 通道必须 Custom(8)，Claude 通道必须 type 14

智谱 coding 口是 `/api/coding/paas/v4/chat/completions`（`/v4` 不是 `/v1`）——OpenAI 类型(1) 会拼成 `.../v4/v1/...` 而 404，必须 Custom(8) 原样透传全路径。Claude 渠道(type 14) 的 base_url **只填到 `/api/anthropic` 为止**（new-api 自动拼 `/v1/messages`，与 Custom 的完整路径语义相反；配错启动即拦）。主模板是智谱 coding 口时，claude 渠道模板**自动补默认**，无须手写：

```toml
[new_api.channel_template]
type = 8
base_url = "https://open.bigmodel.cn/api/coding/paas/v4/chat/completions"
models = "glm-5.2,glm-5.3,glm-5.3-flash"   # 探测失败时 fallback；平时以 /models 实时发现为准
group = "default"

[new_api.channel_template.model_discovery]
url = "https://open.bigmodel.cn/api/coding/paas/v4/models"
auth = "bearer"
```

### 3. opencode 接入：改 provider 的 baseURL，并清掉 auth.json 里的智谱 key

```jsonc
// ~/.config/opencode/opencode.jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "zhipuai-coding-plan": {
      "options": {
        "baseURL": "http://127.0.0.1:3000/v1",
        "apiKey": "<new-api 调用令牌>"     // 不是智谱 key！
      }
    }
  }
}
```

同时把 `~/.local/share/opencode/auth.json` 里的 `zhipuai-coding-plan` 条目清掉——否则 opencode 可能优先用 auth.json 里的智谱 key 连 new-api 被 401。模型名以该 key 的 `/models` 实时返回为准（`sync` 后渠道自动收敛）。

### 4. Claude Code 接入：原生 Anthropic 透传（双渠道自动就绪）

服务起来后每把 key 自带 `-claude` 渠道，`/v1/messages` 全程原生格式（无转换）。客户端只需：

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:3000
export ANTHROPIC_AUTH_TOKEN='<与 opencode 相同的 NewAPI key>'
export ANTHROPIC_MODEL=glm-5.3                 # 模型名映射（按需）
export ANTHROPIC_SMALL_FAST_MODEL=glm-5.3-flash
```

`cache_control` 缓存提示直达智谱（实测大前缀二次请求 `cache_read>0`）、thinking/工具调用/流式全保真。回滚：删掉 `[new_api.claude_channel_template]`（如有）重启，`-claude` 渠道自动清理，Claude 流量回落格式转换路径。

## 设计要点

- **调度零分叉**：用量/合格集/冷却/负载/缓存池全部每 key 一份；协议差异只在代理发送的一瞬体现（单一注入点）。升级 new-api 版本时需回归「Claude 透传 + 逐请求指定渠道」两点。
- **缓存亲和 + 评分路由**：对话粘 key（护缓存），新请求按「周临期(EDF) + 容量 + 负载」评分落点；429 先等待重试原渠道再换道，不当场甩给客户端。
- **鲁棒**：单把 key 查询失败只跳过本轮（不参与决策不动状态）；「查不到用量」一律按未知处理，绝不默认 0%；claude 渠道建失败回落转换路径；管理会话 401 自动重登（带冷却，防锁死）。
- **幂等下发**：稳态下对 new-api 零写入；`up` 幂等可反复执行。
- **看板绝不拖垮主循环**，也绝不逐渠道轮询管理 API（new-api `/api` 有全局限流，预算留给控制循环）。
- **恢复干净**：智谱耗尽报文不撞 new-api 的自动禁用关键词，渠道全程 enabled，窗口重置自动恢复。

## 已知边界

- **吞吐上限不变**：多 key 池扛的是额度轮换，不是无限并发；总额度不够时谁路由都 429。
- **轮询间隔**：默认 60s，间隔内活动 key 可能冲过阈值一点（95%→100% 有 5% 余量，实测 <1%）。
- **多机部署**：可以——每台机器各跑一份（各自的 new-api/代理/看板）共享同一批 key；用量探针同源一致，负载计数各算各的，EDF 语义天然对齐。
- **合规**：多个**个人** Coding Plan 拼 key 池扛团队用量可能违反智谱条款；团体套餐是正规做法。

## 开发

遵循 `docs/workflow.md`（半形式化 SDD 流程：调研→架构→细化→v2 契约审核）。设计文档在 `docs/design/`；项目约定与踩坑记录在 `CLAUDE.md`。`cargo test` 全单测；真实环境验收以 `up` 实测为准。

```bash
cargo build --release
cargo test
```

## License

MIT
