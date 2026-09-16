mod boot;
mod config;
mod model_catalog;
mod newapi;
mod orchestrator;
mod proxy;
mod router;
mod quota;
mod status;

use crate::boot::NewApiProcess;
use crate::config::{Config, ResolvedKey};
use crate::newapi::{NewApiClient, SyncOutcome};
use crate::orchestrator::Orchestrator;
use anyhow::{bail, Context, Result};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const USAGE: &str = "\
用法: quota-throttle <子命令> [config.toml]
  up     下载/启动 new-api（若配了 manage）→ sync 建渠道 → 进入切换循环
  sync   （确保 new-api 起着）按 key 列表建/对齐渠道并打印 name→channel_id，不进循环
  run    假设 new-api 已在跑，只解析渠道并进入切换循环
  down   停掉本工具托管的 new-api
省略子命令时按 run 处理。";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let mut args = std::env::args().skip(1);
    let first = args.next();
    let (cmd, cfg_path) = match first.as_deref() {
        Some("up") | Some("down") | Some("sync") | Some("run") => (
            first.clone().unwrap(),
            args.next().unwrap_or_else(|| "config.toml".to_string()),
        ),
        Some("-h") | Some("--help") => {
            println!("{USAGE}");
            return Ok(());
        }
        Some(path) => ("run".to_string(), path.to_string()),
        None => ("run".to_string(), "config.toml".to_string()),
    };

    let cfg = Config::load(&cfg_path).with_context(|| format!("加载配置失败: {cfg_path}"))?;

    match cmd.as_str() {
        "down" => cmd_down(&cfg),
        "sync" => cmd_sync(cfg).await,
        "up" => cmd_up(cfg).await,
        "run" => cmd_run(cfg).await,
        _ => {
            println!("{USAGE}");
            Ok(())
        }
    }
}

/// F4/review#2：代理 listener **尽早绑定**（在 ensure_newapi_up/align 之前）——
/// 否则每次重启的对齐阶段（模型发现+sync 可达 ~50s）里客户端吃 connection-refused；
/// 早绑后至少是 502-可重试。首次迁移（旧 new-api 还占着 3000）时先停托管进程再绑，
/// 把拒连窗口从 ~50s 压到 ~1s。返回 None = cache_pool 未启用。
async fn early_bind_proxy(cfg: &Config) -> Result<Option<tokio::net::TcpListener>> {
    if !cfg.cache_pool.enabled {
        return Ok(None);
    }
    let addr = proxy_listen_addr(cfg)?;
    match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => Ok(Some(l)),
        Err(first_err) => {
            // 大概率旧 new-api 还占着端口——停掉托管进程再试一次
            if let Some(m) = &cfg.new_api.manage {
                NewApiProcess::new(m, &cfg.upstream_base())?.stop()?;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            tokio::net::TcpListener::bind(&addr)
                .await
                .with_context(|| format!("代理监听 {addr} 失败（早绑定）：{first_err:#}"))
                .map(Some)
        }
    }
}

/// 若配了 manage，就确保 new-api 原生进程在跑（不在则下载+启动）。
async fn ensure_newapi_up(cfg: &Config) -> Result<()> {
    // F4：管理面/健康检查一律打**内部 upstream**（cache_pool 开启时代理才占 base_url）
    let upstream = cfg.upstream_base();
    match &cfg.new_api.manage {
        Some(m) => {
            let proc = NewApiProcess::new(m, &upstream)?;
            proc.ensure_running().await
        }
        None => {
            // 没配托管：只健康检查，起不起来是用户自己的事
            let url = format!("{}/api/status", upstream);
            if reqwest::Client::new()
                .get(&url)
                .timeout(Duration::from_secs(3))
                .send()
                .await
                .map(|r| r.status().is_success())
                .unwrap_or(false)
            {
                Ok(())
            } else {
                bail!(
                    "new-api 在 {} 上不可达，且未配 [new_api.manage] 让本工具托管——\
                     请自行启动 new-api，或配置 manage 让本工具下载运行",
                    upstream
                )
            }
        }
    }
}

fn cmd_down(cfg: &Config) -> Result<()> {
    match &cfg.new_api.manage {
        Some(m) => {
            let proc = NewApiProcess::new(m, &cfg.upstream_base())?;
            proc.stop()
        }
        None => {
            warn!("未配 [new_api.manage]，没有本工具托管的 new-api 可停");
            Ok(())
        }
    }
}

/// 代理监听地址 = base_url 的 host:port（客户端入口原样接管；docker 场景 base_url
/// 是 0.0.0.0 时代理也绑 0.0.0.0，由 compose 的端口映射控制暴露面）。
fn proxy_listen_addr(cfg: &Config) -> Result<String> {
    let u = reqwest::Url::parse(&cfg.new_api.base_url)
        .with_context(|| format!("[new_api].base_url 非法：{}", cfg.new_api.base_url))?;
    let host = u.host_str().unwrap_or("127.0.0.1");
    let port = u
        .port_or_known_default()
        .ok_or_else(|| anyhow::anyhow!("base_url 缺端口：{}", cfg.new_api.base_url))?;
    Ok(format!("{host}:{port}"))
}

/// F3：启动时把 new-api 管理用户的内部额度自动调大（**只调大不调小**，仅托管模式）。
/// 额度见底会 403 挡转发（预扣费）；读值走 API、写值直写 SQLite（管理面无此 API）。
/// F4 后全部流量走 qt-proxy 令牌（归 root），这一步是缓存池代理的硬依赖。
async fn ensure_root_quota(cfg: &Config, api: &NewApiClient) {
    let Some(m) = &cfg.new_api.manage else {
        return; // 外部 new-api：无 SQLite 可写，不动
    };
    let target = i64::try_from(m.root_user_quota_units)
        .ok()
        .and_then(|u| u.checked_mul(500_000))
        .unwrap_or(i64::MAX / 2); // 防溢出的保守大值
    let current = match api.user_quota().await {
        Ok(q) => q,
        Err(e) => {
            warn!(error = %e, "读 new-api 用户额度失败，跳过自动调额");
            return;
        }
    };
    if current >= target {
        return;
    }
    match boot::bump_user_quota(m, &cfg.new_api.root_username, target) {
        Ok(()) => info!(current, target, "已把管理用户额度自动调大（直写 SQLite，运行中生效）"),
        Err(e) => warn!(error = %e, "自动调额失败（不影响调度；下次启动重试）"),
    }
}

/// 启动对齐：渠道 sync（补建/模型对账/删弃用残留，**每次启动必跑**）+
/// 把解析到的 channel_id 落进 config（活跃 key 持有 id 的统一规则，id 优先匹配的底座）
/// + 管理用户额度自动调大（F3）+ 确保 qt-proxy 中继令牌就绪（F4 逐请求路由的凭据）。
/// 返回 (sync 结果, 中继令牌)——令牌未就绪时为 None（代理降级透传，调度不受影响）。
///
/// **dry_run 只观察不动**（PR review #4）：对齐的建渠道/删残留/models PUT/调额度/
/// 建令牌全是 new-api 写操作，dry_run 下一律跳过，退回「只列渠道解析 id」的旧 run 语义。
async fn align_startup(
    cfg: &Config,
    api: &NewApiClient,
) -> Result<(SyncOutcome, Option<crate::newapi::RelayTokens>)> {
    if cfg.dry_run {
        warn!("dry_run：跳过启动对齐的写操作（建/删渠道、模型对账、调额度、建中继令牌），只解析渠道 id");
        let map = api.list_channels().await?;
        let mut out = SyncOutcome::default();
        for k in &cfg.keys {
            if let Some(id) = map.get(&k.name) {
                out.primary.insert(k.name.clone(), *id);
            }
        }
        return Ok((out, None));
    }
    ensure_root_quota(cfg, api).await;
    let outcome = api
        .sync_channels(
            &cfg.keys,
            cfg.new_api.channel_template.as_ref(),
            cfg.priority_standby,
        )
        .await?;
    for k in &cfg.keys {
        if k.is_deprecated() {
            continue;
        }
        let Some(&id) = outcome.primary.get(&k.name) else {
            continue;
        };
        if k.channel_id != Some(id) {
            // 落盘失败不阻断启动——对齐结果本次运行仍有效，下次启动再落
            if let Err(e) = config::set_key_channel_id(&cfg.source_path, &k.name, id) {
                warn!(name = %k.name, channel_id = id, error = %e, "channel_id 落盘 config 失败（下次启动重试）");
            }
        }
    }
    let relays = match api.ensure_relay_tokens().await {
        Ok(t) => Some(t),
        Err(e) => {
            warn!(error = %e, "qt-proxy 中继令牌未就绪（LLM 路径降级透传；重启本工具可重试）");
            None
        }
    };
    Ok((outcome, relays))
}

async fn cmd_sync(cfg: Config) -> Result<()> {
    ensure_newapi_up(&cfg).await?;
    let api = NewApiClient::new(&cfg.new_api, &cfg.upstream_base())?;
    api.authenticate().await?;
    let (outcome, _relays) = align_startup(&cfg, &api).await?;
    print_mapping(&cfg, &outcome);
    print_downstream_access(&cfg);
    Ok(())
}

async fn cmd_up(cfg: Config) -> Result<()> {
    let proxy_listener = early_bind_proxy(&cfg).await?;
    ensure_newapi_up(&cfg).await?;
    let api = NewApiClient::new(&cfg.new_api, &cfg.upstream_base())?;
    api.authenticate().await?;
    let (outcome, relays) = align_startup(&cfg, &api).await?;
    let keys = resolve_keys(&cfg, &outcome.primary);
    run_loop(cfg, api, keys, relays, proxy_listener).await
}

async fn cmd_run(cfg: Config) -> Result<()> {
    let proxy_listener = early_bind_proxy(&cfg).await?;
    ensure_newapi_up(&cfg).await?;
    let api = NewApiClient::new(&cfg.new_api, &cfg.upstream_base())?;
    api.authenticate().await?;
    // run 与 up 同样做启动对齐（run 曾只列渠道不建不对账——「config.toml 每次启动同步」）。
    // 但对齐失败不能把 run 拖死（PR review #9）：对齐是增值动作，切换循环才是本职——
    // models PUT 被新版本拒、某渠道回读校验不过这类**对齐层**错误，退回旧的
    // 「只列渠道」路径继续跑，别让 priority 监督整个缺席。
    let (outcome, relays) = match align_startup(&cfg, &api).await {
        Ok(x) => x,
        Err(e) => {
            warn!(error = %e, "启动对齐失败，退回只读解析（切换循环照常）");
            let map = api.list_channels().await.unwrap_or_default();
            (
                SyncOutcome {
                    primary: cfg
                        .keys
                        .iter()
                        .filter_map(|k| map.get(&k.name).map(|id| (k.name.clone(), *id)))
                        .collect(),
                },
                None,
            )
        }
    };
    let keys = resolve_keys(&cfg, &outcome.primary);
    run_loop(cfg, api, keys, relays, proxy_listener).await
}

/// NewAPI 原生同时接收 OpenAI 与 Anthropic 下游格式；两者复用现有访问 key、group 和渠道。
fn print_downstream_access(cfg: &Config) {
    let base = cfg.new_api.base_url.trim_end_matches('/');
    info!("下游接入共用同一把 NewAPI 访问 key：");
    info!("  OpenAI base URL: {base}/v1");
    info!("  ANTHROPIC_BASE_URL={base}");
    info!("  ANTHROPIC_AUTH_TOKEN=<与 OpenAI 客户端相同的 NewAPI key>");
}

/// 把 config.keys + name→id 映射解析成 orchestrator 用的 ResolvedKey。
/// **primary（刚对齐完的新鲜结果）优先，config 显式 channel_id 兜底**（PR review #2：
/// 渠道被删重建后 config 里的陈旧 id 若优先，本会话会一直管理/路由一个幽灵渠道，
/// 直到重启才自愈；primary 兜不住的场景恰是「按 id 匹配的改名渠道」——那种情况
/// config id 仍是正确的，正好由兜底接住）。
/// **弃用 key 不进调度集**（条目留在 config，凭据留给恢复流程用）。
fn resolve_keys(
    cfg: &Config,
    primary: &HashMap<String, i64>,
) -> Vec<ResolvedKey> {
    let mut out = Vec::new();
    for k in &cfg.keys {
        if k.is_deprecated() {
            info!(name = %k.name, "key 已弃用，跳过调度（config 条目保留）");
            continue;
        }
        match primary.get(&k.name).copied().or(k.channel_id) {
            Some(id) => out.push(ResolvedKey {
                name: k.name.clone(),
                zhipu_api_key: k.zhipu_api_key.clone(),
                channel_id: id,
                note: k.note.clone(),
                quota_headers: k.quota_headers.clone(),
            }),
            None => warn!(name = %k.name, "解析不到 channel_id（既无显式配置也无同名渠道），本 key 跳过"),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KeyMapping;

    /// 渠道重建后 primary 里有新 id —— 必须压过 config 里的陈旧 id（幽灵渠道）
    #[test]
    fn resolve_新鲜对齐结果优先于陈旧config_id() {
        let k = KeyMapping {
            name: "a".into(),
            zhipu_api_key: "k".into(),
            channel_id: Some(1), // 陈旧：渠道 1 已被删，重建后是 7
            note: String::new(),
            deprecated: None,
            quota_headers: Vec::new(),
        };
        let cfg = Config {
            keys: vec![k],
            ..test_cfg()
        };
        let primary = HashMap::from([("a".to_string(), 7i64)]);
        let keys = resolve_keys(&cfg, &primary);
        assert_eq!(keys[0].channel_id, 7, "primary 新鲜值应胜出");

        // primary 没有该名字（改名渠道按 id 匹配）→ 兜底用 config id
        let keys = resolve_keys(&cfg, &HashMap::new());
        assert_eq!(keys[0].channel_id, 1, "primary 缺失时 config id 兜底");
    }

    fn test_cfg() -> Config {
        toml::from_str(
            "poll_interval_secs = 60\n[zhipu]\n[new_api]\nbase_url=\"http://127.0.0.1:3000\"\n[[keys]]\nname=\"a\"\nzhipu_api_key=\"k\"\n",
        )
        .unwrap()
    }
}

fn print_mapping(cfg: &Config, outcome: &SyncOutcome) {
    info!("渠道映射 name → channel_id：");
    for k in &cfg.keys {
        if k.is_deprecated() {
            continue; // 设计内状态，不是解析故障——别制造「未找到」假告警
        }
        match outcome.primary.get(&k.name) {
            Some(id) => info!("  {} → {}", k.name, id),
            None => warn!("  {} → (未找到)", k.name),
        }
    }
}

async fn run_loop(
    cfg: Config,
    api: NewApiClient,
    keys: Vec<ResolvedKey>,
    relays: Option<crate::newapi::RelayTokens>,
    proxy_listener: Option<tokio::net::TcpListener>,
) -> Result<()> {    if keys.is_empty() {
        bail!("没有可用的 key（channel_id 都解析不到），无法进入切换循环");
    }
    let interval = Duration::from_secs(cfg.poll_interval_secs);
    info!(
        interval_secs = cfg.poll_interval_secs,
        throttle = cfg.throttle_threshold,
        restore = cfg.restore_threshold,
        dry_run = cfg.dry_run,
        keys = keys.len(),
        "进入切换循环（priority 单活动 key 模式）"
    );
    print_downstream_access(&cfg);

    let api = std::sync::Arc::new(api);

    // 看板 → 控制循环的命令通道（pin 等写操作）。有界(8)：满了 HTTP 侧直接 503，
    // 既不阻塞看板也不阻塞控制循环。
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel(8);

    // 状态看板：独立 task，bind 失败只降级（切换循环照常跑）。
    // 持有 api 是为了 /api/usage 能按需查任意区间的用量（不进 5 秒快照）。
    let snapshot: status::Shared = Default::default();
    if !cfg.status_addr.trim().is_empty() {
        tokio::spawn(status::serve(
            cfg.status_addr.clone(),
            snapshot.clone(),
            api.clone(),
            cmd_tx,
        ));
    }

    // RouterState：代理选路 / Panel 评分展示 / orchestrator（弃用清池）三方共用
    let router = std::sync::Arc::new(router::RouterState::new());

    // 面板循环：独立 task、独立频率（本地 new-api 可高频；智谱是外部 API 该低频）。
    // 只写面板字段，与切换循环的决策字段严格不相交。
    tokio::spawn(
        orchestrator::Panel {
            api: api.clone(),
            snapshot: snapshot.clone(),
            router: router.clone(),
            ratio: cfg.cache_pool.weekly_to_five_hour_ratio,
        }
        .run(Duration::from_secs(cfg.panel_interval_secs.max(1))),
    );

    // F4 缓存池代理：接管 base_url 端口（客户端入口不变），new-api 挪到 upstream。
    // listener 由 early_bind_proxy 在对齐**之前**绑好传入（review #2：压掉重启拒连窗口）；
    // **bind 失败/任务退出 = fail-fast**（客户端全靠这个端口——与看板 bind 失败降级相反）。
    let mut proxy_task = None;
    if cfg.cache_pool.enabled {
        let state = proxy::state_from(
            proxy::ProxyConfig {
                listen: proxy_listen_addr(&cfg)?, // 仅提示用；实际监听用传入的 listener
                upstream: cfg.upstream_base(),
                max_concurrency: cfg.cache_pool.max_concurrency,
                max_body_bytes: cfg.cache_pool.max_body_bytes,
                weekly_to_five_hour_ratio: cfg.cache_pool.weekly_to_five_hour_ratio,
                affinity_wait_ms: cfg.cache_pool.affinity_retry_wait_ms.clone(),
            },
            router.clone(),
            snapshot.clone(),
            relays,
            api.clone(), // 已认证的共享客户端（令牌刷新用）
        )?;
        let listener = match proxy_listener {
            Some(l) => l,
            None => tokio::net::TcpListener::bind(proxy_listen_addr(&cfg)?)
                .await
                .context("代理监听失败（早绑定未提供）")?,
        };
        proxy_task = Some(tokio::spawn(proxy::serve_on(state.clone(), listener)));
        // 先发布一次初始状态（含 routing 标志）——不能等首个请求才让面板知道代理在跑
        proxy::publish_stats(&state).await;
    }

    let mut orch = Orchestrator::new(cfg, api, keys, snapshot, router);
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => orch.tick().await,
            // 看板命令：先落地（tick 下发 priority + 发布快照），**再回执**。
            // 于是「HTTP 200 返回」⇔「/api/status 已能看到这次改动」，前端不会读到旧快照。
            Some(cmd) = cmd_rx.recv() => {
                let ack = orch.handle(cmd).await;
                if ack.changed() { orch.tick().await; }
                ack.send();
            }
            // 代理退出（bind 失败 / panic）= 数据面没了，整个进程 fail-fast
            res = async {
                match proxy_task.as_mut() {
                    Some(t) => t.await,
                    None => std::future::pending::<
                        std::result::Result<std::result::Result<(), anyhow::Error>, tokio::task::JoinError>,
                    >().await,
                }
            } => {
                match res {
                    Ok(Err(e)) => bail!("缓存池代理退出（bind 失败？）：{e:#}"),
                    Err(e) => bail!("缓存池代理任务异常退出：{e}"),
                    Ok(Ok(())) => bail!("缓存池代理意外正常返回"),
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("收到中断信号，退出（托管的 new-api 仍在跑，用 down 停）");
                break;
            }
        }
    }
    Ok(())
}
