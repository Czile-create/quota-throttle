mod boot;
mod config;
mod model_catalog;
mod newapi;
mod orchestrator;
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

/// 若配了 manage，就确保 new-api 原生进程在跑（不在则下载+启动）。
async fn ensure_newapi_up(cfg: &Config) -> Result<()> {
    match &cfg.new_api.manage {
        Some(m) => {
            let proc = NewApiProcess::new(m, &cfg.new_api.base_url)?;
            proc.ensure_running().await
        }
        None => {
            // 没配托管：只健康检查，起不起来是用户自己的事
            let url = format!("{}/api/status", cfg.new_api.base_url.trim_end_matches('/'));
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
                    cfg.new_api.base_url
                )
            }
        }
    }
}

fn cmd_down(cfg: &Config) -> Result<()> {
    match &cfg.new_api.manage {
        Some(m) => {
            let proc = NewApiProcess::new(m, &cfg.new_api.base_url)?;
            proc.stop()
        }
        None => {
            warn!("未配 [new_api.manage]，没有本工具托管的 new-api 可停");
            Ok(())
        }
    }
}

/// 启动对齐：渠道 sync（补建/模型对账/删弃用残留，**每次启动必跑**）+
/// 把解析到的 channel_id 落进 config（活跃 key 持有 id 的统一规则，id 优先匹配的底座）
/// + 确保 qt-proxy 中继令牌就绪（F4 逐请求路由的凭据）。
async fn align_startup(cfg: &Config, api: &NewApiClient) -> Result<SyncOutcome> {
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
    // 中继令牌是 F4 代理的依赖；F4 之前先建好底座，失败不阻断调度
    if let Err(e) = api.ensure_relay_tokens().await {
        warn!(error = %e, "qt-proxy 中继令牌未就绪（缓存池代理将不可用，不影响 priority 调度）");
    }
    Ok(outcome)
}

async fn cmd_sync(cfg: Config) -> Result<()> {
    ensure_newapi_up(&cfg).await?;
    let api = NewApiClient::new(&cfg.new_api)?;
    api.authenticate().await?;
    let outcome = align_startup(&cfg, &api).await?;
    print_mapping(&cfg, &outcome);
    print_downstream_access(&cfg);
    Ok(())
}

async fn cmd_up(cfg: Config) -> Result<()> {
    ensure_newapi_up(&cfg).await?;
    let api = NewApiClient::new(&cfg.new_api)?;
    api.authenticate().await?;
    let outcome = align_startup(&cfg, &api).await?;
    let keys = resolve_keys(&cfg, &outcome.primary);
    run_loop(cfg, api, keys).await
}

async fn cmd_run(cfg: Config) -> Result<()> {
    ensure_newapi_up(&cfg).await?;
    let api = NewApiClient::new(&cfg.new_api)?;
    api.authenticate().await?;
    // run 与 up 同样做启动对齐（run 曾只列渠道不建不对账——「config.toml 每次启动同步」）
    let outcome = align_startup(&cfg, &api).await?;
    let keys = resolve_keys(&cfg, &outcome.primary);
    run_loop(cfg, api, keys).await
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
/// 优先用 config 里显式写的 channel_id，否则按 name 从映射里取；
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
        match k.channel_id.or_else(|| primary.get(&k.name).copied()) {
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

async fn run_loop(cfg: Config, api: NewApiClient, keys: Vec<ResolvedKey>) -> Result<()> {
    if keys.is_empty() {
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

    // 面板循环：独立 task、独立频率（本地 new-api 可高频；智谱是外部 API 该低频）。
    // 只写面板字段，与切换循环的决策字段严格不相交。
    tokio::spawn(
        orchestrator::Panel {
            api: api.clone(),
            snapshot: snapshot.clone(),
        }
        .run(Duration::from_secs(cfg.panel_interval_secs.max(1))),
    );

    let mut orch = Orchestrator::new(cfg, api, keys, snapshot);
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
            _ = tokio::signal::ctrl_c() => {
                info!("收到中断信号，退出（托管的 new-api 仍在跑，用 down 停）");
                break;
            }
        }
    }
    Ok(())
}
