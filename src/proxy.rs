//! 缓存池代理（F4）：接管 base_url 端口的流式反向代理（hyper v1，仅 http1）。
//!
//! 拓扑（用户拍板：代理接管 3000，客户端/docker 零改动）：
//! ```text
//! opencode / Claude Code ──> 代理(:3000)
//!                             ├─ /v1/chat/completions、/v1/messages → 逐请求路由（F4b）
//!                             └─ 其余路径（/、/api/* 管理界面等）→ 原样透传 new-api(:13000)
//! ```
//!
//! 为什么新写而不用 status.rs 的手写 HTTP：那边 8KiB 体上限、Connection: close、
//! 无 chunked/SSE——对 LLM 长流与大请求体都是致命的。代理是数据面，用 hyper。
//!
//! 红线：
//! · **bind 失败必须 fail-fast**（客户端全靠这个端口；与看板 bind 失败降级相反）
//! · **不设总 timeout**（SSE 长流会中道断），只有 connect_timeout + 读头超时
//! · reqwest 不开压缩 feature（否则 bytes_stream 解压体与透传的 Content-Encoding 头不一致）
//! · 任何日志不得打印 Authorization / x-api-key（鉴权泄漏）
//! · Expect: 100-continue 由 hyper 自动处理（handler 首次 poll body 时回 100）
//!
//! 类型注意：reqwest 0.11 内部是 http 0.2，hyper 1 用 http 1.x——**两套 Header/StatusCode
//! 不能直传**，一律经 `as_str()`/`as_bytes()`/`from_u16` 转换。
//!
//! F4a = 纯透传（本文件的全部）；F4b = 在 handle() 的 LLM 路径上叠加评分路由。

use anyhow::{Context, Result};
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{HeaderMap, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::newapi::RelayTokens;
use crate::router::{self, Choice, RouterState};

/// 响应体统一形态：数据帧流或整块（错误页），错误类型统一 io::Error
/// （hyper 1.11 起没有公开的错误构造器，自定义错误类型用 io::Error 最省事）。
pub type BoxBody = http_body_util::combinators::BoxBody<Bytes, std::io::Error>;

/// hop-by-hop 头（双向剥；请求向另剥 Host/Content-Length——reqwest 重算）。
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn full_body(msg: &str) -> BoxBody {
    http_body_util::Full::new(Bytes::copy_from_slice(msg.as_bytes()))
        .map_err(|never| match never {})
        .boxed()
}

fn error_response(status: StatusCode, msg: &str) -> Response<BoxBody> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .body(full_body(&format!("{{\"error\":\"{msg}\"}}")))
        .unwrap()
}

/// 转发用的头集合（hyper http1 → reqwest http0.2，按名字/字节转换）。
fn forward_request_headers(src: &HeaderMap) -> Vec<(String, reqwest::header::HeaderValue)> {
    src.iter()
        .filter(|(n, _)| {
            let n = n.as_str();
            !is_hop_by_hop(n) && n != "host" && n != "content-length"
        })
        .filter_map(|(n, v)| {
            let hv = reqwest::header::HeaderValue::from_bytes(v.as_bytes()).ok()?;
            Some((n.to_string(), hv))
        })
        .collect()
}

/// `Stream<Item=Result<Bytes, reqwest::Error>>` → `Stream<Item=Result<Frame<Bytes>, io::Error>>`
/// （http-body-util 的 StreamBody 吃 Frame；不引 futures-util 就为个 map）
struct FrameMap<S> {
    inner: std::pin::Pin<Box<S>>,
}
impl<S> futures_core::Stream for FrameMap<S>
where
    S: futures_core::Stream<Item = std::result::Result<Bytes, reqwest::Error>>,
{
    type Item = std::result::Result<hyper::body::Frame<Bytes>, std::io::Error>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(b))) => Poll::Ready(Some(Ok(hyper::body::Frame::data(b)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                e,
            )))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// 代理运行参数（main 从 config 派生后传入）
#[derive(Clone)]
pub struct ProxyConfig {
    /// 监听地址（从 base_url 的 host:port 解析）
    pub listen: String,
    /// 内部 new-api 地址（= cfg.upstream_base()）
    pub upstream: String,
    pub max_concurrency: usize,
    pub max_body_bytes: usize,
    /// 周额度:5h 额度比值（评分用）
    pub weekly_to_five_hour_ratio: f64,
}

/// 代理共享状态。
#[derive(Clone)]
pub struct ProxyState {
    cfg: ProxyConfig,
    http: reqwest::Client,
    sem: Arc<Semaphore>,
    /// 路由状态（缓存池/负载/冷却/统计；弃用时 orchestrator 按 channel_id 清）
    pub router: Arc<RouterState>,
    /// 共享快照（只读 eligible/pct + 写 cache_pool 面板字段——与决策/面板字段不相交）
    pub snapshot: crate::status::Shared,
    /// 中继令牌（None = 未就绪：LLM 路径整体降级透传，不逐请求路由）
    pub relays: Option<RelayTokens>,
}

/// 起代理（**永不正常返回**；bind 失败 / accept 循环崩溃都走 Err → 调用方 fail-fast）。
pub async fn serve(state: ProxyState) -> Result<()> {
    let listener = TcpListener::bind(&state.cfg.listen).await.with_context(|| {
        format!(
            "代理监听 {} 失败（端口被占？开 cache_pool 前先停掉还在 3000 上的旧 new-api：`quota-throttle down`）",
            state.cfg.listen
        )
    })?;
    info!(listen = %state.cfg.listen, upstream = %state.cfg.upstream, "缓存池代理已上线");
    serve_on(state, listener).await
}

/// 在既有 listener 上跑 accept 循环（serve 拆出来是为了集成测试能绑 127.0.0.1:0）。
pub async fn serve_on(state: ProxyState, listener: TcpListener) -> Result<()> {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                // 学 status.rs：单连接错误不退出（accept 错误多为瞬态）
                warn!(error = %e, "accept 失败（继续）");
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            // 连接级信号量：permit 活到连接任务结束——serve_connection 会等到响应体
            // 流尽才返回，所以「连接结束 = 响应完成」，慢客户端占坑即背压。
            // 坑耗尽时**限时等待**（30s），超时写裸 503 关连接——绝不能让客户端
            // 挂死在一个「连上了但永远没响应」的 TCP 上（keep-alive 空闲连接也占坑，
            // 管理界面多开几个标签页就可能吃满）。
            let _permit = match tokio::time::timeout(
                Duration::from_secs(30),
                state.sem.clone().acquire_owned(),
            )
            .await
            {
                Ok(Ok(p)) => p,
                Ok(Err(_)) => return, // 信号量关闭 = 进程退出
                Err(_) => {
                    use tokio::io::AsyncWriteExt;
                    let body = "{\"error\":\"代理并发已达上限（max_concurrency），请稍后重试\"}"
                        .as_bytes()
                        .to_vec();
                    let head = format!(
                        "HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let mut s = stream;
                    let _ = s.write_all(head.as_bytes()).await;
                    let _ = s.write_all(&body).await;
                    return;
                }
            };
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req| {
                let state = state.clone();
                async move { Ok::<_, std::convert::Infallible>(handle(req, &state).await) }
            });
            if let Err(e) = http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::default()) // 超时特性需要显式 timer
                .header_read_timeout(Duration::from_secs(60))
                .serve_connection(io, svc)
                .await
            {
                // 客户端中途断开等常态噪音，只记 debug
                debug!(error = %e, "连接处理结束（非正常）");
            }
        });
    }
}

/// 单请求处理：LLM 路径（POST /v1/chat/completions、/v1/messages）走逐请求评分路由；
/// 其余全量透传。中继令牌未就绪或快照无数据时 LLM 路径也降级透传（不 503——
/// priority 阶梯仍是兜底，启动窗口 60s 内不该拒绝全部客户端）。
async fn handle(req: Request<Incoming>, state: &ProxyState) -> Response<BoxBody> {
    let path = req.uri().path().to_string();
    let is_llm = req.method() == hyper::Method::POST
        && (path == "/v1/chat/completions" || path == "/v1/messages");
    if is_llm && state.relays.is_some() {
        route_llm(req, &path, state).await
    } else {
        passthrough(req, state).await
    }
}

/// 换渠道重试的状态码矩阵（PR review 修正）：
/// · 429 额度墙；502/503/504 网关类；**500**（specific-channel 路径 new-api 把上游
///   转发失败包成 500，代理是唯一能补 priority 阶梯的层）
/// · **400**（指定渠道不存在——如弃用后 eligible 快照 60s 滞后期）与 **403**（渠道被
///   禁用/自动封禁）也是**渠道级**失败，换渠道可修——403 另有一义「用户额度不足」
///   （user 级、换渠道无用），重试两次的浪费上限可接受（F3 已把该情形压到近零）
/// · 不重试 401（令牌问题）、404/422（请求本身错）
fn retryable(code: u16) -> bool {
    matches!(code, 400 | 403 | 429 | 500 | 502 | 503 | 504)
}

/// LLM 路径的逐请求路由（F4b 主体）。
/// 取舍（设计定稿）：换渠道重试意味着同一请求可能被两个渠道各扣一次费
/// （上游 5xx 但实际已部分计费）——上限 2 次重试可接受，记 info 日志。
async fn route_llm(req: Request<Incoming>, path: &str, state: &ProxyState) -> Response<BoxBody> {
    // 最小鉴权：Authorization / x-api-key 至少一个非空（信任边界=回环，文档写明；
    // 客户端 token 本身被丢弃——转发时覆写为中继令牌后缀）
    let has_auth = ["authorization", "x-api-key"].iter().any(|h| {
        req.headers()
            .get(*h)
            .map(|v| !v.is_empty())
            .unwrap_or(false)
    });
    if !has_auth {
        return error_response(StatusCode::UNAUTHORIZED, "缺少鉴权头（Authorization / x-api-key）");
    }

    // 聚合请求体（cache_key 计算 + 重试复用；Limited 防 OOM 先于防 413）
    let (parts, body) = req.into_parts();
    let limited = http_body_util::Limited::new(body, state.cfg.max_body_bytes);
    let bytes = match limited.collect().await {
        Ok(c) => c.to_bytes(),
        Err(_) => return error_response(StatusCode::PAYLOAD_TOO_LARGE, "请求体超过上限"),
    };
    let key = router::cache_key(path, &bytes);
    let pq = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());

    // 快照最小集（读锁内提取，不克隆面板字段）
    let view = {
        let g = state.snapshot.read().unwrap_or_else(|e| e.into_inner());
        router::RouteView::from_snap(&g)
    };
    let relays = state.relays.as_ref().expect("handle 已判 is_some");
    let relay_key = if path == "/v1/messages" {
        &relays.claude
    } else {
        &relays.openai
    };
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    // 首次才查缓存池（hit/miss 统计口径按请求计，不按尝试计）
    let hint = if let Some(k) = key {
        state.router.lookup(k, &view.eligible)
    } else {
        None
    };

    const MAX_ATTEMPTS: usize = 3; // 1 次 + 至多 2 次换渠道重试
    let mut tried: Vec<i64> = Vec::new();
    for attempt in 0..MAX_ATTEMPTS {
        let hint_now = if attempt == 0 { hint } else { None };
        let choice = router::choose(&view, &state.router, &tried, hint_now, now_ms, state.cfg.weekly_to_five_hour_ratio);
        match choice {
            // 无数据（刚启动）/ 候选耗尽 → 降级透传：原样转发（客户端自己的 token +
            // new-api 的 priority 阶梯兜底），不记池
            Choice::NoData | Choice::NoEligible => {
                if attempt == 0 {
                    info!(path = %path, "路由降级透传（快照无数据或候选耗尽）");
                }
                return match forward_once(&parts, &bytes, &pq, None, state).await {
                    Ok(resp) => upstream_to_response(resp),
                    Err(e) => {
                        warn!(error = %e, "降级透传转发失败");
                        error_response(StatusCode::BAD_GATEWAY, "上游 new-api 不可达")
                    }
                };
            }
            Choice::Channel { id, via } => {
                // 覆写鉴权：new-api 原生「sk-<key>-<channelId>」逐请求指定渠道（N1 机制）；
                // 客户端的 Authorization/x-api-key 一并剥掉
                let auth = format!("Bearer sk-{relay_key}-{id}");
                match forward_once(&parts, &bytes, &pq, Some(&auth), state).await {
                    Ok(resp) if retryable(resp.status().as_u16()) => {
                        // 任何被换道绕开的失败都记冷却（不只 429——持续 500 的渠道下一请求
                        // 不该立刻又被缓存命中选中）
                        state.router.cool(id);
                        if attempt + 1 == MAX_ATTEMPTS {
                            // 最后一次：把可重试响应原样交给客户端（换无可换）。
                            // 不把池钉在这个刚失败的渠道上（record None 只计负载）。
                            state.router.record(None, id);
                            publish_stats(state);
                            return upstream_to_response(resp);
                        }
                        debug!(channel = id, status = resp.status().as_u16(), "可重试响应，换渠道");
                        let _ = resp.bytes().await; // 丢弃响应体（重试前必须排干连接）
                        tried.push(id);
                    }
                    Ok(resp) => {
                        state.router.record(key, id); // 重试成功也把池指向新渠道
                        publish_stats(state);
                        debug!(channel = id, via = ?via, "已路由");
                        return upstream_to_response(resp);
                    }
                    Err(e) => {
                        // 连接层错误（upstream 拒连/断流）：换渠道
                        warn!(channel = id, error = %e, "转发失败，换渠道重试");
                        tried.push(id);
                    }
                }
            }
        }
    }
    error_response(StatusCode::BAD_GATEWAY, "全部候选渠道转发失败")
}

/// 发一次请求到 upstream。auth = Some 时覆写鉴权头（LLM 路由）；
/// None = 透传原样头（降级路径）。body 为聚合好的 Bytes（重试零拷贝复用）。
async fn forward_once(
    parts: &hyper::http::request::Parts,
    bytes: &Bytes,
    pq: &str,
    auth: Option<&str>,
    state: &ProxyState,
) -> Result<reqwest::Response> {
    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes())
        .unwrap_or(reqwest::Method::POST);
    let url = format!("{}{}", state.cfg.upstream, pq);
    let mut rb = state.http.request(method, &url);
    for (k, v) in forward_request_headers(&parts.headers) {
        if auth.is_some() && (k == "authorization" || k == "x-api-key") {
            continue; // 路由模式：客户端的鉴权头一律换成中继令牌
        }
        rb = rb.header(k, v);
    }
    if let Some(auth) = auth {
        rb = rb.header("authorization", auth);
    }
    rb.body(bytes.clone())
        .send()
        .await
        .map_err(anyhow::Error::from)
}

/// reqwest 响应 → hyper 响应（状态/头剥 hop-by-hop/体流式回传）
fn upstream_to_response(resp: reqwest::Response) -> Response<BoxBody> {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);
    for (k, v) in resp.headers() {
        if is_hop_by_hop(k.as_str()) {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            hyper::header::HeaderName::from_bytes(k.as_str().as_bytes()),
            hyper::header::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            builder = builder.header(name, value);
        }
    }
    let framed = FrameMap {
        inner: Box::pin(resp.bytes_stream()),
    };
    builder
        .body(http_body_util::StreamBody::new(framed).boxed())
        .unwrap_or_else(|_| error_response(StatusCode::BAD_GATEWAY, "构造上游响应失败"))
}

/// 把路由统计写进快照（只写 cache_pool 一个面板字段，与决策/面板循环不相交）。
/// routing=false（令牌未就绪 → 全量透传）也要发布——「代理在跑但没路由」
/// 必须在面板上可见，否则与「代理没开」不可区分（PR review #11）。
pub fn publish_stats(state: &ProxyState) {
    let st = state.router.stats();
    let in_flight = state
        .cfg
        .max_concurrency
        .saturating_sub(state.sem.available_permits());
    crate::status::update(&state.snapshot, |s| {
        s.cache_pool = Some(crate::status::CachePoolStatus {
            enabled: true,
            routing: state.relays.is_some(),
            entries: st.entries,
            hits: st.hits,
            misses: st.misses,
            in_flight,
            per_channel: st
                .routed_per_channel
                .iter()
                .map(|(id, c)| crate::status::CachePoolChan {
                    channel_id: *id,
                    count: *c,
                })
                .collect(),
        });
    });
}

/// 纯透传：method/path/query/头（剥 hop-by-hop）/体（流式，不落缓冲）→ upstream，
/// 响应流式回传。
async fn passthrough(req: Request<Incoming>, state: &ProxyState) -> Response<BoxBody> {
    let method = reqwest::Method::from_bytes(req.method().as_str().as_bytes())
        .unwrap_or(reqwest::Method::GET);
    let pq = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let url = format!("{}{}", state.cfg.upstream, pq);
    let headers = forward_request_headers(&req.headers());
    let body_stream = req.into_body().into_data_stream();

    let mut rb = state.http.request(method, &url);
    for (k, v) in headers {
        rb = rb.header(k, v);
    }
    let resp = match rb.body(reqwest::Body::wrap_stream(body_stream)).send().await {
        Ok(r) => r,
        Err(e) => {
            // 上游连接失败：代理是唯一入口，如实报 502（不带任何鉴权信息）
            warn!(error = %e, "转发到 new-api 失败");
            return error_response(StatusCode::BAD_GATEWAY, "上游 new-api 不可达");
        }
    };
    upstream_to_response(resp)
}

/// 从 config 构造代理状态（main 接线用）。
pub fn state_from(
    cfg: ProxyConfig,
    router: Arc<RouterState>,
    snapshot: crate::status::Shared,
    relays: Option<RelayTokens>,
) -> Result<ProxyState> {
    // 上游 client：connect 有超时、**总时长无超时**（SSE 长流），不开压缩（红线），
    // **禁跟随重定向**——反向代理必须把 3xx 原样交给客户端（reqwest 跟随 301/302/303
    // 会把 LLM POST 变 GET 丢 body，跨 host 还会剥掉中继鉴权头）
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
        .context("构建代理上游 client 失败")?;
    Ok(ProxyState {
        sem: Arc::new(Semaphore::new(cfg.max_concurrency.max(1))),
        cfg,
        http,
        router,
        snapshot,
        relays,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::newapi::RelayTokens;

    fn test_state(upstream: String, relays: Option<RelayTokens>) -> ProxyState {
        state_from(
            ProxyConfig {
                listen: String::new(),
                upstream,
                max_concurrency: 4,
                max_body_bytes: 1024 * 1024,
                weekly_to_five_hour_ratio: 15.5 / 3.5,
            },
            Arc::new(RouterState::new()),
            Default::default(),
            relays,
        )
        .unwrap()
    }

    fn relays() -> RelayTokens {
        RelayTokens {
            openai: "testtokenopenai".into(),
            claude: "testtokenclaude".into(),
        }
    }

    /// 冒烟集成测试：mock 上游（裸 TcpListener 回固定响应）+ 代理 → 客户端经代理拿到
    /// 响应；同时验证请求头透传与 hop-by-hop 剥离（Connection 不该到上游）。
    #[tokio::test]
    async fn 透传_请求路径头与响应体() {
        // —— mock 上游：读请求、回 200 + 固定体，并把收到的请求头存下来 ——
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let upstream_task = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut sock, _) = upstream.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = sock.read(&mut buf).await.unwrap();
            let req_text = String::from_utf8_lossy(&buf[..n]).to_string();
            let resp = "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 5\r\n\r\nhello";
            sock.write_all(resp.as_bytes()).await.unwrap();
            req_text
        });

        // —— 代理：serve_on 在随机端口 ——
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_port = listener.local_addr().unwrap().port();
        let state = test_state(format!("http://{upstream_addr}"), None);
        tokio::spawn(serve_on(state, listener));

        // —— 客户端：经代理打过去 ——
        let client = reqwest::Client::new();
        let resp = client
            .get(format!("http://127.0.0.1:{proxy_port}/v1/chat/completions?x=1"))
            .header("x-test-header", "42")
            .header("connection", "close") // hop-by-hop，不该透传到上游
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        assert_eq!(resp.text().await.unwrap(), "hello");

        let req_text = upstream_task.await.unwrap();
        assert!(
            req_text.contains("GET /v1/chat/completions?x=1 HTTP/1.1"),
            "path+query 应原样透传：{req_text}"
        );
        assert!(req_text.contains("x-test-header: 42"), "自定义头应透传：{req_text}");
    }

    /// 上游不可达 → 502（代理是唯一入口，如实报错）
    #[tokio::test]
    async fn 上游挂了_回502() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_port = listener.local_addr().unwrap().port();
        // 占一个端口然后立刻关掉 → 连接必拒
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);
        let state = test_state(format!("http://{dead_addr}"), None);
        tokio::spawn(serve_on(state, listener));

        let resp = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{proxy_port}/"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 502);
    }

    /// F4b 路由：LLM 请求经中继令牌后缀指定渠道；429 自动换渠道重试。
    /// mock 上游按「请求的 Authorization 尾号」回 429（渠道 1）/200（渠道 2）。
    #[tokio::test]
    async fn 路由_429换渠道重试_中继令牌后缀生效() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen_c = seen.clone();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            loop {
                let Ok((mut sock, _)) = upstream.accept().await else { break };
                let seen_c = seen_c.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16384];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    let auth = req
                        .lines()
                        .find(|l| l.to_lowercase().starts_with("authorization:"))
                        .unwrap_or_default()
                        .to_string();
                    seen_c.lock().unwrap().push(auth.clone());
                    // 渠道后缀 -1 → 429；-2 → 200（模拟一把烧尽的 key 和一把有余量的）
                    let resp = if auth.ends_with("-1") {
                        "HTTP/1.1 429 Too Many Requests\r\ncontent-length: 0\r\n\r\n"
                    } else {
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\n\r\nok"
                    };
                    sock.write_all(resp.as_bytes()).await.ok();
                });
            }
        });

        // 代理 + 中继令牌
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_port = listener.local_addr().unwrap().port();
        let state = test_state(format!("http://{upstream_addr}"), Some(relays()));
        // 预置快照：渠道 1、2 都合格（routing 视图从这里来）
        {
            let snap = &state.snapshot;
            crate::status::update(snap, |s| {
                s.eligible = vec![1, 2];
                s.keys = vec![
                    crate::status::KeyStatus {
                        channel_id: 1,
                        five_hour_pct: Some(10.0),
                        weekly_pct: Some(10.0),
                        max_pct: Some(10.0),
                        ..Default::default()
                    },
                    crate::status::KeyStatus {
                        channel_id: 2,
                        five_hour_pct: Some(20.0),
                        weekly_pct: Some(20.0),
                        max_pct: Some(20.0),
                        ..Default::default()
                    },
                ];
            });
        }
        tokio::spawn(serve_on(state.clone(), listener));

        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{proxy_port}/v1/chat/completions"))
            .header("authorization", "Bearer client-token")
            .json(&serde_json::json!({
                "model": "glm-5.2",
                "messages": [{"role": "user", "content": "hi"}]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "429 后应换到渠道 2 成功");

        // 两次上游请求都带中继令牌后缀（sk-testtokenopenai-<channelId>），且渠道不同
        let auths = seen.lock().unwrap().clone();
        assert!(auths.len() >= 2, "应发生至少一次重试：{auths:?}");
        assert!(
            auths.iter().all(|a| a.contains("Bearer sk-testtokenopenai-")),
            "鉴权应为中继令牌后缀形式：{auths:?}"
        );
        assert!(
            auths.iter().any(|a| a.ends_with("-2")),
            "重试应换到渠道 2：{auths:?}"
        );
        assert_eq!(state.router.stats().routed_per_channel.len() >= 1, true);
    }

    /// LLM 请求缺鉴权头 → 401（最小鉴权校验）
    #[tokio::test]
    async fn 路由_缺鉴权头401() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_port = listener.local_addr().unwrap().port();
        let state = test_state("http://127.0.0.1:1".into(), Some(relays()));
        tokio::spawn(serve_on(state, listener));
        let resp = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{proxy_port}/v1/chat/completions"))
            .json(&serde_json::json!({"model": "m", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 401);
    }
}
