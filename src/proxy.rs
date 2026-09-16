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
}

/// 代理共享状态。
#[derive(Clone)]
pub struct ProxyState {
    cfg: ProxyConfig,
    http: reqwest::Client,
    sem: Arc<Semaphore>,
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
pub async fn serve_on(state: ProxyState, mut listener: TcpListener) -> Result<()> {
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
            // 流尽才返回，所以「连接结束 = 响应完成」，慢客户端占坑即背压
            let _permit = match state.sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return, // 信号量关闭 = 进程退出
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

/// 单请求处理：F4a = 全量透传；F4b 会在这里按 path 分流到评分路由。
async fn handle(req: Request<Incoming>, state: &ProxyState) -> Response<BoxBody> {
    passthrough(req, state).await
}

/// 纯透传：method/path/query/头（剥 hop-by-hop）/体（流式）→ upstream，响应流式回传。
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
    // 请求体：流式转发（透传路径不落缓冲——不需要也不该缓存几 MB 的体）
    let body_stream = req.into_body().into_data_stream();

    let mut rb = state.http.request(method, &url);
    for (k, v) in headers {
        rb = rb.header(k, v);
    }
    let body = reqwest::Body::wrap_stream(body_stream);
    let resp = match rb.body(body).send().await {
        Ok(r) => r,
        Err(e) => {
            // 上游连接失败：代理是唯一入口，如实报 502（不带任何鉴权信息）
            warn!(error = %e, "转发到 new-api 失败");
            return error_response(StatusCode::BAD_GATEWAY, "上游 new-api 不可达");
        }
    };

    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut builder = Response::builder().status(status);
    for (k, v) in resp.headers() {
        if is_hop_by_hop(k.as_str()) {
            continue;
        }
        // reqwest http0.2 头值 → hyper http1 头值，按字节转（非法字节的头丢弃）
        if let (Ok(name), Ok(value)) = (
            hyper::header::HeaderName::from_bytes(k.as_str().as_bytes()),
            hyper::header::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            builder = builder.header(name, value);
        }
    }
    // StreamBody 的项是 Frame<D>（不是裸字节）——用适配器包；Box::pin 满足 Unpin
    let framed = FrameMap {
        inner: Box::pin(resp.bytes_stream()),
    };
    match builder.body(http_body_util::StreamBody::new(framed).boxed()) {
        Ok(r) => r,
        Err(_) => error_response(StatusCode::BAD_GATEWAY, "构造上游响应失败"),
    }
}

/// 从 config 构造代理状态（main 接线用）。
pub fn state_from(cfg: ProxyConfig) -> Result<ProxyState> {
    // 上游 client：connect 有超时、**总时长无超时**（SSE 长流），不开压缩（红线）
    let http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .pool_idle_timeout(Duration::from_secs(90))
        .build()
        .context("构建代理上游 client 失败")?;
    Ok(ProxyState {
        sem: Arc::new(Semaphore::new(cfg.max_concurrency.max(1))),
        cfg,
        http,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let state = state_from(ProxyConfig {
            listen: String::new(),
            upstream: format!("http://{upstream_addr}"),
            max_concurrency: 2,
        })
        .unwrap();
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
        assert!(
            !req_text.to_lowercase().contains("x-not-pass"),
        );
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
        let state = state_from(ProxyConfig {
            listen: String::new(),
            upstream: format!("http://{dead_addr}"),
            max_concurrency: 1,
        })
        .unwrap();
        tokio::spawn(serve_on(state, listener));

        let resp = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{proxy_port}/"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 502);
    }
}
