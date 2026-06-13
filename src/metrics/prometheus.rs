
use lazy_static::lazy_static; 
use prometheus::{CounterVec, GaugeVec, Gauge, Opts, Registry, TextEncoder, Encoder};
use hyper::{Request, Response};
use hyper::body::{Bytes, Incoming}; 
use hyper::service::service_fn;
use http_body_util::Full;
use std::convert::Infallible;
use std::net::SocketAddr;


lazy_static!{
    pub static ref REGISTRY: Registry = Registry::new();

    pub static ref FEEDS_RUNNING: Gauge = Gauge::new(
        "helix_feeds_running",
        "Number Of Feed tasks currently active"

    ).unwrap();

    pub static ref TOTAL_MESSAGES: CounterVec = CounterVec::new(
        Opts::new("helix_messages_total","Total messages received, labeled by `provider`, `symbol`, `feed_type`"),
        &["provider", "symbol", "feed_type"]
    ).unwrap();

    pub static ref BUFFER_SWAPS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("helix_buffer_swaps_total", "Total buffer swaps, labeled by `provider`, `symbol`, `feed_type`"),
        &["provider", "symbol", "feed_type"]
    ).unwrap();
    
    pub static ref RECONNECT_ATTEMPTS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("helix_reconnect_attempts_total", "Total reconnect attempts, labeled by `provider`, `symbol`, `feed_type`"),
        &["provider", "symbol", "feed_type"]
    ).unwrap();

    pub static ref FEED_UP: GaugeVec = GaugeVec::new(
        Opts::new("helix_feed_up", "1 if feed is running, 0 if stopped"),
        &["provider", "symbol", "feed_type"]
    ).unwrap();


}

pub fn register_metrics() {
    REGISTRY.register(Box::new(FEEDS_RUNNING.clone())).unwrap();
    REGISTRY.register(Box::new(TOTAL_MESSAGES.clone())).unwrap();
    REGISTRY.register(Box::new(BUFFER_SWAPS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(RECONNECT_ATTEMPTS_TOTAL.clone())).unwrap();
    REGISTRY.register(Box::new(FEED_UP.clone())).unwrap();
}

pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn serve_metrics( _req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible>{
    let metrics = gather_metrics(); 

    Ok(Response::new(Full::new(Bytes::from(metrics))))
}

pub async fn start_metrics_server() {
    let addr: SocketAddr = ([0,0,0,0], 9091).into();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    hyper_util::rt::TokioIo::new(stream),
                    service_fn(serve_metrics)
                )
                .await
                .unwrap();
        });
    }
}