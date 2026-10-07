use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;

use crate::config::SymbolConfig;
use crate::db::buffer::{DataBuffer, DoubleBuffer};
use crate::db::postgresql::RawRow;
use crate::logging::feed_logger::{FeedLogger, LoggerContext};
use crate::logging::sys_logger::SysLogger;
use crate::logging::LogType;

/// Anything the inserter can write a batch of raw rows to. PostgresDBRaw is the real one;
/// the tests below use an in-memory fake so the retry / flush / shutdown behaviour can be
/// exercised without a live database.
pub trait RawSink: Send + Sync + 'static {
    fn insert_batch(&self, rows: &[RawRow]) -> impl Future<Output = Result<(), anyhow::Error>> + Send;
}

/// How hard to try before giving up on a single batch. Backoff doubles each attempt,
/// capped at `max_delay`. The default works out to roughly 1.5 minutes of retrying -
/// long enough to ride out a Postgres restart, short enough that one poisoned batch
/// can't wedge the feed forever.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            max_attempts: 8,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        }
    }
}

/// Tries `sink.insert_batch(rows)` until it succeeds or `policy.max_attempts` is used up,
/// returning the last error in that case.
pub async fn insert_with_retry<S: RawSink>(
    sink: &S,
    rows: &[RawRow],
    policy: &RetryPolicy,
    sys_log: &mut SysLogger,
    label: &str,
) -> Result<(), anyhow::Error> {
    let mut delay = policy.base_delay;
    let mut attempt = 1;
    loop {
        match sink.insert_batch(rows).await {
            Ok(()) => return Ok(()),
            Err(e) if attempt >= policy.max_attempts => return Err(e),
            Err(e) => {
                sys_log.sys_log(LogType::Warn, &format!("{} - insert of {} rows failed (attempt {} of {}): {} - retrying in {:?}", label, rows.len(), attempt, policy.max_attempts, e, delay));
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(policy.max_delay);
                attempt += 1;
            }
        }
    }
}

/// The per-symbol/feed_type DB writer: owns the DoubleBuffer, receives raw messages from the
/// WS feed task, and writes batches to the sink.
///
/// Before this existed the same loop lived inline in raw_feed.rs and `break`-ed on the first
/// failed insert - that dropped the receiver, which made the WS task's `tx.send` fail, which
/// sent it into an endless reconnect loop against Kraken. Now a failed batch is retried, and
/// if it still can't be written it's logged and dropped - the inserter itself never stops
/// until the feed closes or shutdown is requested.
pub struct Inserter<S: RawSink> {
    pub sink: S,
    pub buffer: DoubleBuffer,
    pub provider_name: String,
    pub symbol: SymbolConfig,
    pub feed_log: Arc<Mutex<FeedLogger>>,
    pub log_ctx: LoggerContext,
    pub sys_log: SysLogger,
    /// Partial buffers get written at least this often, so a quiet symbol never sits on
    /// rows in memory for hours waiting to hit the swap trigger.
    pub flush_interval: Duration,
    pub retry: RetryPolicy,
}

impl<S: RawSink> Inserter<S> {
    fn label(&self) -> String {
        format!("Kraken Raw Feed Aggregator ({} {} {})", self.provider_name, self.symbol.symbol, self.symbol.feed_type.as_str())
    }

    /// Runs until the feed's sender is dropped or `shutdown` flips. Either way whatever is
    /// still buffered gets written before returning, so a restart/deploy doesn't lose it.
    pub async fn run(mut self, mut rx: mpsc::Receiver<String>, mut shutdown: watch::Receiver<bool>) {
        let mut flush_tick = tokio::time::interval(self.flush_interval);
        flush_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        flush_tick.tick().await; // the first tick completes immediately - skip it

        loop {
            tokio::select! {
                // Checked first so a shutdown request isn't starved by a busy channel.
                biased;

                // Err here means the sender was dropped, which is just as much a reason to stop.
                _ = shutdown.changed() => {
                    // Anything the WS task already handed over is still ours to save.
                    while let Ok(msg) = rx.try_recv() {
                        self.push(msg).await;
                    }
                    self.flush_partial().await;
                    let label = self.label();
                    self.sys_log.sys_log(LogType::Info, &format!("{}: shutdown requested - remaining buffer flushed, DB inserter stopped", label));
                    return;
                }

                msg = rx.recv() => match msg {
                    Some(msg) => self.push(msg).await,
                    None => {
                        // recv() only returns None once every sender is dropped - here that means
                        // the WS feed task for this symbol/feed_type exited (hit its reconnect-attempt
                        // ceiling or a token failure). Whatever the WS-side log says is the real cause;
                        // this line just makes sure "the DB inserter went quiet too" shows up in
                        // system.log rather than the task just vanishing with no trace.
                        self.flush_partial().await;
                        let label = self.label();
                        self.sys_log.sys_log(LogType::Warn, &format!("{}: feed channel closed (WS task exited) - DB inserter shutting down", label));
                        return;
                    }
                },

                _ = flush_tick.tick() => self.flush_partial().await,
            }
        }
    }

    async fn push(&mut self, msg: String) {
        let swap_result = {
            let mut log = self.feed_log.lock().unwrap();
            self.buffer.buffer_push_and_swap(msg, self.provider_name.clone(), self.symbol.clone(), &mut log, &self.log_ctx)
        };
        if let Ok(Some(buff)) = swap_result {
            self.write(buff).await;
        }
    }

    async fn flush_partial(&mut self) {
        if let Some(buff) = self.buffer.take_partial() {
            self.write(buff).await;
        }
    }

    /// Never fails from the caller's point of view - a batch that can't be written is logged
    /// and dropped so the inserter keeps serving every batch after it.
    async fn write(&mut self, buff: DataBuffer) {
        let label = self.label();
        let batch_size = buff.capacity_check();

        let raw_data = match RawRow::data_buff_to_rawrows(buff) {
            Ok(raw_data) => raw_data,
            Err(e) => {
                self.sys_log.sys_log(LogType::Error, &format!("{} failed to convert a batch of {} raw rows: {} - batch dropped, inserter continues", label, batch_size, e));
                return;
            }
        };

        // TODO(v1.1.0 metrics): helix_db_insert_errors_total{provider,symbol,feed_type}
        // counter + a batch-size histogram around this call.
        if let Err(e) = insert_with_retry(&self.sink, &raw_data, &self.retry, &mut self.sys_log, &label).await {
            self.sys_log.sys_log(LogType::Error, &format!("{} gave up inserting a batch of {} rows after {} attempts: {} - batch dropped, inserter continues", label, batch_size, self.retry.max_attempts, e));
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config::{FeedType, Market};
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

    /// A unique log file path under the OS temp dir, so tests never write into ./logs.
    pub(crate) fn temp_log_path(name: &str) -> String {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir()
            .join(format!("helixfeed-test-{}-{}-{}.log", std::process::id(), name, n))
            .to_string_lossy()
            .into_owned()
    }

    /// In-memory sink: fails the first `fail_times` calls, then stores every batch it gets.
    #[derive(Clone, Default)]
    struct FakeSink {
        fail_times: Arc<AtomicU32>,
        calls: Arc<AtomicU32>,
        batches: Arc<Mutex<Vec<Vec<serde_json::Value>>>>,
    }

    impl FakeSink {
        fn failing(times: u32) -> Self {
            let sink = FakeSink::default();
            sink.fail_times.store(times, Ordering::SeqCst);
            sink
        }

        fn stored_rows(&self) -> Vec<serde_json::Value> {
            self.batches.lock().unwrap().iter().flatten().cloned().collect()
        }
    }

    impl RawSink for FakeSink {
        async fn insert_batch(&self, rows: &[RawRow]) -> Result<(), anyhow::Error> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let remaining = self.fail_times.load(Ordering::SeqCst);
            if remaining > 0 {
                self.fail_times.store(remaining - 1, Ordering::SeqCst);
                anyhow::bail!("simulated database outage");
            }
            self.batches.lock().unwrap().push(rows.iter().map(|r| r.raw_json.clone()).collect());
            Ok(())
        }
    }

    fn fast_retry(max_attempts: u32) -> RetryPolicy {
        RetryPolicy { max_attempts, base_delay: Duration::from_millis(1), max_delay: Duration::from_millis(5) }
    }

    fn sys_log() -> SysLogger {
        SysLogger::new(temp_log_path("sys"), "test".to_string()).unwrap()
    }

    fn inserter(sink: FakeSink, capacity: usize, flush_interval: Duration, retry: RetryPolicy) -> Inserter<FakeSink> {
        let symbol = SymbolConfig { markets: Market::Crypto, symbol: "BTC/USD".to_string(), feed_type: FeedType::Trades };
        Inserter {
            sink,
            buffer: DoubleBuffer::new(capacity, 1.0),
            provider_name: "kraken".to_string(),
            log_ctx: LoggerContext::new(symbol.symbol.clone(), symbol.feed_type),
            symbol,
            feed_log: Arc::new(Mutex::new(FeedLogger::new(temp_log_path("feed"), "kraken".to_string()).unwrap())),
            sys_log: sys_log(),
            flush_interval,
            retry,
        }
    }

    fn msg(n: u32) -> String {
        format!(r#"{{"channel":"trade","n":{}}}"#, n)
    }

    fn row(n: u32) -> RawRow {
        RawRow::new(chrono::Utc::now(), "kraken".to_string(), FeedType::Trades, "BTC/USD".to_string(), serde_json::json!({ "n": n }))
    }

    /// Polls until the sink holds `count` rows or a second passes.
    async fn wait_for_rows(sink: &FakeSink, count: usize) -> Vec<serde_json::Value> {
        for _ in 0..100 {
            let rows = sink.stored_rows();
            if rows.len() >= count {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        sink.stored_rows()
    }

    #[tokio::test]
    async fn retry_recovers_after_transient_failures() {
        let sink = FakeSink::failing(2);
        let result = insert_with_retry(&sink, &[row(1)], &fast_retry(5), &mut sys_log(), "test").await;

        assert!(result.is_ok());
        assert_eq!(sink.calls.load(Ordering::SeqCst), 3, "2 failures + 1 success");
        assert_eq!(sink.stored_rows().len(), 1);
    }

    #[tokio::test]
    async fn retry_gives_up_after_max_attempts() {
        let sink = FakeSink::failing(u32::MAX);
        let result = insert_with_retry(&sink, &[row(1)], &fast_retry(3), &mut sys_log(), "test").await;

        assert!(result.is_err());
        assert_eq!(sink.calls.load(Ordering::SeqCst), 3);
    }

    /// Regression test for the original bug: one failed batch used to permanently kill the
    /// inserter (and drop the receiver). Now the bad batch is dropped and the next one lands.
    #[tokio::test]
    async fn inserter_keeps_running_after_a_batch_fails() {
        // Both attempts for the first batch fail; everything after succeeds.
        let sink = FakeSink::failing(2);
        let (tx, rx) = mpsc::channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(inserter(sink.clone(), 2, Duration::from_secs(3600), fast_retry(2)).run(rx, shutdown_rx));

        tx.send(msg(1)).await.unwrap();
        tx.send(msg(2)).await.unwrap(); // swap -> batch [1,2] fails twice and is dropped
        tx.send(msg(3)).await.unwrap();
        tx.send(msg(4)).await.unwrap(); // swap -> batch [3,4] succeeds

        let rows = wait_for_rows(&sink, 2).await;
        assert_eq!(rows, vec![serde_json::json!({"channel":"trade","n":3}), serde_json::json!({"channel":"trade","n":4})]);
        assert!(!tx.is_closed(), "inserter must still be alive and receiving");
    }

    #[tokio::test]
    async fn partial_buffer_is_flushed_on_interval() {
        let sink = FakeSink::default();
        let (tx, rx) = mpsc::channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        // Capacity 100 means these 3 messages would never trigger a swap on their own.
        tokio::spawn(inserter(sink.clone(), 100, Duration::from_millis(50), fast_retry(1)).run(rx, shutdown_rx));

        for n in 1..=3 {
            tx.send(msg(n)).await.unwrap();
        }

        assert_eq!(wait_for_rows(&sink, 3).await.len(), 3);
    }

    #[tokio::test]
    async fn buffer_is_flushed_on_shutdown() {
        let sink = FakeSink::default();
        let (tx, rx) = mpsc::channel(16);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(inserter(sink.clone(), 100, Duration::from_secs(3600), fast_retry(1)).run(rx, shutdown_rx));

        for n in 1..=3 {
            tx.send(msg(n)).await.unwrap();
        }
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(1), handle).await.expect("inserter should stop on shutdown").unwrap();
        assert_eq!(sink.stored_rows().len(), 3, "nothing buffered may be lost on shutdown");
    }

    #[tokio::test]
    async fn buffer_is_flushed_when_feed_closes() {
        let sink = FakeSink::default();
        let (tx, rx) = mpsc::channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(inserter(sink.clone(), 100, Duration::from_secs(3600), fast_retry(1)).run(rx, shutdown_rx));

        tx.send(msg(1)).await.unwrap();
        drop(tx);

        tokio::time::timeout(Duration::from_secs(1), handle).await.expect("inserter should stop when the feed closes").unwrap();
        assert_eq!(sink.stored_rows().len(), 1);
    }
}
