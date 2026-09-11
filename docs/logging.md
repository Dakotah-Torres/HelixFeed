# HelixFeed Logging

Reference map of every log line in the codebase: where it is, what level it's at, and why
it's placed there. Written after the BTC/USD Orders outage on 2026-09-10, where the feed
went silent at 21:19:16 and nothing in the logs said why — `stream.next().await` was just
blocked forever with no timeout, no error, nothing. This doc exists so the next time
something breaks, you know exactly which file to open and what a normal vs. abnormal log
looks like at each point, and so future changes to logging land in the right place instead
of sprinkled wherever's convenient.

## The two loggers

- **`FeedLogger`** (`src/logging/feed_logger.rs`) — one instance per `(symbol, feed_type)`
  pair, all writing to the same file (`log_config.feed_log_location`, e.g.
  `logs/kraken.log`). Every line is tagged with provider, symbol, and feed_type, so you can
  grep for e.g. `BTC/USD | Orders` and see only that pipeline's history. Use this for
  anything specific to one feed's lifecycle: connecting, disconnecting, message handling.
- **`SysLogger`** (`src/logging/sys_logger.rs`) — one instance per system-level component
  (currently "Kraken Raw Aggregator" and "Kraken Provider Setup"), writing to
  `log_config.system_log_location` (e.g. `logs/system.log`). Use this for anything that
  isn't tied to a single symbol/feed_type — provider-level setup failures, DB write
  failures for the aggregator task.

Both share the same `LogType` enum (`Debug`/`Error`/`Info`/`Warn`, in `src/logging/mod.rs`)
and both are dumb line-writers — no log rotation, no level filtering, everything you log
gets written. That's a known limitation (see **Gaps / future work** at the bottom), so log
level choices below matter: `Info` is for expected, low-frequency lifecycle events; `Warn`
is for "this is abnormal but the code recovered on its own"; `Error` is for "this is
abnormal and something stopped working."

## The core fix: detecting a silently-dead connection

**File:** `src/data_feeds/kraken/connection/connector.rs` — `STALE_CONNECTION_TIMEOUT_SECS`
(currently 60s).

**File:** `src/data_feeds/kraken/feeds/{orders,trades,book}.rs` — the inner read loop in
each `kraken_*_data_feed` function.

This is the actual root-cause fix, not just a log line. Before this change, each feed's
inner loop was:

```rust
while let Some(message) = stream.next().await {
    // handle message
}
```

If Kraken's server stops sending anything at all — no data, no ping/pong, no close frame —
`stream.next().await` just never resolves. The TCP socket looks fine to the OS, so it never
errors either. There is no code path that produces a log line, because nothing ever
*happens* to log. That's precisely what took Orders down: the last log line is a normal
"Buffer Trigger Limit Reached" at 21:19:16, then six-plus hours of nothing, with the
process still running and the other five feeds still healthy.

The fix wraps every read in `tokio::time::timeout`:

```rust
let next = tokio::time::timeout(
    std::time::Duration::from_secs(STALE_CONNECTION_TIMEOUT_SECS),
    stream.next(),
).await;
```

Now there are four distinguishable outcomes, each logged separately (all at `Warn` — the
code recovers on its own by reconnecting, so it's not `Error`):

| Outcome | Log message | What it means |
|---|---|---|
| `Ok(Some(message))` | *(none — normal case, falls through to message handling)* | Data arrived, as expected. |
| `Ok(None)` | `"Stream ended (remote closed the connection) - reconnecting"` | Kraken closed the TCP stream cleanly. Not a bug, just the connection's natural end. |
| `Err(_elapsed)` (timeout fired) | `"No messages received in {N}s - connection appears stale, forcing reconnect"` | **This is the new case.** No frame of any kind for `STALE_CONNECTION_TIMEOUT_SECS`. This is what would have caught the 2026-09-10 outage — instead of infinite silence, you'd see this line at the 60s mark and a fresh reconnect attempt right after it. |
| `Ok(Some(Ok(Message::Close(frame))))` | `"Received Close frame from Kraken: {frame:?}"` | Kraken explicitly telling us it's hanging up (maintenance, rate limit, token expiry) rather than us finding out by going quiet. Distinguished from a raw drop because it usually points at something on Kraken's end, not ours. |
| `Ok(Some(Err(e)))` (a WebSocket protocol/IO error) | `"WebSocket read error: {e} - reconnecting"` | The `tungstenite` layer itself errored — bad frame, TLS issue, etc. |

If you ever need to tune how aggressively feeds reconnect on silence, change
`STALE_CONNECTION_TIMEOUT_SECS` in `connector.rs` — it's shared by all three feed files, so
one edit covers Trades/Book/Orders together. 60s was picked as "generous headroom," not a
measured heartbeat interval — if a particular feed type turns out to have long natural
quiet periods (unlikely for Book/Orders on BTC/USD, more plausible for a thin pair), this
is the number to raise for that case, though right now it's a single global constant, not
per-feed-type.

## Per-file breakdown

### `src/data_feeds/kraken/feeds/orders.rs`, `trades.rs`, `book.rs`

All three follow the identical shape (Orders has an extra API-token step Trades/Book
don't). Log points, in order of execution:

1. **`"started"` / `"{X} Engine Starting: {symbols}"`** (`Info`) — first thing logged, before
   any network call. If you don't see this for a symbol/feed_type at all, the task never
   even spawned — check `raw_feed.rs` and `feed_runner.rs` instead.
2. *(Orders only)* **`"API Key Unable to be retrieved: {e} - Ending process"`** (`Error`) —
   the Kraken REST token fetch failed. This is a hard stop; the function returns and the
   feed task is gone for good until the service restarts. Check the underlying error first
   — usually a missing/bad `KRAKEN_API_KEY`/`KRAKEN_API_PRIVATE_KEY` env var or a network
   failure hitting Kraken's REST API, not the WebSocket layer at all.
3. *(Orders only)* **`"API CONNECTED | CONFIRMATION KEY: {hash}"`** (`Info`) — confirms a
   token was obtained. Logs a SHA-256 hash of the token, never the token itself.
4. **`"Connected"`** (`Info`) — **new.** Logged immediately after `kraken_connect` succeeds.
   Previously there was no log between "Starting" and the first data message, so a slow or
   hanging connect attempt was indistinguishable from a dead one in the logs. Now you can
   tell "we tried to connect and it's taking a while" from "we connected fine and it's just
   quiet."
5. **`"Kraken {X} Connection Failed - ... Attempting to reconnect {n} out of {max}
   attempts"`** (`Error`) — `kraken_connect` itself failed (DNS, TLS handshake, Kraken
   rejected the subscribe request, etc.). Logged every attempt.
6. **`"... Max Attemtps reached Ending Connection"`** (`Error`, typo is pre-existing in the
   original code) — the feed gave up permanently after `max_reconnect_attempts`
   (`helix_config.yml`). The task returns; this symbol/feed_type is dead until the service
   restarts. This is the log line to alert on if you ever wire alerting — it's the only
   truly permanent failure for a feed task.
7. **`"Stream ended..."` / `"No messages received in {N}s..."` / `"Received Close
   frame..."` / `"WebSocket read error: {e}..."`** (`Warn`/`Error`) — see the table above.
   These all `break` out of the inner read loop and fall through to a fresh
   `kraken_connect` attempt at the top of the outer loop — none of them end the task.
8. **`"{X}: receiver dropped, shutting down"`** (`Error`) — the `mpsc` channel to the
   buffer/aggregator task is closed, meaning that consumer task has died (see
   `raw_feed.rs` below). Breaks the inner loop the same as the cases above — the feed
   *will* keep trying to reconnect to Kraken even though nothing is listening on the other
   end anymore, which is a known gap (see **Gaps / future work**), not something this pass
   fixed.

Non-text WebSocket frames (`Ping`/`Pong`/`Binary`/raw `Frame`) are deliberately **not**
logged — they prove the connection is alive but carry no diagnostic information, and Ping
frames in particular can be frequent enough to become noise on their own.

### `src/db/buffer.rs` — `DoubleBuffer::buffer_push_and_swap`

- **Removed:** `"Initiating Push To Buffer"` used to log on literally every message pushed
  into the buffer — thousands of lines per second on a busy feed. This is the single
  biggest reason the log was "not helpful": real signal (reconnects, errors) was buried
  under volume that told you nothing except "a message arrived," which is the expected,
  constant, non-actionable case.
- **`"Buffer swap triggered - flushing {N} messages to the DB inserter"`** (`Info`) — fires
  only when the buffer actually swaps (`buffer_capacity * buffer_swap_trigger` messages
  accumulated), which is naturally rate-limited by your config rather than by message
  volume. This is the line to watch for "is data actually flowing from this feed into the
  buffer pipeline" — if a feed's log shows normal "Connected" / reconnect activity but its
  swap lines stop appearing, the WS layer is fine and the problem is downstream.

### `src/data_feeds/kraken/raw_feed.rs` — the DB-inserter task spawned per symbol/feed_type

This task drains the `mpsc` channel the WS feed task sends messages into, pushes them
through `DoubleBuffer`, and on a swap, converts + inserts the batch into Postgres. It uses
`SysLogger`, not `FeedLogger` — these are DB/system-level failures, not feed-protocol ones.

- **`"... failed to convert a batch of {N} raw rows: {e} - this inserter is now
  permanently stopped until the service restarts"`** (`Error`) — `serde_json` failed to
  parse a buffered message as JSON. Fires `break`, which ends this symbol/feed_type's
  inserter task for good. The consequence is spelled out explicitly now because the
  original message ("failed to convert raw data to row") didn't say this was permanent —
  it read like a one-off, recoverable error, when it actually kills the pipeline for that
  symbol/feed_type silently. **The WS feed keeps running and buffering after this** — it
  just has nowhere to send flushed batches anymore, so buffered data past this point is
  lost until restart.
- **`"... failed to insert a batch of {N} rows to DB: {e} - this inserter is now
  permanently stopped until the service restarts"`** (`Error`) — same permanence, but the
  batch parsed fine and the Postgres write itself failed (connection pool exhausted, DB
  down, schema mismatch, etc.). Same consequence as above.
- **`"... feed channel closed (WS task exited) - DB inserter shutting down"`** (`Warn`) —
  **new.** Previously, when the WS feed task's `tx` sender was dropped (feed hit its
  reconnect ceiling, panicked, or the API token fetch failed), this inserter task's `while
  let Some(msg) = rx_feed.recv().await` loop just... ended. No log at all. The task quietly
  vanished. This line makes that visible — if you see this without a matching "Max
  Attempts reached" or similar in `kraken.log` for the same symbol/feed_type, check for a
  panic in `system.log` or the systemd journal instead.

### `src/runners/feed_runner.rs` — provider-level setup

- **`"Kraken feed setup failed: {e}"`** (`Error`, via `SysLogger`) — `kraken_raw_feed_channel`
  returned an error before spawning a single feed task (e.g. `FeedLogger::new` couldn't
  open the log file — bad path, permissions). Previously this went to a bare `eprint!`,
  which systemd redirects to the journal, not to `system.log` where every other
  system-level failure lands — meaning you'd have to think to check `journalctl` instead
  of just grepping the usual log file. There's a stderr fallback (`eprintln!`) if
  `SysLogger::new` itself can't open `system.log`, so the failure is never fully silent
  either way.
- **`"Unknown provider configured: {name} - no feed started for it"`** (stderr, via
  `eprintln!`) — a provider in `helix_config.yml` isn't `kraken` (the only implemented one
  right now). `validate_config` should already reject this before `feed_runner` ever runs,
  so seeing this line means validation was bypassed somehow — worth treating as
  suspicious. Left on stderr rather than `SysLogger` since there's no per-provider log
  context to attach it to and it's genuinely unreachable in normal operation.

## Gaps / future work

Things this pass deliberately did **not** fix, so they don't get lost:

- **No log rotation.** Both loggers append forever via `OpenOptions::append(true)`. On a
  server that's been up a long time, `kraken.log` will keep growing unbounded. Worth a
  `logrotate` config or a size-based rotation in `FeedLogger`/`SysLogger` at some point.
- **A dead consumer doesn't stop the WS feed from reconnecting.** If the DB-inserter task
  dies (see `raw_feed.rs` above) but the WS feed task is still healthy, the feed will keep
  reconnecting to Kraken and buffering forever with nowhere for swapped buffers to go.
  Logging now makes this visible (both sides log their half independently), but the
  reconnect loop itself doesn't check whether anyone's still listening.
- **No metrics wired up.** `src/metrics/prometheus.rs` already defines `FEEDS_RUNNING`,
  `TOTAL_MESSAGES`, `BUFFER_SWAPS_TOTAL`, `RECONNECT_ATTEMPTS_TOTAL`, and `FEED_UP`, and
  they're registered and served on `:9091/metrics` — but nothing in the codebase actually
  increments or sets any of them yet. Wiring these up at the same points documented above
  is the natural next step for dashboards/alerting instead of log-grepping, and is planned
  as separate, hands-on follow-up work rather than part of this logging pass.
- **`STALE_CONNECTION_TIMEOUT_SECS` is one global constant**, not tunable per feed type or
  per symbol. If Trades/Book/Orders end up needing meaningfully different thresholds, this
  would need to move into `ProviderConfig`/`SymbolConfig` and flow through from
  `helix_config.yml` instead.
