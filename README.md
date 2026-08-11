# HelixFeed

🚧 **Active Development** — this is a personal, self-hosted project I'm building and iterating on regularly. 

A Rust market data ingestion engine that connects to exchange WebSocket feeds, buffers incoming messages, and persists raw and normalized data to PostgreSQL and DuckDB — built as the data backbone for a personal quantitative trading research stack.

HelixFeed is the ingestion layer for a larger system: it captures raw tick/trade/book data from exchanges, stores it durably, and hands off normalized data for downstream analysis (indicator calculation, backtesting) via a companion Rust crate, `qasm_core`.

---

## Overview

- **Connects** to exchange WebSocket APIs (currently Kraken v2) and subscribes to trade, book, ticker, and order feeds per symbol.
- **Buffers** incoming messages in memory using a double-buffer pattern, swapping and flushing to the database once a configurable capacity threshold is hit — so writes are batched instead of hitting Postgres per message.
- **Persists** raw JSON payloads to PostgreSQL (`raw_financial_data`) for durability and replay, with a DuckDB layer for normalized, query-friendly analytics data.
- **Exposes** Prometheus metrics (feeds running, messages received, buffer swaps, reconnect attempts, feed up/down) over an embedded HTTP server for observability.
- **Runs** every symbol/feed-type combination as its own isolated Tokio task, so one feed erroring or reconnecting doesn't take down the others.

## Architecture

```
                 ┌────────────────────────────┐
                 │   helix_config.yml          │
                 │  (providers, symbols,       │
                 │   buffer, db, logging)      │
                 └──────────────┬───────────────┘
                                │
                     ┌──────────▼──────────┐
                     │     feed_runner      │
                     │  (loads + validates  │
                     │   config, spawns      │
                     │   tasks, starts        │
                     │   metrics server)      │
                     └──────────┬──────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
┌───────▼────────┐    ┌─────────▼────────┐    ┌─────────▼────────┐
│ Kraken: Trades  │    │  Kraken: Book     │    │ Kraken: Ticker    │
│ (Tokio task per │    │ (Tokio task per   │    │ (Tokio task per   │
│    symbol)      │    │    symbol)        │    │    symbol)        │
└───────┬────────┘    └─────────┬────────┘    └─────────┬────────┘
        │                       │                       │
        │      raw WS messages, per-symbol channel       │
        └───────────────────────┼───────────────────────┘
                                │
                     ┌──────────▼──────────┐
                     │    DoubleBuffer       │
                     │ (active/standby swap, │
                     │  fill-threshold flush)│
                     └──────────┬──────────┘
                                │  mpsc channel
                     ┌──────────▼──────────┐
                     │   PostgresDBRaw       │
                     │ batched UNNEST insert │
                     │→ raw_financial_data   │
                     └──────────┬──────────┘
                                │
                     ┌──────────▼──────────┐
                     │   NormalizedDuckDB    │
                     │ (normalized analytics │
                     │  tables, e.g. trades)  │
                     └────────────────────────┘

Prometheus metrics server runs alongside, scraping feed/task health.
```

## Current Status

**Working**
- [x] Kraken WebSocket connector (public + authenticated token flow) for trades, book, and ticker feeds
- [x] Per-symbol, per-feed-type Tokio tasks with isolated channels
- [x] `DoubleBuffer` active/standby swap with configurable capacity + fill-trigger
- [x] Batched raw data inserts into PostgreSQL via `sqlx`, with schema migrations
- [x] Config loading + validation (`helix_config.yml`) with unit test coverage
- [x] Prometheus metrics server (feed health, message counts, reconnects, buffer swaps)
- [x] DuckDB migration runner for normalized analytics tables
- [x] Self-hosted CI: `cargo build --release` on push to `main`

**In progress / scaffolded**
- [ ] Order feed (`level3`) — defined in config/traits but not yet wired into the Kraken raw feed dispatcher
- [ ] Raw → normalized pipeline (Postgres → DuckDB) — DuckDB schema exists (`trades_normalized`) but the transform/load step isn't implemented yet
- [ ] Reconnect/backoff logic — config supports `reconnect_delay_secs` / `max_reconnect_attempts`, connector-level retry handling still in progress
- [ ] Additional providers — config validates against `kraken` and `databento`, only Kraken is implemented
- [ ] R2 cold-storage archival (config schema exists, upload logic not yet built)

## Tech Stack

- **Language:** Rust (2021 edition)
- **Async runtime:** Tokio
- **WebSocket client:** `tokio-tungstenite`
- **Databases:** PostgreSQL (`sqlx`, raw storage + migrations), DuckDB (normalized analytics, embedded via `include_str!` migrations)
- **Metrics:** `prometheus` + `hyper` (embedded metrics HTTP server)
- **Config:** YAML (`serde_yaml`) with custom validation layer
- **Auth:** HMAC-SHA512 request signing for Kraken's authenticated WebSocket token endpoint
- **Companion crate:** `qasm_core` (local path dependency) — shared trade/book/candle data types used across the quant research stack
- **CI/CD:** self-hosted GitHub Actions runner, builds on every push to `main`

## Getting Started

### Prerequisites
- Rust (stable, 2021 edition or later)
- PostgreSQL instance
- `qasm_core` crate available as a sibling directory (`../qasm_core`) — this is a workspace-local dependency, not published to crates.io
- Kraken API key/secret if using authenticated feeds (order book L3)

### Setup

Clone the repo and set up your environment:

```bash
git clone https://github.com/Dakotah-Torres/HelixFeed.git
cd HelixFeed
```

Create a `.env` file in the project root with:

```
DB_USER=helixfeed
DB_PASS=devpassword
KRAKEN_API_KEY=<your-key>
KRAKEN_API_SECRET=<your-secret>
```

Adjust `helix_config.yml` for your symbols, database, and log paths, then build and run:

```bash
cargo build --release
cargo run
```

Prometheus metrics are served on the embedded HTTP server once the feed runner starts — point your Prometheus scrape config at it to pull in `helix_feeds_running`, `helix_messages_total`, `helix_buffer_swaps_total`, `helix_reconnect_attempts_total`, and `helix_feed_up`.

### Running tests

```bash
cargo test
```

Tests cover config validation (valid/invalid configs, missing fields, buffer bounds) and PostgreSQL integration (connection, migrations, batch insert correctness) — the latter requires a running Postgres instance and `DB_USER`/`DB_PASS` in `.env`.

### Deploying

`deploy.sh` runs `cargo check`, commits the working `dev` branch, merges into `main`, and pushes — which then triggers the self-hosted GitHub Actions workflow to build the release binary on the quant server.

## Project Structure

```
HelixFeed/
├── src/
│   ├── main.rs                        # Entry point — starts the feed runner
│   ├── runners/feed_runner.rs         # Loads config, spawns provider tasks + DB sink
│   ├── config/mod.rs                  # Config structs, YAML loading, validation
│   ├── data_feeds/
│   │   ├── traits.rs                  # DataProvider / feed trait definitions
│   │   └── kraken/
│   │       ├── connection/connector.rs # WebSocket connect + Kraken auth (HMAC signing)
│   │       ├── raw_feed.rs             # Per-symbol task spawning + buffer wiring
│   │       └── feeds/                  # trades.rs, book.rs, ticker.rs, orders.rs, candle.rs
│   ├── db/
│   │   ├── buffer.rs                   # DoubleBuffer — active/standby swap on capacity
│   │   ├── postgresql.rs               # PgPool, batched raw inserts, migrations
│   │   ├── duckdb.rs                   # DuckDB connection + migration runner
│   │   └── duckdb_migrations/          # Embedded (include_str!) normalized schema
│   ├── metrics/prometheus.rs           # Prometheus registry + embedded metrics server
│   ├── logging/                        # Feed-level and system-level logging
│   └── ingest/                         # Ingestion pipeline glue
├── migrations/                         # sqlx PostgreSQL migrations (raw storage schema)
├── helix_config.yml                    # Example provider/symbol/database configuration
├── deploy.sh                           # cargo check → commit → merge dev→main → push
└── .github/workflows/deploy.yml        # Self-hosted CI: cargo build --release on push
```

## Design Notes

A few architecture decisions worth calling out:

- **Raw storage schema evolved from per-feed-type tables (`trades_raw`, `book_raw`) to a single unified `raw_financial_data` table** — simpler to insert into via one batched `UNNEST` query, and feed type becomes a column rather than a table name.
- **Postgres holds raw data; DuckDB holds normalized analytics data.** This keeps ingestion fast and durable (append raw JSON, no transform on the hot path) while giving downstream analysis a fast, typed, query-friendly store.
- **DuckDB migrations are embedded at compile time (`include_str!`)** rather than read from disk, for single-binary distribution. Per-feed normalized tables are provisioned dynamically in Rust rather than tracked as migrations, since their shape depends on the feed being ingested.
- **Every symbol × feed-type combination gets its own Tokio task and its own `DoubleBuffer`**, so a slow or failing feed for one symbol doesn't block others, and buffer swap thresholds can be tuned per feed volume.

## Roadmap

- Wire up the order book (L3) feed end-to-end
- Build the raw → normalized transform pipeline into DuckDB
- Add reconnect/backoff handling at the connector level
- Add a second data provider (Databento is already validated in config)
- R2 cold-storage archival for raw data

## Why This Project

Built as the data foundation for a personal quantitative trading research stack — I wanted full control over data capture (no vendor gaps, no rate-limited history APIs) and a system I understood end-to-end, from WebSocket auth through to the storage layer. It's also been a deep, hands-on way to work through real Rust ownership and concurrency problems: `async move` semantics, `Mutex` guards across `.await` points, and single-ownership `mpsc` channel design.

---

**Author:** Dakotah Torres — [GitHub](https://github.com/Dakotah-Torres)
