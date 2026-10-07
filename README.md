# HelixFeed

🚧 **Active Development** — this is a personal, self-hosted project I'm building and iterating on regularly. 

A Rust market data ingestion engine that connects to exchange WebSocket feeds, buffers incoming messages, and persists raw data to PostgreSQL — built as the data backbone for a personal quantitative trading research stack.

HelixFeed is the ingestion layer for a larger system: it captures raw tick/trade/book data from exchanges, stores it durably, normalizes it into typed tables, and archives the raw history to Cloudflare R2 as Parquet for downstream analysis (indicator calculation, backtesting).

📚 **Full documentation lives in [`docs/`](docs/README.md)** — architecture, data lifecycle, module reference, runtime behaviour, configuration, database, operations, a change guide, and testing.

---

## Overview

- **Connects** to exchange WebSocket APIs (currently Kraken v2) and subscribes to trade, book, and order feeds per symbol.
- **Buffers** incoming messages in memory using a double-buffer pattern, swapping and flushing to the database once a configurable capacity threshold is hit — so writes are batched instead of hitting Postgres per message.
- **Persists** raw JSON payloads to PostgreSQL (`raw_financial_data`) for durability and replay.
- **Exposes** Prometheus metrics (feeds running, messages received, buffer swaps, reconnect attempts, feed up/down) over an embedded HTTP server for observability.
- **Runs** every symbol/feed-type combination as its own isolated Tokio task, so one feed erroring or reconnecting doesn't take down the others.

## Architecture

> Simplified view of the ingestion daemon. The full picture (four binaries, normalization, R2 archiving) is in [docs/architecture.md](docs/architecture.md).

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
│ Kraken: Trades  │    │  Kraken: Book     │    │ Kraken: Orders    │
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
                     └────────────────────────┘

Prometheus metrics server runs alongside, scraping feed/task health.
```

## Current Status

**Working**
- [x] Kraken WebSocket connector (public + authenticated token flow) for trades, book, and order feeds
- [x] Per-symbol, per-feed-type Tokio tasks with isolated channels
- [x] `DoubleBuffer` active/standby swap with configurable capacity + fill-trigger
- [x] Batched raw data inserts into PostgreSQL via `sqlx`, with schema migrations
- [x] Config loading + validation (`helix_config.yml`) with unit test coverage
- [x] Prometheus metrics server (feed health, message counts, reconnects, buffer swaps)
- [x] Self-hosted CI: `cargo build --release` on push to `main`

- [x] Order feed (`level3`) wired end to end, with a fresh auth token on every reconnect
- [x] Reconnect with configurable delay/attempts, plus stale-connection detection (60 s of silence)
- [x] DB writes retried with backoff; partial buffers flushed every 5 s and on graceful shutdown (SIGTERM)
- [x] Hourly normalization (raw → typed `*_normalized` tables + Parquet snapshot)
- [x] Hourly R2 cold-storage archival with retry/corrupt handling

**In progress / scaffolded**
- [ ] Prometheus metrics are registered and served but not yet incremented
- [ ] Additional providers — config validates against `kraken` and `databento`, only Kraken is implemented
- [ ] Book checksum validation / gap detection

## Tech Stack

- **Language:** Rust (2021 edition)
- **Async runtime:** Tokio
- **WebSocket client:** `tokio-tungstenite`
- **Databases:** PostgreSQL (`sqlx`, raw storage + migrations)
- **Metrics:** `prometheus` + `hyper` (embedded metrics HTTP server)
- **Config:** YAML (`serde_yaml`) with custom validation layer
- **Auth:** HMAC-SHA512 request signing for Kraken's authenticated WebSocket token endpoint
- **Archive:** Parquet (`arrow`/`parquet`) uploaded to Cloudflare R2 via `aws-sdk-s3`
- **CI/CD:** self-hosted GitHub Actions runner, builds on a `v*.*.*` tag or a manual run

## Getting Started

### Prerequisites
- Rust (stable, 2021 edition or later)
- PostgreSQL instance
- Kraken API key/secret if using authenticated feeds (order book L3)

### Setup

Clone the repo and set up your environment:

```bash
git clone https://github.com/Dakotah-Torres/HelixFeed.git
cd HelixFeed
```

Create a `.env` file in the project root with:

```
DB_USER=YOUR_DATABASE_USER_NAME
DB_PASS=YOUR_DATABASE_PASSWORD
KRAKEN_API_KEY=<your-key>
KRAKEN_API_PRIVATE_KEY=<your-secret>
R2_API_ACCESS_KEY=<r2-access-key>     # only for the archive job
R2_API_SECRET_KEY=<r2-secret-key>
```

Copy `helix_config.example.yml` to `helix_config.yml` and adjust it for your symbols, database, and log paths (field reference: [docs/configuration.md](docs/configuration.md)). Apply the database migrations (see [docs/database.md](docs/database.md#migrations)), then build and run:

```bash
cargo build --release
cargo run
```

Prometheus metrics are served on the embedded HTTP server once the feed runner starts — point your Prometheus scrape config at it to pull in `helix_feeds_running`, `helix_messages_total`, `helix_buffer_swaps_total`, `helix_reconnect_attempts_total`, and `helix_feed_up`.

### Running tests

```bash
cargo test
```

Most tests run offline (a fake Kraken WebSocket server and an in-memory DB sink). The PostgreSQL integration tests require a running Postgres instance and `DB_USER`/`DB_PASS` in `.env`. See [docs/testing.md](docs/testing.md).

### Deploying

`deploy.sh` runs `cargo check`, commits the working `dev` branch, merges into `main`, and pushes. Pushing a `v*.*.*` tag (or running the workflow manually) triggers the self-hosted GitHub Actions workflow that builds the release binaries on the quant server and restarts the services. Details and a release checklist: [docs/operations.md](docs/operations.md).

## Project Structure

```
HelixFeed/
├── src/
│   ├── main.rs                         # helix_feed daemon entry point
│   ├── bin/                            # normalize.rs, archive.rs, backfill.rs (batch jobs)
│   ├── runners/feed_runner.rs          # daemon setup, pipeline spawning, graceful shutdown
│   ├── config/mod.rs                   # config structs, YAML loading, validation
│   ├── data_feeds/
│   │   ├── traits.rs                   # provider traits (scaffolding for the registry)
│   │   └── kraken/
│   │       ├── connection/connector.rs # WS connect + Kraken REST token (HMAC signing)
│   │       ├── raw_feed.rs             # builds one pipeline per symbol/feed type
│   │       └── feeds/                  # trades.rs, book.rs, orders.rs
│   ├── db/
│   │   ├── buffer.rs                   # DoubleBuffer
│   │   ├── inserter.rs                 # per-pipeline DB writer: retry, timed + shutdown flush
│   │   └── postgresql.rs               # PgPool, batched UNNEST raw inserts
│   ├── normalizer/                     # raw → Parquet + typed tables
│   ├── archive/r2.rs                   # Parquet → Cloudflare R2
│   ├── metrics/prometheus.rs           # Prometheus registry + :9091 server
│   └── logging/                        # FeedLogger + SysLogger
├── migrations/                         # sqlx PostgreSQL migrations
├── deploy/                             # systemd unit + timer templates
├── docs/                               # full documentation
├── helix_config.example.yml            # config template
├── deploy.sh                           # cargo check → commit → merge dev→main → push
└── .github/workflows/deploy.yml        # self-hosted CI build + service restart
```

## Design Notes

A few architecture decisions worth calling out:

- **Raw storage schema evolved from per-feed-type tables (`trades_raw`, `book_raw`) to a single unified `raw_financial_data` table** — simpler to insert into via one batched `UNNEST` query, and feed type becomes a column rather than a table name.
- **Every symbol × feed-type combination gets its own Tokio task and its own `DoubleBuffer`**, so a slow or failing feed for one symbol doesn't block others, and buffer swap thresholds can be tuned per feed volume.

## Roadmap

See [docs/roadmap.md](docs/roadmap.md). Next up: the provider abstraction (M4), book checksum/gap detection (M5), Grafana dashboards (M6), and a second provider (M7).

## Why This Project

Built as the data foundation for a personal quantitative trading research stack — I wanted full control over data capture (no vendor gaps, no rate-limited history APIs) and a system I understood end-to-end, from WebSocket auth through to the storage layer. It's also been a deep, hands-on way to work through real Rust ownership and concurrency problems: `async move` semantics, `Mutex` guards across `.await` points, and single-ownership `mpsc` channel design.

---

**Author:** Dakotah Torres — [GitHub](https://github.com/Dakotah-Torres)
