# HelixFeed Architecture

This is the reference map for the current refactor: fixing the shared-DB bottleneck and making providers pluggable, designed as one shape rather than two separate changes.

## Current State (post-cleanup, pre-refactor)

Per-symbol WebSocket ingestion and buffering are already independent. The break happens at the DB hand-off: every symbol, across every provider, funnels into **one shared channel** drained by **one shared task**, inserted into Postgres **one buffer at a time, in receipt order**.

```mermaid
flowchart TD
    CFG["helix_config.yml"] --> FR["feed_runner()"]
    FR -->|"spawns per provider<br/>match provider.provider.as_str()"| KP["kraken_raw_feed_channel()"]
    FR -->|"created once"| POOL[("Shared PgPool<br/>(PostgresDBRaw)")]
    FR -->|"created once"| SHCH{{"Shared mpsc channel<br/>tx_provider_channel"}}

    subgraph SYM1["Symbol: BTC/USD"]
        direction TB
        WS1["WS task<br/>kraken_trade_data_feed"] -->|"own mpsc&lt;String&gt;"| BUF1["own DoubleBuffer task"]
    end

    subgraph SYM2["Symbol: ETC/USD"]
        direction TB
        WS2["WS task<br/>kraken_trade_data_feed"] -->|"own mpsc&lt;String&gt;"| BUF2["own DoubleBuffer task"]
    end

    KP --> SYM1
    KP --> SYM2

    BUF1 -->|"on swap: send(buffer)"| SHCH
    BUF2 -->|"on swap: send(buffer)"| SHCH

    SHCH --> INS[["⚠ ONE consumer task<br/>serial loop: recv → insert → recv..."]]
    INS --> POOL
    POOL --> PG[("Postgres<br/>raw_financial_data")]

    classDef bottleneck fill:#e8998d,stroke:#a4243b,color:#1a1a1a,stroke-width:2px
    class SHCH,INS bottleneck
```

**The problem this causes:** the shared channel is bounded (`buffer_capacity` slots). If the single inserter task is slow — or one symbol is dumping messages fast enough to fill the channel — `send(buffer).await` blocks for *every* symbol trying to flush, not just the busy one. No data gets mixed up (every row carries its own provider/symbol/feed_type tag), but throughput for an unrelated, healthy symbol can stall behind a slow or broken one. That's the head-of-line blocking we're removing.

## Target State (independent per-symbol pipeline, pluggable providers)

Each symbol, on each provider, owns its pipeline end to end — including its own database write. The only thing shared across all of them is the connection **pool** (cheap to clone, meant to be shared), never a channel or a task. A provider registry sits above everything so adding an exchange means implementing one interface and registering it — not editing `feed_runner`'s dispatch logic. (Whether that registry is a static enum or dynamic trait objects is still open — see `docs/provider-pattern.md`.)

```mermaid
flowchart TD
    CFG["helix_config.yml"] --> FR["feed_runner()"]
    FR -->|"for each registered provider"| REG{{"Provider registry<br/>(dispatch: static enum vs.<br/>dynamic trait object — TBD)"}}
    FR -->|"created once, clone() is cheap"| POOL[("Shared PgPool")]

    REG --> P1["Kraken"]
    REG --> P2["Databento"]
    REG --> P3["...next provider"]

    subgraph SYM1["Kraken · BTC/USD · trades"]
        direction TB
        WS1["WS connect task"] -->|"own mpsc&lt;String&gt;"| BUF1["own DoubleBuffer task"]
        BUF1 -->|"on swap"| INS1["own inserter task<br/>pool.clone()"]
    end

    subgraph SYM2["Kraken · BTC/USD · book"]
        direction TB
        WS2["WS connect task"] -->|"own mpsc&lt;String&gt;"| BUF2["own DoubleBuffer task"]
        BUF2 -->|"on swap"| INS2["own inserter task<br/>pool.clone()"]
    end

    subgraph SYM3["Databento · ETH/USD · trades"]
        direction TB
        WS3["WS connect task"] -->|"own mpsc&lt;String&gt;"| BUF3["own DoubleBuffer task"]
        BUF3 -->|"on swap"| INS3["own inserter task<br/>pool.clone()"]
    end

    P1 --> SYM1
    P1 --> SYM2
    P2 --> SYM3

    INS1 --> POOL
    INS2 --> POOL
    INS3 --> POOL
    POOL --> PG[("Postgres<br/>raw_financial_data")]

    classDef independent fill:#9fd8b8,stroke:#1b6e4c,color:#1a1a1a,stroke-width:2px
    classDef open fill:#f5deb3,stroke:#b8860b,color:#1a1a1a,stroke-width:2px,stroke-dasharray: 4 3
    class SYM1,SYM2,SYM3 independent
    class REG open
```

A symbol dying, reconnecting, or backing up can only ever block *itself*. Postgres still sees bounded concurrency (the pool enforces that), but that's real backpressure on a shared resource, not artificial backpressure from a shared queue.

## Current vs. Target, at a glance

| | Current | Target |
|---|---|---|
| Per-symbol WS + buffer | ✅ already independent | ✅ unchanged |
| Path to Postgres | ❌ one shared channel, one shared task | ✅ own channel, own task per symbol |
| Postgres connections | pool exists but only 1 task ever uses it | pool shared, used concurrently by every symbol's task |
| Adding a provider | edit `feed_runner`'s string match + write a parallel `<provider>_raw_feed_channel` fn | implement one trait/interface, register it |
| Blast radius of a slow/broken symbol | can stall unrelated symbols' DB writes | contained to itself |

## Open question

Static dispatch (enum + trait) vs. dynamic dispatch (trait objects via `async-trait`) for the provider registry — to be resolved in `docs/provider-pattern.md` with both sketched out before choosing.
