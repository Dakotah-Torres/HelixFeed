# HelixFeed Roadmap

The guiding principle: build the target pipeline shape (see [architecture.md](architecture.md)) **once, correctly, on a single symbol**, before spreading it out to every symbol and every provider. Each milestone below ends in something that runs and something you can point at as "this works now" — not a half-finished rewrite blocking the next thing.

```mermaid
flowchart TD
    M1["<b>M1 · Reference docs</b><br/>provider-pattern.md + observability.md<br/>dispatch style decided"]
    M2["<b>M2 · Vertical slice</b><br/>One symbol, one feed, full new shape:<br/>reconnect + own buffer + own DB write + live metrics"]
    M3["<b>M3 · Generalize</b><br/>Every configured Kraken symbol/feed_type<br/>gets its own independent pipeline<br/>shared channel + serial inserter removed"]
    M4["<b>M4 · Provider abstraction</b><br/>feed_runner dispatches via trait/enum,<br/>not string-match \"kraken\""]
    M5["<b>M5 · Orders + data completeness</b><br/>Wire up orders.rs · book checksum/gap detection"]
    M6["<b>M6 · Grafana</b><br/>Dashboard panels: feed health, throughput,<br/>pipeline integrity, data completeness"]
    M7["<b>M7 · Second provider (stretch)</b><br/>Real proof the abstraction holds"]

    M1 --> M2 --> M3 --> M4 --> M5 --> M6 --> M7

    classDef docs fill:#a8c6e0,stroke:#2b5f82,color:#1a1a1a,stroke-width:2px
    classDef core fill:#e8e2c8,stroke:#8a7f3f,color:#1a1a1a,stroke-width:2px
    classDef payoff fill:#9fd8b8,stroke:#1b6e4c,color:#1a1a1a,stroke-width:2px
    classDef stretch fill:#f5deb3,stroke:#b8860b,color:#1a1a1a,stroke-width:2px,stroke-dasharray: 4 3

    class M1 docs
    class M2,M3,M4,M5 core
    class M6 payoff
    class M7 stretch
```

---

### M1 — Reference docs (no code)
**Goal:** `provider-pattern.md` (static vs. dynamic dispatch, both sketched) and `observability.md` (which metrics, which labels, the checksum/gap design) written and dispatch style decided.
**Why first:** everything after this points back to these two files whenever you lose the thread mid-build.

### M2 — Vertical slice: one symbol, full new shape
**Goal:** Just BTC/USD trades, just Kraken. A WS connection with real reconnect/backoff → its own `DoubleBuffer` → its own inserter task holding a `pool.clone()` → the metrics that task should emit (`feed_up`, `helix_messages_total`, `helix_buffer_swaps_total`) as live, labeled calls instead of the dead registrations sitting there today.
**Concepts:** `Arc`/`Clone` semantics for a shared `PgPool`, task-owned error handling (a task's failure shouldn't take the process down), a proper reconnect loop, `CounterVec`/`GaugeVec` with labels.
**Why this size:** hardest milestone conceptually, smallest in surface area — one symbol, fully proven, before it touches anything else.

### M3 — Generalize to every configured symbol/feed_type
**Goal:** Rip out the shared `mpsc` channel and the single serial inserter task in `feed_runner.rs`; replace with N independent copies of the M2 pattern, one per `symbol_feed` entry in config.
**Concepts:** avoiding copy-paste by extracting a small helper now that the pattern is proven; this is where the actual bottleneck dies.

### M4 — Lift it behind the provider abstraction
**Goal:** Wrap "spawn a symbol's independent pipeline" behind the trait/enum decided in M1. `feed_runner` iterates registered providers instead of `match provider.provider.as_str() { "kraken" => ... }`. Kraken is still the only implementor.
**Why not first:** designing the interface before M2/M3 prove the shape risks revising it once reality disagrees with the guess.

### M5 — Orders feed + book data-completeness
**Goal:** Wire the already-written `orders.rs` into the registry (it's built, just not connected). Add Kraken's checksum validation to `book.rs` so a dropped/out-of-order message becomes a counted, visible gap instead of silent book corruption.

### M6 — Grafana
**Goal:** Point Grafana at `:9091`, build panels for the four signal categories (feed health, throughput, pipeline integrity, data completeness) using metrics that already exist by now. Mostly not Rust — the payoff milestone after five rounds of plumbing.

### M7 — A real second provider (stretch)
**Goal:** Whichever exchange you actually want next, implemented against the M4 trait. The actual test of whether the abstraction was worth building.
