# Database

PostgreSQL holds two kinds of tables: one **raw** landing table (a hand-off queue between
the daemon and the normalizer) and three **normalized** tables (the long-lived,
query-friendly data). Long-term raw history lives in Parquet on R2, not in Postgres.

## Tables and who touches them

```mermaid
flowchart LR
    D["helix_feed daemon<br/>insert_raw_data_batch"] -->|INSERT| RAW[("raw_financial_data")]
    N["normalize"] -->|"SELECT ≤200k by data_type<br/>DELETE by id"| RAW
    N -->|INSERT| T[("trades_normalized")]
    N -->|INSERT| B[("book_normalized")]
    N -->|INSERT| O[("orders_normalized")]
    BF["backfill"] -->|INSERT| T & B & O
    YOU["you / research tools"] -->|SELECT| T & B & O
```

| Table | Written by | Read by | Deleted by | Expected size |
|---|---|---|---|---|
| `raw_financial_data` | daemon | normalize | normalize (after archive + insert) | about one hour of messages |
| `trades_normalized` | normalize, backfill | you | nobody | grows forever |
| `book_normalized` | normalize, backfill | you | nobody | grows forever (largest) |
| `orders_normalized` | normalize, backfill | you | nobody | grows forever |
| `_sqlx_migrations` | `sqlx migrate` | `sqlx migrate` | — | migration history |

## Schemas

### `raw_financial_data`

| Column | Type | Null | Set from |
|---|---|---|---|
| `id` | `BIGINT` (`SERIAL` → widened by `20261006120000`) | no | sequence |
| `received` | `TIMESTAMPTZ` | yes | `Utc::now()` when the batch is converted (same for the whole batch) |
| `data_provider` | `TEXT` | yes | `ProviderConfig.provider` (`"kraken"`) |
| `data_type` | `TEXT` | yes | `FeedType::as_str()` (`trades` / `book` / `orders`) |
| `symbol` | `TEXT` | yes | `SymbolConfig.symbol` |
| `raw_json` | `JSONB` | yes | the WS message, unmodified |

Indexes: primary key on `id` only. ⚠️ The normalizer filters on `data_type` and orders by
`id`. That's fine while the table holds about an hour of data, but slow if a backlog
builds. `CREATE INDEX ON raw_financial_data (data_type, id);` would fix it.

### `trades_normalized`

| Column | Type | From raw |
|---|---|---|
| `id` | `BIGINT` | sequence |
| `provider` | `TEXT` | `data_provider` |
| `symbol` | `TEXT` | `data[].symbol` |
| `side` | `TEXT` | `data[].side` (`buy`/`sell`) |
| `price`, `qty` | `DOUBLE PRECISION` | `data[].price`, `data[].qty` |
| `trade_id` | `BIGINT` | `data[].trade_id` |
| `event_time` | `TIMESTAMPTZ` | `data[].timestamp` (exchange time) |
| `received` | `TIMESTAMPTZ` | raw `received` |
| `provider_meta` | `JSONB` | `{"ord_type": …}` |

Index: `(symbol, provider, event_time)`. No unique constraint, so duplicates are possible
(see [data-lifecycle.md](data-lifecycle.md#where-data-can-be-lost-or-duplicated)).
Dedupe key in practice: `(provider, symbol, trade_id)`.

### `book_normalized` / `orders_normalized`

Identical columns: `id BIGINT`, `provider`, `symbol`, `event_time`, `received`,
`checksum BIGINT`, `bids JSONB`, `asks JSONB`, `provider_meta JSONB`.
Index: `(symbol, provider, event_time)`.

- `book`: each bid/ask is `{price, qty}`.
- `orders` (L3): each bid/ask is `{event?, order_id, limit_price, order_qty, timestamp}`.
  `event` (`add`/`modify`/`delete`) appears only on updates.
- ⚠️ Neither table records whether the row was a **snapshot** or an **update**.

## Migrations

Files live in [migrations/](../migrations/) (sqlx format: `<timestamp>_<name>.sql`, applied
in filename order, each one exactly once, tracked in `_sqlx_migrations`).

| Migration | Does |
|---|---|
| `20260723151028_create_trades_raw` | old per-feed table (later dropped) |
| `20260723161917_create_book_raw` | old per-feed table (later dropped) |
| `20260726151926_simplified_raw_storage` | drops those, creates `raw_financial_data` |
| `20260904120000/01/02_create_*_normalized` | the three normalized tables + indexes |
| `20261006120000_bigint_ids` | widens every `id` column and sequence to 64-bit |

### ⚠️ Nothing applies migrations automatically

`feed_runner` uses `PostgresDBRaw::new`, **not** `new_with_migration`. Only the tests run
`sqlx::migrate!`. Apply migrations yourself, before deploying code that needs them:

```bash
cargo install sqlx-cli --no-default-features --features postgres
```

```bash
DATABASE_URL=postgres://USER:PASS@localhost:5432/helixfeed_dev sqlx migrate run
```

`sqlx migrate info` shows which migrations are applied.

### Order of operations for `20261006120000_bigint_ids`

The normalizer decodes `id` as `i64`, which fails against an `INTEGER` column. So:
**apply the migration → then deploy the new binaries.** The `ALTER … TYPE BIGINT` rewrites
each table under an exclusive lock. Inserts wait (they don't fail) until it's done, and on
large `*_normalized` tables that can take minutes.

### Local dev DB is out of sync

On the dev machine, `_sqlx_migrations` lists only the first two migrations, but
`raw_financial_data` already exists (it was created by hand). `sqlx migrate run` then
fails with "relation already exists", and so does `test_insert_raw_data_batch`. Fix it once
by marking the already-applied migrations as done, or by recreating the dev DB. See
[testing.md](testing.md#postgres-tests).

## Writing a migration

1. `sqlx migrate add <name>` (or create `migrations/<YYYYMMDDHHMMSS>_<name>.sql` by hand).
2. **Never edit a migration that has already been applied anywhere.** sqlx checksums
   applied files and refuses to run if one changed. Add a new migration instead.
3. Update every Rust query that touches the changed columns. The SQL lives in strings, so
   the compiler won't catch a mismatch. Where to look:

| Table | Rust code that names its columns |
|---|---|
| `raw_financial_data` | `postgresql.rs::insert_raw_data_batch`, `normalizer/mod.rs::fetch_unprocessed` + `delete_processed`, `postgresql.rs` tests |
| `trades_normalized` | `normalizer/trades.rs::insert` |
| `book_normalized` | `normalizer/book.rs::insert` |
| `orders_normalized` | `normalizer/orders.rs::insert` |

4. If the column also appears in the Parquet archive, update `write_parquet_archive` **and**
   `backfill` (it reads by column index).

## Useful queries

```sql
-- How far is the raw backlog behind?
SELECT data_type, count(*), min(received), max(received) FROM raw_financial_data GROUP BY 1;

-- Is a pipeline still delivering? (last normalized row per symbol)
SELECT symbol, max(received) FROM trades_normalized GROUP BY 1;

-- How close is a sequence to its limit?
SELECT last_value FROM raw_financial_data_id_seq;

-- Duplicate trades (after a normalizer crash)
SELECT provider, symbol, trade_id, count(*) FROM trades_normalized
GROUP BY 1,2,3 HAVING count(*) > 1;
```
