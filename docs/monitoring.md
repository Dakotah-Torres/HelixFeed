# Monitoring

How to tell from the outside whether HelixFeed is working, without SSHing in and running
SQL by hand. Written after the 2026-10-09 outage (see [production.md](production.md#incident-log)),
where `systemctl status` said `active` for 3¾ hours while every feed was dead.

This file covers what the **daemon exposes** on `:9091/metrics`. Setting up the Prometheus
server that scrapes it, and the Grafana dashboards on top, is not part of this repo. The
PromQL below is a starting point for that.

## What answers which question

| Question | Signal | Where it comes from |
|---|---|---|
| Is the daemon process up? | `up{job="helixfeed"}` | **Prometheus itself**, once it scrapes `:9091`. If the process is dead the port doesn't answer and `up` drops to 0. No HelixFeed code involved. |
| Is each feed actually delivering data? | `helix_feed_up`, `helix_feed_last_received_timestamp_seconds` | health poller, from the raw table |
| Did the last archive run work? | `helix_archive_last_run_*` | health poller, from the archiver's status file |
| Are the normalize and archive timers even on? | `helix_systemd_unit_active` | health poller, from `systemctl show` |

## The health poller

[src/health/mod.rs](../src/health/mod.rs). One Tokio task inside the daemon, spawned by
`feed_runner` next to the metrics server. Every 30 s (`HEALTH_POLL_INTERVAL`, the first
check is 30 s after startup) it does three things:

1. **Feeds.** Queries `raw_financial_data` for the newest `received` per
   `(provider, symbol, data_type)` and compares it to now. Newer than 5 minutes
   (`STALE_THRESHOLD_SECS`, the same threshold as production.md's check 2) = up.
   It checks the **database**, not the feed tasks' own view of themselves, so "process
   alive, nothing landing" shows up as down.
2. **Archiver.** Reads `archive_status.json` (next to `system.log`, so `logs/` by default),
   which `archive` rewrites at the end of every run, including failed ones.
3. **Timers.** Runs `systemctl show <unit> --property=ActiveState --value` for
   `helixfeed-normalize.timer` and `helixfeed-archive.timer` (`MONITORED_UNITS`).
   Read-only, so no root needed.

Every change of state is logged once to `system.log` under `Health Poller`, never once per
tick:

```
[…] [ERROR] | Health Poller | kraken | BTC/USD | book is DOWN - newest row is from 2026-…, over 300s ago
[…] [INFO]  | Health Poller | kraken | BTC/USD | book is UP
[…] [ERROR] | Health Poller | Archiver FAILING - run at …: 2 uploaded, 1 failed, 0 moved to corrupt (…)
[…] [WARN]  | Health Poller | systemd unit helixfeed-normalize.timer is NOT active - its job is not being scheduled
```

## Metrics reference

| Metric | Labels | Meaning |
|---|---|---|
| `helix_feed_up` | `provider`, `symbol`, `feed_type` | 1 if that pipeline's newest raw row is ≤ 5 min old, else 0. One series per configured pipeline. |
| `helix_feed_last_received_timestamp_seconds` | `provider`, `symbol`, `feed_type` | Unix time of that pipeline's newest raw row. Absent until a row has been seen. |
| `helix_archive_last_run_timestamp_seconds` | — | Unix time the last archive run finished. |
| `helix_archive_last_run_success` | — | 1 if the last run uploaded everything, 0 if it errored **or** any file failed or was moved to `corrupt/`. |
| `helix_archive_last_run_files` | `result` = `uploaded` / `failed` / `corrupt` | File counts from the last run. |
| `helix_systemd_unit_active` | `unit` | 1 if the unit's `ActiveState` is `active`. |

Still always 0, deliberately left for later (find the call sites with
`grep -rn "TODO(v1.1.0 metrics)" src`): `helix_messages_total`,
`helix_buffer_swaps_total`, `helix_reconnect_attempts_total`, `helix_feeds_running`.

## Pointing Prometheus at it

A minimal scrape job, for your own `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: helixfeed
    static_configs:
      - targets: ["quant-node:9091"]
```

Useful PromQL to start panels and alerts from:

| What | PromQL |
|---|---|
| Daemon down | `up{job="helixfeed"} == 0` |
| Any feed down | `helix_feed_up == 0` |
| How far behind each feed is, in seconds | `time() - helix_feed_last_received_timestamp_seconds` |
| Last archive run failed | `helix_archive_last_run_success == 0` |
| Archiver hasn't run in 2 h (timer off, or the job crashes before writing its status) | `time() - helix_archive_last_run_timestamp_seconds > 7200` |
| Normalize timer off | `helix_systemd_unit_active{unit="helixfeed-normalize.timer"} == 0` |

## Sharp edges

- ⚠️ **`helix_feed_up` belongs to the health poller.** Don't also `.set()` it from the feed
  tasks, or the two writers will overwrite each other every few seconds. The feed-side
  TODOs point at a separate `FEED_CONNECTED` gauge for "is the WebSocket connected".
- ⚠️ **Only the newest 250 000 raw rows are checked** (`last_received_per_pipeline` in
  [postgresql.rs](../src/db/postgresql.rs)). The raw table has no index except its primary
  key, so a full `GROUP BY` would read every row every 30 s. 250k rows is about 15 min at
  the ~270 msg/s measured on 2026-10-09. If total throughput ever rises past ~800 msg/s,
  a quiet pipeline could fall outside that window and read as down while it's still
  alive. Raise the `LIMIT` if that happens.
- ⚠️ **No rows in the window counts as down**, including right after the normalizer has
  drained everything for a very quiet pipeline. It's the safe default: no evidence of life
  is not treated as healthy.
- ⚠️ `archive` exits before writing a status file if `database_conf.r2` isn't configured,
  or if `helix_config.yml` can't be loaded. In both cases the archive gauges just keep
  their previous value, and the "hasn't run in 2 h" query above is what catches it.
- On a machine where the timers aren't installed (your dev box), `helix_systemd_unit_active`
  reads 0 for both. That's expected.
- `:9091` listens on all interfaces. See [operations.md](operations.md#metrics).
