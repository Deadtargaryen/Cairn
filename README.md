# BTCUSDT Research & Paper-Trading System

A solo-built, research-first trading system for Binance Spot BTCUSDT. It covers the whole
path from public exchange data to a safety-gated execution loop: downloadable kline archives
are verified and normalized into SQLite, then evaluated with cost-aware backtests,
walk-forward selection, signal diagnostics, risk review, and a fail-closed deployment gate.
A local FastAPI dashboard exposes the live market, the paper engine, and every safety check
in plain language.

**This is research software, not a profitable strategy and not financial advice.** The
project's own deployment gate currently returns `ABSTAIN_NO_LIVE_TRADING`: no baseline rule
survived out-of-sample testing after modeled costs, and the $3 account scenario fails Binance
minimum-notional requirements. Live order submission is disabled by default and requires
several independent gates to be opened deliberately.

## Status

- **Mode:** research and paper trading only.
- **Live orders:** blocked by default. `ENGINE_MODE=PAPER`, `BINANCE_ORDER_MODE=observe`,
  `BINANCE_ENABLE_LIVE_ORDERS=false`, plus a required gate file and kill-switch check.
- **Data:** generated archives, the warehouse, and experiment reports are **not** committed
  (see [`.gitignore`](.gitignore)); they are reproducible from Binance's public data.
- **Roadmap:** see [ROADMAP.md](ROADMAP.md) for planned work and the current state of each
  item.

## Development history

This is a personal project that was developed locally and published to GitHub on 2026-11-03.
The commit log therefore starts at that import: the first commits contain the project as it
stood on publication day, and the earlier local development is not represented
commit-by-commit. No history was squashed, rewritten, or re-uploaded from a previous
repository, and every commit is authored by the repository owner. The tree is original work;
the only third-party code included is the attributed `binance-public-data` reference snapshot
under `research/` (see its [NOTICE](research/binance-public-data-master/NOTICE.md)). Work
after publication is added as ordinary incremental commits.

## What it does

1. **Acquire** Binance public Spot kline archives (daily/monthly ZIPs) resumably, with SHA-256
   and ZIP verification.
2. **Normalize** them into a canonical, UTC, integer-microsecond SQLite warehouse with
   archive-level provenance and per-row quality flags.
3. **Research** with chronological train/validation/test splits, explicit fee/spread/slippage/
   impact proxies, walk-forward selection, predeclared signal diagnostics, and block-bootstrap
   risk review.
4. **Gate** deployment with a report that hashes every upstream artifact and refuses to
   promote a strategy without sufficient evidence.
5. **Operate** a local dashboard and a paper/live engine that shares one decision and risk
   pipeline. Only the live executor can call Binance order endpoints, and only after all
   safety gates pass.

## Who it's for

Anyone who wants a transparent, reproducible example of systematic-trading research that
takes costs, data provenance, and failure modes seriously. It is built for a single operator
running it locally; it is not a hosted service and does not accept other people's data.

## Built with

- **Python 3.10+** (developed on 3.14), standard library plus:
- `numpy`, `pandas`, `scipy` for research
- `fastapi`, `uvicorn`, `websockets` for the dashboard and streams
- `python-dotenv` for configuration
- **SQLite** as the normalized warehouse
- Vanilla HTML/CSS/JS for the dashboard front end

## Repository layout

```
.
├── README.md
├── LICENSE                  MIT
├── ROADMAP.md
├── requirements.txt
├── .env.example             configuration template (safe defaults)
└── research/
    ├── download_binance_klines.py    resumable archive downloader
    ├── validate_binance_klines.py    stdlib-only archive validator
    ├── normalize_binance_klines.py   SQLite warehouse builder
    ├── backtest_baselines.py         cost-aware baseline simulator
    ├── analyze_baselines.py          out-of-sample analysis
    ├── walk_forward_baselines.py     fixed-rule walk-forward selection
    ├── signal_diagnostics.py         predeclared hypothesis diagnostics
    ├── risk_review.py                bootstrap drawdown/ruin diagnostics
    ├── account_viability.py          small-account cost gate
    ├── deployment_gate.py            fail-closed promotion report
    ├── binance_live.py               guarded REST/WS connector
    ├── live_engine.py                feature → decision → risk → execution loop
    ├── dashboard_app.py              FastAPI control surface
    ├── dashboard/                    dashboard front end
    ├── knowledge_base_references.json
    ├── research_assumptions.json
    ├── binance-public-data-master/   vendored upstream reference snapshot (MIT)
    └── test_*.py                     56 unit tests
```

## Quick start

```bash
git clone https://github.com/Deadtargaryen/crypto.git
cd crypto
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
cp .env.example .env
python3 -m research.dashboard_app
```

Open <http://127.0.0.1:8787>. The dashboard starts in observation mode; the browser never
receives API secrets. Add credentials to `.env` only if you want authenticated read-only
account checks.

### Run the tests

```bash
cd research
python3 -m unittest discover -p 'test_*.py'
```

## Rebuilding the dataset and research artifacts

Run these from `research/`; the modules import each other as top-level modules.

```bash
cd research

# 1. Download public Spot klines (writes a retrieval manifest in the output root)
python3 download_binance_klines.py \
  --output-root raw/binance \
  --symbol BTCUSDT \
  --start-date 2019-01-01 \
  --end-date 2026-09-08

# 2. Validate archives (checksums, schema, timestamp units, candle invariants)
python3 validate_binance_klines.py \
  --root raw/binance \
  --output experiments/validation-report.json \
  --require-checksum

# 3. Build the normalized warehouse
python3 normalize_binance_klines.py \
  --manifest raw/binance/binance-kline-retrieval-manifest.json \
  --output warehouse.sqlite

# 4. Baseline backtests and analysis
python3 backtest_baselines.py \
  --warehouse warehouse.sqlite \
  --output-dir experiments/baseline
python3 analyze_baselines.py \
  --reports experiments/baseline/baseline_report.json \
  --output experiments/analysis.json

# 5. Walk-forward, signal diagnostics, risk, viability, gate
python3 walk_forward_baselines.py --warehouse warehouse.sqlite --output experiments/walk-forward.json
python3 signal_diagnostics.py --warehouse warehouse.sqlite --output experiments/signals.json
python3 risk_review.py \
  --journal experiments/baseline/trade_journal.jsonl \
  --baseline experiments/baseline/baseline_report.json \
  --output experiments/risk-review.json
python3 account_viability.py --output experiments/viability.json
python3 deployment_gate.py \
  --warehouse warehouse.sqlite \
  --baseline experiments/baseline/baseline_report.json \
  --analysis experiments/analysis.json \
  --walk-forward experiments/walk-forward.json \
  --viability experiments/viability.json \
  --risk-review experiments/risk-review.json \
  --signal-diagnostics experiments/signals.json \
  --output experiments/deployment-gate.json
```

The full download is several GB; `--help` on each command lists every cost and split option.
A complete run is deterministic for a fixed seed, input warehouse, and registry contents.

## Configuration and safety

All settings live in `.env` (ignored) with placeholders in `.env.example`:

- `ENGINE_MODE` — `PAPER` (default) or `LIVE`
- `BINANCE_ORDER_MODE` — `observe` (default) or `live`
- `BINANCE_ENABLE_LIVE_ORDERS` — `false` by default
- `BINANCE_LIVE_CONFIRMATION` — exact phrase required for live writes
- `research/runtime/LIVE_GATE.json` — must contain `"decision": "ALLOW_LIVE_TRADING"`
- `research/runtime/KILL_SWITCH` — if present, everything stops

The connector additionally refuses stale or sequence-gapped order books, rejects unsigned
or ambiguously acknowledged writes, and treats 5xx signed responses as unknown execution
status rather than retrying blindly. Engine-level limits cap position size, total exposure,
daily loss, consecutive losses, spread, volatility, and required expected net profit.

## Limitations

- OHLCV data has no historical bid/ask, queue, or order-book state, so spread, slippage, and
  impact are explicit conservative **proxies**, not observed execution.
- Baseline rules are fixed, not fitted; results are descriptive, not proof of edge.
- The local `materials/` reference corpus (books and specifications) is not distributed
  because of copyright. Research commands that validate the knowledge base expect it to be
  present locally; the assumption registry remains readable without it.
- Large generated data (warehouse, raw archives, experiment reports, runtime state) is
  intentionally excluded from version control.

## License

MIT — see [LICENSE](LICENSE). The vendored `research/binance-public-data-master/` snapshot is
MIT-licensed upstream; see its [NOTICE](research/binance-public-data-master/NOTICE.md).
