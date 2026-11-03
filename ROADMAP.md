# Roadmap

The rule for this project: **evidence before capital.** No strategy is promoted, and no live
order path is opened, until the research gate says the evidence supports it. Work is ordered
by what most improves the credibility of that decision.

## Done

- [x] Resumable, checksum-verified Binance kline downloader
- [x] Stdlib-only archive validator (schema, timestamps, candle invariants, coverage)
- [x] Canonical SQLite warehouse with provenance and per-row quality flags
- [x] Cost-aware baseline backtests on chronological splits
- [x] Descriptive out-of-sample analysis against `NO_TRADE` and `BUY_AND_HOLD`
- [x] Fixed-rule walk-forward selection with `NO_TRADE` fallback
- [x] Predeclared signal diagnostics with multiple-testing correction
- [x] Block-bootstrap drawdown / ruin diagnostics
- [x] Small-account viability gate
- [x] Fail-closed deployment gate with artifact hashes
- [x] Guarded Binance REST/WS connector and paper/live engine
- [x] Local dashboard with plain-language safety readout
- [x] 56 unit tests covering data tools, research outputs, gates, and engine behavior

## Now (documentation and reproducibility, ~6–10 hours)

- [ ] Add dashboard screenshots and a short architecture diagram to the README
- [ ] Commit a small synthetic kline fixture so the pipeline can be exercised without a
      multi-GB download
- [ ] Add CI (GitHub Actions) running the unit tests on push and pull request
- [ ] Make the dashboard show a clear "research artifacts not built yet" state on a fresh clone
- [ ] Document each research command's cost assumptions and split dates in one place
- [ ] Replace machine-specific paths in the reference registry with repo-relative paths
      (in progress)

## Next (execution realism, ~20–30 hours)

- [ ] Acquire and validate historical trades, best bid/ask, and order-book updates
- [ ] Replace OHLCV spread/slippage/impact proxies with exchange-specific fee, latency, and
      fill models
- [ ] Verify current Binance symbol filters, minimum notional, and fee schedules from official
      documentation instead of scenario defaults
- [ ] Run predeclared feature/target experiments with confidence intervals and strict
      multiple-testing control
- [ ] Separate calibration from evaluation; report reliability curves and drift monitoring
- [ ] Extend paper trading to multi-day sessions with restart/reconciliation tests
- [ ] Add property-based tests around order sizing, rounding, and kill-switch behavior

## Later (only after the gate passes, ~20+ hours)

- [ ] Portfolio-level exposure and correlation limits across symbols
- [ ] Reproducible experiment registry with immutable run IDs and artifact retention
- [ ] Optional read-only multi-venue comparison (fees, liquidity, minimums)
- [ ] Packaging and a one-command research run
- [ ] Operator runbook for interruption, reconciliation, and incident review

## Explicitly out of scope

- Promising or advertising returns
- Collecting other people's personal or account data
- Unattended live trading without a human-reviewed gate and kill switch
- Model training, fine-tuning, or benchmarking
